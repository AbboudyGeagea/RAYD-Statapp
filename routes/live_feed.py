"""
routes/live_feed.py
────────────────────────────────────────────────────────────────
RAYD Live AE Status Feed
Real-time map of which AEs are busy / free / delayed right now.

Formula per AE:
  now <= order.scheduled_datetime + procedure_duration  → BUSY
  now > scheduled_datetime + duration                   → FINISHED (or FREE if no order)
  now > scheduled_datetime (but within duration)        → check delay

Registered in registry.py:
    from routes.live_feed import live_feed_bp
    app.register_blueprint(live_feed_bp)
"""

import logging
from datetime import datetime, timedelta, timezone
from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db

logger       = logging.getLogger("LIVE_FEED")
live_feed_bp = Blueprint("live_feed", __name__)

# ── Page ──────────────────────────────────────────────────────────────────────
@live_feed_bp.route("/viewer/live")
@login_required
def live_page():
    return render_template("live_feed.html")


# ── API — full AE status snapshot ────────────────────────────────────────────
@live_feed_bp.route("/viewer/live/status")
@login_required
def live_status():
    """
    Returns current status for every AE.
    Clients call this on load and whenever the next_refresh_in countdown hits 0.
    """
    try:
        now     = datetime.now()
        today   = now.date()
        dow     = today.weekday()
        now_str = now.strftime("%H:%M:%S")

        # All AEs
        aes = db.session.execute(text("""
            SELECT aetitle, modality
            FROM aetitle_modality_map
            ORDER BY modality, aetitle
        """)).mappings().fetchall()

        # Today's orders with durations — all of them
        orders = db.session.execute(text("""
            SELECT
                o.proc_id,
                o.proc_text,
                o.scheduled_datetime,
                o.patient_dbid,
                o.order_status,
                COALESCE(pm.duration_minutes, 15)   AS duration,
                COALESCE(m.aetitle, s.storing_ae)   AS aetitle
            FROM etl_orders o
            LEFT JOIN etl_didb_studies s
                ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
            LEFT JOIN aetitle_modality_map m
                ON m.aetitle = s.storing_ae
            LEFT JOIN procedure_duration_map pm
                ON pm.procedure_code = o.proc_id
            WHERE o.scheduled_datetime::date = :today
              AND o.has_study = TRUE
            ORDER BY o.scheduled_datetime
        """), {"today": today}).mappings().fetchall()

        # Build a lookup: aetitle → list of orders today
        ae_orders = {}
        for o in orders:
            ae = o["aetitle"]
            if not ae:
                continue
            ae_orders.setdefault(ae, []).append(dict(o))

        # Weekly schedule for opening hours check
        schedules = db.session.execute(text("""
            SELECT aetitle, std_opening_minutes
            FROM device_weekly_schedule
            WHERE day_of_week = :dow
        """), {"dow": dow}).mappings().fetchall()
        opening_map = {r["aetitle"]: r["std_opening_minutes"] for r in schedules}

        # Exceptions today
        exceptions = db.session.execute(text("""
            SELECT aetitle, actual_opening_minutes
            FROM device_exceptions
            WHERE exception_date = :today
        """), {"today": today}).mappings().fetchall()
        for exc in exceptions:
            opening_map[exc["aetitle"]] = exc["actual_opening_minutes"]

        result         = []
        next_event_min = None  # Earliest future state change (minutes from now)

        for ae in aes:
            aetitle  = ae["aetitle"]
            modality = ae["modality"]
            opening  = opening_map.get(aetitle, 0)

            if opening == 0:
                result.append(_make_tile(aetitle, modality, "closed", None, None, None))
                continue

            ae_day_orders = ae_orders.get(aetitle, [])

            # Find active order: scheduled_datetime <= now < scheduled_datetime + duration
            active    = None
            next_order = None
            for o in ae_day_orders:
                sched = o["scheduled_datetime"]
                if not isinstance(sched, datetime):
                    try:    sched = datetime.fromisoformat(str(sched))
                    except: continue

                end_time = sched + timedelta(minutes=int(o["duration"]))

                if sched <= now < end_time:
                    active = {**o, "sched": sched, "end_time": end_time}
                    break
                elif sched > now and next_order is None:
                    next_order = {**o, "sched": sched, "end_time": sched + timedelta(minutes=int(o["duration"]))}

            if active:
                mins_remaining = int((active["end_time"] - now).total_seconds() / 60)
                delay_mins     = int((now - active["sched"]).total_seconds() / 60) if now > active["sched"] else 0

                # Update next refresh trigger
                if next_event_min is None or mins_remaining < next_event_min:
                    next_event_min = mins_remaining

                status = "delayed" if delay_mins > 5 else "busy"
                tile   = _make_tile(
                    aetitle, modality, status,
                    proc_code  = active["proc_id"],
                    proc_name  = active["proc_text"] or active["proc_id"],
                    patient_id = _mask(active["patient_dbid"]),
                    mins_remaining = mins_remaining,
                    delay_mins     = delay_mins if delay_mins > 0 else None,
                    next_order     = _format_next(next_order),
                )
            else:
                # Check if the last order finished early (within last 15 min)
                finished_early = None
                for o in reversed(ae_day_orders):
                    sched = o["scheduled_datetime"]
                    if not isinstance(sched, datetime):
                        try:    sched = datetime.fromisoformat(str(sched))
                        except: continue
                    end_time      = sched + timedelta(minutes=int(o["duration"]))
                    expected_end  = sched + timedelta(minutes=int(o["duration"]))
                    if end_time > now:
                        # Order window hasn't expired yet but no active study → finished early
                        early_by = int((expected_end - now).total_seconds() / 60)
                        finished_early = {
                            "proc_name": o["proc_text"] or o["proc_id"],
                            "early_by":  early_by,
                        }
                        if next_event_min is None or early_by < next_event_min:
                            next_event_min = early_by
                        break

                status = "early" if finished_early else "free"
                tile   = _make_tile(
                    aetitle, modality, status,
                    next_order    = _format_next(next_order),
                    finished_early= finished_early,
                )

            result.append(tile)

        # Sort: busy/delayed first, then early, then free, then closed
        ORDER = {"delayed":0, "busy":1, "early":2, "free":3, "closed":4}
        result.sort(key=lambda t: ORDER.get(t["status"], 5))

        return jsonify({
            "tiles":           result,
            "as_of":           now.strftime("%H:%M:%S"),
            "next_refresh_in": max(next_event_min, 1) if next_event_min is not None else 60,
            "version":         int(now.timestamp()),
        })

    except Exception as e:
        logger.error(f"Live status error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── API — lightweight version check for HL7-triggered refresh ─────────────────
@live_feed_bp.route("/viewer/live/version")
@login_required
def live_version():
    """
    Returns the timestamp of the latest HL7 order received today.
    Client polls this every 15s — if version changes, triggers full refresh.
    """
    try:
        row = db.session.execute(text("""
            SELECT MAX(received_at) AS latest
            FROM hl7_orders
            WHERE received_at::date = CURRENT_DATE
        """)).fetchone()
        latest = row[0] if row and row[0] else None
        return jsonify({"version": latest.isoformat() if latest else "none"})
    except Exception as e:
        return jsonify({"version": "none", "error": str(e)})


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_tile(aetitle, modality, status, proc_code=None, proc_name=None,
               patient_id=None, mins_remaining=None, delay_mins=None,
               next_order=None, finished_early=None):
    return {
        "aetitle":        aetitle,
        "modality":       modality,
        "status":         status,
        "proc_code":      proc_code,
        "proc_name":      proc_name,
        "patient_id":     patient_id,
        "mins_remaining": mins_remaining,
        "delay_mins":     delay_mins,
        "next_order":     next_order,
        "finished_early": finished_early,
    }


def _mask(patient_id):
    """Mask patient ID — show first 2 and last 2 chars only."""
    s = str(patient_id or "")
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]


def _format_next(order):
    if not order:
        return None
    sched = order.get("sched")
    return {
        "proc_name": order.get("proc_text") or order.get("proc_id") or "—",
        "at":        sched.strftime("%H:%M") if isinstance(sched, datetime) else "—",
    }
