"""
routes/live_feed.py
────────────────────────────────────────────────────────────────
RAYD Live Modality Status Feed  (admin only)

Data source : hl7_orders  (real-time, not etl_orders which arrives next-day)
Grouped by  : modality    (AE assignment requires didb_studies — 1-day lag)

Status per modality:
  delayed  → any active order has passed its expected finish time
  busy     → has active orders, none overrun
  free     → no active orders right now
  closed   → all AEs of this modality have 0 opening minutes today

Refresh triggers:
  1. New HL7 insert  → pg_notify 'hl7_new_order' → SSE push → immediate reload
  2. Countdown       → earliest active-order finish time  → next_refresh_in
  3. All overrun     → 2-minute fallback instead of 60-minute default

Registered in registry.py:
    from routes.live_feed import live_feed_bp
    app.register_blueprint(live_feed_bp)
"""
#live_feed.py
import logging
import select
import psycopg2
from datetime import datetime, timedelta
from flask import (Blueprint, Response, jsonify, render_template,
                   request, stream_with_context, abort, current_app)
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page

logger       = logging.getLogger("LIVE_FEED")
live_feed_bp = Blueprint("live_feed", __name__)


# ── Page ──────────────────────────────────────────────────────────────────────
@live_feed_bp.route("/viewer/live")
@login_required
def live_page():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    return render_template("live_feed.html")


# ── API — full modality status snapshot ───────────────────────────────────────
@live_feed_bp.route("/viewer/live/status")
@login_required
def live_status():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    try:
        now   = datetime.now()
        today = now.date()
        dow   = today.weekday()

        # Distinct modalities from device map
        mod_rows = db.session.execute(text("""
            SELECT DISTINCT modality
            FROM aetitle_modality_map
            ORDER BY modality
        """)).mappings().fetchall()
        modalities = [r["modality"] for r in mod_rows]

        # Opening hours per modality — any AE open means modality is open
        schedules = db.session.execute(text("""
            SELECT m.modality, SUM(s.std_opening_minutes) AS total_mins
            FROM device_weekly_schedule s
            JOIN aetitle_modality_map m ON m.aetitle = s.aetitle
            WHERE s.day_of_week = :dow
            GROUP BY m.modality
        """), {"dow": dow}).mappings().fetchall()
        opening_map = {r["modality"]: (r["total_mins"] or 0) for r in schedules}

        # Override with today's exceptions
        exceptions = db.session.execute(text("""
            SELECT m.modality, SUM(e.actual_opening_minutes) AS total_mins
            FROM device_exceptions e
            JOIN aetitle_modality_map m ON m.aetitle = e.aetitle
            WHERE e.exception_date = :today
            GROUP BY m.modality
        """), {"today": today}).mappings().fetchall()
        for exc in exceptions:
            opening_map[exc["modality"]] = exc["total_mins"] or 0

        # Orders from yesterday onwards — catches procedures that cross midnight.
        # Python logic below determines which are truly active.
        orders = db.session.execute(text("""
            SELECT
                o.patient_id,
                o.scheduled_datetime,
                o.procedure_text,
                o.procedure_code,
                o.modality,
                COALESCE(pm.duration_minutes, 15) AS duration,
                (pm.procedure_code IS NULL)        AS unknown_code
            FROM hl7_orders o
            LEFT JOIN procedure_duration_map pm
                   ON pm.procedure_code = o.procedure_code
            WHERE o.scheduled_datetime >= CURRENT_DATE - INTERVAL '1 day'
              AND o.scheduled_datetime <  CURRENT_DATE + INTERVAL '1 day'
              AND COALESCE(o.order_status, '') != 'CA'
            ORDER BY o.scheduled_datetime
        """)).mappings().fetchall()

        # Group by modality
        mod_orders = {}
        for o in orders:
            mod = (o["modality"] or "").upper() or "UNKNOWN"
            mod_orders.setdefault(mod, []).append(dict(o))

        result         = []
        next_event_min = None

        for mod in modalities:
            opening = opening_map.get(mod, 0)

            if opening == 0:
                result.append(_make_tile(mod, "closed", [], None))
                continue

            day_orders    = mod_orders.get(mod, [])
            active_orders = []
            next_order    = None

            for o in day_orders:
                sched = o["scheduled_datetime"]
                if not isinstance(sched, datetime):
                    try:    sched = datetime.fromisoformat(str(sched))
                    except: continue

                duration       = int(o["duration"])
                end_time       = sched + timedelta(minutes=duration)
                mins_remaining = int((end_time - now).total_seconds() / 60)
                overrun        = mins_remaining < 0

                if sched <= now:
                    active_orders.append({
                        "patient_id":     _mask(o["patient_id"]),
                        "procedure_text": o["procedure_text"] or o["procedure_code"] or "—",
                        "procedure_code": o["procedure_code"] or "",
                        "unknown_code":   bool(o["unknown_code"]),
                        "end_time":       end_time.strftime("%H:%M"),
                        "mins_remaining": mins_remaining,
                        "overrun":        overrun,
                    })
                    # Countdown to next non-overrun finish
                    if not overrun and mins_remaining > 0:
                        if next_event_min is None or mins_remaining < next_event_min:
                            next_event_min = mins_remaining
                elif next_order is None:
                    mins_until = int((sched - now).total_seconds() / 60)
                    next_order = {
                        "proc_name": o["procedure_text"] or o["procedure_code"] or "—",
                        "at":        sched.strftime("%H:%M"),
                    }
                    if next_event_min is None or mins_until < next_event_min:
                        next_event_min = max(mins_until, 1)

            if active_orders:
                status = "delayed" if any(a["overrun"] for a in active_orders) else "busy"
            else:
                status = "free"

            result.append(_make_tile(mod, status, active_orders, next_order))

        # Sort: delayed → busy → free → closed
        ORDER = {"delayed": 0, "busy": 1, "free": 2, "closed": 3}
        result.sort(key=lambda t: ORDER.get(t["status"], 5))

        # If every active order is overrun, fall back to 2-min refresh instead of 60
        has_overrun = any(a["overrun"] for t in result for a in t.get("active_orders", []))
        fallback    = 2 if has_overrun else 60

        return jsonify({
            "tiles":           result,
            "as_of":           now.strftime("%H:%M:%S"),
            "next_refresh_in": max(next_event_min, 1) if next_event_min is not None else fallback,
        })

    except Exception as e:
        logger.error(f"Live status error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── API — lightweight version check (SSE fallback) ───────────────────────────
@live_feed_bp.route("/viewer/live/version")
@login_required
def live_version():
    """
    Returns the timestamp of the latest HL7 order received today.
    Used only when the SSE connection is unavailable (polled every 15 s).
    """
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
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


# ── API — SSE push on new HL7 insert ──────────────────────────────────────────
@live_feed_bp.route("/viewer/live/events")
@login_required
def live_events():
    """
    Server-Sent Events endpoint.
    Keeps a persistent psycopg2 connection listening on 'hl7_new_order'.
    Sends 'data: new_order' whenever a new HL7 order is committed.
    Sends a heartbeat comment every 25 s to keep proxies from closing the pipe.
    """
    if not user_has_page(current_user, 'live_feed'):
        abort(403)

    # Resolve DSN once — captured in the generator closure
    raw_url = current_app.config['SQLALCHEMY_DATABASE_URI']
    dsn     = raw_url.replace('postgresql+psycopg2://', 'postgresql://')

    def event_stream():
        conn = None
        try:
            conn = psycopg2.connect(dsn)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("LISTEN hl7_new_order;")

            while True:
                # Wait up to 25 s for a notification; send heartbeat if nothing arrives
                ready = select.select([conn], [], [], 25)[0]
                if ready:
                    conn.poll()
                    while conn.notifies:
                        conn.notifies.pop(0)
                        yield "data: new_order\n\n"
                else:
                    yield ": heartbeat\n\n"

        except GeneratorExit:
            pass
        except Exception as exc:
            logger.warning(f"SSE stream error: {exc}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    return Response(
        stream_with_context(event_stream()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':    'no-cache',
            'X-Accel-Buffering':'no',
            'Connection':       'keep-alive',
        },
    )


# ── API — add / update procedure duration ─────────────────────────────────────
@live_feed_bp.route("/viewer/live/add_procedure", methods=["POST"])
@login_required
def add_procedure():
    """
    Insert or update a procedure code in procedure_duration_map.
    Called from the unknown-code modal on the live feed page.
    """
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    try:
        data     = request.get_json(force=True)
        code     = (data.get("procedure_code") or "").strip()
        duration = int(data.get("duration_minutes") or 15)
        if not code:
            return jsonify({"error": "procedure_code is required"}), 400
        if duration < 1:
            return jsonify({"error": "duration_minutes must be >= 1"}), 400

        db.session.execute(text("""
            INSERT INTO procedure_duration_map (procedure_code, duration_minutes)
            VALUES (:code, :duration)
            ON CONFLICT (procedure_code)
            DO UPDATE SET duration_minutes = EXCLUDED.duration_minutes
        """), {"code": code, "duration": duration})
        db.session.commit()
        logger.info(f"Procedure code added/updated: {code} → {duration} min")
        return jsonify({"ok": True})

    except Exception as e:
        db.session.rollback()
        logger.error(f"add_procedure error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_tile(modality, status, active_orders, next_order):
    return {
        "modality":      modality,
        "status":        status,
        "active_orders": active_orders,
        "next_order":    next_order,
    }


def _mask(patient_id):
    """Show first 2 and last 2 chars only."""
    s = str(patient_id or "")
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]
