"""
routes/live_feed.py
────────────────────────────────────────────────────────────────
RAYD Live Department View  (admin only)

NOTE (2026-07-27): this docstring previously said the status feed read
hl7_orders (grouped by modality) — that was true of an earlier version of
this file but is NO LONGER ACCURATE for the main /viewer/live/status route.
Corrected below; see live_status()'s own docstring for the full detail.

Data source : live query against RIS's SPS (Scheduled Procedure Step) table
              via OracleConnector — NOT hl7_orders, NOT etl_orders. Nothing
              from this query is persisted to Postgres; it runs fresh on
              every request. hl7_orders is still used, but only for three
              unrelated, narrower concerns: orphan-order detection (this
              file's /orphans + the orphan count on /status), the TAT
              endpoint (/tat), and the SSE-fallback version poll (/version).
Grouped by  : DEVICE (aetitle), one tile per row of aetitle_modality_map —
              not pooled by modality type. SPS.MODALITY_KEY → MODALITY.AE_TITLE
              gives the real per-device assignment at scheduling time, so the
              old "AE-level grouping needs didb_studies, which lags a day"
              constraint no longer applies. "modality" is kept per-tile only
              as the type label used for the filter chips/color-coding.

Status per device:
  delayed  → any active order has passed its expected finish time
  busy     → has active orders, none overrun
  free     → no active orders right now
  closed   → this device has 0 opening minutes today (device_weekly_schedule /
             device_exceptions)

Refresh triggers (unchanged by the RIS-SPS migration above, still real):
  1. New HL7 order insert (hl7_listener.py) → pg_notify 'hl7_new_order' →
     this file's /events SSE endpoint LISTENs and pushes "new_order" →
     browser reloads immediately. Verified wired end-to-end. Caveat: since
     the tile data source is now RIS SPS rather than hl7_orders, a new HL7
     order is a proxy signal for "the board may have changed," not a direct
     one — a pure RIS-side reschedule with no corresponding HL7 message
     would not trigger this push (covered instead by triggers 2/3 below).
  2. Countdown       → earliest active-order finish time  → next_refresh_in
  3. All overrun     → 2-minute fallback instead of 60-minute default

Known dead weight (not fixed here — presentation-only pass, no backend
rewrite): the arrive/start/dismiss/revert/link workflow endpoints below key
off hl7_orders.message_id, but tile active_orders (sourced from SPS) never
carry a message_id — only order_id (=SPS_ID). They're also unreachable from
templates/live_feed.html today (no button ever called them). Left in place;
would need an SPS→hl7_orders join (there's no shared key pre-PACS-creation)
to make functional again.

Registered in registry.py:
    from routes.live_feed import live_feed_bp
    app.register_blueprint(live_feed_bp)
"""
#live_feed.py
import os
import logging
import select
import psycopg2
from datetime import datetime, timedelta
from flask import (Blueprint, Response, jsonify, render_template,
                   request, stream_with_context, abort, current_app)
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page, OracleConnector
from utils.hl7_forward import forward_message as _hl7_forward

logger       = logging.getLogger("LIVE_FEED")
live_feed_bp = Blueprint("live_feed", __name__)


# ── Page ──────────────────────────────────────────────────────────────────────
@live_feed_bp.route("/viewer/live")
@login_required
def live_page():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    return render_template("live_feed.html")


# ── API — full device status snapshot (live RIS SPS query) ────────────────────
@live_feed_bp.route("/viewer/live/status")
@login_required
def live_status():
    """
    Per-DEVICE (not per-modality) live status, sourced from a live query against
    RIS's SPS (Scheduled Procedure Step) table — not hl7_orders. hl7_orders only
    ever carried modality TYPE (see the old comment this replaced: "AE assignment
    requires didb_studies — 1-day lag"); SPS.MODALITY_KEY is the actual device
    assignment, known at scheduling time, which is what makes per-device tiles
    possible at all. Never persisted to Postgres — run fresh on every request,
    same as hl7_orders was read fresh before. Refresh mechanics (SSE push,
    countdown-to-next-event, 60s/2min fallback) are unchanged.
    """
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    try:
        now   = datetime.now()
        today = now.date()
        dow   = today.weekday()

        # Ensure workflow + link columns exist before any route in this blueprint
        # queries/updates them — this was the only place that ran this guard;
        # arrive_order/start_order/dismiss_order have no guard of their own.
        try:
            db.session.execute(text("""
                ALTER TABLE hl7_orders
                    ADD COLUMN IF NOT EXISTS linked_accession_number VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS linked_study_db_uid BIGINT,
                    ADD COLUMN IF NOT EXISTS linked_by VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS linked_at TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS arrived_at TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS arrived_by VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS started_at  TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS started_by  VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS done_at     TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS done_by     VARCHAR(100)
            """))
            db.session.commit()
        except Exception:
            db.session.rollback()

        # All known devices (not just ones with active orders — need the full
        # set to show free/closed tiles too), with floor-plan position if placed.
        device_rows = db.session.execute(text("""
            SELECT aetitle, modality, COALESCE(display_aetitle, aetitle) AS label,
                   floor_x, floor_y
            FROM aetitle_modality_map
            ORDER BY modality, aetitle
        """)).mappings().fetchall()
        devices = [dict(r) for r in device_rows]

        # Opening hours per DEVICE (was: summed across every AE of a modality —
        # each device now gets its own tile, so it needs its own opening minutes).
        schedules = db.session.execute(text("""
            SELECT aetitle, std_opening_minutes FROM device_weekly_schedule WHERE day_of_week = :dow
        """), {"dow": dow}).mappings().fetchall()
        opening_map = {r["aetitle"]: (r["std_opening_minutes"] or 0) for r in schedules}

        exceptions = db.session.execute(text("""
            SELECT aetitle, actual_opening_minutes FROM device_exceptions WHERE exception_date = :today
        """), {"today": today}).mappings().fetchall()
        for exc in exceptions:
            opening_map[exc["aetitle"]] = exc["actual_opening_minutes"] or 0

        # RIS status_key -> short hl7-style code (NW/SC/CM/CA/DC/IP), same
        # convention ETL_JOBS/etl_orders.py already translates to via
        # worklist_status_map — lets us reuse the identical CA/CM exclusion
        # against live RIS data instead of hl7_orders.order_status.
        status_map = dict(db.session.execute(text(
            "SELECT status_key, hl7_code FROM std_status_ris WHERE type = 'SPS'"
        )).fetchall())

        # ── Live query against RIS — today's scheduled procedure steps, with the
        # actual device (SPS.MODALITY_KEY -> MODALITY.AE_TITLE), not just modality
        # type. Patient name/DOB shown live (never written to Postgres) to match
        # what this admin-only operational board already showed via hl7_orders —
        # not a new PHI exposure, same fields, different (more accurate) source.
        orders = []
        try:
            ris_src  = os.getenv('RAYD_RIS_SOURCE', 'ris')
            ora_conn = OracleConnector.get_connection(ris_src)
            cursor   = ora_conn.cursor()
            cursor.execute("""
                SELECT
                    sps.SPS_ID, m.AE_TITLE, sps.START_DATETIME, sps.DURATION,
                    sps.STATUS_KEY, sps.PATIENT_ARRIVED_DATE,
                    sc.CODE, sc.DESCRIPTION,
                    per.FIRST_NAME, per.LAST_NAME, pat.BIRTH_DATE,
                    w.PATIENT_PERSON_KEY
                FROM SPS sps
                JOIN SITE_WORKLIST w  ON w.SPS_ID = sps.SPS_ID
                JOIN ORDERS o         ON o.ORDER_KEY = w.ORDER_KEY
                LEFT JOIN MODALITY m  ON m.MODALITY_KEY = sps.MODALITY_KEY
                LEFT JOIN SPS_CODE sc ON sc.SPS_CODE_KEY = sps.SPS_CODE_KEY
                LEFT JOIN PATIENT pat ON pat.PATIENT_PERSON_KEY = w.PATIENT_PERSON_KEY
                LEFT JOIN PERSON per  ON per.PERSON_KEY = w.PATIENT_PERSON_KEY
                WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
                  AND sps.START_DATETIME >= TRUNC(SYSDATE)
                  AND sps.START_DATETIME <  TRUNC(SYSDATE) + 1
            """)
            cols = [d[0].lower() for d in cursor.description]
            orders = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            ora_conn.close()
        except Exception:
            logger.exception("Live RIS SPS query failed — showing devices with no schedule data this refresh")

        # Group by device (AE title) — was: modality
        dev_orders = {}
        for o in orders:
            ae = (o.get("ae_title") or "").upper() or "UNKNOWN"
            dev_orders.setdefault(ae, []).append(o)

        result         = []
        next_event_min = None

        for dev in devices:
            aetitle = dev["aetitle"]
            opening = opening_map.get(aetitle, 0)

            if opening == 0:
                result.append(_make_tile(dev, "closed", [], None))
                continue

            day_orders    = dev_orders.get((aetitle or "").upper(), [])
            active_orders = []
            next_order    = None

            for o in day_orders:
                hl7_code = status_map.get(o.get("status_key"))
                if hl7_code in ("CA", "CM"):
                    continue  # cancelled / already completed — not "active"

                sched = o.get("start_datetime")
                if not isinstance(sched, datetime):
                    try:    sched = datetime.fromisoformat(str(sched))
                    except: continue

                duration       = int(o.get("duration") or 15)
                end_time       = sched + timedelta(minutes=duration)
                mins_remaining = int((end_time - now).total_seconds() / 60)
                overrun        = mins_remaining < 0
                is_present     = o.get("patient_arrived_date") is not None

                if sched <= now or is_present:
                    dob  = o.get("birth_date")
                    name = " ".join(p for p in [o.get("first_name"), o.get("last_name")] if p) or "—"
                    active_orders.append({
                        "order_id":            o.get("sps_id"),
                        "order_status":        hl7_code or "—",
                        "patient_id":          str(o.get("patient_person_key") or "—"),
                        "patient_name":        name,
                        "date_of_birth":       dob.strftime("%d-%m-%Y") if dob else "—",
                        "referring_physician": "—",  # not on SPS/SITE_WORKLIST — needs ORDERS' requesting resource, follow-up
                        "accession_number":    "—",  # not assigned until PACS study creation, by design
                        "procedure_text":      o.get("description") or "—",
                        "procedure_code":      o.get("code") or "",
                        "unknown_code":        False,
                        "end_time":            end_time.strftime("%H:%M"),
                        "mins_remaining":      mins_remaining,
                        "overrun":             overrun,
                    })
                    # Countdown to next non-overrun finish
                    if not overrun and mins_remaining > 0:
                        if next_event_min is None or mins_remaining < next_event_min:
                            next_event_min = mins_remaining
                elif next_order is None:
                    mins_until = int((sched - now).total_seconds() / 60)
                    next_order = {
                        "proc_name": o.get("description") or o.get("code") or "—",
                        "at":        sched.strftime("%H:%M"),
                    }
                    if next_event_min is None or mins_until < next_event_min:
                        next_event_min = max(mins_until, 1)

            if active_orders:
                status = "delayed" if any(a["overrun"] for a in active_orders) else "busy"
            else:
                status = "free"

            result.append(_make_tile(dev, status, active_orders, next_order))

        # Sort: delayed → busy → free → closed
        ORDER = {"delayed": 0, "busy": 1, "free": 2, "closed": 3}
        result.sort(key=lambda t: ORDER.get(t["status"], 5))

        # If every active order is overrun, fall back to 2-min refresh instead of 60
        has_overrun = any(a["overrun"] for t in result for a in t.get("active_orders", []))
        fallback    = 2 if has_overrun else 60

        # Orphan-order detection stays hl7_orders-based — a distinct concern
        # (is the HL7 feed itself keeping up / syncing to PACS), unrelated to
        # which device a study is scheduled on.
        orphan_orders = db.session.execute(text("""
            SELECT COUNT(*)
            FROM hl7_orders o
            LEFT JOIN etl_didb_studies s ON s.accession_number = o.accession_number
            WHERE (
                (o.scheduled_datetime >= CURRENT_DATE AND o.scheduled_datetime < CURRENT_DATE + INTERVAL '1 day')
                OR
                (o.scheduled_datetime IS NULL AND o.received_at >= CURRENT_DATE AND o.received_at < CURRENT_DATE + INTERVAL '1 day')
            )
              AND COALESCE(o.order_status, '') NOT IN ('CA', 'CM')
              AND o.pacs_done_at IS NULL
              AND s.accession_number IS NULL
              AND o.linked_accession_number IS NULL
              AND o.linked_study_db_uid IS NULL
        """)).scalar() or 0

        return jsonify({
            "tiles":           result,
            "as_of":           now.strftime("%H:%M:%S"),
            "next_refresh_in": max(next_event_min, 1) if next_event_min is not None else fallback,
            "orphan_orders":   int(orphan_orders),
        })

    except Exception as e:
        db.session.rollback()
        logger.error(f"Live status error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── API — Patient Waiting Time TAT ───────────────────────────────────────────
@live_feed_bp.route("/viewer/live/tat")
@login_required
def live_tat():
    """
    Returns completed exam TAT stats (two sources) for a given date (defaults to today).
      Done TAT      = done_at      - scheduled_datetime  (technician manual done)
      PACS Done TAT = pacs_done_at - scheduled_datetime  (PACS/scanner confirmation)
    A row appears if either done_at or pacs_done_at is set for the target date.
    Accepts optional ?date=YYYY-MM-DD query parameter.
    """
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    from datetime import date as _date
    raw_date = request.args.get('date', '').strip()
    try:
        target_date = str(_date.fromisoformat(raw_date)) if raw_date else str(_date.today())
    except ValueError:
        target_date = str(_date.today())
    try:
        rows = db.session.execute(text("""
            SELECT
                o.message_id,
                o.patient_id,
                o.accession_number,
                o.procedure_text,
                o.modality,
                o.scheduled_datetime,
                o.done_at,
                o.done_by,
                o.pacs_done_at,
                COALESCE(p.duration_minutes, 30) AS proc_duration,
                CASE WHEN o.done_at IS NOT NULL AND o.done_at > o.scheduled_datetime
                     THEN EXTRACT(EPOCH FROM (o.done_at - o.scheduled_datetime)) / 60.0
                END AS done_tat_min,
                CASE WHEN o.pacs_done_at IS NOT NULL AND o.pacs_done_at > o.scheduled_datetime
                     THEN EXTRACT(EPOCH FROM (o.pacs_done_at - o.scheduled_datetime)) / 60.0
                END AS pacs_tat_min
            FROM hl7_orders o
            LEFT JOIN procedure_duration_map p
                   ON UPPER(TRIM(o.procedure_code)) = UPPER(TRIM(p.procedure_code))
            WHERE o.scheduled_datetime IS NOT NULL
              AND (
                  (o.done_at IS NOT NULL      AND o.done_at::date      = :target_date)
               OR (o.pacs_done_at IS NOT NULL AND o.pacs_done_at::date = :target_date)
              )
            ORDER BY GREATEST(COALESCE(o.done_at, '-infinity'), COALESCE(o.pacs_done_at, '-infinity')) DESC
        """), {'target_date': target_date}).mappings().fetchall()

        exams = []
        for r in rows:
            done_tat  = float(r["done_tat_min"])  if r["done_tat_min"]  is not None else None
            pacs_tat  = float(r["pacs_tat_min"])  if r["pacs_tat_min"]  is not None else None
            exams.append({
                "message_id":       r["message_id"],
                "patient_id":       r["patient_id"] or "—",
                "accession_number": r["accession_number"] or "—",
                "procedure_text":   r["procedure_text"] or "—",
                "modality":         r["modality"] or "—",
                "scheduled_at":     r["scheduled_datetime"].strftime("%H:%M") if r["scheduled_datetime"] else "—",
                "done_at":          r["done_at"].strftime("%H:%M") if r["done_at"] else "—",
                "done_by":          r["done_by"] or "—",
                "pacs_done_at":     r["pacs_done_at"].strftime("%H:%M") if r["pacs_done_at"] else "—",
                "proc_duration":    int(r["proc_duration"]) if r["proc_duration"] is not None else None,
                "done_tat_min":     round(done_tat, 1) if done_tat is not None else None,
                "pacs_tat_min":     round(pacs_tat, 1) if pacs_tat is not None else None,
                # Legacy field — keep for any callers that still read wait_minutes
                "wait_minutes":     round(done_tat, 1) if done_tat is not None else (
                                    round(pacs_tat, 1) if pacs_tat is not None else None),
            })

        done_vals = [e["done_tat_min"] for e in exams if e["done_tat_min"] is not None]
        pacs_vals = [e["pacs_tat_min"] for e in exams if e["pacs_tat_min"] is not None]

        def _stats(vals):
            if not vals: return None, None, None
            return round(sum(vals)/len(vals), 1), round(min(vals), 1), round(max(vals), 1)

        avg_done, min_done, max_done = _stats(done_vals)
        avg_pacs, min_pacs, max_pacs = _stats(pacs_vals)

        return jsonify({
            "exams":      exams,
            "count":      len(exams),
            # Done TAT stats
            "avg_wait":   avg_done,
            "min_wait":   min_done,
            "max_wait":   max_done,
            # PACS Done TAT stats
            "avg_pacs":   avg_pacs,
            "min_pacs":   min_pacs,
            "max_pacs":   max_pacs,
        })

    except Exception as e:
        db.session.rollback()
        logger.error(f"TAT error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── API — orphan order details ────────────────────────────────────────────────
@live_feed_bp.route("/viewer/live/orphans")
@login_required
def live_orphans():
    """Returns today's unmatched HL7 orders (no study, not linked, not cancelled)."""
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    try:
        rows = db.session.execute(text("""
            SELECT
                o.message_id,
                o.patient_id,
                o.accession_number,
                o.procedure_text,
                o.procedure_code,
                o.modality,
                COALESCE(o.scheduled_datetime, o.received_at) AS scheduled_datetime
            FROM hl7_orders o
            LEFT JOIN etl_didb_studies s ON s.accession_number = o.accession_number
            WHERE (
                (o.scheduled_datetime >= CURRENT_DATE AND o.scheduled_datetime < CURRENT_DATE + INTERVAL '1 day')
                OR
                (o.scheduled_datetime IS NULL AND o.received_at >= CURRENT_DATE AND o.received_at < CURRENT_DATE + INTERVAL '1 day')
            )
              AND COALESCE(o.order_status, '') NOT IN ('CA', 'CM')
              AND o.pacs_done_at IS NULL
              AND s.accession_number IS NULL
              AND o.linked_accession_number IS NULL
              AND o.linked_study_db_uid IS NULL
            ORDER BY COALESCE(o.scheduled_datetime, o.received_at)
        """)).mappings().fetchall()

        orphans = [{
            "message_id":       r["message_id"],
            "patient_id":       r["patient_id"] or "—",
            "accession_number": r["accession_number"] or "—",
            "procedure_text":   r["procedure_text"] or r["procedure_code"] or "—",
            "modality":         r["modality"] or "—",
            "scheduled_at":     r["scheduled_datetime"].strftime("%H:%M") if r["scheduled_datetime"] else "—",
        } for r in rows]

        return jsonify({"orphans": orphans, "count": len(orphans)})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Orphans error: {e}", exc_info=True)
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
        db.session.rollback()
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


# ── Status-change helper ─────────────────────────────────────────────────────
def _log_status_change(order_id, message_id, from_status, to_status, username, source):
    try:
        db.session.execute(text("""
            INSERT INTO order_status_log
                (order_id, message_id, from_status, to_status, changed_by, source)
            VALUES (:oid, :mid, :from_s, :to_s, :user, :src)
        """), {"oid": order_id, "mid": message_id, "from_s": from_status,
               "to_s": to_status, "user": username, "src": source})
    except Exception as e:
        logger.error(f"Status log write failed: {e}")


_REVERT_ROLES  = {"admin", "viewer", "tec"}
_PREV_STATUS   = {"CM": "IP", "IP": "AR", "AR": "SC"}
_REVERT_CLEAR  = {
    "CM": "done_at    = NULL, done_by    = NULL",
    "IP": "started_at = NULL, started_by = NULL",
    "AR": "arrived_at = NULL, arrived_by = NULL",
}


# ── API — mark patient as arrived (SC → AR) ───────────────────────────────────
@live_feed_bp.route("/viewer/live/arrive", methods=["POST"])
@login_required
def arrive_order():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    data       = request.get_json(force=True)
    message_id = data.get("message_id")
    if not message_id:
        return jsonify({"error": "message_id required"}), 400
    try:
        row = db.session.execute(text("""
            SELECT id, COALESCE(NULLIF(order_status,''),'SC') AS order_status, raw_message
            FROM hl7_orders WHERE message_id = :mid
        """), {"mid": message_id}).mappings().fetchone()
        if not row:
            return jsonify({"error": "Order not found"}), 404
        prev = row["order_status"]
        if prev != "SC":
            return jsonify({"error": f"Expected SC, current status is {prev}"}), 400
        db.session.execute(text("""
            UPDATE hl7_orders SET order_status='AR', arrived_at=NOW(), arrived_by=:user
            WHERE message_id=:mid
        """), {"mid": message_id, "user": current_user.username})
        _log_status_change(row["id"], message_id, prev, "AR", current_user.username, "live_feed")
        db.session.commit()
        _hl7_forward(row["raw_message"], current_app._get_current_object(), order_id=row["id"])
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── API — mark exam as started (AR → IP) ─────────────────────────────────────
@live_feed_bp.route("/viewer/live/start", methods=["POST"])
@login_required
def start_order():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    data       = request.get_json(force=True)
    message_id = data.get("message_id")
    if not message_id:
        return jsonify({"error": "message_id required"}), 400
    try:
        row = db.session.execute(text("""
            SELECT id, COALESCE(NULLIF(order_status,''),'SC') AS order_status
            FROM hl7_orders WHERE message_id = :mid
        """), {"mid": message_id}).mappings().fetchone()
        if not row:
            return jsonify({"error": "Order not found"}), 404
        prev = row["order_status"]
        if prev != "AR":
            return jsonify({"error": f"Expected AR, current status is {prev}"}), 400
        db.session.execute(text("""
            UPDATE hl7_orders SET order_status='IP', started_at=NOW(), started_by=:user
            WHERE message_id=:mid
        """), {"mid": message_id, "user": current_user.username})
        _log_status_change(row["id"], message_id, prev, "IP", current_user.username, "live_feed")
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── API — mark exam as done (IP → CM) ────────────────────────────────────────
@live_feed_bp.route("/viewer/live/dismiss", methods=["POST"])
@login_required
def dismiss_order():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    data       = request.get_json(force=True)
    message_id = data.get("message_id")
    if not message_id:
        return jsonify({"error": "message_id required"}), 400
    try:
        row = db.session.execute(text("""
            SELECT id, COALESCE(NULLIF(order_status,''),'SC') AS order_status
            FROM hl7_orders WHERE message_id = :mid
        """), {"mid": message_id}).mappings().fetchone()
        if not row:
            return jsonify({"error": "Order not found"}), 404
        prev = row["order_status"]
        if prev != "IP":
            return jsonify({"error": f"Expected IP, current status is {prev}"}), 400
        db.session.execute(text("""
            UPDATE hl7_orders SET order_status='CM', done_at=NOW(), done_by=:user
            WHERE message_id=:mid
        """), {"mid": message_id, "user": current_user.username})
        _log_status_change(row["id"], message_id, prev, "CM", current_user.username, "live_feed")
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── API — revert order one step backward (role-restricted) ───────────────────
@live_feed_bp.route("/viewer/live/revert", methods=["POST"])
@login_required
def revert_order():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    if current_user.role not in _REVERT_ROLES:
        return jsonify({"error": "Insufficient permissions to revert"}), 403
    data       = request.get_json(force=True)
    message_id = data.get("message_id")
    if not message_id:
        return jsonify({"error": "message_id required"}), 400
    try:
        row = db.session.execute(text("""
            SELECT id, COALESCE(NULLIF(order_status,''),'SC') AS order_status
            FROM hl7_orders WHERE message_id = :mid
        """), {"mid": message_id}).mappings().fetchone()
        if not row:
            return jsonify({"error": "Order not found"}), 404
        cur = row["order_status"]
        if cur not in _PREV_STATUS:
            return jsonify({"error": f"Cannot revert from status {cur}"}), 400
        prev    = _PREV_STATUS[cur]
        nullify = _REVERT_CLEAR[cur]
        db.session.execute(text(f"""
            UPDATE hl7_orders SET order_status=:prev, {nullify}
            WHERE message_id=:mid
        """), {"prev": prev, "mid": message_id})
        _log_status_change(row["id"], message_id, cur, prev, current_user.username, "live_feed")
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── API — link active HL7 order to an existing study without dismissing it ──────────
@live_feed_bp.route("/viewer/live/link", methods=["POST"])
@login_required
def link_order():
    if not user_has_page(current_user, 'live_feed'):
        abort(403)
    data                = request.get_json(force=True)
    message_id          = data.get("message_id")
    linked_accession    = (data.get("linked_accession_number") or "").strip()
    linked_study_db_uid = data.get("linked_study_db_uid")
    if not message_id or not linked_accession:
        return jsonify({"error": "message_id and linked_accession_number are required"}), 400
    try:
        db.session.execute(text("""
            ALTER TABLE hl7_orders
                ADD COLUMN IF NOT EXISTS linked_accession_number VARCHAR(100),
                ADD COLUMN IF NOT EXISTS linked_study_db_uid BIGINT,
                ADD COLUMN IF NOT EXISTS linked_by VARCHAR(100),
                ADD COLUMN IF NOT EXISTS linked_at TIMESTAMP
        """))
        db.session.execute(text("""
            UPDATE hl7_orders
            SET linked_accession_number = NULLIF(:linked_accession_number, ''),
                linked_study_db_uid = NULLIF(:linked_study_db_uid, '')::BIGINT,
                linked_by = :user,
                linked_at = NOW()
            WHERE message_id = :mid
        """), {
            "mid": message_id,
            "linked_accession_number": linked_accession,
            "linked_study_db_uid": str(linked_study_db_uid) if linked_study_db_uid else '',
            "user": current_user.username,
        })
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


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
def _make_tile(dev, status, active_orders, next_order):
    """dev: a row from aetitle_modality_map (aetitle, modality, label, floor_x, floor_y).
    One tile per DEVICE now (was: one tile per modality, pooling every AE of that
    type together) — "modality" is kept as the type for the existing filter
    dropdown/color-coding; "label"/"aetitle" identify the specific device."""
    return {
        "aetitle":       dev["aetitle"],
        "label":         dev.get("label") or dev["aetitle"],
        "modality":      dev.get("modality"),
        "floor_x":       float(dev["floor_x"]) if dev.get("floor_x") is not None else None,
        "floor_y":       float(dev["floor_y"]) if dev.get("floor_y") is not None else None,
        "status":        status,
        "active_orders": active_orders,
        "next_order":    next_order,
    }


