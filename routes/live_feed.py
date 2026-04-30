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

        # Ensure link metadata columns exist before we query them
        try:
            db.session.execute(text("""
                ALTER TABLE hl7_orders
                    ADD COLUMN IF NOT EXISTS linked_accession_number VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS linked_study_db_uid BIGINT,
                    ADD COLUMN IF NOT EXISTS linked_by VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS linked_at TIMESTAMP
            """))
            db.session.commit()
        except Exception:
            db.session.rollback()

        # Today's orders — use scheduled_datetime when available, fall back to received_at.
        orders = db.session.execute(text("""
            SELECT
                o.message_id,
                o.patient_id,
                o.patient_name,
                o.date_of_birth,
                o.ordering_physician,
                o.accession_number,
                COALESCE(o.scheduled_datetime, o.received_at) AS scheduled_datetime,
                o.procedure_text,
                o.procedure_code,
                o.modality,
                COALESCE(pm.duration_minutes, 15) AS duration,
                (pm.procedure_code IS NULL)        AS unknown_code,
                o.linked_accession_number,
                o.linked_study_db_uid
            FROM hl7_orders o
            LEFT JOIN procedure_duration_map pm
                   ON pm.procedure_code = o.procedure_code
            WHERE (
                (o.scheduled_datetime >= CURRENT_DATE AND o.scheduled_datetime < CURRENT_DATE + INTERVAL '1 day')
                OR
                (o.scheduled_datetime IS NULL AND o.received_at >= CURRENT_DATE AND o.received_at < CURRENT_DATE + INTERVAL '1 day')
            )
              AND COALESCE(o.order_status, '') NOT IN ('CA', 'CM')
            ORDER BY COALESCE(o.scheduled_datetime, o.received_at)
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
                    dob = o["date_of_birth"]
                    active_orders.append({
                        "message_id":          o["message_id"],
                        "patient_id":          o["patient_id"] or "—",
                        "patient_name":        o["patient_name"] or "—",
                        "date_of_birth":       dob.strftime("%d-%m-%Y") if dob else "—",
                        "referring_physician": o["ordering_physician"] or "—",
                        "accession_number":    o["accession_number"] or "—",
                        "procedure_text":      o["procedure_text"] or o["procedure_code"] or "—",
                        "procedure_code":      o["procedure_code"] or "",
                        "unknown_code":        bool(o["unknown_code"]),
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


# ── API — mark exam as done ───────────────────────────────────────────────────
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
        # Ensure columns exist (safe to run every time)
        db.session.execute(text("""
            ALTER TABLE hl7_orders
                ADD COLUMN IF NOT EXISTS done_at  TIMESTAMP,
                ADD COLUMN IF NOT EXISTS done_by  VARCHAR(100),
                ADD COLUMN IF NOT EXISTS linked_accession_number VARCHAR(100),
                ADD COLUMN IF NOT EXISTS linked_study_db_uid BIGINT,
                ADD COLUMN IF NOT EXISTS linked_by VARCHAR(100),
                ADD COLUMN IF NOT EXISTS linked_at TIMESTAMP
        """))
        db.session.execute(text("""
            UPDATE hl7_orders
            SET order_status = 'CM',
                done_at  = NOW(),
                done_by  = :user
            WHERE message_id = :mid
        """), {"mid": message_id, "user": current_user.username})
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
def _make_tile(modality, status, active_orders, next_order):
    return {
        "modality":      modality,
        "status":        status,
        "active_orders": active_orders,
        "next_order":    next_order,
    }


