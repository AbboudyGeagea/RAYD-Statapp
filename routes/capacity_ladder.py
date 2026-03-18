"""
routes/capacity_ladder.py
--------------------------
Capacity Ladder — shows daily modality utilization based on
device_weekly_schedule, device_exceptions, procedure_duration_map,
and etl_orders.

Register in registry.py:
    from routes.capacity_ladder import capacity_ladder_bp
    app.register_blueprint(capacity_ladder_bp)
"""

import logging
from datetime import datetime, date
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required
from sqlalchemy import text
from db import db

logger = logging.getLogger("CAPACITY_LADDER")
capacity_ladder_bp = Blueprint("capacity_ladder", __name__)


# ─────────────────────────────────────────────
#  PAGE
# ─────────────────────────────────────────────

@capacity_ladder_bp.route("/viewer/capacity-ladder")
@login_required
def capacity_ladder_page():
    return render_template("capacity_ladder.html")


# ─────────────────────────────────────────────
#  OVERVIEW — all AEs for a given date
# ─────────────────────────────────────────────

@capacity_ladder_bp.route("/viewer/capacity-ladder/overview")
@login_required
def overview():
    """Return utilization summary for all AEs on a given date."""
    day_str = request.args.get("date")
    if not day_str:
        return jsonify({"error": "date required"}), 400

    try:
        day        = datetime.strptime(day_str, "%Y-%m-%d").date()
        dow        = day.weekday()   # 0=Mon … 6=Sun
        result     = []

        # Get all AEs
        aes = db.session.execute(text("""
            SELECT a.aetitle, a.modality, a.daily_capacity_minutes
            FROM aetitle_modality_map a
            ORDER BY a.modality, a.aetitle
        """)).mappings().fetchall()

        for ae in aes:
            aetitle  = ae["aetitle"]
            modality = ae["modality"]

            # Get opening minutes for this day (exception overrides schedule)
            opening = _get_opening_minutes(aetitle, day, dow)
            if opening == 0:
                result.append({
                    "aetitle":       aetitle,
                    "modality":      modality,
                    "opening_min":   0,
                    "scheduled_min": 0,
                    "utilization":   0,
                    "status":        "closed",
                    "scheduled_count": 0,
                })
                continue

            # Get scheduled orders for this AE on this day
            scheduled = _get_scheduled(aetitle, day)
            scheduled_min = sum(p["duration"] for p in scheduled)
            utilization   = round(scheduled_min / opening * 100, 1) if opening else 0

            status = "ok"
            if utilization >= 100: status = "overbooked"
            elif utilization >= 85: status = "tight"
            elif utilization == 0:  status = "empty"

            result.append({
                "aetitle":         aetitle,
                "modality":        modality,
                "opening_min":     opening,
                "scheduled_min":   scheduled_min,
                "utilization":     utilization,
                "status":          status,
                "scheduled_count": len(scheduled),
            })

        return jsonify(result)

    except Exception as e:
        logger.error(f"Overview error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  DETAIL — full ladder for one AE
# ─────────────────────────────────────────────

@capacity_ladder_bp.route("/viewer/capacity-ladder/detail")
@login_required
def detail():
    """Return full timeline ladder for one AE on a given date."""
    day_str = request.args.get("date")
    aetitle = request.args.get("aetitle")
    if not day_str or not aetitle:
        return jsonify({"error": "date and aetitle required"}), 400

    try:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
        dow = day.weekday()

        # AE info
        ae_row = db.session.execute(text("""
            SELECT aetitle, modality, daily_capacity_minutes
            FROM aetitle_modality_map WHERE aetitle = :ae
        """), {"ae": aetitle}).mappings().fetchone()

        if not ae_row:
            return jsonify({"error": "AE not found"}), 404

        opening_min = _get_opening_minutes(aetitle, day, dow)

        # Get scheduled procedures
        scheduled = _get_scheduled(aetitle, day)
        scheduled_min = sum(p["duration"] for p in scheduled)

        # Build timeline slots (opening hours assumed to start at 08:00)
        start_hour   = 8
        start_minute = start_hour * 60
        end_minute   = start_minute + opening_min

        # Place scheduled blocks on timeline
        blocks = []
        cursor = start_minute
        for proc in scheduled:
            blocks.append({
                "type":       "scheduled",
                "start_min":  cursor,
                "end_min":    cursor + proc["duration"],
                "duration":   proc["duration"],
                "label":      proc["label"],
                "proc_code":  proc["proc_code"],
                "patient":    proc["patient"],
                "color":      "green",
            })
            cursor += proc["duration"]

        # Find gaps and suggest procedures that fit
        gaps      = _find_gaps(blocks, start_minute, end_minute)
        all_procs = _get_all_procedures()
        suggestions = []

        for gap in gaps:
            gap_dur = gap["end_min"] - gap["start_min"]
            if gap_dur < 10:
                continue
            fits = [p for p in all_procs if p["duration"] <= gap_dur]
            fits_sorted = sorted(fits, key=lambda x: x["duration"], reverse=True)[:3]
            suggestions.append({
                "start_min": gap["start_min"],
                "end_min":   gap["end_min"],
                "duration":  gap_dur,
                "fits":      fits_sorted,
            })

        utilization = round(scheduled_min / opening_min * 100, 1) if opening_min else 0

        return jsonify({
            "aetitle":       aetitle,
            "modality":      ae_row["modality"],
            "date":          day_str,
            "opening_min":   opening_min,
            "start_min":     start_minute,
            "end_min":       end_minute,
            "scheduled_min": scheduled_min,
            "utilization":   utilization,
            "blocks":        blocks,
            "gaps":          suggestions,
        })

    except Exception as e:
        logger.error(f"Detail error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _get_opening_minutes(aetitle, day, dow):
    """Get effective opening minutes for an AE on a specific date."""
    # Check exceptions first
    exc = db.session.execute(text("""
        SELECT actual_opening_minutes
        FROM device_exceptions
        WHERE aetitle = :ae AND exception_date = :day
    """), {"ae": aetitle, "day": day}).fetchone()

    if exc is not None:
        return exc[0]

    # Fall back to weekly schedule
    sched = db.session.execute(text("""
        SELECT std_opening_minutes
        FROM device_weekly_schedule
        WHERE aetitle = :ae AND day_of_week = :dow
    """), {"ae": aetitle, "dow": dow}).fetchone()

    return sched[0] if sched else 0


def _get_scheduled(aetitle, day):
    """Get scheduled orders for an AE on a date with durations."""
    rows = db.session.execute(text("""
        SELECT
            o.proc_id,
            o.proc_text,
            o.patient_dbid,
            COALESCE(pm.duration_minutes, 15) AS duration,
            o.scheduled_datetime
        FROM etl_orders o
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = o.proc_id
        LEFT JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
        LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
        WHERE s.storing_ae = :ae
          AND o.scheduled_datetime::date = :day
        ORDER BY o.scheduled_datetime
    """), {"ae": aetitle, "day": day}).mappings().fetchall()

    # Fallback: query by modality if nothing found by AE
    if not rows:
        ae_mod = db.session.execute(text(
            "SELECT modality FROM aetitle_modality_map WHERE aetitle = :ae"
        ), {"ae": aetitle}).fetchone()

        if ae_mod:
            rows = db.session.execute(text("""
                SELECT
                    o.proc_id,
                    o.proc_text,
                    o.patient_dbid,
                    COALESCE(pm.duration_minutes, 15) AS duration,
                    o.scheduled_datetime
                FROM etl_orders o
                LEFT JOIN procedure_duration_map pm ON pm.procedure_code = o.proc_id
                WHERE o.modality = :mod
                  AND o.scheduled_datetime::date = :day
                ORDER BY o.scheduled_datetime
            """), {"mod": ae_mod[0], "day": day}).mappings().fetchall()

    return [{
        "proc_code": r["proc_id"] or "—",
        "label":     r["proc_text"] or r["proc_id"] or "Unknown Procedure",
        "patient":   str(r["patient_dbid"] or ""),
        "duration":  int(r["duration"] or 15),
    } for r in rows]


def _get_all_procedures():
    """Get all procedures with durations for gap suggestions."""
    rows = db.session.execute(text("""
        SELECT procedure_code, duration_minutes
        FROM procedure_duration_map
        WHERE duration_minutes > 0
        ORDER BY duration_minutes
    """)).mappings().fetchall()
    return [{"code": r["procedure_code"], "duration": r["duration_minutes"]} for r in rows]


def _find_gaps(blocks, start_min, end_min):
    """Find gaps in the scheduled blocks within opening hours."""
    gaps   = []
    cursor = start_min
    for b in sorted(blocks, key=lambda x: x["start_min"]):
        if b["start_min"] > cursor:
            gaps.append({"start_min": cursor, "end_min": b["start_min"]})
        cursor = max(cursor, b["end_min"])
    if cursor < end_min:
        gaps.append({"start_min": cursor, "end_min": end_min})
    return gaps


# ─────────────────────────────────────────────
#  SUGGESTIONS — 3 AI strategies
# ─────────────────────────────────────────────

@capacity_ladder_bp.route("/viewer/capacity-ladder/suggestions")
@login_required
def suggestions():
    """
    Return 3 scheduling strategies for the gaps on a given AE + date.
    Strategy 1 — Variety:  one unique historically-common procedure per gap
    Strategy 2 — Volume:   pack in as many short procedures as possible
    Strategy 3 — Priority: highest-frequency procedures repeated to fill each gap
    """
    day_str = request.args.get("date")
    aetitle = request.args.get("aetitle")
    if not day_str or not aetitle:
        return jsonify({"error": "date and aetitle required"}), 400

    try:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
        dow = day.weekday()

        # Get modality
        ae_row = db.session.execute(text(
            "SELECT modality FROM aetitle_modality_map WHERE aetitle = :ae"
        ), {"ae": aetitle}).mappings().fetchone()
        modality = ae_row["modality"] if ae_row else None

        # Opening & schedule
        opening_min = _get_opening_minutes(aetitle, day, dow)
        if not opening_min:
            return jsonify({"strategies": []})

        scheduled    = _get_scheduled(aetitle, day)
        start_minute = 8 * 60
        end_minute   = start_minute + opening_min

        # Build placed blocks to find gaps
        blocks = []
        cursor = start_minute
        for proc in scheduled:
            blocks.append({
                "start_min": cursor,
                "end_min":   cursor + proc["duration"],
                "duration":  proc["duration"],
            })
            cursor += proc["duration"]

        raw_gaps = _find_gaps(blocks, start_minute, end_minute)
        gaps     = [g for g in raw_gaps if (g["end_min"] - g["start_min"]) >= 10]

        if not gaps:
            return jsonify({"strategies": [], "message": "No gaps available"})

        # Get historically common procedures for this modality
        mod_filter = "AND m.modality = :mod" if modality else ""
        mod_params = {"mod": modality} if modality else {}

        hist_rows = db.session.execute(text(f"""
            SELECT
                s.procedure_code                       AS code,
                COALESCE(pm.duration_minutes, 15)      AS duration,
                COALESCE(pm.rvu_value, 1.0)            AS rvu,
                COUNT(*)                               AS freq
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m  ON m.aetitle = s.storing_ae
            LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
            WHERE s.procedure_code IS NOT NULL
              AND COALESCE(pm.duration_minutes, 15) > 0
              {mod_filter}
            GROUP BY s.procedure_code, pm.duration_minutes, pm.rvu_value
            ORDER BY freq DESC
            LIMIT 60
        """), mod_params).mappings().fetchall()

        procs = [dict(r) for r in hist_rows]

        # Fallback if no history
        if not procs:
            fallback = db.session.execute(text("""
                SELECT procedure_code AS code,
                       duration_minutes AS duration,
                       COALESCE(rvu_value, 1.0) AS rvu,
                       0 AS freq
                FROM procedure_duration_map
                WHERE duration_minutes > 0
                ORDER BY duration_minutes LIMIT 60
            """)).mappings().fetchall()
            procs = [dict(r) for r in fallback]

        # ── Strategy builder ──────────────────────────────────────────
        def build(name, description, placer_fn):
            placements = placer_fn(gaps, procs)
            total_gap  = sum(g["end_min"] - g["start_min"] for g in gaps)
            total_fill = sum(p["duration"] for p in placements)
            return {
                "name":             name,
                "description":      description,
                "placements":       placements,
                "total_filled_min": total_fill,
                "fill_pct":         round(total_fill / total_gap * 100, 1) if total_gap else 0,
            }

        # Strategy 1 — Variety: one unique procedure per gap, most common first
        def variety(gaps, procs):
            placed    = []
            used_codes = set()
            for gap in sorted(gaps, key=lambda g: g["start_min"]):
                gap_dur = gap["end_min"] - gap["start_min"]
                for p in procs:
                    if p["code"] not in used_codes and p["duration"] <= gap_dur:
                        placed.append({
                            "start_min": gap["start_min"],
                            "duration":  p["duration"],
                            "code":      p["code"],
                            "freq":      int(p["freq"]),
                            "rvu":       float(p["rvu"]),
                        })
                        used_codes.add(p["code"])
                        break
            return placed

        # Strategy 2 — Volume: fill each gap with as many short procedures as possible
        def volume(gaps, procs):
            placed = []
            # Sort procs by duration ascending (shortest first)
            short_procs = sorted(procs, key=lambda p: p["duration"])
            for gap in sorted(gaps, key=lambda g: g["start_min"]):
                cursor   = gap["start_min"]
                gap_end  = gap["end_min"]
                slot_placed = []
                while cursor < gap_end:
                    remaining = gap_end - cursor
                    best = next((p for p in short_procs if p["duration"] <= remaining), None)
                    if not best:
                        break
                    slot_placed.append({
                        "start_min": cursor,
                        "duration":  best["duration"],
                        "code":      best["code"],
                        "freq":      int(best["freq"]),
                        "rvu":       float(best["rvu"]),
                    })
                    cursor += best["duration"]
                placed.extend(slot_placed)
            return placed

        # Strategy 3 — Priority: highest frequency procedure repeated across gaps
        def priority(gaps, procs):
            placed = []
            # Sort by freq descending — most used first
            top_procs = sorted(procs, key=lambda p: -int(p["freq"]))
            for gap in sorted(gaps, key=lambda g: g["start_min"]):
                cursor  = gap["start_min"]
                gap_end = gap["end_min"]
                while cursor < gap_end:
                    remaining = gap_end - cursor
                    best = next((p for p in top_procs if p["duration"] <= remaining), None)
                    if not best:
                        break
                    placed.append({
                        "start_min": cursor,
                        "duration":  best["duration"],
                        "code":      best["code"],
                        "freq":      int(best["freq"]),
                        "rvu":       float(best["rvu"]),
                    })
                    cursor += best["duration"]
            return placed

        strategies = [
            build("Variety",  "One unique procedure per gap — diverse mix of the most common procedures", variety),
            build("Volume",   "Pack as many procedures as possible — maximum throughput per gap",          volume),
            build("Priority", "Most historically common procedures repeated — best utilization by demand", priority),
        ]

        return jsonify({
            "strategies": strategies,
            "modality":   modality,
            "gaps":       [{"start_min": g["start_min"], "end_min": g["end_min"],
                            "duration": g["end_min"] - g["start_min"]} for g in gaps],
        })

    except Exception as e:
        logger.error(f"Suggestions error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
