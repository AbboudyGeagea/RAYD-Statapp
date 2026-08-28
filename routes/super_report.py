"""
routes/super_report.py
----------------------
Super Report — dynamic filters from ETL tables, comparison period,
and rule-based plain-language narrative. No LLM API required.

Register in registry.py:
    from routes.super_report import super_report_bp
    app.register_blueprint(super_report_bp)
"""

import logging
import json
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db
from routes.insights_engine import run_dept_insights
from routes.report_cache import cache_get, cache_put
from utils.stats import _pct, _fmt
from utils.site_resolver import default_site
from utils.radiologist_resolve import rad_alias_join_sql, rad_display_sql, rad_ok_sql

logger = logging.getLogger("SUPER_REPORT")
super_report_bp = Blueprint("super_report", __name__)


# ─────────────────────────────────────────────
#  PAGE ROUTE
# ─────────────────────────────────────────────

@super_report_bp.route("/viewer/super-report-page")
@login_required
def super_report_page():
    return render_template("super_report.html")


# ─────────────────────────────────────────────
#  SAVED REPORTS ENDPOINTS
# ─────────────────────────────────────────────

@super_report_bp.route("/viewer/super-report/save", methods=["POST"])
@login_required
def save_super_report():
    """Save current filter preset under a user-defined name."""
    body = request.get_json() or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"success": False, "error": "Name is required"}), 400

    filter_json = body.get("filters", {})

    try:
        # Check for duplicate name for this user
        # Super Report saves have base_report_id = NULL (no backing report_template row)
        existing = db.session.execute(text("""
            SELECT id FROM saved_reports
            WHERE owner_user_id = :uid
              AND base_report_id IS NULL
              AND name = :name
        """), {"uid": current_user.id, "name": name}).fetchone()

        if existing:
            # Update existing
            db.session.execute(text("""
                UPDATE saved_reports
                SET filter_json = :fj, updated_at = NOW()
                WHERE id = :id
            """), {"fj": json.dumps(filter_json), "id": existing[0]})
            saved_id = existing[0]
        else:
            # Insert new — base_report_id is NULL for Super Report presets
            result = db.session.execute(text("""
                INSERT INTO saved_reports (name, owner_user_id, base_report_id, is_public, filter_json)
                VALUES (:name, :uid, NULL, FALSE, :fj)
                RETURNING id
            """), {
                "name": name,
                "uid":  current_user.id,
                "fj":   json.dumps(filter_json)
            })
            saved_id = result.fetchone()[0]

        db.session.commit()
        return jsonify({"success": True, "id": saved_id, "name": name})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Save report error: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/saved")
@login_required
def list_saved_reports():
    """List all saved Super Report presets for the current user."""
    try:
        rows = db.session.execute(text("""
            SELECT id, name, filter_json, created_at, updated_at
            FROM saved_reports
            WHERE owner_user_id = :uid
              AND base_report_id IS NULL
            ORDER BY updated_at DESC
        """), {"uid": current_user.id}).mappings().fetchall()

        return jsonify([{
            "id":         r["id"],
            "name":       r["name"],
            "filters":    r["filter_json"] if isinstance(r["filter_json"], dict) else json.loads(r["filter_json"] or "{}"),
            "updated_at": r["updated_at"].strftime("%Y-%m-%d %H:%M") if r["updated_at"] else None,
        } for r in rows])

    except Exception as e:
        logger.error(f"List saved reports error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/saved/<int:report_id>", methods=["DELETE"])
@login_required
def delete_saved_report(report_id):
    """Delete a saved preset — only if owned by current user."""
    try:
        result = db.session.execute(text("""
            DELETE FROM saved_reports
            WHERE id = :id
              AND owner_user_id = :uid
              AND base_report_id IS NULL
        """), {"id": report_id, "uid": current_user.id})
        db.session.commit()

        if result.rowcount == 0:
            return jsonify({"success": False, "error": "Not found or not yours"}), 404
        return jsonify({"success": True})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Delete saved report error: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500



@super_report_bp.route("/viewer/super-report/snapshots")
@login_required
def super_report_snapshots():
    """Return the latest pre-computed snapshot for each of the 3 standard periods."""
    try:
        rows = db.session.execute(text("""
            SELECT DISTINCT ON (period_label)
                period_label, period_start::text, period_end::text,
                computed_at, narrative, status
            FROM analytics_snapshots
            ORDER BY period_label, computed_at DESC
        """)).mappings().fetchall()

        return jsonify([{
            "period_label": r["period_label"],
            "period_start": r["period_start"],
            "period_end":   r["period_end"],
            "computed_at":  r["computed_at"].strftime("%d %b %Y %H:%M") if r["computed_at"] else None,
            "narrative":    r["narrative"] or "",
            "status":       r["status"],
        } for r in rows])

    except Exception as e:
        logger.error(f"Snapshots error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/filters")
@login_required
def super_report_filters():
    # 19 SELECT DISTINCT queries below are expensive on the full ETL tables and
    # never change within a TTL window -- cached like everything else in
    # routes/report_cache.py (fixed key, no request params to vary on).
    cached = cache_get("super_report_filters", {})
    if cached is not None:
        return jsonify(cached)

    try:
        def distinct(sql):
            rows = db.session.execute(text(sql)).fetchall()
            return sorted([r[0] for r in rows if r[0] is not None and str(r[0]).strip() != ''])

        data = {
            "modality":            distinct("SELECT DISTINCT COALESCE(m.modality, s.study_modality) FROM etl_didb_studies s LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle)) WHERE COALESCE(m.modality, s.study_modality) IS NOT NULL"),
            "storing_ae":          distinct("SELECT DISTINCT storing_ae FROM etl_didb_studies WHERE storing_ae IS NOT NULL"),
            "study_status":        distinct("SELECT DISTINCT study_status FROM etl_didb_studies WHERE study_status IS NOT NULL"),
            "report_status":       distinct("SELECT DISTINCT report_status FROM etl_didb_studies WHERE report_status IS NOT NULL"),
            "order_status":        distinct("SELECT DISTINCT order_status FROM etl_didb_studies WHERE order_status IS NOT NULL"),
            "patient_class":       distinct("SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL"),
            "patient_location":    distinct("SELECT DISTINCT patient_location FROM etl_didb_studies WHERE patient_location IS NOT NULL"),
            "body_part":           distinct("SELECT DISTINCT study_body_part FROM etl_didb_studies WHERE study_body_part IS NOT NULL"),
            "procedure_code":      distinct("SELECT DISTINCT procedure_code FROM etl_didb_studies WHERE procedure_code IS NOT NULL LIMIT 200"),
            "signing_physician":   distinct("SELECT DISTINCT TRIM(CONCAT(signing_physician_first_name,' ',signing_physician_last_name)) FROM etl_didb_studies WHERE signing_physician_last_name IS NOT NULL AND signing_physician_last_name != ''"),
            "referring_physician": distinct("SELECT DISTINCT TRIM(CONCAT(referring_physician_first_name,' ',referring_physician_last_name)) FROM etl_didb_studies WHERE referring_physician_last_name IS NOT NULL AND referring_physician_last_name != ''"),
            "sex":                 distinct("SELECT DISTINCT sex FROM etl_patient_view WHERE sex IS NOT NULL"),
            "age_group":           distinct("SELECT DISTINCT age_group FROM etl_patient_view WHERE age_group IS NOT NULL"),
            "order_control":       distinct("SELECT DISTINCT order_control FROM etl_orders WHERE order_control IS NOT NULL"),
            "order_modality":      distinct("SELECT DISTINCT modality FROM etl_orders WHERE modality IS NOT NULL"),
            "series_modality":     distinct("SELECT DISTINCT modality FROM etl_didb_serieses WHERE modality IS NOT NULL"),
            "body_part_series":    distinct("SELECT DISTINCT body_part_examined FROM etl_didb_serieses WHERE body_part_examined IS NOT NULL"),
            "protocol_name":       distinct("SELECT DISTINCT protocol_name FROM etl_didb_serieses WHERE protocol_name IS NOT NULL LIMIT 100"),
            "ae_modality_map":     dict(db.session.execute(text(
                "SELECT aetitle, modality FROM aetitle_modality_map WHERE aetitle IS NOT NULL AND modality IS NOT NULL"
            )).fetchall()),
            "ae_station_map":      dict(db.session.execute(text(
                "SELECT aetitle, station_name FROM aetitle_modality_map WHERE aetitle IS NOT NULL AND station_name IS NOT NULL AND TRIM(station_name) != ''"
            )).fetchall()),
            "procedure_name_map":  dict(db.session.execute(text(
                "SELECT procedure_code, procedure_name FROM procedure_duration_map WHERE procedure_code IS NOT NULL AND procedure_name IS NOT NULL AND TRIM(procedure_name) != ''"
            )).fetchall()),
        }
        cache_put("super_report_filters", {}, data)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Filters error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  MAIN ROUTE
# ─────────────────────────────────────────────

@super_report_bp.route("/viewer/super-report")
@login_required
def super_report():
    start     = request.args.get("start")
    end       = request.args.get("end")
    cmp_start = request.args.get("cmp_start")
    cmp_end   = request.args.get("cmp_end")
    if not start or not end:
        return jsonify({"error": "start and end date required"}), 400

    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='super_report',
              detail={'from': start, 'to': end, 'cmp_from': cmp_start, 'cmp_to': cmp_end})

    try:
        d_start = datetime.strptime(start, "%Y-%m-%d")
        d_end   = datetime.strptime(end,   "%Y-%m-%d")
        delta   = (d_end - d_start).days + 1
        if not cmp_start or not cmp_end:
            cmp_end_dt   = d_start - timedelta(days=1)
            cmp_start_dt = cmp_end_dt - timedelta(days=delta - 1)
            cmp_start    = cmp_start_dt.strftime("%Y-%m-%d")
            cmp_end      = cmp_end_dt.strftime("%Y-%m-%d")
    except Exception:
        delta = 30

    filters = {k: request.args.getlist(k) for k in [
        "modality","storing_ae","study_status","report_status","order_status",
        "patient_class","patient_location","body_part","procedure_code",
        "signing_physician","referring_physician","sex","age_group",
        "order_control","order_modality","series_modality","body_part_series","protocol_name"
    ]}
    filters["has_report"] = request.args.get("has_report")
    filters["age_min"]    = request.args.get("age_min")
    filters["age_max"]    = request.args.get("age_max")

    # Cache key built post-resolution so an implicit vs. explicit cmp_start/end
    # (auto-computed above when omitted) still lands on the same key, and
    # multi-select values are pre-sorted so UI selection order doesn't cause a
    # spurious cache miss (report_cache's generic key-builder sorts only the
    # outer list, not values nested a level deeper).
    cache_key = {
        "start": start, "end": end, "cmp_start": cmp_start, "cmp_end": cmp_end,
        **{k: (sorted(v) if isinstance(v, list) else v) for k, v in filters.items()},
    }
    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH excluded, for now. Resolved once per request and threaded
    # through both periods rather than re-resolved inside _collect_data (which
    # runs twice per request, current + comparison period).
    rh_site_id = default_site()
    cache_key["rh_site_id"] = rh_site_id

    cached = cache_get("super_report", cache_key)
    if cached is not None:
        return jsonify(cached)

    try:
        crn_enabled_row = db.session.execute(
            text("SELECT value FROM settings WHERE key = 'crn_enabled'")
        ).fetchone()
        crn_enabled = bool(crn_enabled_row and crn_enabled_row[0] == 'true')

        current   = _collect_data(start, end, filters, rh_site_id)
        previous  = _collect_data(cmp_start, cmp_end, filters, rh_site_id)
        narrative = _generate_narrative(current, previous, start, end, cmp_start, cmp_end, delta)
        result = {
            "current":     current,
            "previous":    previous,
            "narrative":   narrative,
            "cmp_start":   cmp_start,
            "cmp_end":     cmp_end,
            "delta_days":  delta,
            "crn_enabled": crn_enabled,
        }
        cache_put("super_report", cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Super report error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  DATA COLLECTION
# ─────────────────────────────────────────────

def _build_where(start, end, filters, rh_site_id=None):
    clauses = [
        "s.study_date BETWEEN :start AND :end",
        "COALESCE(m.modality, s.study_modality, 'Unknown') != 'SR'",
    ]
    params  = {"start": start, "end": end}

    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH excluded, for now. Same default_site()/aetitle_modality_map
    # pattern used throughout (report_25, report_34, viewer_controller, etc.) --
    # `m` (aetitle_modality_map) is already joined into every query built on this
    # WHERE clause. Resolves to None (filter skipped) on a non-LAUMC/single-site
    # install. Note: the orders (etl_orders) and storage (summary_storage_daily)
    # queries in _collect_data below have no site-resolvable join and remain
    # unscoped -- same known, documented gap as etl_orders elsewhere (see
    # viewer_controller.py's daily_briefing()), not fixed here.
    if rh_site_id is not None:
        clauses.append("m.site_id = :rh_site_id")
        params["rh_site_id"] = rh_site_id

    multi = {
        "storing_ae":          ("s.storing_ae",         "storing_ae"),
        "study_status":        ("s.study_status",        "study_status"),
        "report_status":       ("s.report_status",       "report_status"),
        "order_status":        ("s.order_status",        "order_status"),
        "patient_class":       ("s.patient_class",       "patient_class"),
        "patient_location":    ("s.patient_location",    "patient_location"),
        "body_part":           ("s.study_body_part",     "body_part"),
        "procedure_code":      ("s.procedure_code",      "procedure_code"),
        "sex":                 ("p.sex",                 "sex"),
        "age_group":           ("p.age_group",           "age_group"),
    }
    for fk, (col, pk) in multi.items():
        if filters.get(fk):
            clauses.append(f"{col} = ANY(:{pk})")
            params[pk] = filters[fk]

    if filters.get("modality"):
        clauses.append("COALESCE(m.modality, s.study_modality) = ANY(:modality)")
        params["modality"] = filters["modality"]

    if filters.get("signing_physician"):
        clauses.append("TRIM(CONCAT(s.signing_physician_first_name,' ',s.signing_physician_last_name)) = ANY(:signing_physician)")
        params["signing_physician"] = filters["signing_physician"]

    if filters.get("referring_physician"):
        clauses.append("TRIM(CONCAT(s.referring_physician_first_name,' ',s.referring_physician_last_name)) = ANY(:referring_physician)")
        params["referring_physician"] = filters["referring_physician"]

    if filters.get("has_report") == "Yes":
        clauses.append("s.study_has_report = TRUE")
    elif filters.get("has_report") == "No":
        clauses.append("s.study_has_report = FALSE")

    if filters.get("age_min"):
        clauses.append("s.age_at_exam >= :age_min")
        params["age_min"] = float(filters["age_min"])
    if filters.get("age_max"):
        clauses.append("s.age_at_exam <= :age_max")
        params["age_max"] = float(filters["age_max"])

    return " AND ".join(clauses), params


def _collect_data(start, end, filters, rh_site_id=None):
    where, params = _build_where(start, end, filters, rh_site_id)
    mj = "LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))"
    pj = "LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid"

    kpis = db.session.execute(text(f"""
        SELECT COUNT(DISTINCT s.study_db_uid)     AS total_studies,
               COUNT(DISTINCT s.patient_db_uid)   AS total_patients,
               SUM(s.number_of_study_images)      AS total_images,
               COUNT(DISTINCT s.storing_ae)       AS active_aes,
               COUNT(*) FILTER (WHERE s.study_has_report=TRUE) AS studies_with_report
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """), params).mappings().fetchone()

    # orders
    op = {"start": start, "end": end}
    oc = ["o.scheduled_datetime::date BETWEEN :start AND :end"]
    if filters.get("order_modality"):
        oc.append("o.modality = ANY(:order_modality)")
        op["order_modality"] = filters["order_modality"]
    if filters.get("order_control"):
        oc.append("o.order_control = ANY(:order_control)")
        op["order_control"] = filters["order_control"]
    orders = db.session.execute(text(f"""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE has_study=TRUE) AS fulfilled,
               ROUND(COUNT(*) FILTER (WHERE has_study=TRUE)*100.0/NULLIF(COUNT(*),0),1) AS fulfillment_pct
        FROM etl_orders o WHERE {" AND ".join(oc)}
    """), op).mappings().fetchone()

    # storage
    sp = {"start": start, "end": end}
    sc = ["study_date BETWEEN :start AND :end"]
    if filters.get("modality"):
        sc.append("modality = ANY(:modality)")
        sp["modality"] = filters["modality"]
    storage = db.session.execute(text(f"""
        SELECT COALESCE(SUM(total_gb),0) AS total_gb,
               COALESCE(SUM(study_count),0) AS total_study_count,
               ROUND(COALESCE(SUM(total_gb),0)/NULLIF(COUNT(DISTINCT study_date),0),2) AS avg_gb_per_day
        FROM summary_storage_daily WHERE {" AND ".join(sc)}
    """), sp).mappings().fetchone()

    storage_by_mod = db.session.execute(text(f"""
        SELECT COALESCE(modality,'N/A') AS modality, ROUND(SUM(total_gb)::numeric,2) AS gb
        FROM summary_storage_daily WHERE {" AND ".join(sc)}
        GROUP BY 1 ORDER BY gb DESC LIMIT 5
    """), sp).mappings().fetchall()

    top_mods = db.session.execute(text(f"""
        SELECT COALESCE(m.modality,s.study_modality,'N/A') AS modality, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 5
    """), params).mappings().fetchall()

    peak = db.session.execute(text(f"""
        SELECT s.study_date::text AS peak_day, COUNT(*) AS peak_count
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY s.study_date ORDER BY peak_count DESC LIMIT 1
    """), params).mappings().fetchone()

    avg_day = db.session.execute(text(f"""
        SELECT ROUND(COUNT(*)/NULLIF(COUNT(DISTINCT s.study_date),0)::numeric,1) AS avg_per_day
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """), params).mappings().fetchone()

    physicians = db.session.execute(text(f"""
        SELECT TRIM(CONCAT(s.referring_physician_first_name,' ',s.referring_physician_last_name)) AS physician,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
          AND s.referring_physician_last_name IS NOT NULL AND s.referring_physician_last_name != ''
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """), params).mappings().fetchall()

    # ── Patient-class category mapping (configurable via settings table) ────
    _pc_rows = db.session.execute(text(
        "SELECT key, value FROM settings WHERE key IN ('pc_inpatient', 'pc_outpatient', 'pc_emergency')"
    )).fetchall()
    _pc_cfg = {r[0]: r[1] for r in _pc_rows}

    def _pc_filter(cfg_key, default):
        vals = [v.strip() for v in _pc_cfg.get(cfg_key, default).split(',') if v.strip()]
        if not vals:
            return 'FALSE'
        safe = [v.replace("'", "''") for v in vals]
        return "s.patient_class IN (" + ','.join(f"'{v}'" for v in safe) + ")"

    inp_filter  = _pc_filter('pc_inpatient',  'I,IP,INPAT,INPATIENT,INN')
    outp_filter = _pc_filter('pc_outpatient', 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY')

    demo = db.session.execute(text(f"""
        SELECT COUNT(*) FILTER (WHERE p.sex ILIKE 'M%') AS male,
               COUNT(*) FILTER (WHERE p.sex ILIKE 'F%') AS female,
               COUNT(*) FILTER (WHERE {inp_filter}) AS inpatient,
               COUNT(*) FILTER (WHERE {outp_filter}) AS outpatient,
               ROUND(AVG(s.age_at_exam),1) AS avg_age,
               MIN(s.age_at_exam) AS min_age,
               MAX(s.age_at_exam) AS max_age
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """), params).mappings().fetchone()

    pc_breakdown = db.session.execute(text(f"""
        SELECT COALESCE(s.patient_class, 'Unknown') AS class, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC
    """), params).mappings().fetchall()

    er_filter = _pc_filter('pc_emergency', 'E,EP,ER,EMERGENCY,URG,URGENT')
    _er_vals  = [v.strip() for v in _pc_cfg.get('pc_emergency', 'E,EP,ER,EMERGENCY,URG,URGENT').split(',') if v.strip()]

    # Busiest AE title by study count in period
    ae_busy_row = db.session.execute(text(f"""
        SELECT s.storing_ae AS aetitle, m.station_name AS station_name, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where} AND s.storing_ae IS NOT NULL
        GROUP BY s.storing_ae, m.station_name ORDER BY cnt DESC LIMIT 1
    """), params).mappings().fetchone()

    # Most idle configured AE (fewest studies in period — includes 0-study AEs)
    _idle_site_clause = "WHERE am.site_id = :rh_site_id" if rh_site_id is not None else ""
    _idle_params = {"start": start, "end": end}
    if rh_site_id is not None:
        _idle_params["rh_site_id"] = rh_site_id
    ae_idle_row = db.session.execute(text(f"""
        SELECT am.aetitle, am.station_name AS station_name, COALESCE(sub.cnt, 0) AS cnt
        FROM aetitle_modality_map am
        LEFT JOIN (
            SELECT storing_ae, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date BETWEEN :start AND :end
            GROUP BY storing_ae
        ) sub ON UPPER(TRIM(sub.storing_ae)) = UPPER(TRIM(am.aetitle))
        {_idle_site_clause}
        ORDER BY cnt ASC LIMIT 1
    """), _idle_params).mappings().fetchone()

    # rep_final_timestamp (PACS) is sparse/unreliable on this install (operator,
    # 2026-07-27) -- rep_study_last_composed_ts is the PACS field confirmed
    # reliable here and already the standard TAT anchor in report_25.py.
    # Non-ER studies with TAT > P75 on days that also had ER volume (resource contention proxy)
    if _er_vals:
        _er_in = ','.join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in _er_vals)
        er_delayed_row = db.session.execute(text(f"""
            WITH er_days AS (
                SELECT DISTINCT study_date FROM etl_didb_studies
                WHERE study_date BETWEEN :start AND :end
                  AND patient_class IN ({_er_in})
            ),
            p75 AS (
                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0
                ) AS val
                FROM etl_didb_studies s {mj} {pj}
                WHERE {where}
                  AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL AND s.insert_time IS NOT NULL
            )
            SELECT COUNT(*) AS cnt
            FROM etl_didb_studies s {mj} {pj}
            JOIN er_days ed ON s.study_date = ed.study_date
            CROSS JOIN p75
            WHERE {where}
              AND NOT ({er_filter})
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL AND s.insert_time IS NOT NULL
              AND EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0 > p75.val
        """), params).mappings().fetchone()
        er_delayed = int(er_delayed_row.get("cnt") or 0) if er_delayed_row else 0
    else:
        er_delayed = 0

    # ── TAT & reporting (null-safe — columns populated by ETL) ────
    # ER vs non-ER split reuses er_filter (already resolved above from the
    # configurable pc_emergency setting) via FILTER clauses in the same scan,
    # rather than a second round trip.
    _tat_epoch = ("EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) "
                  "- s.insert_time)) / 60.0")
    tat = db.session.execute(text(f"""
        SELECT
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch})::numeric, 1) AS median_tat_min,
            COUNT(*) FILTER (WHERE COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS reported_count,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch}) FILTER (WHERE {er_filter})::numeric, 1) AS er_median_tat_min,
            COUNT(*) FILTER (WHERE {er_filter} AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS er_reported_count,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch}) FILTER (WHERE NOT ({er_filter}))::numeric, 1) AS non_er_median_tat_min,
            COUNT(*) FILTER (WHERE NOT ({er_filter}) AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS non_er_reported_count
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where} AND s.insert_time IS NOT NULL
    """), params).mappings().fetchone()

    tat_by_mod = db.session.execute(text(f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0
               )::numeric, 1) AS median_tat_min,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where}
          AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
          AND s.insert_time IS NOT NULL
        GROUP BY 1 ORDER BY median_tat_min DESC LIMIT 5
    """), params).mappings().fetchall()

    # ── Daily study volume (time series for charts) ───────────────
    daily_series = db.session.execute(text(f"""
        SELECT s.study_date::text AS date, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY s.study_date ORDER BY s.study_date
    """), params).mappings().fetchall()

    # ── Studies per modality (full breakdown) ─────────────────────
    modality_series = db.session.execute(text(f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 15
    """), params).mappings().fetchall()

    # ── Radiologist matrix: reports × modality / AE title / procedure ─
    # rep_final_signed_by/rep_final_timestamp (PACS) are sparse/unreliable on this
    # install (operator, 2026-07-27) -- rep_study_last_composed_by/_ts is the PACS
    # field confirmed reliable here and already the standard anchor in report_25.py.
    _RAD_BASE_SR = ("COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                    "s.signing_physician_last_name)),''),s.rep_study_last_composed_by,"
                    "s.rep_final_signed_by)")
    _PAM_SR = rad_alias_join_sql(_RAD_BASE_SR)
    _RAD = rad_display_sql(_RAD_BASE_SR)
    _RAD_OK = rad_ok_sql(_RAD_BASE_SR,
                          ts_expr="COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp)")

    rad_mod_rows = db.session.execute(text(f"""
        SELECT {_RAD} AS radiologist,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK}
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), params).mappings().fetchall()

    rad_ae_rows = db.session.execute(text(f"""
        SELECT {_RAD} AS radiologist,
               COALESCE(NULLIF(TRIM(m.station_name),''), s.storing_ae, 'Unknown') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK}
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), params).mappings().fetchall()

    rad_proc_rows = db.session.execute(text(f"""
        WITH top_procs AS (
            SELECT s.procedure_code
            FROM etl_didb_studies s {mj} {pj}
            WHERE {where} AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
              AND s.procedure_code IS NOT NULL AND s.procedure_code != ''
            GROUP BY 1 ORDER BY COUNT(DISTINCT s.study_db_uid) DESC LIMIT 60
        )
        SELECT {_RAD} AS radiologist,
               COALESCE(NULLIF(TRIM(pm.procedure_name),''), s.procedure_code) AS proc,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
        JOIN top_procs tp ON tp.procedure_code = s.procedure_code
        WHERE {where} AND {_RAD_OK}
        GROUP BY 1, 2 ORDER BY 2, 3 DESC
    """), params).mappings().fetchall()

    rad_month_rows = db.session.execute(text(f"""
        SELECT {_RAD} AS radiologist,
               TO_CHAR(s.study_date, 'YYYY-MM') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK}
        GROUP BY 1, 2 ORDER BY 1, 2
    """), params).mappings().fetchall()

    return {
        "kpis":        dict(kpis),
        "orders":      dict(orders),
        "storage":     {**dict(storage), "by_modality": [dict(r) for r in storage_by_mod]},
        "volume":      {
            "avg_per_day":    dict(avg_day).get("avg_per_day", 0),
            "peak_day":       dict(peak).get("peak_day") if peak else None,
            "peak_count":     dict(peak).get("peak_count") if peak else 0,
            "top_modalities": [dict(r) for r in top_mods],
        },
        "physicians":    [dict(r) for r in physicians],
        "demographics":  {**dict(demo), "pc_breakdown": [dict(r) for r in pc_breakdown]},
        "ae_ops": {
            "busiest_ae":  (ae_busy_row["station_name"] or ae_busy_row["aetitle"]) if ae_busy_row else None,
            "busiest_cnt": int(ae_busy_row["cnt"] or 0) if ae_busy_row else 0,
            "idle_ae":     (ae_idle_row["station_name"] or ae_idle_row["aetitle"]) if ae_idle_row else None,
            "idle_cnt":    int(ae_idle_row["cnt"] or 0) if ae_idle_row else 0,
            "er_delayed":  er_delayed,
        },
        "tat": {
            "median_tat_min":        float(tat.get("median_tat_min") or 0) if tat else 0,
            "reported_count":        int(tat.get("reported_count") or 0) if tat else 0,
            "er_median_tat_min":     float(tat.get("er_median_tat_min") or 0) if tat else 0,
            "er_reported_count":     int(tat.get("er_reported_count") or 0) if tat else 0,
            "non_er_median_tat_min": float(tat.get("non_er_median_tat_min") or 0) if tat else 0,
            "non_er_reported_count": int(tat.get("non_er_reported_count") or 0) if tat else 0,
            "by_modality":           [dict(r) for r in tat_by_mod],
        },
        "daily_series":    [dict(r) for r in daily_series],
        "modality_series": [dict(r) for r in modality_series],
        "rad_matrix": {
            "by_modality":  [dict(r) for r in rad_mod_rows],
            "by_aetitle":   [dict(r) for r in rad_ae_rows],
            "by_procedure": [dict(r) for r in rad_proc_rows],
            "by_month":     [dict(r) for r in rad_month_rows],
        },
        "crn":         _get_crn_summary(start, end),
        "utilization": _get_utilization_summary(start, end),
    }


def _get_crn_summary(start, end):
    """Critical Result Notification activity for the period.

    Not site-scoped (no established site-resolution join for hl7_oru_reports /
    crn_notifications yet -- CRN is single dry-run scaffold today, not part of
    the RH/SJH rollout) and not filtered by Super Report's other active
    filters -- a period-only rollup, same simplification as
    _get_utilization_summary below.
    """
    critical = db.session.execute(text("""
        SELECT COUNT(*) AS critical_count
        FROM hl7_oru_analysis a
        JOIN hl7_oru_reports r ON r.id = a.report_id
        WHERE a.is_critical = TRUE
          AND COALESCE(r.result_datetime, r.received_at)::date BETWEEN :start AND :end
    """), {"start": start, "end": end}).mappings().fetchone()

    notif = db.session.execute(text("""
        SELECT
            COUNT(*)                                      AS total_notifications,
            COUNT(*) FILTER (WHERE status = 'acknowledged') AS acknowledged,
            COUNT(*) FILTER (WHERE status = 'exhausted')    AS exhausted,
            COUNT(*) FILTER (WHERE status = 'pending')      AS pending,
            ROUND(AVG(EXTRACT(EPOCH FROM (acknowledged_at - created_at)) / 60.0)
                  FILTER (WHERE acknowledged_at IS NOT NULL)::numeric, 1) AS avg_ack_min
        FROM crn_notifications
        WHERE created_at::date BETWEEN :start AND :end
    """), {"start": start, "end": end}).mappings().fetchone()

    return {
        "critical_count":      int(critical["critical_count"] or 0) if critical else 0,
        "total_notifications": int(notif["total_notifications"] or 0) if notif else 0,
        "acknowledged":        int(notif["acknowledged"] or 0) if notif else 0,
        "exhausted":           int(notif["exhausted"] or 0) if notif else 0,
        "pending":             int(notif["pending"] or 0) if notif else 0,
        "avg_ack_min":         float(notif["avg_ack_min"]) if notif and notif["avg_ack_min"] is not None else None,
    }


def _get_utilization_summary(start, end):
    """Compact device/modality utilization rollup for the period.

    Reuses report_34's device-utilization matrix verbatim (same std_pps-actuals
    / procedure_duration_map-estimate load logic, same RH-only site filter
    already wired into report_34) rather than re-deriving it -- report_34.py's
    get_device_utilization_data() is the source of truth this ports from, kept
    in sync automatically since it's a direct call, not a copy. Scoped by date
    range only -- Super Report's other active filters (modality, patient
    class, etc.) aren't forwarded, matching report_34's own behavior when none
    of its filter toggles are enabled. Shares report_34's cache (keyed on the
    same start_date/end_date), so a Report 34 view for the same range makes
    this instant.
    """
    from routes.report_34 import get_device_utilization_data
    from werkzeug.datastructures import MultiDict

    data, _, _ = get_device_utilization_data(MultiDict({"start_date": start, "end_date": end}))
    if not data:
        return {"available": False}

    matrix = [row for row in data["matrix"] if not row["no_schedule"]]
    avg_util = round(sum(r["avg"] for r in matrix) / len(matrix), 1) if matrix else 0
    ranked = sorted(matrix, key=lambda r: r["avg"], reverse=True)

    return {
        "available":         True,
        "avg_utilization":   avg_util,
        "high_stress_count": data["summary"]["high_stress_count"],
        "low_util_count":    data["summary"]["low_util_count"],
        "no_schedule_count": data["summary"]["no_schedule_count"],
        "busiest": [{"ae": r["ae"], "avg": r["avg"]} for r in ranked[:3]],
        "idlest":  [{"ae": r["ae"], "avg": r["avg"]} for r in ranked[-3:][::-1]] if len(ranked) >= 6 else [],
    }


# ─────────────────────────────────────────────
#  RULE-BASED NARRATIVE
# ─────────────────────────────────────────────

def _fp(p):
    if p is None: return None
    return f"{'+'if p>=0 else ''}{p}%"

def _trend(p):
    # Bands derived from Intermedic operational experience: ±5% is day-to-day
    # noise; ±20% marks a shift meaningful enough to warrant attention.
    if p is None: return "unchanged"
    if p > 20:   return "significantly up"
    if p > 5:    return "up"
    if p > 0:    return "slightly up"
    if p < -20:  return "significantly down"
    if p < -5:   return "down"
    if p < 0:    return "slightly down"
    return "flat"


def _generate_narrative(cur, prev, start, end, cmp_start, cmp_end, delta):
    current_kpis      = cur["kpis"]
    prev_kpis         = prev["kpis"]
    current_orders    = cur["orders"]
    prev_orders       = prev["orders"]
    current_storage   = cur["storage"]
    prev_storage      = prev["storage"]
    current_volume    = cur["volume"]
    current_demo      = cur["demographics"]
    current_physicians = cur["physicians"]
    tat_data          = cur.get("tat", {})

    studies_chg  = _pct(current_kpis.get("total_studies"),  prev_kpis.get("total_studies"))
    patients_chg = _pct(current_kpis.get("total_patients"), prev_kpis.get("total_patients"))
    storage_chg  = _pct(current_storage.get("total_gb"),    prev_storage.get("total_gb"))
    orders_chg   = _pct(current_orders.get("total"),        prev_orders.get("total"))
    images_chg   = _pct(current_kpis.get("total_images"),   prev_kpis.get("total_images"))

    sections = []

    # ── Overview ──────────────────────────────────────────────
    overview = []
    chg_str = f" ({_fp(studies_chg)} vs prior {delta}-day period)" if studies_chg is not None else ""
    overview.append(
        f"{_fmt(current_kpis.get('total_studies'))} studies completed between {start} and {end}{chg_str}"
    )
    overview.append(
        f"{_fmt(current_kpis.get('total_patients'))} unique patients seen"
        + (f", {_fp(patients_chg)} vs prior period" if patients_chg is not None else "")
    )
    overview.append(
        f"{_fmt(current_kpis.get('total_images'))} images acquired across {_fmt(current_kpis.get('active_aes'))} active devices"
        + (f" ({_fp(images_chg)})" if images_chg is not None else "")
    )
    fulfillment_pct = float(current_orders.get("fulfillment_pct") or 0)
    overview.append(
        f"{_fmt(current_orders.get('total'))} orders received — {fulfillment_pct:.1f}% fulfillment rate "
        f"({_fmt(current_orders.get('fulfilled'))} completed)"
    )
    sections.append({"icon": "bi-bar-chart-line", "color": "#60a5fa", "title": "Overview", "bullets": overview})

    # ── Volume & Modality ─────────────────────────────────────
    volume_bullets = []
    avg_day = float(current_volume.get("avg_per_day") or 0)
    volume_bullets.append(f"Average throughput: {avg_day:.1f} studies per day")
    if current_volume.get("peak_day"):
        volume_bullets.append(f"Peak day: {current_volume['peak_day']} with {_fmt(current_volume['peak_count'])} studies")
    mods = current_volume.get("top_modalities", [])
    study_count = float(current_kpis.get("total_studies") or 1)
    for m in mods:
        pct_share = round(float(m["cnt"]) / study_count * 100, 1)
        volume_bullets.append(f"{m['modality']}: {_fmt(m['cnt'])} studies ({pct_share}% of total)")
    sections.append({"icon": "bi-graph-up", "color": "#34d399", "title": "Volume & Modality Mix", "bullets": volume_bullets})

    # ── Physicians ────────────────────────────────────────────
    if current_physicians:
        phys_bullets = []
        for i, p in enumerate(current_physicians[:5]):
            share = round(float(p["cnt"]) / study_count * 100, 1)
            prefix = ["Top referrer", "2nd", "3rd", "4th", "5th"][i]
            phys_bullets.append(f"{prefix}: {p['physician']} — {_fmt(p['cnt'])} referrals ({share}%)")
        sections.append({"icon": "bi-person-badge", "color": "#a78bfa", "title": "Top Referring Physicians", "bullets": phys_bullets})

    # ── Demographics ──────────────────────────────────────────
    demo_bullets = []
    male   = float(current_demo.get("male") or 0)
    female = float(current_demo.get("female") or 0)
    inp    = float(current_demo.get("inpatient") or 0)
    outp   = float(current_demo.get("outpatient") or 0)
    tot_gen = male + female
    if tot_gen > 0:
        demo_bullets.append(f"Gender split: {round(male/tot_gen*100)}% male / {round(female/tot_gen*100)}% female")
    tot_cls = inp + outp
    if tot_cls > 0:
        demo_bullets.append(f"Patient class: {round(inp/tot_cls*100)}% inpatient / {round(outp/tot_cls*100)}% outpatient")
    avg_age = current_demo.get("avg_age")
    if avg_age:
        demo_bullets.append(f"Average patient age: {float(avg_age):.1f} years (range {int(current_demo.get('min_age') or 0)}–{int(current_demo.get('max_age') or 0)})")
    if demo_bullets:
        sections.append({"icon": "bi-people", "color": "#f472b6", "title": "Patient Demographics", "bullets": demo_bullets})

    # ── TAT & Reporting ───────────────────────────────────────
    tat_bullets = []
    median_tat = float(tat_data.get("median_tat_min") or 0)
    reported   = int(tat_data.get("reported_count") or 0)
    total_st   = int(current_kpis.get("total_studies") or 1)
    if median_tat > 0:
        tat_h = median_tat / 60
        # 1440 min = 24 h (industry-standard outpatient reporting target)
        # 480 min  =  8 h (inpatient guideline per ACR/ESR recommendations)
        if median_tat > 1440:
            tat_bullets.append(f"Median TAT: {tat_h:.1f} hours — exceeds 24-hour reporting target ⚠")
        elif median_tat > 480:
            tat_bullets.append(f"Median TAT: {tat_h:.1f} hours — above the 8-hour inpatient guideline")
        else:
            tat_bullets.append(f"Median TAT: {median_tat:.0f} minutes — within acceptable benchmark")
    if reported > 0:
        cov = round(reported / total_st * 100, 1)
        tat_bullets.append(
            f"Reporting coverage: {cov}% ({_fmt(reported)} of {_fmt(total_st)} studies signed)"
        )
        if cov < 70:
            tat_bullets.append("Coverage below 70% — significant reporting backlog detected ⚠")
        elif cov < 90:
            tat_bullets.append("Coverage below 90% target — minor backlog present")
    for row in (tat_data.get("by_modality") or [])[:3]:
        m_tat = float(row.get("median_tat_min") or 0)
        if m_tat > 0:
            tat_bullets.append(
                f"{row['modality']}: median {m_tat/60:.1f}h TAT ({_fmt(row['cnt'])} studies)"
            )
    if not tat_bullets:
        tat_bullets.append("TAT data not yet available — ETL reporting timestamps pending")
    sections.append({"icon": "bi-clock-history", "color": "#f59e0b",
                     "title": "TAT & Reporting", "bullets": tat_bullets})

    # ── Storage ───────────────────────────────────────────────
    storage_bullets = []
    storage_bullets.append(
        f"Total storage consumed: {float(current_storage.get('total_gb') or 0):.1f} GB"
        + (f" ({_fp(storage_chg)} vs prior period)" if storage_chg is not None else "")
    )
    avg_gb = float(current_storage.get("avg_gb_per_day") or 0)
    storage_bullets.append(f"Average daily intake: {avg_gb:.1f} GB/day")
    for m in (current_storage.get("by_modality") or [])[:4]:
        storage_bullets.append(f"{m['modality']}: {float(m['gb']):.1f} GB")
    sections.append({"icon": "bi-hdd-stack", "color": "#2dd4bf", "title": "Storage", "bullets": storage_bullets})

    # ── Period Comparison ─────────────────────────────────────
    comp_bullets = []
    if studies_chg  is not None: comp_bullets.append(f"Study volume: {_trend(studies_chg)} at {_fp(studies_chg)}")
    if patients_chg is not None: comp_bullets.append(f"Unique patients: {_trend(patients_chg)} at {_fp(patients_chg)}")
    if orders_chg   is not None: comp_bullets.append(f"Orders received: {_trend(orders_chg)} at {_fp(orders_chg)}")
    if storage_chg  is not None: comp_bullets.append(f"Storage consumed: {_trend(storage_chg)} at {_fp(storage_chg)}")
    if images_chg   is not None: comp_bullets.append(f"Images acquired: {_trend(images_chg)} at {_fp(images_chg)}")
    if comp_bullets:
        comp_bullets.insert(0, f"Comparing {start} → {end} against {cmp_start} → {cmp_end}")
        sections.append({"icon": "bi-arrow-left-right", "color": "#fb923c", "title": "Period Comparison", "bullets": comp_bullets})

    # ── Alerts ────────────────────────────────────────────────
    alerts = []
    if fulfillment_pct < 70:
        unfulfilled = int(current_orders.get("total", 0) or 0) - int(current_orders.get("fulfilled", 0) or 0)
        alerts.append(f"Order fulfillment critically low at {fulfillment_pct:.1f}% — {_fmt(unfulfilled)} orders not completed")
    elif fulfillment_pct < 85:
        alerts.append(f"Order fulfillment below 85% target — currently at {fulfillment_pct:.1f}%")
    if avg_gb > 2:
        alerts.append(f"Storage intake elevated at {avg_gb:.1f} GB/day — review archiving policy")
    if storage_chg is not None and storage_chg > 30:
        alerts.append(f"Storage grew {_fp(storage_chg)} vs prior period — capacity planning recommended")
    if studies_chg is not None and studies_chg < -20:
        alerts.append(f"Study volume fell {_fp(studies_chg)} — investigate scheduling gaps or cancellations")
    if tot_gen > 0:
        mp = round(male / tot_gen * 100)
        if mp > 70:   alerts.append(f"Notable gender skew: {mp}% male — verify referral population")
        elif mp < 30: alerts.append(f"Notable gender skew: {100-mp}% female — verify referral population")
    if alerts:
        sections.append({"icon": "bi-exclamation-triangle", "color": "#f87171", "title": "Alerts & Anomalies", "bullets": alerts})
    else:
        sections.append({"icon": "bi-check-circle", "color": "#34d399", "title": "Alerts & Anomalies", "bullets": ["No critical anomalies detected — all key metrics within normal ranges"]})

    # ── Clinical Insights (statistical signal engine) ─────────────────
    try:
        dept_signals = run_dept_insights(cur, prev)
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        dept_signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 2))
        if dept_signals:
            insight_bullets = []
            for s in dept_signals:
                icon = {"critical": "⚠", "warning": "▲", "info": "ℹ"}.get(s.get("severity", "info"), "•")
                insight_bullets.append(f"{icon} {s.get('message', '')}")
        else:
            insight_bullets = ["All department metrics within normal thresholds — no anomalies detected."]
        sections.append({
            "icon": "bi-lightbulb",
            "color": "#fbbf24",
            "title": "Clinical Insights",
            "bullets": insight_bullets,
            "signals": dept_signals,
        })
    except Exception as _ins_e:
        logger.warning(f"Clinical insights error: {_ins_e}")

    return sections
