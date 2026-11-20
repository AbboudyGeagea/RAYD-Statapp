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
from utils.site_db import served_site_id   # the site this database serves (item 13a)
from utils.ae_display import ae_display_sql
from utils.radiologist_resolve import rad_alias_join_sql, rad_display_sql, rad_ok_sql

logger = logging.getLogger("SUPER_REPORT")
super_report_bp = Blueprint("super_report", __name__)


# ─────────────────────────────────────────────
#  PAGE ROUTE
# ─────────────────────────────────────────────

@super_report_bp.route("/viewer/super-report-page")
@login_required
def super_report_page():
    # Opens on the report start date (settings, migration 0149), as every report does;
    # it used to open on the last 30 days.
    from db import get_report_start_date
    start = get_report_start_date()
    return render_template("super_report.html",
                           report_start_date=start.isoformat() if start else "",
                           start=start.isoformat() if start else "",
                           end=datetime.now().date().isoformat(),
                           panel=_panel())


# The Super Report's dropdowns on the shared left panel (_report_filter_panel.html), named
# as super_report() reads them. Their options come from /viewer/super-report/filters, which
# the page loads; each has an "Apply filter" tick (<name>_on) that the page honours when it
# builds the query. Protocol Name and Body Part (Series) are not offered: the query never
# applied them, so picking one changed nothing.
_PANEL_FILTERS = (
    ("modality", "Modality"), ("storing_ae", "AE Title"), ("study_status", "Study Status"),
    ("procedure_code", "Procedure"), ("body_part", "Body Part"),
    ("patient_class", "Patient Class"), ("patient_location", "Patient Location"), ("sex", "Sex"),
    ("report_status", "Report Status"), ("order_status", "Order Status"),
    ("order_modality", "Order Modality"), ("signing_physician", "Signing Physician"),
    ("referring_physician", "Referring Physician"),
)


def _panel():
    return [{"name": name, "toggle": f"{name}_on", "label": label, "values": [],
             "selected": [], "enabled": True} for name, label in _PANEL_FILTERS]


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
            # Only AE titles the Super Report can count: it keeps a study only when its
            # device is mapped to the default site (RH), so any other AE was offered and
            # always gave 0 (meeting of 2026-11-13). Spelled as stored, since the filter
            # compares the raw storing_ae.
            "storing_ae":          distinct("""
                SELECT x.ae FROM (SELECT DISTINCT storing_ae AS ae FROM etl_didb_studies
                                  WHERE storing_ae IS NOT NULL) x
                WHERE rayd_served_site_id() IS NULL
                   OR EXISTS (SELECT 1 FROM aetitle_modality_map m
                              WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(x.ae))
                                AND m.site_id = rayd_served_site_id())"""),
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
            "order_control":       distinct("SELECT DISTINCT order_control FROM etl_orders WHERE order_control IS NOT NULL"),
            "order_modality":      distinct("SELECT DISTINCT modality FROM etl_orders WHERE modality IS NOT NULL"),
            # series modality / body part / protocol and age group are no longer lists here: the
            # query never applied the first three, and nothing on the page offers age group
            # (_request_filters, 2026-11-20). They were the costliest DISTINCTs of this endpoint.
            "ae_modality_map":     dict(db.session.execute(text(
                "SELECT aetitle, modality FROM aetitle_modality_map WHERE aetitle IS NOT NULL AND modality IS NOT NULL"
            )).fetchall()),
            # AE -> its RIS room name (utils.ae_display); AEs with no name are left out
            "ae_station_map":      dict(db.session.execute(text(
                f"SELECT aetitle, {ae_display_sql('m', 'NULL')} FROM aetitle_modality_map m "
                f"WHERE aetitle IS NOT NULL AND {ae_display_sql('m', 'NULL')} IS NOT NULL"
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

    # The server validates every date it is given, the comparison window included: the page's
    # updateCmpDates() is the single source of the AUTOMATIC comparison window (the same length,
    # ending the day before start) and sends it, but a hand-written URL or an old preset can
    # send anything. A date that does not parse used to fall through to delta = 30 with the
    # comparison dates left empty, so the previous period ran BETWEEN NULL AND NULL and every
    # change read as zero.
    try:
        d_start = datetime.strptime(start, "%Y-%m-%d")
        d_end   = datetime.strptime(end,   "%Y-%m-%d")
        if cmp_start and cmp_end:
            d_cmp_start = datetime.strptime(cmp_start, "%Y-%m-%d")
            d_cmp_end   = datetime.strptime(cmp_end,   "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "dates must be YYYY-MM-DD"}), 400
    if d_end < d_start:
        return jsonify({"error": "the end date is before the start date"}), 400
    delta = (d_end - d_start).days + 1
    if not cmp_start or not cmp_end:
        d_cmp_end   = d_start - timedelta(days=1)
        d_cmp_start = d_cmp_end - timedelta(days=delta - 1)
        cmp_start   = d_cmp_start.strftime("%Y-%m-%d")
        cmp_end     = d_cmp_end.strftime("%Y-%m-%d")
    elif d_cmp_end < d_cmp_start:
        return jsonify({"error": "the comparison end date is before its start date"}), 400
    cmp_days = (d_cmp_end - d_cmp_start).days + 1

    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='super_report',
              detail={'from': start, 'to': end, 'cmp_from': cmp_start, 'cmp_to': cmp_end})

    filters = _request_filters()

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
    rh_site_id = served_site_id()
    cache_key["rh_site_id"] = rh_site_id

    cached = cache_get("super_report", cache_key)
    if cached is not None:
        return jsonify(cached)

    try:
        crn_cfg = dict(db.session.execute(text(
            "SELECT key, value FROM settings WHERE key IN ('crn_live_since', 'crn_sms_provider')"
        )).fetchall())
        crn_enabled = bool((crn_cfg.get('crn_live_since') or '').strip())
        crn_dry_run = (crn_cfg.get('crn_sms_provider') or 'log') == 'log'

        current   = _collect_data(start, end, filters, rh_site_id)
        previous  = _collect_data(cmp_start, cmp_end, filters, rh_site_id, include_rollups=False)
        narrative = _generate_narrative(current, previous, start, end, cmp_start, cmp_end, cmp_days)
        result = {
            "current":     current,
            "previous":    previous,
            "narrative":   narrative,
            "cmp_start":   cmp_start,
            "cmp_end":     cmp_end,
            "delta_days":  delta,
            "cmp_days":    cmp_days,
            "crn_enabled": crn_enabled,
            "crn_dry_run": crn_dry_run,
        }
        cache_put("super_report", cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Super report error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/sites")
@login_required
def super_report_sites():
    """RH and SJH side by side, per modality (meeting of 2026-11-13, item 13b: "direct
    comparison only in the Super Report and the Live Department board, per modality"). For
    an account holding two or more sites that have a database (utils/site_db.py): the same
    period and filters run on each site's own database, with the Super Report's own
    definitions (_build_where, _joins, the TAT clock of _collect_data). Anyone else gets no
    sites, and the page shows nothing."""
    from utils import site_db
    from utils.roles import all_sites
    start, end = request.args.get("start"), request.args.get("end")
    if not start or not end:
        return jsonify({"error": "start and end date required"}), 400
    site_ids = sorted(site_db.selectable_site_ids(current_user))
    if len(site_ids) < 2:
        return jsonify({"sites": [], "modalities": [], "totals": {}})
    filters = _request_filters()
    key = {"start": start, "end": end, "sites": site_ids,
           **{k: (sorted(v) if isinstance(v, list) else v) for k, v in filters.items()}}
    cached = cache_get("super_report_sites", key)
    if cached is not None:
        return jsonify(cached)

    names = {sid: (code, name) for sid, code, name in all_sites()}
    codes = {sid: names[sid][0] if sid in names else str(sid) for sid in site_ids}
    per_site = {}
    try:
        for sid in site_ids:
            with site_db.use_site(sid):
                per_site[sid] = _site_modalities(start, end, filters, sid)
    except Exception as e:
        db.session.rollback()
        logger.error(f"Super report sites error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

    modalities = {}
    for sid, rows in per_site.items():
        for r in rows:
            modalities.setdefault(r["modality"], {})[codes[sid]] = r
    order = sorted(modalities, key=lambda m: -sum(v["studies"] for v in modalities[m].values()))
    result = {
        "sites": [{"id": sid, "code": codes[sid], "name": names[sid][1] if sid in names else ""}
                  for sid in site_ids],
        "modalities": [{"modality": m, "by_site": modalities[m]} for m in order],
        "totals": {codes[sid]: {"studies": sum(r["studies"] for r in rows),
                                "patients": sum(r["patients"] for r in rows),
                                "gb": round(sum(r["gb"] for r in rows), 2)}
                   for sid, rows in per_site.items()},
    }
    cache_put("super_report_sites", key, result)
    return jsonify(result)


def _site_modalities(start, end, filters, site_id):
    """Per modality, on the database the session reads (use_site): studies, patients,
    reported studies and their median TAT, storage. Run once per site."""
    where, params = _build_where(start, end, filters, site_id)
    mj, pj = _joins()
    tat = ("EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) "
           "- s.insert_time)) / 60.0")
    reported = ("COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL "
                "AND s.insert_time IS NOT NULL")
    rows = db.session.execute(text(f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               COUNT(DISTINCT s.study_db_uid)   AS studies,
               COUNT(DISTINCT s.patient_db_uid) AS patients,
               COUNT(*) FILTER (WHERE {reported}) AS reported,
               ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {tat})
                      FILTER (WHERE {reported}))::numeric, 1) AS median_tat_min
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1
    """), params).mappings().fetchall()
    sp = {"start": start, "end": end}
    sc = "study_date BETWEEN :start AND :end"
    if filters.get("modality"):
        sc += " AND modality = ANY(:modality)"
        sp["modality"] = filters["modality"]
    gb = dict(db.session.execute(text(f"""
        SELECT COALESCE(modality, 'Unknown'), ROUND(SUM(total_gb)::numeric, 2)
        FROM summary_storage_daily WHERE {sc} GROUP BY 1
    """), sp).fetchall())
    out = [{"modality": r["modality"], "studies": int(r["studies"] or 0),
            "patients": int(r["patients"] or 0), "reported": int(r["reported"] or 0),
            "median_tat_min": float(r["median_tat_min"]) if r["median_tat_min"] is not None else None,
            "gb": float(gb.pop(r["modality"], 0) or 0)} for r in rows]
    for modality, value in gb.items():          # storage for a modality with no study counted
        out.append({"modality": modality, "studies": 0, "patients": 0, "reported": 0,
                    "median_tat_min": None, "gb": float(value or 0)})
    return out


# ─────────────────────────────────────────────
#  DATA COLLECTION
# ─────────────────────────────────────────────

def _build_where(start, end, filters, rh_site_id=None):
    clauses = [
        "s.study_date BETWEEN :start AND :end",
        "COALESCE(m.modality, s.study_modality, 'Unknown') NOT IN ('SR', 'BMD')",
    ]
    params  = {"start": start, "end": end}

    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH excluded, for now. Same served_site_id()/aetitle_modality_map
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
        # No age_group: the page's age filter is the min / max range below (sr_extra); no
        # dropdown offers age_group, and the two together ANDed (review of 2026-11-20).
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


# Per database (each site's database answers for itself, rule 10): once etl_orders.site_id
# carries data it always will, so a yes is kept for the life of the process and the probe runs
# once per process and database, instead of twice per request (both periods). A no is not kept
# (the ETL may fill the column later; until then the probe runs as before), nor a failure. A
# restart asks again.
_ORDERS_SITE_KNOWN = {}


def _orders_have_site():
    """True when etl_orders.site_id exists and is actually populated.

    See the call site: scoping orders to a site is only safe once the ETL has filled
    that column, otherwise the filter matches nothing and the Overview block reports
    zero orders.
    """
    from utils import site_db
    key = site_db.current_site_id()
    if _ORDERS_SITE_KNOWN.get(key):
        return True
    try:
        known = bool(db.session.execute(text("""
            SELECT EXISTS (SELECT 1 FROM etl_orders WHERE site_id IS NOT NULL)
        """)).scalar())
    except Exception:
        db.session.rollback()
        return False
    if known:
        _ORDERS_SITE_KNOWN[key] = True
        logger.info("etl_orders.site_id carries data on this database (site %s): orders scoped to "
                    "the site; asked once per process", key)
    return known


def _request_filters():
    """The filters of /viewer/super-report and /viewer/super-report/sites, as named there.
    Only filters a query applies: series_modality, body_part_series and protocol_name were read
    and never applied, age_group was offered by nothing on the page (review of 2026-11-20);
    migration 0180 took them out of the saved presets. order_control and order_modality apply
    to the orders figures."""
    filters = {k: request.args.getlist(k) for k in [
        "modality","storing_ae","study_status","report_status","order_status",
        "patient_class","patient_location","body_part","procedure_code",
        "signing_physician","referring_physician","sex",
        "order_control","order_modality",
    ]}
    filters["has_report"] = request.args.get("has_report")
    filters["age_min"]    = request.args.get("age_min")
    filters["age_max"]    = request.args.get("age_max")
    return filters


def _joins():
    """The device (m) and patient (p) joins every study query here uses. Why the device join
    is LATERAL ... LIMIT 1: see _collect_data. station_name is the device's label: the RIS
    room name (utils.ae_display). etl_patient_view.patient_db_uid is the PK, so p is 1:1."""
    mj = ("LEFT JOIN LATERAL ("
          f"SELECT m0.modality, {ae_display_sql('m0', 'NULL')} AS station_name, m0.site_id "
          "FROM aetitle_modality_map m0 "
          "WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) "
          "ORDER BY m0.id LIMIT 1) m ON TRUE")
    pj = "LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid"
    return mj, pj


def _collect_data(start, end, filters, rh_site_id=None, include_rollups=True):
    """include_rollups=False for the comparison period: of the rollups the page reads only the
    critical-findings count (the CRN card's change badge: render() pct(c.critical_count,
    pc_.critical_count)) and the device utilization (its badge: pu.available,
    pu.avg_utilization); the rest of the CRN summary and the residents' wet reads are never read
    for it, so they are not computed (review of 2026-11-20)."""
    where, params = _build_where(start, end, filters, rh_site_id)
    # LATERAL ... LIMIT 1, not a plain LEFT JOIN. aetitle_modality_map's UNIQUE is on
    # the RAW aetitle while every consumer matches on UPPER(TRIM(...)), so 'ct99' and
    # 'CT99' are both legal rows AND both match the same study -- the fan-out migration
    # 0116 had to repair, and which the RIS import can recreate on any cycle (0116's
    # own closing note).
    #
    # Why it mattered here more than elsewhere: total_studies is COUNT(DISTINCT
    # s.study_db_uid) and is immune, while every other figure feeding the brief is
    # COUNT(*) or SUM() over this join. A duplicated AE inflated the numerators and left
    # the denominator alone, so the narrative printed shares against a smaller total --
    # reproduced with one duplicate row on 20% of studies: modality shares summing to
    # 120%, physician shares 20% high, total_images 20% high, and "Reporting coverage:
    # 115.0% (1,150 of 1,000 studies signed)".
    #
    # LIMIT 1 makes one study exactly one row whether or not duplicates exist, so this
    # holds even after the next RIS import. ORDER BY id keeps the choice deterministic.
    # station_name here is the device's label: the RIS room name (utils.ae_display).
    mj, pj = _joins()

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
    # Site-scope the orders the same way every study figure is scoped. Without this the
    # brief printed "N orders received" (all sites) directly beside "M studies
    # completed" (RH only) in the same Overview block -- on LAUMC the satellite is ~45%
    # of RIS volume, so the two numbers were never comparable and the fulfillment rate
    # was measured against a population half the report never shows.
    #
    # etl_orders.site_id is populated by the orders ETL; the older comment here said
    # orders had "no site-resolvable join", which stopped being true when that column
    # landed. Guarded on the column actually carrying data, because filtering on a
    # column that is NULL everywhere would silently zero the whole section -- if it is
    # unpopulated this leaves the previous (unscoped) behaviour exactly as it was.
    if rh_site_id is not None and _orders_have_site():
        oc.append("o.site_id = :o_site_id")
        op["o_site_id"] = rh_site_id
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
    # Each category's classes are a bound array parameter (= ANY, as in _build_where), never
    # quoted into the SQL text. pc_params joins params wherever one of these filters is used.
    pc_params = {}

    def _pc_filter(cfg_key, default):
        vals = [v.strip() for v in _pc_cfg.get(cfg_key, default).split(',') if v.strip()]
        if not vals:
            return 'FALSE'
        pc_params[cfg_key] = vals
        return f"s.patient_class = ANY(:{cfg_key})"

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
    """), {**params, **pc_params}).mappings().fetchone()

    pc_breakdown = db.session.execute(text(f"""
        SELECT COALESCE(s.patient_class, 'Unknown') AS class, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC
    """), params).mappings().fetchall()

    er_filter = _pc_filter('pc_emergency', 'E,EP,ER,EMERGENCY,URG,URGENT')

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
        SELECT am.aetitle, {ae_display_sql('am', 'NULL')} AS station_name, COALESCE(sub.cnt, 0) AS cnt
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
    if 'pc_emergency' in pc_params:
        # er_days applies the page's own WHERE (site, modality, patient class and every other
        # filter), like p75 and the count: it used to take any ER study of any site or
        # modality, so with CT on RH picked a day counted for an ER MR or an SJH ER study.
        er_delayed_row = db.session.execute(text(f"""
            WITH er_days AS (
                SELECT DISTINCT s.study_date FROM etl_didb_studies s {mj} {pj}
                WHERE {where} AND {er_filter}
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
        """), {**params, **pc_params}).mappings().fetchone()
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
    """), {**params, **pc_params}).mappings().fetchone()

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
               COALESCE(m.station_name, s.storing_ae, 'Unknown') AS dim,
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
        "crn":         (_get_crn_summary(start, end) if include_rollups
                        else {"critical_count": _critical_count(start, end)}),
        "utilization": _get_utilization_summary(start, end),    # both periods: see the docstring
        "resident_wetreads": (_get_resident_wetreads(where, params, mj, pj) if include_rollups
                              else None),
    }


def _get_resident_wetreads(where, params, mj, pj):
    """Item 8 (operator 2026-11-19): residents counted by their wet reads, from the PACS audit
    trail copy (etl_pacs_audit_events, kind 'wetread', the login and second of each one): each
    study once, for the resident whose wet read came first; a resident is a login of the PACS
    "residents" group or marked so (utils/pacs_roles). Only studies that reached PACS while the
    audit copy holds them count (from 1 Oct 2026). Same dates and filters as the rest of the page;
    staff who left read "Former staff" while that switch is on (the audit copy has no rh view)."""
    from ETL_JOBS.etl_pacs_audit import coverage as audit_coverage
    from utils import staff_status
    from utils.pacs_roles import role_lookup_cte
    from utils.sticky_notes import person_name
    out = {'coverage': None, 'modalities': [], 'residents': [], 'total': 0}
    try:
        audit_from, audit_until = audit_coverage(db.session)
        if audit_from is None:
            return out
        out['coverage'] = {'from': audit_from.strftime('%Y-%m-%d'), 'until': audit_until.strftime('%Y-%m-%d %H:%M')}
        rows = db.session.execute(text(f"""
            WITH {role_lookup_cte('role_lookup')},
            studies AS MATERIALIZED (
                SELECT s.study_db_uid, BTRIM(s.study_instance_uid) AS uid,
                       COALESCE(m.modality, s.study_modality, 'Unknown') AS modality
                FROM etl_didb_studies s {mj} {pj}
                WHERE {where} AND s.study_instance_uid IS NOT NULL
                  AND s.insert_time >= :wr_from AND s.insert_time < :wr_until
            ),
            wet AS (
                SELECT DISTINCT ON (st.study_db_uid) st.study_db_uid, st.modality, e.user_name
                FROM studies st
                JOIN etl_pacs_audit_events e ON e.study_instance_uid = st.uid AND e.kind = 'wetread'
                JOIN role_lookup r ON r.login_id = UPPER(SPLIT_PART(e.user_name, '@', 1)) AND r.role = 'residents'
                ORDER BY st.study_db_uid, e.event_at, e.log_dbid
            )
            SELECT w.user_name, alias.canonical_name AS alias,
                   {staff_status.login_sql('w.user_name')} IN {staff_status.left_logins_sql()} AS left_staff,
                   w.modality, COUNT(*) AS cnt
            FROM wet w
            LEFT JOIN physician_alias_map alias ON alias.dismissed = false AND alias.alias = w.user_name
            GROUP BY 1, 2, 3, 4
        """), {**params, 'wr_from': audit_from, 'wr_until': audit_until}).mappings().all()
    except Exception:
        logger.exception("resident wet reads not computed")
        db.session.rollback()
        return out
    by_res, mods = {}, {}
    for r in rows:
        name = staff_status.FORMER if r['left_staff'] else (person_name(r['alias'], r['user_name']) or r['user_name'])
        res = by_res.setdefault(name, {'name': name, 'total': 0, 'by_modality': {}})
        res['by_modality'][r['modality']] = res['by_modality'].get(r['modality'], 0) + int(r['cnt'])
        res['total'] += int(r['cnt'])
        mods[r['modality']] = mods.get(r['modality'], 0) + int(r['cnt'])
    out['modalities'] = sorted(mods, key=lambda m: (-mods[m], m))
    out['residents'] = sorted(by_res.values(), key=lambda r: (-r['total'], r['name']))
    out['total'] = sum(mods.values())
    return out


def _get_crn_summary(start, end):
    """Critical Result Notification activity for the period.

    Not site-scoped (no established site-resolution join for hl7_oru_reports /
    crn_notifications yet) and not filtered by Super Report's other active
    filters -- a period-only rollup, same simplification as
    _get_utilization_summary below. The CRN numbers are Admin > CRN > Report's
    own summary (utils/crn_admin_ops.report), so the two always agree.
    """
    from utils.crn_admin_ops import report as crn_report

    s = crn_report(start, end)['summary']
    return {
        "critical_count":      _critical_count(start, end),
        "total_notifications": s['total'],
        "acknowledged":        s['acknowledged'],
        "overdue":             s['overdue'],
        "open":                s['open'],
        "median_ack_min":      s['median_min'],
    }


def _critical_count(start, end):
    """Reports the NLP worker flagged critical in the period (the CRN card's first line)."""
    n = db.session.execute(text("""
        SELECT COUNT(*)
        FROM hl7_oru_analysis a
        JOIN hl7_oru_reports r ON r.id = a.report_id
        WHERE a.is_critical = TRUE
          AND COALESCE(r.result_datetime, r.received_at)::date BETWEEN :start AND :end
    """), {"start": start, "end": end}).scalar()
    return int(n or 0)


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
    from routes.report_34 import utilization_data

    data = utilization_data(start, end)
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


# The narrative's thresholds, each a settings key; these values when a key is absent or not a
# number (review of 2026-11-20: they were literals in the code).
_NARRATIVE_DEFAULTS = {
    "sr_tat_outpatient_warn_min": 1440,   # 24 h: the outpatient reporting target
    "sr_tat_inpatient_warn_min":  480,    # 8 h: the inpatient guideline (ACR/ESR)
    "sr_coverage_warn_pct":       70,     # below: "significant reporting backlog"
    "sr_coverage_target_pct":     90,     # below: "minor backlog"
    "sr_storage_gb_day_warn":     2,      # GB a day above which intake is "elevated"
    "sr_storage_growth_warn_pct": 30,     # storage growth vs the comparison period
    "sr_volume_drop_warn_pct":    -20,    # study volume change vs the comparison period
    "sr_gender_skew_warn_pct":    70,     # share of one sex (and 100 minus it the other way)
}


def _narrative_thresholds():
    rows = db.session.execute(text("SELECT key, value FROM settings WHERE key = ANY(:keys)"),
                              {"keys": list(_NARRATIVE_DEFAULTS)}).fetchall()
    out = dict(_NARRATIVE_DEFAULTS)
    for key, value in rows:
        try:
            out[key] = float(value)
        except (TypeError, ValueError):
            logger.warning("settings %s = %r is not a number; using %s", key, value, out[key])
    return out


def _num(v):
    """A threshold as text: 24 not 24.0, 2.5 as it is."""
    return f"{v:g}"


def _generate_narrative(cur, prev, start, end, cmp_start, cmp_end, cmp_days):
    """cmp_days: the length of the COMPARISON window, which the user can set to any length;
    "vs prior N-day period" names that window, not the current one."""
    th = _narrative_thresholds()
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
    chg_str = f" ({_fp(studies_chg)} vs prior {cmp_days}-day period)" if studies_chg is not None else ""
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
        outp_min, inp_min = th["sr_tat_outpatient_warn_min"], th["sr_tat_inpatient_warn_min"]
        if median_tat > outp_min:
            tat_bullets.append(f"Median TAT: {tat_h:.1f} hours — exceeds {_num(outp_min / 60)}-hour reporting target ⚠")
        elif median_tat > inp_min:
            tat_bullets.append(f"Median TAT: {tat_h:.1f} hours — above the {_num(inp_min / 60)}-hour inpatient guideline")
        else:
            tat_bullets.append(f"Median TAT: {median_tat:.0f} minutes — within acceptable benchmark")
    if reported > 0:
        cov = round(reported / total_st * 100, 1)
        tat_bullets.append(
            f"Reporting coverage: {cov}% ({_fmt(reported)} of {_fmt(total_st)} studies signed)"
        )
        cov_warn, cov_target = th["sr_coverage_warn_pct"], th["sr_coverage_target_pct"]
        if cov < cov_warn:
            tat_bullets.append(f"Coverage below {_num(cov_warn)}% — significant reporting backlog detected ⚠")
        elif cov < cov_target:
            tat_bullets.append(f"Coverage below {_num(cov_target)}% target — minor backlog present")
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
    if avg_gb > th["sr_storage_gb_day_warn"]:
        alerts.append(f"Storage intake elevated at {avg_gb:.1f} GB/day — review archiving policy")
    if storage_chg is not None and storage_chg > th["sr_storage_growth_warn_pct"]:
        alerts.append(f"Storage grew {_fp(storage_chg)} vs prior period — capacity planning recommended")
    if studies_chg is not None and studies_chg < th["sr_volume_drop_warn_pct"]:
        alerts.append(f"Study volume fell {_fp(studies_chg)} — investigate scheduling gaps or cancellations")
    if tot_gen > 0:
        mp = round(male / tot_gen * 100)
        skew = th["sr_gender_skew_warn_pct"]
        if mp > skew:         alerts.append(f"Notable gender skew: {mp}% male — verify referral population")
        elif mp < 100 - skew: alerts.append(f"Notable gender skew: {100-mp}% female — verify referral population")
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
