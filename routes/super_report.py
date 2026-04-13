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

SUPER_REPORT_SENTINEL = 0  # base_report_id for all Super Report saves

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
        existing = db.session.execute(text("""
            SELECT id FROM saved_reports
            WHERE owner_user_id = :uid
              AND base_report_id = :bid
              AND name = :name
        """), {"uid": current_user.id, "bid": SUPER_REPORT_SENTINEL, "name": name}).fetchone()

        if existing:
            # Update existing
            db.session.execute(text("""
                UPDATE saved_reports
                SET filter_json = :fj, updated_at = NOW()
                WHERE id = :id
            """), {"fj": json.dumps(filter_json), "id": existing[0]})
            saved_id = existing[0]
        else:
            # Insert new
            result = db.session.execute(text("""
                INSERT INTO saved_reports (name, owner_user_id, base_report_id, is_public, filter_json)
                VALUES (:name, :uid, :bid, FALSE, :fj)
                RETURNING id
            """), {
                "name": name,
                "uid":  current_user.id,
                "bid":  SUPER_REPORT_SENTINEL,
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
              AND base_report_id = :bid
            ORDER BY updated_at DESC
        """), {"uid": current_user.id, "bid": SUPER_REPORT_SENTINEL}).mappings().fetchall()

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
              AND base_report_id = :bid
        """), {"id": report_id, "uid": current_user.id, "bid": SUPER_REPORT_SENTINEL})
        db.session.commit()

        if result.rowcount == 0:
            return jsonify({"success": False, "error": "Not found or not yours"}), 404
        return jsonify({"success": True})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Delete saved report error: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500



@super_report_bp.route("/viewer/super-report/filters")
@login_required
def super_report_filters():
    try:
        def distinct(sql):
            rows = db.session.execute(text(sql)).fetchall()
            return sorted([r[0] for r in rows if r[0] is not None and str(r[0]).strip() != ''])

        return jsonify({
            "modality":            distinct("SELECT DISTINCT COALESCE(m.modality, s.study_modality) FROM etl_didb_studies s LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle WHERE COALESCE(m.modality, s.study_modality) IS NOT NULL"),
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
        })
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

    try:
        current   = _collect_data(start, end, filters)
        previous  = _collect_data(cmp_start, cmp_end, filters)
        narrative = _generate_narrative(current, previous, start, end, cmp_start, cmp_end, delta)
        return jsonify({
            "current":    current,
            "previous":   previous,
            "narrative":  narrative,
            "cmp_start":  cmp_start,
            "cmp_end":    cmp_end,
            "delta_days": delta,
        })
    except Exception as e:
        logger.error(f"Super report error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  DATA COLLECTION
# ─────────────────────────────────────────────

def _build_where(start, end, filters):
    clauses = ["s.study_date BETWEEN :start AND :end"]
    params  = {"start": start, "end": end}

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


def _collect_data(start, end, filters):
    where, params = _build_where(start, end, filters)
    mj = "LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle"
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

    demo = db.session.execute(text(f"""
        SELECT COUNT(*) FILTER (WHERE p.sex ILIKE 'M%') AS male,
               COUNT(*) FILTER (WHERE p.sex ILIKE 'F%') AS female,
               COUNT(*) FILTER (WHERE s.patient_class ILIKE '%IN%') AS inpatient,
               COUNT(*) FILTER (WHERE s.patient_class ILIKE '%OUT%' OR s.patient_class ILIKE '%AMB%') AS outpatient,
               ROUND(AVG(s.age_at_exam),1) AS avg_age,
               MIN(s.age_at_exam) AS min_age,
               MAX(s.age_at_exam) AS max_age
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """), params).mappings().fetchone()

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
        "physicians":  [dict(r) for r in physicians],
        "demographics": dict(demo),
    }


# ─────────────────────────────────────────────
#  RULE-BASED NARRATIVE
# ─────────────────────────────────────────────

def _pct(cur, prev):
    try:
        c, p = float(cur or 0), float(prev or 0)
        return round((c - p) / p * 100, 1) if p else None
    except Exception:
        return None

def _fmt(n):
    try: return f"{int(n):,}"
    except Exception: return "—"

def _fp(p):
    if p is None: return None
    return f"{'+'if p>=0 else ''}{p}%"

def _trend(p):
    if p is None: return "unchanged"
    if p > 20:  return "significantly up"
    if p > 5:   return "up"
    if p > 0:   return "slightly up"
    if p < -20: return "significantly down"
    if p < -5:  return "down"
    if p < 0:   return "slightly down"
    return "flat"


def _generate_narrative(cur, prev, start, end, cmp_start, cmp_end, delta):
    ck, pk = cur["kpis"],    prev["kpis"]
    co, po = cur["orders"],  prev["orders"]
    cs, ps = cur["storage"], prev["storage"]
    cv     = cur["volume"]
    cd     = cur["demographics"]
    cp     = cur["physicians"]

    s_chg  = _pct(ck.get("total_studies"),  pk.get("total_studies"))
    pt_chg = _pct(ck.get("total_patients"), pk.get("total_patients"))
    st_chg = _pct(cs.get("total_gb"),       ps.get("total_gb"))
    or_chg = _pct(co.get("total"),          po.get("total"))
    im_chg = _pct(ck.get("total_images"),   pk.get("total_images"))

    sections = []

    # ── Overview ──────────────────────────────────────────────
    overview = []
    chg_str = f" ({_fp(s_chg)} vs prior {delta}-day period)" if s_chg is not None else ""
    overview.append(
        f"{_fmt(ck.get('total_studies'))} studies completed between {start} and {end}{chg_str}"
    )
    overview.append(
        f"{_fmt(ck.get('total_patients'))} unique patients seen"
        + (f", {_fp(pt_chg)} vs prior period" if pt_chg is not None else "")
    )
    overview.append(
        f"{_fmt(ck.get('total_images'))} images acquired across {_fmt(ck.get('active_aes'))} active devices"
        + (f" ({_fp(im_chg)})" if im_chg is not None else "")
    )
    ff = float(co.get("fulfillment_pct") or 0)
    overview.append(
        f"{_fmt(co.get('total'))} orders received — {ff:.1f}% fulfillment rate "
        f"({_fmt(co.get('fulfilled'))} completed)"
    )
    sections.append({"icon": "bi-bar-chart-line", "color": "#60a5fa", "title": "Overview", "bullets": overview})

    # ── Volume & Modality ─────────────────────────────────────
    volume_bullets = []
    avg_day = float(cv.get("avg_per_day") or 0)
    volume_bullets.append(f"Average throughput: {avg_day:.1f} studies per day")
    if cv.get("peak_day"):
        volume_bullets.append(f"Peak day: {cv['peak_day']} with {_fmt(cv['peak_count'])} studies")
    mods = cv.get("top_modalities", [])
    tot  = float(ck.get("total_studies") or 1)
    for m in mods:
        pct_share = round(float(m["cnt"]) / tot * 100, 1)
        volume_bullets.append(f"{m['modality']}: {_fmt(m['cnt'])} studies ({pct_share}% of total)")
    sections.append({"icon": "bi-graph-up", "color": "#34d399", "title": "Volume & Modality Mix", "bullets": volume_bullets})

    # ── Physicians ────────────────────────────────────────────
    if cp:
        phys_bullets = []
        for i, p in enumerate(cp[:5]):
            share = round(float(p["cnt"]) / tot * 100, 1)
            prefix = ["Top referrer", "2nd", "3rd", "4th", "5th"][i]
            phys_bullets.append(f"{prefix}: {p['physician']} — {_fmt(p['cnt'])} referrals ({share}%)")
        sections.append({"icon": "bi-person-badge", "color": "#a78bfa", "title": "Top Referring Physicians", "bullets": phys_bullets})

    # ── Demographics ──────────────────────────────────────────
    demo_bullets = []
    male   = float(cd.get("male") or 0)
    female = float(cd.get("female") or 0)
    inp    = float(cd.get("inpatient") or 0)
    outp   = float(cd.get("outpatient") or 0)
    tot_gen = male + female
    if tot_gen > 0:
        demo_bullets.append(f"Gender split: {round(male/tot_gen*100)}% male / {round(female/tot_gen*100)}% female")
    tot_cls = inp + outp
    if tot_cls > 0:
        demo_bullets.append(f"Patient class: {round(inp/tot_cls*100)}% inpatient / {round(outp/tot_cls*100)}% outpatient")
    avg_age = cd.get("avg_age")
    if avg_age:
        demo_bullets.append(f"Average patient age: {float(avg_age):.1f} years (range {int(cd.get('min_age') or 0)}–{int(cd.get('max_age') or 0)})")
    if demo_bullets:
        sections.append({"icon": "bi-people", "color": "#f472b6", "title": "Patient Demographics", "bullets": demo_bullets})

    # ── Storage ───────────────────────────────────────────────
    storage_bullets = []
    storage_bullets.append(
        f"Total storage consumed: {float(cs.get('total_gb') or 0):.1f} GB"
        + (f" ({_fp(st_chg)} vs prior period)" if st_chg is not None else "")
    )
    avg_gb = float(cs.get("avg_gb_per_day") or 0)
    storage_bullets.append(f"Average daily intake: {avg_gb:.1f} GB/day")
    for m in (cs.get("by_modality") or [])[:4]:
        storage_bullets.append(f"{m['modality']}: {float(m['gb']):.1f} GB")
    sections.append({"icon": "bi-hdd-stack", "color": "#2dd4bf", "title": "Storage", "bullets": storage_bullets})

    # ── Period Comparison ─────────────────────────────────────
    comp_bullets = []
    if s_chg  is not None: comp_bullets.append(f"Study volume: {_trend(s_chg)} at {_fp(s_chg)}")
    if pt_chg is not None: comp_bullets.append(f"Unique patients: {_trend(pt_chg)} at {_fp(pt_chg)}")
    if or_chg is not None: comp_bullets.append(f"Orders received: {_trend(or_chg)} at {_fp(or_chg)}")
    if st_chg is not None: comp_bullets.append(f"Storage consumed: {_trend(st_chg)} at {_fp(st_chg)}")
    if im_chg is not None: comp_bullets.append(f"Images acquired: {_trend(im_chg)} at {_fp(im_chg)}")
    if comp_bullets:
        comp_bullets.insert(0, f"Comparing {start} → {end} against {cmp_start} → {cmp_end}")
        sections.append({"icon": "bi-arrow-left-right", "color": "#fb923c", "title": "Period Comparison", "bullets": comp_bullets})

    # ── Alerts ────────────────────────────────────────────────
    alerts = []
    if ff < 70:
        unfulfilled = int(co.get("total", 0) or 0) - int(co.get("fulfilled", 0) or 0)
        alerts.append(f"Order fulfillment critically low at {ff:.1f}% — {_fmt(unfulfilled)} orders not completed")
    elif ff < 85:
        alerts.append(f"Order fulfillment below 85% target — currently at {ff:.1f}%")
    if avg_gb > 2:
        alerts.append(f"Storage intake elevated at {avg_gb:.1f} GB/day — review archiving policy")
    if st_chg is not None and st_chg > 30:
        alerts.append(f"Storage grew {_fp(st_chg)} vs prior period — capacity planning recommended")
    if s_chg is not None and s_chg < -20:
        alerts.append(f"Study volume fell {_fp(s_chg)} — investigate scheduling gaps or cancellations")
    if tot_gen > 0:
        mp = round(male / tot_gen * 100)
        if mp > 70:   alerts.append(f"Notable gender skew: {mp}% male — verify referral population")
        elif mp < 30: alerts.append(f"Notable gender skew: {100-mp}% female — verify referral population")
    if alerts:
        sections.append({"icon": "bi-exclamation-triangle", "color": "#f87171", "title": "Alerts & Anomalies", "bullets": alerts})
    else:
        sections.append({"icon": "bi-check-circle", "color": "#34d399", "title": "Alerts & Anomalies", "bullets": ["No critical anomalies detected — all key metrics within normal ranges"]})

    return sections
