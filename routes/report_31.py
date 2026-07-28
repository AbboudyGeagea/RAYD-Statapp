"""
routes/report_31.py
--------------------
Report 31 — Operations.

Standalone extraction of Report 25's "Operations" tab (templates/report_25.html
lines ~277-414 / tab-ops): department-wide study volume, active scan time,
revenue capture (RVU), TAT percentiles, TAT-by-patient-class, hourly arrival
pattern, modality volume split, and the procedure-duration vs. turnaround-time
correlation scatter.

Deliberately excludes the AE utilization matrix (per-device, per-weekday
capacity grid) that used to live alongside this data in Report 25 — that
now belongs to Report 34 (Device Utilization), built in parallel. See the
"High Stress Devices" note in get_report_31_data() for what that means for
the KPI row.

Register in registry.py:
    import routes.report_31
"""
import csv
import io
import logging
from datetime import date

import pandas as pd
from flask import Blueprint, Response, render_template, request
from flask_login import login_required
from sqlalchemy import text

from db import db, get_etl_cutoff_date
from utils.site_resolver import default_site

logger = logging.getLogger("report_31")

report_31_bp = Blueprint("report_31", __name__)


def _iqr_filter(series):
    """Keep only values within 1.5x IQR of Q1/Q3 — same outlier rule report_25.py
    uses (_iqr_filter()) to strip extreme duration/TAT pairs before computing
    Pearson r for the correlation scatter."""
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    return (series >= q1 - 1.5 * iqr) & (series <= q3 + 1.5 * iqr)


def get_report_config(form):
    """
    Builds the base CTE source + filter fragments for Report 31.

    Sourced from the SAME report_sql_query as Report 25 (report_template.report_id
    = 25) — operator instruction: Report 31 is a standalone extraction of that
    report's Operations tab, not a new data source. The live template currently
    returns (verified against the report_template table directly):

        aetitle, modality, study_date, patient_class, patient_location,
        reading_radiologist, procedure_code, total_tat_min, proc_duration,
        rvu, base_daily_capacity, patient_id

    scoped to s.rep_final_timestamp IS NOT NULL AND s.rep_final_signed_by IS
    NOT NULL (only studies with a signed final report) with :start/:end already
    bound inside the template text itself. NOTE: the template only emits a
    single 'rvu' column (no clinical_rvu/technical_rvu split) — see
    get_report_31_data()'s revenue-capture comment for how that's handled.
    """
    base_sql = db.session.execute(
        text("SELECT report_sql_query FROM report_template WHERE report_id = 25")
    ).scalar()

    go_live = get_etl_cutoff_date()
    default_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    default_end = date.today().strftime("%Y-%m-%d")

    start = form.get("start_date", default_start)
    end = form.get("end_date", default_end)

    params = {"start": start, "end": end}

    # SR/OT exclusion — load-bearing per CLAUDE.md: every query touching
    # etl_didb_studies must filter these out (SR = auto-generated Structured
    # Report; OT = "Other" — neither is a real interpretable study).
    extra = ["COALESCE(modality, '') NOT IN ('SR', 'OT')"]

    if form.get("f_mod_active") == "on" and form.get("f_mod"):
        extra.append("UPPER(modality) = UPPER(:mod)")
        params["mod"] = form.get("f_mod")

    if form.get("f_class_active") == "on" and form.get("f_class"):
        extra.append("UPPER(patient_class) = UPPER(:p_class)")
        params["p_class"] = form.get("f_class")

    # LAUMC site rule (operator instruction, 2026-07-26 — full rationale in
    # report_25.py's get_gold_standard_data()): reports show RH (main site)
    # only, SJH (satellite) excluded, resolved via the device
    # (storing_ae -> aetitle_modality_map.site_id) since etl_didb_studies.site_id
    # is never populated. default_site() resolves to None (filter skipped,
    # not applied) on a non-LAUMC/single-site install, or on an install where
    # the sites table/site_id column hasn't been migrated in yet.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        extra.append(
            "aetitle IN (SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE site_id = :rh_site_id)"
        )

    extra_where = " AND " + " AND ".join(extra)
    return base_sql, start, end, params, extra_where, rh_site_id


def get_report_31_data(form):
    """Runs the Operations queries and returns the data dict used by the template,
    or None if the base template is missing or the query returns no rows."""
    base_sql, start, end, params, extra_where, rh_site_id = get_report_config(form)
    if not base_sql:
        return None, start, end

    sql = text(f"""
        WITH base_data AS ({base_sql})
        SELECT modality, patient_class, study_date, total_tat_min, proc_duration, rvu
        FROM base_data
        WHERE 1=1{extra_where}
    """)

    try:
        df = pd.DataFrame(db.session.execute(sql, params).mappings().all())
    except Exception:
        logger.exception("Report 31 base query failed")
        db.session.rollback()
        return None, start, end

    if df.empty:
        logger.warning("Report 31 query returned 0 rows (start=%s end=%s)", start, end)
        return None, start, end

    df["total_tat_min"] = pd.to_numeric(df["total_tat_min"], errors="coerce").fillna(0)
    df["proc_duration"] = pd.to_numeric(df["proc_duration"], errors="coerce").fillna(0)
    # Same defensive default as report_25.py's cleaning pass: rows with an
    # unparseable RVU default to 1.0 rather than 0, so a data-quality gap
    # doesn't silently zero out revenue capture for that study.
    df["rvu"] = pd.to_numeric(df["rvu"], errors="coerce").fillna(1.0)
    # clinical_rvu / technical_rvu — same backward-compat split report_25.py
    # applies (see its get_gold_standard_data() defensive-cleaning comment):
    # the live report_id=25 template still emits only a single legacy 'rvu'
    # column (procedure_duration_map has no real clinical_rvu/technical_rvu
    # split yet — migrations/0045_split_rvu.sql has not been applied on this
    # DB), so both derive from the same source figure for now. Both are kept
    # so this report's "Revenue Capture" totals stay numerically consistent
    # with Report 25's — see the summary dict below for why they're never
    # summed into one figure.
    df["clinical_rvu"] = df["rvu"]
    df["technical_rvu"] = df["rvu"]

    tat_vals = df.loc[df["total_tat_min"] > 0, "total_tat_min"]

    # Revenue Capture (RVU): reported as two separate totals — clinical
    # (physician) and technical (device/facility) — never summed into one
    # combined figure. Report 25 used to sum clinical_rvu + technical_rvu into
    # a single "total_rvu" KPI, which double-counted the same money as if it
    # were two distinct revenue streams; that bug has been fixed there too
    # (routes/report_25.py get_gold_standard_data()), and this report now uses
    # the same convention so the two reports' RVU figures are directly
    # comparable for the same period/filters instead of silently diverging.
    summary = {
        "total": int(len(df)),
        "work_hours": round(float(df.loc[df["proc_duration"] > 0, "proc_duration"].sum() / 60), 1),
        "clinical_rvu": round(float(df["clinical_rvu"].sum()), 1),
        "technical_rvu": round(float(df["technical_rvu"].sum()), 1),
        "tat_median": round(float(tat_vals.median()), 1) if len(tat_vals) > 0 else 0.0,
        "tat_p25": round(float(tat_vals.quantile(0.25)), 1) if len(tat_vals) > 0 else 0.0,
        "tat_p75": round(float(tat_vals.quantile(0.75)), 1) if len(tat_vals) > 0 else 0.0,
    }

    class_tat = {}
    if "patient_class" in df.columns:
        class_tat = (
            df[df["total_tat_min"] > 0]
            .groupby("patient_class")["total_tat_min"]
            .mean()
            .round(1)
            .to_dict()
        )

    modality_split = []
    if "modality" in df.columns:
        modality_split = [
            {"name": k, "value": int(v)} for k, v in df["modality"].value_counts().items()
        ]

    correlation = []
    pearson_r = None
    scatter_outliers_removed = 0
    raw = df[(df["proc_duration"] > 0) & (df["total_tat_min"] > 0)]
    if not raw.empty:
        mask = _iqr_filter(raw["proc_duration"]) & _iqr_filter(raw["total_tat_min"])
        clean = raw[mask]
        correlation = clean[["proc_duration", "total_tat_min"]].values.tolist()
        scatter_outliers_removed = int((~mask).sum())
        if len(clean) > 2:
            r = clean["proc_duration"].corr(clean["total_tat_min"])
            pearson_r = round(float(r), 3) if pd.notna(r) else None

    # Hourly arrival pattern — a separate query against etl_orders, since
    # scheduled_datetime has no equivalent column in the report_template output.
    # Mirrors report_25.py's get_gold_standard_data() "hourly_patterns" lambda,
    # scoped down to the filters this report actually offers (modality, patient
    # class, LAUMC site).
    hourly_patterns = {}
    try:
        _sec_filters = ""
        if "mod" in params:
            _sec_filters += " AND UPPER(m.modality) = UPPER(:mod)"
        if "p_class" in params:
            _sec_filters += " AND UPPER(s.patient_class) = UPPER(:p_class)"
        if rh_site_id is not None:
            _sec_filters += " AND m.site_id = :rh_site_id"

        hourly_rows = db.session.execute(text(f"""
            SELECT EXTRACT(HOUR FROM o.scheduled_datetime)::int AS hr, COUNT(*) AS cnt
            FROM etl_orders o
            JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_date BETWEEN :start AND :end
              AND o.scheduled_datetime IS NOT NULL
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
              {_sec_filters}
            GROUP BY 1 ORDER BY 1
        """), params).fetchall()
        hourly_patterns = {str(r[0]): int(r[1]) for r in hourly_rows}
    except Exception:
        logger.exception("Report 31: failed to load hourly arrival pattern")
        db.session.rollback()

    data = {
        "summary": summary,
        "class_tat": class_tat,
        "modality_split": modality_split,
        "correlation": correlation,
        "pearson_r": pearson_r,
        "scatter_outliers_removed": scatter_outliers_removed,
        "hourly_patterns": hourly_patterns,
    }
    return data, start, end


@report_31_bp.route("/report/31", methods=["GET", "POST"])
@login_required
def report_31():
    run_report = "start_date" in request.values

    # Loaded asynchronously via /api/filter-options — not blocking here.
    mod_list = class_list = []

    filters = {
        "f_mod_active": request.values.get("f_mod_active") == "on",
        "f_class_active": request.values.get("f_class_active") == "on",
        "mod": request.values.get("f_mod"),
        "p_class": request.values.get("f_class"),
    }

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end = date.today().strftime("%Y-%m-%d")

    data = None

    if run_report:
        from utils.audit import log_event
        log_event("report_run", category="report", resource_type="report_31",
                   detail={"from": request.values.get("start_date"), "to": request.values.get("end_date"),
                           "modality": filters.get("mod"), "class": filters.get("p_class")})

        data, display_start, display_end = get_report_31_data(request.values)

    return render_template(
        "report_31.html",
        data=data, filters=filters, run_report=run_report,
        display_start=display_start, display_end=display_end,
        mod_list=mod_list, class_list=class_list,
    )


@report_31_bp.route("/report/31/export", methods=["POST"])
@login_required
def export_report_31():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, "export")
    if not ok:
        return jsonify({"error": msg}), 403

    base_sql, start_date, end_date, params, extra_where, _rh_site_id = get_report_config(request.form)
    if not base_sql:
        return Response("No query configured.", status=500)

    sql = text(f"""
        WITH base_data AS ({base_sql})
        SELECT study_date, modality, patient_class, total_tat_min, proc_duration, rvu
        FROM base_data
        WHERE 1=1{extra_where}
        ORDER BY study_date DESC
    """)

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Study Date", "Modality", "Patient Class", "Total TAT (min)", "Proc Duration (min)", "RVU"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)
        with db.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(sql, params)
            for row in result:
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)

    filename = f"operations_{start_date}_to_{end_date}.csv"
    return Response(generate(), mimetype="text/csv",
                     headers={"Content-Disposition": f"attachment; filename={filename}"})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(31, report_31_bp, report_31, export_report_31)
