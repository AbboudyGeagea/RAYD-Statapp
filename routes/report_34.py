"""
routes/report_34.py
--------------------
Report 34 — Device Utilization.

Standalone split of Report 25's "Infrastructure" tab into a single-topic base
report (report_25's module docstring covers the original combined dashboard;
docs/LAUMC_NEXT_SESSION.md punch-list #5 is the revamp this split is part of).
Report 25 itself is untouched by this file — it stays live until the split
reports are validated.

Covers: weekly device (AE title) utilization/revenue matrix, best/worst AE by
average TAT, and TAT by modality.

Formerly also carried an "Efficiency Intel" section (modality efficiency score,
stress-vs-output scatter, TAT histogram, RVU-vs-TAT scatter, IQR-based TAT
outlier detection) re-derived from the same utilization/RVU numbers the matrix
computes. Removed entirely per operator instruction — it added no independent
data, just a second analysis pass over the matrix figures. Only the Device
Utilization matrix itself remains.

Data source: the SAME report_sql_query template report_25 uses (report_template
WHERE report_id = 25), wrapped in a CTE selecting only the columns this report
needs — same pattern as report_23.get_report_config() for report_id = 23.

Register in registry.py:
    import routes.report_34
"""
import io
import logging
import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, send_file, jsonify
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from utils.site_resolver import default_site

logger = logging.getLogger("report_34")

report_34_bp = Blueprint("report_34", __name__)


def get_device_utilization_data(form_data):
    """
    Main data-fetch for Report 34. Ports report_25.get_gold_standard_data()'s
    matrix-building block (~lines 342-427) verbatim, trimmed to only what the
    Infrastructure tab actually consumed — everything else (rad_cards, tech
    tab, shift patterns, KPI Detailed Reading, the old "Efficiency Intel"
    re-derived analytics, etc.) belongs either to the other report_25 splits
    or was removed outright, not to this one.

    Returns a 3-tuple: (data_dict | None, start_date_str, end_date_str).
    Returns (None, start, end) when the query produces no rows, so the template
    can render an empty state.
    """
    cached = cache_get(34, form_data)
    if cached is not None:
        return cached

    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")

    params = {"start": start, "end": end}
    where_clauses = ["study_date BETWEEN :start AND :end", "COALESCE(modality, '') NOT IN ('SR', 'OT')"]

    if form_data.get("class_enabled") == "on" and form_data.getlist("patient_class"):
        where_clauses.append("patient_class IN :classes")
        params["classes"] = tuple(form_data.getlist("patient_class"))

    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        where_clauses.append("modality IN :modalities")
        params["modalities"] = tuple(form_data.getlist("modality"))

    if form_data.get("ae_enabled") == "on" and form_data.getlist("aetitle"):
        where_clauses.append("aetitle IN :aetitles")
        params["aetitles"] = tuple(form_data.getlist("aetitle"))

    if form_data.get("loc_enabled") == "on" and form_data.getlist("patient_location"):
        where_clauses.append("patient_location IN :locations")
        params["locations"] = tuple(form_data.getlist("patient_location"))

    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main site) only,
    # SJH (satellite) excluded, for now. etl_didb_studies.site_id is never actually populated
    # (the enrichment pass was designed — migration 0051 — but never built), and raw PACS
    # SITE_ID has a known mislabeling bug (SJH mammo shows as RH), so this resolves site via
    # the device instead: storing_ae -> aetitle_modality_map.site_id, which IS populated
    # (RIS-authoritative, via ORG_STRUCTURE_KEY -> site_org_map, Phase 10). default_site() =
    # the sites row flagged is_default (RH). Resolves to None (filter skipped, not applied)
    # on a non-LAUMC/single-site install rather than zeroing every report. Ported verbatim
    # from report_25.get_gold_standard_data() — see that function for the full history.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        where_clauses.append(
            "aetitle IN (SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE site_id = :rh_site_id)"
        )

    # Secondary filter fragments for the raw SQL query below against etl_didb_studies
    # (prefix "s.") — used by the std_pps actuals lookup for the utilization matrix.
    # Mirrors report_25's _sec_filters / _sec_needs_mod_join pattern exactly.
    _sec_filters = ""
    if "classes" in params:
        _sec_filters += " AND s.patient_class IN :classes"
    if "modalities" in params:
        _sec_filters += " AND UPPER(TRIM(m.modality)) IN :modalities"
    if "aetitles" in params:
        _sec_filters += " AND s.storing_ae IN :aetitles"
    if "locations" in params:
        _sec_filters += " AND s.patient_location IN :locations"
    if rh_site_id is not None:
        _sec_filters += " AND m.site_id = :rh_site_id"
    _sec_needs_mod_join = "modalities" in params or rh_site_id is not None

    # 2. Fetch the SAME SQL template report_25 uses (report_id = 25 — Report 34 has no
    # template of its own; it re-slices the same gold-standard query).
    template_res = db.session.execute(text("SELECT report_sql_query FROM report_template WHERE report_id = 25")).fetchone()
    if not template_res:
        return None, start, end

    # 3. Wrap in a CTE and select only the columns this report needs (report_23's
    # get_report_config() pattern), rather than report_25's "SELECT * FROM (...) sub".
    sql_exec = f"""
        WITH base_data AS ({template_res[0]})
        SELECT aetitle, modality, study_date, patient_class, procedure_code,
               reading_radiologist, total_tat_min, proc_duration, clinical_rvu, technical_rvu
        FROM base_data
        WHERE {' AND '.join(where_clauses)}
    """
    df = pd.DataFrame(db.session.execute(text(sql_exec), params).mappings().all())

    if df.empty:
        logger.warning("Report 34 query returned 0 rows (start=%s end=%s)", start, end)
        return None, start, end

    # 4. Defensive Data Cleaning (same as report_25)
    for col in ['total_tat_min', 'proc_duration', 'clinical_rvu', 'technical_rvu']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(1.0 if col.endswith('_rvu') else 0)
        else:
            logger.warning("Report 34: expected column '%s' missing from query result — defaulting", col)
            df[col] = 1.0 if col.endswith('_rvu') else 0.0

    df['study_date_dt'] = pd.to_datetime(df['study_date'], errors='coerce') if 'study_date' in df.columns else pd.to_datetime(date.today())

    # ── Device Utilization & Revenue Matrix (verbatim port of report_25's matrix loop) ──
    matrix_rows = []
    high_stress = 0
    under_utilized = 0
    no_schedule_count = 0

    if 'aetitle' in df.columns:
        date_range = pd.date_range(start, end)
        weekday_counts = date_range.dayofweek.value_counts().to_dict()

        sched_q = db.session.execute(text("""
            SELECT
                UPPER(TRIM(ws.aetitle)) AS ae,
                ws.day_of_week,
                COALESCE(m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS std_opening_minutes
            FROM device_weekly_schedule ws
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle))
        """)).mappings().all()
        schedule_lookup = {(s['ae'], int(s['day_of_week'])): s['std_opening_minutes'] for s in sched_q}

        # Actual (measured) per-device minutes from std_pps (RIS Performed Procedure
        # Step — start_datetime/end_datetime), where available. Falls back to the
        # procedure_duration_map ESTIMATE per-cell (proc_duration, already in df) when a
        # given AE/weekday has no PPS actuals in range (e.g. non-RIS sites, or dates
        # before the PPS feed started).
        actual_lookup = {}
        try:
            pps_rows = db.session.execute(text(f"""
                SELECT
                    UPPER(TRIM(pps.performing_ae_title)) AS ae,
                    pps.start_datetime,
                    EXTRACT(EPOCH FROM (pps.end_datetime - pps.start_datetime)) / 60 AS mins
                FROM std_pps pps
                JOIN etl_didb_studies s ON s.study_db_uid = pps.study_db_uid
                {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
                WHERE pps.start_datetime BETWEEN :start AND :end
                  AND pps.end_datetime IS NOT NULL
                  AND pps.end_datetime > pps.start_datetime
                  AND pps.performing_ae_title IS NOT NULL
                  AND COALESCE({"m.modality, " if _sec_needs_mod_join else ""}s.study_modality, '') != 'SR'
                  {_sec_filters}
            """), params).mappings().all()
            if pps_rows:
                pps_df = pd.DataFrame(pps_rows)
                pps_df['mins'] = pd.to_numeric(pps_df['mins'], errors='coerce').fillna(0)
                pps_df['weekday'] = pd.to_datetime(pps_df['start_datetime']).dt.weekday
                actual_lookup = pps_df.groupby(['ae', 'weekday'])['mins'].sum().to_dict()
        except Exception:
            logger.exception("Failed to load std_pps actuals for utilization matrix")
            db.session.rollback()

        for ae in sorted(df['aetitle'].unique()):
            ae_upper = str(ae).upper().strip()
            ae_df = df[df['aetitle'] == ae]
            ae_total_load = 0
            ae_total_cap = 0
            ae_days_scheduled = 0  # count of weekdays that actually have a device_weekly_schedule row
            days_util = []

            for i in range(7):
                pps_mins = actual_lookup.get((ae_upper, i))
                if pps_mins is not None:
                    day_load = pps_mins
                else:
                    day_load = ae_df[ae_df['study_date_dt'].dt.weekday == i]['proc_duration'].sum()
                # IMPORTANT: distinguish "no device_weekly_schedule row for this (ae, weekday)"
                # from "a row exists and explicitly says 0 opening minutes" (e.g. a device
                # closed on Sundays). dict.get(..., 0) collapses both to the same number,
                # which made a device with NO schedule configured indistinguishable from one
                # legitimately measured at 0% utilization — every cell silently rendered "0%"
                # instead of surfacing the real "no capacity data" condition.
                sched_mins = schedule_lookup.get((ae_upper, i))
                has_sched = sched_mins is not None
                if has_sched:
                    ae_days_scheduled += 1
                opening_mins = sched_mins if has_sched else 0
                occ = weekday_counts.get(i, 0)
                total_cap = opening_mins * occ

                util = round((day_load / total_cap) * 100, 1) if total_cap > 0 else 0
                days_util.append({"pct": util, "mins": int(day_load), "no_schedule": not has_sched})
                ae_total_load += day_load
                ae_total_cap += total_cap

            ae_no_schedule = ae_days_scheduled == 0  # AE has zero device_weekly_schedule rows at all
            ae_avg = round((ae_total_load / ae_total_cap * 100), 1) if ae_total_cap > 0 else 0
            if ae_no_schedule:
                no_schedule_count += 1
            else:
                # >85% utilisation → device is overloaded (standard capacity-management threshold)
                # <30% utilisation → device is underutilised (less than a third of booked capacity used)
                # Only meaningful once we actually have capacity data for this AE — an AE with no
                # schedule at all shouldn't be silently counted as "healthy" or "under-utilized".
                if ae_avg > 85:
                    high_stress += 1
                elif 0 < ae_avg < 30:
                    under_utilized += 1

            matrix_rows.append({
                "ae": ae, "days": days_util, "avg": ae_avg,
                "total_rvu": round(ae_df['technical_rvu'].sum(), 1),
                "total_cap": ae_total_cap,
                "no_schedule": ae_no_schedule,
            })

    # ── Best vs Worst AE by TAT + Modality TAT (Infrastructure tab charts) ──
    ae_tat = []
    if 'aetitle' in df.columns and 'total_tat_min' in df.columns:
        ae_g = df[df['total_tat_min'] > 0].groupby('aetitle')['total_tat_min'].agg(['mean', 'count']).reset_index()
        ae_g = ae_g[ae_g['count'] >= 5].sort_values('mean')
        ae_tat = [{'ae': r['aetitle'], 'avg_tat': round(float(r['mean']), 1), 'cnt': int(r['count'])} for _, r in ae_g.iterrows()]

    modality_tat = []
    try:
        if 'modality' in df.columns and 'total_tat_min' in df.columns:
            mod_g = df[df['total_tat_min'] > 0].groupby('modality')['total_tat_min'].agg(
                ['mean', 'median', 'count']
            ).reset_index()
            mod_g = mod_g[mod_g['count'] >= 5].sort_values('mean')
            modality_tat = [
                {'mod': r['modality'], 'avg': round(float(r['mean']), 1),
                 'median': round(float(r['median']), 1), 'cnt': int(r['count'])}
                for _, r in mod_g.iterrows()
            ]
    except Exception:
        logger.warning("Failed to build modality TAT breakdown", exc_info=True)

    result = ({
        "summary": {
            "total": len(df),
            "high_stress_count": high_stress,
            "low_util_count": under_utilized,
            # AEs with zero device_weekly_schedule rows in range — utilization could not be
            # computed for these (denominator missing, not measured-and-zero). See report_34.py
            # matrix loop comment for why this is tracked separately from high/low util counts.
            "no_schedule_count": no_schedule_count,
        },
        "matrix": matrix_rows,
        "ae_tat": ae_tat,
        "modality_tat": modality_tat,
        "raw_df": df,
    }, start, end)
    cache_put(34, form_data, result)
    return result


@report_34_bp.route("/report/34", methods=["GET", "POST"])
@login_required
def report_34():
    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end = date.today().strftime("%Y-%m-%d")

    run_report = 'start_date' in request.values
    display_start = request.values.get("start_date", display_start)
    display_end = request.values.get("end_date", display_end)

    filters = {
        "class_enabled": request.values.get("class_enabled") == "on",
        "mod_enabled":   request.values.get("mod_enabled") == "on",
        "ae_enabled":    request.values.get("ae_enabled") == "on",
        "loc_enabled":   request.values.get("loc_enabled") == "on",
        "p_class": request.values.get("patient_class"),
        "mod":     request.values.get("modality"),
        "ae":      request.values.get("aetitle"),
        "loc":     request.values.get("patient_location"),
    }

    data = None
    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_34',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date')})
        data, display_start, display_end = get_device_utilization_data(request.values)

    template_data = {k: v for k, v in data.items() if k != 'raw_df'} if data else None

    return render_template(
        "report_34.html",
        data=template_data,
        display_start=display_start,
        display_end=display_end,
        run_report=run_report,
        filters=filters,
    )


@report_34_bp.route("/report/34/export", methods=["POST"])
@login_required
def export_report_34():
    from flask import current_app
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    data, _, _ = get_device_utilization_data(request.values)
    if not data:
        return "Error", 400
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['raw_df'].drop(columns=['study_date_dt'], errors='ignore').to_excel(writer, index=False, sheet_name='RawData')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"RAYD_DeviceUtilization_{date.today()}.xlsx")


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(34, report_34_bp, report_34, export_report_34)
