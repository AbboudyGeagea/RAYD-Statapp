"""
routes/report_25.py
-------------------
Report 25 — Device & Radiologist Performance Dashboard.

Covers: AE-station utilisation matrix and radiologist RVU/TAT performance cards.

The heavy data-fetch (get_gold_standard_data) runs synchronously and is
cached for 5 minutes (report_cache).

The Technicians tab (HL7-order-based technician compliance monitoring, its
background /report/25/bg endpoint, the flag-acknowledgement API, and the
"Daily Technician TAT" live-status panel) was removed 2026-07-31 (operator
instruction) -- it depended on hl7_orders.scheduled_datetime, which is empty
because the R2I HL7 ORM feed isn't flowing yet, so the tab only ever showed
empty states. See git history if that feed goes live and this needs rebuilding.

Register in registry.py:
    import routes.report_25
"""
import json
import logging
import pandas as pd
import io
from datetime import date
from datetime import datetime
from flask import Blueprint, render_template, request, send_file, url_for, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from utils.site_resolver import default_site
from utils.report_filters import sidebar_filters as _sidebar_filters

logger = logging.getLogger("report_25")

report_25_bp = Blueprint("report_25", __name__)

def _load_shift_config():
    defaults = {'morning_start': 7, 'morning_end': 15,
                'afternoon_start': 15, 'afternoon_end': 23,
                'night_start': 23, 'night_end': 7}
    try:
        rows = db.session.execute(text(
            "SELECT key, value FROM settings WHERE key LIKE 'shift_%'"
        )).fetchall()
        for key, val in rows:
            k = key.replace('shift_', '')
            if k in defaults:
                defaults[k] = int(val)
    except Exception:
        pass
    return defaults


# _sidebar_filters (date/class/modality/AE/location + RH site scope) now lives in
# utils/report_filters.py, imported above -- shared with routes/report_36.py so both
# report modules stay in sync on what "follow the report's left filters" means.



def _tat_anchor_result_shell():
    return {"summary": {"n": 0, "avg_tat_h": None, "median_tat_h": None}, "matrix": [], "trend": []}


_TAT_ANCHOR_SUMMARY_SQL = """
    SELECT
        COUNT(*) AS n,
        ROUND(AVG(tat_hours)::numeric, 2) AS avg_tat_h,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours))::numeric, 2) AS median_tat_h
    FROM tat
"""
_TAT_ANCHOR_MATRIX_SQL = """
    SELECT
        modality, patient_class_bucket,
        COUNT(*) AS n,
        ROUND(AVG(tat_hours)::numeric, 2) AS avg_tat_h,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours))::numeric, 2) AS median_tat_h,
        COUNT(*) FILTER (WHERE tat_hours <= 3)                   AS bucket_0_3h,
        COUNT(*) FILTER (WHERE tat_hours > 3 AND tat_hours <= 5) AS bucket_3_5h,
        COUNT(*) FILTER (WHERE tat_hours > 5)                    AS bucket_5h_plus
    FROM tat
    GROUP BY modality, patient_class_bucket
    ORDER BY modality, patient_class_bucket
"""
_TAT_ANCHOR_TREND_SQL = """
    SELECT
        study_date AS day,
        COUNT(*) AS n,
        ROUND(AVG(tat_hours)::numeric, 2) AS avg_tat_h,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours))::numeric, 2) AS median_tat_h
    FROM tat
    GROUP BY study_date
    ORDER BY study_date
"""


def _run_tat_anchor_queries(base_cte, params, log_label):
    result = _tat_anchor_result_shell()
    try:
        summary_row = db.session.execute(text(base_cte + _TAT_ANCHOR_SUMMARY_SQL), params).mappings().fetchone()
        if summary_row:
            result["summary"] = dict(summary_row)

        matrix_rows = db.session.execute(text(base_cte + _TAT_ANCHOR_MATRIX_SQL), params).mappings().fetchall()
        result["matrix"] = [dict(r) for r in matrix_rows]

        trend_rows = db.session.execute(text(base_cte + _TAT_ANCHOR_TREND_SQL), params).mappings().fetchall()
        result["trend"] = [
            {**dict(r), "day": r["day"].strftime("%Y-%m-%d") if r["day"] else None}
            for r in trend_rows
        ]
    except Exception:
        logger.exception(f"Failed to compute {log_label} TAT")
        db.session.rollback()
    return result


def get_tat_pacs_insert_time(form_data):
    """
    TAT (insert -> signed) anchored on PACS etl_didb_studies.insert_time — the
    ingestion timestamp PACS itself stamps on the study row (ETL_JOBS/etl_didb_studies.py,
    Oracle DIDB_STUDIES.INSERT_TIME), NOT the ETL's own last_update sync time.

    Companion to get_tat_ris_exam_done — same shape (summary/matrix/trend), a
    different start-of-clock anchor, for the "PACS vs RIS" TAT comparison tab.
    This is the anchor validated 2026-07-31 as the fix for report_template's
    study_date-is-midnight TAT bug (see project memory), reused here per-study
    for a fuller breakdown than report_template exposes.

    RH only (same site-scope reasoning as get_gold_standard_data: raw PACS
    SITE_ID has a known SJH mislabeling bug, so site comes from the device via
    aetitle_modality_map, not etl_didb_studies directly) — SJH is excluded per
    operator instruction (2026-07-31): SJH images route through a PACS-side
    middleware/gateway before landing in the PACS DB this app queries, so
    PACS-side timestamps for SJH aren't trustworthy from here, and SJH itself
    is phase 2 (out of scope for now). SJH TAT calculations belong on the SJH
    PACS server directly, not this comparison.

    Full left-sidebar filter set applies (see _sidebar_filters) — every chart on
    this page must follow date range/class/modality/AE/location, not just dates.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)

    base_cte = f"""
        WITH tat AS (
            SELECT
                s.study_db_uid,
                s.study_date,
                COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                CASE
                    WHEN s.patient_location = 'ER' THEN 'ER'
                    WHEN s.patient_class = 'I' THEN 'Inpatient'
                    WHEN s.patient_class = 'O' THEN 'Outpatient'
                    ELSE 'Other'
                END AS patient_class_bucket,
                EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 3600.0 AS tat_hours
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            WHERE s.study_date BETWEEN :start AND :end
              AND s.insert_time IS NOT NULL
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) > s.insert_time
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
              {filter_clause}
        )
    """
    return _run_tat_anchor_queries(base_cte, params, "PACS insert_time")


def get_tat_ris_exam_done(form_data):
    """
    TAT (exam done -> signed) anchored on the RIS's own "Exam Done" status
    transition — WORKLIST_STATUS_HISTORY status_key=100, ETL'd via
    ETL_JOBS/etl_ris_worklist_exam_done.py into std_worklist_exam_done, joined
    through std_pps.pps_key -> study_instance_uid -> etl_didb_studies (same
    join path routes/report_36.py's get_patient_wait_time uses for arrivals).

    Companion to get_tat_pacs_insert_time — same shape, a RIS-side anchor
    instead of the PACS insert_time proxy. Same RH-only scope and SJH
    exclusion reasoning (see that function's docstring) — RIS is LAUMC-wide
    (both RH and SJH), so the site filter still needs to apply here too.

    Full left-sidebar filter set applies (see _sidebar_filters).
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)

    base_cte = f"""
        WITH exam_done_ris AS (
            SELECT p.study_instance_uid, MAX(ed.exam_done_at) AS exam_done_time
            FROM std_worklist_exam_done ed
            JOIN std_pps p ON p.pps_key = ed.pps_key
            WHERE p.study_instance_uid IS NOT NULL
            GROUP BY p.study_instance_uid
        ),
        tat AS (
            SELECT
                s.study_db_uid,
                s.study_date,
                COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                CASE
                    WHEN s.patient_location = 'ER' THEN 'ER'
                    WHEN s.patient_class = 'I' THEN 'Inpatient'
                    WHEN s.patient_class = 'O' THEN 'Outpatient'
                    ELSE 'Other'
                END AS patient_class_bucket,
                EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - ed.exam_done_time)) / 3600.0 AS tat_hours
            FROM etl_didb_studies s
            JOIN exam_done_ris ed ON ed.study_instance_uid = s.study_instance_uid
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) > ed.exam_done_time
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
              {filter_clause}
        )
    """
    return _run_tat_anchor_queries(base_cte, params, "RIS exam_done")


def get_gold_standard_data(form_data):
    """
    Main data-fetch for Report 25.  Runs the SQL template from the DB,
    applies active filters, and computes all KPI structures returned to
    the template and the background endpoint.

    Returns a 3-tuple: (data_dict | None, start_date_str, end_date_str).
    Returns (None, start, end) when the query produces no rows, so callers
    can render an appropriate empty state.
    """
    # Cache hit check — skip full DB scan for identical re-runs within 5 min
    cached = cache_get(25, form_data)
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
    # SITE_ID has a known mislabeling bug (SJH mammo shows as RH — see 0051's comment), so this
    # resolves site via the device instead: storing_ae -> aetitle_modality_map.site_id, which
    # IS populated (RIS-authoritative, via ORG_STRUCTURE_KEY -> site_org_map, Phase 10).
    # default_site() = the sites row flagged is_default, which is RH. Resolves to None (filter
    # skipped, not applied) on a non-LAUMC/single-site install rather than zeroing every report.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        where_clauses.append(
            "aetitle IN (SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE site_id = :rh_site_id)"
        )

    # Build secondary filter fragments for raw SQL queries against etl_didb_studies (prefix "s.")
    # aetitle_modality_map is joined whenever a modality filter OR the site rule needs it —
    # every _sec_filters consumer below must therefore use the join unconditionally once either
    # is active (mirrors _sec_needs_mod_join's existing per-query conditional-join pattern).
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
    # Whether secondary queries need the modality JOIN
    _sec_needs_mod_join = "modalities" in params or rh_site_id is not None

    # 2. Fetch SQL Template
    template_res = db.session.execute(text("SELECT report_sql_query FROM report_template WHERE report_id = 25")).fetchone()
    if not template_res:
        return None, start, end

    # 3. Execute Query
    sql_exec = f"SELECT * FROM ({template_res[0]}) as sub WHERE {' AND '.join(where_clauses)}"
    df = pd.DataFrame(db.session.execute(text(sql_exec), params).mappings().all())
    
    if df.empty:
        logger.warning("Report 25 query returned 0 rows (start=%s end=%s)", start, end)
        return None, start, end

    # 4. Defensive Data Cleaning
    # backward compat: old template still emits a single 'rvu' column (verified
    # against report_template.report_id=25 directly — as of this fix the live
    # query still returns COALESCE(pm.rvu_value, 1.0) AS rvu, no clinical_rvu/
    # technical_rvu split; procedure_duration_map has no such columns either —
    # migrations/0045_split_rvu.sql has not been applied on this DB). This MUST
    # run before the generic column-default loop below: that loop's "column
    # missing from query result" branch used to run first and permanently stomp
    # clinical_rvu/technical_rvu with a flat 1.0 for every row (since the query
    # never emits those names), which made this shim's own guard
    # ('clinical_rvu' not in df.columns) always false — i.e. the shim could
    # never fire and every RVU-derived figure (matrix_rows/rad_cards/summary)
    # silently degraded into a row count instead of real RVU. Do the split
    # first so real per-study RVU values propagate everywhere downstream.
    if 'rvu' in df.columns and 'clinical_rvu' not in df.columns:
        df['clinical_rvu']  = pd.to_numeric(df['rvu'], errors='coerce').fillna(1.0)
        df['technical_rvu'] = df['clinical_rvu']

    for col in ['total_tat_min', 'proc_duration', 'clinical_rvu', 'technical_rvu']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(1.0 if col.endswith('_rvu') else 0)
        else:
            logger.warning("Report 25: expected column '%s' missing from query result — defaulting", col)
            df[col] = 1.0 if col.endswith('_rvu') else 0.0
    
    df['study_date_dt'] = pd.to_datetime(df['study_date'], errors='coerce') if 'study_date' in df.columns else pd.to_datetime(date.today())

    # --- Metrics Generation ---
    matrix_rows = []
    high_stress = 0
    under_utilized = 0
    total_active_mins = df.loc[df['proc_duration'] > 0, 'proc_duration'].sum()

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
        # procedure_duration_map ESTIMATE per-cell when a given AE/weekday has no PPS
        # actuals in range (e.g. non-RIS sites, or dates before the PPS feed started).
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
            days_util = []

            for i in range(7):
                pps_mins = actual_lookup.get((ae_upper, i))
                if pps_mins is not None:
                    day_load = pps_mins
                else:
                    day_load = ae_df[ae_df['study_date_dt'].dt.weekday == i]['proc_duration'].sum()
                opening_mins = schedule_lookup.get((ae_upper, i), 0)
                occ = weekday_counts.get(i, 0)
                total_cap = opening_mins * occ
                
                util = round((day_load / total_cap) * 100, 1) if total_cap > 0 else 0
                days_util.append({"pct": util, "mins": int(day_load)})
                ae_total_load += day_load
                ae_total_cap += total_cap

            ae_avg = round((ae_total_load / ae_total_cap * 100), 1) if ae_total_cap > 0 else 0
            # >85% utilisation → device is overloaded (standard capacity-management threshold)
            # <30% utilisation → device is underutilised (less than a third of booked capacity used)
            if ae_avg > 85:
                high_stress += 1
            elif 0 < ae_avg < 30:
                under_utilized += 1

            matrix_rows.append({
                "ae": ae, "days": days_util, "avg": ae_avg,
                "total_rvu": round(ae_df['technical_rvu'].sum(), 1),
                "total_cap": ae_total_cap,
            })

    # TAT percentiles for the whole dataset
    tat_vals_all = df[df['total_tat_min'] > 0]['total_tat_min'] if 'total_tat_min' in df.columns else pd.Series([], dtype=float)
    tat_median = round(float(tat_vals_all.median()), 1) if len(tat_vals_all) > 0 else 0.0
    tat_p25    = round(float(tat_vals_all.quantile(0.25)), 1) if len(tat_vals_all) > 0 else 0.0
    tat_p75    = round(float(tat_vals_all.quantile(0.75)), 1) if len(tat_vals_all) > 0 else 0.0

    # Apply physician alias mapping so migrated name variants collapse to canonical
    try:
        from utils.physician_aliases import get_alias_dict as _get_aliases
        _alias_dict = _get_aliases()
        if _alias_dict and 'reading_radiologist' in df.columns:
            df['reading_radiologist'] = df['reading_radiologist'].map(
                lambda x: _alias_dict.get(x, x) if x else x
            )
    except Exception:
        pass

    # Rad Performance — exclude SR and OT; only count studies with a final report
    rad_cards = []
    if 'reading_radiologist' in df.columns:
        _excl_mask = df['modality'].str.upper().isin(['SR', 'OT']) if 'modality' in df.columns else pd.Series(False, index=df.index)
        _df_rads = df[~_excl_mask]
        # Only count studies that have a final report (rep_final_timestamp is not null)
        if 'rep_final_timestamp' in _df_rads.columns:
            _df_rads = _df_rads[_df_rads['rep_final_timestamp'].notna()]
        elif 'study_has_report' in _df_rads.columns:
            _df_rads = _df_rads[_df_rads['study_has_report'] == True]
        for rad, r_df in _df_rads.groupby('reading_radiologist'):
            drill = []
            loc_col = 'patient_location' if 'patient_location' in df.columns else 'modality'
            for loc, l_df in r_df.groupby(loc_col):
                mods = [{"m": m, "avg": round(m_df['total_tat_min'].mean(), 1), "count": len(m_df), "rvu": round(m_df['clinical_rvu'].sum(), 1)} for m, m_df in l_df.groupby('modality')]
                drill.append({"loc": loc, "mods": mods, "loc_rvu": round(l_df['clinical_rvu'].sum(), 1)})

            # Only count studies with a mapped duration — unmapped studies (0 min)
            # would contribute RVU without time, inflating the rate
            r_df_mapped = r_df[r_df['proc_duration'] > 0]
            total_scan_hours = r_df_mapped['proc_duration'].sum() / 60
            rvu_per_hour = round(r_df_mapped['clinical_rvu'].sum() / total_scan_hours, 2) if total_scan_hours > 0 else 0.0

            r_df_valid = r_df[r_df['total_tat_min'] > 0]
            rad_cards.append({
                "name": rad,
                "count": int(len(r_df)),
                "overall": round(r_df_valid['total_tat_min'].mean(), 1) if len(r_df_valid) > 0 else 0.0,
                "tat_median": round(float(r_df[r_df['total_tat_min'] > 0]['total_tat_min'].median()), 1) if (r_df['total_tat_min'] > 0).any() else 0.0,
                "total_rvu": round(r_df['clinical_rvu'].sum(), 1),
                "rvu_per_hour": rvu_per_hour,
                "drilldown": drill
            })

        # Add percentile rank among peers (lower TAT = better = lower percentile)
        peer_tats = sorted([r['overall'] for r in rad_cards if r['overall'] > 0])
        n = len(peer_tats)
        for r in rad_cards:
            if r['overall'] > 0 and n > 0:
                rank = sum(1 for t in peer_tats if t <= r['overall'])
                r['tat_percentile'] = round(rank / n * 100)
            else:
                r['tat_percentile'] = None

    # Technician TAT by AE Station — group df by aetitle, exclude SR/OT
    tech_tat_cards   = []
    if 'aetitle' in df.columns and 'total_tat_min' in df.columns and 'modality' in df.columns:
        try:
            _excl_mask_ae = df['modality'].str.upper().isin(['SR', 'OT'])
            tat_df = df[~_excl_mask_ae & (df['total_tat_min'] > 0)].copy()
            # Split normal vs outliers (> 24h = 1440 min) — outliers just excluded from
            # the per-AE averages below, no longer surfaced as a row-per-study list (was
            # unbounded — 141k+ rows on LAUMC's post-migration-0070 data, rendered as one
            # HTML table row each).
            normal_df  = tat_df[tat_df['total_tat_min'] <= 1440]

            for ae_title, ae_grp in normal_df.groupby('aetitle'):
                tat_series = ae_grp['total_tat_min']
                tech_tat_cards.append({
                    'aetitle': ae_title,
                    'count': int(len(ae_grp)),
                    'avg_tat': round(float(tat_series.mean()), 1),
                    'median_tat': round(float(tat_series.median()), 1),
                })
            tech_tat_cards.sort(key=lambda x: x['avg_tat'])
        except Exception:
            logger.warning("Failed to build tech TAT cards", exc_info=True)

    # shift_patterns and ts_rows are deferred to /report/25/bg (background endpoint)
    shift_patterns = {}
    ts_rows = []

    # ── New analytics ─────────────────────────────────────────────────
    tat_hist, ae_tat, rvu_tat, outlier_studies, global_mean_tat = [], [], [], [], 0.0

    if 'total_tat_min' in df.columns:
        tat_vals = df[df['total_tat_min'] > 0]['total_tat_min']
        global_mean_tat = round(float(tat_vals.mean()), 1) if len(tat_vals) > 0 else 0.0
        bins   = [0, 30, 60, 90, 120, 180, 240, 360]
        labels = ['0-30m', '31-60m', '61-90m', '91-120m', '121-180m', '181-240m', '241-360m', '360m+']
        for i, label in enumerate(labels):
            lo = bins[i]
            hi = bins[i + 1] if i < len(bins) - 1 else float('inf')
            tat_hist.append({'label': label, 'count': int(((tat_vals > lo) & (tat_vals <= hi)).sum())})

        if 'aetitle' in df.columns:
            ae_g = df[df['total_tat_min'] > 0].groupby('aetitle')['total_tat_min'].agg(['mean', 'count']).reset_index()
            ae_g = ae_g[ae_g['count'] >= 5].sort_values('mean')
            ae_tat = [{'ae': r['aetitle'], 'avg_tat': round(float(r['mean']), 1), 'cnt': int(r['count'])} for _, r in ae_g.iterrows()]

        # IQR-based outlier threshold (robust to skew, unlike mean*2)
        q1_tat, q3_tat = tat_vals.quantile(0.25), tat_vals.quantile(0.75)
        iqr_tat = q3_tat - q1_tat
        threshold = q3_tat + 1.5 * iqr_tat
        out_cols = [c for c in ['aetitle', 'modality', 'reading_radiologist', 'patient_class', 'procedure_code', 'study_date', 'total_tat_min'] if c in df.columns]
        out_df = df[df['total_tat_min'] > threshold][out_cols].sort_values('total_tat_min', ascending=False).head(50)
        for row in out_df.to_dict('records'):
            if 'study_date' in row and hasattr(row['study_date'], 'strftime'):
                row['study_date'] = str(row['study_date'])[:10]
            if 'total_tat_min' in row:
                row['total_tat_min'] = round(float(row['total_tat_min']), 1)
            outlier_studies.append(row)

    # IQR-based outlier filter for scatter plots
    def _iqr_filter(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        return (series >= q1 - 1.5 * iqr) & (series <= q3 + 1.5 * iqr)

    scatter_outliers_removed = 0
    if 'proc_duration' in df.columns and 'total_tat_min' in df.columns:
        raw = df[(df['proc_duration'] > 0) & (df['total_tat_min'] > 0)]
        mask = _iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])
        scatter_outliers_removed = int((~mask).sum())

    # RVU-vs-TAT scatter: read 'clinical_rvu' (populated above for both the
    # legacy single-'rvu'-column query and any future real clinical/technical
    # split — see the defensive-cleaning comment above), not the raw 'rvu'
    # column directly. Reading 'rvu' here was a latent trap: it happened to
    # work today only because the live query still emits that legacy alias,
    # but the moment migrations/0045_split_rvu.sql is actually applied and the
    # template starts emitting clinical_rvu/technical_rvu instead, 'rvu' stops
    # existing and this chart (and its outlier badge) would silently go dead.
    rvu_outliers_removed = 0
    if 'clinical_rvu' in df.columns and 'total_tat_min' in df.columns:
        tmp_raw = df[(df['clinical_rvu'] > 0) & (df['total_tat_min'] > 0)]
        mask_rvu = _iqr_filter(tmp_raw['clinical_rvu']) & _iqr_filter(tmp_raw['total_tat_min'])
        rvu_outliers_removed = int((~mask_rvu).sum())
        tmp = tmp_raw[mask_rvu][['clinical_rvu', 'total_tat_min']]
        rvu_tat = [[round(float(r[0]), 2), round(float(r[1]), 1)] for r in tmp.values.tolist()]

    # TAT by modality (from existing df)
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

    # Unread study aging buckets
    unread_aging = []
    try:
        aging_rows = db.session.execute(text(f"""
            SELECT
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 24 THEN '0-24h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 48 THEN '24-48h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 72 THEN '48-72h'
                    ELSE '72h+'
                END AS bucket,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_status ILIKE '%unread%'
              AND s.study_date BETWEEN :start AND :end
              {_sec_filters}
            GROUP BY 1, 2
            ORDER BY
                CASE bucket WHEN '0-24h' THEN 1 WHEN '24-48h' THEN 2 WHEN '48-72h' THEN 3 ELSE 4 END
        """), params).fetchall()
        for bucket, modality, cnt in aging_rows:
            unread_aging.append({'bucket': bucket, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        logger.exception("Failed to load unread aging buckets")
        db.session.rollback()

    # Studies per shift
    shift_breakdown = []
    try:
        sc = _load_shift_config()
        shift_rows = db.session.execute(text(f"""
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :ms AND EXTRACT(HOUR FROM o.scheduled_datetime) < :me THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :as AND EXTRACT(HOUR FROM o.scheduled_datetime) < :ae THEN 'Afternoon'
                    ELSE 'Night'
                END AS shift,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_orders o
            JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_date BETWEEN :start AND :end
              AND o.scheduled_datetime IS NOT NULL
              {_sec_filters}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """), {**params,
            "ms": sc['morning_start'],   "me": sc['morning_end'],
            "as": sc['afternoon_start'], "ae": sc['afternoon_end'],
        }).fetchall()
        for shift, modality, cnt in shift_rows:
            shift_breakdown.append({'shift': shift, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        logger.exception("Failed to load shift breakdown")
        db.session.rollback()

    # Addendum rate by radiologist
    addendum_data = {'overall_pct': 0.0, 'by_rad': []}
    try:
        add_rows = db.session.execute(text(f"""
            SELECT
                COALESCE(pam.canonical_name, s.rep_final_signed_by, 'Unknown') AS radiologist,
                COUNT(*) AS total,
                SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END) AS addendum_count,
                ROUND(SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS addendum_pct
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
            LEFT JOIN physician_alias_map pam ON pam.dismissed = false AND pam.alias = s.rep_final_signed_by
            WHERE s.study_date BETWEEN :start AND :end
              AND s.rep_final_signed_by IS NOT NULL
              AND s.rep_final_timestamp IS NOT NULL
              {_sec_filters}
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY addendum_pct DESC
        """), params).fetchall()
        by_rad = [
            {'rad': r[0], 'total': int(r[1]), 'addendum_count': int(r[2]), 'pct': float(r[3] or 0)}
            for r in add_rows
        ]
        total_studies = sum(r['total'] for r in by_rad)
        total_addenda = sum(r['addendum_count'] for r in by_rad)
        overall_pct = round(total_addenda / total_studies * 100, 1) if total_studies > 0 else 0.0
        addendum_data = {'overall_pct': overall_pct, 'total_addenda': total_addenda, 'by_rad': by_rad}
    except Exception:
        logger.exception("Failed to load addendum rate data")
        db.session.rollback()

    # ── Reports per radiologist × modality / AE title / procedure ────────
    # Field fixed 2026-07-31 (operator instruction): was keyed off
    # rep_final_timestamp / rep_final_signed_by / signing_physician_*, all PACS-side
    # fields already established as unreliable/sparse at LAUMC (radiologists sign in
    # RIS, not PACS) -- this was silently undercounting every study whose PACS-side
    # signature fields never got populated even though it was really signed via RIS.
    # Now keyed off rep_study_last_composed_by/_ts, the RIS field validated all
    # night as the reliable one on this install.
    rad_volume_matrix = {"by_modality": [], "by_aetitle": [], "by_procedure": [], "by_month": [], "roles": {}}
    try:
        _RAD25_BASE = "COALESCE(NULLIF(TRIM(s.rep_study_last_composed_by),''),'Unknown')"
        _PAM25 = ("LEFT JOIN physician_alias_map pam "
                  "ON pam.dismissed = false "
                  "AND pam.alias = NULLIF(TRIM(s.rep_study_last_composed_by),'')")
        _RAD25 = "COALESCE(pam.canonical_name, " + _RAD25_BASE + ")"
        _RAD25_OK = (f"AND {_RAD25} NOT IN ('','Unknown')"
                     f" AND s.rep_study_last_composed_ts IS NOT NULL")
        _MJ25 = "LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))"

        rad_volume_matrix["by_modality"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD25} AS radiologist,
                   COALESCE(UPPER(m.modality), 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {_MJ25} {_PAM25}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {_sec_filters} {_RAD25_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """), params).mappings().fetchall()]

        rad_volume_matrix["by_aetitle"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD25} AS radiologist,
                   COALESCE(s.storing_ae, 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
            {_PAM25}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(s.study_modality, '') != 'SR'
              {_sec_filters} {_RAD25_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """), params).mappings().fetchall()]

        rad_volume_matrix["by_procedure"] = [dict(r) for r in db.session.execute(text(f"""
            WITH top_procs AS (
                SELECT s.procedure_code
                FROM etl_didb_studies s
                {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
                WHERE s.study_date BETWEEN :start AND :end
                  AND s.rep_study_last_composed_ts IS NOT NULL
                  AND s.procedure_code IS NOT NULL AND s.procedure_code != ''
                  {_sec_filters}
                GROUP BY 1 ORDER BY COUNT(DISTINCT s.study_db_uid) DESC LIMIT 60
            )
            SELECT {_RAD25} AS radiologist,
                   s.procedure_code AS proc,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
            {_PAM25}
            JOIN top_procs tp ON tp.procedure_code = s.procedure_code
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(s.study_modality, '') != 'SR'
              {_sec_filters} {_RAD25_OK}
            GROUP BY 1, 2 ORDER BY 2, 3 DESC
        """), params).mappings().fetchall()]
        rad_volume_matrix["by_month"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD25} AS radiologist,
                   TO_CHAR(s.study_date, 'YYYY-MM') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {_MJ25} {_PAM25}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {_sec_filters} {_RAD25_OK}
            GROUP BY 1, 2 ORDER BY 1, 2
        """), params).mappings().fetchall()]

        # Real role per radiologist (residents/radiologists), from PACS reading-
        # permission group membership (std_pacs_user_groups) -- not RIS's unreliable
        # resource_role_key (see etl_ris_resources.py's docstring). Matched on the raw
        # login before alias canonicalization, since std_pacs_user_groups.login_id is
        # a username, not a display name.
        role_rows = db.session.execute(text(f"""
            WITH role_lookup AS (
                SELECT DISTINCT UPPER(login_id) AS login_id, group_name AS role
                FROM std_pacs_user_groups
                WHERE group_name IN ('radiologists', 'residents')
            )
            SELECT DISTINCT {_RAD25} AS radiologist, rl.role
            FROM etl_didb_studies s {_MJ25} {_PAM25}
            LEFT JOIN role_lookup rl
                ON rl.login_id = SPLIT_PART(UPPER(TRIM(s.rep_study_last_composed_by)), '@', 1)
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {_sec_filters} {_RAD25_OK}
        """), params).mappings().fetchall()
        for r in role_rows:
            if r["role"]:
                rad_volume_matrix["roles"][r["radiologist"]] = r["role"]
    except Exception:
        logger.exception("Failed to build radiologist × modality/AE/procedure volume matrix")
        db.session.rollback()

    rad_insights  = []

    result = ({
        "summary": {
            "total": len(df), "global_util": f"{(sum(r['avg'] * r.get('total_cap', 1) for r in matrix_rows) / sum(r.get('total_cap', 1) for r in matrix_rows) if matrix_rows and sum(r.get('total_cap', 1) for r in matrix_rows) > 0 else 0):.1f}%",
            "er_wait": f"{df[df['accession_number'].str.upper().str.startswith('2XE').fillna(False) & (df['total_tat_min'] > 0)]['total_tat_min'].mean():.1f}m" if 'accession_number' in df.columns and (df['accession_number'].str.upper().str.startswith('2XE').fillna(False) & (df['total_tat_min'] > 0)).any() else "0m",
            "high_stress_count": high_stress, "low_util_count": under_utilized,
            "work_hours": round(total_active_mins / 60, 1),
            # Revenue Capture (RVU) — clinical (physician) and technical
            # (device/facility) RVU are two distinct revenue streams and are
            # reported as two separate totals; they must NEVER be summed into
            # one "total_rvu" figure — that would double-count the same money
            # as if it were two different revenues. (Ground truth, verified
            # against report_template.report_id=25 directly: the live query
            # still emits a single legacy 'rvu' column — no real clinical/
            # technical split exists yet in procedure_duration_map, see
            # migrations/0045_split_rvu.sql, which has not been applied on
            # this DB — so both totals below currently derive from the same
            # source number. That's an honest reflection of the data as it
            # exists today, not a fabricated split; if/when a real split is
            # added to procedure_duration_map these two totals will diverge
            # automatically with no further code changes needed here.)
            "clinical_rvu": round(df['clinical_rvu'].sum(), 1),
            "technical_rvu": round(df['technical_rvu'].sum(), 1),
            "tat_median": tat_median, "tat_p25": tat_p25, "tat_p75": tat_p75,
        },
        "matrix": matrix_rows, 
        "class_tat": df[df['total_tat_min'] > 0].groupby('patient_class')['total_tat_min'].mean().round(1).to_dict() if 'patient_class' in df.columns else {},
        "rad_cards": rad_cards,
        "tech_tat_cards": tech_tat_cards,
        "modality_split": [{"name": k, "value": int(v)} for k, v in df['modality'].value_counts().items()] if 'modality' in df.columns else [], 
        "hourly_patterns": (lambda: {
            str(r[0]): int(r[1])
            for r in db.session.execute(text(f"""
                SELECT EXTRACT(HOUR FROM o.scheduled_datetime)::int AS hr, COUNT(*) AS cnt
                FROM etl_orders o
                JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
                {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
                WHERE s.study_date BETWEEN :start AND :end
                  AND o.scheduled_datetime IS NOT NULL
                  {_sec_filters}
                GROUP BY 1 ORDER BY 1
            """), params).fetchall()
        })(),
        "correlation": (lambda: (
            lambda raw: raw[_iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])][['proc_duration','total_tat_min']].values.tolist()
        )(df[(df['proc_duration']>0)&(df['total_tat_min']>0)]) if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else [])(),
        "pearson_r": (lambda: (
            lambda clean: round(clean['proc_duration'].corr(clean['total_tat_min']), 3)
            if len(clean) > 2 else None
        )((lambda raw: raw[_iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])])(
            df[(df['proc_duration']>0)&(df['total_tat_min']>0)]
        )) if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else None)(),
        "scatter_outliers_removed": scatter_outliers_removed,
        "rvu_outliers_removed": rvu_outliers_removed,
        "raw_df": df,
        "tat_hist": tat_hist,
        "ae_tat": ae_tat,
        "rvu_tat": rvu_tat,
        "outlier_studies": outlier_studies,
        "global_mean_tat": global_mean_tat,
        "modality_tat":    modality_tat,
        "unread_aging":    unread_aging,
        "shift_breakdown": shift_breakdown,
        "addendum_data":      addendum_data,
        "rad_volume_matrix":  rad_volume_matrix,
        "shift_patterns":  shift_patterns,
        "rad_insights":    rad_insights,
    }, start, end)
    cache_put(25, form_data, result)
    return result


@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    # Filter options are loaded asynchronously via /api/filter-options after
    # page render — do NOT query here, as DISTINCT on etl_didb_studies blocks
    # the entire page load.
    classes = locations = modalities = aetitles = []
    
    tree_raw = db.session.execute(text("SELECT modality, aetitle FROM aetitle_modality_map")).all()
    tree_dict = {}
    for mod, ae in tree_raw:
        if mod not in tree_dict: tree_dict[mod] = []
        tree_dict[mod].append({"name": ae})
    tree_json = json.dumps({"name": "FLEET", "children": [{"name": k, "children": v} for k, v in tree_dict.items()]})

    shift_config = _load_shift_config()
    run_report = 'start_date' in request.values
    active_tab = request.values.get("active_tab", "ops")

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    data           = None
    journey_json   = None
    template_data  = None
    tat_pacs_data  = None
    tat_ris_data   = None

    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_25',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date'),
                          'tab': active_tab})
        data, display_start, display_end = get_gold_standard_data(request.values)
        tat_pacs_data = get_tat_pacs_insert_time(request.values)
        tat_ris_data = get_tat_ris_exam_done(request.values)

        # NOTE: Patient Journey used to be built inline here from a `fallback_id`
        # request param, joining etl_didb_studies to etl_patient_view on
        # fallback_id and estimating a "true entry" time as
        # insert_time - proc_duration (proc_duration falling back to a generic
        # 15-minute default from procedure_duration_map). That query referenced
        # etl_didb_studies.fallback_id/scheduled_datetime/report_time/proc_duration
        # -- none of which exist on etl_didb_studies (confirmed against the live
        # schema: it threw psycopg2.errors.UndefinedColumn on every call) -- and
        # it was never wired to any UI element (no template referenced
        # journey_json). The real, UI-connected Patient Journey feature is the
        # `/report/25/patient-journey` endpoint below (patient_journey_api),
        # which has been rebuilt to resolve patients via std_patient_ids/
        # std_pps/hl7_orders/etl_patient_view instead of fallback_id, and to use
        # real std_pps.start_datetime/end_datetime PPS timestamps instead of an
        # estimated proc_duration offset. This dead/broken block has been
        # removed rather than patched.

        template_data = {k: v for k, v in data.items() if k != 'raw_df'} if data else None

    return render_template("report_25.html", data=template_data, tat_pacs_data=tat_pacs_data, tat_ris_data=tat_ris_data, display_start=display_start, display_end=display_end, classes=classes, locations=locations, modalities=modalities, aetitles=aetitles, tree_json=tree_json, journey_json=journey_json, run_report=run_report, active_tab=active_tab, shift_config=shift_config)

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    data, _, _ = get_gold_standard_data(request.values)
    if not data: return "Error", 400
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['raw_df'].drop(columns=['study_date_dt'], errors='ignore').to_excel(writer, index=False, sheet_name='RawData')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"RAYD_PRO_Export_{date.today()}.xlsx")

@report_25_bp.route("/report/25/save-shifts", methods=["POST"])
@login_required
def save_shifts_25():
    from flask import redirect
    keys = ['morning_start', 'morning_end', 'afternoon_start', 'afternoon_end', 'night_start', 'night_end']
    for k in keys:
        val = request.form.get(k)
        if val is not None:
            existing = db.session.execute(
                text("SELECT id FROM settings WHERE key = :k"), {"k": f"shift_{k}"}
            ).fetchone()
            if existing:
                db.session.execute(
                    text("UPDATE settings SET value = :v WHERE key = :k"),
                    {"k": f"shift_{k}", "v": val}
                )
            else:
                db.session.execute(
                    text("INSERT INTO settings (key, value) VALUES (:k, :v)"),
                    {"k": f"shift_{k}", "v": val}
                )
    db.session.commit()
    return redirect(url_for('report_25.report_25'))

@report_25_bp.route("/report/25/patient-journey")
@login_required
def patient_journey_api():
    from flask import jsonify as _json
    from datetime import datetime as _dt

    pid       = (request.args.get('pid', '') or '').strip()
    accession = (request.args.get('accession', '') or '').strip()

    if not pid and not accession:
        return _json({'studies': [], 'error': 'Provide patient ID or accession number'})

    try:
        accessions  = set()
        pid_fuzzy   = False   # true once we had to fall back to substring matching

        # ── Find accessions by accession number ───────────────────────────────
        if accession:
            rows = db.session.execute(text(
                "SELECT DISTINCT accession_number FROM etl_didb_studies "
                "WHERE accession_number ILIKE :acc LIMIT 15"
            ), {'acc': f'%{accession}%'}).fetchall()
            accessions.update(r[0] for r in rows if r[0])

        # ── Find accessions by patient ID ──────────────────────────────────────
        # Tiered, most-to-least reliable identifier source (mirrors the pattern
        # already established for report timestamps in migrations 0070/0074/0075:
        # try the reliable source first, only fall back when it comes up empty).
        #
        # Exact-match tiers first -- NOT substring ILIKE -- because a raw
        # substring match on a bare identifier is exactly the kind of thing that
        # silently blends unrelated patients into one "journey" (the same class
        # of problem std_patient_ids/migration 0060 was built to get away from
        # for fallback_id). Only if every exact tier misses do we fall back to
        # substring search, and we flag that in the response so the UI can warn
        # the operator the results are approximate.
        #
        # NOTE: etl_didb_studies has NO patient_id column (confirmed against the
        # live schema) -- the previous version of this query referenced it and
        # threw psycopg2.errors.UndefinedColumn on every call; that branch's
        # try/except silently swallowed the error via db.session.rollback(),
        # so pid search was silently running on hl7_orders alone. The real PACS
        # patient identifier lives on etl_patient_view.id, joined via
        # patient_db_uid -- fixed below.
        if pid:
            # Tier 1: std_patient_ids -> std_pps -> etl_didb_studies (RIS identity,
            # most reliable -- a dedicated identifier table keyed to a stable
            # patient_person_key, not a raw/overloaded PACS or HL7 field).
            try:
                rows = db.session.execute(text("""
                    SELECT DISTINCT s.accession_number
                    FROM std_patient_ids i
                    JOIN std_pps pp ON pp.patient_person_key = i.patient_person_key
                    JOIN etl_didb_studies s ON s.study_db_uid = pp.study_db_uid
                    WHERE i.patient_id = :pid
                    LIMIT 20
                """), {'pid': pid}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()

            # Tier 2: hl7_orders exact match (RIS/HL7-sourced, already trusted
            # elsewhere in this file).
            try:
                rows = db.session.execute(text(
                    "SELECT DISTINCT accession_number FROM hl7_orders "
                    "WHERE patient_id = :pid AND accession_number IS NOT NULL LIMIT 20"
                ), {'pid': pid}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()

            # Tier 3: PACS-sourced exact match, via the correct table/column
            # (etl_patient_view.id -- NOT etl_didb_studies.patient_id, which
            # doesn't exist).
            try:
                rows = db.session.execute(text("""
                    SELECT DISTINCT s.accession_number
                    FROM etl_didb_studies s
                    JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
                    WHERE p.id = :pid
                    LIMIT 20
                """), {'pid': pid}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()

            # Tier 4 (fallback only): no exact match anywhere -- fall back to the
            # old substring behavior so a partially-typed ID still returns
            # something, but flag it as fuzzy.
            if not accessions:
                pid_fuzzy = True
                try:
                    rows = db.session.execute(text(
                        "SELECT DISTINCT accession_number FROM hl7_orders "
                        "WHERE patient_id ILIKE :pid AND accession_number IS NOT NULL LIMIT 20"
                    ), {'pid': f'%{pid}%'}).fetchall()
                    accessions.update(r[0] for r in rows if r[0])
                except Exception:
                    db.session.rollback()
                try:
                    rows = db.session.execute(text("""
                        SELECT DISTINCT s.accession_number
                        FROM etl_didb_studies s
                        JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
                        WHERE p.id ILIKE :pid
                        LIMIT 20
                    """), {'pid': f'%{pid}%'}).fetchall()
                    accessions.update(r[0] for r in rows if r[0])
                except Exception:
                    db.session.rollback()

        if not accessions:
            return _json({'studies': [], 'error': None, 'message': 'No matching studies found'})

        accn_list = list(accessions)[:15]

        # ── Batch fetch studies (1 query for all accessions) ─────────────────
        # Final-report timestamp/attribution uses the same reliability-ordered
        # COALESCE chain the main report_25 query uses (migrations 0074/0075):
        # rep_study_last_composed_ts/_by (confirmed populated on this install)
        # -> rep_final_timestamp/rep_final_signed_by (PACS-native, unreliable
        # here) -> hl7_oru_reports.result_datetime/physician_id (RIS-sourced,
        # migration 0070). The previous version of this endpoint used bare
        # s.rep_final_timestamp, which is mostly NULL for recent studies on
        # this PACS install -- so "Final Report Signed" was silently missing
        # from most journeys even for studies that really were reported.
        study_rows = db.session.execute(text("""
            SELECT DISTINCT ON (s.accession_number)
                s.accession_number,
                s.study_db_uid,
                s.study_date::text                                                AS study_date,
                s.study_time,
                COALESCE(s.study_description, '')                                 AS study_description,
                COALESCE(m.modality, s.study_modality, 'Unknown')                 AS modality,
                COALESCE(s.patient_class, '')                                     AS patient_class,
                COALESCE(s.patient_location, '')                                  AS patient_location,
                s.insert_time,
                s.rep_prelim_timestamp,
                s.rep_transcribed_timestamp,
                COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) AS final_ts,
                COALESCE(s.rep_study_last_composed_by, s.rep_final_signed_by, o.physician_id)     AS final_by,
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.signing_physician_first_name,''), ' ',
                    COALESCE(s.signing_physician_last_name,'')
                )), '')                                                            AS radiologist
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            LEFT JOIN hl7_oru_reports o
                ON o.accession_number = s.accession_number
            WHERE s.accession_number = ANY(:accns)
        """), {'accns': accn_list}).mappings().fetchall()
        studies_map = {r['accession_number']: dict(r) for r in study_rows}

        # ── Batch fetch std_pps (real RIS Performed-Procedure-Step timestamps) ─
        # This replaces the "true entry = insert_time - proc_duration" estimate
        # that used to live in this report's dead inline journey_json block:
        # proc_duration there came from procedure_duration_map with a bare
        # `COALESCE(pm.duration_minutes, 15)` default (see migrations
        # 0069/0070/0075) -- for any procedure without a mapped duration it
        # produced a generic 15-minute guess, not a real "true entry" time.
        # std_pps.start_datetime/end_datetime are actual RIS PPS timestamps for
        # the study, when the PPS enrichment job has matched it -- use those
        # instead of estimating.
        pps_map = {}
        study_ids = [r['study_db_uid'] for r in study_rows if r['study_db_uid']]
        if study_ids:
            try:
                pps_rows = db.session.execute(text("""
                    SELECT study_db_uid,
                           MIN(start_datetime) AS pps_start,
                           MAX(end_datetime)   AS pps_end
                    FROM std_pps
                    WHERE study_db_uid = ANY(:sids)
                    GROUP BY study_db_uid
                """), {'sids': study_ids}).mappings().fetchall()
                pps_map = {r['study_db_uid']: dict(r) for r in pps_rows}
            except Exception:
                db.session.rollback()

        # ── Batch fetch hl7_orders (1 query for all accessions) ──────────────
        orders_map = {}  # accn -> list of order dicts
        try:
            order_rows = db.session.execute(text("""
                SELECT
                    accession_number,
                    received_at,
                    scheduled_datetime,
                    done_at,
                    done_by,
                    order_status,
                    COALESCE(procedure_text, procedure_code, '') AS procedure,
                    modality   AS order_modality,
                    patient_id AS order_pid
                FROM hl7_orders
                WHERE accession_number = ANY(:accns)
                ORDER BY accession_number, received_at NULLS LAST
            """), {'accns': accn_list}).mappings().fetchall()
            for r in order_rows:
                orders_map.setdefault(r['accession_number'], []).append(dict(r))
        except Exception:
            db.session.rollback()

        # ── Build timeline per accession (pure Python, no more DB calls) ─────
        def _ev(events, ts, ev_type, label, detail='', by=None):
            if ts is None:
                return
            events.append({
                'ts':     str(ts),
                'type':   ev_type,
                'label':  label,
                'detail': detail,
                'by':     str(by) if by else None,
            })

        results = []
        for accn in accn_list:
            study  = studies_map.get(accn)
            if not study:
                continue
            orders = orders_map.get(accn, [])
            pps    = pps_map.get(study.get('study_db_uid'), {})

            events  = []
            pid_val = None
            for o in orders:
                pid_val = pid_val or o.get('order_pid')
                _ev(events, o.get('received_at'),       'order_received', 'Order Received',
                    o.get('procedure') or '')
                _ev(events, o.get('scheduled_datetime'), 'scheduled',      'Exam Scheduled',
                    f"Status: {o.get('order_status') or '?'}")
                _ev(events, o.get('done_at'),            'tech_done',      'Exam Completed by Tech',
                    f"Modality: {o.get('order_modality') or ''}",
                    o.get('done_by'))

            # Real RIS PPS timestamps (when the PPS enrichment job has matched
            # this study) -- the reliable replacement for the old estimated
            # "true entry" time.
            _ev(events, pps.get('pps_start'), 'pps_start', 'Exam Started (RIS PPS)',
                f"Modality: {study.get('modality','')}")
            _ev(events, pps.get('pps_end'),   'pps_end',   'Exam Completed (RIS PPS)', '')

            _ev(events, study.get('insert_time'),               'pacs_in',     'Arrived in PACS',
                f"Modality: {study.get('modality','')}")
            _ev(events, study.get('rep_prelim_timestamp'),      'prelim',      'Preliminary Report', '')
            _ev(events, study.get('rep_transcribed_timestamp'), 'transcribed', 'Transcribed', '')
            _ev(events, study.get('final_ts'),                  'final',       'Final Report Signed',
                '', study.get('radiologist') or study.get('final_by'))

            events.sort(key=lambda x: x['ts'])
            for i in range(1, len(events)):
                try:
                    t1 = _dt.fromisoformat(str(events[i-1]['ts']).replace('Z', '').split('.')[0])
                    t2 = _dt.fromisoformat(str(events[i]['ts']).replace('Z', '').split('.')[0])
                    events[i]['gap_min'] = round((t2 - t1).total_seconds() / 60)
                except Exception:
                    events[i]['gap_min'] = None

            results.append({
                'accession':        accn,
                'study_date':       study.get('study_date', ''),
                'modality':         study.get('modality', ''),
                'patient_id':       pid_val or '',
                'patient_class':    study.get('patient_class', ''),
                'patient_location': study.get('patient_location', ''),
                'description':      study.get('study_description', ''),
                'events':           events,
            })

        results.sort(key=lambda x: x['study_date'], reverse=True)
        resp = {'studies': results, 'error': None}
        if pid_fuzzy and results:
            resp['message'] = (
                'No exact patient-ID match was found, so this list falls back to a '
                'partial/substring match -- it may include studies from other '
                'patients whose ID happens to contain the same characters. '
                'Verify identity before relying on it.'
            )
        return _json(resp)

    except Exception as e:
        db.session.rollback()
        return _json({'studies': [], 'error': str(e)}), 500

# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(25, report_25_bp, report_25, export_report_25)
