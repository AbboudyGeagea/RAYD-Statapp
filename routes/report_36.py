"""
routes/report_36.py
--------------------
Report 36 — Radiology Reporting Analytics.

Split out of Report 25's "Radiologists" tab (tab-clinical) 2026-07-31 (operator
instruction): originally KPI Detailed Reading, Resident vs Radiologist TAT, Patient
Wait Time (Scheduled -> Arrived), Technician Efficiency (Arrived -> Exam Done),
Radiologist Workload Matrix, Reporting Cadence Analysis, Technician TAT by AE
Station, and a statistical-insights panel. KPI Detailed Reading, Technician TAT by AE
Station, and the insights panel were removed 2026-07-31 (operator instruction) --
KPI needs rework before it's worth showing again; the other two weren't earning their
keep. Technician Efficiency was removed again later (operator instruction) and moved
to Report 35 (routes/report_35.py), extended there to group by technologist person
instead of AE device -- see that module's get_technician_tat_data() docstring. What's
left here: Resident vs Radiologist TAT, Patient Wait Time, Radiologist Workload
Matrix, Reporting Cadence Analysis.

No single base SQL applies to every remaining chart here (checked each one's data
source before moving anything) -- most run their own bespoke RIS/PPS-anchored queries
against etl_didb_studies directly; Radiologist Workload Matrix instead reuses
report_25's own get_gold_standard_data() result (report_25's report_template-driven
dataset) rather than duplicating that SQL here -- a Python-level function-call
dependency on routes.report_25, not a stored report_template row
(report_template.report_id=36 has report_sql_query = NULL, see migration 0093).

Unlike report_25, this page has no other tabs competing for a fast initial render, so
everything here is computed synchronously in one request -- report_25's original
deferral of shift_patterns to a background /report/25/bg fetch existed specifically to
keep report_25's Operations tab snappy despite this heavier computation; that reason
doesn't apply to a standalone page. If load time becomes a real problem, the same
background-fetch pattern could be added here later.

Register in registry.py:
    import routes.report_36
"""
import logging
import pandas as pd
from datetime import date, datetime
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from utils.site_resolver import default_site
from utils.report_filters import sidebar_filters as _sidebar_filters
from routes.report_25 import get_gold_standard_data

logger = logging.getLogger("report_36")

report_36_bp = Blueprint("report_36", __name__)




_RES_RAD_TAT_SQL_TEMPLATE = """
    WITH exam_done_anchor AS (
        SELECT p.study_instance_uid, MAX(w.exam_done_at) AS exam_done_time
        FROM std_worklist_exam_done w
        JOIN std_pps p ON p.pps_key = w.pps_key
        WHERE p.study_instance_uid IS NOT NULL
        GROUP BY p.study_instance_uid
    ),
    role_lookup AS (
        SELECT DISTINCT UPPER(login_id) AS login_id, group_name AS role
        FROM std_pacs_user_groups
        WHERE group_name IN ('radiologists', 'residents')
    ),
    signed_events AS (
        SELECT s.study_db_uid, s.study_instance_uid, s.storing_ae, s.patient_class,
               s.patient_location, s.insert_time,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               UPPER(TRIM(s.rep_prelim_signed_by)) AS signer, s.rep_prelim_timestamp AS sign_time
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.rep_prelim_signed_by IS NOT NULL AND s.rep_prelim_timestamp IS NOT NULL
          AND s.study_date BETWEEN :start AND :end
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
          {filter_clause}
        UNION ALL
        SELECT s.study_db_uid, s.study_instance_uid, s.storing_ae, s.patient_class,
               s.patient_location, s.insert_time,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               UPPER(TRIM(s.rep_study_last_composed_by)) AS signer, s.rep_study_last_composed_ts AS sign_time
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.rep_study_last_composed_by IS NOT NULL AND s.rep_study_last_composed_ts IS NOT NULL
          AND s.study_date BETWEEN :start AND :end
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
          {filter_clause}
    ),
    classified AS (
        SELECT
            se.*,
            rl.role,
            CASE
                WHEN se.patient_location = 'ER' THEN 'ER'
                WHEN se.patient_class = 'I' THEN 'Inpatient'
                WHEN se.patient_class = 'O' THEN 'Outpatient'
                ELSE 'Other'
            END AS patient_class_bucket,
            CASE WHEN ed.exam_done_time IS NOT NULL AND se.sign_time > ed.exam_done_time
                 THEN EXTRACT(EPOCH FROM (se.sign_time - ed.exam_done_time)) / 3600.0 END AS tat_hours_ris,
            CASE WHEN se.insert_time IS NOT NULL AND se.sign_time > se.insert_time
                 THEN EXTRACT(EPOCH FROM (se.sign_time - se.insert_time)) / 3600.0 END AS tat_hours_pacs
        FROM signed_events se
        LEFT JOIN exam_done_anchor ed ON ed.study_instance_uid = se.study_instance_uid
        LEFT JOIN role_lookup rl ON rl.login_id = SPLIT_PART(se.signer, '@', 1)
        WHERE (ed.exam_done_time IS NOT NULL AND se.sign_time > ed.exam_done_time)
           OR (se.insert_time IS NOT NULL AND se.sign_time > se.insert_time)
    )
    SELECT
        COALESCE(role, '__unclassified__') AS role,
        SPLIT_PART(signer, '@', 1) AS resident_name,
        modality, patient_class_bucket,
        COUNT(*) FILTER (WHERE tat_hours_ris IS NOT NULL) AS n_ris,
        ROUND((AVG(tat_hours_ris) FILTER (WHERE tat_hours_ris IS NOT NULL))::numeric, 2) AS avg_tat_h_ris,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours_ris) FILTER (WHERE tat_hours_ris IS NOT NULL))::numeric, 2) AS median_tat_h_ris,
        COUNT(*) FILTER (WHERE tat_hours_ris <= 3)                           AS bucket_0_3h_ris,
        COUNT(*) FILTER (WHERE tat_hours_ris > 3 AND tat_hours_ris <= 5)     AS bucket_3_5h_ris,
        COUNT(*) FILTER (WHERE tat_hours_ris > 5)                           AS bucket_5h_plus_ris,
        COUNT(*) FILTER (WHERE tat_hours_pacs IS NOT NULL) AS n_pacs,
        ROUND((AVG(tat_hours_pacs) FILTER (WHERE tat_hours_pacs IS NOT NULL))::numeric, 2) AS avg_tat_h_pacs,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours_pacs) FILTER (WHERE tat_hours_pacs IS NOT NULL))::numeric, 2) AS median_tat_h_pacs,
        COUNT(*) FILTER (WHERE tat_hours_pacs <= 3)                          AS bucket_0_3h_pacs,
        COUNT(*) FILTER (WHERE tat_hours_pacs > 3 AND tat_hours_pacs <= 5)   AS bucket_3_5h_pacs,
        COUNT(*) FILTER (WHERE tat_hours_pacs > 5)                          AS bucket_5h_plus_pacs
    FROM classified
    GROUP BY role, resident_name, modality, patient_class_bucket
    ORDER BY role, resident_name, modality, patient_class_bucket
"""


def _split_dual_anchor_role_rows(rows, anchor):
    """Project the merged RIS+PACS query's rows into the single-anchor shape the
    template expects, dropping rows where this anchor had zero qualifying events --
    matches the pre-merge behavior of never emitting an empty (role, resident,
    modality, class) group."""
    suffix = f"_{anchor}"
    result = {"residents": [], "radiologists": [], "unclassified_count": 0}
    for r in rows:
        row = dict(r)
        n = row[f"n{suffix}"]
        if not n:
            continue
        role = row["role"]
        projected = {
            "resident_name": row["resident_name"],
            "modality": row["modality"],
            "patient_class_bucket": row["patient_class_bucket"],
            "n": n,
            "avg_tat_h": row[f"avg_tat_h{suffix}"],
            "median_tat_h": row[f"median_tat_h{suffix}"],
            "bucket_0_3h": row[f"bucket_0_3h{suffix}"],
            "bucket_3_5h": row[f"bucket_3_5h{suffix}"],
            "bucket_5h_plus": row[f"bucket_5h_plus{suffix}"],
        }
        if role == "radiologists":
            result["radiologists"].append(projected)
        elif role == "residents":
            result["residents"].append(projected)
        else:
            result["unclassified_count"] += n
    return result


def get_resident_radiologist_tat(form_data):
    """
    Resident vs. Radiologist TAT (exam-done -> signature) per modality per patient
    class (Inpatient/Outpatient/ER) per individual signer, split by REAL role.

    Role source: std_pacs_user_groups (PACS MEDILINK reading-permission groups --
    "radiologists" / "residents"), matched to the signer via login name. NOT RIS's
    resource_role_key -- that one was over-granted to every user as a workaround for
    an installation-time RIS permissions bug and doesn't reflect real job function
    (see ETL_JOBS/etl_ris_resources.py's docstring, and the concrete false positive/
    negatives it produced: a radiologist misclassified Resident, two real residents
    missed entirely -- caught 2026-07-31 by comparing it against this real group data).

    Counts every signed event (prelim AND final) attributed to whoever actually
    signed it, not the stage it happened at -- a resident's final signature (rare)
    still counts as resident work, and vice versa. resident_name is the raw
    login-style signer value (e.g. "abdallah.noufaily") -- resolving it to a real
    display name is explicitly deferred by the operator (see migration 0075's note).

    Both anchor variants -- "ris" (WORKLIST_STATUS_HISTORY status_key=100 "Exam Done",
    via std_worklist_exam_done -> std_pps.pps_key) and "pacs" (etl_didb_studies.insert_time,
    labeled "PACS end time calculation" in the UI) -- are now computed by ONE query
    instead of two (2026-07-31, load-time optimization): the anchor CTE is LEFT JOINed
    unconditionally and both tat_hours_ris/tat_hours_pacs are computed as parallel
    per-row columns, aggregated independently via FILTER (WHERE ...) clauses so a row's
    inclusion in one anchor's numbers is unaffected by the other -- same row-inclusion
    logic as the old two-query version (sign_time must be strictly after that anchor's
    exam-done time), just evaluated per-column instead of via a single shared WHERE.
    Every chart on this page must also honor the full left-sidebar filter set (date
    range, patient class, modality, AE title, patient location), not just dates --
    see utils/report_filters.sidebar_filters.

    Returns {"ris": {...}, "pacs": {...}}, each shaped {"residents": [...], "radiologists":
    [...], "unclassified_count": N}.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)
    sql = _RES_RAD_TAT_SQL_TEMPLATE.format(filter_clause=filter_clause)
    rows = []
    try:
        rows = db.session.execute(text(sql), params).mappings().fetchall()
    except Exception:
        logger.exception("Failed to compute resident/radiologist TAT")
        db.session.rollback()
    return {
        "ris": _split_dual_anchor_role_rows(rows, "ris"),
        "pacs": _split_dual_anchor_role_rows(rows, "pacs"),
    }


_MODALITY_TAT_SQL_TEMPLATE = """
    WITH exam_done_anchor AS (
        SELECT p.study_instance_uid, MAX(w.exam_done_at) AS exam_done_time
        FROM std_worklist_exam_done w
        JOIN std_pps p ON p.pps_key = w.pps_key
        WHERE p.study_instance_uid IS NOT NULL
        GROUP BY p.study_instance_uid
    ),
    signed_events AS (
        SELECT s.study_db_uid, s.study_instance_uid, s.patient_class, s.patient_location,
               s.insert_time,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               s.rep_prelim_timestamp AS sign_time
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.rep_prelim_signed_by IS NOT NULL AND s.rep_prelim_timestamp IS NOT NULL
          AND s.study_date BETWEEN :start AND :end
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
          {filter_clause}
        UNION ALL
        SELECT s.study_db_uid, s.study_instance_uid, s.patient_class, s.patient_location,
               s.insert_time,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               s.rep_study_last_composed_ts AS sign_time
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.rep_study_last_composed_by IS NOT NULL AND s.rep_study_last_composed_ts IS NOT NULL
          AND s.study_date BETWEEN :start AND :end
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
          {filter_clause}
    ),
    classified AS (
        SELECT
            se.*,
            CASE
                WHEN se.patient_location = 'ER' THEN 'ER'
                WHEN se.patient_class = 'I' THEN 'Inpatient'
                WHEN se.patient_class = 'O' THEN 'Outpatient'
                ELSE 'Other'
            END AS patient_class_bucket,
            CASE WHEN ed.exam_done_time IS NOT NULL AND se.sign_time > ed.exam_done_time
                 THEN EXTRACT(EPOCH FROM (se.sign_time - ed.exam_done_time)) / 3600.0 END AS tat_hours_ris,
            CASE WHEN se.insert_time IS NOT NULL AND se.sign_time > se.insert_time
                 THEN EXTRACT(EPOCH FROM (se.sign_time - se.insert_time)) / 3600.0 END AS tat_hours_pacs
        FROM signed_events se
        LEFT JOIN exam_done_anchor ed ON ed.study_instance_uid = se.study_instance_uid
        WHERE (ed.exam_done_time IS NOT NULL AND se.sign_time > ed.exam_done_time)
           OR (se.insert_time IS NOT NULL AND se.sign_time > se.insert_time)
    )
    SELECT
        modality, patient_class_bucket,
        COUNT(*) FILTER (WHERE tat_hours_ris IS NOT NULL) AS n_ris,
        ROUND((AVG(tat_hours_ris) FILTER (WHERE tat_hours_ris IS NOT NULL))::numeric, 2) AS avg_tat_h_ris,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours_ris) FILTER (WHERE tat_hours_ris IS NOT NULL))::numeric, 2) AS median_tat_h_ris,
        COUNT(*) FILTER (WHERE tat_hours_pacs IS NOT NULL) AS n_pacs,
        ROUND((AVG(tat_hours_pacs) FILTER (WHERE tat_hours_pacs IS NOT NULL))::numeric, 2) AS avg_tat_h_pacs,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours_pacs) FILTER (WHERE tat_hours_pacs IS NOT NULL))::numeric, 2) AS median_tat_h_pacs
    FROM classified
    GROUP BY modality, patient_class_bucket
    ORDER BY modality, patient_class_bucket
"""


def get_modality_tat(form_data):
    """
    Modality TAT (exam-done -> signature) per modality per patient class, all
    signers combined -- the department-wide view of get_resident_radiologist_tat's
    data, dropping individual identity. Same RIS/PACS anchor toggle, same
    left-sidebar filters.

    Single merged query (2026-07-31, load-time optimization) -- see
    get_resident_radiologist_tat's docstring for why this replaced the old
    two-query-per-anchor version and how the FILTER-clause split preserves the
    original per-anchor row-inclusion semantics.

    Returns {"ris": [...], "pacs": [...]}, each a flat list of per-(modality,
    patient_class_bucket) rows.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)
    sql = _MODALITY_TAT_SQL_TEMPLATE.format(filter_clause=filter_clause)
    rows = []
    try:
        rows = db.session.execute(text(sql), params).mappings().fetchall()
    except Exception:
        logger.exception("Failed to compute modality TAT")
        db.session.rollback()
    ris, pacs = [], []
    for r in rows:
        row = dict(r)
        if row["n_ris"]:
            ris.append({
                "modality": row["modality"],
                "patient_class_bucket": row["patient_class_bucket"],
                "n": row["n_ris"],
                "avg_tat_h": row["avg_tat_h_ris"],
                "median_tat_h": row["median_tat_h_ris"],
            })
        if row["n_pacs"]:
            pacs.append({
                "modality": row["modality"],
                "patient_class_bucket": row["patient_class_bucket"],
                "n": row["n_pacs"],
                "avg_tat_h": row["avg_tat_h_pacs"],
                "median_tat_h": row["median_tat_h_pacs"],
            })
    return {"ris": ris, "pacs": pacs}


def get_patient_wait_time(form_data):
    """
    Patient Wait Time (Scheduled -> Arrived) per modality per patient class
    (Inpatient/Outpatient/ER) -- redefined 2026-07-31 (operator instruction); the OLD
    Patient Wait Time (Arrived -> Exam Done) is now Technician TAT, moved to Report 35
    (routes/report_35.py's get_technician_tat_data()).

    Both ends are RIS status transitions off WORKLIST_STATUS_HISTORY:
    "Scheduled" (status_key=40 -> std_worklist_scheduled, ETL_JOBS/etl_ris_worklist_scheduled.py)
    and "Arrived" (status_key=60 -> std_worklist_arrivals), joined via std_pps.pps_key
    -> study_instance_uid -> etl_didb_studies. PACS has no equivalent concept of a
    scheduling status, so there is no RIS/PACS toggle for this one.

    Full left-sidebar filter set applies.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)

    rows = []
    try:
        rows = db.session.execute(text(f"""
            WITH scheduled AS (
                SELECT p.study_instance_uid, MIN(sc.scheduled_at) AS scheduled_at
                FROM std_worklist_scheduled sc
                JOIN std_pps p ON p.pps_key = sc.pps_key
                WHERE p.study_instance_uid IS NOT NULL
                GROUP BY p.study_instance_uid
            ),
            arrival AS (
                SELECT p.study_instance_uid, MIN(wa.arrived_at) AS arrived_at
                FROM std_worklist_arrivals wa
                JOIN std_pps p ON p.pps_key = wa.pps_key
                WHERE p.study_instance_uid IS NOT NULL
                GROUP BY p.study_instance_uid
            ),
            wait AS (
                SELECT
                    s.study_db_uid,
                    COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                    CASE
                        WHEN s.patient_location = 'ER' THEN 'ER'
                        WHEN s.patient_class = 'I' THEN 'Inpatient'
                        WHEN s.patient_class = 'O' THEN 'Outpatient'
                        ELSE 'Other'
                    END AS patient_class_bucket,
                    EXTRACT(EPOCH FROM (ar.arrived_at - sc.scheduled_at)) / 60.0 AS wait_minutes
                FROM etl_didb_studies s
                JOIN scheduled sc ON sc.study_instance_uid = s.study_instance_uid
                JOIN arrival ar ON ar.study_instance_uid = s.study_instance_uid
                LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
                WHERE ar.arrived_at > sc.scheduled_at
                  AND s.study_date BETWEEN :start AND :end
                  AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
                  {filter_clause}
            )
            SELECT
                modality, patient_class_bucket,
                COUNT(*) AS n,
                ROUND(AVG(wait_minutes)::numeric, 1) AS avg_wait_min,
                ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wait_minutes))::numeric, 1) AS median_wait_min,
                COUNT(*) FILTER (WHERE wait_minutes <= 30)                          AS bucket_0_30,
                COUNT(*) FILTER (WHERE wait_minutes > 30 AND wait_minutes <= 60)    AS bucket_30_60,
                COUNT(*) FILTER (WHERE wait_minutes > 60)                           AS bucket_60_plus
            FROM wait
            GROUP BY modality, patient_class_bucket
            ORDER BY modality, patient_class_bucket
        """), params).mappings().fetchall()
    except Exception:
        logger.exception("Failed to compute patient wait time")
        db.session.rollback()

    return [dict(r) for r in rows]


def get_reporting_cadence(form_data):
    """
    Reporting Cadence Analysis (per-radiologist signing density heatmap + daily
    arrival/departure/break log), ported from report_25's compute_bg_data().

    (Used to also compute the statistical insights panel from this same
    rep_final_timestamp query -- removed 2026-07-31 along with KPI Detailed Reading
    and Technician TAT by AE Station, operator instruction.)
    """
    params, filter_clause, start, end = _sidebar_filters(form_data)

    shift_patterns = {}
    try:
        _BREAK_MIN = 20
        ts_rows = db.session.execute(text(f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(CONCAT(
                        COALESCE(s.signing_physician_first_name, ''), ' ',
                        COALESCE(s.signing_physician_last_name,  '')
                    )), ''),
                    s.rep_final_signed_by,
                    'Unknown'
                ) AS radiologist,
                s.rep_final_timestamp,
                s.accession_number
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.rep_final_timestamp IS NOT NULL
              AND s.rep_final_timestamp::date BETWEEN :start AND :end
              {filter_clause}
            ORDER BY 1, 2
        """), params).fetchall()

        if ts_rows:
            ts_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts', 'accession_number'])
            ts_df['ts']        = pd.to_datetime(ts_df['ts'])
            ts_df['work_date'] = ts_df['ts'].dt.date
            ts_df['hour']      = ts_df['ts'].dt.hour
            ts_df['dow']       = ts_df['ts'].dt.dayofweek

            def _h_to_hhmm(h):
                hh = int(h); mm = int(round((h - hh) * 60))
                return f"{hh:02d}:{mm:02d}"

            for rad, rdf in ts_df.groupby('radiologist'):
                if rad.strip() in ('Unknown', ''):
                    continue
                rdf = rdf.sort_values('ts')
                hm = rdf.groupby(['dow', 'hour']).size().reset_index(name='cnt')
                heatmap = [[int(r['hour']), int(r['dow']), int(r['cnt'])] for _, r in hm.iterrows()]
                hm_max  = int(hm['cnt'].max()) if not hm.empty else 1
                arrivals, departures, break_cnts, break_durs = [], [], [], []
                daily_log = []
                for work_date, ddf in rdf.groupby('work_date'):
                    times = ddf['ts'].sort_values().tolist()
                    first, last = times[0], times[-1]
                    arr_h = first.hour + first.minute / 60
                    dep_h = last.hour  + last.minute  / 60
                    arrivals.append(arr_h)
                    departures.append(dep_h)
                    breaks = []
                    for i in range(1, len(times)):
                        gap = (times[i] - times[i - 1]).total_seconds() / 60
                        if gap >= _BREAK_MIN:
                            dur = round(gap)
                            icon = '☕' if dur <= 35 else ('🚬' if dur <= 70 else '🏃')
                            kind = 'Coffee' if dur <= 35 else ('Long break' if dur <= 70 else 'Disappeared')
                            breaks.append({'start': times[i-1].strftime('%H:%M'),
                                           'end':   times[i].strftime('%H:%M'),
                                           'duration': dur, 'icon': icon, 'kind': kind})
                    break_cnts.append(len(breaks))
                    break_durs.extend([b['duration'] for b in breaks])
                    daily_log.append({
                        'date': str(work_date), 'dow': first.strftime('%a'),
                        'arrival': first.strftime('%H:%M'), 'departure': last.strftime('%H:%M'),
                        'studies': len(times),
                        'span_h': round(dep_h - arr_h, 1),
                        'breaks': breaks,
                    })
                wd = len(daily_log)
                shift_patterns[rad] = {
                    'avg_arrival':    _h_to_hhmm(sum(arrivals)   / len(arrivals))   if arrivals   else '—',
                    'avg_departure':  _h_to_hhmm(sum(departures) / len(departures)) if departures else '—',
                    'avg_breaks_day': round(sum(break_cnts) / wd, 1)                if wd         else 0,
                    'avg_break_dur':  int(round(sum(break_durs) / len(break_durs))) if break_durs else 0,
                    'working_days':   wd,
                    'heatmap':        heatmap,
                    'hm_max':         hm_max,
                    'daily_log':      daily_log[-60:],
                }
    except Exception:
        logger.exception("Failed to compute shift patterns")
        db.session.rollback()

    return shift_patterns


def _compute_report_36_data(form_data):
    """
    Bundle all of report_36's per-request queries under one 5-min TTL cache entry
    (report_cache, keyed report_id=36) -- added 2026-07-31 for load-time
    optimization, mirrors get_gold_standard_data's own bundle-cache pattern
    (report_id=25) exactly. Before this, every page load/reload re-ran all five
    queries fresh even with identical filters; a repeat view within 5 minutes is
    now a single dict lookup instead of five-plus round trips.

    get_gold_standard_data() itself is already cached under its own key (25), so
    calling it here doesn't double the cost on a cache miss for report_25 --
    only report_36's own bespoke queries (res_rad_tat, modality_tat, wait_time,
    shift_patterns) are new work being cached by this wrapper.
    """
    from routes.report_cache import cache_get, cache_put
    cached = cache_get(36, form_data)
    if cached is not None:
        return cached

    gold_data, display_start, display_end = get_gold_standard_data(form_data)

    result = {
        'rad_volume_matrix':    (gold_data or {}).get('rad_volume_matrix'),
        'tech_tat_cards':       (gold_data or {}).get('tech_tat_cards'),
        'res_rad_tat':          get_resident_radiologist_tat(form_data),
        'modality_tat':         get_modality_tat(form_data),
        'wait_time_data':       get_patient_wait_time(form_data),
        'shift_patterns':       get_reporting_cadence(form_data),
        'display_start':        display_start,
        'display_end':          display_end,
    }
    cache_put(36, form_data, result)
    return result


@report_36_bp.route("/report/36", methods=["GET", "POST"])
@login_required
def report_36():
    classes = locations = modalities = aetitles = []
    run_report = 'start_date' in request.values

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    res_rad_tat = modality_tat = wait_time_data = None
    rad_volume_matrix = shift_patterns = tech_tat_cards = None

    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_36',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date')})

        data = _compute_report_36_data(request.values)
        rad_volume_matrix    = data['rad_volume_matrix']
        tech_tat_cards        = data['tech_tat_cards']
        res_rad_tat           = data['res_rad_tat']
        modality_tat          = data['modality_tat']
        wait_time_data        = data['wait_time_data']
        shift_patterns        = data['shift_patterns']
        display_start         = data['display_start']
        display_end           = data['display_end']

    return render_template(
        "report_36.html",
        res_rad_tat=res_rad_tat,
        modality_tat=modality_tat,
        wait_time_data=wait_time_data,
        rad_volume_matrix=rad_volume_matrix,
        tech_tat_cards=tech_tat_cards,
        shift_patterns=shift_patterns,
        display_start=display_start, display_end=display_end,
        classes=classes, locations=locations, modalities=modalities, aetitles=aetitles,
        run_report=run_report,
    )


from routes.report_registry import register_report
register_report(36, report_36_bp, report_36, None)
