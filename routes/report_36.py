"""
routes/report_36.py
--------------------
Report 36 — Radiology Reporting Analytics.

Split out of Report 25's "Radiologists" tab (tab-clinical) 2026-07-31 (operator
instruction): KPI Detailed Reading, Resident vs Radiologist TAT, Patient Wait Time
(Scheduled -> Arrived), Technician Efficiency (Arrived -> Exam Done), Radiologist
Workload Matrix, Reporting Cadence Analysis, Technician TAT by AE Station.

No single base SQL applies to every chart here (checked each one's data source before
moving anything) -- most run their own bespoke RIS/PPS-anchored queries against
etl_didb_studies directly; Radiologist Workload Matrix, Technician TAT by AE Station,
and the radiologist insights panel instead reuse report_25's own
get_gold_standard_data() result (report_25's report_template-driven dataset) rather
than duplicating that SQL here -- a Python-level function-call dependency on
routes.report_25, not a stored report_template row (report_template.report_id=36 has
report_sql_query = NULL, see migration 0093).

Unlike report_25, this page has no other tabs competing for a fast initial render, so
everything here is computed synchronously in one request -- report_25's original
deferral of shift_patterns/rad_insights to a background /report/25/bg fetch existed
specifically to keep report_25's Operations tab snappy despite this heavier
computation; that reason doesn't apply to a standalone page. If load time becomes a
real problem, the same background-fetch pattern could be added here later.

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
from routes.insights_engine import run_rad_insights
from routes.report_25 import get_gold_standard_data

logger = logging.getLogger("report_36")

report_36_bp = Blueprint("report_36", __name__)


# ── KPI Detailed Reading (hospital TAT-per-modality-per-radiologist spec) ────
# Format matches "KPI Detailed reading.xlsx" (repo root) exactly: for each
# modality x patient-class block (e.g. CT-IN / CT-Urg / CT-Out), three TAT
# stages bucketed into named time ranges, radiologist rows per stage (except
# "Exam Done to Read", which is a single aggregate row — not attributable to
# an individual since it's queue/pickup time, not a specific radiologist's
# turnaround).
#
# Stage definitions (assumption, confirm against real data — operator:
# "let's test it, we can change the queries if the data is illogical"):
#   Ex. Done to Read      : COALESCE(hl7_orders.done_at, .pacs_done_at) -> rep_prelim_timestamp
#   Signed 1 to Approved  : rep_prelim_timestamp -> rep_final_timestamp        (per radiologist)
#   Exam done to Approved : COALESCE(done_at, pacs_done_at) -> COALESCE(rep_final_timestamp,
#                            hl7_oru_reports.result_datetime)                  (per radiologist)
#
# Patient-class block assumption (confirm): "Urgent" = ER, detected the same
# way the rest of this file already does (accession_number starts with '2XE');
# Inpatient / Outpatient split on patient_class text — exact value vocabulary
# unconfirmed (no CHECK constraint in schema), using broad ILIKE matches.
#
# Radiologist identity: hl7_oru_reports.physician_id is a raw RIS code, not a
# name (see migrations/0070's known-limitation note) -- resolved here via
# std_resources_ris.resource_id, which uses the SAME composite-ID format
# already confirmed for etl_didb_studies.reading/signing_physician_id
# (migration 0063). Falls back to the raw code if no match, so a resolution
# gap is visible (a code on screen) rather than silently dropped.
#
# Radiologist list is NOT hardcoded -- whoever actually has signed studies in
# the selected period shows up, per operator instruction.

_KPI_BUCKETS_STANDARD = [   # CT-IN / CT-Urg style cutoffs
    ('0:00-0:10', 0, 10), ('0:11-0:30', 11, 30), ('0:31-1h', 31, 60),
    ('1h-2h', 61, 120), ('2h-4h', 121, 240), ('4h-6h', 241, 360),
    ('6h-12h', 361, 720), ('12h-18h', 721, 1080), ('18h-24h', 1081, 1440),
    ('24h-48h', 1441, 2880), ('48h-72h', 2881, 4320), ('72h-96h', 4321, 5760),
    ('96h-168h', 5761, 10080), ('>7 days', 10081, None),
]
_KPI_BUCKETS_OUTPATIENT = [   # CT-Out style cutoffs (coarser 12-24h step, no 18h split)
    ('0:00-0:10', 0, 10), ('0:11-0:30', 11, 30), ('0:31-1h', 31, 60),
    ('1h-2h', 61, 120), ('2h-4h', 121, 240), ('4h-6h', 241, 360),
    ('6h-12h', 361, 720), ('12h-24h', 721, 1440),
    ('24h-36h', 1441, 2160), ('36h-48h', 2161, 2880),
    ('48h-72h', 2881, 4320), ('72h-96h', 4321, 5760),
    ('96h-168h', 5761, 10080), ('>7 days', 10081, None),
]
_KPI_PROCEDURE_EXCLUSIONS = ['%TAVI%', '%CORO%']   # confirmed: exclude TAVI + Coro CT
# Modalities are NOT hardcoded — every modality present in the queried period's data gets
# its own block, using the same bucket scheme as CT (the only one with a defined SLA in the
# source spreadsheet) as a default until other modalities get their own confirmed windows.


def _kpi_bucket_label(minutes, buckets):
    if pd.isna(minutes):
        return None
    for label, lo, hi in buckets:
        if minutes >= lo and (hi is None or minutes <= hi):
            return label
    return None


def _kpi_class_bucket(row):
    acc = str(row.get('accession_number') or '').upper()
    if acc.startswith('2XE'):
        return 'Urg'
    pc = str(row.get('patient_class') or '').upper()
    if pc.startswith('IN') or pc in ('I', 'IP'):
        return 'IN'
    if pc.startswith('OUT') or pc.startswith('AMB') or pc in ('O', 'OP'):
        return 'Out'
    return None   # unclassified — excluded from the KPI table, not guessed


def get_kpi_detailed_reading(form_data):
    """
    Bucketed multi-stage TAT per modality x patient-class x radiologist,
    matching the hospital's "KPI Detailed reading.xlsx" format. See module
    comment block above for stage/bucket definitions and open assumptions.

    Returns a flat list of blocks — one per (modality, patient_class) pair
    actually present in the queried period, discovered from the data itself
    (not a hardcoded modality list). The template renders this as a dynamic
    modality/class selector rather than dumping every block statically.
    """
    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    params = {"start": start, "end": end}

    rh_site_id = default_site()
    site_clause = ""
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        site_clause = "AND m.site_id = :rh_site_id"

    excl_clause = " AND ".join(
        f"UPPER(COALESCE(s.procedure_code, '')) NOT LIKE '{p}'" for p in _KPI_PROCEDURE_EXCLUSIONS
    )

    blocks = []
    try:
        # One query across every modality — grouping happens in pandas below,
        # so new modalities need no code change, just data.
        rows = db.session.execute(text(f"""
            SELECT
                s.accession_number,
                s.patient_class,
                COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'N/A') AS modality,
                COALESCE(s.rep_final_signed_by, res.common_name, o.physician_id) AS radiologist,
                s.rep_prelim_timestamp,
                s.rep_final_timestamp,
                o.result_datetime AS ris_result_datetime,
                ho.done_at,
                ho.pacs_done_at
            FROM etl_didb_studies s
            JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            LEFT JOIN hl7_orders ho ON ho.accession_number = s.accession_number
            LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
            LEFT JOIN std_resources_ris res ON res.resource_id = o.physician_id
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {site_clause}
              AND {excl_clause}
        """), params).mappings().fetchall()

        if rows:
            kdf = pd.DataFrame(rows)
            kdf['exam_done'] = pd.to_datetime(kdf['done_at'], errors='coerce').fillna(
                pd.to_datetime(kdf['pacs_done_at'], errors='coerce')
            )
            kdf['approved'] = pd.to_datetime(kdf['rep_final_timestamp'], errors='coerce').fillna(
                pd.to_datetime(kdf['ris_result_datetime'], errors='coerce')
            )
            kdf['prelim'] = pd.to_datetime(kdf['rep_prelim_timestamp'], errors='coerce')

            kdf['exam_to_read_min']       = (kdf['prelim']   - kdf['exam_done']).dt.total_seconds() / 60
            kdf['signed_to_approved_min'] = (kdf['approved'] - kdf['prelim']).dt.total_seconds() / 60
            kdf['exam_to_approved_min']   = (kdf['approved'] - kdf['exam_done']).dt.total_seconds() / 60

            kdf['class_bucket'] = kdf.apply(_kpi_class_bucket, axis=1)
            kdf = kdf[kdf['class_bucket'].notna()]

            for (modality, class_bucket), cdf in kdf.groupby(['modality', 'class_bucket']):
                buckets = _KPI_BUCKETS_OUTPATIENT if class_bucket == 'Out' else _KPI_BUCKETS_STANDARD
                bucket_labels = [b[0] for b in buckets]

                block = {
                    'modality': modality,
                    'class_bucket': class_bucket,
                    'label': f"{modality}-{class_bucket}",
                    'bucket_labels': bucket_labels,
                    'stages': {},
                }

                # Ex. Done to Read — single aggregate row, not per-radiologist
                exam_read = cdf[cdf['exam_to_read_min'].notna() & (cdf['exam_to_read_min'] >= 0)].copy()
                exam_read['bucket'] = exam_read['exam_to_read_min'].apply(lambda m: _kpi_bucket_label(m, buckets))
                counts = exam_read['bucket'].value_counts().to_dict()
                block['stages']['Ex. Done to Read'] = {
                    'radiologists': [{
                        'name': 'Res.',
                        'counts': {label: int(counts.get(label, 0)) for label in bucket_labels},
                        'total': int(exam_read['bucket'].notna().sum()),
                    }]
                }

                # Signed 1 to Approved / Exam done to Approved — per radiologist
                for stage_name, col in [
                    ('Signed 1 to Approved', 'signed_to_approved_min'),
                    ('Exam done to Approved', 'exam_to_approved_min'),
                ]:
                    sdf = cdf[cdf[col].notna() & (cdf[col] >= 0) & cdf['radiologist'].notna()].copy()
                    sdf['bucket'] = sdf[col].apply(lambda m: _kpi_bucket_label(m, buckets))
                    rad_rows = []
                    for rad, rdf in sdf.groupby('radiologist'):
                        counts = rdf['bucket'].value_counts().to_dict()
                        rad_rows.append({
                            'name': rad,
                            'counts': {label: int(counts.get(label, 0)) for label in bucket_labels},
                            'total': int(rdf['bucket'].notna().sum()),
                        })
                    rad_rows.sort(key=lambda r: r['name'])
                    block['stages'][stage_name] = {'radiologists': rad_rows}

                blocks.append(block)
        blocks.sort(key=lambda b: (b['modality'], b['class_bucket']))
    except Exception:
        logger.exception("Failed to build KPI Detailed Reading data")
        db.session.rollback()

    return blocks


_RES_RAD_TAT_SQL_TEMPLATE = """
    WITH {anchor_cte}
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
            EXTRACT(EPOCH FROM (se.sign_time - {exam_done_col})) / 3600.0 AS tat_hours
        FROM signed_events se
        {anchor_join}
        LEFT JOIN role_lookup rl ON rl.login_id = SPLIT_PART(se.signer, '@', 1)
        WHERE se.sign_time > {exam_done_col}
    )
    SELECT
        COALESCE(role, '__unclassified__') AS role,
        SPLIT_PART(signer, '@', 1) AS resident_name,
        modality, patient_class_bucket,
        COUNT(*) AS n,
        ROUND(AVG(tat_hours)::numeric, 2) AS avg_tat_h,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tat_hours))::numeric, 2) AS median_tat_h,
        COUNT(*) FILTER (WHERE tat_hours <= 3)                   AS bucket_0_3h,
        COUNT(*) FILTER (WHERE tat_hours > 3 AND tat_hours <= 5) AS bucket_3_5h,
        COUNT(*) FILTER (WHERE tat_hours > 5)                    AS bucket_5h_plus
    FROM classified
    GROUP BY role, resident_name, modality, patient_class_bucket
    ORDER BY role, resident_name, modality, patient_class_bucket
"""

_RES_RAD_TAT_RIS_ANCHOR_CTE = """
    exam_done_anchor AS (
        SELECT p.study_instance_uid, MAX(w.exam_done_at) AS exam_done_time
        FROM std_worklist_exam_done w
        JOIN std_pps p ON p.pps_key = w.pps_key
        WHERE p.study_instance_uid IS NOT NULL
        GROUP BY p.study_instance_uid
    ),
"""


def _resident_radiologist_tat_variant(params, filter_clause, anchor):
    """anchor: 'ris' (WORKLIST_STATUS_HISTORY status_key=100, via std_pps.pps_key) or
    'pacs' (etl_didb_studies.insert_time -- "PACS end time calculation")."""
    result = {"residents": [], "radiologists": [], "unclassified_count": 0}
    if anchor == "ris":
        sql = _RES_RAD_TAT_SQL_TEMPLATE.format(
            anchor_cte=_RES_RAD_TAT_RIS_ANCHOR_CTE,
            anchor_join="JOIN exam_done_anchor ed ON ed.study_instance_uid = se.study_instance_uid",
            exam_done_col="ed.exam_done_time",
            filter_clause=filter_clause,
        )
    else:
        sql = _RES_RAD_TAT_SQL_TEMPLATE.format(
            anchor_cte="",
            anchor_join="",
            exam_done_col="se.insert_time",
            filter_clause=(filter_clause + " AND s.insert_time IS NOT NULL"),
        )
    try:
        rows = db.session.execute(text(sql), params).mappings().fetchall()
        for r in rows:
            row = dict(r)
            role = row.pop("role")
            if role == "radiologists":
                result["radiologists"].append(row)
            elif role == "residents":
                result["residents"].append(row)
            else:
                result["unclassified_count"] += row["n"]
    except Exception:
        logger.exception(f"Failed to compute resident/radiologist TAT ({anchor} anchor)")
        db.session.rollback()
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

    Two anchor variants computed and returned side by side -- "ris" (WORKLIST_STATUS_HISTORY
    status_key=100 "Exam Done", the RIS's own status transition, via std_worklist_exam_done ->
    std_pps.pps_key) is the default/primary; "pacs" (etl_didb_studies.insert_time, labeled
    "PACS end time calculation" in the UI) is the toggle alternative -- operator instruction
    2026-07-31. Every chart on this page must also honor the full left-sidebar filter set
    (date range, patient class, modality, AE title, patient location), not just dates --
    see utils/report_filters.sidebar_filters.

    Returns {"ris": {...}, "pacs": {...}}, each shaped {"residents": [...], "radiologists":
    [...], "unclassified_count": N}.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)
    return {
        "ris": _resident_radiologist_tat_variant(params, filter_clause, "ris"),
        "pacs": _resident_radiologist_tat_variant(params, filter_clause, "pacs"),
    }


_TECH_EFFICIENCY_SQL_TEMPLATE = """
    WITH {anchor_cte}
    arrival AS (
        SELECT p.study_instance_uid, MIN(wa.arrived_at) AS arrived_at
        FROM std_worklist_arrivals wa
        JOIN std_pps p ON p.pps_key = wa.pps_key
        WHERE p.study_instance_uid IS NOT NULL
        GROUP BY p.study_instance_uid
    ),
    eff AS (
        SELECT
            s.study_db_uid,
            UPPER(TRIM(s.storing_ae)) AS aetitle,
            COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
            CASE
                WHEN s.patient_location = 'ER' THEN 'ER'
                WHEN s.patient_class = 'I' THEN 'Inpatient'
                WHEN s.patient_class = 'O' THEN 'Outpatient'
                ELSE 'Other'
            END AS patient_class_bucket,
            COALESCE(pm.duration_minutes, 15) AS expected_min,
            EXTRACT(EPOCH FROM ({exam_done_col} - ar.arrived_at)) / 60.0 AS actual_min
        FROM etl_didb_studies s
        JOIN arrival ar ON ar.study_instance_uid = s.study_instance_uid
        {anchor_join}
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
        WHERE {exam_done_col} > ar.arrived_at
          AND s.study_date BETWEEN :start AND :end
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'PACS')
          {filter_clause}
    )
    SELECT
        aetitle, modality, patient_class_bucket,
        COUNT(*) AS n,
        ROUND(AVG(actual_min)::numeric, 1) AS avg_actual_min,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_min))::numeric, 1) AS median_actual_min,
        ROUND(AVG(expected_min)::numeric, 1) AS avg_expected_min,
        COUNT(*) FILTER (WHERE actual_min <= 30)                       AS bucket_0_30,
        COUNT(*) FILTER (WHERE actual_min > 30 AND actual_min <= 60)   AS bucket_30_60,
        COUNT(*) FILTER (WHERE actual_min > 60)                        AS bucket_60_plus
    FROM eff
    GROUP BY aetitle, modality, patient_class_bucket
    ORDER BY aetitle, modality, patient_class_bucket
"""

_TECH_EFFICIENCY_RIS_ANCHOR_CTE = """
    exam_done_anchor AS (
        SELECT p.study_instance_uid, MAX(w.exam_done_at) AS exam_done_time
        FROM std_worklist_exam_done w
        JOIN std_pps p ON p.pps_key = w.pps_key
        WHERE p.study_instance_uid IS NOT NULL
        GROUP BY p.study_instance_uid
    ),
"""


def _technician_efficiency_variant(params, filter_clause, anchor):
    """anchor: 'ris' (WORKLIST_STATUS_HISTORY status_key=100 Exam Done, via
    std_pps.pps_key) or 'pacs' (etl_didb_studies.insert_time, "PACS end time
    calculation")."""
    if anchor == "ris":
        sql = _TECH_EFFICIENCY_SQL_TEMPLATE.format(
            anchor_cte=_TECH_EFFICIENCY_RIS_ANCHOR_CTE,
            anchor_join="JOIN exam_done_anchor ed ON ed.study_instance_uid = s.study_instance_uid",
            exam_done_col="ed.exam_done_time",
            filter_clause=filter_clause,
        )
    else:
        sql = _TECH_EFFICIENCY_SQL_TEMPLATE.format(
            anchor_cte="",
            anchor_join="",
            exam_done_col="s.insert_time",
            filter_clause=(filter_clause + " AND s.insert_time IS NOT NULL"),
        )
    rows = []
    try:
        rows = db.session.execute(text(sql), params).mappings().fetchall()
    except Exception:
        logger.exception(f"Failed to compute technician efficiency ({anchor} anchor)")
        db.session.rollback()
    return [dict(r) for r in rows]


def get_technician_efficiency(form_data):
    """
    Technician Efficiency (arrived -> exam done) per AE title per modality per
    patient class (Inpatient/Outpatient/ER), actual duration vs. the
    procedure_duration_map expected duration side by side (operator instruction
    2026-07-31: renamed from "Patient Wait Time" -- that name now means Scheduled ->
    Arrived instead, see get_patient_wait_time).

    "Arrived" comes from std_worklist_arrivals (RIS WORKLIST_STATUS_HISTORY,
    status_key=60), NOT hl7_orders.arrived_at -- that column was built for this exact
    purpose but is empty, since it depends on the live HL7 ORM feed from R2I, which
    isn't flowing yet.

    IMPORTANT CAVEAT (operator, 2026-07-31): for Inpatient orders this is normal
    workflow, not a data bug -- staff set "Arrived" well before the actual exam
    specifically to trigger the DICOM Modality Worklist (so the device/portable unit
    can pull the order), then mark the study done once they return with images. So
    Inpatient numbers here reflect order-to-DMWL-trigger-to-completion lag, not
    literal exam duration the way it does for Outpatient/ER. Surfaced with a caveat
    in the UI rather than excluded, per operator instruction.

    Two anchor variants for "exam done" (see _resident_radiologist_tat_variant's
    docstring for the same ris/pacs split) -- "ris" (WORKLIST_STATUS_HISTORY
    status_key=100) is default/primary, "pacs" (insert_time, "PACS end time
    calculation") is the toggle alternative. Full left-sidebar filter set applies.

    Returns {"ris": [...], "pacs": [...]}, each a flat list of per-(aetitle,
    modality, patient_class_bucket) rows.
    """
    params, filter_clause, _start, _end = _sidebar_filters(form_data)
    return {
        "ris": _technician_efficiency_variant(params, filter_clause, "ris"),
        "pacs": _technician_efficiency_variant(params, filter_clause, "pacs"),
    }


def get_patient_wait_time(form_data):
    """
    Patient Wait Time (Scheduled -> Arrived) per modality per patient class
    (Inpatient/Outpatient/ER) -- redefined 2026-07-31 (operator instruction); the OLD
    Patient Wait Time (Arrived -> Exam Done) is now Technician Efficiency, see
    get_technician_efficiency.

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


def get_reporting_cadence_and_insights(form_data, rad_cards):
    """
    Reporting Cadence Analysis (per-radiologist signing density heatmap + daily
    arrival/departure/break log) and the statistical insights panel, ported from
    report_25's compute_bg_data(). Both derive from the same rep_final_timestamp
    signing-event query, so it's run once and shared -- matches the original.

    rad_cards comes from report_25.get_gold_standard_data()'s result (per-radiologist
    RVU/TAT performance cards) -- run_rad_insights needs it to compute skew/batch-
    signing signals; report_36's caller already has it from the one
    get_gold_standard_data() call this page makes for the Workload Matrix / Technician
    TAT by AE Station sections, so no extra query here.

    Returns (shift_patterns, rad_insights).
    """
    params, filter_clause, start, end = _sidebar_filters(form_data)

    shift_patterns = {}
    rad_insights = []
    ts_rows = []
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

    try:
        _signing_df = None
        if ts_rows:
            _signing_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts', 'accession_number'])
        rad_insights = run_rad_insights(rad_cards, _signing_df)
    except Exception:
        logger.exception("Failed to run radiologist insight signals")

    return shift_patterns, rad_insights


@report_36_bp.route("/report/36", methods=["GET", "POST"])
@login_required
def report_36():
    classes = locations = modalities = aetitles = []
    run_report = 'start_date' in request.values

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    kpi_data = res_rad_tat = wait_time_data = tech_efficiency_data = None
    rad_volume_matrix = tech_tat_cards = shift_patterns = rad_insights = None

    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_36',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date')})

        # Reused for the Workload Matrix / Technician TAT by AE Station / insights panel
        # -- see get_reporting_cadence_and_insights' docstring for why this isn't
        # duplicated as its own query here.
        gold_data, display_start, display_end = get_gold_standard_data(request.values)
        rad_volume_matrix = (gold_data or {}).get('rad_volume_matrix')
        tech_tat_cards    = (gold_data or {}).get('tech_tat_cards')
        rad_cards         = (gold_data or {}).get('rad_cards', [])

        kpi_data              = get_kpi_detailed_reading(request.values)
        res_rad_tat            = get_resident_radiologist_tat(request.values)
        wait_time_data         = get_patient_wait_time(request.values)
        tech_efficiency_data   = get_technician_efficiency(request.values)
        shift_patterns, rad_insights = get_reporting_cadence_and_insights(request.values, rad_cards)

    return render_template(
        "report_36.html",
        kpi_data=kpi_data,
        res_rad_tat=res_rad_tat,
        wait_time_data=wait_time_data,
        tech_efficiency_data=tech_efficiency_data,
        rad_volume_matrix=rad_volume_matrix,
        tech_tat_cards=tech_tat_cards,
        shift_patterns=shift_patterns,
        rad_insights=rad_insights,
        display_start=display_start, display_end=display_end,
        classes=classes, locations=locations, modalities=modalities, aetitles=aetitles,
        run_report=run_report,
    )


from routes.report_registry import register_report
register_report(36, report_36_bp, report_36, None)
