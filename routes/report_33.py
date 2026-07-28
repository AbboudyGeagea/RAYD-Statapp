"""
routes/report_33.py
--------------------
Report 33 — KPI Detailed Reading.

Standalone single-topic report split out of Report 25's "Radiologists" tab.
Bucketed multi-stage TAT (turnaround-time) per modality x patient-class x
radiologist, matching the hospital's own "KPI Detailed reading.xlsx" format.

This report's backend is fully independent of the shared report_template
(id=25) SQL — it runs its own query directly against etl_didb_studies /
hl7_orders / hl7_oru_reports / std_resources_ris. Ported as-is from
routes/report_25.py's get_kpi_detailed_reading() (previously lines 81-242),
including the module-level comment block documenting open, unconfirmed
assumptions — see below. Those assumptions are NOT resolved here; they are
carried forward verbatim as documented uncertainties, not bugs.

Register in registry.py:
    import routes.report_33
"""
import logging
import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from utils.site_resolver import default_site

logger = logging.getLogger("report_33")

report_33_bp = Blueprint("report_33", __name__)

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


@report_33_bp.route("/report/33", methods=["GET", "POST"])
@login_required
def report_33():
    go_live = get_etl_cutoff_date()
    default_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    default_end = date.today().strftime("%Y-%m-%d")

    display_start = request.values.get("start_date", default_start)
    display_end = request.values.get("end_date", default_end)

    run_report = "start_date" in request.values
    kpi_data = None

    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_33',
                  detail={'from': display_start, 'to': display_end})
        kpi_data = get_kpi_detailed_reading(request.values)

    return render_template(
        "report_33.html",
        kpi_data=kpi_data,
        run_report=run_report,
        display_start=display_start,
        display_end=display_end,
    )


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(33, report_33_bp, report_33, None)
