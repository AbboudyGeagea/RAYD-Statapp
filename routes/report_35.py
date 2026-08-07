"""
routes/report_35.py
--------------------
Report 35 — Technician TAT & Compliance Monitoring.

Standalone extraction of report_25's "Technicians" tab. Ports report_25's
technician-monitoring section from compute_bg_data() (routes/report_25.py,
lines ~817-1219 originally) — the section of that function that was already
independent of the shared report_template id=25 query used by report_25's
other tabs:

  - Technician monitoring: per-technician / per-modality TAT compliance,
    flagged exams (negative-TAT / too-early / overlap / too-late),
    never-marked-done overdue exams, daily TAT trend. Runs its own SQL
    directly against the RIS PPS status-change tables (std_worklist_arrivals
    / _scheduled / _exam_done + std_pps + std_resources_ris + procedure_duration_map)
    -- NOT hl7_orders, see get_technician_tat_data()'s docstring for why.

NOTE: this report previously also carried a "Reporting Cadence Analysis"
(per-radiologist signing-time heatmap, derived from
etl_didb_studies.rep_final_timestamp) here, only because it happened to be
bundled into report_25's tech compute function for reuse. That data is
radiologist behaviour, not technician — it has been moved to Report 32
("Radiologists Performance", routes/report_32.py), which now computes it
independently with its own self-contained query.

The technician-monitoring section + the flagged-exam acknowledgement feature
are ported verbatim (same queries/logic), just re-homed under report id 35
instead of 25, with a dedicated cache slot (cache_get(35, ...)/cache_put(35,
...)) so this report's cache never collides with report_25's own (25) or its
deferred-background (9925) cache slots.

Unlike report_25, this report does NOT depend on report_template id=25 —
so unlike report_25 (which defers this exact data to a background
/report/25/bg endpoint to keep its much heavier main page fast), this
report computes synchronously in the main GET/POST view. The underlying
queries here are bounded by date range only (no joins against the big
report_template CTE), so there's no other heavy work on this page for it
to compete with — a plain synchronous route is simpler and was preferred
per the task brief.

Flagged-exam acknowledgements are stored in the shared `tech_flag_acknowledgements`
table (db.TechFlagAck), keyed by (accession_number, flag_date) — NOT by
report id. Acks made here are keyed the same way report_25's live page
keys them, so acknowledgement state is naturally shared/consistent between
this page and report_25's existing Technicians tab (no migration needed).

Register in registry.py:
    import routes.report_35
"""
import io
import logging
import statistics as _stats
import time
from datetime import date, datetime

import numpy as np
import pandas as pd
from flask import Blueprint, render_template, request, send_file, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put, cache_invalidate
from utils.site_resolver import default_site

logger = logging.getLogger("report_35")

report_35_bp = Blueprint("report_35", __name__)

_REPORT_ID = 35

# Default PACS-vs-RIS exam-done significance threshold (minutes) when a modality has no
# 'pacs_ris_diff_threshold:<MODALITY>' override in `settings` -- see get_technician_tat_data().
_DEFAULT_PACS_RIS_THRESHOLD_MIN = 30


def get_technician_tat_data(form_data):
    """
    Main data-fetch for Report 35. Returns a dict: { 'tech_data': {...} }.

    Anchored on RIS PPS status-change tables (std_worklist_arrivals / _scheduled /
    _exam_done + std_pps), NOT hl7_orders -- that feed depends on the R2I HL7 ORM
    order feed, which was never turned on (see report_25.py's module docstring: the
    identical hl7_orders-based tab was removed from report_25 for this exact reason).
    TAT = RIS Exam-Done timestamp minus Arrived timestamp, same tables/definition as
    report_36.get_technician_efficiency (removed from report_36 in favor of this
    per-technologist version) -- extended here to resolve a real technologist name
    instead of report_36's per-AE-device grouping.

    Technologist identity does NOT come from std_pps.primary_tech_person_key -- that
    column was confirmed 100% NULL in production and abandoned. Instead it resolves via
    std_pps_person_reference, a per-PPS list of person references cross-referenced
    against std_resources_ris.role_code = 'TEC' (NOT by trusting
    person_reference_type_key directly, since that column mixes technologists with
    receptionists/nurses/radiologists under its most common value). A PPS can carry more
    than one qualifying TEC reference, so `tech_ref` picks one deterministically via
    DISTINCT ON ordered by display_sort_order ASC NULLS LAST (lowest = primary).

    Also flags 'pacs_ris_mismatch' when PACS's own insert_time (etl_didb_studies) and
    the RIS's self-reported exam_done_at disagree by more than a per-modality
    significance threshold (settings key 'pacs_ris_diff_threshold:<MODALITY>', minutes;
    falls back to _DEFAULT_PACS_RIS_THRESHOLD_MIN when a modality has no override) -- a
    cross-system data-integrity check, independent of the TAT-based flags.

    CAVEAT carried over from report_36's removed docstring (operator, 2026-07-31): for
    Inpatient orders, "Arrived" is normal workflow set well before the actual exam,
    specifically to trigger the DICOM Modality Worklist (so the device/portable unit can
    pull the order) -- staff mark the study done once they return with images. So
    Inpatient TAT numbers here reflect order-to-DMWL-trigger-to-completion lag, not
    literal exam duration the way they do for Outpatient/ER. Not a data bug to chase.
    """
    cached = cache_get(_REPORT_ID, form_data)
    if cached is not None:
        return cached

    # Temporary timing instrumentation (2026-08-04, operator request): logs how
    # long each phase of this function takes so a slow real-world date range
    # can be diagnosed from production logs instead of guessed at. Remove once
    # the report_35 slowness investigation is closed out.
    _t_start = time.perf_counter()
    _t_prev = [_t_start]

    def _lap(label, **extra):
        now = time.perf_counter()
        elapsed = now - _t_prev[0]
        _t_prev[0] = now
        extra_str = (' ' + ' '.join(f'{k}={v}' for k, v in extra.items())) if extra else ''
        logger.info(f"[report_35][timing] {label}: {elapsed:.3f}s{extra_str}")

    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end   = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")

    params = {"start": start, "end": end}
    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        params["modalities"] = tuple(form_data.getlist("modality"))

    # LAUMC site rule (see report_25.py's get_gold_standard_data for full rationale):
    # reports show RH (main site) only, SJH (satellite) excluded, for now.
    # default_site() resolves to None (filter skipped) on a non-LAUMC/single-site
    # install rather than zeroing every report.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id

    # ── Technician monitoring ─────────────────────────────────────────────
    tech_data = {
        'summary': {}, 'by_technician': [], 'by_modality': [],
        'flagged': [], 'never_done': [], 'daily_trend': [],
        'jci_compliance': {'by_tier': [], 'overall_pct': None, 'overall_median_min': None},
    }
    _jci_thresholds = {}  # populated below; kept defined here so it's safe to read even
                           # if an earlier query in the try block fails first
    try:
        _now = datetime.utcnow()

        # Base query starts from std_worklist_arrivals, not etl_didb_studies: an order
        # that arrived but was never scanned has no std_pps row at all (etl_ris_pps.py
        # only pulls once Oracle PPS.START_DATETIME exists) and therefore no PACS study
        # either, so everything past `arrival` is a LEFT JOIN. This keeps completed AND
        # never-done exams in one fetch, same shape as before (single fetch, split into
        # completed/pending in pandas below).
        #
        # Modality/site filters are applied in an outer WHERE against the already-
        # COALESCEd `base` columns rather than reusing utils.report_filters.sidebar_filters()
        # verbatim -- that helper assumes m/s always resolve (every other report starts
        # FROM etl_didb_studies), but here m/s can be legitimately NULL for never-started
        # exams, and its "AND m.site_id = :rh_site_id" clause would silently drop those.
        _filters = []
        if "modalities" in params:
            _filters.append("UPPER(TRIM(base.modality)) IN :modalities")
        if rh_site_id is not None:
            _filters.append("(base.site_id IS NULL OR base.site_id = :rh_site_id)")
        _filter_clause = ("WHERE " + " AND ".join(_filters)) if _filters else ""

        # Per-modality PACS-vs-RIS significance threshold (operator instruction: must be
        # configurable per modality, not one fixed number). Stored as settings rows keyed
        # 'pacs_ris_diff_threshold:<MODALITY>' (minutes), same key-per-item convention as
        # e.g. 'oru_crit:<keyword>' elsewhere in this app. No admin UI for these yet --
        # set/adjust via a settings row directly until one exists. Modalities with no
        # override fall back to _DEFAULT_PACS_RIS_THRESHOLD_MIN.
        _pacs_ris_thresholds = dict(db.session.execute(text(
            "SELECT key, value FROM settings WHERE key LIKE 'pacs_ris_diff_threshold:%'"
        )).fetchall())
        _pacs_ris_thresholds = {
            k.split(':', 1)[1].upper(): float(v)
            for k, v in _pacs_ris_thresholds.items()
            if v is not None and str(v).strip()
        }
        _lap("settings thresholds query")

        # JCI order-to-acquisition compliance targets, per priority tier (operator
        # instruction, 2026-08-07: no fixed policy number exists yet -- must be
        # admin-editable on this page since policy may change or need tuning against
        # real data). Same prefixed-settings-key convention as
        # 'pacs_ris_diff_threshold:<MODALITY>' above, keyed by priority code instead:
        # 'jci_threshold:<CODE>' (minutes). Tiers with no threshold set are shown in the
        # raw distribution only -- no pass/fail judgement is made for them.
        _jci_thresholds = dict(db.session.execute(text(
            "SELECT key, value FROM settings WHERE key LIKE 'jci_threshold:%'"
        )).fetchall())
        _jci_thresholds = {
            k.split(':', 1)[1]: float(v)
            for k, v in _jci_thresholds.items()
            if v is not None and str(v).strip()
        }
        _lap("JCI threshold settings query")

        # scheduled/exam_done/tech_ref are LATERAL subqueries keyed on ar.pps_key, NOT
        # pre-aggregated CTEs over their full source tables -- std_pps_person_reference
        # alone is 667k+ rows; aggregating it (and std_worklist_scheduled/_exam_done)
        # unfiltered before joining down to `arrival`'s much smaller date-bounded row set
        # made this query take "a decade" in production (operator report). A LATERAL
        # join lets each of these use their existing pps_key index (migrations
        # 0088/0090/0092/0097) per arrival row instead of scanning/aggregating the whole
        # table up front. Same reasoning as CLAUDE.md's "expensive CTEs must use
        # MATERIALIZED" convention, just solved by scoping the work down instead.
        #
        # `arrived_at >= CAST(:start AS date) AND arrived_at < CAST(:end AS date) + 1`
        # (not `arrived_at::date BETWEEN :start AND :end`) so the filter is sargable
        # against idx_worklist_arrivals_arrived_at (migration 0098) -- casting the
        # *column* blocks index use even when one exists. CAST(), not `:start::date` --
        # a bind parameter directly followed by Postgres's :: cast operator confuses
        # SQLAlchemy's colon-parameter parser and corrupts the compiled SQL (confirmed
        # in production, 2026-08-01: "syntax error at or near ':'" on this exact line,
        # silently swallowed by this function's broad except-and-log, which is why the
        # report kept showing empty instead of erroring visibly).
        tech_rows = db.session.execute(text(f"""
            WITH arrival AS (
                SELECT pps_key, MIN(arrived_at) AS arrived_at, MAX(sps_id) AS sps_id
                FROM std_worklist_arrivals
                WHERE arrived_at >= CAST(:start AS date) AND arrived_at < CAST(:end AS date) + 1
                GROUP BY pps_key
            ),
            base AS (
                SELECT
                    ar.pps_key,
                    COALESCE(s.accession_number, ar.sps_id, 'WL#' || ar.pps_key::text) AS accession_number,
                    COALESCE(m.modality, s.study_modality, 'Unknown')       AS modality,
                    m.site_id,
                    pps.procedure_code,
                    s.patient_class, s.patient_location,
                    tr.tech_name AS done_by,
                    sc.scheduled_at, ar.arrived_at, ed.exam_done_at,
                    pps.start_datetime AS scan_start_at,
                    pps.end_datetime AS scanner_done_at,
                    s.insert_time AS pacs_insert_time,
                    COALESCE(pdm.duration_minutes, 30) AS proc_duration,
                    pri.code AS priority_code, pri.description AS priority_desc
                FROM arrival ar
                LEFT JOIN std_pps pps            ON pps.pps_key = ar.pps_key
                LEFT JOIN etl_didb_studies s      ON s.study_instance_uid = pps.study_instance_uid
                LEFT JOIN aetitle_modality_map m  ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(COALESCE(s.storing_ae, pps.performing_ae_title)))
                LEFT JOIN std_procedure_priorities pri ON pri.priority_key = pps.priority_key
                LEFT JOIN LATERAL (
                    SELECT MIN(scheduled_at) AS scheduled_at
                    FROM std_worklist_scheduled sc0
                    WHERE sc0.pps_key = ar.pps_key
                ) sc ON true
                LEFT JOIN LATERAL (
                    SELECT MAX(exam_done_at) AS exam_done_at
                    FROM std_worklist_exam_done ed0
                    WHERE ed0.pps_key = ar.pps_key
                ) ed ON true
                LEFT JOIN LATERAL (
                    SELECT COALESCE(NULLIF(TRIM(CONCAT(res.first_name, ' ', res.last_name)), ''), res.common_name) AS tech_name
                    FROM std_pps_person_reference ppr
                    JOIN std_resources_ris res ON res.resource_id_key = ppr.resource_id_key
                    WHERE ppr.pps_key = ar.pps_key AND res.role_code = 'TEC'
                    ORDER BY ppr.display_sort_order ASC NULLS LAST
                    LIMIT 1
                ) tr ON true
                LEFT JOIN procedure_duration_map pdm ON UPPER(TRIM(pps.procedure_code)) = UPPER(TRIM(pdm.procedure_code))
                WHERE COALESCE(m.modality, s.study_modality, '') != 'SR'
            )
            SELECT accession_number, modality, procedure_code, done_by,
                   patient_class, patient_location,
                   scheduled_at, arrived_at, exam_done_at, scan_start_at, scanner_done_at,
                   pacs_insert_time, proc_duration, priority_code, priority_desc
            FROM base
            {_filter_clause}
            ORDER BY modality, arrived_at
        """), params).mappings().fetchall()
        _lap("main tech_rows SQL query (arrival+base CTE, LATERAL joins)", rows=len(tech_rows))

        if tech_rows:
            tdf = pd.DataFrame(tech_rows)
            tdf['proc_duration']   = pd.to_numeric(tdf['proc_duration'], errors='coerce').fillna(30)
            tdf['arrived_at']      = pd.to_datetime(tdf['arrived_at'])
            tdf['exam_done_at']    = pd.to_datetime(tdf['exam_done_at'],    errors='coerce')
            tdf['scanner_done_at'] = pd.to_datetime(tdf['scanner_done_at'], errors='coerce')
            tdf['pacs_insert_time'] = pd.to_datetime(tdf['pacs_insert_time'], errors='coerce')
            tdf['tat_min']         = (tdf['exam_done_at']    - tdf['arrived_at']).dt.total_seconds() / 60.0
            tdf['pacs_tat_min']    = (tdf['scanner_done_at'] - tdf['arrived_at']).dt.total_seconds() / 60.0
            # PACS-vs-RIS cross-system consistency check: how far apart is PACS's own
            # insert_time from the RIS's self-reported exam_done_at for the SAME exam.
            # Signed (not abs()) so "PACS registered before RIS marked done" is visible
            # too, not just the more intuitive "PACS lagging RIS" direction.
            tdf['pacs_ris_diff_min'] = (tdf['pacs_insert_time'] - tdf['exam_done_at']).dt.total_seconds() / 60.0
            # JCI order-to-acquisition interval: order-scheduled time to actual scan
            # start (std_pps.start_datetime) -- NOT exam_done_at, which marks RIS
            # completion status, not acquisition start.
            tdf['scan_start_at'] = pd.to_datetime(tdf['scan_start_at'], errors='coerce')
            tdf['scheduled_at']  = pd.to_datetime(tdf['scheduled_at'],  errors='coerce')
            tdf['jci_interval_min'] = (tdf['scan_start_at'] - tdf['scheduled_at']).dt.total_seconds() / 60.0
            tdf['priority_code'] = tdf['priority_code'].fillna('UNSPECIFIED')
            tdf['priority_desc'] = tdf['priority_desc'].fillna('Unspecified')
            _lap("dataframe construction + dtype conversion", rows=len(tdf))

            completed = tdf[tdf['exam_done_at'].notna()].copy()
            pending   = tdf[tdf['exam_done_at'].isna()].copy()
            _lap("completed/pending split", completed=len(completed), pending=len(pending))

            # Pre-index ER orders: modality → sorted arrays of (arrived_at, accession,
            # patient_class). An order is "ER" if accession_number starts with '2XE'
            # (case-insensitive). Sorted + numpy so _find_concurrent_er can binary-search
            # the arrival-time range instead of linear-scanning every ER row per lookup --
            # matters on ER-heavy modalities (CT/CR) over wide date ranges, where that scan
            # was O(too_late_flagged_count x ER_count_for_that_modality).
            if 'patient_class' not in tdf.columns:
                tdf['patient_class'] = None
            er_rows = tdf[tdf['accession_number'].str.upper().str.startswith('2XE').fillna(False)].copy()
            er_by_modality = {}
            for mod, grp in er_rows.groupby(er_rows['modality'].fillna('').astype(str).str.upper()):
                grp = grp.sort_values('arrived_at')
                er_by_modality[mod] = {
                    'arrived_at':    grp['arrived_at'].to_numpy(),
                    'accession':     grp['accession_number'].to_numpy(),
                    'patient_class': grp['patient_class'].to_numpy(),
                }
            _lap("ER index build", er_rows=len(er_rows))

            def _find_concurrent_er(row):
                """Return list of ER accessions whose arrived_at falls inside row's exam window."""
                if pd.isna(row.get('exam_done_at')):
                    return []
                mod = str(row.get('modality') or '').upper()
                bucket = er_by_modality.get(mod)
                if bucket is None:
                    return []
                arrived = bucket['arrived_at']
                t0  = np.datetime64(row['arrived_at'])
                t1  = np.datetime64(row['exam_done_at'])
                acc = row.get('accession_number')
                lo = np.searchsorted(arrived, t0, side='left')
                hi = np.searchsorted(arrived, t1, side='right')
                found = []
                for i in range(lo, hi):
                    er_acc = bucket['accession'][i]
                    if er_acc == acc:
                        continue
                    pc = bucket['patient_class'][i]
                    found.append({
                        'accession':     str(er_acc or ''),
                        'patient_class': str(pc) if pd.notna(pc) else '',
                    })
                return found

            # Overlap = cur.exam_done_at > next-row.arrived_at, both within the same
            # modality, rows ordered by arrived_at. Global sort + numpy np.roll shift
            # instead of a per-modality Python loop with .iloc[i]/.iloc[i+1] -- each
            # .iloc[] call reconstructs a pandas Series (dtype coercion across every
            # column), same class of cost as .iterrows(). Production logs showed this
            # loop alone taking 39s at ~250k completed rows on a wide date range;
            # verified equivalent via a standalone equivalence test (incl. modality-
            # boundary and single-row-group edge cases) before landing this change.
            completed = completed.sort_values(['modality', 'arrived_at']).reset_index(drop=True)
            _mod_arr        = completed['modality'].to_numpy()
            _arrived_arr    = completed['arrived_at'].to_numpy()
            _exam_done_arr  = completed['exam_done_at'].to_numpy()
            _acc_arr        = completed['accession_number'].to_numpy()
            _nxt_mod        = np.roll(_mod_arr, -1)
            _nxt_arrived    = np.roll(_arrived_arr, -1)
            _overlap_mask   = (_mod_arr == _nxt_mod) & ~pd.isna(_exam_done_arr) & (_exam_done_arr > _nxt_arrived)
            if len(_overlap_mask):
                _overlap_mask[-1] = False  # np.roll wraps the last row's "next" to row 0 -- not a real neighbor
            overlap_accessions = set(_acc_arr[_overlap_mask])
            _lap("overlap detection", overlaps=len(overlap_accessions))

            # Flags computed as numpy boolean arrays over the whole `completed` frame at
            # once, instead of a per-row .iterrows() loop -- iterrows() reconstructs a
            # pandas Series (with dtype coercion across every column) for each row, which
            # dominates wall time on wide date ranges. Same if/elif semantics as before,
            # just evaluated as arrays first. Output dicts (strftime calls, ER lookup) are
            # then only built for rows that end up flagged -- previously every completed
            # row got a dict built and the unflagged ones (the majority, on a healthy
            # department) were discarded right after by the `if r['flags']` filter.
            # completed is already index-reset from the overlap sort above.
            tat_arr = completed['tat_min'].to_numpy(dtype=float)
            dur_arr = completed['proc_duration'].to_numpy(dtype=float)
            valid_tat = ~np.isnan(tat_arr)

            is_negative  = valid_tat & (tat_arr < 0)
            is_too_early = valid_tat & ~is_negative & (tat_arr < dur_arr * 0.5)
            is_overlap   = completed['accession_number'].isin(overlap_accessions).to_numpy()
            is_too_late  = valid_tat & (tat_arr > dur_arr * 2)

            mod_upper = completed['modality'].fillna('').astype(str).str.upper()
            threshold_map = {
                m: _pacs_ris_thresholds.get(m, _DEFAULT_PACS_RIS_THRESHOLD_MIN)
                for m in mod_upper.unique()
            }
            threshold_arr = mod_upper.map(threshold_map).to_numpy(dtype=float)
            pacs_ris_diff_arr = completed['pacs_ris_diff_min'].to_numpy(dtype=float)
            is_mismatch = ~np.isnan(pacs_ris_diff_arr) & (np.abs(pacs_ris_diff_arr) > threshold_arr)

            has_any_flag = valid_tat & (is_negative | is_too_early | is_overlap | is_too_late | is_mismatch)
            flagged_idx  = np.nonzero(has_any_flag)[0]
            _lap("flag vectorization", flagged=len(flagged_idx))

            # Row formatting for the flagged subset, vectorized: one bulk .iloc[]
            # slice instead of N per-row .iloc[i] calls, and .dt.strftime() applied
            # once per column instead of one Python .strftime() call per row per
            # column. Production logs showed the old per-row loop taking ~30s at
            # ~350k flagged rows (a department that's mostly flagged during a wide
            # date range, so the "only format flagged rows" change alone wasn't
            # enough -- verified equivalent via a standalone test before landing).
            fc     = completed.iloc[flagged_idx].reset_index(drop=True)
            f_tat  = tat_arr[flagged_idx]
            f_dur  = dur_arr[flagged_idx]
            f_is_negative  = is_negative[flagged_idx]
            f_is_too_early = is_too_early[flagged_idx]
            f_is_overlap   = is_overlap[flagged_idx]
            f_is_too_late  = is_too_late[flagged_idx]
            f_is_mismatch  = is_mismatch[flagged_idx]

            def _fmt_col(col):
                s = fc[col].dt.strftime('%Y-%m-%d %H:%M')
                return s.where(fc[col].notna(), None).to_numpy()

            f_arrived_s     = fc['arrived_at'].dt.strftime('%Y-%m-%d %H:%M').to_numpy()
            f_done_s        = _fmt_col('exam_done_at')
            f_pacs_done_s   = _fmt_col('scanner_done_at')
            f_pacs_insert_s = _fmt_col('pacs_insert_time')

            f_acc_arr    = fc['accession_number'].to_numpy()
            f_mod_arr    = fc['modality'].to_numpy()
            f_proc_arr   = fc['procedure_code'].to_numpy()
            f_tech_arr   = fc['done_by'].to_numpy()
            f_tech_notna = fc['done_by'].notna().to_numpy()
            f_pclass_arr = fc['patient_class'].to_numpy()
            f_pacs_tat_arr  = fc['pacs_tat_min'].to_numpy(dtype=float)
            f_pacs_diff_arr = fc['pacs_ris_diff_min'].to_numpy(dtype=float)

            flagged_rows = []
            for i in range(len(fc)):
                flags = []
                if f_is_negative[i]: flags.append('negative_tat')
                elif f_is_too_early[i]: flags.append('too_early')
                if f_is_overlap[i]: flags.append('overlap')
                if f_is_too_late[i]: flags.append('too_late')
                if f_is_mismatch[i]: flags.append('pacs_ris_mismatch')
                # ER-overlap lookup still needs the full row (arrived_at/exam_done_at
                # as Timestamps, not pre-formatted strings) -- only called for the
                # (usually small) too_late subset, same scoping as before.
                er_concurrent = _find_concurrent_er(fc.iloc[i]) if 'too_late' in flags else []
                flagged_rows.append({
                    'accession':       str(f_acc_arr[i] or ''),
                    'modality':        str(f_mod_arr[i] or ''),
                    'procedure':       str(f_proc_arr[i] or ''),
                    'technician':      str(f_tech_arr[i]) if f_tech_notna[i] else '',
                    'patient_class':   str(f_pclass_arr[i] or ''),
                    'arrived_at':      f_arrived_s[i],
                    'done_at':         f_done_s[i],
                    'tat_min':         round(float(f_tat[i]), 1),
                    'pacs_done_at':    f_pacs_done_s[i],
                    'pacs_tat_min':    round(float(f_pacs_tat_arr[i]), 1) if not np.isnan(f_pacs_tat_arr[i]) else None,
                    'pacs_insert_at':  f_pacs_insert_s[i],
                    'pacs_ris_diff':   round(float(f_pacs_diff_arr[i]), 1) if not np.isnan(f_pacs_diff_arr[i]) else None,
                    'proc_duration':   int(f_dur[i]),
                    'flags':           flags,
                    'er_concurrent':   er_concurrent,
                })
            tech_data['flagged'] = sorted(flagged_rows, key=lambda x: len(x['flags']), reverse=True)
            _lap("flagged row formatting (vectorized strftime + ER lookup)", flagged=len(flagged_rows))

            # Same treatment for overdue pending exams: compute the deadline check as a
            # vectorized comparison, then only build output dicts for the (usually small)
            # overdue subset instead of every still-open exam.
            pending = pending.reset_index(drop=True)
            if len(pending):
                deadline_arr = pending['arrived_at'] + pd.to_timedelta(pending['proc_duration'].astype(float), unit='m')
                now_ts = pd.Timestamp(_now)
                overdue_idx = np.nonzero((deadline_arr < now_ts).to_numpy())[0]
                for i in overdue_idx:
                    r = pending.iloc[i]
                    deadline = deadline_arr.iloc[i]
                    tech_data['never_done'].append({
                        'accession':   str(r.get('accession_number') or ''),
                        'modality':    str(r.get('modality') or ''),
                        'procedure':   str(r.get('procedure_code') or ''),
                        'arrived_at':  r['arrived_at'].strftime('%Y-%m-%d %H:%M'),
                        'overdue_min': round((now_ts - deadline).total_seconds() / 60, 1),
                    })
            _lap("never_done computation", never_done=len(tech_data['never_done']))

            flagged_accessions = {r['accession'] for r in tech_data['flagged']}
            tech_data['summary'] = {
                'total_arrived':         len(tdf),
                'total_completed':       len(completed),
                'never_done':            len(tech_data['never_done']),
                'flag_negative_tat':     sum(1 for r in tech_data['flagged'] if 'negative_tat'     in r['flags']),
                'flag_too_early':        sum(1 for r in tech_data['flagged'] if 'too_early'        in r['flags']),
                'flag_overlap':          sum(1 for r in tech_data['flagged'] if 'overlap'          in r['flags']),
                'flag_too_late':         sum(1 for r in tech_data['flagged'] if 'too_late'         in r['flags']),
                'flag_pacs_ris_mismatch': sum(1 for r in tech_data['flagged'] if 'pacs_ris_mismatch' in r['flags']),
            }
            _lap("summary flag counts")

            daily_trend = []
            if len(completed):
                completed = completed.copy()
                completed['_date'] = completed['arrived_at'].dt.date
                for day, gdf in completed.groupby('_date'):
                    tats = gdf['tat_min'].dropna()
                    daily_trend.append({
                        'date':    str(day),
                        'avg_tat': round(float(tats.mean()), 1) if len(tats) else 0,
                        'count':   len(gdf),
                        'flags':   int(gdf['accession_number'].isin(flagged_accessions).sum()),
                    })
                daily_trend.sort(key=lambda x: x['date'])
            tech_data['daily_trend'] = daily_trend
            _lap("daily trend computation", days=len(daily_trend))

            def _skew_insight(avg, median):
                if avg is None or median is None or median == 0: return None
                ratio = avg / median
                if ratio >= 2.0: return f"Avg is {ratio:.1f}× the median — outliers inflating average."
                if ratio >= 1.5: return f"Avg is {ratio:.1f}× the median — some delayed exams pulling up average."
                return None

            # Dept averages computed first so per-tech delta can reference them
            # TAT > 24h (1440 min) outliers are excluded from dept-level stats
            all_tats = completed['tat_min'].dropna()
            normal_dept_tats = all_tats[all_tats <= 1440]
            dept_avg = dept_median = None
            if len(normal_dept_tats):
                dept_avg    = round(float(normal_dept_tats.mean()),   1)
                dept_median = round(float(normal_dept_tats.median()), 1)
                tech_data['summary']['avg_tat']      = dept_avg
                tech_data['summary']['median_tat']   = dept_median
                tech_data['summary']['dept_insight'] = _skew_insight(dept_avg, dept_median)
            _lap("dept averages")

            # Per-tech modality breakdown (pure pandas, no extra query)
            _done_with_tech = completed[
                completed['done_by'].notna() & completed['modality'].notna()
            ]
            tech_mod_breakdown = {}
            for (tech, mod), gdf in _done_with_tech.groupby(['done_by', 'modality']):
                tats  = gdf['tat_min'].dropna()
                ptats = gdf['pacs_tat_min'].dropna()
                tech_mod_breakdown.setdefault(str(tech), []).append({
                    'modality':     str(mod),
                    'count':        len(gdf),
                    'avg_tat':      round(float(tats.mean()),  1) if len(tats)  else None,
                    'avg_pacs_tat': round(float(ptats.mean()), 1) if len(ptats) else None,
                })
            _lap("tech/modality breakdown", techs=len(tech_mod_breakdown))

            for tech, gdf in completed[completed['done_by'].notna()].groupby('done_by'):
                all_tats_tech = gdf['tat_min'].dropna().tolist()
                # Separate outliers (> 24h) before computing stats
                normal_tats  = [v for v in all_tats_tech if v <= 1440]
                outlier_tats = [v for v in all_tats_tech if v >  1440]
                ptats = gdf['pacs_tat_min'].dropna()
                avg         = round(sum(normal_tats) / len(normal_tats), 1)        if normal_tats  else None
                median      = round(_stats.median(normal_tats), 1)                  if normal_tats  else None
                avg_pacs    = round(float(ptats.mean()),   1) if len(ptats) else None
                median_pacs = round(float(ptats.median()), 1) if len(ptats) else None
                flags_count = sum(1 for r in tech_data['flagged'] if r['technician'] == str(tech) and r['flags'])
                flag_rate   = round(flags_count / len(gdf) * 100, 1) if len(gdf) else 0.0
                dept_delta  = round(avg - dept_avg, 1) if avg is not None and dept_avg is not None else None
                mods        = sorted(tech_mod_breakdown.get(str(tech), []), key=lambda x: x['count'], reverse=True)
                top_mod     = mods[0]['modality'] if mods else None
                tech_data['by_technician'].append({
                    'name':            str(tech),
                    'count':           len(gdf),
                    'avg_tat':         avg,
                    'median_tat':      median,
                    'avg_pacs_tat':    avg_pacs,
                    'median_pacs_tat': median_pacs,
                    'flags':           flags_count,
                    'flag_rate':       flag_rate,
                    'dept_delta':      dept_delta,
                    'top_modality':    top_mod,
                    'modalities':      mods,
                    'outlier_count':   len(outlier_tats),
                    'insight':         _skew_insight(avg, median),
                })
            tech_data['by_technician'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)
            _lap("by_technician aggregation", technicians=len(tech_data['by_technician']))

            for mod, gdf in completed[completed['modality'].notna()].groupby('modality'):
                tats = gdf['tat_min'].dropna()
                avg    = round(float(tats.mean()),   1) if len(tats) else None
                median = round(float(tats.median()), 1) if len(tats) else None
                tech_data['by_modality'].append({
                    'modality': str(mod), 'count': len(gdf),
                    'avg_tat': avg, 'median_tat': median,
                    'insight': _skew_insight(avg, median),
                })
            tech_data['by_modality'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)
            _lap("by_modality aggregation", modalities=len(tech_data['by_modality']))

            # ── JCI order-to-acquisition compliance, per priority tier ────────
            jci_rows = tdf[tdf['jci_interval_min'].notna() & (tdf['jci_interval_min'] >= 0)]
            by_tier = []
            for code, gdf in jci_rows.groupby('priority_code'):
                intervals = gdf['jci_interval_min']
                threshold = _jci_thresholds.get(code)
                pct_compliant = (
                    round(float((intervals <= threshold).mean() * 100), 1)
                    if threshold is not None else None
                )
                by_tier.append({
                    'code': code,
                    'label': gdf['priority_desc'].iloc[0],
                    'count': int(len(intervals)),
                    'median_min': round(float(intervals.median()), 1),
                    'p90_min': round(float(intervals.quantile(0.9)), 1),
                    'threshold_min': threshold,
                    'pct_compliant': pct_compliant,
                })
            by_tier.sort(key=lambda x: x['count'], reverse=True)
            tech_data['jci_compliance']['by_tier'] = by_tier

            _thresholded = jci_rows[jci_rows['priority_code'].isin(_jci_thresholds.keys())]
            if len(_thresholded):
                _within = _thresholded.apply(
                    lambda r: r['jci_interval_min'] <= _jci_thresholds[r['priority_code']], axis=1
                )
                tech_data['jci_compliance']['overall_pct'] = round(float(_within.mean() * 100), 1)
            if len(jci_rows):
                tech_data['jci_compliance']['overall_median_min'] = round(float(jci_rows['jci_interval_min'].median()), 1)
            _lap("JCI compliance aggregation", tiers=len(by_tier))

    except Exception:
        logger.exception("Failed to build technician monitoring data")
        db.session.rollback()

    # ── Acknowledgements for flagged exams ────────────────────────────────
    try:
        from db import TechFlagAck
        acks = TechFlagAck.query.filter(
            TechFlagAck.flag_date.between(start, end)
        ).all()
        tech_data['ack_map'] = {
            a.accession_number: {
                'by':   a.acknowledged_by_name,
                'at':   a.acknowledged_at.strftime('%Y-%m-%d %H:%M'),
                'note': a.note or '',
            }
            for a in acks
        }
    except Exception:
        db.session.rollback()
        tech_data['ack_map'] = {}
    _lap("acknowledgements query", acks=len(tech_data.get('ack_map', {})))

    # Priority catalog for the JCI threshold admin editor -- one input per active RIS
    # priority tier, so the editor always reflects the real tiers in use rather than a
    # hardcoded guess. Kept separate from tech_data['jci_compliance']['by_tier'] since
    # that list only contains tiers with data in the selected date range; the editor
    # should offer every active tier even if it has zero studies right now.
    jci_priority_catalog = []
    try:
        jci_priority_catalog = [
            {'code': r.code, 'description': r.description}
            for r in db.session.execute(text(
                "SELECT code, description FROM std_procedure_priorities WHERE active ORDER BY code"
            )).fetchall()
            if r.code
        ]
    except Exception:
        logger.exception("Failed to load JCI priority catalog")
        db.session.rollback()

    result = {
        'tech_data': tech_data,
        'jci_priority_catalog': jci_priority_catalog,
        'jci_thresholds': _jci_thresholds,
    }
    cache_put(_REPORT_ID, form_data, result)
    logger.info(f"[report_35][timing] TOTAL: {time.perf_counter() - _t_start:.3f}s "
                f"(start={start} end={end} modalities={params.get('modalities')})")
    return result


@report_35_bp.route("/report/35", methods=["GET", "POST"])
@login_required
def report_35():
    run_report = 'start_date' in request.values

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    filters = {
        "mod_enabled": request.values.get("mod_enabled") == "on",
        "modality":    request.values.getlist("modality"),
    }

    data = None
    if run_report:
        from utils.audit import log_event
        display_start = request.values.get('start_date', display_start)
        display_end   = request.values.get('end_date',   display_end)
        log_event('report_run', category='report', resource_type='report_35',
                  detail={'from': display_start, 'to': display_end})
        data = get_technician_tat_data(request.values)

    return render_template(
        "report_35.html",
        data=data,
        display_start=display_start,
        display_end=display_end,
        run_report=run_report,
        filters=filters,
    )


@report_35_bp.route("/report/35/jci-thresholds", methods=["POST"])
@login_required
def save_jci_thresholds():
    """Admin-editable JCI order-to-acquisition compliance targets, one per priority
    tier. Stored as 'jci_threshold:<CODE>' settings rows -- same convention as
    'pacs_ris_diff_threshold:<MODALITY>' above. A tier with no value submitted (blank
    input) has its threshold removed, reverting that tier to distribution-only display."""
    if current_user.role != 'admin':
        return jsonify({"status": "error", "message": "Admin only"}), 403
    try:
        thresholds = request.get_json(force=True).get("thresholds", {})
        if not isinstance(thresholds, dict):
            return jsonify({"status": "error", "message": "Invalid payload"}), 400

        for code, raw_val in thresholds.items():
            code = str(code).strip()
            if not code:
                continue
            key = f"jci_threshold:{code}"
            if raw_val in (None, ""):
                db.session.execute(text("DELETE FROM settings WHERE key = :k"), {"k": key})
                continue
            val = float(raw_val)
            if val <= 0:
                return jsonify({"status": "error", "message": f"{code}: must be > 0"}), 400
            db.session.execute(text("""
                INSERT INTO settings (key, value) VALUES (:k, :v)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """), {"k": key, "v": str(val)})

        db.session.commit()
        cache_invalidate(_REPORT_ID)  # next load recomputes compliance with the new thresholds
        return jsonify({"status": "success"})
    except (TypeError, ValueError) as e:
        return jsonify({"status": "error", "message": f"Invalid number: {e}"}), 400
    except Exception as e:
        db.session.rollback()
        logger.exception("Failed to save JCI thresholds")
        return jsonify({"status": "error", "message": str(e)}), 500


@report_35_bp.route("/report/35/export", methods=["POST"])
@login_required
def export_report_35():
    """CSV export of the technician monitoring sections. Ported from
    report_25.export_technician_25."""
    from flask import current_app
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403

    bg = get_technician_tat_data(request.values)
    tech = bg.get('tech_data', {})

    sections = []
    if tech.get('summary'):
        sections.append(pd.DataFrame([tech['summary']]).assign(_section='Summary'))
    if tech.get('by_technician'):
        sections.append(pd.DataFrame(tech['by_technician']).assign(_section='By Technician'))
    if tech.get('by_modality'):
        sections.append(pd.DataFrame(tech['by_modality']).assign(_section='By Modality'))
    if tech.get('flagged'):
        df_f = pd.DataFrame(tech['flagged'])
        df_f['flags'] = df_f['flags'].apply(lambda x: ', '.join(x))
        sections.append(df_f.assign(_section='Flagged'))
    if tech.get('never_done'):
        sections.append(pd.DataFrame(tech['never_done']).assign(_section='Never Done'))

    if not sections:
        return "No data", 400

    output = io.BytesIO()
    pd.concat(sections, ignore_index=True).to_csv(output, index=False)
    output.seek(0)
    return send_file(output, mimetype='text/csv', as_attachment=True,
                     download_name=f"RAYD_Technician_TAT_{date.today()}.csv")


# ── Flag acknowledgement API ────────────────────────────────────────────
# Same tech_flag_acknowledgements table as report_25's /api/tech/flag/* routes
# (keyed by accession_number + flag_date, not by report id) — routed under a
# distinct path here since report_25's blueprint stays registered until its
# Technicians tab is retired, and Flask won't allow two blueprints to claim
# the same URL rule.

@report_35_bp.route('/report/35/flag/acknowledge', methods=['POST'])
@login_required
def ack_tech_flag_35():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    from db import TechFlagAck
    data      = request.get_json(silent=True) or {}
    accession = (data.get('accession') or '').strip()
    flag_date = (data.get('flag_date') or '').strip()
    flags     = data.get('flags', [])
    note      = (data.get('note') or '').strip() or None
    if not accession or not flag_date:
        return jsonify({'error': 'Missing fields'}), 400
    existing = TechFlagAck.query.filter_by(
        accession_number=accession, flag_date=flag_date
    ).first()
    if existing:
        existing.note                 = note
        existing.acknowledged_by_id   = current_user.id
        existing.acknowledged_by_name = current_user.username
        existing.acknowledged_at      = datetime.utcnow()
    else:
        db.session.add(TechFlagAck(
            accession_number=accession,
            flag_date=flag_date,
            flags=flags,
            note=note,
            acknowledged_by_id=current_user.id,
            acknowledged_by_name=current_user.username,
            acknowledged_at=datetime.utcnow(),
        ))
    db.session.commit()
    return jsonify({'ok': True})


@report_35_bp.route('/report/35/flag/unacknowledge', methods=['POST'])
@login_required
def unack_tech_flag_35():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    from db import TechFlagAck
    data      = request.get_json(silent=True) or {}
    accession = (data.get('accession') or '').strip()
    flag_date = (data.get('flag_date') or '').strip()
    if not accession or not flag_date:
        return jsonify({'error': 'Missing fields'}), 400
    ack = TechFlagAck.query.filter_by(
        accession_number=accession, flag_date=flag_date
    ).first()
    if ack:
        db.session.delete(ack)
        db.session.commit()
    return jsonify({'ok': True})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(35, report_35_bp, report_35, export_report_35)
