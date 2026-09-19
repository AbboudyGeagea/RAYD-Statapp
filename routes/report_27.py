import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date
from utils.site_resolver import default_site

report_27_bp = Blueprint("report_27", __name__)

# A study is not "unreported" just because it is recent -- it has to be given time
# to be read. Months whose end is inside this window are still maturing: they render
# greyed and are excluded from the headline and from any agreement judgement, so a
# part-finished month never looks like an incident. Measured against the real approval
# TAT, p95 sits comfortably under two weeks at LAUMC.
_MATURATION_DAYS = 14

# RIS status_key for "Approved" (worklist_status_map / RIS STATUS table, key 160).
_RIS_APPROVED_KEY = 160

def calculate_age(birth_date):
    if birth_date is None or pd.isna(birth_date):
        return np.nan
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))


# ── Order Status Mix ──────────────────────────────────────────────────────────
# etl_orders.order_status is a lossy translation of the RIS status: 42 codes
# collapse into 'CM' / 'CA' plus four stage names (ETL_JOBS/etl_orders.py
# _translate_order_status). Those codes are meaningless to a reader, and the
# card used to promise "current RIS status (e.g. Scheduled, Completed...)" while
# the axis said 'CM'.
#
# The bar stays aggregated rather than becoming one bar per RIS status: at LAUMC
# 'CM' is 99.5% "Approved" (82,013 of 82,413), so a flat per-status chart is two
# readable bars and five invisible slivers. The real status names ride along in
# the breakdown, which the tooltip renders -- that is the part the customer can
# tick off against their own worklist.
_STATUS_BAR_LABELS = {
    'requested':   'Requested',
    'scheduled':   'Scheduled',
    'arrived':     'Arrived',
    'in_progress': 'In Progress',
    'CM':          'Completed / Reported',
    'CA':          'Cancelled',
}

# Lifecycle order, not count order — a status chart that reorders itself run to
# run is hard to compare against last month's export. 'Unknown' is last and is
# only ever non-empty when something upstream needs fixing.
_STATUS_BAR_ORDER = ['Requested', 'Scheduled', 'Arrived', 'In Progress',
                     'Completed / Reported', 'Cancelled', 'Unknown']


def _build_status_mix(df):
    """[{label, total, breakdown: [{name, count}]}] in lifecycle order.

    'Unknown' collects two different failures, deliberately in one visible bucket
    rather than dropped: an order_status that is NULL (a RIS status_key missing
    from worklist_status_map — migration 0047 requires these be surfaced, but
    value_counts() used to drop them silently), and a status_key that is itself
    NULL because migration 0112's backfill has not run yet.
    """
    work = df[['order_status', 'ris_status_name']].copy()
    work['bar'] = work['order_status'].map(_STATUS_BAR_LABELS).fillna('Unknown')
    work['ris_status_name'] = work['ris_status_name'].fillna('Unknown')

    mix = []
    for label in _STATUS_BAR_ORDER:
        grp = work[work['bar'] == label]
        if grp.empty:
            continue
        counts = grp['ris_status_name'].value_counts()
        mix.append({
            'label':     label,
            'total':     int(len(grp)),
            'breakdown': [{'name': str(n), 'count': int(c)} for n, c in counts.items()],
        })
    return mix

def _has_status_key():
    """True when migration 0112's etl_orders.status_key column exists AND carries data.

    Without it the only RIS status available is etl_orders.order_status, which is the
    lossy translation ('CM' covers Approved, Signed 1/2/3, Exam Done, Dictated, Pending
    and Reviewed alike -- ~0.7% wider than Approved on its own). Worth using as a
    fallback, worth labelling on screen so nobody quotes it as exact.
    """
    try:
        exists = db.session.execute(text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'etl_orders' AND column_name = 'status_key'
        """)).scalar()
        if not exists:
            return False
        return bool(db.session.execute(
            text("SELECT EXISTS (SELECT 1 FROM etl_orders WHERE status_key IS NOT NULL)")
        ).scalar())
    except Exception:
        db.session.rollback()
        return False


def _build_system_agreement(start, end):
    """Monthly RIS-vs-PACS reported-volume agreement.

    THE UNIT PROBLEM THIS SOLVES. The RIS counts scheduled procedure STEPS; PACS counts
    STUDIES. A CT abdomen+pelvis is two steps and one study, so the raw RIS number runs
    ~17.5% above PACS and looks like PACS is losing reports. Measured on LAUMC RH,
    Jan-Aug 2026: 43,566 RIS steps against 37,361 PACS approved studies -- a 14% "gap"
    that is entirely an artefact of counting different things.

    etl_orders.linked_id is the RIS's own grouping key for multi-part protocols, so
    counting DISTINCT linked_id converts steps to studies. Same period, regrouped:
    37,075 RIS vs 37,361 PACS -- 0.8% apart, and that residual is real (walk-ins and
    imported outside studies that never had a RIS order). Rows with a NULL linked_id
    are single-step exams and are their own group, hence the COALESCE onto order_dbid.

    Returns rows oldest-first; each carries `maturing` so the template can grey out
    months that have not had time to be read yet.
    """
    site_id = default_site()
    params = {"start": start, "end": end}

    ris_site  = ""
    pacs_site = ""
    if site_id is not None:
        params["site_id"] = site_id
        ris_site  = "AND o.site_id = :site_id"
        # etl_didb_studies carries the RAW PACS marker, not the resolved site id --
        # resolve it through `sites` so the ~2027 PACS upgrade only touches that table.
        pacs_site = ("AND s.pacs_site_id_raw = "
                     "(SELECT pacs_site_id FROM sites WHERE id = :site_id)")

    exact = _has_status_key()
    ris_approved = (f"o.status_key = {_RIS_APPROVED_KEY}" if exact
                    else "UPPER(TRIM(COALESCE(o.order_status, ''))) = 'CM'")

    rows = db.session.execute(text(f"""
        WITH ris AS (
            SELECT DATE_TRUNC('month', o.scheduled_datetime)::date AS ym,
                   COUNT(*) AS ris_steps,
                   COUNT(DISTINCT COALESCE(o.linked_id::text, 'S' || o.order_dbid::text))
                       AS ris_studies
            FROM etl_orders o
            WHERE o.scheduled_datetime >= :start
              AND o.scheduled_datetime <  (CAST(:end AS DATE) + 1)
              AND {ris_approved}
              {ris_site}
            GROUP BY 1
        ),
        pacs AS (
            SELECT DATE_TRUNC('month', s.study_date)::date AS ym,
                   COUNT(*) FILTER (WHERE UPPER(TRIM(s.study_status)) = 'APPROVED')
                       AS pacs_approved,
                   COUNT(*) AS pacs_total
            FROM etl_didb_studies s
            -- LATERAL ... LIMIT 1, not a plain join: aetitle_modality_map's UNIQUE is
            -- on the raw aetitle while this matches on UPPER(TRIM(...)), so two
            -- case-variant rows would both match and double every count here (the
            -- fan-out migration 0116 had to repair). LIMIT 1 makes that impossible.
            LEFT JOIN LATERAL (
                SELECT m.modality FROM aetitle_modality_map m
                WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
            ) m ON TRUE
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT', 'BMD')
              {pacs_site}
            GROUP BY 1
        )
        SELECT COALESCE(r.ym, p.ym)            AS ym,
               COALESCE(r.ris_steps, 0)        AS ris_steps,
               COALESCE(r.ris_studies, 0)      AS ris_studies,
               COALESCE(p.pacs_approved, 0)    AS pacs_approved,
               COALESCE(p.pacs_total, 0)       AS pacs_total
        FROM ris r
        FULL OUTER JOIN pacs p ON p.ym = r.ym
        ORDER BY 1
    """), params).mappings().all()

    cutoff = date.today() - timedelta(days=_MATURATION_DAYS)
    out, matured = [], []
    for r in rows:
        ym = r["ym"]
        # Last calendar day of this month -- if it falls after the cutoff the month
        # has studies too recent to have been read, so it is not yet comparable.
        nxt = (ym.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = nxt - timedelta(days=1)

        ris, pacs = int(r["ris_studies"]), int(r["pacs_approved"])
        row = {
            "month":         ym.strftime("%Y-%m"),
            "label":         ym.strftime("%b %Y"),
            "ris_steps":     int(r["ris_steps"]),
            "ris_studies":   ris,
            "pacs_approved": pacs,
            "pacs_total":    int(r["pacs_total"]),
            "diff":          pacs - ris,
            "diff_pct":      round((pacs - ris) / ris * 100, 1) if ris else None,
            "maturing":      month_end > cutoff,
        }
        out.append(row)
        if not row["maturing"] and ris:
            matured.append(row)

    ris_tot  = sum(r["ris_studies"]   for r in matured)
    pacs_tot = sum(r["pacs_approved"] for r in matured)
    worst    = max((abs(r["diff_pct"]) for r in matured if r["diff_pct"] is not None),
                   default=0.0)

    return {
        "months":        out,
        "exact_status":  exact,
        "has_matured":   bool(matured),
        "ris_total":     ris_tot,
        "pacs_total":    pacs_tot,
        "diff_total":    pacs_tot - ris_tot,
        "diff_pct":      round((pacs_tot - ris_tot) / ris_tot * 100, 1) if ris_tot else None,
        "worst_pct":     worst,
        # Plain-language verdict. Thresholds are set off the measured baseline: eight
        # matured months at LAUMC RH never exceeded 1.4%, so 3% is already abnormal.
        "verdict":       ("agree"   if worst <  3 else
                          "drifting" if worst < 10 else "broken"),
        "steps_per_study": (round(sum(r["ris_steps"] for r in matured) / ris_tot, 2)
                            if ris_tot else None),
    }


def get_report_data(start, end):
    # The duration-map join is LATERAL / LIMIT 1 rather than the obvious
    # "ON m.procedure_code = s.procedure_code OR m.procedure_code = o.proc_id".
    # procedure_code is UNIQUE, so each side of that OR matched at most one row --
    # but BOTH sides matched whenever the PACS procedure code differed from the
    # RIS proc code, returning the order twice. Everything downstream counts rows
    # (value_counts / len / groupby.size), so those orders were counted twice.
    #
    # The skew was not uniform: fan-out needs s.procedure_code to be non-NULL,
    # which only happens once an order resolves to a PACS study -- i.e. exam-done
    # or later -- so it inflated the 'CM' bar and almost nothing else. Measured on
    # LAUMC 2026-09-18: 'CM' read 90,108 against a true 82,071 (+9.8%), while
    # in_progress was exact and scheduled/CA/arrived were off by 6-13 rows.
    #
    # LIMIT 1 restores one-row-per-order. The ORDER BY keeps the old preference
    # (PACS procedure code first, RIS proc_id as fallback) so a code present in
    # only one of the two still resolves a duration -- COALESCE would have
    # silently dropped those.
    sql = text("""
        SELECT
            o.order_dbid,
            o.order_status,
            o.status_key,
            COALESCE(wsm.status_name, 'Unknown') AS ris_status_name,
            o.proc_id,
            o.proc_text,
            o.scheduled_datetime,
            o.has_study,
            s.study_date,
            s.storing_ae,
            s.procedure_code,
            p.birth_date,
            p.gender_code AS sex,
            m.duration_minutes,
            -- Report 27 is ABOUT the codes (RIS proc_id vs PACS procedure_code match
            -- rate), so both raw codes stay. This adds the human-readable procedure
            -- DESCRIPTION alongside them so the CSV export is legible: catalog name
            -- first, then the RIS order text, then the code as a last resort.
            COALESCE(NULLIF(TRIM(m.procedure_name), ''), NULLIF(TRIM(o.proc_text), ''),
                     s.procedure_code, o.proc_id) AS procedure_description
        FROM etl_orders o
        LEFT JOIN worklist_status_map wsm
            ON wsm.status_key = o.status_key
        LEFT JOIN etl_didb_studies s
            ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
        LEFT JOIN std_patients_ris p
            ON p.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$'
                                            THEN o.patient_dbid::BIGINT END
        LEFT JOIN LATERAL (
            SELECT dm.duration_minutes, dm.procedure_name
            FROM procedure_duration_map dm
            WHERE dm.procedure_code::TEXT IN (s.procedure_code::TEXT, o.proc_id::TEXT)
            ORDER BY (dm.procedure_code::TEXT IS NOT DISTINCT FROM s.procedure_code::TEXT) DESC
            LIMIT 1
        ) m ON TRUE
        WHERE o.scheduled_datetime >= :start
          AND o.scheduled_datetime <  (CAST(:end AS DATE) + INTERVAL '1 day')
    """)
    res = db.session.execute(sql, {"start": start, "end": end}).fetchall()
    df = pd.DataFrame(res)
    
    if not df.empty:
        # 1. Standardize types and clean data
        df['scheduled_datetime'] = pd.to_datetime(df['scheduled_datetime'])
        df['study_date'] = pd.to_datetime(df['study_date'])
        df['match'] = df['proc_id'].astype(str).str.strip() == df['procedure_code'].astype(str).str.strip()
        
        # 2. Handle Age Grouping Safely
        df['age'] = df['birth_date'].apply(calculate_age)
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        
        bins = [-0.001, 8.999, 18.999, 64.999, 150]
        labels = ['0-8', '9-18', '19-64', '65+']
        
        # Use observed=False in groupby later, and fillna for categories
        df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels)
        df['age_group'] = df['age_group'].cat.add_categories('Unknown').fillna('Unknown')

        # 3. Normalize sex/gender so unresolved patients (no RIS match, unmapped
        # gender code) still land in the demo breakdown instead of being silently
        # dropped — pandas groupby excludes NaN keys entirely by default.
        df['sex'] = df['sex'].fillna('Unknown')

    return df

@report_27_bp.route("/report/27", methods=["GET", "POST"])
@login_required
def report_27():
    today = date.today()
    go_live = get_go_live_date() or date(2025, 1, 1)

    start_a = request.values.get("start_date", go_live.strftime('%Y-%m-%d'))
    end_a   = request.values.get("end_date",   today.strftime('%Y-%m-%d'))

    run_report = 'start_date' in request.values
    data = {}

    if run_report:
        df_a = get_report_data(start_a, end_a)

        mo_sql = text("""
            WITH mo_groups AS (
                SELECT
                    patient_dbid,
                    proc_id,
                    DATE(scheduled_datetime) AS sched_date,
                    COUNT(*) AS cnt
                FROM etl_orders
                WHERE scheduled_datetime >= :start
                  AND scheduled_datetime <  (CAST(:end AS DATE) + INTERVAL '1 day')
                GROUP BY patient_dbid, proc_id, DATE(scheduled_datetime)
                HAVING COUNT(*) > 1
            )
            SELECT
                COALESCE(SUM(cnt), 0)         AS total_flagged_orders,
                COUNT(*)                       AS flagged_groups,
                COUNT(DISTINCT patient_dbid)   AS flagged_patients
            FROM mo_groups
        """)
        try:
            data['agreement'] = _build_system_agreement(start_a, end_a)
        except Exception as e:
            db.session.rollback()
            logger_msg = f"Report 27 system-agreement section failed: {e}"
            print(logger_msg)
            data['agreement'] = None

        mo_row = db.session.execute(mo_sql, {"start": start_a, "end": end_a}).fetchone()
        data['multi_order'] = {
            "orders":   int(mo_row.total_flagged_orders or 0),
            "groups":   int(mo_row.flagged_groups       or 0),
            "patients": int(mo_row.flagged_patients     or 0),
        }

        if not df_a.empty:
            df_a['duration_minutes'] = pd.to_numeric(df_a['duration_minutes'], errors='coerce')
            dur_raw = df_a[df_a['duration_minutes'] > 0]['duration_minutes']
            if len(dur_raw) > 0:
                q1, q3 = dur_raw.quantile(0.25), dur_raw.quantile(0.75)
                iqr = q3 - q1
                dur_clean = dur_raw[dur_raw <= q3 + 1.5 * iqr]
                avg_duration = round(dur_clean.mean(), 1)
                duration_outliers_removed = int(len(dur_raw) - len(dur_clean))
            else:
                avg_duration = 0.0
                duration_outliers_removed = 0

            data['audit'] = {
                "total":                     len(df_a),
                "orphans":                   int(len(df_a[df_a['has_study'] == False])),
                "matches":                   int(df_a['match'].sum()),
                "mismatches":                int(len(df_a) - df_a['match'].sum()),
                "avg_duration":              avg_duration,
                "duration_outliers_removed": duration_outliers_removed,
                "hourly":                    df_a['scheduled_datetime'].dt.hour.value_counts().sort_index().to_dict(),
                "status_mix":                _build_status_mix(df_a),
                "ae_mix":                    df_a['storing_ae'].fillna('Unknown').value_counts().to_dict(),
                "demo":                      df_a.groupby(['age_group', 'sex'], observed=False).size().unstack(fill_value=0).to_dict('index'),
            }

            # Busiest days (operator instruction 2026-07-31): "Hourly Load" above is
            # already an aggregate-by-hour profile across the whole period -- this
            # extends the same idea across two more dimensions. weekday x time-block
            # matrix for "busiest day of week (and time)", separate day-of-month bar
            # for "busiest day of month" (day-of-month has no natural second axis to
            # pair with, unlike weekday x hour, so it doesn't fit the matrix).
            sched = df_a[['scheduled_datetime']].dropna()
            if not sched.empty:
                sched = sched.copy()
                sched['weekday']  = sched['scheduled_datetime'].dt.dayofweek   # 0=Mon..6=Sun
                sched['hour']     = sched['scheduled_datetime'].dt.hour
                sched['cal_date'] = sched['scheduled_datetime'].dt.date

                # 2-hour blocks spanning the REAL observed hour range in this data
                # (not a hardcoded clinic-hours assumption -- this includes ER, which
                # can run round the clock).
                min_hour = int(sched['hour'].min())
                max_hour = int(sched['hour'].max())
                block_floor = (min_hour // 2) * 2
                block_ceil  = (max_hour // 2) * 2 + 2
                block_starts = list(range(block_floor, block_ceil, 2))
                sched['block_idx'] = (sched['hour'] - block_floor) // 2

                daily = sched.groupby(['weekday', 'block_idx', 'cal_date']).size().reset_index(name='n')
                grouped = {key: grp.sort_values('cal_date') for key, grp in daily.groupby(['weekday', 'block_idx'])}

                matrix_cells = []
                for wd in range(7):
                    for bi in range(len(block_starts)):
                        grp = grouped.get((wd, bi))
                        if grp is not None:
                            series = [[d.strftime('%Y-%m-%d'), int(n)] for d, n in zip(grp['cal_date'], grp['n'])]
                            total = int(grp['n'].sum())
                        else:
                            series, total = [], 0
                        matrix_cells.append({
                            "weekday_idx": wd,
                            "block_idx":   bi,
                            "total":       total,
                            "series":      series,   # [[date_str, count], ...] real calendar dates in range
                        })

                data['busiest'] = {
                    "weekday_labels":     ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                    "hour_block_starts":  block_starts,
                    "cells":              matrix_cells,
                    "by_day_of_month":    sched['scheduled_datetime'].dt.day.value_counts().sort_index().to_dict(),
                }

    return render_template("report_27.html", data=data, run_report=run_report,
                           display_start=start_a, display_end=end_a)

# ── Cancelled exam list (RIS) ─────────────────────────────────────────────────
# Operator request: exportable list of cancelled exams, from RIS data.
#
# SOURCE. etl_orders is RIS-sourced on this install (the PACS-side MDB_ORDERS was
# swapped out -- see ETL_JOBS/etl_orders.py's module docstring), so "cancelled
# exams, RIS data" is etl_orders at SITE_WORKLIST grain: one row per scheduled
# procedure step, which is what a cancellation actually happens to. A multi-part
# protocol cancelled as a unit therefore produces several rows sharing one
# linked_id -- the Linked Group ID column is there so that is visible rather than
# looking like duplicates.
#
# WHAT COUNTS AS CANCELLED. Deliberately the same set the report's "Cancelled"
# bar draws, so the CSV reconciles against the chart it sits next to. That bar is
# order_status = 'CA', which ETL_JOBS/etl_orders.py _translate_order_status emits
# for is_cancel OR stage = 'discontinued' -- i.e. it includes Discontinued (RIS
# status 90), which is NOT strictly a cancellation. Rather than pick one
# definition and disagree with the chart, the export carries both and adds a
# Cancellation Type column so Discontinued can be filtered out in Excel.
#
# Row selection prefers status_key (the raw RIS code, migration 0112) over the
# lossy order_status, falling back per-row where the backfill has not reached.
# The two agree by construction today; status_key additionally picks up any
# status newly classified as a cancellation in worklist_status_map AFTER those
# rows were written, which the frozen order_status string cannot.
#
# NO PATIENT NAMES. Explicit operator instruction (ETL_JOBS/etl_ris_patients.py:7)
# -- RIS name columns are never extracted, so they do not exist to export. The
# patient is identified by MRN from std_patient_ids.
#
# WHO CANCELLED IT / WHY is NOT available. The RIS ORDERS table carries
# cancelled_by_person_key and status_reason_key (see the std_orders_ris block in
# ETL_JOBS/system_type_registry.py), but that table is registered, not ETL'd --
# there is no std_orders_ris in the database. The RIS status name is therefore
# the closest thing to a reason ("Cancelled by Patient" vs "Cancelled by OP" vs
# "Cancelled Duplicate"), which is genuinely useful but is not a free-text
# reason. Wiring std_orders_ris in would make a real reason column possible.
#
# SITE SCOPE. Intentionally unfiltered, matching get_report_data() above (which
# the Cancelled bar is built from) rather than the RH-only rule applied in
# _build_system_agreement. Filtering here and not there would make the CSV
# disagree with the chart. Both sites are labelled in the Site column instead.

# Row selection, when etl_orders.status_key exists. Per row: use the RIS code when
# it has been backfilled, fall back to the lossy order_status when it has not, so a
# partial backfill never drops cancelled rows from the list.
_CANCELLED_PREDICATE_EXACT = """
        CASE WHEN o.status_key IS NOT NULL
             THEN (wsm.is_cancel = TRUE OR wsm.stage = 'discontinued')
             ELSE UPPER(TRIM(COALESCE(o.order_status, ''))) = 'CA'
        END
"""
# Row selection when the column does not exist at all (migration 0112 not applied
# on this install). order_status is then the ONLY signal available, and 'CA' is
# exactly what the chart's Cancelled bar draws, so the list still reconciles --
# it just cannot name which flavour of cancellation each row was.
_CANCELLED_PREDICATE_FALLBACK = "UPPER(TRIM(COALESCE(o.order_status, ''))) = 'CA'"

# Column order is the order they are read in: when, who, what, why, then the
# traceability keys that only matter once someone is chasing a specific row.
_CANCELLED_EXPORT_COLUMNS = [
    ("scheduled_date",         "Scheduled Date"),
    ("scheduled_time",         "Scheduled Time"),
    ("site_code",              "Site"),
    ("patient_id",             "Patient ID (MRN)"),
    ("sex",                    "Sex"),
    ("age_at_scheduled",       "Age at Scheduled Date"),
    ("modality",               "Modality"),
    ("proc_id",                "Procedure Code (RIS)"),
    ("procedure_description",  "Procedure"),
    ("ris_status_name",        "RIS Status"),
    ("cancellation_type",      "Cancellation Type"),
    ("cancelled_recorded_at",  "Cancellation Recorded (approx)"),
    ("days_notice",            "Days Notice (approx)"),
    ("study_in_pacs",          "Study in PACS"),
    ("storing_ae",             "AE Title"),
    ("accession_number",       "Accession Number"),
    ("order_dbid",             "RIS Order ID"),
    ("linked_id",              "Linked Group ID"),
]


def _status_key_column_exists():
    """True when migration 0112's etl_orders.status_key COLUMN is present.

    Deliberately distinct from _has_status_key() above, which also requires the
    backfill to have run. Whether the column EXISTS decides whether SQL may name
    it at all — referencing it when absent is an outright query failure, not a
    degraded answer. Whether it holds DATA only decides how precise the answer is,
    and the per-row CASE in _CANCELLED_PREDICATE_EXACT handles that.

    Not hypothetical: the HL7-branch installs run a separate migration lineage
    that never received 0112, so their etl_orders has no such column.
    """
    try:
        return bool(db.session.execute(text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'etl_orders' AND column_name = 'status_key'
        """)).scalar())
    except Exception:
        db.session.rollback()
        return False


def get_cancelled_exams(start, end):
    """Cancelled / discontinued RIS orders in [start, end], one row per exam step.

    Returns a DataFrame with _CANCELLED_EXPORT_COLUMNS' keys. Date-filtered on
    scheduled_datetime — the same anchor the rest of Report 27 uses, so this
    answers "exams that were due in this window and did not happen", not "exams
    cancelled during this window". Those are different questions; the
    Cancellation Recorded column carries the other one per row.
    """
    # See _status_key_column_exists(). Everything that names status_key — both
    # joins, the status label and the row filter — is switched together, so the
    # query is always internally consistent.
    exact = _status_key_column_exists()
    if exact:
        status_joins = ("LEFT JOIN worklist_status_map wsm ON wsm.status_key = o.status_key\n"
                        "        LEFT JOIN std_status_ris      sr  ON sr.status_key  = o.status_key")
        # worklist_status_map is the curated map (migration 0047); std_status_ris is
        # the raw RIS lookup and covers codes the map has not been extended to yet,
        # so an unmapped cancellation still exports with its real RIS label.
        status_name_sql = "COALESCE(wsm.status_name, sr.name, 'Unknown')"
        stage_sql = "wsm.stage"
        predicate = _CANCELLED_PREDICATE_EXACT
    else:
        status_joins = ""
        status_name_sql = "'Unknown (RIS status code not ingested on this install)'"
        stage_sql = "NULL::TEXT"
        predicate = _CANCELLED_PREDICATE_FALLBACK

    sql = text(f"""
        SELECT
            o.scheduled_datetime,
            o.accession_number,
            pid.patient_id,
            p.gender_code                                   AS sex,
            -- Age AS AT the scheduled date, not today. calculate_age() above is
            -- age-now, which is right for a live demographic mix and wrong for a
            -- historical list that may span years.
            CASE WHEN p.birth_date IS NOT NULL AND o.scheduled_datetime IS NOT NULL
                 THEN EXTRACT(YEAR FROM AGE(o.scheduled_datetime, p.birth_date))::INT
            END                                             AS age_at_scheduled,
            o.modality,
            o.proc_id,
            COALESCE(NULLIF(TRIM(dm.procedure_name), ''), NULLIF(TRIM(o.proc_text), ''),
                     o.proc_id)                             AS procedure_description,
            {status_name_sql}                               AS ris_status_name,
            {stage_sql}                                     AS stage,
            o.has_study,
            s.storing_ae,
            o.last_update                                   AS cancelled_recorded_at,
            site.code                                       AS site_code,
            o.order_dbid,
            o.linked_id
        FROM etl_orders o
        {status_joins}
        LEFT JOIN etl_didb_studies    s   ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
        LEFT JOIN std_patients_ris    p
            ON p.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$'
                                           THEN o.patient_dbid::BIGINT END
        -- One MRN per patient, chosen deterministically. std_patient_ids.is_primary is
        -- TEXT whose real value vocabulary is still unconfirmed (migration 0060 says so
        -- outright), so a flag that LOOKS primary only wins the sort -- it is never
        -- required. Without that, an install whose flag reads 'P' would export blanks.
        LEFT JOIN LATERAL (
            SELECT pi.patient_id
            FROM std_patient_ids pi
            WHERE pi.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$'
                                               THEN o.patient_dbid::BIGINT END
              AND NULLIF(TRIM(pi.patient_id), '') IS NOT NULL
            ORDER BY (UPPER(TRIM(COALESCE(pi.is_primary, ''))) IN ('Y','1','T','TRUE')) DESC,
                     pi.display_sort_order NULLS LAST,
                     pi.sequence_id NULLS LAST,
                     pi.patient_id_list_key
            LIMIT 1
        ) pid ON TRUE
        -- Same LATERAL/LIMIT 1 shape as get_report_data(), for the same reason: a plain
        -- OR-join on (PACS code, RIS code) duplicates the row whenever the two differ.
        LEFT JOIN LATERAL (
            SELECT dm.procedure_name
            FROM procedure_duration_map dm
            WHERE dm.procedure_code::TEXT IN (s.procedure_code::TEXT, o.proc_id::TEXT)
            ORDER BY (dm.procedure_code::TEXT IS NOT DISTINCT FROM s.procedure_code::TEXT) DESC
            LIMIT 1
        ) dm ON TRUE
        LEFT JOIN sites site ON site.id = o.site_id
        WHERE o.scheduled_datetime >= :start
          AND o.scheduled_datetime <  (CAST(:end AS DATE) + INTERVAL '1 day')
          AND {predicate}
        ORDER BY o.scheduled_datetime, o.accession_number
    """)
    df = pd.DataFrame(db.session.execute(sql, {"start": start, "end": end}).mappings().all())
    if df.empty:
        return pd.DataFrame(columns=[k for k, _ in _CANCELLED_EXPORT_COLUMNS])

    sched = pd.to_datetime(df['scheduled_datetime'], errors='coerce')
    recorded = pd.to_datetime(df['cancelled_recorded_at'], errors='coerce')
    df['scheduled_date'] = sched.dt.strftime('%Y-%m-%d')
    df['scheduled_time'] = sched.dt.strftime('%H:%M')
    df['cancelled_recorded_at'] = recorded.dt.strftime('%Y-%m-%d %H:%M')

    # Positive = cancelled ahead of the slot; 0 or negative = cancelled on the day
    # or after it, i.e. a slot that was almost certainly lost. Approximate because
    # last_update is the RIS row's last change, which for a terminal status is the
    # cancellation itself unless the order was edited again afterwards.
    df['days_notice'] = (sched.dt.normalize() - recorded.dt.normalize()).dt.days

    # 'Discontinued' rides along in the same export as the chart's Cancelled bar
    # but is not a cancellation — named, so it can be filtered out rather than
    # silently inflating a cancellation count. Without status_key there is nothing
    # to tell the two apart, and the column says that outright instead of
    # labelling every row "Cancelled" and quietly overstating the total.
    df['cancellation_type'] = df['stage'].map(
        {'cancelled': 'Cancelled', 'discontinued': 'Discontinued'}
    ).fillna('Cancelled or Discontinued (not distinguishable)' if not exact
             else 'Cancelled (status not in map)')

    # A cancelled order that still has a PACS study is an operational anomaly worth
    # seeing, not a data error to hide — the exam may have gone ahead after all.
    df['study_in_pacs'] = df['has_study'].map({True: 'Yes', False: 'No'}).fillna('No')

    df['sex'] = df['sex'].fillna('Unknown')

    # Nullable Int64, not the float64 pandas upcasts to the moment one row is NULL
    # — otherwise a RIS order id exports as "8821.0" and an age as "45.0", which
    # looks like a rounding artefact and breaks a lookup pasted back into the RIS.
    for col in ('age_at_scheduled', 'days_notice', 'order_dbid', 'linked_id'):
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df[[k for k, _ in _CANCELLED_EXPORT_COLUMNS]]


@report_27_bp.route("/report/27/export-cancelled", methods=["POST"])
@login_required
def export_cancelled_exams():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403

    start = request.form.get("start_date")
    end = request.form.get("end_date")

    from utils.audit import log_event
    log_event('report_export', category='report', resource_type='report_27_cancelled',
              detail={'from': start, 'to': end})

    df = get_cancelled_exams(start, end)
    df.columns = [label for _, label in _CANCELLED_EXPORT_COLUMNS]

    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-disposition":
                 f"attachment; filename=Cancelled_Exams_{start}_to_{end}.csv"},
    )


@report_27_bp.route("/report/27/export", methods=["POST"])
@login_required
def export_report_27():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    start = request.form.get("start_date")
    end = request.form.get("end_date")
    df = get_report_data(start, end)
    return Response(df.to_csv(index=False), mimetype="text/csv",
                    headers={"Content-disposition": f"attachment; filename=Audit_Export_{start}.csv"})

# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(27, report_27_bp, report_27, export_report_27)
