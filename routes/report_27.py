import pandas as pd
import numpy as np
from datetime import date, datetime
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_27_bp = Blueprint("report_27", __name__)

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
            m.duration_minutes
        FROM etl_orders o
        LEFT JOIN worklist_status_map wsm
            ON wsm.status_key = o.status_key
        LEFT JOIN etl_didb_studies s
            ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
        LEFT JOIN std_patients_ris p
            ON p.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$'
                                            THEN o.patient_dbid::BIGINT END
        LEFT JOIN LATERAL (
            SELECT dm.duration_minutes
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
