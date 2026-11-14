"""
routes/report_38.py — Report 38, the ER dashboard: turnaround, target compliance, unread
studies and workload for emergency patients.

Until 2026-11-14 this was the standalone page /er (routes/er_dashboard.py) behind the licence
feature 'er_dashboard'. Like every report now (CLAUDE.md rule 8, to-do item 25): its
report_template row, licence entry and access come from migration 0150, it registers itself
below, it opens through /viewer/38, access is per-user report access, and its filters are the
shared left panel (templates/_report_filter_panel.html). It keeps its own default period, the
last 30 days.

    /report/38        the page (/viewer/38 opens it; /er redirects)
    /report/38/data   the figures, for the filters in the page address:
                      start_date, end_date, sla_hours (the target, in hours) and the
                      modality dropdown (modality + mod_enabled)
"""
from flask import Blueprint, render_template, request, jsonify, abort, redirect
from flask_login import login_required, current_user
from sqlalchemy import text
from db import ReportAccessControl, db
from datetime import date, timedelta
from utils.radiologist_resolve import rad_alias_join_sql, rad_display_sql
from utils.report_filters import panel_filters, picked, picked_sql

REPORT_ID = 38
report_38_bp = Blueprint('report_38', __name__)
DEFAULT_SLA_HOURS = 1

_ER_WHERE = """(
    UPPER(COALESCE(s.patient_location, '')) = 'ER'
    OR s.patient_class ILIKE '%ER%'
    OR s.patient_class ILIKE '%Emergency%'
)"""

# s.study_time is a column on etl_didb_studies that the ETL job never actually writes
# to (confirmed: ETL_JOBS/etl_didb_studies.py has no study_time in its column list) --
# it is always NULL, so the old regex-based reconstruction below always fell through to
# its ELSE '0'::interval branch, silently anchoring every study to midnight. That both
# flattened "volume by hour" onto 00:00 and inflated every TAT figure on this page
# (TAT was being measured from midnight of study_date, not from when the study actually
# happened). s.insert_time (PACS ingestion) is the real timestamp with actual
# hour-of-day, and is already the standard TAT-start anchor used elsewhere in this app
# (report_25.py, referring_intel.py) for the same reason.
_STUDY_DT = "s.insert_time"


def _cluster_tat_histogram(vals, k=10):
    """K-means clustering of ER TAT values into a fixed 10 groups, replacing
    fixed-width (0-30/30-60/...) bins with data-driven ones. Fixed k=10 -- the
    earlier inertia-elbow auto-selection (2-6 clusters) produced too few/uneven
    buckets in practice. 1-D K-means naturally produces contiguous, non-overlapping
    ranges when sorted by centroid, so each cluster becomes one clean "low-high h"
    bucket. k is clamped down to len(vals) when there isn't enough data for 10
    distinct clusters (K-means requires n_samples >= n_clusters).

    Returns [] for no data, or a list of {bucket, cnt, avg} sorted by avg ascending,
    plus a 'k' key on each row so the frontend can show how many clusters were found.
    'bucket' is formatted in hours (display unit for this page); 'avg' stays in
    minutes since it's also used server-side-adjacent for SLA (minutes) comparisons
    on the frontend.
    """
    if not vals:
        return []

    try:
        from sklearn.cluster import KMeans
    except ImportError:
        return [{'bucket': f'{round(min(vals)/60,1)}-{round(max(vals)/60,1)}h',
                  'cnt': len(vals), 'avg': round(sum(vals) / len(vals), 1), 'k': 1}]

    n = min(k, len(vals))
    X = [[v] for v in vals]
    km = KMeans(n_clusters=n, n_init=10, max_iter=300, random_state=42)
    labels = km.fit_predict(X)

    clusters = []
    for c in range(n):
        cvals = [v for v, l in zip(vals, labels) if l == c]
        if not cvals:
            continue
        clusters.append({
            'bucket': f'{round(min(cvals)/60,1)}-{round(max(cvals)/60,1)}h',
            'cnt': len(cvals),
            'avg': round(sum(cvals) / len(cvals), 1),
        })
    clusters.sort(key=lambda row: row['avg'])
    for row in clusters:
        row['k'] = len(clusters)
    return clusters


def _require_access():
    """As /viewer/<id> checks it: an admin, or a user whose access to report 38 is on."""
    if current_user.role == 'admin':
        return
    allowed = ReportAccessControl.query.filter_by(
        user_id=current_user.id, report_template_id=REPORT_ID, is_enabled=True).first()
    if not allowed:
        abort(403)


def _period():
    """The dates in the page address, else the last 30 days (this report's own default)."""
    today = date.today()
    try:
        start = date.fromisoformat(request.args.get('start_date') or (today - timedelta(days=30)).isoformat())
        end = date.fromisoformat(request.args.get('end_date') or today.isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    return start.isoformat(), end.isoformat()


def _sla_hours():
    """The reporting target in hours, as typed in the panel; 1 h when blank or not positive."""
    try:
        hours = float(request.args.get('sla_hours') or DEFAULT_SLA_HOURS)
    except ValueError:
        hours = DEFAULT_SLA_HOURS
    return hours if hours > 0 else DEFAULT_SLA_HOURS


@report_38_bp.route('/er')
@login_required
def old_address():
    """The page's address before it became Report 38 (bookmarks, the old dashboard card)."""
    return redirect(f'/viewer/{REPORT_ID}')


@report_38_bp.route('/report/38')
@login_required
def report_38():
    _require_access()
    start, end = _period()
    sla = _sla_hours()
    sla_box = (f'<div><label class="rfp-label" for="sla-input">Reporting target (hours)</label>'
               f'<input id="sla-input" type="number" name="sla_hours" value="{sla:g}" min="0.25" max="48" '
               f'step="0.25" class="rfp-input" aria-label="Reporting target in hours"></div>')
    from markupsafe import Markup
    return render_template('report_38.html', start=start, end=end,
                           filters=panel_filters(request.args, ('modality',)),
                           sla_box=Markup(sla_box))


@report_38_bp.route('/report/38/data')
@login_required
def data():
    _require_access()
    start, end = _period()
    # The box is in hours; every SLA comparison downstream is in minutes.
    sla_limit = round(_sla_hours() * 60)
    params    = {'start': start, 'end': end}
    # the panel's modality dropdown, matched the way every report matches it
    mod_clause = ''
    if request.args.get('mod_enabled') == 'on' and picked(request.args, 'modality'):
        params['modalities'] = picked(request.args, 'modality')
        mod_clause = f"AND UPPER(TRIM(COALESCE(m.modality, s.study_modality, ''))) IN {picked_sql('modalities')}"

    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='report_38',
              detail={'from': start, 'to': end, 'sla': sla_limit,
                      'modality': params.get('modalities')})

    try:
        # Radiologist raw fallback chain (PACS signing_physician_* -> RIS
        # rep_study_last_composed_by -> rep_final_signed_by) wrapped through
        # physician_alias_map for a curated display name, same mechanism
        # super_report.py's radiologist matrix uses -- this CTE previously had no
        # alias resolution at all, so it could surface a raw RIS composed-by
        # username unresolved.
        _rad_base = ("COALESCE("
                     "NULLIF(TRIM(CONCAT(COALESCE(s.signing_physician_first_name,''), ' ', "
                     "COALESCE(s.signing_physician_last_name,''))), ''), "
                     "s.rep_study_last_composed_by, s.rep_final_signed_by)")
        _rad_join = rad_alias_join_sql(_rad_base)
        _rad_display = rad_display_sql(_rad_base)

        # ── Base CTE ──────────────────────────────────────────────────────────
        cte = f"""
        WITH er AS (
            SELECT
                s.study_db_uid,
                s.accession_number,
                s.study_date,
                COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                {_rad_display} AS radiologist,
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.referring_physician_first_name,''), ' ',
                    COALESCE(s.referring_physician_last_name,'')
                )), '') AS physician,
                s.rep_final_timestamp,
                s.study_has_report,
                -- RIS-fallback completion timestamp: PACS's own rep_final_timestamp is not
                -- reliably synced back for recent studies at this hospital (radiologists sign
                -- in the RIS, not PACS) -- same root cause fixed for report_25 in migrations
                -- 0070/0075. Priority: rep_study_last_composed_ts (most reliable on this
                -- install) -> rep_final_timestamp (PACS-native, kept as fallback for older
                -- rows that do have it) -> hl7_oru_reports.result_datetime (RIS-sourced).
                COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) AS completed_ts,
                EXTRACT(HOUR FROM {_STUDY_DT}) AS study_hour,
                CASE WHEN COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) - {_STUDY_DT})) / 60.0
                END AS final_tat_min
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
            {_rad_join}
            WHERE s.study_date BETWEEN :start AND :end
              AND {_ER_WHERE}
              AND COALESCE(m.modality, s.study_modality, 'Unknown') NOT IN ('SR', 'OT', 'BMD')
              {mod_clause}
        )
        """

        # ── KPIs ──────────────────────────────────────────────────────────────
        kpi = db.session.execute(text(cte + f"""
            SELECT
                COUNT(*)                                                         AS total,
                COUNT(*) FILTER (WHERE final_tat_min IS NOT NULL)                AS reported,
                ROUND(AVG(final_tat_min) FILTER (WHERE final_tat_min > 0), 1)   AS avg_tat,
                ROUND(AVG(final_tat_min) FILTER (WHERE final_tat_min > 0
                                                    AND final_tat_min <= 60), 1) AS avg_tat_within_sla,
                COUNT(*) FILTER (WHERE final_tat_min > 0
                                   AND final_tat_min <= {sla_limit})             AS within_sla,
                COUNT(*) FILTER (WHERE final_tat_min > {sla_limit})              AS breached
            FROM er
        """), params).mappings().fetchone()

        total       = int(kpi['total'] or 0)
        reported    = int(kpi['reported'] or 0)
        within_sla  = int(kpi['within_sla'] or 0)
        breached    = int(kpi['breached'] or 0)
        sla_pct     = round(within_sla / reported * 100, 1) if reported else 0
        avg_tat     = float(kpi['avg_tat'] or 0)

        # ── Unread ER today ───────────────────────────────────────────────────
        unread_rows = db.session.execute(text(f"""
            SELECT
                s.accession_number,
                COALESCE(m.modality, s.study_modality, '?') AS modality,
                ROUND(EXTRACT(EPOCH FROM (NOW() - {_STUDY_DT})) / 60.0) AS waiting_min
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
            WHERE s.study_date = CURRENT_DATE
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NULL
              AND COALESCE(s.study_has_report, false) = false
              AND {_ER_WHERE}
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT', 'BMD')
              {mod_clause}
            ORDER BY waiting_min DESC
            LIMIT 50
        """), {k: v for k, v in params.items() if k == 'modalities'}).mappings().fetchall()
        unread = [dict(r) for r in unread_rows]
        unread_count = len(unread)

        # ── Daily TAT trend ───────────────────────────────────────────────────
        trend_rows = db.session.execute(text(cte + """
            SELECT
                study_date::text                                        AS day,
                COUNT(*)                                                AS total,
                ROUND(AVG(final_tat_min) FILTER (WHERE final_tat_min > 0), 1) AS avg_tat
            FROM er
            GROUP BY study_date ORDER BY study_date
        """), params).mappings().fetchall()
        trend = [dict(r) for r in trend_rows]

        # ── TAT histogram (K-means clustered, not fixed-width bins) ────────────
        tat_val_rows = db.session.execute(text(cte + """
            SELECT final_tat_min FROM er WHERE final_tat_min > 0
        """), params).fetchall()
        tat_vals  = [float(r[0]) for r in tat_val_rows]
        histogram = _cluster_tat_histogram(tat_vals)

        # ── TAT by modality ───────────────────────────────────────────────────
        mod_rows = db.session.execute(text(cte + """
            SELECT modality,
                   ROUND(AVG(final_tat_min) FILTER (WHERE final_tat_min > 0), 1) AS avg_tat,
                   COUNT(*) AS cnt
            FROM er
            WHERE modality IS NOT NULL AND modality != 'Unknown'
            GROUP BY modality HAVING COUNT(*) >= 3
            ORDER BY avg_tat ASC
        """), params).mappings().fetchall()
        by_modality = [dict(r) for r in mod_rows]

        # ── Volume by hour ────────────────────────────────────────────────────
        hour_rows = db.session.execute(text(cte + """
            SELECT study_hour::int AS hour, COUNT(*) AS cnt
            FROM er WHERE study_hour IS NOT NULL
            GROUP BY study_hour ORDER BY study_hour
        """), params).mappings().fetchall()
        by_hour = [dict(r) for r in hour_rows]

        # ── TAT by radiologist ────────────────────────────────────────────────
        rad_rows = db.session.execute(text(cte + f"""
            SELECT radiologist,
                   ROUND(AVG(final_tat_min) FILTER (WHERE final_tat_min > 0), 1) AS avg_tat,
                   COUNT(*) AS cnt,
                   COUNT(*) FILTER (WHERE final_tat_min > 0 AND final_tat_min <= {sla_limit}) AS within_sla
            FROM er
            WHERE radiologist IS NOT NULL
            GROUP BY radiologist HAVING COUNT(*) >= 3
            ORDER BY avg_tat ASC LIMIT 15
        """), params).mappings().fetchall()
        by_radiologist = [dict(r) for r in rad_rows]

        return jsonify({
            'kpi': {
                'total':        total,
                'reported':     reported,
                'avg_tat':      avg_tat,
                'sla_pct':      sla_pct,
                'within_sla':   within_sla,
                'breached':     breached,
                'unread_today': unread_count,
                'sla_limit':    sla_limit,
            },
            'unread':        unread,
            'trend':         trend,
            'histogram':     histogram,
            'by_modality':   by_modality,
            'by_hour':       by_hour,
            'by_radiologist':by_radiologist,
            'error':         None,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


from routes.report_registry import register_report  # noqa: E402
register_report(REPORT_ID, report_38_bp, report_38, None)
