from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db
from datetime import date, timedelta

er_bp = Blueprint('er', __name__)

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
    ranges when sorted by centroid, so each cluster becomes one clean "low-high min"
    bucket. k is clamped down to len(vals) when there isn't enough data for 10
    distinct clusters (K-means requires n_samples >= n_clusters).

    Returns [] for no data, or a list of {bucket, cnt, avg} sorted by avg ascending,
    plus a 'k' key on each row so the frontend can show how many clusters were found.
    """
    if not vals:
        return []

    try:
        from sklearn.cluster import KMeans
    except ImportError:
        return [{'bucket': f'{round(min(vals))}-{round(max(vals))} min',
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
            'bucket': f'{round(min(cvals))}-{round(max(cvals))} min',
            'cnt': len(cvals),
            'avg': round(sum(cvals) / len(cvals), 1),
        })
    clusters.sort(key=lambda row: row['avg'])
    for row in clusters:
        row['k'] = len(clusters)
    return clusters


@er_bp.route('/er')
@login_required
def er_page():
    if current_user.role == 'tec':
        abort(403)
    default_end   = date.today().isoformat()
    default_start = (date.today() - timedelta(days=30)).isoformat()
    return render_template('er_dashboard.html',
                           default_start=default_start,
                           default_end=default_end)


@er_bp.route('/er/data')
@login_required
def er_data():
    if current_user.role == 'tec':
        abort(403)

    start     = request.args.get('start', (date.today() - timedelta(days=30)).isoformat())
    end       = request.args.get('end',   date.today().isoformat())
    sla_limit = int(request.args.get('sla', 60))
    params    = {'start': start, 'end': end}

    from utils.audit import log_event
    log_event('er_accessed', category='report', resource_type='er_dashboard',
              detail={'from': start, 'to': end, 'sla': sla_limit})

    try:
        # ── Base CTE ──────────────────────────────────────────────────────────
        cte = f"""
        WITH er AS (
            SELECT
                s.study_db_uid,
                s.accession_number,
                s.study_date,
                COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                -- signing_physician_first/last_name (PACS-native) is sparse/unreliable on
                -- this install -- radiologists sign in the RIS, not PACS (same root cause
                -- as completed_ts below, and already fixed the same way in report_32.py's
                -- radiologist resolution). Falls back to the PACS RIS-sourced
                -- rep_study_last_composed_by, then rep_final_signed_by.
                COALESCE(
                    NULLIF(TRIM(CONCAT(
                        COALESCE(s.signing_physician_first_name,''), ' ',
                        COALESCE(s.signing_physician_last_name,'')
                    )), ''),
                    s.rep_study_last_composed_by,
                    s.rep_final_signed_by
                ) AS radiologist,
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
                -- ER Volume by Hour of Day: {_STUDY_DT} (insert_time) was flattening every ER
                -- order onto midnight. Confirmed with HIS/clinical (2026-08-20): ORC-7.4 (Quantity/
                -- Timing, Start date/time -- hl7_orders.orc_start_datetime, migration 0110) is the
                -- reliable order-start time here. Scoped to this chart only -- final_tat_min below
                -- still anchors on {_STUDY_DT}, unchanged, since TAT wasn't reported as wrong.
                -- Falls back to {_STUDY_DT} for ER studies with no matching hl7_orders row, so
                -- unmatched studies don't just vanish from the chart.
                EXTRACT(HOUR FROM COALESCE(ho.orc_start_datetime, {_STUDY_DT})) AS study_hour,
                CASE WHEN COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) - {_STUDY_DT})) / 60.0
                END AS final_tat_min
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
            LEFT JOIN LATERAL (
                SELECT orc_start_datetime
                FROM hl7_orders
                WHERE accession_number = s.accession_number
                  AND orc_start_datetime IS NOT NULL
                ORDER BY received_at DESC
                LIMIT 1
            ) ho ON true
            WHERE s.study_date BETWEEN :start AND :end
              AND {_ER_WHERE}
              AND COALESCE(m.modality, s.study_modality, 'Unknown') NOT IN ('SR', 'OT')
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
              AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
            ORDER BY waiting_min DESC
            LIMIT 50
        """), {}).mappings().fetchall()
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
