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

_STUDY_DT = """(
    s.study_date::timestamp +
    CASE
        WHEN s.study_time ~ '^[0-9]{6}'
        THEN make_interval(
            hours => SUBSTRING(s.study_time,1,2)::int,
            mins  => SUBSTRING(s.study_time,3,2)::int,
            secs  => SUBSTRING(s.study_time,5,2)::int
        )
        WHEN s.study_time ~ '^[0-9]{2}:[0-9]{2}'
        THEN s.study_time::interval
        ELSE '0'::interval
    END
)"""


def _cluster_tat_histogram(vals, max_k=6):
    """K-means clustering of ER TAT values into natural groups, replacing fixed-width
    (0-30/30-60/...) bins with data-driven ones. Auto-selects k (2 <= k <= max_k) via
    inertia elbow -- same convention as nlp_worker/clustering.py's cluster_reports()
    (pick the smallest k where the next cluster only buys <15% inertia improvement).
    1-D K-means naturally produces contiguous, non-overlapping ranges when sorted by
    centroid, so each cluster becomes one clean "low-high min" bucket.

    Falls back to a single bucket spanning the full range when there's too little data
    for clustering to be meaningful (mirrors cluster_reports()'s own len-based bailout).
    Returns [] for no data, or a list of {bucket, cnt, avg} sorted by avg ascending, plus
    a 'k' key on each row so the frontend can show how many clusters were found.
    """
    if not vals:
        return []
    if len(vals) < 10:
        return [{'bucket': f'{round(min(vals))}-{round(max(vals))} min',
                  'cnt': len(vals), 'avg': round(sum(vals) / len(vals), 1), 'k': 1}]

    try:
        from sklearn.cluster import KMeans
    except ImportError:
        return [{'bucket': f'{round(min(vals))}-{round(max(vals))} min',
                  'cnt': len(vals), 'avg': round(sum(vals) / len(vals), 1), 'k': 1}]

    X = [[v] for v in vals]
    k_range = list(range(2, min(max_k, len(vals) // 5) + 1))
    if len(k_range) <= 1:
        k = 2
    else:
        inertias = []
        for k_try in k_range:
            km = KMeans(n_clusters=k_try, n_init=5, max_iter=100, random_state=42)
            km.fit(X)
            inertias.append(km.inertia_)
        k = k_range[0]
        for i in range(1, len(inertias)):
            drop = (inertias[i - 1] - inertias[i]) / (inertias[0] + 1e-9)
            if drop < 0.15:
                k = k_range[i]
                break

    km = KMeans(n_clusters=k, n_init=10, max_iter=300, random_state=42)
    labels = km.fit_predict(X)

    clusters = []
    for c in range(k):
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
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.signing_physician_first_name,''), ' ',
                    COALESCE(s.signing_physician_last_name,'')
                )), '') AS radiologist,
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.referring_physician_first_name,''), ' ',
                    COALESCE(s.referring_physician_last_name,'')
                )), '') AS physician,
                s.rep_final_timestamp,
                s.study_has_report,
                EXTRACT(HOUR FROM {_STUDY_DT}) AS study_hour,
                CASE WHEN s.rep_final_timestamp IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (s.rep_final_timestamp - {_STUDY_DT})) / 60.0
                END AS final_tat_min
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
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
            LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
            WHERE s.study_date = CURRENT_DATE
              AND (s.rep_final_timestamp IS NULL AND COALESCE(s.study_has_report, false) = false)
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

        # ── SLA breach heatmap (radiologist × modality) ───────────────────────
        heatmap_rows = db.session.execute(text(cte + f"""
            SELECT
                COALESCE(radiologist, 'Unassigned') AS radiologist,
                modality,
                COUNT(*)                                        AS breach_count,
                ROUND(AVG(final_tat_min - {sla_limit}))        AS avg_over_sla_min,
                ROUND(MAX(final_tat_min))                       AS worst_tat_min
            FROM er
            WHERE final_tat_min > {sla_limit}
              AND modality IS NOT NULL AND modality != 'Unknown'
            GROUP BY radiologist, modality
            ORDER BY breach_count DESC
        """), params).mappings().fetchall()
        breaches = [dict(r) for r in heatmap_rows]

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
            'breaches':      breaches,
            'error':         None,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
