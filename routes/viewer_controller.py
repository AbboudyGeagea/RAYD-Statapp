# routes/viewer_controller.py
from flask import Blueprint, render_template, request, abort, Response, jsonify, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import text
from db import ReportAccessControl, db
from utils.permissions import permission_required

from routes.report_registry import get_report

viewer_bp = Blueprint('viewer', __name__, url_prefix='/viewer')


def seed_report_access(user_id):
    """Grant access to every ReportTemplate for a user, skipping existing rows."""
    from db import ReportTemplate
    all_reports = ReportTemplate.query.filter_by(is_base=True).all()
    existing_ids = {
        r.report_template_id
        for r in ReportAccessControl.query.filter_by(user_id=user_id).all()
    }
    new_rows = [
        ReportAccessControl(user_id=user_id, report_template_id=r.report_id, is_enabled=True)
        for r in all_reports if r.report_id not in existing_ids
    ]
    if new_rows:
        db.session.bulk_save_objects(new_rows)
        db.session.commit()


@viewer_bp.route('/')
@login_required
def index():
    return viewer_dashboard()


@viewer_bp.route('/dashboard')
@login_required
def viewer_dashboard():
    if current_user.role == 'tec':
        return redirect(url_for('hl7_orders.hl7_orders_page'))

    from db import ReportTemplate

    is_admin = current_user.role == 'admin'

    if is_admin:
        reports = ReportTemplate.query.filter_by(is_base=True).all()
    else:
        # Auto-seed any missing access rows so existing viewers aren't locked out.
        seed_report_access(current_user.id)
        reports = (
            db.session.query(ReportTemplate)
            .join(
                ReportAccessControl,
                ReportTemplate.report_id == ReportAccessControl.report_template_id
            )
            .filter(
                ReportAccessControl.user_id == current_user.id,
                ReportAccessControl.is_enabled == True,
                ReportTemplate.is_base == True
            )
            .all()
        )

    return render_template('viewer_dashboard.html', reports=reports, is_admin=is_admin)


@viewer_bp.route('/briefing')
@login_required
def daily_briefing():
    try:
        row = db.session.execute(text("""
            WITH
            latest AS (SELECT MAX(study_date) AS d FROM etl_didb_studies),
            s AS MATERIALIZED (
                SELECT
                    study_date,
                    patient_location, patient_class, study_status,
                    rep_final_timestamp, rep_final_signed_by,
                    COALESCE(study_modality, 'Unknown') AS modality,
                    CASE WHEN rep_final_timestamp IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (rep_final_timestamp - study_date::timestamp))/60
                    END AS tat_min
                FROM etl_didb_studies
                WHERE study_date >= (SELECT d FROM latest) - 7
                  AND COALESCE(study_modality, '') != 'SR'
            ),
            avg30 AS (
                SELECT COALESCE(COUNT(*)::float / NULLIF(COUNT(DISTINCT study_date),0), 0) AS v
                FROM etl_didb_studies
                WHERE study_date BETWEEN (SELECT d FROM latest) - 30
                                     AND (SELECT d FROM latest) - 1
                  AND COALESCE(study_modality, '') != 'SR'
            ),
            top_mod AS (
                SELECT modality, COUNT(*) AS cnt
                FROM s WHERE study_date = (SELECT d FROM latest)
                GROUP BY modality ORDER BY cnt DESC LIMIT 1
            )
            SELECT
                (SELECT d   FROM latest)                                                              AS latest_date,
                (SELECT COUNT(*)::int FROM s WHERE study_date = (SELECT d FROM latest))               AS today_count,
                (SELECT COUNT(*)::int FROM s WHERE study_date = (SELECT d FROM latest) - 7)           AS last_week_count,
                (SELECT v   FROM avg30)                                                               AS avg_daily,
                (SELECT COUNT(*)::int FROM s
                 WHERE study_date = (SELECT d FROM latest) AND rep_final_timestamp IS NOT NULL)        AS signed_today,
                (SELECT COUNT(*)::int FROM s
                 WHERE study_date = (SELECT d FROM latest) AND study_status ILIKE '%unread%')          AS unread,
                (SELECT COUNT(DISTINCT rep_final_signed_by)::int FROM s
                 WHERE DATE(rep_final_timestamp) = (SELECT d FROM latest)
                   AND rep_final_signed_by IS NOT NULL)                                               AS active_rads,
                (SELECT ROUND(AVG(tat_min)::numeric,1) FROM s
                 WHERE study_date = (SELECT d FROM latest) AND tat_min > 0 AND tat_min < 2880)        AS avg_tat_today,
                (SELECT ROUND(AVG(tat_min)::numeric,1) FROM s
                 WHERE study_date = (SELECT d FROM latest) - 7 AND tat_min > 0 AND tat_min < 2880)   AS avg_tat_prev,
                (SELECT ROUND(AVG(tat_min)::numeric,1) FROM s
                 WHERE study_date = (SELECT d FROM latest)
                   AND tat_min > 0 AND tat_min < 2880
                   AND (UPPER(COALESCE(patient_location,'')) = 'ER'
                        OR patient_class ILIKE '%ER%'
                        OR patient_class ILIKE '%Emergency%'))                                        AS er_tat_today,
                (SELECT ROUND(AVG(tat_min)::numeric,1) FROM s
                 WHERE study_date = (SELECT d FROM latest) - 7
                   AND tat_min > 0 AND tat_min < 2880
                   AND (UPPER(COALESCE(patient_location,'')) = 'ER'
                        OR patient_class ILIKE '%ER%'
                        OR patient_class ILIKE '%Emergency%'))                                        AS er_tat_prev,
                (SELECT modality FROM top_mod)                                                        AS top_modality,
                (SELECT cnt::int FROM top_mod)                                                        AS top_mod_count,
                (SELECT COUNT(DISTINCT patient_class)::int FROM s
                 WHERE study_date = (SELECT d FROM latest) AND patient_class IS NOT NULL)              AS active_classes,
                (SELECT ROUND(AVG(tat_min)::numeric,1) FROM s
                 WHERE study_date BETWEEN (SELECT d FROM latest) - 30
                                      AND (SELECT d FROM latest) - 1
                   AND tat_min > 0 AND tat_min < 2880)                                                AS avg_tat_30d
        """)).fetchone()

        if not row or not row[0]:
            return jsonify({'headline': 'No data available.', 'kpis': [], 'insights': [], 'pct_vs_avg': 0})

        (latest_date, today_count, last_week_count, avg_daily, signed_today,
         unread, active_rads, avg_tat_today, avg_tat_prev,
         er_tat_today, er_tat_prev, top_modality, top_mod_count,
         active_classes, avg_tat_30d) = row

        today_count    = today_count    or 0
        last_week_count= last_week_count or 0
        avg_daily      = float(avg_daily or 0)
        signed_today   = signed_today   or 0
        unread         = unread         or 0
        active_rads    = active_rads    or 0

        # ── Derived comparisons ────────────────────────────────────────
        pct_vs_avg  = round((today_count - avg_daily) / avg_daily * 100) if avg_daily else 0
        vs_last_wk  = today_count - last_week_count
        sign_rate   = round(signed_today / today_count * 100) if today_count else 0

        def _fmt_tat(v): return f"{v}m" if v is not None else "—"
        def _diff_label(a, b, unit="m", lower_better=True):
            if a is None or b is None: return None
            d = round(float(a) - float(b), 1)
            if d == 0: return "unchanged"
            better = (d < 0) if lower_better else (d > 0)
            arrow = "↓" if d < 0 else "↑"
            color = "good" if better else "warn"
            return {"delta": f"{arrow} {abs(d)}{unit}", "color": color}

        tat_vs_prev  = _diff_label(avg_tat_today, avg_tat_prev)
        tat_vs_30d   = _diff_label(avg_tat_today, avg_tat_30d)
        er_vs_prev   = _diff_label(er_tat_today,  er_tat_prev)

        # ── Headline ───────────────────────────────────────────────────
        vol_word = "strong" if pct_vs_avg >= 10 else ("quiet" if pct_vs_avg <= -10 else "steady")
        wk_phrase = (f", {abs(vs_last_wk)} {'more' if vs_last_wk >= 0 else 'fewer'} than last {latest_date.strftime('%A') if latest_date else 'week'}")
        headline = (
            f"{'Busy' if pct_vs_avg >= 15 else ('Slow' if pct_vs_avg <= -15 else 'Steady')} day — "
            f"{today_count:,} {'study' if today_count == 1 else 'studies'} "
            f"({'+'if pct_vs_avg >= 0 else ''}{pct_vs_avg}% vs 30-day avg{wk_phrase})."
        )

        # ── KPI pills ──────────────────────────────────────────────────
        kpis = [
            {"label": "Studies Today",   "value": f"{today_count:,}",
             "sub": f"{'+'if pct_vs_avg>=0 else ''}{pct_vs_avg}% vs 30d avg",
             "trend": "up" if pct_vs_avg >= 0 else "down"},
            {"label": "vs Last Week",    "value": f"{'+'if vs_last_wk>=0 else ''}{vs_last_wk}",
             "sub": f"same weekday ({last_week_count:,} studies)",
             "trend": "up" if vs_last_wk >= 0 else "down"},
            {"label": "Signed",          "value": f"{signed_today:,}",
             "sub": f"{sign_rate}% reporting rate",
             "trend": "good" if sign_rate >= 80 else ("warn" if sign_rate >= 50 else "down")},
            {"label": "Unread",          "value": f"{unread:,}",
             "sub": "pending report",
             "trend": "good" if unread == 0 else ("warn" if unread <= 10 else "down")},
            {"label": "Avg TAT",         "value": _fmt_tat(avg_tat_today),
             "sub": (f"{tat_vs_prev['delta']} vs last week" if isinstance(tat_vs_prev, dict) else "vs last week"),
             "trend": tat_vs_prev.get("color", "neutral") if isinstance(tat_vs_prev, dict) else "neutral"},
            {"label": "ER TAT",          "value": _fmt_tat(er_tat_today),
             "sub": (f"{er_vs_prev['delta']} vs last week" if isinstance(er_vs_prev, dict) else "no ER data"),
             "trend": er_vs_prev.get("color", "neutral") if isinstance(er_vs_prev, dict) else "neutral"},
            {"label": "Active Rads",     "value": str(active_rads),
             "sub": "signed reports today",
             "trend": "neutral"},
            {"label": "Top Modality",    "value": top_modality or "—",
             "sub": f"{top_mod_count or 0} studies" if top_modality else "",
             "trend": "neutral"},
        ]

        # ── Insight sentences ──────────────────────────────────────────
        insights = []

        # Volume
        avg_str = f"{avg_daily:.0f}" if avg_daily else "—"
        insights.append(
            f"Volume is {abs(pct_vs_avg)}% {'above' if pct_vs_avg >= 0 else 'below'} the 30-day average "
            f"({today_count:,} studies today vs {avg_str} daily average). "
            f"Compared to the same day last week: {'+' if vs_last_wk >= 0 else ''}{vs_last_wk} studies "
            f"({last_week_count:,} last {latest_date.strftime('%A') if latest_date else 'week'})."
        )

        # Reporting throughput
        unsigned = today_count - signed_today
        insights.append(
            f"{signed_today:,} of {today_count:,} studies ({sign_rate}%) have a final report. "
            f"{'All studies are reported.' if unread == 0 else f'{unread:,} remain unread — reporting backlog detected.' if unread > 20 else f'{unread:,} studies are still pending a report.'}"
        )

        # TAT analysis
        if avg_tat_today is not None:
            tat_parts = [f"Overall average TAT today is {avg_tat_today}m"]
            if isinstance(tat_vs_30d, dict):
                tat_parts.append(f"({tat_vs_30d['delta']} vs the 30-day baseline of {_fmt_tat(avg_tat_30d)})")
            if isinstance(tat_vs_prev, dict):
                tat_parts.append(f"and {tat_vs_prev['delta']} vs last week ({_fmt_tat(avg_tat_prev)})")
            insights.append(" ".join(tat_parts) + ".")

        # ER TAT
        if er_tat_today is not None:
            er_parts = [f"ER turnaround is averaging {er_tat_today}m today"]
            if isinstance(er_vs_prev, dict):
                direction = "improvement" if er_vs_prev["color"] == "good" else "regression"
                er_parts.append(f"— a {er_vs_prev['delta']} {direction} vs the same day last week ({_fmt_tat(er_tat_prev)})")
            insights.append(" ".join(er_parts) + ".")
        else:
            insights.append("No ER studies with signed reports found for today.")

        # Radiologists + modality
        if active_rads:
            mod_note = f"Top modality is {top_modality} with {top_mod_count} studies." if top_modality else ""
            insights.append(
                f"{active_rads} radiologist{'s' if active_rads != 1 else ''} signed reports today. "
                + mod_note
            )

        return jsonify({
            'headline':    headline,
            'kpis':        kpis,
            'insights':    insights,
            'pct_vs_avg':  pct_vs_avg,
            'today':       today_count,
            'unread':      unread,
            'active_rads': active_rads,
            'er_tat_today': float(er_tat_today) if er_tat_today else None,
            # legacy plain-text for any other consumer
            'text': headline + " " + " ".join(insights),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'headline': 'Briefing unavailable.', 'kpis': [], 'insights': [], 'text': str(e)})


@viewer_bp.route('/yesterday')
@login_required
def yesterday_overview():
    try:
        rows = lambda q, p={}: db.session.execute(text(q), p).fetchall()
        one  = lambda q, p={}: db.session.execute(text(q), p).fetchone()

        # ── Query 1: all scalar KPIs in one round trip ─────────────────
        # MATERIALIZED forces PG to compute s/o once and reuse across all
        # scalar subqueries.  The first_visit window is capped at 1 year —
        # the old unbounded GROUP BY scanned the entire table every time.
        kpi = one("""
            WITH
            s AS MATERIALIZED (
                SELECT patient_db_uid, storing_ae, patient_location,
                       referring_physician_first_name, referring_physician_last_name
                FROM etl_didb_studies
                WHERE study_date = CURRENT_DATE - 1
            ),
            o AS MATERIALIZED (
                SELECT order_status, scheduled_datetime
                FROM etl_orders
                WHERE scheduled_datetime::date = CURRENT_DATE - 1
            ),
            fv AS (
                SELECT patient_db_uid, MIN(study_date) AS first_date
                FROM etl_didb_studies
                WHERE study_date >= CURRENT_DATE - 365
                GROUP BY patient_db_uid
            ),
            avg7 AS (
                SELECT COALESCE(
                    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT study_date), 0), 1), 0
                ) AS v
                FROM etl_didb_studies
                WHERE study_date >= CURRENT_DATE - 8
                  AND study_date <  CURRENT_DATE - 1
            ),
            peak AS (
                SELECT EXTRACT(HOUR FROM scheduled_datetime)::int AS hr, COUNT(*) AS cnt
                FROM o GROUP BY 1 ORDER BY 2 DESC LIMIT 1
            ),
            nr AS (
                SELECT
                    COUNT(*) FILTER (WHERE fv.first_date = CURRENT_DATE - 1) AS new_pts,
                    COUNT(*) FILTER (WHERE fv.first_date <  CURRENT_DATE - 1) AS ret_pts
                FROM s LEFT JOIN fv USING (patient_db_uid)
            )
            SELECT
                (SELECT COUNT(*)::int            FROM o)                                              AS orders_total,
                (SELECT COUNT(*)::int            FROM o WHERE UPPER(COALESCE(order_status,''))='CA') AS orders_ca,
                (SELECT COUNT(*)::int            FROM s)                                              AS studies_total,
                (SELECT v                        FROM avg7)                                           AS avg_7d,
                (SELECT COUNT(DISTINCT patient_db_uid)::int FROM s)                                   AS unique_patients,
                (SELECT new_pts::int             FROM nr)                                             AS new_patients,
                (SELECT ret_pts::int             FROM nr)                                             AS returning_patients,
                (SELECT COUNT(*)::int            FROM s WHERE UPPER(COALESCE(patient_location,''))='ER') AS er_patients,
                (SELECT hr                       FROM peak)                                           AS peak_hr,
                (SELECT cnt::int                 FROM peak)                                           AS peak_cnt
        """)

        orders_total       = kpi[0] or 0
        orders_ca          = kpi[1] or 0
        studies_total      = kpi[2] or 0
        avg_7d             = float(kpi[3]) if kpi[3] else 0.0
        unique_patients    = kpi[4] or 0
        new_patients       = kpi[5] or 0
        returning_patients = kpi[6] or 0
        er_patients        = kpi[7] or 0
        peak_hour          = {"hour": kpi[8], "count": kpi[9]} if kpi[8] is not None else None
        vs_avg             = round((studies_total - avg_7d) / avg_7d * 100, 1) if avg_7d else None

        # ── Query 2a: top referring physicians ─────────────────────────
        phys_rows = rows("""
            SELECT
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ',
                    referring_physician_first_name,
                    referring_physician_last_name)), ''), 'Unknown') AS name,
                COUNT(*)::int AS count
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
              AND referring_physician_first_name IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 5
        """)
        physicians = [{"name": r[0], "count": r[1]} for r in phys_rows]

        # ── Query 2b: AE by study count ────────────────────────────────
        ae_rows = rows("""
            SELECT storing_ae AS ae, COUNT(*)::int AS count
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
              AND storing_ae IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        ae_by_count_raw = [{"ae": r[0], "count": r[1]} for r in ae_rows]

        # ── Query 3: AE utilisation (join-heavy, kept separate) ────────
        ae_by_util = rows("""
            SELECT
                s.storing_ae,
                SUM(COALESCE(pm.duration_minutes, 15))                                    AS used_mins,
                COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480)  AS capacity_mins
            FROM etl_didb_studies s
            LEFT JOIN procedure_duration_map pm
                ON pm.procedure_code = s.procedure_code
            LEFT JOIN device_weekly_schedule ws
                ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(s.storing_ae))
               AND ws.day_of_week = (EXTRACT(ISODOW FROM (CURRENT_DATE - 1))::int - 1)
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            WHERE s.study_date = CURRENT_DATE - 1
              AND s.storing_ae IS NOT NULL
            GROUP BY s.storing_ae
            HAVING COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480) > 0
            ORDER BY
                SUM(COALESCE(pm.duration_minutes, 15))::float /
                NULLIF(COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480), 0)
                DESC
        """)

        util_list = [
            {"ae": r[0], "used_min": int(r[1] or 0), "cap_min": int(r[2] or 0),
             "util_pct": round(int(r[1] or 0) / int(r[2]) * 100, 1) if r[2] else 0}
            for r in ae_by_util
        ]

        return jsonify({
            "orders_total":      orders_total,
            "orders_cancelled":  orders_ca,
            "cancel_rate":       round(orders_ca / orders_total * 100, 1) if orders_total else 0,
            "studies_total":     studies_total,
            "avg_7d":            avg_7d,
            "vs_avg_pct":        vs_avg,
            "unique_patients":   unique_patients,
            "new_patients":      new_patients,
            "returning_patients":returning_patients,
            "er_patients":       er_patients,
            "peak_hour":         peak_hour,
            "physicians":        physicians,
            "ae_by_count":       ae_by_count_raw,
            "ae_by_util":        util_list,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@viewer_bp.route('/<int:report_id>', methods=['GET', 'POST'])
@login_required
def viewer_report(report_id):
    """Render report directly based on report_id"""
    # Access control for non-admins
    if current_user.role != 'admin':
        access = ReportAccessControl.query.filter_by(
            user_id=current_user.id,
            report_template_id=report_id,
            is_enabled=True
        ).first()
        if not access:
            abort(403)

    reg = get_report(report_id)
    if reg and reg['view']:
        return reg['view']()
    abort(404, description=f"Report {report_id} is not implemented yet")


@viewer_bp.route('/<int:report_id>/export', methods=['POST'])
@login_required
@permission_required('can_export')
def viewer_export_report(report_id):
    """Export report directly, no url_for needed"""
    # Access control for non-admins
    if current_user.role != 'admin':
        access = ReportAccessControl.query.filter_by(
            user_id=current_user.id,
            report_template_id=report_id,
            is_enabled=True
        ).first()
        if not access:
            abort(403)

    reg = get_report(report_id)
    if reg and reg['export']:
        return reg['export']()
    abort(404, description=f"Export for Report {report_id} is not implemented yet")

