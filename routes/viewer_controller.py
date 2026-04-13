# routes/viewer_controller.py
from flask import Blueprint, render_template, request, abort, Response, jsonify, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import text
from db import ReportAccessControl, db

from routes.report_registry import get_report

viewer_bp = Blueprint('viewer', __name__, url_prefix='/viewer')


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
    one = lambda q, p={}: db.session.execute(text(q), p).scalar()
    parts = []
    meta  = {}

    # ── Latest day volume vs 30-day average ──────────────────────────
    try:
        latest_date = one("SELECT MAX(study_date) FROM etl_didb_studies")
        if latest_date:
            today_count = int(one(
                "SELECT COUNT(*) FROM etl_didb_studies WHERE study_date = :d",
                {'d': latest_date}
            ) or 0)
            avg_daily = float(one("""
                SELECT COALESCE(COUNT(*)::float / NULLIF(COUNT(DISTINCT study_date), 0), 0)
                FROM etl_didb_studies
                WHERE study_date BETWEEN :d::date - 30 AND :d::date - 1
            """, {'d': latest_date}) or 0)
            pct = round((today_count - avg_daily) / avg_daily * 100) if avg_daily else 0
            meta['today'] = today_count
            meta['pct_vs_avg'] = pct
            direction = "above" if pct >= 0 else "below"
            parts.append(
                f"Today: {today_count:,} {'study' if today_count == 1 else 'studies'}, "
                f"{abs(pct)}% {direction} the 30-day average."
            )
    except Exception:
        db.session.rollback()

    # ── ER TAT today vs same day last week ───────────────────────────
    try:
        er_filter = """(UPPER(COALESCE(patient_location,'')) = 'ER'
                        OR patient_class ILIKE '%ER%'
                        OR patient_class ILIKE '%Emergency%')"""
        er_tat_today = one(f"""
            SELECT ROUND(AVG(
                EXTRACT(EPOCH FROM (rep_final_timestamp - study_date::timestamp)) / 60
            )::numeric, 1)
            FROM etl_didb_studies
            WHERE study_date = (SELECT MAX(study_date) FROM etl_didb_studies)
              AND rep_final_timestamp IS NOT NULL
              AND {er_filter}
        """)
        er_tat_prev = one(f"""
            SELECT ROUND(AVG(
                EXTRACT(EPOCH FROM (rep_final_timestamp - study_date::timestamp)) / 60
            )::numeric, 1)
            FROM etl_didb_studies
            WHERE study_date = (SELECT MAX(study_date) FROM etl_didb_studies) - 7
              AND rep_final_timestamp IS NOT NULL
              AND {er_filter}
        """)
        if er_tat_today is not None and er_tat_prev is not None:
            diff = round(float(er_tat_prev) - float(er_tat_today), 1)
            if diff > 0:
                parts.append(f"ER turnaround improved by {diff} min vs last week ({er_tat_today} min avg).")
            elif diff < 0:
                parts.append(f"ER turnaround up {abs(diff)} min vs last week ({er_tat_today} min avg).")
            else:
                parts.append(f"ER turnaround unchanged at {er_tat_today} min avg.")
        elif er_tat_today is not None:
            parts.append(f"ER turnaround averaging {er_tat_today} min today.")
        meta['er_tat_today'] = float(er_tat_today) if er_tat_today else None
    except Exception:
        db.session.rollback()

    # ── Active radiologists today ─────────────────────────────────────
    try:
        active_rads = int(one("""
            SELECT COUNT(DISTINCT rep_final_signed_by)
            FROM etl_didb_studies
            WHERE DATE(rep_final_timestamp) = (SELECT MAX(study_date) FROM etl_didb_studies)
              AND rep_final_signed_by IS NOT NULL
        """) or 0)
        meta['active_rads'] = active_rads
        if active_rads:
            parts.append(f"{active_rads} radiologist{'s' if active_rads != 1 else ''} active.")
    except Exception:
        db.session.rollback()

    # ── Unread studies ────────────────────────────────────────────────
    try:
        unread = int(one("""
            SELECT COUNT(*) FROM etl_didb_studies
            WHERE study_date = (SELECT MAX(study_date) FROM etl_didb_studies)
              AND study_status ILIKE '%unread%'
        """) or 0)
        meta['unread'] = unread
        if unread:
            parts.append(f"{unread:,} {'study' if unread == 1 else 'studies'} pending report.")
    except Exception:
        db.session.rollback()

    text_out = ' '.join(parts) if parts else 'No data available for today.'
    return jsonify({'text': text_out, **meta})


@viewer_bp.route('/yesterday')
@login_required
def yesterday_overview():
    try:
        rows = lambda q, p={}: db.session.execute(text(q), p).fetchall()
        one  = lambda q, p={}: db.session.execute(text(q), p).fetchone()

        # ── Core counts ───────────────────────────────────────────────
        orders_total = one("""
            SELECT COUNT(*) FROM etl_orders
            WHERE scheduled_datetime::date = CURRENT_DATE - 1
        """)[0] or 0

        orders_ca = one("""
            SELECT COUNT(*) FROM etl_orders
            WHERE scheduled_datetime::date = CURRENT_DATE - 1
              AND UPPER(COALESCE(order_status,'')) = 'CA'
        """)[0] or 0

        studies_total = one("""
            SELECT COUNT(*) FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
        """)[0] or 0

        # ── 7-day average ─────────────────────────────────────────────
        avg_7d_row = one("""
            SELECT ROUND(AVG(cnt),1) FROM (
                SELECT study_date, COUNT(*) as cnt
                FROM etl_didb_studies
                WHERE study_date >= CURRENT_DATE - 8
                  AND study_date <  CURRENT_DATE - 1
                GROUP BY study_date
            ) sub
        """)
        avg_7d = float(avg_7d_row[0]) if avg_7d_row and avg_7d_row[0] else 0
        vs_avg = round((studies_total - avg_7d) / avg_7d * 100, 1) if avg_7d else None

        # ── Unique patients (via etl_patient_view fallback_id) ─────────
        unique_patients = one("""
            SELECT COUNT(DISTINCT p.fallback_id)
            FROM etl_didb_studies s
            JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
            WHERE s.study_date = CURRENT_DATE - 1
              AND p.fallback_id IS NOT NULL
        """)[0] or 0

        # ── New vs returning patients ──────────────────────────────────
        new_returning = one("""
            WITH yest AS (
                SELECT DISTINCT patient_db_uid FROM etl_didb_studies
                WHERE study_date = CURRENT_DATE - 1
            ),
            first_visit AS (
                SELECT patient_db_uid, MIN(study_date) as first_date
                FROM etl_didb_studies GROUP BY patient_db_uid
            )
            SELECT
                SUM(CASE WHEN f.first_date = CURRENT_DATE - 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN f.first_date < CURRENT_DATE - 1 THEN 1 ELSE 0 END)
            FROM yest y JOIN first_visit f ON f.patient_db_uid = y.patient_db_uid
        """)
        new_patients       = int(new_returning[0] or 0) if new_returning else 0
        returning_patients = int(new_returning[1] or 0) if new_returning else 0

        # ── ER patients ───────────────────────────────────────────────
        er_patients = one("""
            SELECT COUNT(*) FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
              AND UPPER(COALESCE(patient_location,'')) = 'ER'
        """)[0] or 0

        # ── Top referring physicians ───────────────────────────────────
        physicians = rows("""
            SELECT
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ',
                    referring_physician_first_name,
                    referring_physician_last_name)), ''), 'Unknown') as name,
                COUNT(*) as cnt
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
              AND referring_physician_first_name IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """)

        # ── Peak hour (from orders) ────────────────────────────────────
        peak_row = one("""
            SELECT EXTRACT(HOUR FROM scheduled_datetime)::int as hr, COUNT(*) as cnt
            FROM etl_orders
            WHERE scheduled_datetime::date = CURRENT_DATE - 1
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        peak_hour = {"hour": peak_row[0], "count": int(peak_row[1])} if peak_row else None

        # ── AE by procedure count ──────────────────────────────────────
        ae_by_count = rows("""
            SELECT storing_ae, COUNT(*) as cnt
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - 1
              AND storing_ae IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC
        """)

        # ── AE by utilization ──────────────────────────────────────────
        ae_by_util = rows("""
            SELECT
                s.storing_ae,
                SUM(COALESCE(pm.duration_minutes, 15)) as used_mins,
                COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480) as capacity_mins
            FROM etl_didb_studies s
            LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
            LEFT JOIN device_weekly_schedule ws
                ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(s.storing_ae))
               AND ws.day_of_week = (EXTRACT(ISODOW FROM (CURRENT_DATE - 1))::int - 1)
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
            WHERE s.study_date = CURRENT_DATE - 1
              AND s.storing_ae IS NOT NULL
            GROUP BY s.storing_ae
            HAVING COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480) > 0
            ORDER BY (SUM(COALESCE(pm.duration_minutes,15))::float /
                      NULLIF(COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480),0)) DESC
        """)

        util_list = []
        for r in ae_by_util:
            cap  = int(r[2]) if r[2] else 0
            used = int(r[1]) if r[1] else 0
            util_list.append({
                "ae":       r[0],
                "used_min": used,
                "cap_min":  cap,
                "util_pct": round(used / cap * 100, 1) if cap else 0
            })

        return jsonify({
            "orders_total":      int(orders_total),
            "orders_cancelled":  int(orders_ca),
            "cancel_rate":       round(orders_ca / orders_total * 100, 1) if orders_total else 0,
            "studies_total":     int(studies_total),
            "avg_7d":            avg_7d,
            "vs_avg_pct":        vs_avg,
            "unique_patients":   int(unique_patients),
            "new_patients":      new_patients,
            "returning_patients":returning_patients,
            "er_patients":       int(er_patients),
            "peak_hour":         peak_hour,
            "physicians":        [{"name": r[0], "count": int(r[1])} for r in physicians],
            "ae_by_count":       [{"ae": r[0], "count": int(r[1])} for r in ae_by_count],
            "ae_by_util":        util_list,
        })

    except Exception as e:
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

