# routes/viewer_controller.py
from flask import Blueprint, render_template, request, abort, Response, jsonify, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import text
from db import ReportAccessControl, db

# Import report functions directly
from routes.report_22 import report_22 as report_22_func, export_report_22 as export_22_func
from routes.report_23 import report_23 as report_23_func, export_report_23 as export_23_func
from routes.report_27 import report_27 as report_27_func, export_report_27 as export_27_func
from routes.report_25 import report_25 as report_25_func, export_report_25 as export_25_func
from routes.report_29 import report_29 as report_29_func, export_report_29 as export_29_func

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
                COALESCE(MAX(ws.std_opening_minutes), 0) as capacity_mins
            FROM etl_didb_studies s
            LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
            LEFT JOIN device_weekly_schedule ws
                ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(s.storing_ae))
               AND ws.day_of_week = (EXTRACT(ISODOW FROM (CURRENT_DATE - 1))::int - 1)
            WHERE s.study_date = CURRENT_DATE - 1
              AND s.storing_ae IS NOT NULL
            GROUP BY s.storing_ae
            HAVING COALESCE(MAX(ws.std_opening_minutes), 0) > 0
            ORDER BY (SUM(COALESCE(pm.duration_minutes,15))::float /
                      NULLIF(MAX(ws.std_opening_minutes),0)) DESC
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

    # MANUAL SWITCH: render the correct report
    if report_id == 22:
        return report_22_func()
    elif report_id == 23:
        return report_23_func()
    elif report_id == 27:
        return report_27_func()
    elif report_id == 25:
        return report_25_func()
    elif report_id == 29:
        return report_29_func()     
    else:
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

    # MANUAL SWITCH: call the correct export function
    if report_id == 22:
        return export_22_func()
    elif report_id == 27:
        return export_27_func()
    elif report_id == 23:
        return export_23_func()
    elif report_id == 25:
        return export_25_func()
    elif report_id == 29:
        return export_29_func()    
    else:
        abort(404, description=f"Export for Report {report_id} is not implemented yet")

