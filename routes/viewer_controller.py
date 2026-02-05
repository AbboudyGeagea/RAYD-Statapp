# routes/viewer_controller.py
from flask import Blueprint, render_template, request, abort, Response
from flask_login import login_required, current_user
from db import ReportAccessControl, db

# Import report functions directly
from routes.report_22 import report_22 as report_22_func, export_report_22 as export_22_func
from routes.report_23 import report_23 as report_23_func, export_report_23 as export_23_func
from routes.report_27 import report_27 as report_27_func, export_report_27 as export_27_func
from routes.report_25 import report_25 as report_25_func, export_report_25 as export_25_func

viewer_bp = Blueprint('viewer', __name__, url_prefix='/viewer')


@viewer_bp.route('/')
@login_required
def index():
    return viewer_dashboard()


@viewer_bp.route('/dashboard')
@login_required
def viewer_dashboard():
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
    else:
        abort(404, description=f"Export for Report {report_id} is not implemented yet")

