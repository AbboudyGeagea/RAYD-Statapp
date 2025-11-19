# routes/viewer_controller.py
from flask import Blueprint, render_template
from flask_login import login_required, current_user
from db import ReportTemplate, ReportAccessControl

viewer_bp = Blueprint('viewer', __name__, url_prefix='/viewer')


@viewer_bp.route('/dashboard')
@login_required
def viewer_dashboard():
    is_admin = current_user.role == 'admin'

    if is_admin:
        reports = (
            ReportTemplate.query
            .filter_by(is_base=True)
            .order_by(ReportTemplate.report_name)
            .all()
        )
        enabled_report_ids = [r.report_id for r in reports]

    else:
        access = ReportAccessControl.query.filter_by(
            user_id=current_user.id,
            is_enabled=True
        ).all()

        enabled_report_ids = [a.report_template_id for a in access]

        reports = (
            ReportTemplate.query
            .filter(
                ReportTemplate.report_id.in_(enabled_report_ids),
                ReportTemplate.is_base.is_(True)
            )
            .order_by(ReportTemplate.report_name)
            .all()
        )

    return render_template(
        "viewer_dashboard.html",
        reports=reports,
        enabled_report_ids=enabled_report_ids,
        is_admin=is_admin
    )

