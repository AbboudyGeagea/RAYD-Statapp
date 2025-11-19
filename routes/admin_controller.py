# routes/admin_controller.py
from flask import Blueprint, render_template, redirect, url_for, request, jsonify, flash
from flask_login import login_required, current_user
from db import User, ReportTemplate, ReportAccessControl, db

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# ----------------------------
# LIST USERS
# ----------------------------
@admin_bp.route('/users', methods=['GET'])
@login_required
def list_users():
    if current_user.role != 'admin':
        flash("Unauthorized", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    users = User.query.order_by(User.username).all()
    return render_template('admin_users.html', users=users)

# ----------------------------
# MANAGE REPORTS FOR USER
# ----------------------------
@admin_bp.route('/user/<int:user_id>/reports', methods=['GET'])
@login_required
def manage_user_reports(user_id):
    if current_user.role != 'admin':
        flash("Unauthorized", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    user = User.query.get_or_404(user_id)
    reports = ReportTemplate.query.order_by(ReportTemplate.report_name).all()
    access_dict = {
        r.report_id: ReportAccessControl.query.filter_by(user_id=user.id, report_template_id=r.report_id).first()
        for r in reports
    }
    return render_template('admin_user_reports.html', user=user, reports=reports, access_dict=access_dict)

# ----------------------------
# TOGGLE REPORT ACCESS
# ----------------------------
@admin_bp.route('/user/<int:user_id>/report/<int:report_id>/toggle', methods=['POST'])
@login_required
def toggle_report_access(user_id, report_id):
    if current_user.role != 'admin':
        return jsonify({"error": "unauthorized"}), 403

    access = ReportAccessControl.query.filter_by(user_id=user_id, report_template_id=report_id).first()
    if not access:
        access = ReportAccessControl(user_id=user_id, report_template_id=report_id, is_enabled=True)
        db.session.add(access)
    else:
        access.is_enabled = not access.is_enabled

    db.session.commit()
    return jsonify({"enabled": access.is_enabled})

