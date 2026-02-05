from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ReportAccessControl, ETLJobLog, db
from sqlalchemy import func, and_
import subprocess
import sys
import os

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

@admin_bp.route('/dashboard', endpoint='admin_dashboard')
@login_required
def admin_dashboard():
    if current_user.role != 'admin':
        flash("Admin access required.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))
    
    # --- 1. User Management Data ---
    users = User.query.order_by(User.username).all()
    target_user_id = request.args.get('target_user_id')
    target_user = None
    enabled_report_ids = []
    
    if target_user_id:
        target_user = db.session.get(User, target_user_id)
        if target_user:
            access_records = ReportAccessControl.query.filter_by(user_id=target_user_id).all()
            enabled_report_ids = [r.report_template_id for r in access_records]

    # --- 2. Report Templates Data ---
    reports = ReportTemplate.query.order_by(ReportTemplate.report_name).all()
    
    # --- 3. ETL Monitoring (Status Hub Logic) ---
    subquery = db.session.query(
        ETLJobLog.job_name, 
        func.max(ETLJobLog.id).label('max_id')
    ).group_by(ETLJobLog.job_name).subquery()
    
    etl_logs = ETLJobLog.query.join(
        subquery, and_(ETLJobLog.id == subquery.c.max_id)
    ).order_by(ETLJobLog.id.desc()).all()

    # --- 4. Last Sync Badge ---
    last_sync_entry = ETLJobLog.query.filter_by(status='SUCCESS').order_by(ETLJobLog.end_time.desc()).first()
    last_sync_time = last_sync_entry.end_time.strftime('%b %d, %H:%M') if last_sync_entry else "Never"
    
    return render_template(
        'admin_panel.html', 
        users=users, 
        target_user=target_user, 
        reports=reports, 
        enabled_report_ids=enabled_report_ids,
        etl_logs=etl_logs,
        last_sync_time=last_sync_time
    )

@admin_bp.route('/etl/refresh')
@login_required
def refresh_etl_logs():
    if current_user.role != 'admin': return abort(403)
    date_str = request.args.get('date')
    if date_str:
        logs = ETLJobLog.query.filter(func.date(ETLJobLog.start_time) == date_str).order_by(ETLJobLog.id.desc()).all()
    else:
        subquery = db.session.query(ETLJobLog.job_name, func.max(ETLJobLog.id).label('max_id')).group_by(ETLJobLog.job_name).subquery()
        logs = ETLJobLog.query.join(subquery, and_(ETLJobLog.id == subquery.c.max_id)).order_by(ETLJobLog.id.desc()).all()
    return render_template('admin/etl_log_rows.html', etl_logs=logs)

@admin_bp.route('/etl/trigger', methods=['POST'])
@login_required
def trigger_etl():
    if current_user.role != 'admin': return abort(403)
    script_path = os.path.join(os.getcwd(), "scripts", "etl_runner.py")
    try:
        subprocess.Popen([sys.executable, script_path])
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@admin_bp.route('/user/<int:user_id>/report/<int:report_id>/toggle', methods=['POST'])
@login_required
def toggle_report_access(user_id, report_id):
    if current_user.role != 'admin': return abort(403)
    try:
        existing = ReportAccessControl.query.filter_by(user_id=user_id, report_template_id=report_id).first()
        if existing: db.session.delete(existing)
        else: db.session.add(ReportAccessControl(user_id=user_id, report_template_id=report_id))
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
