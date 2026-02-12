#admin_controller.py
from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, db # Removed ReportAccessControl
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
    
    # --- 1. User Management (Simplified: No access control logic) ---
    users = User.query.order_by(User.username).all()

    # --- 2. Report Templates Data ---
    reports = ReportTemplate.query.order_by(ReportTemplate.report_name).all()
    
    # --- 3. ETL Monitoring ---
    subquery = db.session.query(
        ETLJobLog.job_name, 
        func.max(ETLJobLog.id).label('max_id')
    ).group_by(ETLJobLog.job_name).subquery()
    
    etl_logs = ETLJobLog.query.join(
        subquery, and_(ETLJobLog.id == subquery.c.max_id)
    ).order_by(ETLJobLog.id.desc()).all()

    # --- 4. Last Sync Badge (Safety added for null end_time) ---
    last_sync_entry = ETLJobLog.query.filter_by(status='SUCCESS').order_by(ETLJobLog.end_time.desc()).first()
    last_sync_time = "Never"
    if last_sync_entry and last_sync_entry.end_time:
        last_sync_time = last_sync_entry.end_time.strftime('%b %d, %H:%M')
    
    return render_template(
        'admin_panel.html', 
        users=users, 
        reports=reports, 
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
        # Using Popen to run detached so the UI doesn't hang
        subprocess.Popen([sys.executable, script_path])
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Toggle route removed as per requirement #1
