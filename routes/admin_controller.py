from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, db
from sqlalchemy import func
from datetime import datetime

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

@admin_bp.route('/dashboard', endpoint='admin_dashboard')
@login_required
def admin_dashboard():
    if current_user.role != 'admin':
        flash("Admin access required.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # --- 1. User & Report Management ---
    users   = User.query.order_by(User.username).all()
    reports = ReportTemplate.query.order_by(ReportTemplate.report_name).all()

    # --- 2. Date & Pagination Logic ---
    selected_date = request.args.get('date')   # YYYY-MM-DD
    page     = request.args.get('page', 1, type=int)
    per_page = 20

    # Base query — newest first, all columns including the new ones
    query = ETLJobLog.query.order_by(ETLJobLog.start_time.desc())

    if selected_date:
        query = query.filter(func.date(ETLJobLog.start_time) == selected_date)

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    etl_logs   = pagination.items

    # --- 3. System Status ---
    last_sync_entry = (
        ETLJobLog.query
        .filter_by(status='SUCCESS')
        .order_by(ETLJobLog.end_time.desc())
        .first()
    )
    last_sync_time = (
        last_sync_entry.end_time.strftime('%b %d, %H:%M')
        if last_sync_entry and last_sync_entry.end_time
        else "Never"
    )

    return render_template(
        'admin_panel.html',
        users          = users,
        reports        = reports,
        etl_logs       = etl_logs,
        pagination     = pagination,
        last_sync_time = last_sync_time,
        selected_date  = selected_date,
    )


@admin_bp.route('/etl/trigger', methods=['POST'])
@login_required
def trigger_etl():
    if current_user.role != 'admin':
        return abort(403)

    try:
        from flask import current_app
        from ETL_JOBS.etl_runner import execute_sync
        import threading

        def _run():
            with current_app.app_context():
                execute_sync(current_app._get_current_object())

        threading.Thread(target=_run, daemon=True).start()
        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
