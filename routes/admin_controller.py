from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, ReportAccessControl, UserPagePermission, db
from sqlalchemy import func, text
from datetime import datetime
import sys, os
_etl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ETL_JOBS')
if _etl_path not in sys.path: sys.path.insert(0, _etl_path)
from etl_settings import ETL_GEAR

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

    # Demo mode settings
    demo_rows = db.session.execute(
        text("SELECT key, value FROM settings WHERE key IN ('demo_mode','demo_start','demo_end')")
    ).fetchall()
    demo = {r[0]: r[1] for r in demo_rows}
    demo_mode  = demo.get('demo_mode', 'false').lower() == 'true'
    demo_start = demo.get('demo_start', '')
    demo_end   = demo.get('demo_end', '')

    # Build page permissions map: {user_id: {page_key: is_enabled}}
    all_perms = UserPagePermission.query.all()
    page_perms = {}
    for p in all_perms:
        page_perms.setdefault(p.user_id, {})[p.page_key] = p.is_enabled

    page_keys = ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru']

    return render_template(
        'admin_panel.html',
        users          = users,
        reports        = reports,
        etl_logs       = etl_logs,
        pagination     = pagination,
        last_sync_time = last_sync_time,
        selected_date  = selected_date,
        etl_gear       = ETL_GEAR,
        page_perms     = page_perms,
        page_keys      = page_keys,
        demo_mode      = demo_mode,
        demo_start     = demo_start,
        demo_end       = demo_end,
    )


@admin_bp.route('/users')
@login_required
def user_management():
    if current_user.role != 'admin':
        return abort(403)

    users = User.query.order_by(User.username).all()
    all_perms = UserPagePermission.query.all()
    page_perms = {}
    for p in all_perms:
        page_perms.setdefault(p.user_id, {})[p.page_key] = p.is_enabled

    page_keys = ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru']

    return render_template('user_management.html',
        users=users, page_perms=page_perms, page_keys=page_keys)


@admin_bp.route('/users/permissions', methods=['POST'])
@login_required
def update_user_permissions():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')
    page_key = data.get('page_key')
    enabled  = bool(data.get('enabled'))

    if not user_id or not page_key:
        return jsonify({'status': 'error', 'message': 'Missing fields'}), 400

    perm = UserPagePermission.query.filter_by(user_id=user_id, page_key=page_key).first()
    if perm:
        perm.is_enabled = enabled
    else:
        db.session.add(UserPagePermission(user_id=user_id, page_key=page_key, is_enabled=enabled))
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/role', methods=['POST'])
@login_required
def update_user_role():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')
    new_role = data.get('role')

    if new_role not in ('viewer', 'tec'):
        return jsonify({'status': 'error', 'message': 'Invalid role'}), 400

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    user.role = new_role
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/delete', methods=['POST'])
@login_required
def delete_user():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    UserPagePermission.query.filter_by(user_id=user_id).delete()
    ReportAccessControl.query.filter_by(user_id=user_id).delete()
    db.session.delete(user)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/sync-mappings', methods=['POST'])
@login_required
def sync_mappings():
    if current_user.role != 'admin':
        return abort(403)
    try:
        from ETL_JOBS.etl_runner import _sync_lookup_tables
        _sync_lookup_tables(db.engine)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@admin_bp.route('/demo-mode', methods=['POST'])
@login_required
def set_demo_mode():
    if current_user.role != 'admin':
        return abort(403)
    data    = request.get_json()
    enabled = 'true' if data.get('enabled') else 'false'
    start   = data.get('start', '')
    end     = data.get('end', '')
    for key, val in [('demo_mode', enabled), ('demo_start', start), ('demo_end', end)]:
        exists = db.session.execute(text("SELECT 1 FROM settings WHERE key = :k"), {'k': key}).fetchone()
        if exists:
            db.session.execute(text("UPDATE settings SET value = :v WHERE key = :k"), {'k': key, 'v': val})
        else:
            db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v)"), {'k': key, 'v': val})
    db.session.commit()
    return jsonify({'status': 'ok'})


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
