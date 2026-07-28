from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, ReportAccessControl, UserPagePermission, SchedulingEntry, UserAuditLog, active_sessions, db
from sqlalchemy import func, text
from datetime import datetime, timedelta, date as date_type
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
        last_sync_entry.end_time.strftime('%d %b, %H:%M')
        if last_sync_entry and last_sync_entry.end_time
        else "Never"
    )

    # --- 4. ETL Stats for KPI strip ---
    today_str = date_type.today().isoformat()
    etl_stats = db.session.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE start_time::date = CURRENT_DATE)                        AS runs_today,
            COUNT(*) FILTER (WHERE start_time >= NOW() - INTERVAL '7 days' AND status = 'SUCCESS')::float
              / NULLIF(COUNT(*) FILTER (WHERE start_time >= NOW() - INTERVAL '7 days'), 0) * 100
                                                                                            AS success_rate_7d,
            ROUND(AVG(duration_seconds) FILTER (
                WHERE status = 'SUCCESS' AND duration_seconds IS NOT NULL
                  AND start_time >= NOW() - INTERVAL '30 days'
            ))                                                                              AS avg_duration,
            COALESCE(SUM(records_processed) FILTER (WHERE start_time::date = CURRENT_DATE), 0)
                                                                                            AS records_today
        FROM etl_job_log
    """)).fetchone()
    runs_today      = int(etl_stats[0] or 0)
    success_rate_7d = round(float(etl_stats[1] or 0))
    avg_duration    = int(etl_stats[2] or 0)
    records_today   = int(etl_stats[3] or 0)

    # Is an ETL job currently running?
    etl_running = ETLJobLog.query.filter(
        ETLJobLog.end_time.is_(None),
        ETLJobLog.status == 'RUNNING'
    ).first() is not None

    # Demo mode settings
    demo_rows = db.session.execute(
        text("SELECT key, value FROM settings WHERE key IN ('demo_mode','demo_start','demo_end','demo_user')")
    ).fetchall()
    demo = {r[0]: r[1] for r in demo_rows}
    demo_mode  = demo.get('demo_mode', 'false').lower() == 'true'
    demo_start = demo.get('demo_start', '')
    demo_end   = demo.get('demo_end', '')
    demo_user  = demo.get('demo_user', '')

    # Build page permissions map: {user_id: {page_key: is_enabled}}
    all_perms = UserPagePermission.query.all()
    page_perms = {}
    for p in all_perms:
        page_perms.setdefault(p.user_id, {})[p.page_key] = p.is_enabled

    page_keys = ['live_feed', 'hl7_orders', 'report_ai', 'oru', 'mapping']

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
        runs_today     = runs_today,
        success_rate_7d= success_rate_7d,
        avg_duration   = avg_duration,
        records_today  = records_today,
        etl_running    = etl_running,
    )


# ── Scheduling module REMOVED at LAUMC (page + cancel/arrive/reschedule/suggest routes) ──


def _get_user_page_columns():
    return [
        ('live_feed',       'Live Department View'),
        ('hl7_orders',      'HL7 Orders'),
        # scheduling / patient_portal: modules removed at LAUMC
        ('cd_print',        'Patient CD Log'),
        ('oru',             'Report Intelligence'),
        ('referring_intel', 'Referring Intel'),
        ('custom_reports',  'Custom Reports'),
        ('mapping',         'Modality / Procedures'),
        ('report_ai',       'AI Reports'),
        ('financial',       'Revenue Intelligence'),
    ]


def _apply_role_default_permissions(user, role):
    from db import ALL_FEATURE_KEYS, ROLE_PAGE_DEFAULTS
    defaults = ROLE_PAGE_DEFAULTS.get(role, set())

    existing_perms = {p.page_key: p for p in UserPagePermission.query.filter_by(user_id=user.id).all()}

    for page_key in ALL_FEATURE_KEYS:
        desired = page_key in defaults
        perm = existing_perms.get(page_key)
        if perm:
            perm.is_enabled = desired
        else:
            db.session.add(UserPagePermission(user_id=user.id, page_key=page_key, is_enabled=desired))

    if role == 'viewer':
        from routes.viewer_controller import seed_report_access
        seed_report_access(user.id)


def _admin_audit(action, target_user_id, detail=None, category='user_mgmt'):
    try:
        db.session.add(UserAuditLog(
            actor_user_id=current_user.id,
            target_user_id=target_user_id,
            action=action,
            event_category=category,
            detail=detail,
            ip_address=request.remote_addr,
        ))
    except Exception:
        pass


@admin_bp.route('/users')
@login_required
def user_management():
    if current_user.role != 'admin':
        return abort(403)

    active_users  = User.query.filter(User.role != 'admin', User.status != 'pending') \
                              .order_by(User.role, User.username).all()
    pending_users = User.query.filter_by(status='pending').order_by(User.created_at.desc()).all()

    all_perms  = UserPagePermission.query.all()
    page_perms = {}
    for p in all_perms:
        page_perms.setdefault(p.user_id, {})[p.page_key] = p.is_enabled

    page_keys = _get_user_page_columns()

    # Active sessions keyed by user_id
    sessions_by_user = {}
    for s in active_sessions.query.all():
        sessions_by_user.setdefault(s.user_id, []).append(s)

    return render_template('user_management.html',
        users=active_users,
        pending_users=pending_users,
        page_perms=page_perms,
        page_keys=page_keys,
        sessions_by_user=sessions_by_user,
        ui_theme=current_user.ui_theme or 'dark',
    )


@admin_bp.route('/users/permissions', methods=['POST'])
@login_required
def update_user_permissions():
    if current_user.role != 'admin':
        return abort(403)

    from db import ALL_FEATURE_KEYS
    data     = request.get_json()
    user_id  = data.get('user_id')
    page_key = data.get('page_key')
    enabled  = bool(data.get('enabled'))

    if not user_id or not page_key:
        return jsonify({'status': 'error', 'message': 'Missing fields'}), 400

    if page_key not in ALL_FEATURE_KEYS:
        return jsonify({'status': 'error', 'message': 'Invalid page key'}), 400

    perm = UserPagePermission.query.filter_by(user_id=user_id, page_key=page_key).first()
    if perm:
        perm.is_enabled = enabled
    else:
        db.session.add(UserPagePermission(user_id=user_id, page_key=page_key, is_enabled=enabled))

    _admin_audit('perm_changed', user_id, {'page_key': page_key, 'enabled': enabled})
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/role', methods=['POST'])
@login_required
def update_user_role():
    if current_user.role != 'admin':
        return abort(403)

    data     = request.get_json()
    user_id  = data.get('user_id')
    new_role = data.get('role')

    if new_role not in ('viewer', 'viewer2', 'tec', 'finance', 'secretary'):
        return jsonify({'status': 'error', 'message': 'Invalid role'}), 400

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    old_role = user.role
    if old_role != new_role:
        user.role = new_role
        _apply_role_default_permissions(user, new_role)
        _admin_audit('role_changed', user.id, {'from': old_role, 'to': new_role})
        # Invalidate all existing sessions so the user re-logs in with the new role
        active_sessions.query.filter_by(user_id=user.id).delete()
        db.session.commit()

    return jsonify({'status': 'ok'})


@admin_bp.route('/users/approve', methods=['POST'])
@login_required
def approve_user():
    if current_user.role != 'admin':
        return abort(403)

    data     = request.get_json()
    user_id  = data.get('user_id')
    new_role = data.get('role', 'viewer')

    if new_role not in ('viewer', 'viewer2', 'tec', 'finance', 'secretary'):
        return jsonify({'status': 'error', 'message': 'Invalid role'}), 400

    user = User.query.get(user_id)
    if not user or user.status != 'pending':
        return jsonify({'status': 'error', 'message': 'User not found or not pending'}), 400

    user.status = 'active'
    user.role   = new_role
    _apply_role_default_permissions(user, new_role)
    _admin_audit('approved', user.id, {'role': new_role})
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/reject', methods=['POST'])
@login_required
def reject_user():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    user = User.query.get(user_id)
    if not user or user.status != 'pending':
        return jsonify({'status': 'error', 'message': 'User not found or not pending'}), 400

    _admin_audit('rejected', user.id)
    UserPagePermission.query.filter_by(user_id=user_id).delete()
    db.session.delete(user)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/disable', methods=['POST'])
@login_required
def disable_user():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    user.status = 'disabled'
    # Revoke all active sessions for this user
    active_sessions.query.filter_by(user_id=user_id).delete()
    _admin_audit('disabled', user.id)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/enable', methods=['POST'])
@login_required
def enable_user():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    user.status = 'active'
    _admin_audit('enabled', user.id)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/force-reset', methods=['POST'])
@login_required
def force_password_reset():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    user.must_change_password = True
    _admin_audit('password_reset_forced', user.id)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/set-password', methods=['POST'])
@login_required
def set_user_password():
    if current_user.role != 'admin':
        return abort(403)

    data       = request.get_json()
    user_id    = data.get('user_id')
    new_pw     = data.get('password', '')

    if len(new_pw) < 6:
        return jsonify({'status': 'error', 'message': 'Password must be at least 6 characters'}), 400

    user = User.query.get(user_id)
    if not user or user.role == 'admin':
        return jsonify({'status': 'error', 'message': 'User not found or protected'}), 400

    from werkzeug.security import generate_password_hash
    user.password_hash               = generate_password_hash(new_pw, method='pbkdf2:sha256')
    user.must_change_password        = True
    user.password_reset_requested    = False
    _admin_audit('password_set_by_admin', user.id)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/session/revoke', methods=['POST'])
@login_required
def revoke_session():
    if current_user.role != 'admin':
        return abort(403)

    data       = request.get_json()
    session_id = data.get('session_id')

    row = active_sessions.query.get(session_id)
    if not row:
        return jsonify({'status': 'error', 'message': 'Session not found'}), 404

    _admin_audit('session_revoked', row.user_id, {'session_id': session_id})
    db.session.delete(row)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/users/session/revoke-all', methods=['POST'])
@login_required
def revoke_all_sessions():
    if current_user.role != 'admin':
        return abort(403)

    data    = request.get_json()
    user_id = data.get('user_id')

    count = active_sessions.query.filter_by(user_id=user_id).delete()
    _admin_audit('all_sessions_revoked', user_id, {'count': count})
    db.session.commit()
    return jsonify({'status': 'ok', 'revoked': count})


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

    _admin_audit('deleted', user.id, {'username': user.username})
    active_sessions.query.filter_by(user_id=user_id).delete()
    UserPagePermission.query.filter_by(user_id=user_id).delete()
    ReportAccessControl.query.filter_by(user_id=user_id).delete()
    db.session.delete(user)
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/audit')
@login_required
def audit_log():
    if current_user.role != 'admin':
        return abort(403)

    category = request.args.get('category', '')
    actor_id = request.args.get('user_id', '', type=str)

    q = UserAuditLog.query.order_by(UserAuditLog.created_at.desc())
    if category:
        q = q.filter(UserAuditLog.event_category == category)
    if actor_id:
        q = q.filter(UserAuditLog.actor_user_id == int(actor_id))
    entries = q.limit(500).all()

    all_users = User.query.order_by(User.username).all()
    user_map = {u.id: u.username for u in all_users}

    categories = ['auth', 'user_mgmt', 'report', 'etl', 'ai', 'config']
    return render_template('admin_audit.html',
        entries=entries,
        user_map=user_map,
        all_users=all_users,
        categories=categories,
        active_category=category,
        active_user_id=actor_id,
    )


@admin_bp.route('/audit/export')
@login_required
def audit_log_export():
    import csv, io
    if current_user.role != 'admin':
        return abort(403)

    category = request.args.get('category', '')
    actor_id = request.args.get('user_id', '', type=str)

    q = UserAuditLog.query.order_by(UserAuditLog.created_at.desc())
    if category:
        q = q.filter(UserAuditLog.event_category == category)
    if actor_id:
        q = q.filter(UserAuditLog.actor_user_id == int(actor_id))
    entries = q.limit(5000).all()

    user_map = {u.id: u.username for u in User.query.all()}

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['time', 'category', 'action', 'user', 'target', 'ip', 'detail'])
    for e in entries:
        target = user_map.get(e.target_user_id, '') if e.target_user_id else (e.resource_type or '')
        writer.writerow([
            e.created_at.strftime('%Y-%m-%d %H:%M:%S') if e.created_at else '',
            e.event_category or '',
            e.action or '',
            user_map.get(e.actor_user_id, ''),
            target,
            e.ip_address or '',
            e.detail if e.detail else '',
        ])

    from flask import Response
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=audit_log.csv'},
    )


@admin_bp.route('/oracle-config', endpoint='oracle_config')
@login_required
def oracle_config():
    return redirect(url_for('db_manager.db_manager_page'))


@admin_bp.route('/hl7-forward', methods=['GET', 'POST'], endpoint='hl7_forward_config')
@login_required
def hl7_forward_config():
    if current_user.role != 'admin':
        return abort(403)

    from utils.hl7_forward import test_forward, invalidate_cache

    msg = None

    if request.method == 'POST':
        action = request.form.get('action', '')

        if action == 'save':
            host    = (request.form.get('host', '') or '').strip()
            port    = (request.form.get('port', '') or '').strip()
            enabled = '1' if request.form.get('enabled') else '0'
            for key, val in [
                ('hl7_forward_host',    host),
                ('hl7_forward_port',    port),
                ('hl7_forward_enabled', enabled),
            ]:
                exists = db.session.execute(text("SELECT 1 FROM settings WHERE key=:k"), {'k': key}).fetchone()
                if exists:
                    db.session.execute(text("UPDATE settings SET value=:v WHERE key=:k"), {'k': key, 'v': val})
                else:
                    db.session.execute(text("INSERT INTO settings (key,value) VALUES (:k,:v)"), {'k': key, 'v': val})
            db.session.commit()
            invalidate_cache()
            msg = ('success', 'Settings saved.')

        elif action == 'test':
            host     = (request.form.get('host', '') or '').strip()
            port_str = (request.form.get('port', '') or '').strip()
            sample   = db.session.execute(text("""
                SELECT raw_message FROM hl7_orders
                WHERE raw_message IS NOT NULL
                ORDER BY received_at DESC LIMIT 1
            """)).scalar()
            if not host or not port_str:
                msg = ('error', 'Enter host and port before testing.')
            else:
                ok, detail = test_forward(host, port_str, sample)
                msg = ('success' if ok else 'error', detail)

    rows = db.session.execute(text("""
        SELECT key, value FROM settings
        WHERE key IN ('hl7_forward_host','hl7_forward_port','hl7_forward_enabled')
    """)).fetchall()
    cfg = {r[0]: r[1] for r in rows}

    sample_preview = db.session.execute(text("""
        SELECT raw_message, received_at FROM hl7_orders
        WHERE raw_message IS NOT NULL
        ORDER BY received_at DESC LIMIT 1
    """)).fetchone()

    return render_template(
        'admin_hl7_forward.html',
        cfg            = cfg,
        msg            = msg,
        sample_preview = sample_preview,
    )


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
    data           = request.get_json()
    enabled        = 'true' if data.get('enabled') else 'false'
    start          = data.get('start', '')
    end            = data.get('end', '')
    demo_username  = (data.get('demo_user') or '').strip()
    demo_password  = (data.get('demo_password') or '').strip()

    for key, val in [('demo_mode', enabled), ('demo_start', start), ('demo_end', end), ('demo_user', demo_username)]:
        exists = db.session.execute(text("SELECT 1 FROM settings WHERE key = :k"), {'k': key}).fetchone()
        if exists:
            db.session.execute(text("UPDATE settings SET value = :v WHERE key = :k"), {'k': key, 'v': val})
        else:
            db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v)"), {'k': key, 'v': val})

    # If a password was provided, update (or create) the demo user account
    if demo_username and demo_password:
        from werkzeug.security import generate_password_hash
        from db import UserPagePermission
        user = User.query.filter_by(username=demo_username).first()
        if user:
            user.password_hash = generate_password_hash(demo_password, method='pbkdf2:sha256')
        else:
            # Create the demo user as a viewer if they don't exist yet
            user = User(
                username=demo_username,
                password_hash=generate_password_hash(demo_password, method='pbkdf2:sha256'),
                role='viewer'
            )
            db.session.add(user)
            db.session.flush()
            # Grant all page permissions to the new demo account
            for page_key in ['live_feed', 'hl7_orders', 'report_ai', 'oru', 'mapping']:
                db.session.add(UserPagePermission(user_id=user.id, page_key=page_key, is_enabled=True))
        # (patient_portal strip removed — module absent at LAUMC)

    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/etl/trigger-phase9', methods=['POST'])
@login_required
def trigger_phase9():
    if current_user.role != 'admin':
        return abort(403)
    try:
        import logging, io
        from ETL_JOBS.etl_phase9_clustering import run_phase9_clustering

        # Capture log output so we can return it
        log_buf = io.StringIO()
        handler = logging.StreamHandler(log_buf)
        handler.setLevel(logging.DEBUG)
        logger = logging.getLogger('phase9_manual')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        with db.engine.connect() as conn:
            run_phase9_clustering(conn, logger)
            conn.commit()

        logger.removeHandler(handler)
        return jsonify({"status": "success", "log": log_buf.getvalue()})
    except Exception as e:
        import traceback
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@admin_bp.route('/etl/trigger', methods=['POST'])
@login_required
def trigger_etl():
    if current_user.role != 'admin':
        return abort(403)

    # Block ETL when demo mode is active
    demo_row = db.session.execute(
        text("SELECT value FROM settings WHERE key = 'demo_mode'")
    ).fetchone()
    if demo_row and demo_row[0].lower() == 'true':
        return jsonify({"status": "error", "message": "ETL is locked during demo mode."}), 403

    try:
        from flask import current_app
        from ETL_JOBS.etl_runner import execute_sync
        import threading

        app = current_app._get_current_object()

        def _run():
            with app.app_context():
                execute_sync(app)

        threading.Thread(target=_run, daemon=True).start()
        _admin_audit('etl_triggered', current_user.id, category='etl')
        db.session.commit()
        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
