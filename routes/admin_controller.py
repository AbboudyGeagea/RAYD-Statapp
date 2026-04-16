from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, ReportAccessControl, UserPagePermission, SchedulingEntry, db
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

    page_keys = ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru', 'patient_portal']

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


@admin_bp.route('/scheduling', endpoint='scheduling_page', methods=['GET', 'POST'])
@login_required
def scheduling_page():
    if current_user.role != 'admin':
        flash("Admin access required.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # Ensure the scheduling table exists before using it.
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS scheduling_entries (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                middle_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                date_of_birth DATE NOT NULL,
                referring_physician TEXT NOT NULL,
                patient_class TEXT NOT NULL,
                procedures JSONB NOT NULL DEFAULT '[]',
                third_party_approvals JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
            );
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    schedule_id = request.args.get('schedule_id', type=int)
    schedule = SchedulingEntry.query.get(schedule_id) if schedule_id else None

    if request.method == 'POST':
        form_schedule_id = request.form.get('schedule_id', type=int)
        first_name = (request.form.get('first_name') or '').strip()
        middle_name = (request.form.get('middle_name') or '').strip()
        last_name = (request.form.get('last_name') or '').strip()
        date_of_birth_raw = (request.form.get('date_of_birth') or '').strip()
        referring_physician = (request.form.get('referring_physician') or '').strip()
        patient_class = (request.form.get('patient_class') or '').strip().upper()
        procedures = [p.strip() for p in request.form.getlist('procedure_name') if p.strip()]
        third_party_approvals = [p.strip() for p in request.form.getlist('third_party_approval') if p.strip()]

        if not (first_name and middle_name and last_name and date_of_birth_raw and referring_physician and patient_class and procedures and third_party_approvals):
            flash("Please complete all required scheduling fields.", "danger")
        else:
            try:
                date_of_birth = datetime.strptime(date_of_birth_raw, '%Y-%m-%d').date()
            except ValueError:
                flash("Please enter a valid date of birth.", "danger")
                date_of_birth = None

            if date_of_birth:
                if form_schedule_id:
                    schedule = SchedulingEntry.query.get(form_schedule_id)
                    if schedule:
                        schedule.first_name = first_name
                        schedule.middle_name = middle_name
                        schedule.last_name = last_name
                        schedule.date_of_birth = date_of_birth
                        schedule.referring_physician = referring_physician
                        schedule.patient_class = patient_class
                        schedule.procedures = procedures
                        schedule.third_party_approvals = third_party_approvals
                        schedule.updated_at = datetime.utcnow()
                        db.session.commit()
                        flash("Scheduling entry updated.", "success")
                        return redirect(url_for('admin.scheduling_page', schedule_id=schedule.id))
                new_entry = SchedulingEntry(
                    first_name=first_name,
                    middle_name=middle_name,
                    last_name=last_name,
                    date_of_birth=date_of_birth,
                    referring_physician=referring_physician,
                    patient_class=patient_class,
                    procedures=procedures,
                    third_party_approvals=third_party_approvals,
                )
                db.session.add(new_entry)
                db.session.commit()
                flash("Scheduling entry saved.", "success")
                return redirect(url_for('admin.scheduling_page', schedule_id=new_entry.id))

    schedules = SchedulingEntry.query.order_by(SchedulingEntry.updated_at.desc()).all()

    return render_template('admin_scheduling.html', schedule=schedule, schedules=schedules)


def _get_user_page_columns():
    from flask import current_app
    page_columns = [
        ('live_feed', 'Live AE Status'),
        ('hl7_orders', 'HL7 Orders'),
        ('report_ai', 'AI Reports'),
        ('bitnet', 'AI Assistant'),
        ('oru', 'Report Intelligence'),
    ]
    if current_app.config.get('PATIENT_PORTAL_ENABLED', False):
        page_columns.append(('patient_portal', 'Patient Portal'))
    return page_columns


def _apply_role_default_permissions(user, role):
    default_map = {
        'viewer': {'report_ai', 'oru'},
        'tec': {'hl7_orders'},
    }
    allowed_keys = {'live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru', 'patient_portal'}
    defaults = default_map.get(role, set())

    existing_perms = {p.page_key: p for p in UserPagePermission.query.filter_by(user_id=user.id).all()}

    for page_key in allowed_keys:
        desired = page_key in defaults
        perm = existing_perms.get(page_key)
        if perm:
            perm.is_enabled = desired
        elif desired:
            db.session.add(UserPagePermission(user_id=user.id, page_key=page_key, is_enabled=True))

    # keep any extra, unsupported page permissions untouched


@admin_bp.route('/users')
@login_required
def user_management():
    if current_user.role != 'admin':
        return abort(403)

    users = User.query.filter(User.role != 'admin').order_by(User.role, User.username).all()
    all_perms = UserPagePermission.query.all()
    page_perms = {}
    for p in all_perms:
        page_perms.setdefault(p.user_id, {})[p.page_key] = p.is_enabled

    page_keys = _get_user_page_columns()

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

    if user.role != new_role:
        user.role = new_role
        _apply_role_default_permissions(user, new_role)
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
            # Grant all page permissions to the new demo account — patient_portal is always excluded
            for page_key in ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru']:
                db.session.add(UserPagePermission(user_id=user.id, page_key=page_key, is_enabled=True))

        # Always strip patient_portal from the demo user, even if previously granted manually
        db.session.flush()
        db.session.execute(
            text("DELETE FROM user_page_permissions WHERE user_id = :uid AND page_key = 'patient_portal'"),
            {"uid": user.id}
        )

    db.session.commit()
    return jsonify({'status': 'ok'})


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

        def _run():
            with current_app.app_context():
                execute_sync(current_app._get_current_object())

        threading.Thread(target=_run, daemon=True).start()
        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
