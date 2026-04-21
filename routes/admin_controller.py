from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request, abort
from flask_login import login_required, current_user
from db import User, ReportTemplate, ETLJobLog, ReportAccessControl, UserPagePermission, SchedulingEntry, db
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

    page_keys = ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru', 'mapping', 'patient_portal']

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
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'scheduling'):
        flash("Access denied.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # Ensure table exists
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS scheduling_entries (
                id                    SERIAL PRIMARY KEY,
                first_name            TEXT NOT NULL,
                middle_name           TEXT NOT NULL,
                last_name             TEXT NOT NULL,
                date_of_birth         DATE NOT NULL,
                referring_physician   TEXT NOT NULL,
                patient_class         TEXT NOT NULL,
                procedure_datetime    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                modality_type         VARCHAR(50) NOT NULL,
                procedures            JSONB NOT NULL DEFAULT '[]',
                third_party_approvals JSONB NOT NULL DEFAULT '[]',
                created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                updated_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # ── Date navigation ───────────────────────────────────────────────────────
    view_date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    try:
        view_date = datetime.strptime(view_date_str, '%Y-%m-%d').date()
    except ValueError:
        view_date = datetime.now().date()
    prev_date = (view_date - timedelta(days=1)).strftime('%Y-%m-%d')
    next_date = (view_date + timedelta(days=1)).strftime('%Y-%m-%d')
    is_today  = (view_date == datetime.now().date())

    # ── Handle form POST ──────────────────────────────────────────────────────
    schedule_id = request.args.get('schedule_id', type=int)
    schedule = db.session.get(SchedulingEntry, schedule_id) if schedule_id else None

    if request.method == 'POST':
        form_schedule_id      = request.form.get('schedule_id', type=int)
        first_name            = (request.form.get('first_name')            or '').strip()
        middle_name           = (request.form.get('middle_name')           or '').strip()
        last_name             = (request.form.get('last_name')             or '').strip()
        date_of_birth_raw     = (request.form.get('date_of_birth')         or '').strip()
        referring_physician   = (request.form.get('referring_physician')   or '').strip()
        patient_class         = (request.form.get('patient_class')         or '').strip()
        procedure_datetime_raw = request.form.get('procedure_datetime', '').strip()
        modality_type         = (request.form.get('modality_type')         or '').strip()
        procedures            = [p.strip() for p in request.form.getlist('procedure_name')       if p.strip()]
        third_party_approvals = [p.strip() for p in request.form.getlist('third_party_approval') if p.strip()]

        if not all([first_name, middle_name, last_name, date_of_birth_raw,
                    referring_physician, patient_class, procedure_datetime_raw,
                    modality_type, procedures, third_party_approvals]):
            flash("Please complete all required scheduling fields.", "danger")
        else:
            try:
                date_of_birth      = datetime.strptime(date_of_birth_raw,      '%Y-%m-%d').date()
                procedure_datetime = datetime.strptime(procedure_datetime_raw,  '%Y-%m-%dT%H:%M')
            except ValueError:
                flash("Please enter valid date and datetime values.", "danger")
                date_of_birth = procedure_datetime = None

            if date_of_birth and procedure_datetime:
                if form_schedule_id:
                    entry = db.session.get(SchedulingEntry, form_schedule_id)
                    if entry:
                        entry.first_name = first_name; entry.middle_name = middle_name
                        entry.last_name = last_name;   entry.date_of_birth = date_of_birth
                        entry.referring_physician = referring_physician
                        entry.patient_class = patient_class
                        entry.procedure_datetime = procedure_datetime
                        entry.modality_type = modality_type
                        entry.procedures = procedures
                        entry.third_party_approvals = third_party_approvals
                        entry.updated_at = datetime.utcnow()
                        db.session.commit()
                        flash("Scheduling entry updated.", "success")
                        return redirect(url_for('admin.scheduling_page',
                                                schedule_id=entry.id, date=procedure_datetime.strftime('%Y-%m-%d')))
                else:
                    new_entry = SchedulingEntry(
                        first_name=first_name, middle_name=middle_name, last_name=last_name,
                        date_of_birth=date_of_birth, referring_physician=referring_physician,
                        patient_class=patient_class, procedure_datetime=procedure_datetime,
                        modality_type=modality_type, procedures=procedures,
                        third_party_approvals=third_party_approvals,
                    )
                    db.session.add(new_entry)
                    db.session.commit()
                    flash("Scheduling entry saved.", "success")
                    return redirect(url_for('admin.scheduling_page',
                                            schedule_id=new_entry.id, date=procedure_datetime.strftime('%Y-%m-%d')))

    # ── Build day grid ────────────────────────────────────────────────────────
    # Collect appointments from both sources for view_date
    appointments = []

    # Source 1: manually entered scheduling entries
    day_start = datetime.combine(view_date, datetime.min.time())
    day_end   = datetime.combine(view_date, datetime.max.time())
    manual_entries = SchedulingEntry.query.filter(
        SchedulingEntry.procedure_datetime >= day_start,
        SchedulingEntry.procedure_datetime <= day_end
    ).order_by(SchedulingEntry.procedure_datetime).all()

    for e in manual_entries:
        appointments.append({
            'source':    'manual',
            'id':        e.id,
            'time':      e.procedure_datetime.strftime('%H:%M'),
            'hour_slot': e.procedure_datetime.hour * 60 + (30 if e.procedure_datetime.minute >= 30 else 0),
            'modality':  e.modality_type or 'OT',
            'name':      f"{e.first_name} {e.last_name}",
            'procedure': ', '.join(e.procedures) if e.procedures else '',
            'class':     e.patient_class,
            'ref':       e.referring_physician,
        })

    # Source 2: HL7 orders from HIS (scheduled for this day, all statuses)
    unscheduled = []
    try:
        hl7_rows = db.session.execute(text("""
            SELECT id, patient_name, patient_id, procedure_code, procedure_text,
                   modality, scheduled_datetime, ordering_physician, order_status, accession_number
            FROM hl7_orders
            WHERE DATE(scheduled_datetime) = :d
            ORDER BY scheduled_datetime
        """), {"d": view_date}).fetchall()

        for r in hl7_rows:
            if not r.scheduled_datetime:
                continue
            dt = r.scheduled_datetime
            status = (r.order_status or '').upper()
            appointments.append({
                'source':    'hl7',
                'id':        r.id,
                'time':      dt.strftime('%H:%M'),
                'hour_slot': dt.hour * 60 + (30 if dt.minute >= 30 else 0),
                'modality':  (r.modality or 'OT').strip().upper(),
                'name':      r.patient_name or f"ID: {r.patient_id or '—'}",
                'procedure': (r.procedure_text or r.procedure_code or '—')[:60],
                'class':     'OP',
                'ref':       r.ordering_physician or '',
                'status':    status,
                'accession': r.accession_number or '',
                'cancelled': status in ('CA', 'DC'),
            })

        # Unscheduled HL7 orders (no scheduled_datetime) — worklist
        unscheduled_rows = db.session.execute(text("""
            SELECT id, patient_name, patient_id, procedure_code, procedure_text,
                   modality, ordering_physician, order_status, accession_number, received_at
            FROM hl7_orders
            WHERE scheduled_datetime IS NULL
              AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
            ORDER BY received_at DESC
            LIMIT 100
        """)).fetchall()

        unscheduled = [
            {
                'id':        r.id,
                'name':      r.patient_name or f"ID: {r.patient_id or '—'}",
                'procedure': (r.procedure_text or r.procedure_code or '—')[:60],
                'modality':  (r.modality or 'OT').strip().upper(),
                'physician': r.ordering_physician or '',
                'status':    (r.order_status or ''),
                'accession': r.accession_number or '',
                'received':  r.received_at.strftime('%H:%M') if r.received_at else '',
            }
            for r in unscheduled_rows
        ]
    except Exception:
        pass

    # Build grid: time_slot (minutes) → modality → [appointments]
    from collections import defaultdict
    grid_raw = defaultdict(lambda: defaultdict(list))
    modalities_set = []
    for appt in appointments:
        slot = appt['hour_slot']
        mod  = appt['modality']
        grid_raw[slot][mod].append(appt)
        if mod not in modalities_set:
            modalities_set.append(mod)
    modalities_set.sort()

    # Time slots 00:00 – 23:30 (full 24 h)
    time_slots = []
    for h in range(0, 24):
        for m in (0, 30):
            minutes = h * 60 + m
            label   = f"{h:02d}:{m:02d}"
            row     = {mod: grid_raw[minutes].get(mod, []) for mod in modalities_set}
            has_any = any(row.values())
            time_slots.append({'minutes': minutes, 'label': label, 'row': row, 'has_any': has_any})

    # Summary counts
    from collections import Counter
    mod_counts = Counter(a['modality'] for a in appointments)
    class_counts = Counter(a['class'] for a in appointments)

    # All saved entries for "All Bookings" tab
    schedules = SchedulingEntry.query.order_by(
        SchedulingEntry.updated_at.desc().nullslast(),
        SchedulingEntry.id.desc()
    ).all()

    return render_template('admin_scheduling.html',
        schedule=schedule,
        schedules=schedules,
        view_date=view_date,
        view_date_str=view_date_str,
        prev_date=prev_date,
        next_date=next_date,
        is_today=is_today,
        time_slots=time_slots,
        modalities=modalities_set,
        appointments=appointments,
        unscheduled=unscheduled,
        mod_counts=mod_counts,
        class_counts=class_counts,
        total_count=len(appointments),
    )


@admin_bp.route('/scheduling/hl7/<int:hl7_id>/reschedule', methods=['POST'])
@login_required
def reschedule_hl7(hl7_id):
    if current_user.role != 'admin':
        return abort(403)
    data = request.get_json()
    new_dt_str = (data or {}).get('scheduled_datetime', '')
    try:
        new_dt = datetime.strptime(new_dt_str, '%Y-%m-%dT%H:%M')
    except (ValueError, TypeError):
        return jsonify({'status': 'error', 'message': 'Invalid datetime'}), 400
    db.session.execute(
        text("UPDATE hl7_orders SET scheduled_datetime = :dt WHERE id = :id"),
        {'dt': new_dt, 'id': hl7_id}
    )
    db.session.commit()
    return jsonify({'status': 'ok', 'scheduled_datetime': new_dt.strftime('%Y-%m-%dT%H:%M')})


@admin_bp.route('/scheduling/suggest', methods=['GET'])
@login_required
def suggest_hl7_slot():
    if current_user.role != 'admin':
        return abort(403)
    modality   = request.args.get('modality', '').strip().upper()
    date_str   = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    exclude_id = request.args.get('exclude_id', type=int) or -1
    try:
        view_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'status': 'error', 'message': 'Invalid date'}), 400

    day_start = datetime.combine(view_date, datetime.min.time())
    day_end   = datetime.combine(view_date, datetime.max.time())
    slot_counts = {}

    manual_rows = db.session.execute(text("""
        SELECT procedure_datetime FROM scheduling_entries
        WHERE modality_type = :mod AND procedure_datetime BETWEEN :s AND :e
    """), {'mod': modality, 's': day_start, 'e': day_end}).fetchall()
    for r in manual_rows:
        slot = r.procedure_datetime.hour * 60 + (30 if r.procedure_datetime.minute >= 30 else 0)
        slot_counts[slot] = slot_counts.get(slot, 0) + 1

    hl7_rows = db.session.execute(text("""
        SELECT scheduled_datetime FROM hl7_orders
        WHERE UPPER(modality) = :mod
          AND scheduled_datetime BETWEEN :s AND :e
          AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
          AND id != :excl
    """), {'mod': modality, 's': day_start, 'e': day_end, 'excl': exclude_id}).fetchall()
    for r in hl7_rows:
        if r.scheduled_datetime:
            slot = r.scheduled_datetime.hour * 60 + (30 if r.scheduled_datetime.minute >= 30 else 0)
            slot_counts[slot] = slot_counts.get(slot, 0) + 1

    best_slot, best_count = None, float('inf')
    for h in range(7, 20):
        for m in (0, 30):
            minutes = h * 60 + m
            cnt = slot_counts.get(minutes, 0)
            if cnt < best_count:
                best_count, best_slot = cnt, minutes

    if best_slot is None:
        return jsonify({'status': 'error', 'message': 'No slots found'}), 404

    bh, bm = best_slot // 60, best_slot % 60
    suggested = datetime.combine(view_date, datetime.min.time()).replace(hour=bh, minute=bm)
    return jsonify({
        'status':    'ok',
        'suggested': suggested.strftime('%Y-%m-%dT%H:%M'),
        'label':     suggested.strftime('%H:%M'),
        'load':      best_count,
    })


def _get_user_page_columns():
    return [
        ('live_feed',      'Live AE Status'),
        ('hl7_orders',     'HL7 Orders'),
        ('report_ai',      'AI Reports'),
        ('bitnet',         'AI Assistant'),
        ('oru',            'Report Intelligence'),
        ('mapping',        'Modality Mapping'),
        ('patient_portal', 'Patient Portal'),
        ('scheduling',     'Scheduling'),
    ]


def _apply_role_default_permissions(user, role):
    from db import ALL_FEATURE_KEYS
    defaults = {
        'viewer': set(ALL_FEATURE_KEYS),
        'tec':    {'hl7_orders', 'live_feed'},
    }.get(role, set())

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


@admin_bp.route('/oracle-config', endpoint='oracle_config')
@login_required
def oracle_config():
    return redirect(url_for('db_manager.db_manager_page'))


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
            for page_key in ['live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru', 'mapping']:
                db.session.add(UserPagePermission(user_id=user.id, page_key=page_key, is_enabled=True))

        # Always strip patient_portal from the demo user, even if previously granted manually
        db.session.flush()
        db.session.execute(
            text("DELETE FROM user_page_permissions WHERE user_id = :uid AND page_key = 'patient_portal'"),
            {"uid": user.id}
        )

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
        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
