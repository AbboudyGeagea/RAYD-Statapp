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
        runs_today     = runs_today,
        success_rate_7d= success_rate_7d,
        avg_duration   = avg_duration,
        records_today  = records_today,
        etl_running    = etl_running,
    )


@admin_bp.route('/scheduling', endpoint='scheduling_page', methods=['GET', 'POST'])
@login_required
def scheduling_page():
    from db import user_has_page
    from collections import defaultdict, Counter
    if current_user.role != 'admin' and not user_has_page(current_user, 'scheduling'):
        flash("Access denied.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # ── Date navigation ───────────────────────────────────────────────────────
    view_mode     = request.args.get('view', 'day')  # 'day' or 'week'
    view_date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    try:
        view_date = datetime.strptime(view_date_str, '%Y-%m-%d').date()
    except ValueError:
        view_date = datetime.now().date()
    prev_date = (view_date - timedelta(days=1)).strftime('%Y-%m-%d')
    next_date = (view_date + timedelta(days=1)).strftime('%Y-%m-%d')
    is_today  = (view_date == datetime.now().date())

    # week boundaries (Mon–Sun of current week)
    week_start = view_date - timedelta(days=view_date.weekday())
    week_end   = week_start + timedelta(days=6)
    prev_week  = (week_start - timedelta(days=7)).strftime('%Y-%m-%d')
    next_week  = (week_start + timedelta(days=7)).strftime('%Y-%m-%d')

    # 7-day procedure count strip
    strip_start = view_date - timedelta(days=3)
    strip_end   = view_date + timedelta(days=3)
    try:
        count_rows = db.session.execute(text("""
            SELECT DATE(scheduled_datetime) AS d, COUNT(*) AS cnt
            FROM hl7_orders
            WHERE DATE(scheduled_datetime) BETWEEN :s AND :e
              AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
              AND (message_type IS NULL OR message_type NOT LIKE 'ADT%')
            GROUP BY 1
        """), {"s": strip_start, "e": strip_end}).fetchall()
        manual_count_rows = db.session.execute(text("""
            SELECT DATE(procedure_datetime) AS d, COUNT(*) AS cnt
            FROM scheduling_entries
            WHERE DATE(procedure_datetime) BETWEEN :s AND :e
              AND (cancelled IS NULL OR cancelled = false)
            GROUP BY 1
        """), {"s": strip_start, "e": strip_end}).fetchall()
        date_counts: dict = {}
        for r in count_rows:
            date_counts[r.d] = date_counts.get(r.d, 0) + r.cnt
        for r in manual_count_rows:
            date_counts[r.d] = date_counts.get(r.d, 0) + r.cnt
    except Exception:
        date_counts = {}
    strip_dates = [strip_start + timedelta(days=i) for i in range(7)]

    # ── Device list (for columns + form dropdown) ─────────────────────────────
    device_rows = []
    mod_to_aetitles: dict = {}
    device_map: dict = {}   # aetitle → {label, modality, capacity_slots}
    aetitle_list = []       # ordered list for form dropdown
    try:
        device_rows = db.session.execute(text("""
            SELECT m.aetitle,
                   m.modality,
                   COALESCE(m.description, m.aetitle) AS label,
                   COALESCE(dws.std_opening_minutes, m.daily_capacity_minutes, 480) AS capacity_minutes
            FROM aetitle_modality_map m
            LEFT JOIN device_weekly_schedule dws
                ON dws.aetitle = m.aetitle AND dws.day_of_week = :dow
            ORDER BY m.modality, m.aetitle
        """), {"dow": view_date.weekday()}).fetchall()
        for r in device_rows:
            device_map[r.aetitle] = {
                'aetitle':        r.aetitle,
                'modality':       r.modality,
                'label':          r.label,
                'capacity_slots': max(1, (r.capacity_minutes or 480) // 30),
            }
            mod_to_aetitles.setdefault(r.modality, []).append(r.aetitle)
            aetitle_list.append({'aetitle': r.aetitle, 'label': r.label, 'modality': r.modality})
    except Exception:
        pass

    # ── Handle form POST ──────────────────────────────────────────────────────
    schedule_id = request.args.get('schedule_id', type=int)
    schedule = db.session.get(SchedulingEntry, schedule_id) if schedule_id else None

    if request.method == 'POST':
        form_schedule_id       = request.form.get('schedule_id', type=int)
        first_name             = (request.form.get('first_name')            or '').strip()
        middle_name            = (request.form.get('middle_name')           or '').strip()
        last_name              = (request.form.get('last_name')             or '').strip()
        date_of_birth_raw      = (request.form.get('date_of_birth')         or '').strip()
        referring_physician    = (request.form.get('referring_physician')   or '').strip()
        patient_class          = (request.form.get('patient_class')         or '').strip()
        procedure_datetime_raw = request.form.get('procedure_datetime', '').strip()
        modality_type          = (request.form.get('modality_type')         or '').strip()
        aetitle_val            = (request.form.get('aetitle')               or '').strip() or None
        procedures             = [p.strip() for p in request.form.getlist('procedure_name')       if p.strip()]
        third_party_approvals  = [p.strip() for p in request.form.getlist('third_party_approval') if p.strip()]

        if not all([first_name, middle_name, last_name, date_of_birth_raw,
                    referring_physician, patient_class, procedure_datetime_raw, modality_type]):
            flash("Please complete all required scheduling fields.", "danger")
        else:
            try:
                date_of_birth      = datetime.strptime(date_of_birth_raw,     '%Y-%m-%d').date()
                procedure_datetime = datetime.strptime(procedure_datetime_raw, '%Y-%m-%dT%H:%M')
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
                        entry.aetitle = aetitle_val
                        entry.procedures = procedures or []
                        entry.third_party_approvals = third_party_approvals or []
                        entry.updated_at = datetime.utcnow()
                        db.session.commit()
                        return redirect(url_for('admin.scheduling_page',
                                                schedule_id=entry.id,
                                                date=procedure_datetime.strftime('%Y-%m-%d')))
                else:
                    new_entry = SchedulingEntry(
                        first_name=first_name, middle_name=middle_name, last_name=last_name,
                        date_of_birth=date_of_birth, referring_physician=referring_physician,
                        patient_class=patient_class, procedure_datetime=procedure_datetime,
                        modality_type=modality_type, aetitle=aetitle_val,
                        procedures=procedures or [], third_party_approvals=third_party_approvals or [],
                    )
                    db.session.add(new_entry)
                    db.session.commit()
                    return redirect(url_for('admin.scheduling_page',
                                            schedule_id=new_entry.id,
                                            date=procedure_datetime.strftime('%Y-%m-%d')))

    # ── Collect appointments for day view ─────────────────────────────────────
    appointments = []
    day_start = datetime.combine(view_date, datetime.min.time())
    day_end   = datetime.combine(view_date, datetime.max.time())

    manual_entries = SchedulingEntry.query.filter(
        SchedulingEntry.procedure_datetime >= day_start,
        SchedulingEntry.procedure_datetime <= day_end,
    ).order_by(SchedulingEntry.procedure_datetime).all()

    for e in manual_entries:
        appointments.append({
            'source':    'manual',
            'id':        e.id,
            'time':      e.procedure_datetime.strftime('%H:%M'),
            'hour_slot': e.procedure_datetime.hour * 60 + (30 if e.procedure_datetime.minute >= 30 else 0),
            'modality':  e.modality_type or 'OT',
            'aetitle':   e.aetitle or '',
            'name':      f"{e.first_name} {e.last_name}",
            'procedure': ', '.join(e.procedures) if e.procedures else '',
            'class':     e.patient_class,
            'ref':       e.referring_physician,
            'cancelled': bool(e.cancelled),
        })

    unscheduled = []
    try:
        hl7_rows = db.session.execute(text("""
            SELECT id, patient_name, patient_id, procedure_code, procedure_text,
                   modality, scheduled_datetime, ordering_physician, order_status,
                   accession_number,
                   COALESCE(target_aetitle, '') AS target_aetitle
            FROM hl7_orders
            WHERE DATE(scheduled_datetime) = :d
              AND (message_type IS NULL OR message_type NOT LIKE 'ADT%')
            ORDER BY scheduled_datetime
        """), {"d": view_date}).fetchall()

        for r in hl7_rows:
            if not r.scheduled_datetime:
                continue
            dt     = r.scheduled_datetime
            status = (r.order_status or '').upper()
            appointments.append({
                'source':    'hl7',
                'id':        r.id,
                'time':      dt.strftime('%H:%M'),
                'hour_slot': dt.hour * 60 + (30 if dt.minute >= 30 else 0),
                'modality':  (r.modality or 'OT').strip().upper(),
                'aetitle':   r.target_aetitle or '',
                'name':      r.patient_name or f"ID: {r.patient_id or '—'}",
                'procedure': (r.procedure_text or r.procedure_code or '—')[:60],
                'class':     'OP',
                'ref':       r.ordering_physician or '',
                'status':    status,
                'accession': r.accession_number or '',
                'cancelled': status in ('CA', 'DC'),
            })

        unscheduled_rows = db.session.execute(text("""
            SELECT id, patient_name, patient_id, procedure_code, procedure_text,
                   modality, ordering_physician, order_status, accession_number, received_at
            FROM hl7_orders
            WHERE scheduled_datetime IS NULL
              AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
              AND (message_type IS NULL OR message_type NOT LIKE 'ADT%')
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
                'received':  r.received_at.strftime('%d/%m %H:%M') if r.received_at else '',
            }
            for r in unscheduled_rows
        ]
    except Exception:
        pass

    # ── Build per-device columns ──────────────────────────────────────────────
    # col_key: aetitle string if device assigned, else "{MODALITY}-any" pool
    grid_raw: dict = defaultdict(lambda: defaultdict(list))
    col_keys_seen: list = []

    def _col_key_for(appt: dict) -> str:
        ae = appt.get('aetitle', '').strip()
        if ae and ae in device_map:
            return ae
        mod = appt['modality']
        ae_list = mod_to_aetitles.get(mod, [])
        if len(ae_list) == 1:
            return ae_list[0]
        return f"_pool_{mod}"

    for appt in appointments:
        ck = _col_key_for(appt)
        appt['col_key'] = ck
        grid_raw[appt['hour_slot']][ck].append(appt)
        if ck not in col_keys_seen:
            col_keys_seen.append(ck)

    # Build ordered columns: first all AE titles (from device_map, sorted), then any pool cols
    ordered_cols = [ae for ae in device_map if ae in col_keys_seen]
    for ck in col_keys_seen:
        if ck not in ordered_cols:
            ordered_cols.append(ck)

    def _col_meta(ck: str) -> dict:
        if ck in device_map:
            d = device_map[ck]
            booked = sum(len(grid_raw[s].get(ck, [])) for s in grid_raw)
            return {
                'key':            ck,
                'label':          d['label'],
                'modality':       d['modality'],
                'capacity_slots': d['capacity_slots'],
                'booked':         booked,
                'is_pool':        False,
            }
        mod = ck.replace('_pool_', '')
        booked = sum(len(grid_raw[s].get(ck, [])) for s in grid_raw)
        return {
            'key':            ck,
            'label':          mod,
            'modality':       mod,
            'capacity_slots': 16,
            'booked':         booked,
            'is_pool':        True,
        }

    columns = [_col_meta(ck) for ck in ordered_cols]

    # Time slots: 07:00–20:00 default (show_all toggle expands to 00:00–23:30)
    time_slots = []
    for h in range(0, 24):
        for m in (0, 30):
            minutes = h * 60 + m
            label   = f"{h:02d}:{m:02d}"
            row     = {ck: grid_raw[minutes].get(ck, []) for ck in ordered_cols}
            has_any = any(row.values())
            in_work_hours = 7 * 60 <= minutes <= 20 * 60
            time_slots.append({
                'minutes':       minutes,
                'label':         label,
                'row':           row,
                'has_any':       has_any,
                'in_work_hours': in_work_hours,
            })

    mod_counts   = Counter(a['modality']  for a in appointments)
    class_counts = Counter(a['class']     for a in appointments)

    # ── Week view data ────────────────────────────────────────────────────────
    week_data = {}  # col_key → {weekday_int: count}
    week_totals = {}  # weekday_int → count
    try:
        hl7_week = db.session.execute(text("""
            SELECT DATE(scheduled_datetime) AS d,
                   UPPER(TRIM(COALESCE(target_aetitle, modality, 'OT'))) AS col_key,
                   COUNT(*) AS cnt
            FROM hl7_orders
            WHERE DATE(scheduled_datetime) BETWEEN :ws AND :we
              AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
            GROUP BY 1, 2
        """), {"ws": week_start, "we": week_end}).fetchall()
        man_week = db.session.execute(text("""
            SELECT DATE(procedure_datetime) AS d,
                   COALESCE(aetitle, modality_type, 'OT') AS col_key,
                   COUNT(*) AS cnt
            FROM scheduling_entries
            WHERE DATE(procedure_datetime) BETWEEN :ws AND :we
              AND (cancelled IS NULL OR cancelled = false)
            GROUP BY 1, 2
        """), {"ws": week_start, "we": week_end}).fetchall()

        for r in list(hl7_week) + list(man_week):
            dow = r.d.weekday()
            ck  = r.col_key
            week_data.setdefault(ck, {})[dow] = week_data.get(ck, {}).get(dow, 0) + r.cnt
            week_totals[dow] = week_totals.get(dow, 0) + r.cnt
    except Exception:
        pass

    # Week column meta: use same device_map for labels
    week_col_keys = list(week_data.keys())
    week_cols = []
    for ck in week_col_keys:
        if ck in device_map:
            week_cols.append({'key': ck, 'label': device_map[ck]['label'], 'modality': device_map[ck]['modality']})
        else:
            week_cols.append({'key': ck, 'label': ck, 'modality': ck})
    week_cols.sort(key=lambda c: (c['modality'], c['label']))
    week_days = [week_start + timedelta(days=i) for i in range(7)]

    # All saved entries for "All Bookings" tab
    schedules = SchedulingEntry.query.order_by(
        SchedulingEntry.procedure_datetime.desc()
    ).all()

    return render_template('admin_scheduling.html',
        schedule=schedule,
        schedules=schedules,
        view_date=view_date,
        view_date_str=view_date_str,
        view_mode=view_mode,
        prev_date=prev_date,
        next_date=next_date,
        is_today=is_today,
        time_slots=time_slots,
        columns=columns,
        appointments=appointments,
        unscheduled=unscheduled,
        mod_counts=mod_counts,
        class_counts=class_counts,
        total_count=len(appointments),
        date_counts=date_counts,
        strip_dates=strip_dates,
        aetitle_list=aetitle_list,
        # week view
        week_start=week_start,
        week_end=week_end,
        week_days=week_days,
        week_cols=week_cols,
        week_data=week_data,
        week_totals=week_totals,
        prev_week=prev_week,
        next_week=next_week,
    )


@admin_bp.route('/scheduling/cancel/<int:entry_id>', methods=['POST'])
@login_required
def cancel_scheduling_entry(entry_id):
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'scheduling'):
        return abort(403)
    entry = db.session.get(SchedulingEntry, entry_id)
    if not entry:
        return jsonify({'status': 'error', 'message': 'Entry not found'}), 404
    entry.cancelled    = True
    entry.cancelled_at = datetime.utcnow()
    entry.cancelled_by = current_user.username
    db.session.commit()
    return jsonify({'status': 'ok'})


@admin_bp.route('/scheduling/hl7/<int:hl7_id>/arrive', methods=['POST'])
@login_required
def arrive_hl7(hl7_id):
    from db import user_has_page
    from utils.hl7_forward import forward_message as _hl7_forward
    from flask import current_app
    if current_user.role != 'admin' and not user_has_page(current_user, 'scheduling'):
        return abort(403)
    try:
        row = db.session.execute(text("""
            SELECT id, message_id, COALESCE(NULLIF(order_status,''),'SC') AS order_status, raw_message
            FROM hl7_orders WHERE id = :id
        """), {"id": hl7_id}).mappings().fetchone()
        if not row:
            return jsonify({'status': 'error', 'message': 'Order not found'}), 404
        prev = row["order_status"]
        if prev != "SC":
            return jsonify({'status': 'error', 'message': f'Expected SC, current status is {prev}'}), 400
        db.session.execute(text("""
            UPDATE hl7_orders SET order_status='AR', arrived_at=NOW(), arrived_by=:user
            WHERE id=:id
        """), {"id": hl7_id, "user": current_user.username})
        db.session.execute(text("""
            INSERT INTO order_status_log
                (order_id, message_id, from_status, to_status, changed_by, source)
            VALUES (:oid, :mid, :from_s, 'AR', :user, 'scheduling')
        """), {"oid": row["id"], "mid": row["message_id"], "from_s": prev,
               "user": current_user.username})
        db.session.commit()
        _hl7_forward(row["raw_message"], current_app._get_current_object(), order_id=row["id"])
        return jsonify({'status': 'ok'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@admin_bp.route('/scheduling/hl7/<int:hl7_id>/reschedule', methods=['POST'])
@login_required
def reschedule_hl7(hl7_id):
    if current_user.role != 'admin':
        return abort(403)
    data = request.get_json() or {}
    new_dt_str = data.get('scheduled_datetime', '')
    try:
        new_dt = datetime.strptime(new_dt_str, '%Y-%m-%dT%H:%M')
    except (ValueError, TypeError):
        return jsonify({'status': 'error', 'message': 'Invalid datetime'}), 400

    params = {'dt': new_dt, 'id': hl7_id}
    set_clauses = ['scheduled_datetime = :dt']

    if 'procedure_text' in data:
        params['procedure_text'] = (data['procedure_text'] or '').strip()
        set_clauses.append('procedure_text = :procedure_text')
    if 'modality' in data:
        params['modality'] = (data['modality'] or '').strip().upper()
        set_clauses.append('modality = :modality')
    if 'physician' in data:
        params['physician'] = (data['physician'] or '').strip()
        set_clauses.append('ordering_physician = :physician')
    if 'target_aetitle' in data:
        params['ae'] = (data['target_aetitle'] or '').strip() or None
        set_clauses.append('target_aetitle = :ae')

    db.session.execute(
        text(f"UPDATE hl7_orders SET {', '.join(set_clauses)} WHERE id = :id"),
        params
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
    slot_counts: dict = {}

    manual_rows = db.session.execute(text("""
        SELECT procedure_datetime FROM scheduling_entries
        WHERE modality_type = :mod AND procedure_datetime BETWEEN :s AND :e
          AND (cancelled IS NULL OR cancelled = false)
    """), {'mod': modality, 's': day_start, 'e': day_end}).fetchall()
    for r in manual_rows:
        slot = r.procedure_datetime.hour * 60 + (30 if r.procedure_datetime.minute >= 30 else 0)
        slot_counts[slot] = slot_counts.get(slot, 0) + 1

    hl7_rows = db.session.execute(text("""
        SELECT scheduled_datetime FROM hl7_orders
        WHERE UPPER(modality) = :mod
          AND scheduled_datetime BETWEEN :s AND :e
          AND (order_status IS NULL OR order_status NOT IN ('CA','DC'))
          AND (message_type IS NULL OR message_type NOT LIKE 'ADT%')
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
        ('live_feed',       'Live AE Status'),
        ('hl7_orders',      'HL7 Orders'),
        ('scheduling',      'Scheduling'),
        ('cd_print',        'Patient CD Log'),
        ('oru',             'Report Intelligence'),
        ('referring_intel', 'Referring Intel'),
        ('custom_reports',  'Custom Reports'),
        ('patient_portal',  'Patient Portal'),
        ('mapping',         'Modality / Procedures'),
        ('report_ai',       'AI Reports'),
        ('bitnet',          'AI Assistant'),
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
    q = UserAuditLog.query.order_by(UserAuditLog.created_at.desc())
    if category:
        q = q.filter(UserAuditLog.event_category == category)
    entries = q.limit(500).all()

    user_map = {u.id: u.username for u in User.query.all()}

    categories = ['auth', 'user_mgmt', 'report', 'etl', 'ai', 'config']
    return render_template('admin_audit.html',
        entries=entries,
        user_map=user_map,
        categories=categories,
        active_category=category,
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
        _admin_audit('etl_triggered', current_user.id, category='etl')
        db.session.commit()
        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
