"""
routes/crn_admin.py
───────────────────
Admin > CRN (Critical Result Notification):
  board, notification timeline, acknowledgement report   admin, viewer, viewer2 (read-only)
  settings, go-live switch, test send, retry,
  acknowledgement by phone, contacts and CSV import      admin only
The work itself is in utils/crn_admin_ops.py.

Contacts: the referring-doctor contact list (doctor code, name, phone, email)
and its CSV import.

Import flow: upload → the file is kept in crn_contact_imports → preview of how
every row was read → Import applies it. Nothing changes the contact list before
Import. Applying upserts by doctor code; a blank cell keeps the value already on
file, so a partial file never erases a phone number. Contacts missing from the
file are left as they are.
"""
import csv
import io
import json
import logging
from datetime import date, timedelta

from flask import Blueprint, Response, render_template, request, redirect, url_for, abort, flash
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db
from utils import crn_admin_ops as ops
from utils.crn_contacts import decode, parse_contacts_csv, rows_to_apply

logger = logging.getLogger("CRN_ADMIN")
crn_admin_bp = Blueprint('crn_admin', __name__, url_prefix='/admin/crn')

MAX_UPLOAD_BYTES = 2 * 1024 * 1024


BOARD_ROLES = ('admin', 'viewer', 'viewer2')


def _admin_only():
    if current_user.role != 'admin':
        abort(403)


def _board_roles():
    if current_user.role not in BOARD_ROLES:
        abort(403)


def _audit(action, detail):
    from utils.audit import log_event
    log_event(action, category='admin', resource_type='crn', detail=detail)


# ── board, timeline, report ───────────────────────────────────────────────────

@crn_admin_bp.route('/')
@login_required
def home():
    _board_roles()
    return redirect(url_for('crn_admin.board_page'))


@crn_admin_bp.route('/board')
@login_required
def board_page():
    _board_roles()
    status = request.args.get('status', 'open')
    q = (request.args.get('q') or '').strip()
    start = request.args.get('start') or None
    end = request.args.get('end') or None
    rows, counts = ops.board(status, q, start, end)
    return render_template('crn_board.html', rows=rows, counts=counts, status=status, q=q, start=start or '',
                           end=end or '', labels=ops.STATUS_LABELS, live_since=ops.load_settings()['crn_live_since'])


@crn_admin_bp.route('/notification/<int:notification_id>')
@login_required
def notification_page(notification_id):
    _board_roles()
    n, recipients, events = ops.timeline(notification_id)
    if not n:
        abort(404)
    return render_template('crn_notification.html', n=n, recipients=recipients, events=events,
                           labels=ops.STATUS_LABELS)


@crn_admin_bp.route('/notification/<int:notification_id>/retry', methods=['POST'])
@login_required
def retry(notification_id):
    _admin_only()
    if ops.retry(notification_id, current_user.username):
        _audit('crn_retry', {'notification_id': notification_id})
        flash('Sent back to routing: it will be retried within 30 seconds.', 'success')
    else:
        flash('Only CRNs with no contact or a failed send can be retried.', 'error')
    return redirect(url_for('crn_admin.notification_page', notification_id=notification_id))


@crn_admin_bp.route('/notification/<int:notification_id>/ack-phone', methods=['POST'])
@login_required
def ack_by_phone(notification_id):
    _admin_only()
    error = ops.acknowledge_by_phone(notification_id, request.form.get('note'), current_user.username)
    if error:
        flash(error, 'error')
    else:
        _audit('crn_acknowledged_by_phone', {'notification_id': notification_id})
        flash('Recorded as acknowledged by phone. Escalation stopped.', 'success')
    return redirect(url_for('crn_admin.notification_page', notification_id=notification_id))


def _period():
    end = request.args.get('end') or date.today().isoformat()
    start = request.args.get('start') or (date.fromisoformat(end) - timedelta(days=29)).isoformat()
    return start, end


@crn_admin_bp.route('/report')
@login_required
def report_page():
    _board_roles()
    start, end = _period()
    return render_template('crn_report.html', data=ops.report(start, end), start=start, end=end)


@crn_admin_bp.route('/report.csv')
@login_required
def report_csv():
    _board_roles()
    start, end = _period()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['reference', 'detected_at', 'report_received_at', 'accession', 'doctor_code', 'doctor_name',
                'radiologist', 'status', 'first_sms_at', 'acknowledged_at', 'minutes_to_acknowledge',
                'how_acknowledged', 'resent', 'fallback', 'overdue'])
    for r in ops.report(start, end)['rows']:
        w.writerow([r['ref_code'], r['detected_at'], r['report_received_at'], r['accession_number'],
                    r['doctor_code'], r['doctor_name'], r['signing_radiologist'], r['status'], r['first_sent_at'],
                    r['acknowledged_at'], round(float(r['ack_min']), 1) if r['ack_min'] is not None else '',
                    r['how'], r['resent'], r['fallback'], r['overdue']])
    _audit('crn_report_exported', {'start': start, 'end': end})
    return Response(out.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=crn_report_{start}_{end}.csv'})


# ── settings, switch, test ────────────────────────────────────────────────────

@crn_admin_bp.route('/settings', methods=['GET', 'POST'])
@login_required
def settings_page():
    _admin_only()
    if request.method == 'POST':
        errors = ops.save_settings(request.form)
        if errors:
            for e in errors:
                flash(e, 'error')
        else:
            _audit('crn_settings_saved', {k: v for k, v in request.form.items()
                                          if k not in ('crn_gateway_api_key', 'csrf_token')})
            flash('Settings saved.', 'success')
        return redirect(url_for('crn_admin.settings_page'))
    cfg = ops.load_settings()
    contacts = db.session.execute(text("""
        SELECT doctor_code, full_name, phone_e164 FROM crn_contacts WHERE active ORDER BY doctor_code
    """)).mappings().fetchall()
    sample, _ = ops.dispatch.render_sms(cfg['crn_sms_template'], 'CRN-J2N8-L38G',
                                        (cfg['crn_public_base_url'] or 'https://crn.hospital') + '/n/AbCdEfGhIjKlMnOpQrStUv')
    return render_template('crn_settings.html', cfg=cfg, checks=ops.readiness(cfg), contacts=contacts,
                           sample=sample, api_key_set=bool(cfg['crn_gateway_api_key']))


@crn_admin_bp.route('/settings/live', methods=['POST'])
@login_required
def switch_live():
    _admin_only()
    on = request.form.get('on') == '1'
    ops.set_live(on)
    _audit('crn_switched_on' if on else 'crn_switched_off', {})
    flash('CRN is ON: reports received from now on can notify.' if on else
          'CRN is OFF: no new notifications. Open ones keep escalating until acknowledged.', 'success')
    return redirect(url_for('crn_admin.settings_page'))


@crn_admin_bp.route('/settings/test', methods=['POST'])
@login_required
def test_send():
    _admin_only()
    code = (request.form.get('doctor_code') or '').strip().upper()
    ok, message, link = ops.test_send(code)
    _audit('crn_test_sent', {'doctor_code': code, 'ok': ok, 'message': message})
    flash(message + (f' Link: {link}' if ok and link else ''), 'success' if ok else 'error')
    return redirect(url_for('crn_admin.settings_page'))


# ── contacts ──────────────────────────────────────────────────────────────────

@crn_admin_bp.route('/contacts/new', methods=['GET', 'POST'])
@crn_admin_bp.route('/contacts/<code>/edit', methods=['GET', 'POST'])
@login_required
def contact_form(code=None):
    _admin_only()
    contact = None
    if code:
        contact = db.session.execute(text("SELECT * FROM crn_contacts WHERE doctor_code = :c"),
                                     {'c': code.upper()}).mappings().fetchone()
        if not contact:
            abort(404)
    if request.method == 'POST':
        errors = ops.save_contact(request.form, current_user.username, existing_code=contact and contact['doctor_code'])
        if not errors:
            saved = contact['doctor_code'] if contact else request.form.get('doctor_code', '').strip().upper()
            _audit('crn_contact_saved', {'doctor_code': saved})
            flash(f'Contact {saved} saved.', 'success')
            return redirect(url_for('crn_admin.contacts'))
        for e in errors:
            flash(e, 'error')
        contact = dict(contact or {}, **{k: request.form.get(k) for k in ('doctor_code', 'full_name', 'email')},
                       phone_raw=request.form.get('phone'), active=bool(request.form.get('active')))
    return render_template('crn_contact_form.html', contact=contact, is_new=code is None)


@crn_admin_bp.route('/contacts')
@login_required
def contacts():
    _admin_only()
    q = (request.args.get('q') or '').strip()
    rows = db.session.execute(text("""
        SELECT doctor_code, full_name, phone_raw, phone_e164, email, active, updated_at, updated_by
        FROM crn_contacts
        WHERE :q = '' OR doctor_code ILIKE :like OR full_name ILIKE :like
              OR phone_raw ILIKE :like OR email ILIKE :like
        ORDER BY doctor_code
    """), {'q': q, 'like': f'%{q}%'}).mappings().fetchall()
    stats = db.session.execute(text("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE phone_e164 IS NOT NULL) AS with_phone,
               COUNT(*) FILTER (WHERE active) AS active
        FROM crn_contacts
    """)).mappings().fetchone()
    imports = db.session.execute(text("""
        SELECT id, filename, uploaded_at, uploaded_by, applied_at, applied_by, summary
        FROM crn_contact_imports ORDER BY id DESC LIMIT 10
    """)).mappings().fetchall()
    return render_template('crn_contacts.html', rows=rows, stats=stats, imports=imports, q=q)


@crn_admin_bp.route('/contacts/upload', methods=['POST'])
@login_required
def upload():
    _admin_only()
    f = request.files.get('file')
    if not f or not f.filename:
        flash('Choose a CSV file first.', 'error')
        return redirect(url_for('crn_admin.contacts'))
    data = f.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        flash('The file is larger than 2 MB. Is it the contact list?', 'error')
        return redirect(url_for('crn_admin.contacts'))
    content, encoding = decode(data)
    import_id = db.session.execute(text("""
        INSERT INTO crn_contact_imports (filename, content, uploaded_by, summary)
        VALUES (:fn, :content, :by, CAST(:summary AS JSONB))
        RETURNING id
    """), {'fn': f.filename[:255], 'content': content.replace('\x00', ''),
           'by': current_user.username, 'summary': json.dumps({'encoding': encoding})}).scalar()
    db.session.commit()
    return redirect(url_for('crn_admin.preview', import_id=import_id))


def _load_import(import_id):
    imp = db.session.execute(text("""
        SELECT id, filename, content, uploaded_at, uploaded_by, applied_at, applied_by, summary
        FROM crn_contact_imports WHERE id = :id
    """), {'id': import_id}).mappings().fetchone()
    if not imp:
        abort(404)
    return imp


@crn_admin_bp.route('/contacts/import/<int:import_id>')
@login_required
def preview(import_id):
    _admin_only()
    imp = _load_import(import_id)
    parsed = parse_contacts_csv(imp['content'])
    parsed['encoding'] = (imp['summary'] or {}).get('encoding', parsed['encoding'])
    to_apply = rows_to_apply(parsed)
    existing = {}
    if to_apply:
        existing = {r['doctor_code']: r for r in db.session.execute(text("""
            SELECT doctor_code, full_name, phone_raw, phone_e164, email FROM crn_contacts
            WHERE doctor_code = ANY(:codes)
        """), {'codes': [r['doctor_code'] for r in to_apply]}).mappings().fetchall()}
    applied = {id(r) for r in to_apply}
    for r in parsed['rows']:
        r['change'] = None
        if id(r) in applied:
            r['change'] = 'update' if r['doctor_code'] in existing else 'new'
    return render_template('crn_contacts_import.html', imp=imp, parsed=parsed,
                           apply_count=len(to_apply),
                           new_count=sum(1 for r in to_apply if r['change'] == 'new'))


@crn_admin_bp.route('/contacts/import/<int:import_id>/apply', methods=['POST'])
@login_required
def apply(import_id):
    _admin_only()
    imp = _load_import(import_id)
    if imp['applied_at']:
        flash('This file was already imported.', 'error')
        return redirect(url_for('crn_admin.contacts'))
    parsed = parse_contacts_csv(imp['content'])
    if parsed['error']:
        flash(parsed['error'], 'error')
        return redirect(url_for('crn_admin.preview', import_id=import_id))

    inserted = updated = 0
    for r in rows_to_apply(parsed):
        was_insert = db.session.execute(text("""
            INSERT INTO crn_contacts (doctor_code, full_name, phone_raw, phone_e164, email,
                                      updated_at, updated_by)
            VALUES (:code, :name, :phone_raw, :phone_e164, :email, NOW(), :by)
            ON CONFLICT (doctor_code) DO UPDATE SET
                full_name  = COALESCE(EXCLUDED.full_name, crn_contacts.full_name),
                phone_raw  = COALESCE(EXCLUDED.phone_raw, crn_contacts.phone_raw),
                phone_e164 = CASE WHEN EXCLUDED.phone_raw IS NOT NULL
                                  THEN EXCLUDED.phone_e164 ELSE crn_contacts.phone_e164 END,
                email      = COALESCE(EXCLUDED.email, crn_contacts.email),
                updated_at = NOW(),
                updated_by = EXCLUDED.updated_by
            RETURNING (xmax = 0)
        """), {'code': r['doctor_code'], 'name': r['full_name'], 'phone_raw': r['phone_raw'],
               'phone_e164': r['phone_e164'], 'email': r['email'], 'by': current_user.username}).scalar()
        if was_insert:
            inserted += 1
        else:
            updated += 1

    counts = parsed['counts']
    summary = {'encoding': (imp['summary'] or {}).get('encoding'), 'inserted': inserted, 'updated': updated,
               'warnings': counts.get('warning', 0), 'skipped': counts.get('skipped', 0),
               'duplicates': counts.get('duplicate', 0)}
    db.session.execute(text("""
        UPDATE crn_contact_imports
        SET applied_at = NOW(), applied_by = :by, summary = CAST(:summary AS JSONB)
        WHERE id = :id
    """), {'by': current_user.username, 'summary': json.dumps(summary), 'id': import_id})
    db.session.commit()

    from utils.audit import log_event
    log_event('crn_contacts_imported', category='admin', resource_type='crn_contacts',
              detail={'import_id': import_id, 'filename': imp['filename'], **summary})
    flash(f"Imported {inserted} new and updated {updated} existing contact(s).", 'success')
    return redirect(url_for('crn_admin.contacts'))
