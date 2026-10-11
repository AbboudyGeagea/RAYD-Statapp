"""
routes/crn_admin.py
───────────────────
CRN admin: the referring-doctor contact list (doctor code, name, phone, email)
and its CSV import. Admin only.

Import flow: upload → the file is kept in crn_contact_imports → preview of how
every row was read → Import applies it. Nothing changes the contact list before
Import. Applying upserts by doctor code; a blank cell keeps the value already on
file, so a partial file never erases a phone number. Contacts missing from the
file are left as they are.
"""
import json
import logging

from flask import Blueprint, render_template, request, redirect, url_for, abort, flash
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db
from utils.crn_contacts import decode, parse_contacts_csv, rows_to_apply

logger = logging.getLogger("CRN_ADMIN")
crn_admin_bp = Blueprint('crn_admin', __name__, url_prefix='/admin/crn')

MAX_UPLOAD_BYTES = 2 * 1024 * 1024


def _admin_only():
    if current_user.role != 'admin':
        abort(403)


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
