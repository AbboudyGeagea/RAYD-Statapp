"""
routes/referring_contacts_admin.py
───────────────────────────────────────────────────────────────
Admin tool for managing referring-physician contact info and preferred
notification channel — feeds the CRN (Critical Result Notification) build
(docs/LAUMC_SCOPE.md #10). See migrations/0077_add_referring_contacts.sql.

Admin-only (not license-gated in spirit — a config tool, like DB Manager /
Floor Plan — registered through the license system the same way for
consistency, always on in every tier preset).

Registered in registry.py:
    from routes.referring_contacts_admin import referring_contacts_bp
    app.register_blueprint(referring_contacts_bp)
"""
import csv
import io
import logging
from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

logger = logging.getLogger("REFERRING_CONTACTS_ADMIN")
referring_contacts_bp = Blueprint(
    'referring_contacts_admin', __name__, url_prefix='/admin/referring-contacts'
)

_CHANNELS = {'email', 'sms', 'whatsapp'}


@referring_contacts_bp.route('/')
@login_required
def referring_contacts_page():
    if current_user.role != 'admin':
        abort(403)
    return render_template('referring_contacts_admin.html')


@referring_contacts_bp.route('/api/list')
@login_required
def api_list():
    if current_user.role != 'admin':
        abort(403)
    rows = db.session.execute(text("""
        SELECT id, physician_name, middle_name, email, phone, whatsapp_number,
               preferred_channel, active, notes, auto_filled,
               to_char(updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at
        FROM referring_contacts
        ORDER BY physician_name
    """)).mappings().fetchall()
    return jsonify([dict(r) for r in rows])


@referring_contacts_bp.route('/api/known-names')
@login_required
def api_known_names():
    """
    Referring-physician names seen in PACS that don't have a contact row yet —
    lets the admin pick from real data instead of typing free text (a typo
    would silently break the name match back to etl_didb_studies).
    """
    if current_user.role != 'admin':
        abort(403)
    rows = db.session.execute(text("""
        SELECT DISTINCT TRIM(CONCAT(referring_physician_first_name, ' ', referring_physician_last_name)) AS name
        FROM etl_didb_studies
        WHERE referring_physician_last_name IS NOT NULL AND referring_physician_last_name != ''
          AND TRIM(CONCAT(referring_physician_first_name, ' ', referring_physician_last_name)) NOT IN (
              SELECT physician_name FROM referring_contacts
          )
        ORDER BY 1
        LIMIT 500
    """)).fetchall()
    return jsonify([r[0] for r in rows])


def _validate_payload(data):
    name = (data.get('physician_name') or '').strip()
    channel = (data.get('preferred_channel') or 'email').strip().lower()
    email = (data.get('email') or '').strip() or None
    phone = (data.get('phone') or '').strip() or None
    whatsapp = (data.get('whatsapp_number') or '').strip() or None
    middle_name = (data.get('middle_name') or '').strip() or None
    if not name:
        return None, "physician_name is required"
    if channel not in _CHANNELS:
        return None, f"preferred_channel must be one of {sorted(_CHANNELS)}"
    if channel == 'email' and not email:
        return None, "email is required when preferred_channel is 'email'"
    if channel == 'sms' and not phone:
        return None, "phone is required when preferred_channel is 'sms'"
    if channel == 'whatsapp' and not whatsapp:
        return None, "whatsapp_number is required when preferred_channel is 'whatsapp'"
    return {
        'physician_name': name, 'email': email, 'phone': phone,
        'whatsapp_number': whatsapp, 'preferred_channel': channel,
        'middle_name': middle_name,
        'notes': (data.get('notes') or '').strip() or None,
    }, None


@referring_contacts_bp.route('/api/save', methods=['POST'])
@login_required
def api_save():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    payload, err = _validate_payload(data)
    if err:
        return jsonify({"error": err}), 400
    contact_id = data.get('id')
    try:
        if contact_id:
            db.session.execute(text("""
                UPDATE referring_contacts
                SET email = :email, phone = :phone, whatsapp_number = :whatsapp_number,
                    preferred_channel = :preferred_channel, notes = :notes, middle_name = :middle_name,
                    auto_filled = FALSE, updated_by = :uid, updated_at = NOW()
                WHERE id = :id
            """), {**payload, 'id': contact_id, 'uid': current_user.id})
        else:
            db.session.execute(text("""
                INSERT INTO referring_contacts
                    (physician_name, email, phone, whatsapp_number, preferred_channel, notes, middle_name, created_by, updated_by)
                VALUES
                    (:physician_name, :email, :phone, :whatsapp_number, :preferred_channel, :notes, :middle_name, :uid, :uid)
                ON CONFLICT (physician_name) DO UPDATE SET
                    email = EXCLUDED.email, phone = EXCLUDED.phone,
                    whatsapp_number = EXCLUDED.whatsapp_number,
                    preferred_channel = EXCLUDED.preferred_channel,
                    notes = EXCLUDED.notes, middle_name = EXCLUDED.middle_name, auto_filled = FALSE,
                    updated_by = EXCLUDED.updated_by, updated_at = NOW()
            """), {**payload, 'uid': current_user.id})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Referring contact save error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@referring_contacts_bp.route('/api/toggle-active', methods=['POST'])
@login_required
def api_toggle_active():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    contact_id = data.get('id')
    if not contact_id:
        return jsonify({"error": "id is required"}), 400
    try:
        db.session.execute(text("""
            UPDATE referring_contacts SET active = NOT active, updated_by = :uid, updated_at = NOW()
            WHERE id = :id
        """), {"id": contact_id, "uid": current_user.id})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Referring contact toggle error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@referring_contacts_bp.route('/api/delete', methods=['POST'])
@login_required
def api_delete():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    contact_id = data.get('id')
    if not contact_id:
        return jsonify({"error": "id is required"}), 400
    try:
        db.session.execute(text("DELETE FROM referring_contacts WHERE id = :id"), {"id": contact_id})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Referring contact delete error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── CSV bulk import (operator instruction, 2026-08-01: a second population path
# alongside manual entry and the RIS auto-sync, for lists the hospital already has
# on file) ──────────────────────────────────────────────────────────────────────

_CSV_COLUMNS = ['physician_name', 'email', 'phone', 'whatsapp_number', 'preferred_channel', 'middle_name', 'notes']


@referring_contacts_bp.route('/api/upload-csv', methods=['POST'])
@login_required
def api_upload_csv():
    if current_user.role != 'admin':
        abort(403)
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        raw = file.read().decode('utf-8-sig')  # -sig strips a BOM from Excel-exported CSVs
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded CSV"}), 400

    reader = csv.DictReader(io.StringIO(raw))
    if reader.fieldnames is None:
        return jsonify({"error": "Empty file"}), 400
    unknown = set(reader.fieldnames) - set(_CSV_COLUMNS)
    if unknown:
        return jsonify({"error": f"Unknown column(s): {', '.join(sorted(unknown))}. "
                                  f"Expected: {', '.join(_CSV_COLUMNS)}"}), 400

    created, updated, errors = 0, 0, []
    for i, raw_row in enumerate(reader, start=2):  # row 1 is the header
        payload, err = _validate_payload(raw_row)
        if err:
            errors.append({"row": i, "physician_name": raw_row.get('physician_name', ''), "error": err})
            continue
        try:
            existing = db.session.execute(
                text("SELECT id FROM referring_contacts WHERE physician_name = :name"),
                {"name": payload['physician_name']}
            ).fetchone()
            db.session.execute(text("""
                INSERT INTO referring_contacts
                    (physician_name, email, phone, whatsapp_number, preferred_channel, notes, middle_name, created_by, updated_by)
                VALUES
                    (:physician_name, :email, :phone, :whatsapp_number, :preferred_channel, :notes, :middle_name, :uid, :uid)
                ON CONFLICT (physician_name) DO UPDATE SET
                    email = EXCLUDED.email, phone = EXCLUDED.phone,
                    whatsapp_number = EXCLUDED.whatsapp_number,
                    preferred_channel = EXCLUDED.preferred_channel,
                    notes = EXCLUDED.notes, middle_name = EXCLUDED.middle_name, auto_filled = FALSE,
                    updated_by = EXCLUDED.updated_by, updated_at = NOW()
            """), {**payload, 'uid': current_user.id})
            if existing:
                updated += 1
            else:
                created += 1
        except Exception as e:
            db.session.rollback()
            errors.append({"row": i, "physician_name": raw_row.get('physician_name', ''), "error": str(e)})
            continue

    db.session.commit()
    return jsonify({"created": created, "updated": updated, "errors": errors})


@referring_contacts_bp.route('/api/csv-template')
@login_required
def api_csv_template():
    if current_user.role != 'admin':
        abort(403)
    from flask import Response
    sample = (
        ','.join(_CSV_COLUMNS) + '\n' +
        'Ramzi Nakhle,ramzi.nakhle@example.com,+96170000000,,email,Elie,\n'
    )
    return Response(sample, mimetype='text/csv', headers={
        'Content-Disposition': 'attachment; filename=referring_contacts_template.csv'
    })


# ── CRN message template (operator instruction, 2026-08-01: admin-editable, not
# hardcoded in utils/crn_scan.py) ────────────────────────────────────────────────

@referring_contacts_bp.route('/api/message-template')
@login_required
def api_get_message_template():
    if current_user.role != 'admin':
        abort(403)
    row = db.session.execute(
        text("SELECT value FROM settings WHERE key = 'crn_message_template'")
    ).fetchone()
    return jsonify({"template": row[0] if row else ""})


@referring_contacts_bp.route('/api/message-template', methods=['POST'])
@login_required
def api_save_message_template():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    template = (data.get('template') or '').strip()
    if not template:
        return jsonify({"error": "Template cannot be empty"}), 400
    if '{accession}' not in template or '{ack_url}' not in template:
        return jsonify({"error": "Template must contain both {accession} and {ack_url}"}), 400
    try:
        db.session.execute(text("""
            INSERT INTO settings (key, value) VALUES ('crn_message_template', :v)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """), {"v": template})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"CRN message template save error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
