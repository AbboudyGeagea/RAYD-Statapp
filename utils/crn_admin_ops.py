"""
utils/crn_admin_ops.py
──────────────────────
What the Admin > CRN pages do, kept out of the routes so it can be tested:
settings, the go-live switch and its readiness checklist, the test send, the live
board, a notification's timeline, retry, acknowledgement by phone, the dry-run
link, and the CRN Acknowledgement report.
"""
import hashlib
import secrets
import statistics
from datetime import datetime, timedelta, timezone

from sqlalchemy import bindparam, text

from db import db
from utils import crn_dispatch as dispatch
from utils import crn_gateway_client as gateway
from utils.crypto import decrypt, encrypt
from utils.crn_contacts import normalize_phone
from utils.crn_sms import send_sms

OPEN_STATUSES = ('detected', 'sent', 'overdue', 'unroutable', 'send_failed')
STATUS_LABELS = {
    'detected': 'Detected', 'sent': 'Sent', 'overdue': 'Overdue', 'acknowledged': 'Acknowledged',
    'unroutable': 'No contact', 'send_failed': 'Send failed',
}
INT_SETTINGS = {'crn_resend_min': (1, 1440), 'crn_fallback_min': (1, 1440), 'crn_overdue_min': (1, 2880),
                'crn_link_ttl_hours': (1, 720)}


# ── settings ──────────────────────────────────────────────────────────────────

def load_settings():
    cfg = dispatch._settings()
    cfg.setdefault('crn_marker', 'CRN')
    db.session.rollback()
    return cfg


def _set(key, value):
    db.session.execute(text("""
        INSERT INTO settings (key, value) VALUES (:k, :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """), {'k': key, 'v': value})


def save_settings(form):
    """Validate and store the settings form. Returns a list of errors (empty = saved)."""
    errors, values = [], {}
    marker = (form.get('crn_marker') or '').strip()
    if not marker or len(marker) > 40:
        errors.append('The marker must be 1 to 40 characters.')
    values['crn_marker'] = marker
    for key, (lo, hi) in INT_SETTINGS.items():
        try:
            v = int(form.get(key, ''))
            if not lo <= v <= hi:
                raise ValueError
            values[key] = str(v)
        except ValueError:
            errors.append(f'{key.replace("crn_", "").replace("_", " ")} must be a whole number from {lo} to {hi}.')
    if not errors and not (int(values['crn_resend_min']) < int(values['crn_fallback_min']) <= int(values['crn_overdue_min'])):
        errors.append('Times must run in order: resend before fallback, fallback no later than overdue.')

    fallback = (form.get('crn_fallback_code') or '').strip().upper()
    if fallback and not db.session.execute(text("SELECT 1 FROM crn_contacts WHERE doctor_code = :c"),
                                           {'c': fallback}).fetchone():
        errors.append(f'Fallback contact {fallback} is not in the contact list.')
    values['crn_fallback_code'] = fallback

    for key in ('crn_gateway_url', 'crn_public_base_url'):
        url = (form.get(key) or '').strip().rstrip('/')
        if url and not url.startswith(('https://', 'http://')):
            errors.append(f'{key.replace("crn_", "").replace("_", " ")} must start with https://')
        values[key] = url

    template = form.get('crn_sms_template') or ''
    _body, template_ok = dispatch.render_sms(template, 'CRN-XXXX-XXXX', 'https://x/n/y')
    if not template_ok:
        errors.append('The SMS text may only use {ref} and {link}, and must contain {link}.')
    values['crn_sms_template'] = template.strip()

    values['crn_hospital_name'] = (form.get('crn_hospital_name') or '').strip()[:120]
    values['crn_page_footer'] = (form.get('crn_page_footer') or '').strip()[:300]

    if errors:
        db.session.rollback()
        return errors
    for k, v in values.items():
        _set(k, v)
    key = (form.get('crn_gateway_api_key') or '').strip()
    if form.get('clear_api_key'):
        _set('crn_gateway_api_key', '')
    elif key:
        _set('crn_gateway_api_key', encrypt(key))
    db.session.commit()
    return []


def readiness(cfg, check_gateway=True):
    """Checklist shown next to the on/off switch: [(state, label, detail)], state in ok|warn|bad."""
    rows = []
    c = db.session.execute(text("""
        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE active AND phone_e164 IS NOT NULL) AS textable
        FROM crn_contacts
    """)).mappings().fetchone()
    rows.append(('ok' if c['textable'] else 'bad', 'Contacts',
                 f"{c['textable']} of {c['total']} contacts have a usable mobile"))
    fb = dispatch._fallback(cfg)
    rows.append(('ok' if fb else 'warn', 'Fallback contact',
                 f"{fb['doctor_code']} {fb['full_name'] or ''}" if fb else
                 'not set or no usable mobile: CRNs with no reachable doctor will be unroutable'))
    if check_gateway:
        ok, detail = gateway.health(cfg)
        rows.append(('ok' if ok else 'bad', 'CRN gateway', detail))
    rows.append(('ok' if cfg['crn_public_base_url'] else 'bad', 'Link address',
                 cfg['crn_public_base_url'] or 'not set: links in the SMS would point nowhere'))
    rows.append(('warn' if cfg['crn_sms_provider'] == 'log' else 'ok', 'SMS provider',
                 'dry run: messages are recorded, not sent' if cfg['crn_sms_provider'] == 'log'
                 else cfg['crn_sms_provider']))
    db.session.rollback()
    return rows


def set_live(on):
    _set('crn_live_since', datetime.now().isoformat(timespec='seconds') if on else '')
    db.session.commit()


# ── test send ─────────────────────────────────────────────────────────────────

def test_send(contact_code):
    """Place a test page (no patient data) and text it to one contact. Nothing is
    stored in the CRN tables, so a test never reaches the board or the report.
    Returns (ok, message, link or None)."""
    cfg = load_settings()
    contact = dispatch._contact(contact_code)
    if not contact or not contact['phone_e164']:
        return False, 'That contact has no usable mobile number.', None
    if not gateway.configured(cfg):
        return False, 'Set the gateway address and API key first.', None
    token = secrets.token_urlsafe(16)
    ref = 'TEST-' + secrets.token_hex(2).upper()
    expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    payload = {
        'hospital': {'name': cfg['crn_hospital_name'] or None, 'footer': 'TEST notification. No patient data.'},
        'recipient': {'name': contact['full_name'], 'role': 'doctor'},
        'patient': {'name': 'TEST - no patient', 'mrn': 'TEST'},
        'order': {'accession': 'TEST', 'procedure': 'Test of Critical Result Notification'},
        'report': {'text': 'This is a test of Critical Result Notification. No action is needed.'},
    }
    ok, error = gateway.push_page(cfg, hashlib.sha256(token.encode()).hexdigest(), ref, 0, expires, payload)
    if not ok:
        return False, f'The test page could not be placed: {error}', None
    link = f"{(cfg['crn_public_base_url'] or dispatch.PLACEHOLDER_BASE_URL).rstrip('/')}/n/{token}"
    body, _ = dispatch.render_sms(cfg['crn_sms_template'], ref, link)
    result = send_sms(cfg['crn_sms_provider'], contact['phone_e164'], 'TEST ' + body)
    if not result['ok']:
        return False, f"The SMS could not be sent: {result['error']}", link
    note = ' (dry run: the SMS was recorded, not sent)' if result['dry_run'] else ''
    return True, f"Test {ref} sent to {contact['phone_e164']}{note}. The link works for 1 hour.", link


# ── board and timeline ────────────────────────────────────────────────────────

def board(status='open', q='', start=None, end=None):
    where, params = ['TRUE'], {}
    if status == 'open':
        where.append('n.status IN :open')
        params['open'] = OPEN_STATUSES
    elif status and status != 'all':
        where.append('n.status = :status')
        params['status'] = status
    if q:
        where.append('(n.ref_code ILIKE :q OR n.accession_number ILIKE :q OR n.doctor_code ILIKE :q '
                     'OR n.doctor_name ILIKE :q OR n.patient_id ILIKE :q)')
        params['q'] = f'%{q}%'
    if start:
        where.append('n.detected_at >= :start')
        params['start'] = start
    if end:
        where.append("n.detected_at < CAST(:end AS DATE) + 1")
        params['end'] = end
    stmt = text(f"""
        SELECT n.id, n.ref_code, n.detected_at, n.accession_number, n.patient_id, n.doctor_code, n.doctor_name,
               n.status, n.first_sent_at, n.acknowledged_at,
               EXTRACT(EPOCH FROM (COALESCE(n.acknowledged_at, NOW()) - n.first_sent_at)) / 60 AS minutes,
               (SELECT MAX(opened_at) FROM crn_recipients r WHERE r.notification_id = n.id) AS opened_at
        FROM crn_notifications n
        WHERE {' AND '.join(where)}
        ORDER BY n.detected_at DESC
        LIMIT 500
    """)
    if 'open' in params:
        stmt = stmt.bindparams(bindparam('open', expanding=True))
    rows = db.session.execute(stmt, params).mappings().fetchall()
    counts = dict(db.session.execute(text("SELECT status, COUNT(*) FROM crn_notifications GROUP BY status")).fetchall())
    return rows, counts


def timeline(notification_id):
    n = db.session.execute(text("SELECT * FROM crn_notifications WHERE id = :id"),
                           {'id': notification_id}).mappings().fetchone()
    if not n:
        return None, [], []
    recipients = db.session.execute(text("""
        SELECT id, role, contact_code, contact_name, phone_e164, send_count, fail_count, last_sent_at, opened_at,
               page_pushed_at, expires_at < NOW() AS expired
        FROM crn_recipients WHERE notification_id = :id ORDER BY id
    """), {'id': notification_id}).mappings().fetchall()
    events = db.session.execute(text("""
        SELECT event_type, at, detail FROM crn_events WHERE notification_id = :id ORDER BY id
    """), {'id': notification_id}).mappings().fetchall()
    return n, recipients, events


def retry(notification_id, username):
    """Send an unroutable or failed CRN through routing again (after fixing a contact)."""
    n = db.session.execute(text("SELECT status FROM crn_notifications WHERE id = :id FOR UPDATE"),
                           {'id': notification_id}).fetchone()
    if not n or n[0] not in ('unroutable', 'send_failed'):
        db.session.rollback()
        return False
    db.session.execute(text("UPDATE crn_recipients SET fail_count = 0 WHERE notification_id = :id AND send_count = 0"),
                       {'id': notification_id})
    db.session.execute(text("UPDATE crn_notifications SET status = 'detected' WHERE id = :id"), {'id': notification_id})
    dispatch._event(notification_id, 'retry_requested', {'by': username, 'previous_status': n[0]})
    db.session.commit()
    return True


def acknowledge_by_phone(notification_id, note, username):
    """Record that the result was communicated by phone. Stops escalation; kept
    separate from the doctor's own acknowledgement on the page."""
    note = (note or '').strip()
    if len(note) < 5:
        return 'Write a note (who was called and what was said).'
    n = db.session.execute(text("SELECT acknowledged_at FROM crn_notifications WHERE id = :id FOR UPDATE"),
                           {'id': notification_id}).fetchone()
    if not n:
        db.session.rollback()
        return 'Notification not found.'
    if n[0]:
        db.session.rollback()
        return 'This CRN is already acknowledged.'
    dispatch._event(notification_id, 'acknowledged_by_phone', {'by': username, 'note': note[:1000]})
    db.session.execute(text("""
        UPDATE crn_notifications SET acknowledged_at = NOW(), status = 'acknowledged' WHERE id = :id
    """), {'id': notification_id})
    db.session.commit()
    return None


def dry_run_link(notification_id, recipient_id, username):
    """The link a recipient's SMS would carry, so an admin can test the doctor's
    side (open the page, acknowledge) before an SMS provider exists. Dry run only:
    with a real provider the link must stay in the SMS. Every use is recorded in
    the notification's history. Returns (link, error)."""
    cfg = load_settings()
    if cfg['crn_sms_provider'] != 'log':
        return None, 'Only available in dry run, while no SMS provider is connected.'
    rec = db.session.execute(text("""
        SELECT role, contact_code, contact_name, token_enc, page_pushed_at, expires_at < NOW() AS expired
        FROM crn_recipients WHERE id = :rid AND notification_id = :nid
    """), {'rid': recipient_id, 'nid': notification_id}).mappings().fetchone()
    if not rec:
        db.session.rollback()
        return None, 'Recipient not found.'
    if rec['page_pushed_at'] is None:
        db.session.rollback()
        return None, 'The page is not on the CRN gateway yet.'
    if rec['expired']:
        db.session.rollback()
        return None, 'This link has expired.'
    token = decrypt(rec['token_enc'])
    if not token or token == rec['token_enc']:
        db.session.rollback()
        return None, 'The link could not be decrypted (SECRET_KEY changed?).'
    dispatch._event(notification_id, 'dry_run_link_opened', {
        'role': rec['role'], 'contact_code': rec['contact_code'], 'contact_name': rec['contact_name'],
        'by': username})
    db.session.commit()
    base = (cfg['crn_public_base_url'] or dispatch.PLACEHOLDER_BASE_URL).rstrip('/')
    return f'{base}/n/{token}', None


# ── CRN Acknowledgement report ────────────────────────────────────────────────

def _median(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 1) if values else None


def report(start, end):
    rows = db.session.execute(text("""
        SELECT n.id, n.ref_code, n.detected_at, n.report_received_at, n.accession_number, n.doctor_code,
               n.doctor_name, n.signing_radiologist, n.status, n.first_sent_at, n.acknowledged_at,
               r.role AS acknowledged_role,
               EXTRACT(EPOCH FROM (n.acknowledged_at - COALESCE(n.report_received_at, n.detected_at))) / 60 AS ack_min,
               EXISTS (SELECT 1 FROM crn_events e WHERE e.notification_id = n.id AND e.event_type = 'resent') AS resent,
               EXISTS (SELECT 1 FROM crn_events e WHERE e.notification_id = n.id
                       AND e.event_type IN ('escalated_fallback', 'sent_to_fallback')) AS fallback,
               EXISTS (SELECT 1 FROM crn_events e WHERE e.notification_id = n.id AND e.event_type = 'overdue') AS overdue,
               EXISTS (SELECT 1 FROM crn_events e WHERE e.notification_id = n.id
                       AND e.event_type = 'acknowledged_by_phone') AS by_phone
        FROM crn_notifications n
        LEFT JOIN crn_recipients r ON r.id = n.acknowledged_by
        WHERE n.detected_at >= :start AND n.detected_at < CAST(:end AS DATE) + 1
        ORDER BY n.detected_at
    """), {'start': start, 'end': end}).mappings().fetchall()

    def how(r):
        if not r['acknowledged_at']:
            return 'not acknowledged'
        if r['by_phone']:          # the phone acknowledgement came first (a later click cannot replace it)
            return 'by phone'
        if r['acknowledged_role'] == 'fallback':
            return 'fallback contact'
        return 'after resend' if r['resent'] else 'first SMS'

    acked = [r for r in rows if r['acknowledged_at']]
    summary = {
        'total': len(rows), 'acknowledged': len(acked),
        'pct': round(len(acked) * 100 / len(rows)) if rows else None,
        'median_min': _median([float(r['ack_min']) for r in acked if r['ack_min'] is not None]),
        'overdue': sum(1 for r in rows if r['overdue']),
        'unroutable': sum(1 for r in rows if r['status'] == 'unroutable'),
        'open': sum(1 for r in rows if not r['acknowledged_at']),
    }

    def group(key_fn):
        out = {}
        for r in rows:
            g = out.setdefault(key_fn(r), {'total': 0, 'acknowledged': 0, 'overdue': 0, 'mins': []})
            g['total'] += 1
            g['overdue'] += 1 if r['overdue'] else 0
            if r['acknowledged_at']:
                g['acknowledged'] += 1
                g['mins'].append(float(r['ack_min']) if r['ack_min'] is not None else None)
        return sorted(({'key': k, 'total': v['total'], 'acknowledged': v['acknowledged'], 'overdue': v['overdue'],
                        'median_min': _median(v['mins'])} for k, v in out.items()),
                      key=lambda g: -g['total'])

    by_how = {}
    for r in rows:
        by_how[how(r)] = by_how.get(how(r), 0) + 1
    return {
        'summary': summary,
        'by_doctor': group(lambda r: f"{r['doctor_code'] or '—'} {r['doctor_name'] or ''}".strip()),
        'by_radiologist': group(lambda r: r['signing_radiologist'] or '—'),
        'by_how': sorted(by_how.items(), key=lambda kv: -kv[1]),
        'rows': [dict(r, how=how(r)) for r in rows],
    }


# ── contacts ──────────────────────────────────────────────────────────────────

def save_contact(form, username, existing_code=None):
    """Add or edit one contact. Returns a list of errors."""
    code = (existing_code or form.get('doctor_code') or '').strip().upper()
    errors = []
    if not code or len(code) > 64:
        errors.append('A doctor code is required.')
    phone_raw = (form.get('phone') or '').strip() or None
    phone_e164, note = normalize_phone(phone_raw)
    if phone_raw and not phone_e164:
        errors.append(note or 'The mobile number could not be read.')
    email = (form.get('email') or '').strip().lower() or None
    if email and '@' not in email:
        errors.append('The email address is not valid.')
    if not existing_code and code and db.session.execute(
            text("SELECT 1 FROM crn_contacts WHERE doctor_code = :c"), {'c': code}).fetchone():
        errors.append(f'{code} already exists: edit it instead.')
    if errors:
        db.session.rollback()
        return errors
    db.session.execute(text("""
        INSERT INTO crn_contacts (doctor_code, full_name, phone_raw, phone_e164, email, active, updated_at, updated_by)
        VALUES (:code, :name, :raw, :e164, :email, :active, NOW(), :by)
        ON CONFLICT (doctor_code) DO UPDATE SET
            full_name = EXCLUDED.full_name, phone_raw = EXCLUDED.phone_raw, phone_e164 = EXCLUDED.phone_e164,
            email = EXCLUDED.email, active = EXCLUDED.active, updated_at = NOW(), updated_by = EXCLUDED.updated_by
    """), {'code': code, 'name': (form.get('full_name') or '').strip() or None, 'raw': phone_raw, 'e164': phone_e164,
           'email': email, 'active': bool(form.get('active')), 'by': username})
    db.session.commit()
    return []
