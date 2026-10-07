"""
utils/crn_dispatch.py
─────────────────────
CRN dispatcher, run every 30 s by the scheduler (app.py). One pass:

1. Route each 'detected' notification: report accession -> order -> doctor code
   (hl7_orders.ordering_physician_code) -> crn_contacts. The doctor gets an SMS
   with a link of their own. When a step is missing (no order, no code, code not
   in the contact list, no usable mobile) the reason is recorded and the fallback
   contact is texted instead; with no usable fallback the notification is
   'unroutable'.
2. Escalate each unacknowledged notification, counted from its first SMS:
   link not opened after crn_resend_min -> resend to the doctor (once);
   after crn_fallback_min -> text the fallback contact;
   after crn_overdue_min -> status 'overdue'.
   A failed send is retried on later passes, up to MAX_SEND_FAILURES.

Every notification is handled in its own transaction under FOR UPDATE SKIP
LOCKED, so two dispatchers can never text the same person twice. Nothing runs
while settings.crn_live_since is empty (CRN off). The SMS carries only the
reference code and the link, never patient data.
"""
import hashlib
import json
import logging
import secrets
import string
from collections import Counter

from sqlalchemy import text

from db import db
from utils.crypto import encrypt, decrypt
from utils.crn_sms import send_sms

logger = logging.getLogger("CRN_DISPATCH")

MAX_SEND_FAILURES = 5
DEFAULT_TEMPLATE = 'Critical result for your patient. Ref {ref}. View and acknowledge: {link}'
PLACEHOLDER_BASE_URL = 'https://crn-gateway.invalid'
DEFAULTS = {
    'crn_live_since': '', 'crn_resend_min': '15', 'crn_fallback_min': '30', 'crn_overdue_min': '60',
    'crn_link_ttl_hours': '72', 'crn_fallback_code': '', 'crn_public_base_url': '',
    'crn_sms_provider': 'log', 'crn_sms_template': DEFAULT_TEMPLATE,
}


def _settings():
    cfg = dict(DEFAULTS)
    for key, value in db.session.execute(text("SELECT key, value FROM settings WHERE key LIKE 'crn%'")):
        cfg[key] = (value or '').strip()
    for key in ('crn_resend_min', 'crn_fallback_min', 'crn_overdue_min', 'crn_link_ttl_hours'):
        try:
            cfg[key] = int(cfg[key])
        except (TypeError, ValueError):
            cfg[key] = int(DEFAULTS[key])
    return cfg


def render_sms(template, ref, link):
    """(body, template_ok). Only {ref} and {link} may appear in the template, and
    {link} must: anything else falls back to the default text, so a template edit
    can never put patient data in an SMS or drop the link."""
    try:
        fields = {f for _, f, _, _ in string.Formatter().parse(template) if f is not None}
    except ValueError:
        fields = None
    if fields is None or not fields <= {'ref', 'link'} or 'link' not in fields:
        return DEFAULT_TEMPLATE.format(ref=ref, link=link), False
    return template.format(ref=ref, link=link), True


def _event(notification_id, event_type, detail):
    db.session.execute(text("""
        INSERT INTO crn_events (notification_id, event_type, detail)
        VALUES (:n, :t, CAST(:d AS JSONB))
    """), {'n': notification_id, 't': event_type, 'd': json.dumps(detail, default=str)})


def _has_event(notification_id, event_type):
    return db.session.execute(text("""
        SELECT 1 FROM crn_events WHERE notification_id = :n AND event_type = :t LIMIT 1
    """), {'n': notification_id, 't': event_type}).fetchone() is not None


def _contact(code):
    if not code:
        return None
    return db.session.execute(text("""
        SELECT doctor_code, full_name, phone_e164, active FROM crn_contacts WHERE doctor_code = :c
    """), {'c': code.strip().upper()}).mappings().fetchone()


def _fallback(cfg, exclude_code=None):
    """The fallback contact, if configured, active, textable and not the doctor."""
    code = cfg['crn_fallback_code'].upper()
    if not code or code == (exclude_code or '').upper():
        return None
    c = _contact(code)
    return c if c and c['active'] and c['phone_e164'] else None


def _recipients(notification_id):
    return {r['role']: r for r in db.session.execute(text("""
        SELECT * FROM crn_recipients WHERE notification_id = :n
    """), {'n': notification_id}).mappings().fetchall()}


def _send(n, role, contact, cfg, event_type):
    """Text the notification's `role` recipient, creating it (and its link) from
    `contact` on first use. Records the send or the failure. Returns True if sent."""
    rec = _recipients(n['id']).get(role)
    if rec is None:
        token = secrets.token_urlsafe(16)
        rec = db.session.execute(text("""
            INSERT INTO crn_recipients (notification_id, role, contact_code, contact_name, phone_e164,
                                        token_hash, token_enc, expires_at)
            VALUES (:n, :role, :code, :name, :phone, :hash, :enc,
                    NOW() + make_interval(hours => :ttl))
            RETURNING *
        """), {'n': n['id'], 'role': role, 'code': contact['doctor_code'], 'name': contact['full_name'],
               'phone': contact['phone_e164'], 'hash': hashlib.sha256(token.encode()).hexdigest(),
               'enc': encrypt(token), 'ttl': cfg['crn_link_ttl_hours']}).mappings().fetchone()
    else:
        token = decrypt(rec['token_enc'])
        if not token:
            _event(n['id'], 'send_failed', {'role': role, 'intended': event_type,
                                            'error': 'the link could not be decrypted (SECRET_KEY changed?)'})
            return False

    base = (cfg['crn_public_base_url'] or PLACEHOLDER_BASE_URL).rstrip('/')
    body, template_ok = render_sms(cfg['crn_sms_template'], n['ref_code'], f'{base}/n/{token}')
    result = send_sms(cfg['crn_sms_provider'], rec['phone_e164'], body)
    detail = {'role': role, 'contact_code': rec['contact_code'], 'contact_name': rec['contact_name'],
              'phone': rec['phone_e164'], 'provider': result['provider'], 'message_id': result['message_id'],
              'dry_run': result['dry_run'], 'chars': len(body),
              'template_sha256': hashlib.sha256(cfg['crn_sms_template'].encode()).hexdigest()[:16]}
    if not template_ok:
        detail['template_note'] = 'the saved template is invalid, the default text was used'
    if not cfg['crn_public_base_url']:
        detail['link_note'] = 'crn_public_base_url is not set, the link points nowhere'

    if result['ok']:
        db.session.execute(text("""
            UPDATE crn_recipients SET send_count = send_count + 1, last_sent_at = NOW() WHERE id = :id
        """), {'id': rec['id']})
        _event(n['id'], event_type, detail)
        return True
    db.session.execute(text("UPDATE crn_recipients SET fail_count = fail_count + 1 WHERE id = :id"),
                       {'id': rec['id']})
    _event(n['id'], 'send_failed', {**detail, 'intended': event_type, 'error': result['error']})
    return False


def _route(n, cfg):
    order = db.session.execute(text("""
        SELECT ordering_physician_code, ordering_physician FROM hl7_orders
        WHERE accession_number = :acc
        ORDER BY (ordering_physician_code IS NULL), received_at DESC
        LIMIT 1
    """), {'acc': n['accession_number']}).mappings().fetchone() if n['accession_number'] else None
    code = (order['ordering_physician_code'] or '').strip().upper() if order else ''
    contact = _contact(code)

    if not order:
        reason = 'no order found for the accession number'
    elif not code:
        reason = 'the order has no doctor code'
    elif not contact:
        reason = f'doctor code {code} is not in the contact list'
    elif not contact['active']:
        reason = f'contact {code} is inactive'
    elif not contact['phone_e164']:
        reason = f'contact {code} has no usable mobile number'
    else:
        reason = None

    if n['doctor_code'] is None and code:
        db.session.execute(text("""
            UPDATE crn_notifications SET doctor_code = :code, doctor_name = :name WHERE id = :id
        """), {'code': code, 'name': (contact and contact['full_name']) or order['ordering_physician'],
               'id': n['id']})

    if reason is None:
        sent = _send(n, 'doctor', contact, cfg, 'sent')
    else:
        if not _has_event(n['id'], 'doctor_unreachable'):
            _event(n['id'], 'doctor_unreachable', {'reason': reason, 'doctor_code': code or None})
        fallback = _fallback(cfg, exclude_code=code)
        if not fallback:
            db.session.execute(text("UPDATE crn_notifications SET status = 'unroutable' WHERE id = :id"),
                               {'id': n['id']})
            _event(n['id'], 'unroutable', {'reason': reason,
                                           'fallback': 'no fallback contact with a usable mobile is configured'})
            return
        sent = _send(n, 'fallback', fallback, cfg, 'sent_to_fallback')

    if sent:
        db.session.execute(text("""
            UPDATE crn_notifications
            SET status = 'sent', first_sent_at = COALESCE(first_sent_at, NOW())
            WHERE id = :id
        """), {'id': n['id']})
    elif any(r['fail_count'] >= MAX_SEND_FAILURES for r in _recipients(n['id']).values()):
        db.session.execute(text("UPDATE crn_notifications SET status = 'send_failed' WHERE id = :id"),
                           {'id': n['id']})
        _event(n['id'], 'send_gave_up', {'failures': MAX_SEND_FAILURES})


def _escalate(n, cfg):
    elapsed = float(n['elapsed_min'] or 0)
    recs = _recipients(n['id'])
    for role, r in recs.items():   # retry sends that failed
        if r['send_count'] == 0 and r['fail_count'] < MAX_SEND_FAILURES:
            _send(n, role, None, cfg, 'sent' if role == 'doctor' else 'escalated_fallback')

    doctor = recs.get('doctor')
    if doctor and doctor['send_count'] == 1 and doctor['opened_at'] is None and elapsed >= cfg['crn_resend_min']:
        _send(n, 'doctor', None, cfg, 'resent')

    if 'fallback' not in recs and elapsed >= cfg['crn_fallback_min']:
        fallback = _fallback(cfg, exclude_code=n['doctor_code'])
        if fallback:
            _send(n, 'fallback', fallback, cfg, 'escalated_fallback')
        elif not _has_event(n['id'], 'no_fallback'):
            _event(n['id'], 'no_fallback', {'reason': 'no fallback contact with a usable mobile is configured'})

    if n['status'] != 'overdue' and elapsed >= cfg['crn_overdue_min']:
        db.session.execute(text("UPDATE crn_notifications SET status = 'overdue' WHERE id = :id"),
                           {'id': n['id']})
        _event(n['id'], 'overdue', {'minutes_since_first_sms': round(elapsed)})


def _claim(where, params, after_id):
    return db.session.execute(text(f"""
        SELECT n.*, EXTRACT(EPOCH FROM (NOW() - n.first_sent_at)) / 60 AS elapsed_min
        FROM crn_notifications n
        WHERE {where} AND n.id > :after
        ORDER BY n.id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """), {**params, 'after': after_id}).mappings().fetchone()


def run_dispatch():
    """One dispatcher pass. Returns counts of what was handled."""
    cfg = _settings()
    if not cfg['crn_live_since']:
        db.session.rollback()
        return {}
    done = Counter()
    passes = (
        ("n.status = 'detected'", {}, _route, 'routed'),
        ("n.status IN ('sent', 'overdue') AND n.acknowledged_at IS NULL"
         " AND n.first_sent_at > NOW() - make_interval(hours => :ttl)",
         {'ttl': cfg['crn_link_ttl_hours']}, _escalate, 'checked'),
    )
    for where, params, handler, label in passes:
        last = 0
        while True:
            n = _claim(where, params, last)
            if not n:
                db.session.rollback()
                break
            last = n['id']
            try:
                handler(n, cfg)
                db.session.commit()
                done[label] += 1
            except Exception:
                db.session.rollback()
                done['errors'] += 1
                logger.exception(f"[CRN] notification {n['id']} failed in {handler.__name__}")
    return dict(done)
