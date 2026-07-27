"""
utils/crn_dispatcher.py
────────────────────────────────────────────────────────────────
CRN (Critical Result Notification) channel dispatcher — channel-agnostic
send + escalation + acknowledgment logic. See
migrations/0078_add_crn_notifications.sql and docs/LAUMC_SCOPE.md #10.

LIVE SENDING IS OFF BY DEFAULT. Until settings.crn_enabled = 'true' AND real
provider credentials exist, every send here is a DRY RUN: it logs the
attempt to crn_notification_attempts (dry_run=True, success=NULL) and never
contacts an external provider. There is no live SMTP/SMS/WhatsApp credential
configured yet (see docs/LAUMC_CRN_FIREWALL_REQUEST.md), and this touches
real physicians and real patient-adjacent data, so nothing here fires for
real until that is explicitly turned on with tested credentials.

All timestamp math is done in SQL (NOW() + INTERVAL ...), never in Python
datetime — the app and Postgres containers must never be trusted to agree
on wall-clock time independently of each other.
"""
import logging
import secrets
from sqlalchemy import text
from db import db

logger = logging.getLogger("CRN")

ACK_TOKEN_TTL_HOURS = 48          # operator instruction, 2026-07-27: one-time use, 48h expiry
ESCALATION_INTERVAL_MINUTES = 30  # operator instruction, 2026-07-27
ESCALATION_ORDER = ['sms', 'whatsapp', 'email']  # order to try the physician's OTHER channels in


def _crn_live():
    row = db.session.execute(
        text("SELECT value FROM settings WHERE key = 'crn_enabled'")
    ).fetchone()
    return bool(row and row[0].lower() == 'true')


def _generate_ack_token():
    return secrets.token_urlsafe(32)


def _available_channels(contact):
    """Channels this contact actually has a value for."""
    avail = []
    if contact.get('email'):
        avail.append('email')
    if contact.get('phone'):
        avail.append('sms')
    if contact.get('whatsapp_number'):
        avail.append('whatsapp')
    return avail


def _next_channel(contact, channels_tried):
    """
    Pick the next untried channel for this contact: their stated preference first,
    then the rest of ESCALATION_ORDER — skipping anything with no contact value on
    file or already attempted. Returns None once every available channel is tried.
    """
    avail = _available_channels(contact)
    preferred = contact.get('preferred_channel')
    candidates = ([preferred] if preferred else []) + [c for c in ESCALATION_ORDER if c != preferred]
    for c in candidates:
        if c in avail and c not in channels_tried:
            return c
    return None


def _send_on_channel(channel, contact, message):
    """
    Contact the provider for one channel. Returns (success, response_or_error).
    DRY RUN unless crn_enabled — see module docstring. Real provider calls land here
    once credentials exist (docs/LAUMC_CRN_FIREWALL_REQUEST.md); deliberately
    unimplemented until then — dead code nobody can test end-to-end helps no one.
    """
    if not _crn_live():
        return None, "DRY RUN — crn_enabled is false, no provider contacted"
    if channel == 'email':
        return None, "Email provider not yet configured"
    if channel == 'sms':
        return None, "SMS provider not yet configured"
    if channel == 'whatsapp':
        return None, "WhatsApp provider not yet configured"
    return None, f"Unknown channel: {channel}"


def _attempt(notification_id, channel, contact, message):
    success, response = _send_on_channel(channel, contact, message)
    db.session.execute(text("""
        INSERT INTO crn_notification_attempts
            (notification_id, channel, dry_run, success, provider_response)
        VALUES (:nid, :channel, :dry_run, :success, :response)
    """), {
        "nid": notification_id, "channel": channel,
        "dry_run": not _crn_live(), "success": success, "response": response,
    })


def create_notification(report_id, accession_number, contact_row, critical_keywords, message_builder):
    """
    Create a crn_notifications row for a newly-detected critical result and send the
    first attempt on the contact's preferred (or first-available) channel.

    message_builder(ack_token) -> message text — the caller builds the actual message
    content (this module doesn't know about accessions/patients/report wording).

    Returns the new notification id, or None if a notification for this report_id
    already exists (UNIQUE constraint — safe to call repeatedly from a poll loop) or
    the contact has no usable channel at all.
    """
    contact = dict(contact_row)
    channel = _next_channel(contact, [])
    if not channel:
        logger.warning(
            f"[CRN] report_id={report_id}: contact '{contact.get('physician_name')}' "
            f"has no usable channel on file — skipped"
        )
        return None

    token = _generate_ack_token()
    message = message_builder(token)

    result = db.session.execute(text("""
        INSERT INTO crn_notifications
            (report_id, accession_number, referring_contact_id, critical_keywords,
             ack_token, ack_token_expires_at, status, current_channel, channels_tried,
             next_escalation_at)
        VALUES
            (:report_id, :accession_number, :contact_id, :keywords,
             :token, NOW() + (:ttl || ' hours')::interval, 'pending', :channel,
             ARRAY[:channel]::text[], NOW() + (:esc || ' minutes')::interval)
        ON CONFLICT (report_id) DO NOTHING
        RETURNING id
    """), {
        "report_id": report_id, "accession_number": accession_number,
        "contact_id": contact.get('id'), "keywords": critical_keywords,
        "token": token, "ttl": ACK_TOKEN_TTL_HOURS, "channel": channel,
        "esc": ESCALATION_INTERVAL_MINUTES,
    }).fetchone()

    if not result:
        return None  # already existed — another scan tick beat us to it

    notification_id = result[0]
    _attempt(notification_id, channel, contact, message)
    db.session.commit()
    return notification_id


def escalate_due_notifications(message_builder):
    """
    Find pending notifications past their next_escalation_at and try the next untried
    channel for each. message_builder(notification_row) -> message text.

    Once a contact's channels are all exhausted, marks the notification 'exhausted'
    (operator instruction, 2026-07-27: resend on the physician's other channels only —
    no fixed backstop contact for v1) and stops escalating it.
    """
    due = db.session.execute(text("""
        SELECT n.*, c.physician_name, c.email, c.phone, c.whatsapp_number, c.preferred_channel
        FROM crn_notifications n
        JOIN referring_contacts c ON c.id = n.referring_contact_id
        WHERE n.status = 'pending' AND n.next_escalation_at <= NOW()
    """)).mappings().fetchall()

    for row in due:
        contact = dict(row)
        channel = _next_channel(contact, row['channels_tried'])
        if not channel:
            db.session.execute(text("""
                UPDATE crn_notifications
                SET status = 'exhausted', next_escalation_at = NULL, updated_at = NOW()
                WHERE id = :id
            """), {"id": row['id']})
            logger.warning(f"[CRN] notification {row['id']}: all channels exhausted, unacknowledged")
            continue

        message = message_builder(row)
        _attempt(row['id'], channel, contact, message)
        db.session.execute(text("""
            UPDATE crn_notifications
            SET current_channel = :channel,
                channels_tried = array_append(channels_tried, :channel),
                next_escalation_at = NOW() + (:mins || ' minutes')::interval,
                updated_at = NOW()
            WHERE id = :id
        """), {"channel": channel, "id": row['id'], "mins": ESCALATION_INTERVAL_MINUTES})

    db.session.commit()
    return len(due)


def acknowledge(token, ip=None):
    """
    Mark a notification acknowledged by token. Returns one of:
    'ok' / 'invalid' / 'expired' / 'already_acknowledged' — the ack landing page
    (routes/crn_ack.py) uses this to decide what to show. Idempotent: re-visiting an
    already-acknowledged link is safe.
    """
    row = db.session.execute(text("""
        SELECT id, status, (ack_token_expires_at < NOW()) AS expired
        FROM crn_notifications WHERE ack_token = :token
    """), {"token": token}).mappings().fetchone()
    if not row:
        return 'invalid'
    if row['status'] == 'acknowledged':
        return 'already_acknowledged'
    if row['expired']:
        return 'expired'
    db.session.execute(text("""
        UPDATE crn_notifications
        SET status = 'acknowledged', acknowledged_at = NOW(), acknowledged_ip = :ip,
            next_escalation_at = NULL, updated_at = NOW()
        WHERE id = :id
    """), {"id": row['id'], "ip": ip})
    db.session.commit()
    return 'ok'
