"""
utils/referring_contacts_sync.py
────────────────────────────────────────────────────────────────
Auto-fills referring_contacts from RIS's std_resources_ris using the
referring-physician code captured from HL7 PV1-8 (see hl7_listener.py and
migrations/0079_referring_physician_resource_key.sql).

Demand-driven: only syncs codes that have actually shown up in hl7_orders,
not a bulk import of every std_resources_ris row — that table holds every
RIS resource (radiologists, technologists, referring physicians alike) and
resource_role_key isn't resolved yet (see docs/LAUMC_NEXT_SESSION.md,
blocked-on-vendor #3), so there's no clean way to filter to "referring
physicians only" from std_resources_ris directly. Only codes actually seen
on a real order are trustworthy as "this is someone we need to notify."

Auto-filled rows are flagged (referring_contacts.auto_filled = TRUE) so the
admin UI can visually distinguish "resolved automatically from RIS, please
confirm the preferred channel" from a manually-entered contact.
preferred_channel defaults to 'email' when RIS has an email on file, else
'sms' when only a phone exists — a starting point for the admin to review,
never assumed final; CRN needs an explicit human channel choice regardless.
"""
import logging
from sqlalchemy import text
from db import db

logger = logging.getLogger("CRN")


def sync_referring_contacts_from_ris():
    """
    For every distinct referring_physician_code seen in hl7_orders that isn't
    already linked to a referring_contacts row, resolve it against
    std_resources_ris and create (or refresh the phone/email on) the contact.
    Safe to call repeatedly/on a schedule. Returns the number of rows
    created or updated.
    """
    codes = db.session.execute(text("""
        SELECT DISTINCT referring_physician_code
        FROM hl7_orders
        WHERE referring_physician_code IS NOT NULL
          AND referring_physician_code ~ '^[0-9]+$'
    """)).fetchall()

    synced = 0
    for (code,) in codes:
        resource = db.session.execute(text("""
            SELECT resource_id_key, last_name, first_name, primary_email_address,
                   secondary_email_address, mobile_phone_number
            FROM std_resources_ris
            WHERE resource_id_key = :key AND COALESCE(active, TRUE) = TRUE
        """), {"key": int(code)}).mappings().fetchone()
        if not resource:
            continue
        email = resource['primary_email_address'] or resource['secondary_email_address']
        phone = resource['mobile_phone_number']
        if not email and not phone:
            continue  # nothing usable to notify on

        existing = db.session.execute(text("""
            SELECT id FROM referring_contacts WHERE resource_id_key = :key
        """), {"key": resource['resource_id_key']}).mappings().fetchone()

        if existing:
            db.session.execute(text("""
                UPDATE referring_contacts
                SET email = COALESCE(:email, email), phone = COALESCE(:phone, phone),
                    updated_at = NOW()
                WHERE id = :id
            """), {"email": email, "phone": phone, "id": existing['id']})
            synced += 1
            continue

        name = ' '.join(filter(None, [resource['first_name'], resource['last_name']])).strip() \
            or f"RIS Resource {resource['resource_id_key']}"
        channel = 'email' if email else 'sms'

        result = db.session.execute(text("""
            INSERT INTO referring_contacts
                (physician_name, email, phone, preferred_channel, resource_id_key, auto_filled)
            VALUES (:name, :email, :phone, :channel, :key, TRUE)
            ON CONFLICT (physician_name) DO NOTHING
        """), {"name": name, "email": email, "phone": phone, "channel": channel, "key": resource['resource_id_key']})

        if result.rowcount:
            synced += 1
        else:
            logger.info(
                f"[CRN] referring_contacts auto-fill: name collision for '{name}' "
                f"(resource_id_key={resource['resource_id_key']}) — an existing manual "
                f"contact already uses this name, skipped, needs manual resolution"
            )

    db.session.commit()
    return synced
