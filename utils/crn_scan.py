"""
utils/crn_scan.py
────────────────────────────────────────────────────────────────
CRN (Critical Result Notification) detection: finds newly-analyzed critical
results (hl7_oru_analysis.is_critical, written by nlp-worker — read only,
never written here) and resolves the referring physician's contact.

Resolution order (operator provided a real HL7 sample + the RIS join,
2026-07-27 — see migrations/0079_referring_physician_resource_key.sql):
  1. hl7_orders.referring_physician_code (captured from PV1-8) matched
     against referring_contacts.resource_id_key — reliable, ID-based,
     auto-filled from std_resources_ris by utils/referring_contacts_sync.py.
  2. Falls back to the free-text name match already used everywhere else in
     the app (routes/referring_intel.py) when no code is on the order, or
     the code doesn't resolve to a known contact — accession numbers from
     before PV1-8 parsing existed will only ever have this path.

Dormant unless settings.crn_enabled = 'true' (see utils/crn_dispatcher.py) —
called from app.py's scheduler on an interval, but a fast no-op return
whenever the flag is off so it stays fully inert on a live system until
explicitly turned on.
"""
import logging
from sqlalchemy import text
from db import db
from utils.crn_dispatcher import create_notification, escalate_due_notifications, _crn_live

logger = logging.getLogger("CRN")

_ACK_URL_BASE = "/crn/ack/"


def _resolve_contact(oru_code, oru_first, oru_last, orm_code, pacs_name):
    """
    Layered resolution, first match wins (migration 0101, operator instruction
    2026-08-01):
      1. hl7_oru_reports.referring_physician_code (ORU-native, via a PV1 segment the
         operator is adding to the live ORU feed) -- the primary path once it exists.
      2. hl7_orders.referring_physician_code (existing ORM-based path, migration 0079)
         -- kept as a fallback in case the R2I ORM feed ever goes live; harmless no-op
         today since that feed isn't flowing and this is always None.
      3. Free-text name match built from the ORU's own first+last (new columns) --
         works even when neither code is available, as long as the ORU carries PV1.
      4. Existing PACS-side fallback (etl_didb_studies.referring_physician_first_name/
         last_name) -- today's only working path, kept last.
    """
    for code in (oru_code, orm_code):
        if code and code.isdigit():
            contact = db.session.execute(text("""
                SELECT * FROM referring_contacts WHERE resource_id_key = :key AND active = TRUE
            """), {"key": int(code)}).mappings().fetchone()
            if contact:
                return contact

    if oru_first and oru_last:
        name = f"{oru_first} {oru_last}".strip()
        contact = db.session.execute(text("""
            SELECT * FROM referring_contacts WHERE physician_name = :name AND active = TRUE
        """), {"name": name}).mappings().fetchone()
        if contact:
            return contact

    if pacs_name:
        return db.session.execute(text("""
            SELECT * FROM referring_contacts WHERE physician_name = :name AND active = TRUE
        """), {"name": pacs_name}).mappings().fetchone()
    return None


_FALLBACK_TEMPLATE = "RAYD: A radiology report requires your acknowledgment (accession {accession}). {ack_url}"


def _build_message(accession_number, ack_token):
    """
    Builds the notification text from the admin-editable settings.crn_message_template
    (migration 0101) -- replaces the old hardcoded placeholder string (operator
    instruction, 2026-08-01; see routes/referring_contacts_admin.py for the editor).
    Merge fields: {accession}, {ack_url} -- deliberately no patient-identifying fields,
    per LAUMC_SCOPE.md's CRN spec: the link carries the detail, not the message itself
    (see routes/crn_ack.py).
    """
    row = db.session.execute(
        text("SELECT value FROM settings WHERE key = 'crn_message_template'")
    ).fetchone()
    template = (row[0] if row and row[0] else _FALLBACK_TEMPLATE)
    ack_url = f"{_ACK_URL_BASE}{ack_token}"
    try:
        return template.format(accession=accession_number, ack_url=ack_url)
    except (KeyError, IndexError):
        # Admin-entered template has a bad/unknown placeholder -- fail safe to the
        # known-good default rather than error out and skip the notification entirely.
        logger.warning("[CRN] crn_message_template has an invalid placeholder, using fallback")
        return _FALLBACK_TEMPLATE.format(accession=accession_number, ack_url=ack_url)


def scan_for_new_critical_results():
    """
    Create a crn_notifications row (+ first send attempt) for any newly-analyzed
    critical result that doesn't have one yet and has a matching active
    referring_contacts row. Reports with no contact on file are logged and skipped
    (not an error — the referring-contacts list is manually curated and will always
    lag real PACS data).
    """
    if not _crn_live():
        return 0

    # etl_didb_studies is a LEFT JOIN (not INNER): a critical result should still be
    # resolvable via the ORU-native PV1 fields (migration 0101) even when no matching
    # PACS study exists yet -- requiring `s` unconditionally would silently drop exactly
    # the reports the new ORU-side resolution path exists to catch.
    rows = db.session.execute(text("""
        SELECT r.id AS report_id, r.accession_number, a.affirmed_labels,
               r.referring_physician_code AS oru_code,
               r.referring_physician_first_name AS oru_first,
               r.referring_physician_last_name AS oru_last,
               o.referring_physician_code AS orm_code,
               NULLIF(TRIM(CONCAT(s.referring_physician_first_name, ' ', s.referring_physician_last_name)), '') AS pacs_name
        FROM hl7_oru_reports r
        JOIN hl7_oru_analysis a ON a.report_id = r.id
        LEFT JOIN etl_didb_studies s ON s.accession_number = r.accession_number
        LEFT JOIN LATERAL (
            SELECT referring_physician_code FROM hl7_orders
            WHERE accession_number = r.accession_number AND referring_physician_code IS NOT NULL
            ORDER BY received_at DESC LIMIT 1
        ) o ON true
        LEFT JOIN crn_notifications n ON n.report_id = r.id
        WHERE a.is_critical = TRUE
          AND n.id IS NULL
          AND (
              r.referring_physician_code IS NOT NULL
              OR (r.referring_physician_first_name IS NOT NULL AND r.referring_physician_last_name IS NOT NULL)
              OR o.referring_physician_code IS NOT NULL
              OR (s.referring_physician_last_name IS NOT NULL AND s.referring_physician_last_name != '')
          )
        ORDER BY r.received_at DESC
        LIMIT 200
    """)).mappings().fetchall()

    created = 0
    for row in rows:
        contact = _resolve_contact(
            row['oru_code'], row['oru_first'], row['oru_last'],
            row['orm_code'], row['pacs_name'],
        )
        if not contact:
            logger.info(
                f"[CRN] report_id={row['report_id']}: no active referring_contacts match "
                f"(oru_code={row['oru_code']!r}, orm_code={row['orm_code']!r}, "
                f"oru_name={row['oru_first']!r} {row['oru_last']!r}, pacs_name={row['pacs_name']!r}) — skipped"
            )
            continue

        nid = create_notification(
            report_id=row['report_id'],
            accession_number=row['accession_number'],
            contact_row=contact,
            critical_keywords=list(row['affirmed_labels'] or []),
            message_builder=lambda token, acc=row['accession_number']: _build_message(acc, token),
        )
        if nid:
            created += 1
    return created


def escalate():
    if not _crn_live():
        return 0
    return escalate_due_notifications(
        lambda notif_row: _build_message(notif_row['accession_number'], notif_row['ack_token'])
    )
