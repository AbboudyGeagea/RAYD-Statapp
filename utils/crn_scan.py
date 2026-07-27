"""
utils/crn_scan.py
────────────────────────────────────────────────────────────────
CRN (Critical Result Notification) detection: finds newly-analyzed critical
results (hl7_oru_analysis.is_critical, written by nlp-worker — read only,
never written here), resolves the referring physician's contact via the
same free-text name-match convention used everywhere else in the app
(routes/referring_intel.py — there is no stable RIS/PACS referring-physician
ID yet), and hands off to utils/crn_dispatcher.create_notification().

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


def _build_message(accession_number, ack_token):
    """
    PLACEHOLDER content — operator has not signed off on real message wording yet
    (see docs/LAUMC_CRN_FIREWALL_REQUEST.md #3). Minimal PHI in the body, per
    LAUMC_SCOPE.md's CRN spec: the link carries the detail, not the message itself.
    """
    return (
        f"RAYD: A radiology report requires your acknowledgment "
        f"(accession {accession_number}). {_ACK_URL_BASE}{ack_token}"
    )


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

    rows = db.session.execute(text("""
        SELECT r.id AS report_id, r.accession_number, a.affirmed_labels,
               TRIM(CONCAT(s.referring_physician_first_name, ' ', s.referring_physician_last_name)) AS physician_name
        FROM hl7_oru_reports r
        JOIN hl7_oru_analysis a ON a.report_id = r.id
        JOIN etl_didb_studies s ON s.accession_number = r.accession_number
        LEFT JOIN crn_notifications n ON n.report_id = r.id
        WHERE a.is_critical = TRUE
          AND n.id IS NULL
          AND s.referring_physician_last_name IS NOT NULL
          AND s.referring_physician_last_name != ''
        ORDER BY r.received_at DESC
        LIMIT 200
    """)).mappings().fetchall()

    created = 0
    for row in rows:
        contact = db.session.execute(text("""
            SELECT * FROM referring_contacts WHERE physician_name = :name AND active = TRUE
        """), {"name": row['physician_name']}).mappings().fetchone()
        if not contact:
            logger.info(
                f"[CRN] report_id={row['report_id']}: no active referring_contacts "
                f"match for '{row['physician_name']}' — skipped"
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
