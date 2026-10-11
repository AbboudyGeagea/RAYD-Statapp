"""
utils/crn_contacts_ris.py
─────────────────────────
LAUMC: fills crn_contacts from the RIS. The referring-doctor code in PV1-8 is the
RIS resource number (std_resources_ris.resource_id_key, Phase 13, migration 0079),
which carries the doctor's name, mobile and email.

Only codes that appear on a report or an order are synced, not every RIS resource
(that table also holds radiologists and technologists). A contact the sync wrote is
marked updated_by = 'RIS' and kept in step with the RIS. A contact added or edited
by hand, or by a CSV import, is never overwritten: a corrected number stays
corrected.

sync_all() runs every 15 minutes (app.py); the dispatcher calls sync_code() when a
CRN names a doctor who is not in the list yet. Neither commits: the caller does.
"""
from sqlalchemy import text

from db import db
from utils.crn_contacts import normalize_phone

RIS_USER = 'RIS'

_SEEN_CODES = """
    SELECT TRIM(referring_physician_code) AS code FROM hl7_oru_reports
    WHERE TRIM(referring_physician_code) ~ '^[0-9]{1,18}$'
    UNION
    SELECT TRIM(referring_physician_code) FROM hl7_orders
    WHERE TRIM(referring_physician_code) ~ '^[0-9]{1,18}$'
"""

# RIS details for the codes in {codes}, only where the contact is missing or is the
# sync's own and the RIS has changed.
_DUE_SQL = """
    SELECT s.* FROM (
        SELECT c.code,
               NULLIF(CONCAT_WS(' ', NULLIF(TRIM(r.first_name), ''), NULLIF(TRIM(r.last_name), '')), '') AS full_name,
               NULLIF(TRIM(r.mobile_phone_number), '') AS phone_raw,
               LOWER(COALESCE(NULLIF(TRIM(r.primary_email_address), ''),
                              NULLIF(TRIM(r.secondary_email_address), ''))) AS email
        FROM ({codes}) c
        JOIN std_resources_ris r ON r.resource_id_key = c.code::bigint AND COALESCE(r.active, TRUE)
    ) s
    LEFT JOIN crn_contacts k ON k.doctor_code = s.code
    WHERE k.doctor_code IS NULL
       OR (k.updated_by = :ris
           AND (k.full_name, k.phone_raw, k.email) IS DISTINCT FROM (s.full_name, s.phone_raw, s.email))
"""


def _sync(codes_sql, params):
    written = 0
    for r in db.session.execute(text(_DUE_SQL.format(codes=codes_sql)),
                                {**params, 'ris': RIS_USER}).mappings().fetchall():
        phone_e164, _ = normalize_phone(r['phone_raw'])
        written += db.session.execute(text("""
            INSERT INTO crn_contacts (doctor_code, full_name, phone_raw, phone_e164, email, updated_at, updated_by)
            VALUES (:code, :name, :raw, :e164, :email, NOW(), :ris)
            ON CONFLICT (doctor_code) DO UPDATE SET
                full_name = EXCLUDED.full_name, phone_raw = EXCLUDED.phone_raw,
                phone_e164 = EXCLUDED.phone_e164, email = EXCLUDED.email, updated_at = NOW()
            WHERE crn_contacts.updated_by = :ris
        """), {'code': r['code'], 'name': r['full_name'], 'raw': r['phone_raw'], 'e164': phone_e164,
               'email': r['email'], 'ris': RIS_USER}).rowcount
    return written


def sync_code(code):
    """Sync one doctor code from the RIS. Returns 1 when a contact was written, else 0."""
    code = (code or '').strip()
    if not code.isdigit() or len(code) > 18:
        return 0
    return _sync('SELECT CAST(:code AS TEXT) AS code', {'code': code})


def sync_all():
    """Sync every referring-doctor code seen on a report or an order. Returns the
    number of contacts written."""
    return _sync(_SEEN_CODES, {})
