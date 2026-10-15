-- Migration 0133: project the referring doctor (PV1-8) into etl_didb_studies.
--
-- Until now the HL7 projector never wrote referring_physician_first_name /
-- referring_physician_last_name, so every report that reads them came up empty on an
-- HL7 install: Referral Trends, Referring Intel, the churn alerts, super-report
-- filters, the daily briefing's top referrers and the CRN name fallback. Several of
-- those filter on a non-empty last name, so a single display string is not enough;
-- the doctor has to arrive as the two parts the reports already expect.
--
-- hl7_orders only kept the joined "First Last" string, which cannot be split back
-- reliably (multi-word first names). It now stores the XCN parts as well
-- (hl7_listener.parse_orm_o01), and utils/hl7_project.py copies them onto the study,
-- preferring the HIS order and falling back to the report's own PV1-8.
--
-- The backfills below bring existing installs level without a replay. Every statement
-- only fills columns that are still NULL, so re-running is harmless.

ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS referring_physician_first_name TEXT;
ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS referring_physician_last_name  TEXT;

-- PV1-8 is XCN: ID^Last^First^Middle. The PV1 segment is cut out of the stored raw
-- message; split_part(..., '|', 8) after the 'PV1|' prefix is PV1-8.
UPDATE hl7_orders o
   SET referring_physician_last_name  = NULLIF(split_part(x.xcn, '^', 2), ''),
       referring_physician_first_name = NULLIF(split_part(x.xcn, '^', 3), '')
  FROM (
        SELECT id,
               split_part(substring(raw_message from '(?:^|[\r\n])PV1\|([^\r\n]*)'), '|', 8) AS xcn
          FROM hl7_orders
         WHERE referring_physician_last_name IS NULL
           AND raw_message IS NOT NULL
       ) x
 WHERE x.id = o.id
   AND COALESCE(x.xcn, '') <> '';

-- Existing study rows, from the most recent order that names a referring doctor.
UPDATE etl_didb_studies s
   SET referring_physician_first_name = o.referring_physician_first_name,
       referring_physician_last_name  = o.referring_physician_last_name
  FROM (
        SELECT DISTINCT ON (accession_number)
               accession_number, referring_physician_first_name, referring_physician_last_name
          FROM hl7_orders
         WHERE accession_number IS NOT NULL
           AND NULLIF(referring_physician_last_name, '') IS NOT NULL
         ORDER BY accession_number, received_at DESC NULLS LAST, id DESC
       ) o
 WHERE o.accession_number = s.accession_number
   AND NULLIF(s.referring_physician_last_name, '') IS NULL;

-- Then from the report's own PV1-8, for studies whose order never reached RAYD.
UPDATE etl_didb_studies s
   SET referring_physician_first_name = r.referring_physician_first_name,
       referring_physician_last_name  = r.referring_physician_last_name
  FROM (
        SELECT DISTINCT ON (accession_number)
               accession_number, referring_physician_first_name, referring_physician_last_name
          FROM hl7_oru_reports
         WHERE accession_number IS NOT NULL
           AND NULLIF(referring_physician_last_name, '') IS NOT NULL
         ORDER BY accession_number, result_datetime DESC NULLS LAST, id DESC
       ) r
 WHERE r.accession_number = s.accession_number
   AND NULLIF(s.referring_physician_last_name, '') IS NULL;
