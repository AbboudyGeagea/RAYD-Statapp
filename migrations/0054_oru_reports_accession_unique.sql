-- Migration 0054: make hl7_oru_reports converge on ONE row per accession.
--
-- Two writers now populate this table:
--   * the live HL7 ORU/MLLP listener (hl7_listener.py) — real-time, may be partial
--     or (at LAUMC) encrypted, and
--   * the RIS report ETL (ETL_JOBS/etl_ris_reports.py) — the authoritative, complete,
--     plain-text body from RIS REPORT.DOCUMENT_PLAIN_TEXT.
--
-- Historically the listener inserted with `ON CONFLICT DO NOTHING` but there was NO
-- unique key on accession_number, so nothing was ever deduped — each ORU (and each
-- future RIS row) created a brand-new row. This migration adds the natural key so both
-- sources UPSERT into the same row instead of duplicating, and tags which writer last
-- touched it.

-- 1. Which writer last populated the row: 'hl7' | 'ris'. Nullable (legacy rows unknown).
ALTER TABLE hl7_oru_reports ADD COLUMN IF NOT EXISTS report_source VARCHAR(8);

-- 2. De-duplicate any existing rows that share an accession, keeping the newest (max id).
--    Empty on a fresh install; this just makes step 3 safe on sites with legacy dup ORUs.
DELETE FROM hl7_oru_reports a
USING hl7_oru_reports b
WHERE a.accession_number IS NOT NULL
  AND a.accession_number = b.accession_number
  AND a.id < b.id;

-- 3. Enforce one row per accession. Postgres treats NULLs as DISTINCT, so ORUs that
--    arrive with no accession still insert normally (they can't be deduped anyway) and
--    never trip the constraint. This index is the arbiter for ON CONFLICT (accession_number).
CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_oru_reports_accession
    ON hl7_oru_reports (accession_number);
