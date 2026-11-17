-- Migration 0156: the PACS audit copy holds the signature events too (items 14A and 14B,
-- radiologist turnaround from the PACS, operator 2026-11-17).
--
-- Migration 0155 copied the opens (images displayed, study locked) for Report 37's first
-- open. The same audit (AUDITING.AUDIT_LOG) records every report status change with its
-- login and second; proven on 2026-11-17 (16 Nov: 458 studies approved in the audit against
-- 455 in the PACS studies view, 13 radiologists both ways):
--
--   approve   status -> PENDING APPROVED, or "ApprovedSign Complete(d)": the radiologist's
--             approval (at LAUMC approved is the final signature; the report is held 900 s,
--             then APPROVED, written by the PACS with no login)
--   wetread   status -> WETREAD: the resident's wet read (two rows in three have no login;
--             the ETL takes it from the tags, never copying the description)
--
-- So the table is renamed for what it holds, and the two kinds are allowed. The nightly ETL
-- reads the copy again from settings.pacs_audit_since once (settings.pacs_audit_version), so
-- the days already copied get their signatures. Report 37 reads opens and locks only.

ALTER TABLE IF EXISTS etl_pacs_audit_opens RENAME TO etl_pacs_audit_events;
ALTER INDEX IF EXISTS idx_etl_pacs_audit_opens_study RENAME TO idx_etl_pacs_audit_events_study;

ALTER TABLE etl_pacs_audit_events DROP CONSTRAINT IF EXISTS etl_pacs_audit_opens_kind_check;
ALTER TABLE etl_pacs_audit_events DROP CONSTRAINT IF EXISTS etl_pacs_audit_events_kind_check;
ALTER TABLE etl_pacs_audit_events
    ADD CONSTRAINT etl_pacs_audit_events_kind_check CHECK (kind IN ('open', 'lock', 'approve', 'wetread'));
