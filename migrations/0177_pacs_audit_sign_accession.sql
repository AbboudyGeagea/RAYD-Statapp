-- Migration 0177: an index for BRAYN reading the PACS's signatures by accession (LAUMC to-do item
-- 17, 2026-11-20). *** LAUMC BRANCH ONLY ***
--
-- Every 15 minutes BRAYN's sweep gives the RIS studies not yet approved their PACS wet read and
-- approval from the audit trail copy (utils/brayn_shadow.link_pacs_signatures), looked up by the
-- PACS accession. The copy is indexed by study UID only; this partial index covers the two
-- signature kinds, a small share of the opens and locks.

CREATE INDEX IF NOT EXISTS idx_etl_pacs_audit_events_sign_accession
    ON etl_pacs_audit_events (accession_number, kind, event_at)
    WHERE kind IN ('wetread', 'approve');
