-- Migration 0182: the PACS audit copy read by kind and day (item 14b, 2026-11-23).
-- *** LAUMC BRANCH ONLY ***
--
-- Report 32's "Reports per Radiologist per Day" (utils/tat_bands.per_day) counts the approvals
-- and wet reads of a range of days. The copy is indexed by study (0155) only, so that read went
-- through every event the copy holds (opens and locks are most of them, ~7,000 a day). This
-- index lets it read the approvals and wet reads of those days alone.

CREATE INDEX IF NOT EXISTS idx_etl_pacs_audit_events_kind_at
    ON etl_pacs_audit_events (kind, event_at);
