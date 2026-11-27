-- Migration 0075: first opens from the PACS audit trail (Report 36 TAT, CHN).
--
-- CHN counts radiologist TAT from the first open to the final report, the same "first open"
-- as LAUMC's urgent emergent report (Report 37 there). Carestream's audit trail,
-- AUDITING.AUDIT_LOG on the PACS Oracle, writes a row for every image open, with the login
-- and the second. Checked on CHN 2026-11-26: the trail runs from 16 Jul 2024 (before CHN's
-- go-live), and a day holds ~800 image-open rows ("Instances Used / Access", Vue PACS Client
-- and Vue Motion, every one with a login) and ~200 study-lock rows ("Instances Used /
-- Modify", Vue PACS Client, "LOCKED").
--
-- The ETL (ETL_JOBS/etl_pacs_audit.py) copies only those two kinds, with every sync:
--   open   images displayed
--   lock   the study locked for reading in the workstation
-- Patient names, patient IDs and the description text are never copied.
--
-- Same table as LAUMC (migrations 0155/0156 there), so later ports line up; CHN keeps only
-- open and lock.
--
-- settings pacs_audit_since         first day to copy (default: the go-live date)
--          pacs_audit_copied_from   first day the copy holds, without a gap ...
--          pacs_audit_copied_until  ... up to this moment; reports trust the copy between them

CREATE TABLE IF NOT EXISTS etl_pacs_audit_events (
    log_dbid            BIGINT PRIMARY KEY,           -- AUDIT_LOG.LOG_DBID
    event_at            TIMESTAMP NOT NULL,           -- AUDIT_LOG.EVENT_DATE, PACS server time
    kind                TEXT NOT NULL CHECK (kind IN ('open', 'lock')),
    user_name           TEXT NOT NULL,                -- the PACS login, lower case
    source              TEXT,                         -- Vue PACS Client, Vue Motion
    location            TEXT,                         -- workstation or address
    study_instance_uid  TEXT NOT NULL,
    accession_number    TEXT,
    loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_pacs_audit_events_study
    ON etl_pacs_audit_events (study_instance_uid, event_at);
