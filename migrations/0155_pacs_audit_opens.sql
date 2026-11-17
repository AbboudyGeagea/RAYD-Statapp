-- Migration 0155: first opens from the PACS audit trail (to-do item 2, Report 37 / JCI).
--
-- Found 2026-11-17: Carestream's audit, AUDITING.AUDIT_LOG on the PACS Oracle (217.8 M rows
-- since July 2018, never purged), writes a row for every image open, with the login and the
-- second. Opening a study in the Vue PACS workstation writes, within the same second:
--
--   Instances Used / Modify   "Update Tags ... LOCKED ... <LOGIN>"   the study locked for reading
--   Instances Used / Access   "Access to N instances"                the images displayed
--
-- and Vue Motion (the web viewer) writes the Access row too. The nightly ETL
-- (ETL_JOBS/etl_pacs_audit.py) copies only those rows, through the audit's EVENT_DATE index:
-- about 6,000 of the ~150,000 rows written a day. Patient names, patient IDs and the
-- description text are never copied (the description can name the patient).
--
-- The rows cover every study on the PACS, SJH included. Like etl_sticky_notes they are only
-- ever read through a study (Report 37 joins on the study UID and keeps RH's studies), so
-- there is no rh view (CLAUDE.md rule 0).
--
-- settings pacs_audit_since         first day to copy (default 2026-10-01)
--          pacs_audit_copied_from   first day the copy holds, without a gap ...
--          pacs_audit_copied_until  ... up to this moment; Report 37 trusts the copy between them

CREATE TABLE IF NOT EXISTS etl_pacs_audit_opens (
    log_dbid            BIGINT PRIMARY KEY,           -- AUDIT_LOG.LOG_DBID
    event_at            TIMESTAMP NOT NULL,           -- AUDIT_LOG.EVENT_DATE, hospital time
    kind                TEXT NOT NULL CHECK (kind IN ('open', 'lock')),
    user_name           TEXT NOT NULL,                -- the PACS login, lower case (trad@ad)
    source              TEXT,                         -- Vue PACS Client, Carestream Vue Motion
    location            TEXT,                         -- workstation or address
    study_instance_uid  TEXT NOT NULL,
    accession_number    TEXT,
    loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_pacs_audit_opens_study
    ON etl_pacs_audit_opens (study_instance_uid, event_at);
