-- Migration 0143: what the JCI urgent read measure needs from PACS, copied by the nightly
-- ETL so that all the work happens in Postgres (operator, 2026-11-05: nothing may keep
-- reading the PACS server except the ETL).
--
-- The measure runs from a resident opening the images to the resident's sticky note, on
-- urgent ER and inpatient cases. PACS keeps no log of opens that RAYD can read. Three
-- columns on DIDB_STUDIES hold the latest lock:
--
--   open    STUDY_LOCKED = LOCKED, STUDY_LOCK_TIME = the second of the open,
--           REP_LAST_ACCESSED_BY = the login, LAST_ACCESS_TIME = the same second
--   close   STUDY_LOCKED = UNLOCKED, STUDY_LOCK_TIME moves on, the rest stay
--   report  STUDY_LOCK_TIME is rewritten about 15 minutes after the last composed report
--
-- They only hold the latest value, so one copy a night does NOT give the first open of
-- every study: it gives the last open before the ETL runs. study_access_history keeps
-- each night's values so they can be compared with the notes, on the studies not yet
-- reported or reopened by then. See ETL_JOBS/etl_sticky_notes.py.
--
-- Note text (DATA) and the author's display name are deliberately not copied: the text
-- is clinical free text and only the time and the login are needed.

ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS study_locked         TEXT,
    ADD COLUMN IF NOT EXISTS study_lock_time      TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rep_last_accessed_by TEXT;

CREATE TABLE IF NOT EXISTS etl_sticky_notes (
    data_db_uid        BIGINT PRIMARY KEY,
    study_db_uid       BIGINT NOT NULL,
    user_name          TEXT,
    date_created       TIMESTAMP,
    last_updated_by    TEXT,
    last_update_time   TIMESTAMP,
    delete_indication  TEXT,
    data_len           BIGINT,        -- text length + 1; 1 is a blank note
    last_update        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_sticky_notes_study ON etl_sticky_notes (study_db_uid, date_created);

CREATE TABLE IF NOT EXISTS study_access_history (
    study_db_uid         BIGINT NOT NULL,
    captured_at          TIMESTAMP NOT NULL,
    study_locked         TEXT,
    study_lock_time      TIMESTAMP,
    rep_last_accessed_by TEXT,
    last_accessed_time   TIMESTAMP,
    PRIMARY KEY (study_db_uid, captured_at)
);
