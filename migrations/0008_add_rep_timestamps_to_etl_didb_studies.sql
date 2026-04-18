-- Migration 0008: Add reporting timestamp and physician columns to etl_didb_studies.
-- These columns exist in schema.sql but were missing from the production table,
-- causing the TAT report (report_25) query to fail with a ProgrammingError.
-- Safe to run repeatedly — uses ADD COLUMN IF NOT EXISTS throughout.

ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS reading_physician_first_name  TEXT,
    ADD COLUMN IF NOT EXISTS reading_physician_last_name   TEXT,
    ADD COLUMN IF NOT EXISTS reading_physician_id          BIGINT,
    ADD COLUMN IF NOT EXISTS signing_physician_first_name  TEXT,
    ADD COLUMN IF NOT EXISTS signing_physician_last_name   TEXT,
    ADD COLUMN IF NOT EXISTS signing_physician_id          BIGINT,
    ADD COLUMN IF NOT EXISTS study_has_report              BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS rep_prelim_timestamp          TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rep_prelim_signed_by          TEXT,
    ADD COLUMN IF NOT EXISTS rep_transcribed_by            TEXT,
    ADD COLUMN IF NOT EXISTS rep_transcribed_timestamp     TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rep_final_signed_by           TEXT,
    ADD COLUMN IF NOT EXISTS rep_final_timestamp           TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rep_addendum_by               TEXT,
    ADD COLUMN IF NOT EXISTS rep_addendum_timestamp        TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rep_has_addendum              BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_linked_study               BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS patient_location              TEXT;

-- Indexes for TAT queries (safe to create if not exists via IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS idx_study_final_time  ON etl_didb_studies (rep_final_timestamp);
CREATE INDEX IF NOT EXISTS idx_study_prelim_time ON etl_didb_studies (rep_prelim_timestamp);
