-- Migration 0076: fix rep_study_last_composed_ts column type (VARCHAR -> TIMESTAMP).
--
-- Migration 0074's `ADD COLUMN IF NOT EXISTS rep_study_last_composed_ts TIMESTAMP`
-- silently no-op'd: by the time it got to run, a column with that exact name already
-- existed as VARCHAR -- almost certainly auto-created (as a generic/safe VARCHAR
-- type) by the upsert helper during one of the concurrent ETL attempts from the
-- migration lock-pileup incident on 2026-07-27, before 0074 itself had a chance to
-- run first. IF NOT EXISTS only checks the column NAME, never the type, so the
-- wrong type stuck. Broke report_25 live: "COALESCE types character varying and
-- timestamp without time zone cannot be matched" (migration 0075's query).
--
-- rep_study_last_composed_by is also VARCHAR rather than the TEXT migration 0074
-- specified, but that's functionally equivalent in Postgres (VARCHAR with no length
-- limit behaves the same as TEXT) and caused no error -- left as-is, not worth a
-- disruptive ALTER for a purely cosmetic type difference.
--
-- Existing values are Python datetime str() output ('YYYY-MM-DD HH:MM:SS[.ffffff]'),
-- directly castable to timestamp. NULLIF guards against any empty-string value a
-- bare cast would otherwise reject.

ALTER TABLE etl_didb_studies
    ALTER COLUMN rep_study_last_composed_ts TYPE TIMESTAMP
    USING NULLIF(TRIM(rep_study_last_composed_ts), '')::timestamp;
