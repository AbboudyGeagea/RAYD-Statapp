-- Migration 0074: add rep_study_last_composed_by/_ts to etl_didb_studies.
--
-- Operator, 2026-07-27: this PACS install is older/sparser than others worked with
-- this session -- rep_final_timestamp/rep_final_signed_by are unreliably populated
-- here (already confirmed: migration 0070's whole reason for existing). The fields
-- that DO carry real data on this install are REP_STUDY_LAST_COMPOSED_BY (e.g.
-- "abdallah.noufaily@ad", "perla.audi@sj" -- login-style identifier, not a display
-- name) and REP_STUDY_LAST_COMPOSED_TS. Raw values captured as-is; resolving to a
-- real name and radiologist-vs-resident role is explicitly deferred by the operator
-- ("i will find a way, for now let's proceed with this change").
--
-- Populated by ETL_JOBS/etl_didb_studies.py going forward; existing rows stay NULL
-- here until the next Studies ETL run re-syncs them.

ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS rep_study_last_composed_by TEXT,
    ADD COLUMN IF NOT EXISTS rep_study_last_composed_ts TIMESTAMP;
