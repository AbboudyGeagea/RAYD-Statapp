-- Migration 0094: performance indexes for report load time.
--
-- Two sources:
--   1. migrations/optimize.sql (pre-existing in the repo, never actually applied --
--      its filename doesn't match update.sh's `[0-9]*.sql` glob, so it was never
--      picked up by the automated migration runner). Promoting its safe index/
--      cleanup sections here as a real numbered migration. Left OUT: its section 1
--      (etl_orders.patient_dbid type change -- needs a manual row-count check first
--      per its own comment, not safe to blindly automate) and section 3 (materialized
--      view for report_25's report_template query -- deferred, needs the actual SQL
--      substituted and a refresh-schedule decision; only covers part of report_36's
--      chart set anyway since most of its charts don't derive from report_template).
--   2. New finding (2026-07-31, while investigating report_36 load time):
--      etl_didb_studies.study_instance_uid has no index anywhere. It's the join key
--      in every RIS/PPS-anchored query added this session -- both anchor variants of
--      Resident/Radiologist TAT, Modality TAT, and Technician Efficiency, plus Patient
--      Wait Time -- each joining etl_didb_studies back to std_pps-derived data on
--      exactly this column. On a ~614K-row table that's a real cost paid on every one
--      of those queries, every page load.
--
-- CREATE INDEX CONCURRENTLY cannot run inside an explicit transaction block; this
-- file has none (matches optimize.sql's own assumption), and update.sh applies
-- migrations via a plain `psql -f` with no wrapping BEGIN, so each statement here
-- commits independently -- safe to re-run, safe if interrupted partway through.

-- ── New: the actual missing index behind this session's RIS/PPS queries ──────
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_studies_study_instance_uid
    ON etl_didb_studies(study_instance_uid);

-- ── Promoted from optimize.sql section 2 (most-queried join/filter columns) ──
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_studies_patient_db_uid
    ON etl_didb_studies(patient_db_uid);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_studies_study_date
    ON etl_didb_studies(study_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_studies_storing_ae_date
    ON etl_didb_studies(storing_ae, study_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_orders_patient_dbid
    ON etl_orders(patient_dbid);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_orders_scheduled_datetime
    ON etl_orders(scheduled_datetime);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_summary_storage_daily_date
    ON summary_storage_daily(study_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_summary_storage_daily_ae_date
    ON summary_storage_daily(storing_ae, study_date);

-- ── Promoted from optimize.sql section 4 (data-quality fix, not just an index) ──
-- COALESCE(duration_minutes, 15) only substitutes on NULL -- a stored 0 silently
-- passes through as "0 minutes expected", polluting Technician Efficiency's
-- avg_expected_min (report_36) and every other avg-duration figure that reads
-- procedure_duration_map. Setting zero-minute entries to NULL lets the existing
-- COALESCE fallbacks do their job.
UPDATE procedure_duration_map
SET duration_minutes = NULL
WHERE duration_minutes = 0;

-- ── Promoted from optimize.sql section 5 ──────────────────────────────────────
ANALYZE etl_didb_studies;
ANALYZE etl_orders;
ANALYZE summary_storage_daily;
ANALYZE procedure_duration_map;
