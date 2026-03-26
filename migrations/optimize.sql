-- ============================================================
-- RAYD Performance Optimization Migration
-- Run once on PostgreSQL. All operations are safe to re-run
-- (CONCURRENTLY indexes, IF NOT EXISTS guards).
-- ============================================================

-- ── 1. Fix join key type mismatch ────────────────────────────
-- etl_orders.patient_dbid is TEXT; etl_patient_view.patient_db_uid is BIGINT.
-- This forces a cast on every join, preventing index use on the patient side.
-- SAFE only if all existing patient_dbid values are pure integers.
-- Run the check first; if it returns 0 rows, the ALTER is safe.

-- CHECK (run first):
-- SELECT COUNT(*) FROM etl_orders WHERE patient_dbid !~ '^\d+$' AND patient_dbid IS NOT NULL;

-- If count = 0, run:
-- ALTER TABLE etl_orders ALTER COLUMN patient_dbid TYPE BIGINT USING patient_dbid::BIGINT;
-- After this change, remove the ::TEXT casts in report_22.py and report_25.py joins.


-- ── 2. Indexes for the most-queried join columns ──────────────
-- These are CONCURRENT so they don't lock the table during build.

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


-- ── 3. Materialized view for Report 25 (Gold Standard) ───────
-- The report_25 SQL is stored in report_template.report_sql_query (report_id=25).
-- Replace <REPORT_25_SQL> below with that query before running.
-- Refresh nightly via pg_cron or the ETL job.

-- CREATE MATERIALIZED VIEW IF NOT EXISTS mv_report_25_base AS
-- <REPORT_25_SQL>
-- WITH NO DATA;

-- CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_report_25_study_db_uid
--     ON mv_report_25_base(study_db_uid);

-- Refresh command (add to ETL job or pg_cron):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_report_25_base;

-- If you use pg_cron:
-- SELECT cron.schedule('refresh-report-25', '0 2 * * *',
--     'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_report_25_base');


-- ── 4. Fix procedure_duration_map zero-minute entries ─────────
-- COALESCE(duration_minutes, 0) silently pollutes averages with zeros.
-- Set zero-minute entries to NULL so they are excluded from AVG() automatically.

UPDATE procedure_duration_map
SET duration_minutes = NULL
WHERE duration_minutes = 0;

-- After this, the COALESCE fallback in report_27.py is unnecessary
-- (already removed in the code change). Confirm with:
-- SELECT COUNT(*) FROM procedure_duration_map WHERE duration_minutes IS NULL;


-- ── 5. Analyze tables after index creation ───────────────────
ANALYZE etl_didb_studies;
ANALYZE etl_orders;
ANALYZE summary_storage_daily;
ANALYZE procedure_duration_map;
