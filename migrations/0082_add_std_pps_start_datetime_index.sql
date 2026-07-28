-- Migration 0082: index std_pps.start_datetime.
--
-- routes/report_25.py, routes/report_34.py, and routes/report_ai.py all filter
-- std_pps actuals with `pps.start_datetime BETWEEN :start AND :end` when computing
-- device utilization (real PPS minutes vs. the procedure_duration_map estimate
-- fallback). No index existed on this column, so every one of those queries did a
-- full sequential scan of std_pps regardless of the requested date range.
--
-- The related etl_didb_studies(study_date) and etl_didb_studies(storing_ae, study_date)
-- indexes already exist (added by migrations/optimize.sql) — this migration only adds
-- the missing std_pps counterpart.
--
-- CONCURRENTLY: build without locking std_pps against concurrent ETL writes.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_std_pps_start_datetime
    ON std_pps (start_datetime);
