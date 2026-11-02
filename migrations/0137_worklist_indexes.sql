-- Migration 0137: indexes the Dynamic Worklist views (0136) rely on.
--
--   etl_didb_serieses(study_db_uid)   rpt.series filtered by the study's date
--   etl_didb_studies(study_date)      every study date filter (the compiler bounds
--                                     study_date). Same name as 0094's and
--                                     optimize.sql's, so this is a no-op where they ran.
--
-- The others each view reads already exist: the ORU accession (0054), etl_orders'
-- scheduled time and study (schema), the RIS milestones on (site_worklist_key, time)
-- (0089 / 0090 / 0092 / 0122), PPS person references (0097), the current RIS report by
-- accession (0119), RIS patient IDs (0060).
--
-- CONCURRENTLY, so the ETL keeps writing while they build (db_migrations runs
-- CONCURRENTLY statements on their own AUTOCOMMIT connection).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_serieses_study_db_uid
    ON etl_didb_serieses (study_db_uid);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_didb_studies_study_date
    ON etl_didb_studies (study_date);
