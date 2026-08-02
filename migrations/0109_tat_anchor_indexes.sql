-- Migration 0109: index the actual TAT-anchor columns.
--
-- etl_didb_studies already has idx_study_final_time (rep_final_timestamp) and
-- idx_study_prelim_time (rep_prelim_timestamp) -- but per operator confirmation
-- (2026-07-27, see routes/report_25.py and routes/super_report.py comments),
-- rep_final_timestamp is sparse/unreliable on this install. Every TAT
-- computation in super_report.py, report_25.py, and viewer_controller.py's
-- daily_briefing actually anchors on rep_study_last_composed_ts (report done)
-- and insert_time (study started), via
-- EXTRACT(EPOCH FROM (rep_study_last_composed_ts - insert_time)). Neither
-- column has ever been indexed, so every TAT query (median TAT, TAT-by-modality,
-- ER-delayed-vs-P75, radiologist matrices) does this arithmetic after a full
-- table scan on a ~614K+ row table.
--
-- CREATE INDEX CONCURRENTLY cannot run inside an explicit transaction block;
-- this file has none, matching migrations/0094_performance_indexes.sql's
-- pattern -- each statement commits independently, safe to re-run.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_study_composed_time
    ON etl_didb_studies (rep_study_last_composed_ts);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_study_insert_time
    ON etl_didb_studies (insert_time);

ANALYZE etl_didb_studies;
