-- Migration 0140: indexes for the Department Live board (routes/department_board.py).
--
-- The board reads, every few seconds, the studies that are open (scheduled, waiting,
-- in progress: the partial indexes of 0119 already cover those) and the work done
-- today, for each device's utilization. "Today's work" had no index: a study that
-- started or completed since midnight was only reachable by reading every study
-- brayn_study_state has ever held.
--
-- CONCURRENTLY, so building them never blocks the listener writing this table
-- (db_migrations runs CONCURRENTLY statements on their own AUTOCOMMIT connection).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_brayn_state_started_at
    ON brayn_study_state (started_at) WHERE started_at IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_brayn_state_completed_at
    ON brayn_study_state (completed_at) WHERE completed_at IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_brayn_state_pacs_completed_at
    ON brayn_study_state (pacs_completed_at) WHERE pacs_completed_at IS NOT NULL;
