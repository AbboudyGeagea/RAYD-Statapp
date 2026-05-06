-- Migration 0026: adapter_etl_log table for generic ETL adapter sync tracking
-- Each confirmed adapter_mapping gets per-table sync log rows here.

CREATE TABLE IF NOT EXISTS adapter_etl_log (
    id              SERIAL PRIMARY KEY,
    mapping_id      INTEGER REFERENCES adapter_mappings(id) ON DELETE CASCADE,
    target_table    VARCHAR(100) NOT NULL,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    rows_synced     INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'running',   -- running | done | error
    error_message   TEXT,
    watermark_col   VARCHAR(100),  -- incremental_key column name
    watermark_val   TEXT           -- last synced value (stored as text for all types)
);

CREATE INDEX IF NOT EXISTS idx_adapter_etl_log_mapping
    ON adapter_etl_log (mapping_id, target_table);

CREATE INDEX IF NOT EXISTS idx_adapter_etl_log_started
    ON adapter_etl_log (started_at DESC);

-- Also ensure adapter_mappings has the scheduled column (for APScheduler toggle)
ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS etl_enabled   BOOLEAN DEFAULT TRUE;
ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS etl_schedule  VARCHAR(50) DEFAULT '02:00';
ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS last_etl_at   TIMESTAMP;
