-- Migration 0026: adapter_etl_log table for generic ETL adapter sync tracking
-- Each confirmed adapter_mapping gets per-table sync log rows here.
--
-- adapter_mappings itself is normally created at runtime by
-- routes/db_manager.py:_ensure_mappings_table() the first time an admin opens the DB
-- Manager page — but this migration references it via FK and runs at app STARTUP,
-- before any admin has necessarily visited that page. On a fresh install that made
-- 0026 fail every single time with "relation adapter_mappings does not exist" (and,
-- since the runner only records successes, it kept re-failing on every restart).
-- Create the minimal table here so the FK always has something to point at;
-- _ensure_mappings_table()'s own IF NOT EXISTS / ADD COLUMN IF NOT EXISTS calls stay
-- harmless no-ops once this has run.
CREATE TABLE IF NOT EXISTS adapter_mappings (
    id              SERIAL PRIMARY KEY,
    connection_name VARCHAR(100) NOT NULL,
    schema_owner    VARCHAR(100),
    dump_file       VARCHAR(255),
    mapping_json    JSONB,
    notes           TEXT,
    status          VARCHAR(20) DEFAULT 'draft',
    system_type     VARCHAR(20),
    target_db       VARCHAR(100),
    target_action   VARCHAR(20) DEFAULT 'provision',
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

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
