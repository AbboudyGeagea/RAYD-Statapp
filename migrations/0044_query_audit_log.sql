-- Migration 0044: Query audit log for Resource Monitor
CREATE TABLE IF NOT EXISTS query_audit_log (
    id           BIGSERIAL PRIMARY KEY,
    captured_at  TIMESTAMP DEFAULT NOW() NOT NULL,
    endpoint     TEXT NOT NULL,
    report_id    SMALLINT,
    total_ms     NUMERIC(8,1) NOT NULL,
    query_count  SMALLINT NOT NULL DEFAULT 0,
    queries      JSONB NOT NULL DEFAULT '[]',
    filters      JSONB NOT NULL DEFAULT '{}',
    user_id      INT REFERENCES users(id) ON DELETE SET NULL,
    username     VARCHAR(100),
    ip_address   VARCHAR(45),
    http_status  SMALLINT,
    cache_hit    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_qal_captured_at  ON query_audit_log (captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_qal_endpoint     ON query_audit_log (endpoint, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_qal_report_id    ON query_audit_log (report_id, captured_at DESC)
    WHERE report_id IS NOT NULL;
