-- Migration 0009: Add analytics_snapshots table for nightly pre-computed briefings.

CREATE TABLE IF NOT EXISTS analytics_snapshots (
    id            SERIAL PRIMARY KEY,
    period_label  VARCHAR(20)  NOT NULL,   -- 'last_30d' | 'last_90d' | 'ytd'
    period_start  DATE         NOT NULL,
    period_end    DATE         NOT NULL,
    computed_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    data_json     JSONB,
    narrative     TEXT,
    status        VARCHAR(20)  NOT NULL DEFAULT 'ok'   -- 'ok' | 'error'
);

CREATE INDEX IF NOT EXISTS idx_snapshots_label_time
    ON analytics_snapshots (period_label, computed_at DESC);
