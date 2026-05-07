-- Migration 0027: Custom report composer tables

CREATE TABLE IF NOT EXISTS custom_reports (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    created_by      INTEGER REFERENCES users(id) ON DELETE SET NULL,
    visibility      VARCHAR(20) DEFAULT 'shared',   -- 'shared' | 'restricted'
    has_financial   BOOLEAN DEFAULT FALSE,
    filters_json    JSONB DEFAULT '{}',             -- {date_from, date_to, modality, physician_id, patient_class, source_db}
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS custom_report_sections (
    id              SERIAL PRIMARY KEY,
    report_id       INTEGER NOT NULL REFERENCES custom_reports(id) ON DELETE CASCADE,
    section_type    VARCHAR(50) NOT NULL,            -- widget key e.g. 'study_count'
    position        INTEGER NOT NULL DEFAULT 0,
    config_json     JSONB DEFAULT '{}'               -- {top_n, chart_type, group_by, ...}
);

CREATE INDEX IF NOT EXISTS idx_custom_report_sections_report ON custom_report_sections(report_id, position);
CREATE INDEX IF NOT EXISTS idx_custom_reports_created_by     ON custom_reports(created_by);
