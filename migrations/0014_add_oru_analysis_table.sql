-- Migration 0014: Pre-computed NLP analysis cache for hl7_oru_reports.
-- Populated every 60 minutes by the ORU NLP batch job.
-- Custom keywords are NOT stored here — they are overlaid at query time.

CREATE TABLE IF NOT EXISTS hl7_oru_analysis (
    id              SERIAL PRIMARY KEY,
    report_id       INTEGER NOT NULL
                        REFERENCES hl7_oru_reports(id) ON DELETE CASCADE,
    affirmed_labels TEXT[]    NOT NULL DEFAULT '{}',
    is_critical     BOOLEAN   NOT NULL DEFAULT FALSE,
    nlp_version     TEXT,
    analyzed_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oru_analysis_report UNIQUE (report_id)
);

CREATE INDEX IF NOT EXISTS idx_oru_analysis_report_id ON hl7_oru_analysis (report_id);
CREATE INDEX IF NOT EXISTS idx_oru_analysis_critical  ON hl7_oru_analysis (is_critical);
CREATE INDEX IF NOT EXISTS idx_oru_analysis_analyzed  ON hl7_oru_analysis (analyzed_at);
