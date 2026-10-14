-- Migration 0047: background job queue for ORU NLP clustering.
--
-- POST /oru/nlp/process used to run TF-IDF vectorisation + K-means synchronously
-- inside the request (nlp_processor.process_reports, up to 500 reports). This table
-- lets the route enqueue a job and return immediately; nlp_worker/worker.py (the
-- existing rayd_nlp container, already polling hl7_oru_reports every 60s for
-- medspaCy) picks up pending rows and does the clustering work instead.

CREATE TABLE IF NOT EXISTS oru_nlp_jobs (
    id              SERIAL PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | running | done | error
    days            INTEGER NOT NULL,
    requested_by    INTEGER REFERENCES users(id),
    processed_count INTEGER,
    cluster_count   INTEGER,
    message         TEXT,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_oru_nlp_jobs_status ON oru_nlp_jobs (status, created_at);
