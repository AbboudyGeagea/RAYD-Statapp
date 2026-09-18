-- Migration 0113: one-time end-user confirmation of ai_nlp_cache's TF-IDF/
-- K-means classification (normal/borderline/critical, nlp_worker/clustering.py
-- classify_report). All 4 installs independently show ~30% "critical" -- a
-- fixed keyword-score threshold converging to the same rate regardless of
-- site case-mix points at a threshold artifact, not real clinical incidence.
-- This table captures ground truth from an actual reviewer against a
-- stratified sample so the threshold can be recalibrated against measured
-- precision/recall instead of guesswork.
--
-- Row lifecycle: seeded with reviewed_at IS NULL and predicted_classification
-- snapshotted at seed time (ai_nlp_cache gets recomputed independently by the
-- worker and could drift after seeding); a reviewer answers once, filling
-- confirmed_correct / corrected_classification / reviewed_by / reviewed_at --
-- after which the row is permanently excluded from the pending queue. That
-- gives "one-time" semantics without a separate seen/asked flag.

CREATE TABLE IF NOT EXISTS oru_classification_reviews (
    id                       SERIAL PRIMARY KEY,
    report_id                INTEGER NOT NULL REFERENCES hl7_oru_reports(id),
    predicted_classification VARCHAR(20) NOT NULL,
    confirmed_correct        BOOLEAN,
    corrected_classification VARCHAR(20),
    reviewed_by              INTEGER REFERENCES users(id),
    queued_at                TIMESTAMP NOT NULL DEFAULT NOW(),
    reviewed_at              TIMESTAMP,
    UNIQUE (report_id)
);

CREATE INDEX IF NOT EXISTS idx_oru_classification_reviews_pending
    ON oru_classification_reviews (queued_at)
    WHERE reviewed_at IS NULL;
