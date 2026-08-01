-- Migration 0104: dedicated storage for NLP cluster labels.
--
-- Cluster labels used to live as a single JSON blob in settings
-- (key = 'nlp_cluster_labels'), overwritten on every processing run, and matched
-- back to a report's cluster_id by array index at query time in
-- routes/oru_analytics.py's nlp_results(). ai_nlp_cache.cluster_label already
-- existed as a column but was never populated on insert. This table replaces the
-- settings blob; nlp_worker/worker.py now upserts into it during processing and
-- writes cluster_label directly onto each ai_nlp_cache row.

CREATE TABLE IF NOT EXISTS oru_cluster_labels (
    cluster_id INTEGER PRIMARY KEY,
    label      TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Clean up the now-superseded settings blob, if present.
DELETE FROM settings WHERE key = 'nlp_cluster_labels';
