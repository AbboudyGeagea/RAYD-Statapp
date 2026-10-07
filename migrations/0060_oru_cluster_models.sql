-- Migration 0060: one stored cluster model for the TF-IDF/K-means analysis.
--
-- The analysis ran only when an admin clicked "Process Reports", 500 reports at a
-- time, and fitted a separate K-means per batch, so "cluster 0" meant something
-- different in every batch. The NLP worker now scores every report automatically
-- against one model stored here, refitted on the first run of each month, when
-- its analysis version changes, or on "Rebuild clusters". A refit re-scores every
-- report in the background.
--
-- ai_nlp_cache.cluster_model_id records the model a row was scored with; the ORU
-- page shows only rows of the latest model. oru_cluster_labels (0049) is no longer
-- written.

CREATE TABLE IF NOT EXISTS oru_cluster_models (
    id          SERIAL PRIMARY KEY,
    fitted_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    nlp_version TEXT NOT NULL,
    sample_size INTEGER NOT NULL,
    terms       JSONB NOT NULL,   -- vocabulary, in vector order
    idf         JSONB NOT NULL,   -- idf per term
    centroids   JSONB NOT NULL,   -- k x len(terms)
    labels      JSONB NOT NULL    -- k cluster names + 'No affirmed findings' + 'Other wording'
);

ALTER TABLE ai_nlp_cache ADD COLUMN IF NOT EXISTS cluster_model_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_nlp_cluster_model ON ai_nlp_cache (cluster_model_id);
