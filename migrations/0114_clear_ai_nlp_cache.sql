-- Migration 0114: clear ai_nlp_cache so every report is reclassified from
-- scratch as part of the same threshold-calibration effort as migration 0113
-- (oru_classification_reviews). These rows are a recomputable cache
-- (TF-IDF/K-means over hl7_oru_reports), never a source of truth, so clearing
-- them is safe -- the next "Backfill All" run from the ORU dashboard (admin,
-- POST /oru/nlp/process with backfill=true) repopulates them.
--
-- oru_cluster_labels is cleared too since its cluster_id values are only
-- meaningful relative to the specific K-means run that produced them.

TRUNCATE TABLE ai_nlp_cache;
TRUNCATE TABLE oru_cluster_labels;
