-- Migration 0126: version the TF-IDF/K-means analysis rows (ai_nlp_cache).
--
-- Until now this analysis ran on the raw report text, so "no fracture" counted as
-- "fracture" in its classification, severity, keywords and clusters. The NLP worker
-- now removes every mention medspaCy marks as negated or historical before scoring,
-- and stamps each row it writes with nlp_version.
--
-- Rows already in the table have no version: they were computed without negation.
-- The ORU page hides them, and the next "Process Reports" run recomputes them. They
-- are not deleted here, so if this migration runs before the new worker is deployed,
-- the old worker only writes more unversioned rows, which stay hidden.

ALTER TABLE ai_nlp_cache ADD COLUMN IF NOT EXISTS nlp_version TEXT;
