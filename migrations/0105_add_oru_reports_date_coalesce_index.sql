-- Migration 0105: functional index to support the COALESCE(result_datetime,
-- received_at) filter used by every ORU analytics query (oru_data, section_gaps,
-- sections, nlp_results) -- previously unindexed, forcing a seq scan of
-- hl7_oru_reports on every request.

CREATE INDEX IF NOT EXISTS idx_oru_reports_coalesce_date
    ON hl7_oru_reports (COALESCE(result_datetime, received_at) DESC);
