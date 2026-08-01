-- Migration 0099: add sps_id to std_worklist_arrivals -- the real accession number.
--
-- Report 35's accession fallback (COALESCE(s.accession_number, 'WL#' || pps_key)) was
-- showing raw pps_key values far more often than expected -- confirmed (operator,
-- 2026-08-01): "Flagged Exams: accession number is incorrect, you are reading pps_key
-- not sps_id." A completed exam per RIS (arrived + exam_done both recorded) doesn't
-- always yet have a matching etl_didb_studies row -- PACS image processing can lag
-- behind the RIS "Exam Done" status, so s.accession_number is NULL more often than the
-- WL# fallback was meant to cover.
--
-- SITE_WORKLIST.SPS_ID *is* the real accession number -- "the accession number the RIS
-- mints at scheduling; same value as the PACS accession number across ALL tables"
-- (docs/LAUMC_RIS_TABLES.md). It was never pulled into any std_* table. Added here to
-- std_worklist_arrivals specifically (not a new table) because that's Report 35's base/
-- anchor table (routes/report_35.py's `arrival` CTE) -- SPS_ID is available right where
-- the row is already established, no extra join needed.
--
-- Nullable, no index: display-only column, never filtered/joined on.

ALTER TABLE std_worklist_arrivals
    ADD COLUMN IF NOT EXISTS sps_id TEXT;
