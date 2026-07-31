-- Migration 0090: create std_worklist_exam_done (RIS WORKLIST_STATUS_HISTORY
-- [STATUS_KEY=100, "Exam Done"] ⋈ SITE_WORKLIST -> std_worklist_exam_done, LAUMC).
--
-- Same table shape/design as std_worklist_arrivals (migrations 0088/0089): the
-- STATUS_KEY lifecycle decoded for this RIS install (60 Arrived, 70 Started,
-- 100 Exam Done, ...) lives entirely in WORKLIST_STATUS_HISTORY, confirmed
-- usable 2026-07-31 once WORKLIST_STATUS_HISTORY_KEY's NULL issue was found
-- and worked around (see 0089). This is the same join/watermark/upsert
-- pattern, just STATUS_KEY=100 instead of 60 -- gives a real RIS "exam done"
-- timestamp per worklist entry, as opposed to etl_didb_studies.insert_time
-- (a PACS-side ingestion proxy) or std_pps.end_datetime (the MPPS event from
-- the modality itself, already in use elsewhere in report_25).
--
-- Synthetic serial PK + uniqueness on (site_worklist_key, exam_done_at), not
-- site_worklist_key alone -- same reasoning as arrivals: a worklist entry can
-- have multiple Exam Done transitions (corrections/re-runs), so the pair is
-- the real natural key.

CREATE TABLE IF NOT EXISTS std_worklist_exam_done (
    id                 BIGSERIAL PRIMARY KEY,
    site_worklist_key  BIGINT NOT NULL,
    pps_key            BIGINT,
    exam_done_at       TIMESTAMP NOT NULL,
    last_update        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_worklist_exam_done_site_time
    ON std_worklist_exam_done (site_worklist_key, exam_done_at);

CREATE INDEX IF NOT EXISTS idx_worklist_exam_done_pps_key
    ON std_worklist_exam_done (pps_key);

CREATE INDEX IF NOT EXISTS idx_worklist_exam_done_exam_done_at
    ON std_worklist_exam_done (exam_done_at);
