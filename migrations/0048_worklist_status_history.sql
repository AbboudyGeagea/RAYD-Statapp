-- Migration 0048: RAYD-maintained worklist status history (LAUMC)
--
-- The LAUMC RIS does NOT keep a usable status-history table (its own
-- WORKLIST_STATUS_HISTORY is unused), and the worklist row has dedicated timestamp
-- columns only for scheduled/performed/approved — NOT for arrived or started.
-- So RAYD builds its own append-only event log: each time the RIS emits an outbound
-- status-change message (or a poll detects a STATUS_KEY change), RAYD appends one row
-- here with the transition time. Wait-time, exam-duration and the live floor map are
-- all derived from this table.
--
-- captured_at = when RAYD recorded the event (message arrival / poll). For arrived &
-- started this IS the authoritative transition time (no source column exists). For
-- scheduled/performed/approved, source_time carries the RIS column value when known.

CREATE TABLE IF NOT EXISTS worklist_status_history (
    id                BIGSERIAL PRIMARY KEY,
    site_worklist_key BIGINT       NOT NULL,          -- RIS SITE_WORKLIST_KEY (the exam)
    sps_id            VARCHAR(64),                     -- RIS accession (for joining)
    pacs_sps_id       VARCHAR(64),                     -- PACS accession (join to studies)
    linked_id         BIGINT,                          -- LINKED_ID group (report-level dedup)
    site_id           INTEGER REFERENCES sites(id),    -- resolved canonical site
    status_key        INTEGER,                         -- raw RIS status
    stage             VARCHAR(20),                     -- canonical stage (worklist_status_map)
    captured_at       TIMESTAMP NOT NULL DEFAULT NOW(),-- when RAYD recorded the transition
    source_time       TIMESTAMP,                       -- RIS-provided timestamp when available
    source            VARCHAR(20) NOT NULL DEFAULT 'mllp', -- 'mllp' | 'poll' | 'backfill'
    message_control_id VARCHAR(64)                     -- HL7 MSH-10, for idempotency/dedup
);

-- One row per (exam, stage) — re-delivery of the same transition (SAP Mirth duplicate
-- bug, or overlapping poll+mllp) must not create duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS idx_wsh_exam_stage
    ON worklist_status_history (site_worklist_key, stage);

CREATE INDEX IF NOT EXISTS idx_wsh_worklist   ON worklist_status_history (site_worklist_key);
CREATE INDEX IF NOT EXISTS idx_wsh_pacs_sps   ON worklist_status_history (pacs_sps_id) WHERE pacs_sps_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wsh_site_stage ON worklist_status_history (site_id, stage, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_wsh_captured   ON worklist_status_history (captured_at DESC);
-- Idempotency guard against duplicate HL7 delivery (permanent SAP Mirth bug).
CREATE INDEX IF NOT EXISTS idx_wsh_msgid      ON worklist_status_history (message_control_id) WHERE message_control_id IS NOT NULL;
