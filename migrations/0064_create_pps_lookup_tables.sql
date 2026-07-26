-- Migration 0064: PPS lookup tables (LAUMC) — STATUS, PROCEDURE_PRIORITY, DICTATION.
--
-- std_status_ris — full-fidelity mirror of the RIS master STATUS table (received
-- 2026-07-27, ~46 rows). This is a BIGGER structural finding than a simple lookup:
-- the TYPE column ('ORDER'/'SPS'/'PPS') confirms this ONE table drives status at all
-- three RIS levels, and CORE_STATUS is the actual alias->canonical-status pointer that
-- docs/LAUMC_DATA_REQUEST.md's "never hardcode 60/70/100, always roll up via the
-- base-status pointer" rule refers to. This is more authoritative/complete than the
-- hand-curated migrations/0047_worklist_status_map.sql seed (codes like 1440, 1722,
-- 1803-1805, 2023 aren't in that seed at all). Used here to resolve PPS.STATUS_KEY.
-- worklist_status_map is left untouched — not migrating SITE_WORKLIST's resolution path
-- to this table in this pass, to avoid destabilizing what's already working there.
--
-- core_status_key is nullable and self-referencing (no FK enforced — some values in the
-- 1000-1003 range appear to be internal UI/workflow-behavior codes, not real status
-- rows, so a strict FK would risk load failures over something cosmetic).
CREATE TABLE IF NOT EXISTS std_status_ris (
    status_key            BIGINT PRIMARY KEY,
    name                  TEXT,
    description           TEXT,
    end_point             BOOLEAN,      -- Y/N: is this a terminal status
    hl7_code              TEXT,         -- e.g. NW/SC/CM/CA/DC/IP
    dicom_status          TEXT,
    type                  VARCHAR(16),  -- ORDER / SPS / PPS
    source_last_updated   TIMESTAMP,
    time_threshold_check  BOOLEAN,
    dragged_behavior      BIGINT,       -- purpose unclear (UI/workflow internal, not a real status ref)
    drag_linked           BIGINT,       -- purpose unclear (as above)
    core_status_key       BIGINT,       -- alias -> canonical status pointer (self-referencing)
    last_update           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_status_ris_type ON std_status_ris (type);
CREATE INDEX IF NOT EXISTS idx_std_status_ris_core ON std_status_ris (core_status_key);

-- std_procedure_priorities — resolves PPS.PRIORITY_KEY. Small catalog, received
-- 2026-07-27 with sample data (ROU/Routine, 003/Emergency, 004/Very Urgent, etc.).
CREATE TABLE IF NOT EXISTS std_procedure_priorities (
    priority_key          BIGINT PRIMARY KEY,
    code                  TEXT,
    description            TEXT,
    active                  BOOLEAN,
    source_last_updated      TIMESTAMP,
    last_update               TIMESTAMP NOT NULL DEFAULT NOW()
);

-- std_dictations — resolves PPS.DICTATION_KEY. dictated_by_resource_id_key resolves via
-- std_resources_ris (Phase 13) at query time — no duplicated name data here.
CREATE TABLE IF NOT EXISTS std_dictations (
    dictation_key               BIGINT PRIMARY KEY,
    dictation_date               TIMESTAMP,
    last_modified_date            TIMESTAMP,
    audio                          TEXT,   -- path/reference or flag; exact semantics unconfirmed
    revision_count                  INTEGER,
    dictated_by_resource_id_key      BIGINT,
    last_update                       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_dictations_resource ON std_dictations (dictated_by_resource_id_key);
