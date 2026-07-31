-- Migration 0092: create std_worklist_scheduled (RIS WORKLIST_STATUS_HISTORY
-- [STATUS_KEY=40, "Scheduled"] ⋈ SITE_WORKLIST -> std_worklist_scheduled, LAUMC).
--
-- Third status transition off the same WORKLIST_STATUS_HISTORY table as
-- std_worklist_arrivals (status_key=60, migrations 0088/0089) and
-- std_worklist_exam_done (status_key=100, migration 0090). Feeds the new
-- "Patient Wait Time" definition (Scheduled -> Arrived, operator instruction
-- 2026-07-31) -- the OLD Patient Wait Time (Arrived -> Exam Done) is being
-- renamed "Technician Efficiency" instead, see report_25.py.
--
-- Same shape/reasoning as the other two: synthetic serial PK + uniqueness on
-- (site_worklist_key, scheduled_at), not site_worklist_key alone, since a
-- worklist entry can have multiple Scheduled transitions (re-scheduling).

CREATE TABLE IF NOT EXISTS std_worklist_scheduled (
    id                 BIGSERIAL PRIMARY KEY,
    site_worklist_key  BIGINT NOT NULL,
    pps_key            BIGINT,
    scheduled_at       TIMESTAMP NOT NULL,
    last_update        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_worklist_scheduled_site_time
    ON std_worklist_scheduled (site_worklist_key, scheduled_at);

CREATE INDEX IF NOT EXISTS idx_worklist_scheduled_pps_key
    ON std_worklist_scheduled (pps_key);

CREATE INDEX IF NOT EXISTS idx_worklist_scheduled_scheduled_at
    ON std_worklist_scheduled (scheduled_at);
