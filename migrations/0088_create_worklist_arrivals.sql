-- Migration 0088: real patient-arrival timestamps -> std_worklist_arrivals (LAUMC).
--
-- Source: RIS WORKLIST_STATUS_HISTORY (a real per-transition audit log: SITE_WORKLIST_KEY,
-- STATUS_KEY, STATUS_TIME), filtered to STATUS_KEY = 60 ("Arrived", confirmed against
-- std_status_ris, 2026-07-31), joined to SITE_WORKLIST for PPS_KEY.
--
-- Why this exists: hl7_orders.arrived_at (migration 0022) was built for exactly this
-- purpose but depends on the live HL7 ORM feed from R2I, which isn't flowing yet
-- (confirmed empty, 2026-07-31 -- operator: "r2i... still working on it"). PPS and
-- SITE_WORKLIST themselves have no arrival field at all (checked directly against
-- Oracle's real column lists). WORKLIST_STATUS_HISTORY is the actual, already-populated
-- source RIS itself uses to know when a patient arrived.
--
-- Grain: one row per SITE_WORKLIST entry's Arrived transition (should be at most one per
-- worklist entry in practice, but not deduplicated here -- if a patient's status reverts
-- and re-arrives, both transitions are kept; downstream queries should use MAX(arrived_at)
-- per pps_key, same pattern as std_pps's own MAX(end_datetime) per study_instance_uid).

CREATE TABLE IF NOT EXISTS std_worklist_arrivals (
    worklist_status_history_key BIGINT PRIMARY KEY,
    site_worklist_key            BIGINT,
    pps_key                       BIGINT,
    arrived_at                     TIMESTAMP,
    last_update                     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worklist_arrivals_pps_key ON std_worklist_arrivals (pps_key);
