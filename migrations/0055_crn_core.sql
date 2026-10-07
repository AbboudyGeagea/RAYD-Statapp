-- CRN (Critical Result Notification), slice 1: detect and record.
--
-- A radiologist writes the agreed marker in a report; the NLP worker detects it
-- and records a notification here. Sending, contacts, escalation and the
-- acknowledgement page come in later slices.
--
-- 1. hl7_orders.ordering_physician_code: the doctor code from ORC-12 (OBR-16
--    fallback), e.g. "ID2" from "ID2^^Jihad Falou". hl7_listener used to keep
--    only the name. Old orders are filled by backfill_ordering_physician_code.py.
-- 2. crn_notifications: one row per report that carries the marker.
-- 3. crn_events: append-only history of each notification (a trigger rejects
--    UPDATE and DELETE), so it can serve as evidence.
-- 4. settings: crn_live_since empty = CRN off. When switched on it holds the
--    switch-on time; only reports received at or after it can notify.
--    crn_marker = the marker, matched as a whole word, exact case.

ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS ordering_physician_code VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_hl7_orders_ordering_physician_code ON hl7_orders (ordering_physician_code);

CREATE TABLE IF NOT EXISTS crn_notifications (
    id                  SERIAL PRIMARY KEY,
    ref_code            VARCHAR(16) NOT NULL UNIQUE,
    report_id           INTEGER NOT NULL UNIQUE REFERENCES hl7_oru_reports(id),
    accession_number    TEXT,
    patient_id          TEXT,
    marker              TEXT NOT NULL,
    signing_radiologist TEXT,
    report_signed_at    TIMESTAMP,
    report_received_at  TIMESTAMP,
    report_fingerprint  CHAR(64) NOT NULL,
    detected_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    status              VARCHAR(20) NOT NULL DEFAULT 'detected'
);
CREATE INDEX IF NOT EXISTS idx_crn_notifications_status ON crn_notifications (status);
CREATE INDEX IF NOT EXISTS idx_crn_notifications_accession ON crn_notifications (accession_number);

CREATE TABLE IF NOT EXISTS crn_events (
    id               BIGSERIAL PRIMARY KEY,
    notification_id  INTEGER NOT NULL REFERENCES crn_notifications(id),
    event_type       VARCHAR(32) NOT NULL,
    at               TIMESTAMP NOT NULL DEFAULT NOW(),
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_crn_events_notification ON crn_events (notification_id, at);

CREATE OR REPLACE FUNCTION crn_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'crn_events is append-only'; END $$;
DROP TRIGGER IF EXISTS trg_crn_events_append_only ON crn_events;
CREATE TRIGGER trg_crn_events_append_only BEFORE UPDATE OR DELETE ON crn_events FOR EACH ROW EXECUTE FUNCTION crn_events_append_only();

INSERT INTO settings (key, value) VALUES ('crn_live_since', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_marker', 'CRN') ON CONFLICT (key) DO NOTHING;
