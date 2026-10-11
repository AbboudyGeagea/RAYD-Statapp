-- CRN (Critical Result Notification), slice 1: detect and record.
--
-- A radiologist writes the agreed marker in a report; the NLP worker detects it
-- and records a notification here. Sending, contacts, escalation and the
-- acknowledgement page come in later slices. Ported from Mazloum (its 0055);
-- 0128 moved the July scaffold's crn_notifications out of the way first.
--
-- 1. crn_notifications: one row per report that carries the marker.
-- 2. crn_events: append-only history of each notification (a trigger rejects
--    UPDATE and DELETE), so it can serve as evidence.
-- 3. settings: crn_live_since empty = CRN off. When switched on it holds the
--    switch-on time; only reports received at or after it can notify.
--    crn_marker = the marker, matched as a whole word, exact case.
--
-- Mazloum also adds hl7_orders.ordering_physician_code (ORC-12) here. LAUMC does
-- not: the doctor is found by the PV1-8 referring-doctor code instead (0079, 0101).

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
