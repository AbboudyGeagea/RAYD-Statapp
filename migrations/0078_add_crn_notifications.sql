-- Migration 0078: CRN (Critical Result Notification) scaffold.
--
-- Detects a critical result (existing hl7_oru_analysis.is_critical, written by
-- nlp-worker) with a resolvable referring_contacts row (migration 0077), sends
-- a notification on the physician's preferred channel, and escalates to their
-- OTHER channels every 30 minutes (operator instruction, 2026-07-27) if
-- unacknowledged -- no fixed backstop contact, exhausts the physician's own
-- channels only, then stops (status = 'exhausted').
--
-- LIVE SENDING IS OFF BY DEFAULT (settings.crn_enabled = 'false'). Provider
-- credentials (SMTP/SMS/WhatsApp) don't exist yet -- see
-- docs/LAUMC_CRN_FIREWALL_REQUEST.md -- so utils/crn_dispatcher.py runs in dry
-- run mode (logs what it WOULD send to crn_notification_attempts, never
-- contacts a real provider) until this flag is flipped on with real,
-- tested credentials. The scan/escalate scheduler job is gated on the same
-- flag so it stays fully dormant on the live system until explicitly enabled.
--
-- Ack link: one-time use, 48h expiry (operator instruction, 2026-07-27) --
-- enforced in utils/crn_dispatcher.acknowledge() via status + expiry check,
-- not a DB constraint (the token itself must remain valid to look up even
-- after use, to show "already acknowledged" rather than a dead link).

CREATE TABLE IF NOT EXISTS crn_notifications (
    id                    SERIAL PRIMARY KEY,
    report_id             INTEGER NOT NULL REFERENCES hl7_oru_reports(id),
    accession_number      TEXT,
    referring_contact_id  INTEGER REFERENCES referring_contacts(id),
    critical_keywords     TEXT[],
    ack_token             TEXT NOT NULL UNIQUE,
    ack_token_expires_at  TIMESTAMP NOT NULL,
    acknowledged_at       TIMESTAMP,
    acknowledged_ip       TEXT,
    status                VARCHAR(20) NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'acknowledged', 'exhausted')),
    current_channel       VARCHAR(20) NOT NULL,
    channels_tried        TEXT[] NOT NULL DEFAULT '{}',
    next_escalation_at    TIMESTAMP,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (report_id)
);

CREATE INDEX IF NOT EXISTS idx_crn_notifications_pending_escalation
    ON crn_notifications (next_escalation_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS crn_notification_attempts (
    id                  SERIAL PRIMARY KEY,
    notification_id     INTEGER NOT NULL REFERENCES crn_notifications(id) ON DELETE CASCADE,
    channel             VARCHAR(20) NOT NULL,
    attempted_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    dry_run             BOOLEAN NOT NULL DEFAULT TRUE,
    success             BOOLEAN,
    provider_response   TEXT,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_crn_notification_attempts_notification
    ON crn_notification_attempts (notification_id);

INSERT INTO settings (key, value) VALUES ('crn_enabled', 'false') ON CONFLICT (key) DO NOTHING;
