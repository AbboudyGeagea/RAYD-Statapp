-- CRN slice 2b: route each detected CRN to the ordering doctor by SMS, with escalation.
--
-- crn_notifications gains the resolved doctor and the routing times. Status flow:
--   detected -> sent -> overdue      (acknowledged arrives with the notification page)
--   detected -> unroutable           (no doctor contact and no fallback contact)
--
-- crn_recipients: one row per person texted about a notification, the ordering
-- doctor and, on escalation, the fallback contact. Each has its own link:
-- token_hash is what the notification page will check; token_enc (Fernet,
-- utils/crypto) lets a resend carry the same link. The raw token is never stored.
--
-- Settings (all editable; times in minutes):
--   crn_resend_min    15  link not opened -> resend the SMS to the doctor
--   crn_fallback_min  30  not acknowledged -> text the fallback contact
--   crn_overdue_min   60  not acknowledged -> overdue
--   crn_link_ttl_hours 72
--   crn_fallback_code     doctor code (in crn_contacts) of the fallback contact
--   crn_public_base_url   address of the CRN gateway, e.g. https://crn.<hospital-domain>
--   crn_sms_provider  log = record messages without sending (until a provider is set up)
--   crn_sms_template  only {ref} and {link} are allowed: no patient data over SMS

ALTER TABLE crn_notifications ADD COLUMN IF NOT EXISTS doctor_code VARCHAR(64);
ALTER TABLE crn_notifications ADD COLUMN IF NOT EXISTS doctor_name TEXT;
ALTER TABLE crn_notifications ADD COLUMN IF NOT EXISTS first_sent_at TIMESTAMP;
ALTER TABLE crn_notifications ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS crn_recipients (
    id               SERIAL PRIMARY KEY,
    notification_id  INTEGER NOT NULL REFERENCES crn_notifications(id),
    role             VARCHAR(16) NOT NULL CHECK (role IN ('doctor', 'fallback')),
    contact_code     VARCHAR(64),
    contact_name     TEXT,
    phone_e164       VARCHAR(20) NOT NULL,
    token_hash       CHAR(64) NOT NULL UNIQUE,
    token_enc        TEXT NOT NULL,
    expires_at       TIMESTAMP NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    send_count       INTEGER NOT NULL DEFAULT 0,
    fail_count       INTEGER NOT NULL DEFAULT 0,
    last_sent_at     TIMESTAMP,
    opened_at        TIMESTAMP,
    UNIQUE (notification_id, role)
);

INSERT INTO settings (key, value) VALUES ('crn_resend_min', '15') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_fallback_min', '30') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_overdue_min', '60') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_link_ttl_hours', '72') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_fallback_code', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_public_base_url', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_sms_provider', 'log') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_sms_template', 'Critical result for your patient. Ref {ref}. View and acknowledge: {link}') ON CONFLICT (key) DO NOTHING;
