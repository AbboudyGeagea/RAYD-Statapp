-- Migration 0077: referring_contacts — per-referring-physician contact info +
-- preferred notification channel.
--
-- Operator instruction (2026-07-27): CRN (Critical Result Notification,
-- docs/LAUMC_SCOPE.md #10) needs to know, per referring physician, how they
-- want to be reached (email / SMS / WhatsApp) and route accordingly — not
-- broadcast to every channel. This table is the lookup CRN's sender will read
-- from once built.
--
-- physician_name is the natural key: matches TRIM(CONCAT(referring_physician_
-- first_name, ' ', referring_physician_last_name)), the same free-text
-- identifier already used to key referring physicians everywhere else in the
-- app (routes/referring_intel.py) — there is currently no stable RIS/PACS ID
-- for a referring physician. Operator is separately researching a proper
-- order<->referring-physician join; this table can gain a stable ID column
-- alongside physician_name later without disrupting contacts already entered.
--
-- The channel-specific contact field is enforced at the app layer (not a CHECK
-- constraint here) so preferred_channel can be changed without a migration.

CREATE TABLE IF NOT EXISTS referring_contacts (
    id                 SERIAL PRIMARY KEY,
    physician_name     TEXT NOT NULL UNIQUE,
    email              TEXT,
    phone              TEXT,
    whatsapp_number    TEXT,
    preferred_channel  VARCHAR(20) NOT NULL DEFAULT 'email'
                       CHECK (preferred_channel IN ('email', 'sms', 'whatsapp')),
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    notes              TEXT,
    created_by         INTEGER REFERENCES users(id),
    created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by         INTEGER REFERENCES users(id),
    updated_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_referring_contacts_active ON referring_contacts (active);
