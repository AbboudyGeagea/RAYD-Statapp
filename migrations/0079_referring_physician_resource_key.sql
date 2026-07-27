-- Migration 0079: referring-physician resource-key resolution.
--
-- Operator provided a real HL7 ORM sample (2026-07-27) showing PV1-8
-- (Referring Doctor, XCN type: ID^Last^First...) carries a numeric code
-- (e.g. "20000191") that resolves directly against
-- std_resources_ris.resource_id_key -> primary_email_address /
-- mobile_phone_number (RIS RESOURCE_ID/PERSON, Phase 13, migration 0063) --
-- a reliable, ID-based resolution path instead of the fragile free-text
-- name match referring_contacts/CRN detection have relied on until now.
--
-- hl7_orders gains the raw code + a readable name from the same PV1-8 field
-- (hl7_listener.py now parses both, see parse_orm_o01). referring_contacts
-- gains an optional resource_id_key -- when set, this is the authoritative
-- match key (utils/referring_contacts_sync.py auto-fills it from
-- std_resources_ris); when NULL, the contact was entered manually and
-- physician_name free-text matching remains the fallback used by
-- utils/crn_scan.py.

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS referring_physician_code TEXT,
    ADD COLUMN IF NOT EXISTS referring_physician_name TEXT;

CREATE INDEX IF NOT EXISTS idx_hl7_orders_referring_physician_code
    ON hl7_orders (referring_physician_code) WHERE referring_physician_code IS NOT NULL;

ALTER TABLE referring_contacts
    ADD COLUMN IF NOT EXISTS resource_id_key BIGINT,
    ADD COLUMN IF NOT EXISTS auto_filled BOOLEAN NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_referring_contacts_resource_id_key
    ON referring_contacts (resource_id_key) WHERE resource_id_key IS NOT NULL;
