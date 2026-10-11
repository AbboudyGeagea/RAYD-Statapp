-- CRN slice 2a: the referring-doctor contact list and its CSV imports.
--
-- Ported from Mazloum (its 0056).
--
-- crn_contacts: one row per doctor code. At LAUMC that is the referring-doctor
-- code the RIS sends in PV1-8, e.g. "20000191" (the RIS resource number, see
-- 0079). Codes are stored trimmed and upper-cased; the dispatcher looks them up
-- with UPPER(TRIM(code)).
-- phone_raw keeps the number exactly as given in the file; phone_e164 is the
-- normalised number SMS is sent to (NULL when the number could not be read).
--
-- crn_contact_imports: every uploaded file, kept as uploaded, with the summary
-- of what was applied, so the contact list's history can be traced.
--
-- SMS is the only channel for now (settings.crn_channel).

CREATE TABLE IF NOT EXISTS crn_contacts (
    doctor_code  VARCHAR(64) PRIMARY KEY,
    full_name    TEXT,
    phone_raw    TEXT,
    phone_e164   VARCHAR(20),
    email        TEXT,
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by   TEXT
);

CREATE TABLE IF NOT EXISTS crn_contact_imports (
    id           SERIAL PRIMARY KEY,
    filename     TEXT,
    content      TEXT NOT NULL,
    uploaded_by  TEXT,
    uploaded_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    applied_at   TIMESTAMP,
    applied_by   TEXT,
    summary      JSONB
);

INSERT INTO settings (key, value) VALUES ('crn_channel', 'sms') ON CONFLICT (key) DO NOTHING;
