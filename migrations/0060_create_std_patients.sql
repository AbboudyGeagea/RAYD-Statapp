-- Migration 0060: create RIS patient identity tables (LAUMC).
--
-- Source (vendor ER diagram, 2026-07-27): RIS PATIENT is a thin administrative row —
-- death info, worklist flagset, permission level, Rcopia (e-prescribing) sync
-- timestamps. Non-name demographics live in a SHARED PERSON table, joined via
-- PATIENT.PATIENT_PERSON_KEY = PERSON.PERSON_KEY. This is the SAME PATIENT_PERSON_KEY
-- already used as a foreign key on SITE_WORKLIST/ORDERS/VISIT (std_worklist/std_orders/
-- std_visits), so once this lands, all three can resolve a real patient identity.
--
-- *** NO PATIENT NAMES *** — explicit operator instruction: patient names are never
-- copied into Postgres. PERSON's name columns (LAST_NAME, FIRST_NAME, MIDDLE_NAME,
-- NAME_PREFIX, NAME_SUFFIX, NAME_REPRESENTATION_CODE, MAIDEN_NAME, COMMON_NAME, etc.)
-- are intentionally excluded below. PATIENT_ALIAS (alternate/previous names) is not
-- created as a table at all — every column on it is a name field, so once names are
-- excluded there is nothing left in it worth keeping.
--
-- Two tables (not three — see above), one per remaining source grain:
--   std_patients_ris — PATIENT ⋈ PERSON, one row per patient (PK patient_person_key),
--                       demographics only, no name
--   std_patient_ids  — PATIENT_ID_LIST, MANY rows per patient (MRN + other ID types,
--                       each tagged with its issuing system)
--
-- PATIENT_PROBLEM_LIST explicitly excluded per operator instruction (separate ask).
--
-- Created directly in the main etl_db, NOT via the generic adapter "provision a separate
-- database" path — same reasoning as std_visits (migration 0059): LAUMC's multi-site
-- model needs everything joinable in one database.
--
-- No site_id column: a patient is not site-scoped (the same person can be seen at both
-- RH and SJH), so there is no correct single site value to assign. For the same reason
-- this table is NOT added to migration 0050's RLS scoped_tables list — patient identity
-- is global, matching how the existing PACS-sourced etl_patient_view is also unscoped.
--
-- GENDER_KEY resolved to its short code at load time (vendor-provided lookup, 2026-07-27):
--   1=F Female, 2=M Male, 4=U Unknown, 3206=I Intermediate, 3207=0 Not known,
--   3208=NSP Not Specified, 3226=O Other, 3266=A Ambiguous.
-- LANGUAGE_KEY is still pulled RAW (unresolved) — no lookup table provided for it yet.

CREATE TABLE IF NOT EXISTS std_patients_ris (
    patient_person_key           BIGINT PRIMARY KEY,
    birth_date                   DATE,
    birth_time                   TEXT,
    gender_key                   BIGINT,   -- raw source key, kept for traceability
    gender_code                  VARCHAR(8), -- resolved: F/M/U/I/0/NSP/O/A
    mobile_phone_number          TEXT,
    pager_number                 TEXT,
    primary_email_address        TEXT,
    secondary_email_address      TEXT,
    language_key                 BIGINT,   -- unresolved, no lookup provided yet
    multiindexref                TEXT,
    edi_location_number          TEXT,
    edi_send                     TEXT,
    preferred_delivery_method_key BIGINT,
    death_date                   TIMESTAMP,
    death_time                   TEXT,
    death_indicator              TEXT,
    worklist_flagset              TEXT,
    permission_level              TEXT,
    rcopia_patient_last_updated   TIMESTAMP,
    rcopia_allergies_last_updated TIMESTAMP,
    rcopia_medication_last_updated TIMESTAMP,
    deleted                       TEXT,
    deleted_date                  TIMESTAMP,
    person_last_updated           TIMESTAMP,  -- PERSON.LAST_UPDATED (source watermark)
    last_update                   TIMESTAMP NOT NULL DEFAULT NOW()  -- RAYD load timestamp
);

CREATE TABLE IF NOT EXISTS std_patient_ids (
    patient_id_list_key  BIGINT PRIMARY KEY,
    patient_person_key   BIGINT NOT NULL REFERENCES std_patients_ris(patient_person_key) ON DELETE CASCADE,
    patient_id           TEXT,        -- the actual ID value (MRN, national ID, etc.)
    description          TEXT,        -- ID type label — semantics TBD from real data
    is_primary            TEXT,        -- source column named PRIMARY; renamed to avoid
                                        -- any ambiguity with the SQL keyword
    sequence_id            INTEGER,
    issuer_of_pid_key       TEXT,       -- may double as a per-site MRN discriminator —
                                        -- confirm once real values are seen
    display_sort_order      INTEGER,
    last_update              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_patient_ids_person ON std_patient_ids (patient_person_key);
CREATE INDEX IF NOT EXISTS idx_std_patient_ids_value  ON std_patient_ids (patient_id);
