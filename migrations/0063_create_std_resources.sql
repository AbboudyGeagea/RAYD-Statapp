-- Migration 0063: create std_resources_ris (RIS RESOURCE_ID -> staff/physician
-- identity, LAUMC), plus a reading/signing-physician enrichment pass.
--
-- Source (vendor ER diagram, 2026-07-27): RESOURCE_ID.RESOURCE_ID_KEY is what
-- REPORT/ORDERS/SITE_WORKLIST's *_RESOURCE_ID_KEY columns actually reference
-- (APPROVED_BY_RESOURCE_ID_KEY, REQUESTED_BY_RESOURCE_ID_KEY, ASSIGNRADCODEPERSONKEY,
-- etc. — vendor-confirmed "connected to report and orders") — NOT PERSON_KEY directly.
-- RESOURCE_ID.PERSON_KEY bridges to the SAME shared PERSON table already used for
-- patients (migration 0060), which is how one physical person can be both a
-- radiologist (one RESOURCE_ID row, one RESOURCE_ROLE_KEY) and, separately, a patient.
--
-- Unlike std_patients_ris, THIS table DOES carry names + contact info. The "no patient
-- names" rule (migration 0060) is specifically about patient PHI; staff/physician
-- identity is operationally required (KPI report needs radiologist names; the CRN needs
-- referring-physician email/phone — both sit on PERSON and are pulled here).
--
-- RESOURCE_ID.RESOURCE_ID (the external composite string, e.g.
-- "kamal.tarabine@ad.umcrh.com_841630390") is the SAME format/value already sitting,
-- unresolved, in etl_didb_studies.reading_physician_id / .signing_physician_id
-- (migration 0056 widened those from BIGINT to TEXT for exactly this composite format).
-- The enrichment pass below matches on that value to resolve both columns to a real
-- resource_id_key — a proper FK, not duplicated name text — so any report needing the
-- name joins std_resources_ris at query time.
--
-- ORG_STRUCTURE_KEY is resolved to site_id via site_org_map (migration 0052), same
-- pattern as the RIS Modality loader — unlike std_visits, this table actually carries
-- a real org/site column to resolve from.
--
-- RESOURCE_ROLE_KEY is pulled RAW (unresolved) — no role lookup table (Radiologist vs
-- Technician vs Referring, etc.) provided yet.
--
-- No FK constraint from person_key to std_patients_ris(patient_person_key): the two
-- tables represent different populations (staff vs patients) even though PERSON is
-- shared, and there's no guarantee every resource has a corresponding patient row (or
-- vice versa).
--
-- Created directly in the main etl_db — same reasoning as std_visits/std_patients_ris.

CREATE TABLE IF NOT EXISTS std_resources_ris (
    resource_id_key                BIGINT PRIMARY KEY,
    person_key                     BIGINT,
    resource_role_key              BIGINT,   -- unresolved, no role lookup provided yet
    resource_id                    TEXT,     -- external composite ID — matches
                                              -- etl_didb_studies.reading/signing_physician_id
    assigning_authority            TEXT,
    org_structure_key              TEXT,     -- raw source key, kept for traceability
    site_id                        INTEGER REFERENCES sites(id),  -- resolved via site_org_map
    pref_rpt_delivery_method_key   BIGINT,
    pref_img_delivery_method_key   BIGINT,
    active                         BOOLEAN,
    alternate_id                   TEXT,
    source_last_updated            TIMESTAMP,  -- RESOURCE_ID.LAST_UPDATED
    -- PERSON demographics (staff — names included, see note above)
    last_name                      TEXT,
    first_name                     TEXT,
    middle_name                    TEXT,
    name_prefix                    TEXT,
    name_suffix                    TEXT,
    name_representation_code       TEXT,
    common_name                    TEXT,
    mobile_phone_number            TEXT,
    pager_number                   TEXT,
    primary_email_address          TEXT,
    secondary_email_address        TEXT,
    deleted                        TEXT,
    deleted_date                   TIMESTAMP,
    person_last_updated            TIMESTAMP,
    last_update                    TIMESTAMP NOT NULL DEFAULT NOW()  -- RAYD load timestamp
);

CREATE INDEX IF NOT EXISTS idx_std_resources_ris_person   ON std_resources_ris (person_key);
CREATE INDEX IF NOT EXISTS idx_std_resources_ris_resource_id ON std_resources_ris (resource_id);

-- Reading/signing physician resolution (see ETL_JOBS/etl_ris_resources.py for the
-- enrichment UPDATE that populates these).
ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS reading_physician_resource_key BIGINT,
    ADD COLUMN IF NOT EXISTS signing_physician_resource_key BIGINT;
