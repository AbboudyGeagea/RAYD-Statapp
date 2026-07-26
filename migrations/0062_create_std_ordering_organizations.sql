-- Migration 0062: create std_ordering_organizations (RIS ORDERING_ORGANIZATION table,
-- LAUMC). Forward-looking — not blocking anything today, added per operator request
-- ("we might need it later"). Resolves ORDERS.ordering_organization_key (already
-- captured on std_orders) to a real referring clinic/organization: name, address,
-- phone/fax — the clinic-level counterpart to referring-physician contact (still
-- blocked on the PERSON/RESOURCE table). Feeds CRN routing per docs/LAUMC_RIS_TABLES.md.
--
-- NAME here is an ORGANIZATION name (a clinic), not a patient's — unrelated to the
-- "no patient names" rule (migration 0060), which is specifically about PHI.
--
-- Reference/catalog data like MODALITY/SPS_CODE: no watermark filter, full pull,
-- refresh-on-conflict (no RAYD-owned fields here to protect, unlike
-- aetitle_modality_map/procedure_duration_map's fill-only policy).
--
-- Created directly in the main etl_db — same reasoning as std_visits/std_patients_ris.

CREATE TABLE IF NOT EXISTS std_ordering_organizations (
    ordering_organization_key  BIGINT PRIMARY KEY,
    code                       TEXT,
    name                       TEXT,   -- organization/clinic name, not a patient name
    active                     BOOLEAN,
    coding_scheme              TEXT,
    alternate_code             TEXT,
    street_address             TEXT,
    other_designation          TEXT,
    city                       TEXT,
    province                   TEXT,
    country                    TEXT,
    postal_code                TEXT,
    phone_number               TEXT,
    fax_number                 TEXT,
    edi_location_number        TEXT,
    edi_send                   TEXT,
    source_last_updated        TIMESTAMP,  -- ORDERING_ORGANIZATION.LAST_UPDATED
    last_update                TIMESTAMP NOT NULL DEFAULT NOW()  -- RAYD load timestamp
);
