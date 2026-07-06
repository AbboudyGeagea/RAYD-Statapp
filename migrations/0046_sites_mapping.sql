-- Migration 0046: sites mapping table (multi-site foundation for LAUMC)
--
-- LAUMC is the first multi-site RAYD deployment: one organization, two hospitals
-- (Rafic Hariri "RH" = main, Saint John "SJH" = satellite). Each source system
-- labels the site with a DIFFERENT value, so this table is the single translator:
--
--   canonical site  |  PACS DB (didb_studies.site_id) | RIS DB (issuer) | HL7 (PV1 building)
--   ----------------|--------------------------------|-----------------|-------------------
--   RH  (main)      |  '0'                           | 'SAP_PROD'      | '1000'
--   SJH (satellite) |  '1'                           | 'SAP_SJH'       | '2000'
--
-- Every ETL/ingest path resolves site by looking up ITS OWN vocabulary column here,
-- then stores the canonical `sites.id`. Source values may change at the ~2027
-- RIS/PACS upgrade — when that happens, only these rows are updated, never code.
-- Single-site deployments simply have one row and see no behavioural change.

CREATE TABLE IF NOT EXISTS sites (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(16)  NOT NULL UNIQUE,   -- short canonical code, e.g. 'RH', 'SJH'
    name            VARCHAR(200) NOT NULL,          -- display name
    is_default      BOOLEAN NOT NULL DEFAULT FALSE, -- the site used when a source carries no marker
    -- source-system vocabularies (nullable: a site may not exist in every source)
    pacs_site_id    VARCHAR(32),                    -- didb_studies.site_id value ('0','1',...)
    ris_issuer      VARCHAR(64),                    -- ISSUER_OF_PLACER_ORDER_NUMBER ('SAP_PROD',...)
    hl7_building    VARCHAR(32),                    -- PV1 building code ('1000','2000',...)
    ris_org_struct  VARCHAR(32),                    -- ORG_STRUCTURE_KEY cross-check ('3926','5320')
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Fast reverse lookups from each source vocabulary to the canonical site.
CREATE INDEX IF NOT EXISTS idx_sites_pacs   ON sites (pacs_site_id) WHERE pacs_site_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sites_issuer ON sites (ris_issuer)   WHERE ris_issuer   IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sites_bldg   ON sites (hl7_building) WHERE hl7_building  IS NOT NULL;

-- Only one site may be the default (the site assumed when a source row has no marker,
-- e.g. an existing single-site PACS that stamps site_id='0' everywhere).
CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_one_default ON sites (is_default) WHERE is_default;

-- ---------------------------------------------------------------------------
-- Seed LAUMC's two sites. On single-site installs, replace with a single
-- default row (or leave empty and let the app treat "no sites" as single-site).
-- ---------------------------------------------------------------------------
INSERT INTO sites (code, name, is_default, pacs_site_id, ris_issuer, hl7_building, ris_org_struct)
VALUES
    ('RH',  'LAUMC - Rafic Hariri (Main)',      TRUE,  '0', 'SAP_PROD', '1000', '3926'),
    ('SJH', 'LAUMC - Saint John (Satellite)',   FALSE, '1', 'SAP_SJH',  '2000', '5320')
ON CONFLICT (code) DO NOTHING;
