-- Migration 0051: support columns for the LAUMC site-enrichment pass
-- *** LAUMC BRANCH ONLY ***
--
-- The enrichment pass (ETL_JOBS/etl_site_enrichment.py) resolves canonical site_id
-- across studies/orders/hl7 from RIS-authoritative source (org_structure via accession),
-- with the PACS SITE_ID as fallback + a mismatch monitor (catches the SJH-mammo-shows-as-RH
-- bug). This adds the raw PACS site value column + the mismatch audit table.

-- Raw PACS SITE_ID ('0'/'1') pulled from medistore.didb_studies.SITE_ID. Populated once the
-- study ETL is extended to select it; until then it stays NULL and the PACS-fallback +
-- mismatch steps are inert (they skip NULLs), so this migration is safe on its own.
ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS pacs_site_id_raw VARCHAR(32);

-- Audit log for RIS-vs-PACS site disagreements. During the mammo bug, SJH mammo studies
-- carry PACS site '0' (RH) while their RIS order says SJH — this table quantifies it and,
-- once the PACS bug is fixed, should trend to zero (permanent early-warning otherwise).
CREATE TABLE IF NOT EXISTS site_mismatch_log (
    id                BIGSERIAL PRIMARY KEY,
    study_db_uid      BIGINT,
    accession_number  TEXT,
    ris_site_id       INTEGER REFERENCES sites(id),   -- authoritative (from RIS order)
    pacs_site_id      INTEGER REFERENCES sites(id),   -- from PACS SITE_ID (disagreeing)
    study_modality    VARCHAR(50),
    study_date        DATE,
    detected_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mismatch_detected  ON site_mismatch_log (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_mismatch_modality  ON site_mismatch_log (study_modality);
-- One open row per study (re-runs update rather than duplicate).
CREATE UNIQUE INDEX IF NOT EXISTS idx_mismatch_study ON site_mismatch_log (study_db_uid);
