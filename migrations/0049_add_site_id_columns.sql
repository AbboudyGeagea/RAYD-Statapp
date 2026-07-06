-- Migration 0049: add site_id to core ETL + HL7 tables (LAUMC multi-site)
--
-- Adds a nullable site_id FK to every table that carries per-site data. Nullable by
-- design: existing single-site deployments leave it NULL (interpreted as the default
-- site), so this migration is a no-op for them. LAUMC's ETL/ingest populate it by
-- resolving each source's own vocabulary through the `sites` table (migration 0046).
--
-- site_id is the column row-level security policies will filter on (added in a later
-- migration once the app-role split is in place). Indexed with study_date / common
-- filters so per-site queries stay cheap even at 600k+ studies.

-- ---- PACS studies ---------------------------------------------------------
ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);
CREATE INDEX IF NOT EXISTS idx_studies_site_date
    ON etl_didb_studies (site_id, study_date);

-- ---- RIS orders -----------------------------------------------------------
ALTER TABLE etl_orders
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);
CREATE INDEX IF NOT EXISTS idx_orders_site
    ON etl_orders (site_id);

-- ---- HL7 orders (live board / floor map) ----------------------------------
ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);
CREATE INDEX IF NOT EXISTS idx_hl7orders_site
    ON hl7_orders (site_id);

-- ---- HL7 ORU reports ------------------------------------------------------
-- ORU carries no site marker; site is enriched by accession lookup after insert,
-- so this stays NULL until enrichment (fail-closed: NULL is invisible to site-scoped
-- users, visible to all-site admins).
ALTER TABLE hl7_oru_reports
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);
CREATE INDEX IF NOT EXISTS idx_oru_site
    ON hl7_oru_reports (site_id);

-- ---- AE title -> modality map (per-device config is per-site) --------------
ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);

-- ---- Analytics snapshots (must be built per-site) --------------------------
-- Add the column now (safe on all installs). NOTE: making snapshots truly per-site
-- also requires widening the PK from (snapshot_date) to (snapshot_date, site_id).
-- That PK restructure is DEFERRED to the analytics-refresh rework, because a composite
-- PK with a nullable site_id needs a NULL strategy that only matters once LAUMC's
-- per-site snapshot job exists. Single-site installs keep snapshot_date as PK unchanged.
ALTER TABLE analytics_snapshots
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id);
