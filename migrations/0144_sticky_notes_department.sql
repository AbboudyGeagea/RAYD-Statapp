-- Migration 0144: where a case came from, for the Sticky Notes page (routes/sticky_notes.py).
--
-- The page lets an admin mark departments as urgent, so it needs a real department for
-- each study. At RH the RIS ordering organization has one far more often than PACS does:
-- Generic on 27% of inpatient exams, against DEFLOC on about 60% in PACS.
--
-- etl_orders.ordering_organization_key  ORDERS.ORDERING_ORGANIZATION_KEY; the names are
--     already in std_ordering_organizations. Orders loaded earlier stay empty until a full
--     reload of Phase 6 (RAYD_ETL_ORDERS_FULL_REBUILD), which reads the RIS, not PACS.
-- etl_didb_studies.patient_location_full  the whole PATIENT_LOCATION. The existing column is
--     cut to 3 characters, which turns MSU3 to MSU7 into MSU; it stays as it is because
--     reports read it. Filled on studies in the nightly 10-day window from now on, so older
--     studies still show the short code when the RIS has no department for them.

ALTER TABLE etl_orders ADD COLUMN IF NOT EXISTS ordering_organization_key BIGINT;

ALTER TABLE etl_didb_studies ADD COLUMN IF NOT EXISTS patient_location_full TEXT;
