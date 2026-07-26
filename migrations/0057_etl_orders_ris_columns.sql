-- Migration 0057: etl_orders now sourced from RIS (SITE_WORKLIST/ORDERS/SPS_CODE), not
-- PACS MEDILINK.MDB_ORDERS. Same table, same columns consumers already read (order_dbid,
-- patient_dbid, study_db_uid, proc_id, proc_text, scheduled_datetime, order_status,
-- modality, has_study, order_control, visit_dbid, study_instance_uid) — this migration
-- only ADDS the two columns the new RIS-sourced loader needs to resolve study_db_uid
-- after load (the RIS↔PACS join isn't available inline at extract time; it's resolved by
-- a separate idempotent enrichment pass within the same ETL job, matching the site's own
-- documented "extract independently, enrich once" ETL design).
--
-- accession_number = SITE_WORKLIST.SPS_ID — the primary RIS<->PACS join key
--   (SPS_ID = etl_didb_studies.accession_number, vendor-confirmed).
-- linked_id         = SITE_WORKLIST.LINKED_ID — groups multiple SPS rows that were
--   scheduled together (e.g. CT abdomen + pelvis) and may resolve to fewer PACS studies
--   than SPS rows. Used as a fallback when an SPS's own accession_number has no direct
--   PACS match but a sibling SPS sharing the same linked_id does.

ALTER TABLE etl_orders
    ADD COLUMN IF NOT EXISTS accession_number TEXT,
    ADD COLUMN IF NOT EXISTS linked_id        BIGINT;

CREATE INDEX IF NOT EXISTS idx_etl_orders_accession_number ON etl_orders (accession_number);
CREATE INDEX IF NOT EXISTS idx_etl_orders_linked_id        ON etl_orders (linked_id);
