-- Migration 0052: rename storing_ae → original_storing_ae
-- Reason: PACS users can overwrite STORING_AE on Oracle; ORIGINAL_STORING_AE
-- is the immutable value set at study creation and is the correct source for
-- device identification in statistics.

ALTER TABLE etl_didb_studies
    RENAME COLUMN storing_ae TO original_storing_ae;

ALTER TABLE summary_storage_daily
    RENAME COLUMN storing_ae TO original_storing_ae;

-- Rename index so its name stays consistent with the column name.
-- The UNIQUE constraint (_date_ae_mod_proc_uc) tracks by OID and needs no DDL.
ALTER INDEX IF EXISTS idx_etl_didb_studies_storing_ae_date
    RENAME TO idx_etl_didb_studies_original_storing_ae_date;
