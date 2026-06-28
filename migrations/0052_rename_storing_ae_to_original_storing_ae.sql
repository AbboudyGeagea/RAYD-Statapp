-- Migration 0052: rename storing_ae → original_storing_ae
-- Reason: PACS users can overwrite STORING_AE on Oracle; ORIGINAL_STORING_AE
-- is the immutable value set at study creation and is the correct source for
-- device identification in statistics.

DO $$ BEGIN
    ALTER TABLE etl_didb_studies RENAME COLUMN storing_ae TO original_storing_ae;
EXCEPTION WHEN undefined_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE summary_storage_daily RENAME COLUMN storing_ae TO original_storing_ae;
EXCEPTION WHEN undefined_column THEN NULL;
END $$;

ALTER INDEX IF EXISTS idx_etl_didb_studies_storing_ae_date
    RENAME TO idx_etl_didb_studies_original_storing_ae_date;
