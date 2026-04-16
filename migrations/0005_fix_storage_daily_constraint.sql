-- Migration 0005: Drop the redundant 3-column unique constraint on
-- summary_storage_daily that conflicts with the correct 4-column constraint.
--
-- The upsert in etl_analytics_refresh.py uses ON CONFLICT on
-- (study_date, storing_ae, modality, procedure_code) — the 4-column constraint
-- _date_ae_mod_proc_uc. The 3-column constraint fires first and crashes the ETL.

ALTER TABLE summary_storage_daily
    DROP CONSTRAINT IF EXISTS summary_storage_daily_study_date_modality_procedure_code_key;
