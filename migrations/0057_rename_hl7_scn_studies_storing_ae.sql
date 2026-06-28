-- Migration 0057: rename hl7_scn_studies.storing_ae → original_storing_ae
-- The HL7 SCN table was not covered by migration 0052 (which only renamed
-- etl_didb_studies and summary_storage_daily). This brings it in line.

DO $$ BEGIN
    ALTER TABLE hl7_scn_studies RENAME COLUMN storing_ae TO original_storing_ae;
EXCEPTION WHEN undefined_column THEN NULL;
END $$;
