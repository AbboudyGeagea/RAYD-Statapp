-- 0045: Split rvu_value into clinical_rvu and technical_rvu
-- clinical_rvu  → radiologist productivity / physician revenue
-- technical_rvu → device/technician utilisation / facility revenue
-- Both default to 1.0. Existing rvu_value > 0 seeds both columns.

BEGIN;

ALTER TABLE procedure_duration_map
    ADD COLUMN clinical_rvu  NUMERIC(10,2) NOT NULL DEFAULT 1.0,
    ADD COLUMN technical_rvu NUMERIC(10,2) NOT NULL DEFAULT 1.0;

UPDATE procedure_duration_map
SET clinical_rvu  = CASE WHEN rvu_value > 0 THEN rvu_value ELSE 1.0 END,
    technical_rvu = CASE WHEN rvu_value > 0 THEN rvu_value ELSE 1.0 END;

ALTER TABLE procedure_duration_map DROP COLUMN rvu_value;

-- Update the report_25 SQL template (stored in DB) to return both RVU columns
-- instead of the single 'rvu' alias.
UPDATE report_template
SET report_sql_query = replace(
    report_sql_query,
    'COALESCE(pm.rvu_value, 1.0) as rvu,',
    E'COALESCE(pm.clinical_rvu,  1.0) AS clinical_rvu,\n    COALESCE(pm.technical_rvu, 1.0) AS technical_rvu,'
)
WHERE report_id = 25;

COMMIT;
