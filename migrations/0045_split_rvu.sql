-- 0045: Split rvu_value into clinical_rvu and technical_rvu (IDEMPOTENT).
-- clinical_rvu  → radiologist productivity / physician revenue
-- technical_rvu → device/technician utilisation / facility revenue
-- Both default to 1.0. A legacy rvu_value > 0 seeds both columns.
--
-- WHY THIS WAS REWRITTEN: the original added the columns unconditionally and ran the
-- whole file inside one BEGIN/COMMIT. On any DB where the columns already exist (e.g. a
-- schema.sql dump taken AFTER the split), the very first ALTER failed with
-- "column clinical_rvu already exists" and, because everything shared one transaction,
-- the report_template rewrite at the bottom rolled back too. That left report 25's
-- stored SQL still pointing at the dropped `pm.rvu_value` column → every load of
-- report 25 returned 500 ("column pm.rvu_value does not exist"). The migration also
-- never got recorded as applied (the runner only records successes), so it re-failed on
-- every startup. Every step below is now guarded so the migration completes from any
-- starting state and the report_template fix always lands.
--
-- No explicit BEGIN/COMMIT: db_migrations.py runs each migration file as a single
-- transaction (commit on success, rollback on error), and a stray BEGIN/COMMIT can clash
-- with SQLAlchemy's own transaction handling.

ALTER TABLE procedure_duration_map
    ADD COLUMN IF NOT EXISTS clinical_rvu  NUMERIC(10,2) NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS technical_rvu NUMERIC(10,2) NOT NULL DEFAULT 1.0;

-- Seed the split columns from the legacy rvu_value and drop it — ONLY if it still exists.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'procedure_duration_map' AND column_name = 'rvu_value') THEN
        UPDATE procedure_duration_map
        SET clinical_rvu  = CASE WHEN rvu_value > 0 THEN rvu_value ELSE 1.0 END,
            technical_rvu = CASE WHEN rvu_value > 0 THEN rvu_value ELSE 1.0 END;
        ALTER TABLE procedure_duration_map DROP COLUMN rvu_value;
    END IF;
END $$;

-- Rewrite report 25's stored SQL to return both RVU columns instead of the single 'rvu'
-- alias. Idempotent: the literal replace only affects rows that still contain the old
-- alias, so re-running (or running after the live-DB hotfix) is a harmless no-op.
UPDATE report_template
SET report_sql_query = replace(
    report_sql_query,
    'COALESCE(pm.rvu_value, 1.0) as rvu,',
    E'COALESCE(pm.clinical_rvu,  1.0) AS clinical_rvu,\n    COALESCE(pm.technical_rvu, 1.0) AS technical_rvu,'
)
WHERE report_id = 25;
