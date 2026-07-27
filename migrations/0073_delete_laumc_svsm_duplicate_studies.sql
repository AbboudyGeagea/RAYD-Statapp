-- Migration 0073: delete existing 'LAUMC'/'SVSM' storing_ae studies — duplicates.
--
-- Operator instruction (2026-07-27): 'LAUMC' and 'SVSM' are not real imaging
-- devices -- studies land there as duplicates of a real device's AE and should
-- not exist in the DB at all. Studies ETL now excludes both going forward
-- (ETL_JOBS/etl_didb_studies.py); this migration removes what's already loaded.
--
-- etl_didb_serieses / etl_didb_raw_images cascade automatically (ON DELETE CASCADE
-- FKs to etl_didb_studies.study_db_uid, init-db/schema.sql:1499/1507) -- no manual
-- child cleanup needed. std_pps.study_db_uid also references this table but is NOT
-- cascading and is empty on every install seen so far (0 rows) -- if that's no
-- longer true by the time this runs, the DELETE below will fail with a foreign key
-- violation naming std_pps, which is the correct, safe failure mode (surfaced, not
-- silently ignored) rather than something to guess a workaround for here.
--
-- Idempotent: re-running finds 0 matching rows and deletes nothing.

DELETE FROM etl_didb_studies
WHERE UPPER(TRIM(storing_ae)) IN ('LAUMC', 'SVSM');
