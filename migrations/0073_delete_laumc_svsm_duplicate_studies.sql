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
--
-- BATCHED (2026-07-27, after the first version ran for 70+ minutes as one giant
-- transaction and eventually had to be aborted via a Postgres restart, taking the
-- whole app down with it): db_migrations.py commits once per FILE, not once per
-- statement, so simply writing several DELETE statements here would still bundle
-- into one long transaction -- no different from before. A DO block can control its
-- own transaction boundaries (COMMIT inside PL/pgSQL, supported since PG11), so this
-- deletes 5,000 studies at a time, committing -- and releasing every lock -- after
-- each batch. Worst case now is losing one 5,000-row batch, not a whole hour.

DO $$
DECLARE
    deleted_count INT;
    total_deleted INT := 0;
BEGIN
    LOOP
        DELETE FROM etl_didb_studies
        WHERE study_db_uid IN (
            SELECT study_db_uid FROM etl_didb_studies
            WHERE UPPER(TRIM(storing_ae)) IN ('LAUMC', 'SVSM')
            LIMIT 5000
        );
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        total_deleted := total_deleted + deleted_count;
        RAISE NOTICE 'etl_didb_studies LAUMC/SVSM cleanup: % rows this batch, % total', deleted_count, total_deleted;
        COMMIT;
        EXIT WHEN deleted_count = 0;
    END LOOP;
END $$;
