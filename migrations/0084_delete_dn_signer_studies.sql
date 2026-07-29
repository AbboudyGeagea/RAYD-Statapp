-- Migration 0084: delete studies signed by an '@dn' account -- not real LAUMC data.
--
-- Operator instruction (2026-07-29): 36 distinct '@dn'-suffixed values in
-- etl_didb_studies.rep_final_signed_by (akehdi@dn, dbustros@dn, eva.d@dn, mi70@dn,
-- etc. -- 160 studies total), all under pacs_site_id_raw=0 (the same raw code
-- documented as LAUMC-RH). Confirmed these are not legitimate LAUMC data and should
-- be removed entirely, not just hidden from report display.
--
-- Only 160 rows -- unlike migration 0073's 9,582-row SVSM/LAUMC cleanup, this does
-- NOT need batching, so it's a single plain DELETE with no internal COMMIT. That
-- also sidesteps the "invalid transaction termination" incompatibility discovered
-- 2026-07-29: a DO block with its own COMMIT can only run via `psql -f`'s
-- autocommit-per-statement behaviour, never through db_migrations.py's
-- raw_connection() (not autocommit), so 0073 fails every time under the app's own
-- runner. A plain top-level DELETE has no such restriction.
--
-- Idempotent: re-running finds 0 matching rows and deletes nothing.
--
-- etl_didb_serieses / etl_didb_raw_images cascade automatically (ON DELETE CASCADE
-- FKs to etl_didb_studies.study_db_uid). std_pps references this table but does NOT
-- cascade and was empty on every install checked so far -- if that's no longer true,
-- this DELETE fails with a foreign key violation naming std_pps, which is the
-- correct, safe failure mode rather than something to silently work around here.

DELETE FROM etl_didb_studies
WHERE UPPER(rep_final_signed_by) LIKE '%@DN';
