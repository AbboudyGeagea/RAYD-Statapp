-- Migration 0056: widen reading_physician_id / signing_physician_id from BIGINT to TEXT.
--
-- These columns assumed every site's PACS emits a pure numeric physician ID. LAUMC's
-- Oracle source instead emits a composite AD-login string, e.g.
-- "kamal.tarabine@ad.umcrh.com_841630390". chunked_upsert() (db.py) validates numeric
-- columns with float() and NULLs anything that fails — so every LAUMC study was
-- silently loaded with reading_physician_id / signing_physician_id = NULL.
--
-- Any row already loaded under the old BIGINT column has NULL here already (that's
-- exactly what the "non-numeric value ... -> NULL" warnings mean — nothing meaningful
-- is lost by this type change; it only stops future loads from being nulled). Re-run
-- ETL Phase 1 after this migration to backfill the real values.
--
-- No USING clause needed: Postgres has a built-in assignment cast bigint::text.
ALTER TABLE etl_didb_studies
    ALTER COLUMN reading_physician_id TYPE TEXT,
    ALTER COLUMN signing_physician_id TYPE TEXT;
