-- Migration: 0003_set_go_live_date_june_2025
-- Originally: unconditionally reset the ETL cutoff from 2000-01-01 to 2025-06-01.
--
-- REWRITTEN (idempotent bootstrap default, never clobbers an operator value):
-- this migration runs at every app boot / `python app.py -m` invocation on a DB
-- where it hasn't been recorded yet (db_migrations.py only records SUCCESS, and
-- a fresh volume always starts with schema_migrations empty). install.sh Step 5
-- starts the containers (which run this migration on Flask import) BEFORE Step 6
-- prompts the operator for the real go-live date and writes it via
-- `TRUNCATE go_live_config; INSERT ...`. That ordering happens to be safe today,
-- but it is fragile: any earlier `python app.py -m` invocation, any future
-- reordering of install.sh, or any recovery flow that skips Step 6 entirely would
-- have let this unconditional UPDATE silently overwrite an already-configured
-- go-live date on every run until it was recorded — which is exactly how LAUMC's
-- operator-entered 2019-01-01 was at risk of reverting to this hardcoded 2025-06-01.
--
-- Guard: only seed the default when the table is genuinely empty. Once ANY row
-- exists (from schema.sql's initial COPY or from install.sh Step 6), this is a
-- permanent no-op.
INSERT INTO go_live_config (go_live_date)
SELECT '2025-06-01'
WHERE NOT EXISTS (SELECT 1 FROM go_live_config);
