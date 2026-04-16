-- Migration 0004: Seed db_params with required connection entries.
-- Safe to run on any install — uses ON CONFLICT to never overwrite existing data.
--
-- oracle_PACS: fixed params across all installations.
--              host is intentionally left blank so the admin sets it per-site
--              via the Oracle Config page (or by directly updating this row).
-- etl_db:      informational postgres entry.

-- NOTE: Update ORACLE_HOST below for each installation before deploying.
INSERT INTO db_params (name, db_role, db_type, host, username, password, port, sid, mode)
VALUES ('oracle_PACS', 'source', 'oracle', '192.168.2.70', 'sys', '', 1521, 'mst1', 'SYSDBA')
ON CONFLICT (name) DO UPDATE SET
    db_role  = 'source',
    db_type  = 'oracle',
    username = 'sys',
    port     = 1521,
    sid      = 'mst1',
    mode     = 'SYSDBA',
    -- Only set host if currently blank (don't overwrite a configured installation)
    host     = CASE
        WHEN db_params.host IS NULL OR TRIM(db_params.host) = ''
        THEN '192.168.2.70'
        ELSE db_params.host
    END;
    -- password is NOT touched — set via Admin > Oracle Config

INSERT INTO db_params (name, db_role, db_type, conn_string)
VALUES ('etl_db', 'dest', 'postgres', 'postgresql://etl_user:etl_pass@db:5432/etl_db')
ON CONFLICT (name) DO NOTHING;
