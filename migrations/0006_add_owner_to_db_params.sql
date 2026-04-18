-- Migration 0006: Add owner column to db_params for schema owner (e.g. MEDISTORE).
-- Also adds database_name as an alias to sid for non-Oracle types.
-- Safe to run multiple times.

ALTER TABLE db_params ADD COLUMN IF NOT EXISTS owner VARCHAR(100);

-- Backfill: oracle_PACS had MEDISTORE as its implied owner historically
UPDATE db_params SET owner = 'MEDISTORE' WHERE name = 'oracle_PACS' AND owner IS NULL;
