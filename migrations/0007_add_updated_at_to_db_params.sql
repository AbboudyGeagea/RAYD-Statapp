-- Migration 0007: Add updated_at column to db_params.
-- Required by the DB Manager edit functionality.

ALTER TABLE db_params ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
