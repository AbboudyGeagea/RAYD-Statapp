-- Migration 0010: Add completion and linking columns to hl7_orders.
-- These were previously added inline inside the mark-complete endpoint,
-- causing live_tat() to fail with UndefinedColumn when called before any
-- order was ever marked complete, which in turn aborted the DB transaction
-- and broke the report_25 technician TAT tab.

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS done_at                    TIMESTAMP,
    ADD COLUMN IF NOT EXISTS done_by                    VARCHAR(100),
    ADD COLUMN IF NOT EXISTS linked_accession_number    VARCHAR(100),
    ADD COLUMN IF NOT EXISTS linked_study_db_uid        BIGINT,
    ADD COLUMN IF NOT EXISTS linked_by                  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS linked_at                  TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_hl7_orders_done_at ON hl7_orders (done_at);
