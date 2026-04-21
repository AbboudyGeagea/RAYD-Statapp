-- Migration 0012: Add pacs_done_at to hl7_orders
-- Stores the timestamp from PACS completion ORM^O01 (ORC-1=SC, ORC-5=CM) messages.
-- TAT (Done)      = done_at      - scheduled_datetime  (manual technician done)
-- TAT (PACS Done) = pacs_done_at - scheduled_datetime  (PACS/scanner confirmation)

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS pacs_done_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_hl7_orders_pacs_done_at ON hl7_orders (pacs_done_at);
