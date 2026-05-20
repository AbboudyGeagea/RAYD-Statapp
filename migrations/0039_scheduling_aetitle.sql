-- Migration: 0039_scheduling_aetitle
-- Adds device assignment and cancellation support to scheduling_entries.
-- Adds target device assignment to hl7_orders for scheduling module.

ALTER TABLE scheduling_entries ADD COLUMN IF NOT EXISTS aetitle VARCHAR(50);
ALTER TABLE scheduling_entries ADD COLUMN IF NOT EXISTS cancelled BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE scheduling_entries ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITHOUT TIME ZONE;
ALTER TABLE scheduling_entries ADD COLUMN IF NOT EXISTS cancelled_by VARCHAR(100);

ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS target_aetitle VARCHAR(50);
