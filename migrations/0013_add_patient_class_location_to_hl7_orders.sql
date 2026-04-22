-- Migration 0013: Add patient_class and patient_location to hl7_orders.
-- The hl7_listener INSERT_SQL already writes these fields but the columns
-- were never created, causing the BG technician query to fail silently
-- and the technician tab to remain stuck on "Loading…".

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS patient_class    VARCHAR(50),
    ADD COLUMN IF NOT EXISTS patient_location VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_hl7_orders_patient_class    ON hl7_orders (patient_class);
CREATE INDEX IF NOT EXISTS idx_hl7_orders_patient_location ON hl7_orders (patient_location);
