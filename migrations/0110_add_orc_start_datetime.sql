-- Migration 0110: dedicated column for ORC-7.4 (Quantity/Timing, Start date/time).
--
-- ER dashboard's "ER Volume by Hour of Day" chart was anchoring on
-- etl_didb_studies.insert_time, which flattens every ER order onto midnight.
-- Confirmed with HIS/clinical (2026-08-20): ORC-7.4 is the reliable order-start
-- time for ER orders. Kept separate from hl7_orders.scheduled_datetime (which
-- prefers OBR-7 first, ORC-7.4 only as one of several fallbacks) rather than
-- changing that column's existing fallback priority, since other reports already
-- depend on its current semantics.

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS orc_start_datetime TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_hl7_orders_orc_start_datetime
    ON hl7_orders (orc_start_datetime);
