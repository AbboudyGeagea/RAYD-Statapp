-- Migration 0112: carry the raw RIS STATUS_KEY into etl_orders.
--
-- Report 27's "Order Status Mix" renders etl_orders.order_status, which is a
-- lossy translation (ETL_JOBS/etl_orders.py _translate_order_status): 42 RIS
-- status codes collapse into 'CM' / 'CA' plus four stage names. Customers read
-- that chart next to their RIS worklist -- where "Approved", "Exam Done" and
-- "Cancelled by Patient" appear as separate lines -- so a single 'CM' bar equal
-- to their sum reads as wrong even when the arithmetic is right.
--
-- Measured on LAUMC 2026-09-18 (2026-01-01 .. 2026-09-18): one 'CM' bar of
-- 82,413 covers Approved (82,013), Signed 1 (223), Exam Done (102), Pending
-- (58), Signed 2 (16) and Dictated (1). One 'CA' bar of 4,877 hides a split
-- that is genuinely operational -- Cancelled by Patient 2,757 vs Cancelled
-- 2,068 vs Cancelled by OP 52.
--
-- status_key is the join key back to worklist_status_map (migration 0047),
-- which already carries status_name and sort_order. Storing it is therefore
-- enough to render both the aggregated bar and its RIS-status breakdown, in
-- correct lifecycle order, without a second lookup table.
--
-- ADDITIVE ON PURPOSE: order_status stays exactly as it is. Several report
-- files hardcode 'CM'/'CA' comparisons and must keep working untouched.
--
-- BACKFILL REQUIRED. The orders ETL is incremental on LAST_UPDATE_DATE, so
-- rows already in the table keep status_key NULL until a full rebuild:
--
--     RAYD_ETL_ORDERS_FULL_REBUILD=1
--
-- Until that runs, report 27 still draws the correct bars (they come from
-- order_status, unchanged) and reports the breakdown as "Unknown" -- degraded,
-- not broken, and visibly so rather than silently.

ALTER TABLE etl_orders ADD COLUMN IF NOT EXISTS status_key INTEGER;

CREATE INDEX IF NOT EXISTS idx_etl_orders_status_key ON etl_orders (status_key);
