-- Migration 0160: the RIS order header's HIS order number and booking time (LAUMC items 1, 21).
-- *** LAUMC BRANCH ONLY ***
--
-- Item 1 (HIS orders vs RIS exams, Report 27): a HIS order arrives as an ORM whose ORC-2 is the
-- HIS order number, which the RIS keeps as ORDERS.PLACER_ORDER_NUMBER (found in the July RIS
-- discovery, docs/LAUMC_RIS_TABLES.md; matching on ORC-2 confirmed by the operator 2026-11-17).
-- Item 21 (patient schedule lead time): "booked" is ORDERS.CREATED_ON_DATE (operator, 17 Nov).
-- ETL_JOBS/etl_orders.py copies both nightly, and once for the rows loaded before.

ALTER TABLE public.etl_orders
    ADD COLUMN IF NOT EXISTS placer_order_number TEXT,
    ADD COLUMN IF NOT EXISTS order_created_at    TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_etl_orders_placer_order_number ON public.etl_orders (placer_order_number);

-- rh.etl_orders is SELECT o.*, expanded when the view was made: rebuilt to show the new columns.
SELECT rh_refresh_views();
