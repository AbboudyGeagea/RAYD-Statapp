-- Migration 0142: remember which RIS orders were left out as not RH
-- *** LAUMC BRANCH ONLY ***
--
-- Operator rule, 2026-11-05: SJH appears in nothing, RH only. ETL_JOBS/etl_rh_only.py
-- removes SJH rows after each load. RIS detail (status history, PPS, reports) carries no
-- site of its own and is only recognisable through its order, and the order itself is
-- removed, so its keys are kept here for later phases and later nights.
--
-- This migration deletes nothing. The ETL does that, in batches, outside app startup:
-- a one-shot delete of studies has run 70+ minutes and needed a Postgres restart before
-- (migration 0111).

CREATE TABLE IF NOT EXISTS etl_excluded_orders (
    order_dbid        BIGINT PRIMARY KEY,   -- RIS SITE_WORKLIST_KEY, as etl_orders.order_dbid
    accession_number  TEXT,
    excluded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_excluded_orders_accession
    ON etl_excluded_orders (accession_number);
