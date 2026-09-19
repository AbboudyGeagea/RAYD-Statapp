-- Migration 0122: real RIS cancellation timestamps -> std_worklist_cancellations.
--
-- WHY. Report 27's cancelled-exam export dated each cancellation from
-- etl_orders.last_update (SITE_WORKLIST.LAST_UPDATE_DATE), a proxy: it is the row's
-- last change, which for a terminal status is USUALLY the cancellation but drifts the
-- moment anyone edits the order afterwards. "Days notice" built on a proxy is the
-- number people would actually act on, so it should not be a guess.
--
-- WORKLIST_STATUS_HISTORY is the RIS's own status-transition audit log and carries the
-- exact moment. Confirmed against LAUMC Oracle 2026-09-19:
--
--     STATUS_KEY                 transitions   earliest      latest
--     20   Cancelled by RIS              266   2019-01-25    2026-06-26
--     30   Cancelled by OP            47,679   2018-12-11    2026-09-19 19:05
--     50   Cancelled                  21,850   2018-12-11    2026-09-19 16:30
--     1742 Cancelled by Patient       15,622   2018-12-21    2026-09-19 11:31
--
-- Live to the hour the query was run, back to 2018 -- so this covers the full reporting
-- history, not just recent orders.
--
-- THE OTHER FIVE RETURNED NOTHING. 90 (Discontinued), 1300 (Rejected), 1702 (Cancelled
-- by PP), 1830 (Not app) and 2422 (Cancelled Duplicate) have ZERO rows in
-- WORKLIST_STATUS_HISTORY. Two readings -- those statuses are unused at LAUMC, or they
-- are reached by a route that does not write history -- and this migration does not need
-- to decide which: orders in those statuses simply keep the last_update proxy, and the
-- export names which source each row used so the split is visible on real data rather
-- than assumed here.
--
-- SITE_WORKLIST_KEY is etl_orders.order_dbid (ETL_JOBS/etl_orders.py builds order_dbid
-- from it), so this joins straight onto the export with no extra resolution step.
--
-- GRAIN. One row per (worklist entry, status, transition time). status_key is IN the
-- natural key, unlike the sibling tables std_worklist_arrivals / _exam_done / _scheduled
-- which each pull a single hardcoded status and therefore do not need it. An order can
-- also be cancelled, reinstated and cancelled again -- each transition is its own row,
-- and consumers take MAX(cancelled_at) for "when was it cancelled".
--
-- The ETL discovers WHICH statuses to pull from worklist_status_map (is_cancel OR
-- stage = 'discontinued'), never from a hardcoded list -- migration 0047's standing
-- rule. Extend that map and this table follows on the next run with no code change.

CREATE TABLE IF NOT EXISTS std_worklist_cancellations (
    id                 BIGSERIAL PRIMARY KEY,
    site_worklist_key  BIGINT NOT NULL,     -- = etl_orders.order_dbid
    status_key         INTEGER NOT NULL,    -- which cancellation flavour
    cancelled_at       TIMESTAMP NOT NULL,  -- WORKLIST_STATUS_HISTORY.STATUS_TIME
    last_update        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Natural key. Same "upsert on the (key, timestamp) pair, not a source-side id" shape as
-- migration 0090, for the same reason: WORKLIST_STATUS_HISTORY_KEY is unreliably NULL on
-- this source (found 2026-07-31), so it cannot be the conflict target.
CREATE UNIQUE INDEX IF NOT EXISTS uq_worklist_cancellations_key_status_time
    ON std_worklist_cancellations (site_worklist_key, status_key, cancelled_at);

-- The export's lookup path: given an order (and its current status), find the latest
-- matching transition.
CREATE INDEX IF NOT EXISTS idx_worklist_cancellations_lookup
    ON std_worklist_cancellations (site_worklist_key, status_key, cancelled_at DESC);

CREATE INDEX IF NOT EXISTS idx_worklist_cancellations_at
    ON std_worklist_cancellations (cancelled_at);
