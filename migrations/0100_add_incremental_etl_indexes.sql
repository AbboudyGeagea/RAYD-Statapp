-- Migration 0100: index the watermark columns Phase 6 (Orders) and Phase 7 (Storage
-- Summary) now use to run incrementally instead of replaying the full go-live window
-- on every sync (ETL_JOBS/etl_orders.py, ETL_JOBS/etl_analytics_refresh.py).
--
-- etl_orders.last_update: incremental Orders ETL computes MAX(last_update) every run
-- to find its watermark -- a full sequential scan of the table without this index.
--
-- etl_image_locations.last_update: incremental Storage Summary filters
-- "WHERE last_update >= :watermark" to find which studies got new/changed image data
-- since the last successful rollup. This table is by far the largest in the schema
-- (100M+ rows per etl_analytics_refresh.py's docstring) -- without an index this
-- WHERE clause is a full scan of the whole table every night, which would erase most
-- of the point of making the rollup incremental. Note this column is deliberately
-- NOT touched by etl_image_locations.py's upsert on conflict (excluded from its
-- col_names' UPDATE SET), so it holds each row's true first-seen-in-Postgres time,
-- not an ETL-touch timestamp that gets bumped by every re-sync.

CREATE INDEX IF NOT EXISTS idx_etl_orders_last_update
    ON etl_orders (last_update);

CREATE INDEX IF NOT EXISTS idx_img_loc_last_update
    ON etl_image_locations (last_update);
