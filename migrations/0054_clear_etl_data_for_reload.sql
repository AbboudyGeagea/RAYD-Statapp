-- Migration 0054: wipe all ETL study/patient/order data for a clean reload
-- Run a full manual ETL sync immediately after this migration is applied.
-- update.sh detects this migration and auto-triggers: python app.py -m

TRUNCATE TABLE
    etl_didb_studies,
    etl_patient_view,
    etl_didb_serieses,
    etl_didb_raw_images,
    etl_image_locations,
    etl_orders
RESTART IDENTITY CASCADE;

TRUNCATE TABLE summary_storage_daily RESTART IDENTITY CASCADE;
TRUNCATE TABLE analytics_snapshots   RESTART IDENTITY CASCADE;
