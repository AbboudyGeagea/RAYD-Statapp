-- Migration 0058: clear ETL job log for a clean slate after data reload

TRUNCATE TABLE etl_job_log RESTART IDENTITY;
