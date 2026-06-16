-- Remove historical summary_storage_daily rows for NonDICOMAAgent.
-- etl_analytics_refresh.py now excludes this AE going forward (see 0048),
-- but rows already written to the table must be cleaned up manually.

DELETE FROM summary_storage_daily WHERE storing_ae = 'NonDICOMAAgent';
