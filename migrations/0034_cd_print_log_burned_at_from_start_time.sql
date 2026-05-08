-- burned_at was incorrectly set from TASKS.END_TIME (burn completion).
-- The correct value is TASKS.START_TIME (burn initiation), which matches PACS audit trail.
-- This migration clears the bad timestamps so the next ETL re-sync pulls correct values.
UPDATE cd_print_log SET burned_at = NULL, synced_at = '2000-01-01' WHERE burned_at IS NOT NULL;
