-- Remove failed/errored CD burn tasks (TASK_STATUS != 6).
-- TASK_STATUS = 6 is the only "Done" state in CDSURF.TASKS.
DELETE FROM cd_print_log
WHERE task_status IS NOT NULL
  AND task_status != 6;
