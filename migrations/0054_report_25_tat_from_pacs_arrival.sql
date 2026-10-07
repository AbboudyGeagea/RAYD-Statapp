-- Report 25: count turnaround time (TAT) from PACS arrival, not from midnight.
--
-- The stored report_template(25) query computed
--     EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date))/60 as total_tat_min
-- study_date is a bare DATE, so every TAT also included the hours between
-- midnight and the scan. etl_didb_studies.insert_time is the time the study
-- arrived in PACS (Oracle DIDB_STUDIES.INSERT_TIME), a real timestamp. Same fix
-- as LAUMC 0091, done here as a targeted replace() because Mazloum's template
-- has no RIS joins; studies without an insert_time keep the old anchor.
--
-- If the text is not found (template edited by hand), nothing changes.
-- Check after deploy (true = fixed):
--   SELECT position('COALESCE(s.insert_time, s.study_date)' in report_sql_query) > 0
--   FROM report_template WHERE report_id = 25
--
-- Rollback:
--   UPDATE report_template SET report_sql_query = replace(report_sql_query,
--       '(s.rep_final_timestamp - COALESCE(s.insert_time, s.study_date))',
--       '(s.rep_final_timestamp - s.study_date)')
--   WHERE report_id = 25

UPDATE report_template
SET report_sql_query = replace(
    report_sql_query,
    '(s.rep_final_timestamp - s.study_date)',
    '(s.rep_final_timestamp - COALESCE(s.insert_time, s.study_date))'
)
WHERE report_id = 25;
