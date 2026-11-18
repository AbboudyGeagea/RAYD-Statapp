-- Report 37 (item 2) spot-check: the first open RAYD shows against the PACS audit trail.
-- Run on the PACS Oracle (SYS or any login that reads AUDITING.AUDIT_LOG), read-only, once.
--
-- 1. Regenerate October in Report 37 and download the workbook.
-- 2. Pick about 6 accessions: ER and another department, early 1 Oct and late 31 Oct, one
--    with "First open" blank ("Not opened by a resident or radiologist...").
-- 3. Put them below and run. Rows are every open (Access) and lock (LOCKED) of each study, in
--    time order. EVENT_DATE is hospital time, as in Report 37.
-- 4. Report 37's "First open" time and login = the first row whose login is a resident,
--    radiologist or mammography reader (a technologist's open does not count). It shows
--    minutes, so it matches the row to the minute.
--
-- Reads through the UPPER(accession) index. Never selects the patient's name or ID, and the
-- description is read only to find the lock and the login, as the nightly ETL does.
SELECT UPPER(TRIM(a.STUDY_ACCESSION_NO)) AS accession,
       TO_CHAR(a.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS') AS event_at,
       CASE WHEN a.ACTION = 'Access' THEN 'open' ELSE 'lock' END AS kind,
       COALESCE(LOWER(TRIM(a.USER_NAME)),
                LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,d6\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,4a\)[^"]*"([^"]*)"', 1, 1, NULL, 1)))) AS login,
       a.SOURCE
FROM auditing.audit_log a
WHERE UPPER(a.STUDY_ACCESSION_NO) IN ('ACC1', 'ACC2', 'ACC3', 'ACC4', 'ACC5', 'ACC6')
  AND a.CATEGORY = 'PHI Access'
  AND a.SUB_CATEGORY = 'Instances Used'
  AND ((a.ACTION = 'Access' AND a.USER_NAME IS NOT NULL)
       OR (a.ACTION = 'Modify' AND a.SOURCE = 'Vue PACS Client'
           AND INSTR(a.DESCRIPTION, 'Value: "LOCKED"') > 0))
ORDER BY 1, a.EVENT_DATE;
