-- Report 37 (item 2) spot-check: the first open RAYD shows against the PACS audit trail.
-- Run on the PACS Oracle (SYS or any login that reads AUDITING.AUDIT_LOG), read-only, once.
--
-- 1. On the server, the command in docs/LAUMC_NEXT_SESSION.md (item 2, "Spot-check") picks 6
--    October ER studies spread over the month, prints RAYD's first open for each (Report 37's
--    rule), and a line of the 6 accessions in quotes.
-- 2. Replace 'ACC1', ... 'ACC6' below with that line and run. Rows are every open (Access) and
--    lock (LOCKED) of each study, in time order. EVENT_DATE is hospital time, as in Report 37.
-- 3. RAYD's first open and login = the first row here whose login is a resident, radiologist or
--    mammography reader (a technologist's open does not count), to the second.
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
