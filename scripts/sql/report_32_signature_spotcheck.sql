-- Report 32 (items 14A/14B) spot-check: the signature RAYD shows against the PACS audit trail.
-- Run on the PACS Oracle (SYS or any login that reads AUDITING.AUDIT_LOG), read-only, once.
--
-- 1. On the server, the command in docs/LAUMC_NEXT_SESSION.md (item 14A, "Spot-check") picks 6
--    signed RH studies of the last two weeks, prints RAYD's signature for each (the first
--    approval in the audit copy, utils/sign_tat.py) and a line of the 6 accessions in quotes.
-- 2. Replace 'ACC1', ... 'ACC6' below with that line and run. Rows are every approval and wet
--    read of each study in time order (EVENT_DATE is hospital time).
-- 3. RAYD's signature time and login = the first "approve" row here, to the second. The
--    "wetread" rows are the residents' preliminary reads (Report 32 does not time them yet).
--
-- The approval is status PENDING APPROVED or "ApprovedSign Complete" from the Vue PACS client;
-- the login is the row's user, else the one in the tags, as the nightly copy reads it.
SELECT UPPER(TRIM(a.STUDY_ACCESSION_NO)) AS accession,
       TO_CHAR(a.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS') AS event_at,
       CASE WHEN INSTR(a.DESCRIPTION, 'ApprovedSign Complete') > 0
              OR REGEXP_LIKE(a.DESCRIPTION, '\(07a1,2a\)[^"]*"PENDING APPROVED *"') THEN 'approve'
            ELSE 'wetread' END AS kind,
       COALESCE(LOWER(TRIM(a.USER_NAME)),
                LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,d6\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,4a\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                NULLIF(LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,42\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                       'not assigned')) AS login
FROM auditing.audit_log a
WHERE UPPER(a.STUDY_ACCESSION_NO) IN ('ACC1', 'ACC2', 'ACC3', 'ACC4', 'ACC5', 'ACC6')
  AND a.CATEGORY = 'PHI Access'
  AND a.SUB_CATEGORY = 'Instances Used'
  AND a.ACTION = 'Modify'
  AND a.SOURCE = 'Vue PACS Client'
  AND (INSTR(a.DESCRIPTION, 'ApprovedSign Complete') > 0
       OR REGEXP_LIKE(a.DESCRIPTION, '\(07a1,2a\)[^"]*"(PENDING APPROVED|WETREAD) *"'))
ORDER BY 1, a.EVENT_DATE;
