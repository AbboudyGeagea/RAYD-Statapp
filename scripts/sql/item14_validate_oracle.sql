-- scripts/sql/item14_validate_oracle.sql — item 14d: RAYD's radiologist figures against the PACS
-- and RIS on one day (Reports 32 and 36, utils/tat_bands.py). READ-ONLY, run by a person in SQL
-- Developer (F5, "Run Script"); RAYD never runs it. Pick a working day, put it in DEFINE below.
--
-- What is compared, with the same definitions RAYD uses:
--   1. PACS: approvals and wet reads per PACS login on that day, each study once, on the day of
--      its first approval / wet read (looked back 30 days for an earlier one). RAYD's side:
--      the first command in docs/LAUMC_NEXT_SESSION.md, item 14d (both hospitals' databases,
--      added up: the PACS audit names no hospital). Expect the same numbers per login; a few
--      more here is normal for studies RAYD keeps out (structured reports, cardiology).
--   2. PACS: for 10 studies RAYD picked (the second command there prints them), every approval
--      and wet read in time order. RAYD's wet read and approval = the first row of each kind
--      here, to the second, with the same login.
--   3. RIS: exam done for the same 10 studies' worklist keys (RAYD's RIS exam done = the
--      earliest here). Connect as the RIS account RAYD reads (db_params oracle_RIS), or put the
--      RIS schema in front of the table name.
--
-- The approval is status PENDING APPROVED or "ApprovedSign Complete" from the Vue PACS client,
-- the wet read status WETREAD; the login is the row's user, else the one in the tags, as the
-- nightly copy (ETL_JOBS/etl_pacs_audit.py) reads it. Every read of the audit goes through its
-- EVENT_DATE index (217 M rows).

DEFINE day = '2026-11-17'

-- ── 1. Approvals and wet reads per login on that day ─────────────────────────
WITH ev AS (
    SELECT /*+ INDEX(a AUDIT_EVENT_DATE_INX) */
           TRIM(a.STUDY_INSTANCE_UID) AS uid, a.EVENT_DATE, a.LOG_DBID,
           CASE WHEN INSTR(a.DESCRIPTION, 'ApprovedSign Complete') > 0
                  OR REGEXP_LIKE(a.DESCRIPTION, '\(07a1,2a\)[^"]*"PENDING APPROVED *"') THEN 'approve'
                ELSE 'wetread' END AS kind,
           COALESCE(LOWER(TRIM(a.USER_NAME)),
                    LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,d6\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                    LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,4a\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                    NULLIF(LOWER(TRIM(REGEXP_SUBSTR(a.DESCRIPTION, '\(07a1,42\)[^"]*"([^"]*)"', 1, 1, NULL, 1))),
                           'not assigned')) AS login
    FROM auditing.audit_log a
    WHERE a.EVENT_DATE >= TO_DATE('&&day', 'YYYY-MM-DD') - 30
      AND a.EVENT_DATE <  TO_DATE('&&day', 'YYYY-MM-DD') + 1
      AND a.CATEGORY = 'PHI Access'
      AND a.SUB_CATEGORY = 'Instances Used'
      AND a.STUDY_INSTANCE_UID IS NOT NULL
      AND a.ACTION = 'Modify'
      AND a.SOURCE = 'Vue PACS Client'
      AND (INSTR(a.DESCRIPTION, 'ApprovedSign Complete') > 0
           OR REGEXP_LIKE(a.DESCRIPTION, '\(07a1,2a\)[^"]*"(PENDING APPROVED|WETREAD) *"'))
),
firsts AS (
    SELECT kind, uid, login, EVENT_DATE,
           ROW_NUMBER() OVER (PARTITION BY kind, uid ORDER BY EVENT_DATE, LOG_DBID) AS rn
    FROM ev
)
SELECT kind, login, COUNT(*) AS studies
FROM firsts
WHERE rn = 1 AND EVENT_DATE >= TO_DATE('&&day', 'YYYY-MM-DD')
GROUP BY kind, login
ORDER BY kind, studies DESC, login;

-- ── 2. Every approval and wet read of the 10 studies RAYD picked ──────────────
-- Replace 'ACC1', ... 'ACC10' with the line of accessions RAYD's second command prints.
SELECT /*+ INDEX(a AUDIT_EVENT_DATE_INX) */
       UPPER(TRIM(a.STUDY_ACCESSION_NO)) AS accession,
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
WHERE a.EVENT_DATE >= TO_DATE('&&day', 'YYYY-MM-DD') - 30
  AND a.EVENT_DATE <  TO_DATE('&&day', 'YYYY-MM-DD') + 2
  AND UPPER(TRIM(a.STUDY_ACCESSION_NO)) IN ('ACC1', 'ACC2', 'ACC3', 'ACC4', 'ACC5',
                                             'ACC6', 'ACC7', 'ACC8', 'ACC9', 'ACC10')
  AND a.CATEGORY = 'PHI Access'
  AND a.SUB_CATEGORY = 'Instances Used'
  AND a.ACTION = 'Modify'
  AND a.SOURCE = 'Vue PACS Client'
  AND (INSTR(a.DESCRIPTION, 'ApprovedSign Complete') > 0
       OR REGEXP_LIKE(a.DESCRIPTION, '\(07a1,2a\)[^"]*"(PENDING APPROVED|WETREAD) *"'))
ORDER BY 1, a.EVENT_DATE, a.LOG_DBID;

-- ── 3. RIS exam done of the same studies (RIS Oracle) ─────────────────────────
-- Replace 0 with the worklist keys RAYD's second command prints (ris_worklist_keys column).
SELECT wsh.SITE_WORKLIST_KEY,
       TO_CHAR(MIN(wsh.STATUS_TIME), 'YYYY-MM-DD HH24:MI:SS') AS exam_done
FROM WORKLIST_STATUS_HISTORY wsh
WHERE wsh.STATUS_KEY = 100                       -- Exam Done
  AND wsh.SITE_WORKLIST_KEY IN (0)
GROUP BY wsh.SITE_WORKLIST_KEY
ORDER BY 1;

UNDEFINE day
