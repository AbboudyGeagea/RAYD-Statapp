-- scripts/sql/assigned_radiologist_oracle.sql — item 45 (27 Nov 2026): orders waiting to be read
-- (exam done on the RIS) per ASSIGNED RADIOLOGIST, the RIS custom field "Assign to radiologist".
-- READ-ONLY, run by a person in SQL Developer (F5, "Run Script") connected as the RIS account RAYD
-- reads (db_params oracle_RIS). RAYD never runs it. A section that errors does not stop the others.
--
-- ROUND 1 (27 Nov):
--   - SITE_WORKLIST.ASSIGNRADCODEPERSONKEY: NULL on every exam since June 2026. Not it.
--   - Unread now (current status 100 Exam Done / 2022 Pending): RH 126-128, SJH 121.
-- ROUND 2 (27 Nov):
--   - SITE_ORDERS.ASSIGNRAD: a RESOURCE_ID_KEY (radiologists' names resolve), but on 345 of
--     928,208 orders and on none of today's unread exams. ASSIGNRAD2 never filled.
--   - VWWORKLIST.ASSIGNED_RADIOLOGIST = ORDER_PERSON_REFERENCE type 1703, sequence 1: NULL on all
--     846,323 rows. Not used.
--   - SITE_PPS.ASSIGNED_TO (VARCHAR2, a RESOURCE_ID_KEY) filled on 613,062 of 617,587 steps,
--     ASSIGNED_TO_RES (a second person) on 5,439. RAYD already copies both nightly, full reload
--     (std_site_pps_ext, ETL_JOBS/etl_ris_site_pps.py). THE CANDIDATE.
--   - One radiologist can have several resource keys (Perla Audi 81778 / 81203 / 82780): RAYD
--     must count per person, not per key.
--
-- ROUND 3 (this script): is SITE_PPS.ASSIGNED_TO set BEFORE the report (an assignment), or only
-- the radiologist who read it?
--   F. per current status: how many exams carry ASSIGNED_TO. If unread exams (100 / 2022) carry
--      it, it is set before reading.
--   G. on approved exams of the last 30 days: ASSIGNED_TO against the approving radiologist of
--      the report (same person / different / none). Mostly "different" or a real share of it =
--      an assignment; always "same" = just the reader.
--   H. THE ANSWER through it: unread per hospital and assigned radiologist (per person), with
--      the second person (ASSIGNED_TO_RES) beside it.
--   J. SITE_ORDERS.ASSIGNRAD: on which orders' dates it was used (first / last), to close it.

-- ── F. Which exams carry ASSIGNED_TO, by current status (orders of the last 90 days) ──
SELECT w.STATUS_KEY, MIN(w.STATUS) AS status_label,
       COUNT(*)                    AS exams,
       COUNT(sp.PPS_KEY)           AS with_site_pps_row,
       COUNT(sp.ASSIGNED_TO)       AS with_assigned_to,
       COUNT(sp.ASSIGNED_TO_RES)   AS with_assigned_to_res
FROM site_worklist w
JOIN orders o          ON o.ORDER_KEY = w.ORDER_KEY
LEFT JOIN site_pps sp  ON sp.PPS_KEY = w.PPS_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
  AND o.CREATED_ON_DATE >= TRUNC(SYSDATE) - 90
GROUP BY w.STATUS_KEY
ORDER BY COUNT(*) DESC;

-- ── G. Approved exams, last 30 days: assigned radiologist vs approving radiologist ──
-- Compared per PERSON (one person can have several resource keys).
WITH x AS (
    SELECT o.ISSUER_OF_PLACER_ORDER_NUMBER AS hospital,
           ra.PERSON_KEY AS assigned_person,
           rb.PERSON_KEY AS approver_person
    FROM site_worklist w
    JOIN orders o          ON o.ORDER_KEY = w.ORDER_KEY
    JOIN site_pps sp       ON sp.PPS_KEY = w.PPS_KEY
    JOIN report r          ON r.REPORT_KEY = w.REPORT_KEY AND r.IS_MAX_VERSION = 'Y'
    LEFT JOIN resource_id ra ON TO_CHAR(ra.RESOURCE_ID_KEY) = TRIM(sp.ASSIGNED_TO)
    LEFT JOIN resource_id rb ON rb.RESOURCE_ID_KEY = r.APPROVED_BY_RESOURCE_ID_KEY
    WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
      AND w.STATUS_KEY = 160
      AND r.APPROVED_DATE >= TRUNC(SYSDATE) - 30
)
SELECT hospital,
       COUNT(*)                                                             AS approved,
       SUM(CASE WHEN assigned_person IS NULL THEN 1 ELSE 0 END)             AS no_assigned,
       SUM(CASE WHEN assigned_person = approver_person THEN 1 ELSE 0 END)   AS same_person,
       SUM(CASE WHEN assigned_person <> approver_person THEN 1 ELSE 0 END)  AS different_person,
       SUM(CASE WHEN assigned_person IS NOT NULL AND approver_person IS NULL THEN 1 ELSE 0 END) AS no_approver
FROM x
GROUP BY hospital
ORDER BY hospital;
-- If IS_MAX_VERSION is not 'Y'/'N' here and G returns 0 rows, replace it with = 1 and rerun G.

-- ── H. THE ANSWER: unread per hospital and assigned radiologist (per person) ──
SELECT o.ISSUER_OF_PLACER_ORDER_NUMBER                                AS hospital,
       NVL(MIN(pa.FIRST_NAME || ' ' || pa.LAST_NAME), '(not assigned)') AS assigned_radiologist,
       MIN(ra.RESOURCE_ROLE_KEY)                                      AS role_key,
       COUNT(*)                                                       AS unread,
       COUNT(sp.ASSIGNED_TO_RES)                                      AS with_second_person,
       SUM(CASE WHEN w.PERFORMED_DATE >= TRUNC(SYSDATE)      THEN 1 ELSE 0 END) AS done_today,
       SUM(CASE WHEN w.PERFORMED_DATE <  TRUNC(SYSDATE)
                 AND w.PERFORMED_DATE >= TRUNC(SYSDATE) - 7  THEN 1 ELSE 0 END) AS days_1_7,
       SUM(CASE WHEN w.PERFORMED_DATE <  TRUNC(SYSDATE) - 7  THEN 1 ELSE 0 END) AS older
FROM site_worklist w
JOIN orders o            ON o.ORDER_KEY = w.ORDER_KEY
LEFT JOIN site_pps sp    ON sp.PPS_KEY = w.PPS_KEY
LEFT JOIN resource_id ra ON TO_CHAR(ra.RESOURCE_ID_KEY) = TRIM(sp.ASSIGNED_TO)
LEFT JOIN person pa      ON pa.PERSON_KEY = ra.PERSON_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
  AND w.STATUS_KEY IN (100, 1724, 1726, 1802, 1828, 2022, 2222)
GROUP BY o.ISSUER_OF_PLACER_ORDER_NUMBER, ra.PERSON_KEY
ORDER BY 1, COUNT(*) DESC;

-- ── J. SITE_ORDERS.ASSIGNRAD: when it was used ───────────────────────────────
SELECT o.ISSUER_OF_PLACER_ORDER_NUMBER AS hospital,
       COUNT(*) AS orders_with_assignrad,
       MIN(o.CREATED_ON_DATE) AS first_order, MAX(o.CREATED_ON_DATE) AS last_order
FROM site_orders so
JOIN orders o ON o.ORDER_KEY = so.ORDER_KEY
WHERE so.ASSIGNRAD IS NOT NULL
GROUP BY o.ISSUER_OF_PLACER_ORDER_NUMBER;
