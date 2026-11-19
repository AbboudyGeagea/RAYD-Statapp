-- Item 1, optional: the room the RIS booked an order on, for orders not performed yet.
-- Run on the RIS Oracle in SQL Developer, as the RIS login RAYD uses (the tables unqualified,
-- as in ETL_JOBS/etl_orders.py), read-only, once. Select nothing about the patient.
--
-- Today Report 27's HIS vs RIS tab guesses the room of an order not performed yet (the room the
-- hospital used most for that procedure over 90 days, else its only bookable room). The RIS
-- itself knows the booked room: SPS.MODALITY_KEY -> MODALITY (the device, the same key the
-- performed step PPS carries). RAYD does not copy SPS yet. This tells whether it is worth it.
--
-- Send back the output of each step. Step 1 reads the data dictionary only. Steps 2-4 read
-- 30-90 days of orders and join SPS on SPS_ID (as the old live page did); if step 1 shows no
-- index on SPS starting with SPS_ID, run them after hours.

-- ── Step 1: SPS's size, columns and indexes (dictionary only) ───────────────
SELECT owner, table_name, num_rows, last_analyzed
FROM all_tables
WHERE table_name IN ('SPS', 'SITE_WORKLIST', 'PPS', 'MODALITY')
ORDER BY table_name, owner;

SELECT owner, column_id, column_name, data_type, nullable
FROM all_tab_columns
WHERE table_name = 'SPS'
ORDER BY owner, column_id;

SELECT table_owner, index_name, column_position, column_name
FROM all_ind_columns
WHERE table_name = 'SPS'
ORDER BY table_owner, index_name, column_position;

-- ── Step 2: how many SPS rows each worklist row has (orders of the last 90 days) ──
-- Expect "1" nearly everywhere. "0" = booked nowhere; "2", "3+" = SPS keeps several rows per
-- exam (reschedules?), and the copy must then pick one.
WITH w AS (
    SELECT w.SITE_WORKLIST_KEY, w.SPS_ID, o.ISSUER_OF_PLACER_ORDER_NUMBER AS issuer
    FROM SITE_WORKLIST w
    JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
    WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
      AND o.CREATED_ON_DATE >= TRUNC(SYSDATE) - 90
), n AS (
    SELECT w.issuer, w.SITE_WORKLIST_KEY, COUNT(s.SPS_ID) AS sps_rows
    FROM w
    LEFT JOIN SPS s ON s.SPS_ID = w.SPS_ID
    GROUP BY w.issuer, w.SITE_WORKLIST_KEY
)
SELECT issuer,
       CASE WHEN sps_rows >= 3 THEN '3+' ELSE TO_CHAR(sps_rows) END AS sps_rows_per_exam,
       COUNT(*) AS worklist_rows
FROM n
GROUP BY issuer, CASE WHEN sps_rows >= 3 THEN '3+' ELSE TO_CHAR(sps_rows) END
ORDER BY issuer, sps_rows_per_exam;

-- ── Step 3: orders not performed yet: is a device booked? (orders of the last 365 days) ──
-- One row per hospital and RIS status. with_booked_device / worklist_rows = how often the RIS
-- names the room; booked_device_has_ae = the device has the AE title RAYD maps rooms by.
SELECT o.ISSUER_OF_PLACER_ORDER_NUMBER AS issuer,
       w.STATUS,
       COUNT(*) AS worklist_rows,
       COUNT(s.SPS_ID) AS with_sps_row,
       COUNT(s.MODALITY_KEY) AS with_booked_device,
       COUNT(m.AE_TITLE) AS booked_device_has_ae,
       COUNT(CASE WHEN s.START_DATETIME < TRUNC(SYSDATE) THEN 1 END) AS booked_before_today,
       COUNT(CASE WHEN s.START_DATETIME >= TRUNC(SYSDATE) THEN 1 END) AS booked_today_or_later
FROM SITE_WORKLIST w
JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
LEFT JOIN SPS s ON s.SPS_ID = w.SPS_ID
LEFT JOIN MODALITY m ON m.MODALITY_KEY = s.MODALITY_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
  AND o.CREATED_ON_DATE >= TRUNC(SYSDATE) - 365
  AND w.PERFORMED_DATE IS NULL
  AND w.PPS_KEY IS NULL
GROUP BY o.ISSUER_OF_PLACER_ORDER_NUMBER, w.STATUS
ORDER BY issuer, worklist_rows DESC;

-- ── Step 4: performed exams (last 90 days): booked device vs the device that performed it ──
-- other_device = rebooked or moved at the scanner. If same_device is close to 100 %, the booked
-- room is the room the RIS will show once the exam is done, i.e. what the tab already uses for
-- performed orders. (MR: the RIS room is not changed when a patient moves scanner, item 4.)
SELECT w.MODALITY_TYPE,
       COUNT(*) AS performed,
       COUNT(CASE WHEN s.MODALITY_KEY = p.MODALITY_KEY THEN 1 END) AS same_device,
       COUNT(CASE WHEN s.MODALITY_KEY <> p.MODALITY_KEY THEN 1 END) AS other_device,
       COUNT(CASE WHEN s.MODALITY_KEY IS NULL THEN 1 END) AS no_booked_device
FROM SITE_WORKLIST w
JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
JOIN PPS p ON p.PPS_KEY = w.PPS_KEY
LEFT JOIN SPS s ON s.SPS_ID = w.SPS_ID
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
  AND o.CREATED_ON_DATE >= TRUNC(SYSDATE) - 90
  AND p.START_DATETIME >= TRUNC(SYSDATE) - 90
GROUP BY w.MODALITY_TYPE
ORDER BY performed DESC;
