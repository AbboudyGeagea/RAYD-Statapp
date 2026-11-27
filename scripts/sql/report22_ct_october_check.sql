-- scripts/sql/report22_ct_october_check.sql — Report 22, CT, October 2026: why the page shows two
-- numbers (888 and 1,241) and the RIS worklist a third (operator, 26 Nov). READ-ONLY. Parts B and C
-- run in SQL Developer (F5, "Run Script") on the PACS and the RIS; RAYD never runs them. Part A is
-- the same three counts in RAYD (the server commands are in the to-do and the reply of 26 Nov).
--
-- The page's two numbers are two units on purpose:
--   888    PACS studies (the Modality > Room tree; the volume table with "Count linked exams" off)
--   1,241  RIS exams (the volume table with "Count linked exams separately (match RIS)" on): a PACS
--          study that covers several RIS exams (CT abdomen + pelvis = 1 study, 2 exams) counts once
--          per exam that reached Exam Done on the RIS; two PACS copies of one accession count once.
-- The page link also ticks "Study status" = APPROVED, PENDING APPROVED, UNREAD, WETREAD, which leaves
-- out CT studies in any other PACS status; A1 shows how many.
--
-- RH's CT devices: CT99 (CT128 GE Emergency Department), CTHD (CT Room - 750 HD), AWS (that room's
-- workstation: cardiac CT angio, TAVI, calcium score; the only PACS copy of those studies, not a
-- second one) and BAY85CT, the CT of the PET-CT room: CT-guided punctures, biopsies and drainages,
-- which the RIS codes as modality PT (2026-11-05). The PET scans themselves come through AWPETCT /
-- BAY87CT and count as PT, not CT. RAYD never loads AWCTHD1 (ETL exclusion).

-- ════ Part B — PACS connection (medistore, as RAYD reads it) ═════════════════

-- ── B1. CT and PET-CT devices, October 2026, per PACS site, device and PACS status ──
SELECT TO_CHAR(s.SITE_ID) AS pacs_site, UPPER(TRIM(s.STORING_AE)) AS storing_ae,
       TRIM(s.STUDY_STATUS) AS study_status, COUNT(*) AS studies,
       COUNT(DISTINCT s.ACCESSION_NUMBER) AS accessions
FROM medistore.didb_studies s
WHERE s.STUDY_DATE >= DATE '2026-10-01' AND s.STUDY_DATE < DATE '2026-11-01'
  AND UPPER(TRIM(s.STORING_AE)) IN ('CT99', 'CTHD', 'AWS', 'BAY85CT', 'AWCTHD1', 'RAPID', 'AWPETCT', 'BAY87CT')
GROUP BY s.SITE_ID, UPPER(TRIM(s.STORING_AE)), TRIM(s.STUDY_STATUS)
ORDER BY 2, 4 DESC;

-- ════ Part C — RIS connection (the account RAYD's RIS copy reads; tables unqualified) ════

-- ── C1. RH's CT and PT worklist rows booked in October 2026, per RIS modality, status and device ──
-- What a RIS worklist screen counts depends on its own filters (booked date or performed date, which
-- statuses, which rooms): compare its number with the row of the same status here. STATUS 100 is
-- Exam Done; cancelled rows keep their own code.
SELECT w.MODALITY_TYPE AS ris_modality, w.STATUS AS ris_status,
       NVL(m.AE_TITLE, '(no device)') AS booked_device, COUNT(*) AS worklist_rows,
       COUNT(w.PPS_KEY) AS performed_on_a_scanner
FROM SITE_WORKLIST w
JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
LEFT JOIN SPS s ON s.SPS_ID = w.SPS_ID
LEFT JOIN MODALITY m ON m.MODALITY_KEY = s.MODALITY_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER = 'SAP_PROD'
  AND s.START_DATETIME >= DATE '2026-10-01' AND s.START_DATETIME < DATE '2026-11-01'
  AND (w.MODALITY_TYPE IN ('CT', 'PT') OR UPPER(m.AE_TITLE) IN ('CT99', 'CTHD', 'BAY85CT', 'AWPETCT', 'BAY87CT'))
GROUP BY w.MODALITY_TYPE, w.STATUS, NVL(m.AE_TITLE, '(no device)')
ORDER BY 1, 4 DESC;

-- ════ Part A — the same in RAYD (production server, RH's database etl_db) ═════
-- A1. PACS studies per CT device, October, and how many the page's four statuses keep:
--   SELECT COALESCE(NULLIF(m.station_name, ''), s.storing_ae) AS device, COUNT(*) AS studies,
--          COUNT(*) FILTER (WHERE TRIM(s.study_status) IN ('APPROVED','PENDING APPROVED','UNREAD','WETREAD')) AS in_the_four_statuses,
--          string_agg(DISTINCT TRIM(s.study_status), ', ') AS statuses
--   FROM etl_didb_studies s
--   LEFT JOIN LATERAL (SELECT m0.modality, m0.station_name FROM aetitle_modality_map m0
--                      WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) m ON TRUE
--   WHERE s.study_date >= DATE '2026-10-01' AND s.study_date < DATE '2026-11-01'
--     AND COALESCE(m.modality, s.study_modality) = 'CT' AND UPPER(TRIM(s.storing_ae)) <> 'LAUMCWFM1AR'
--   GROUP BY 1 ORDER BY 2 DESC;
-- A2. RIS exams in RAYD's copy, booked in October at RH, CT and PT rooms:
--   SELECT COALESCE(room, '(no room)') AS room, ris_modality, COUNT(*) AS exams,
--          COUNT(exam_done_at) AS exam_done, COUNT(cancelled_at) AS cancelled
--   FROM rpt.exams
--   WHERE scheduled_at >= DATE '2026-10-01' AND scheduled_at < DATE '2026-11-01'
--     AND (modality IN ('CT', 'PT') OR ris_modality IN ('CT', 'PT'))
--     AND site = (SELECT name FROM sites WHERE code = 'RH')
--   GROUP BY 1, 2 ORDER BY 3 DESC;
