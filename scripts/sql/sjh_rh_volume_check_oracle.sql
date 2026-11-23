-- scripts/sql/sjh_rh_volume_check_oracle.sql — are SJH's and RH's volumes in RAYD what PACS and
-- the RIS hold? (to-do 13-V, after SJH moved to its own database, 2026-11-21). READ-ONLY, run by a
-- person in SQL Developer (F5, "Run Script"); RAYD never runs it.
--
-- "as_rayd_loads" applies the nightly copy's own filters (ETL_JOBS/etl_didb_studies.py): the
-- devices never loaded (EXCLUDED_AES: duplicates and dedicated cardiology / vascular equipment),
-- the cardiology devices behind SJH's middleware (CARDIOLOGY_AES, as storing or original AE) and
-- signers ending in @dn. RAYD's side is the server commands of the same day (both hospitals'
-- databases: etl_db = RH and the studies with no site, etl_db_sjh = SJH). Expect RAYD = as_rayd_loads
-- per device, a little lower on SJHCSAPWFMFIR / LAUMCWFM2FIR, whose cardiology-labelled studies
-- RAYD purges after the copy (Phase 2c).
--
-- PACS SITE_ID: '0' RH, '1' SJH, '2' LAUMCWFM1AR (not a site). Mammography is the exception:
-- double reading labels most of RH's SENO1 studies '1' (2026-11-05), so compare per device.

-- ════ PACS connection (medistore, as RAYD reads it) ══════════════════════════

-- ── V1. Studies per year and PACS site, up to 22 Nov 2026 ─────────────────────
SELECT EXTRACT(YEAR FROM s.STUDY_DATE) AS year, TO_CHAR(s.SITE_ID) AS pacs_site,
       COUNT(*) AS all_studies,
       SUM(CASE WHEN UPPER(TRIM(s.STORING_AE)) NOT IN ('LAUMC', 'SVSM', 'ECHOPAC-PC', 'ADW_8', 'AETITLE',
                        'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA', 'VIVIDS70-003049', 'TERRA2', 'AWVASC',
                        'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01', 'EPIQCVX', 'AZURION')
                 AND (s.ORIGINAL_STORING_AE IS NULL OR UPPER(TRIM(s.ORIGINAL_STORING_AE)) NOT IN ('LAUMC', 'SVSM',
                        'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
                        'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01', 'EPIQCVX', 'AZURION'))
                 AND (s.REP_FINAL_SIGNED_BY IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY) NOT LIKE '%@DN')
                 AND (s.REP_PRELIM_SIGNED_BY IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY) NOT LIKE '%@DN')
                 AND (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')
                THEN 1 ELSE 0 END) AS as_rayd_loads
FROM medistore.didb_studies s
WHERE s.STUDY_DATE >= DATE '2019-01-01' AND s.STUDY_DATE < DATE '2026-11-23'
GROUP BY EXTRACT(YEAR FROM s.STUDY_DATE), s.SITE_ID
ORDER BY 1, 2;

-- ── V2. October 2026 per PACS site and device ─────────────────────────────────
-- original_ae only where PACS names another device behind the storing AE (SJH's middleware).
SELECT TO_CHAR(s.SITE_ID) AS pacs_site, UPPER(TRIM(s.STORING_AE)) AS storing_ae,
       CASE WHEN UPPER(TRIM(s.ORIGINAL_STORING_AE)) <> UPPER(TRIM(s.STORING_AE))
            THEN UPPER(TRIM(s.ORIGINAL_STORING_AE)) END AS original_ae,
       COUNT(*) AS all_studies,
       SUM(CASE WHEN UPPER(TRIM(s.STORING_AE)) NOT IN ('LAUMC', 'SVSM', 'ECHOPAC-PC', 'ADW_8', 'AETITLE',
                        'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA', 'VIVIDS70-003049', 'TERRA2', 'AWVASC',
                        'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01', 'EPIQCVX', 'AZURION')
                 AND (s.ORIGINAL_STORING_AE IS NULL OR UPPER(TRIM(s.ORIGINAL_STORING_AE)) NOT IN ('LAUMC', 'SVSM',
                        'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
                        'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01', 'EPIQCVX', 'AZURION'))
                 AND (s.REP_FINAL_SIGNED_BY IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY) NOT LIKE '%@DN')
                 AND (s.REP_PRELIM_SIGNED_BY IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY) NOT LIKE '%@DN')
                 AND (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')
                THEN 1 ELSE 0 END) AS as_rayd_loads
FROM medistore.didb_studies s
WHERE s.STUDY_DATE >= DATE '2026-10-01' AND s.STUDY_DATE < DATE '2026-11-01'
GROUP BY s.SITE_ID, UPPER(TRIM(s.STORING_AE)),
         CASE WHEN UPPER(TRIM(s.ORIGINAL_STORING_AE)) <> UPPER(TRIM(s.STORING_AE))
              THEN UPPER(TRIM(s.ORIGINAL_STORING_AE)) END
ORDER BY 1, 4 DESC;

-- ════ RIS connection (the account RAYD's RIS copy reads; or put the RIS schema before the tables) ═══

-- ── R1. October 2026 RIS exams per hospital and RIS modality ──────────────────
-- SAP_PROD = RH, SAP_SJH = SJH. scheduled_rows includes the HIS's duplicate orders (one copy
-- stays Scheduled; RAYD leaves it out), so compare exam_done, a RIS fact neither side filters.
WITH done AS (
    SELECT DISTINCT h.SITE_WORKLIST_KEY
    FROM WORKLIST_STATUS_HISTORY h
    WHERE h.STATUS_KEY = 100                                  -- Exam Done
      AND h.STATUS_TIME >= DATE '2026-09-01'
)
SELECT o.ISSUER_OF_PLACER_ORDER_NUMBER AS issuer, NVL(w.MODALITY_TYPE, '(none)') AS ris_modality,
       COUNT(*) AS scheduled_rows, COUNT(d.SITE_WORKLIST_KEY) AS exam_done
FROM SITE_WORKLIST w
JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
LEFT JOIN done d ON d.SITE_WORKLIST_KEY = w.SITE_WORKLIST_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD', 'SAP_SJH')
  AND w.SCHEDULED_DATE >= DATE '2026-10-01' AND w.SCHEDULED_DATE < DATE '2026-11-01'
GROUP BY GROUPING SETS ((o.ISSUER_OF_PLACER_ORDER_NUMBER),
                        (o.ISSUER_OF_PLACER_ORDER_NUMBER, NVL(w.MODALITY_TYPE, '(none)')))
ORDER BY 1, 2 NULLS FIRST;
