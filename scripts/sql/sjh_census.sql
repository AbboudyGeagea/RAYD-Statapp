-- scripts/sql/sjh_census.sql — how much SJH data RH's Postgres (etl_db) holds.
-- READ-ONLY: SELECTs only, nothing is created or changed. Safe on production.
--
-- Item 13 (meeting of 2026-11-13): SJH gets its own database; its rows are copied there,
-- counted, then deleted from RH's database (13b). This census is the "before" count.
--
-- Run on the RAYD server (container rayd_db):
--   docker exec -i rayd_db psql -U etl_user -d etl_db -f - < scripts/sql/sjh_census.sql
-- or paste it into psql. One result set per section.
--
-- An SJH row is recognised the way RAYD's RH-only views (migration 0145) hide it:
--   studies             etl_didb_studies.site_id = SJH (the ETL resolves it from the device
--                       or the PACS site; double-read RH mammography stays RH)
--   RIS orders          etl_orders.order_control = SJH's RIS issuer (SAP_SJH)
--   RIS steps, reports  through their order
--   ORU reports         through the accession's order or study
--   PACS steps (PPS)    through the device or the study
--   anything else       a site_id column equal to SJH (last section, every such table)

\echo
\echo '== Sites =='
SELECT id, code, name, is_default, pacs_site_id, ris_issuer, hl7_building
FROM sites ORDER BY id;

\echo
\echo '== Studies (PACS) =='
WITH sjh AS (SELECT id FROM sites WHERE code = 'SJH'),
     rh  AS (SELECT id FROM sites WHERE is_default)
SELECT
    COUNT(*)                                                       AS studies_total,
    COUNT(*) FILTER (WHERE s.site_id = (SELECT id FROM sjh))        AS sjh_by_site_id,
    COUNT(*) FILTER (WHERE s.site_id = (SELECT id FROM rh))         AS rh_by_site_id,
    COUNT(*) FILTER (WHERE s.site_id IS NULL)                      AS no_site,
    COUNT(*) FILTER (WHERE s.pacs_site_id_raw = (SELECT pacs_site_id FROM sites WHERE code = 'SJH'))
                                                                   AS sjh_by_pacs_label,
    MIN(s.study_date) FILTER (WHERE s.site_id = (SELECT id FROM sjh)) AS sjh_first_date,
    MAX(s.study_date) FILTER (WHERE s.site_id = (SELECT id FROM sjh)) AS sjh_last_date
FROM etl_didb_studies s;

\echo
\echo '== SJH studies per year =='
SELECT EXTRACT(YEAR FROM s.study_date)::int AS year, COUNT(*) AS sjh_studies
FROM etl_didb_studies s
WHERE s.site_id = (SELECT id FROM sites WHERE code = 'SJH')
GROUP BY 1 ORDER BY 1;

\echo
\echo '== Study detail and patients of SJH studies =='
WITH sjh_st AS (SELECT study_db_uid, patient_db_uid FROM etl_didb_studies
                WHERE site_id = (SELECT id FROM sites WHERE code = 'SJH'))
SELECT 'etl_didb_serieses' AS tbl, COUNT(*) AS sjh_rows,
       (SELECT COUNT(*) FROM etl_didb_serieses) AS total
FROM etl_didb_serieses x WHERE x.study_db_uid IN (SELECT study_db_uid FROM sjh_st)
UNION ALL
SELECT 'etl_patient_view (SJH studies only, no RH study)', COUNT(*), (SELECT COUNT(*) FROM etl_patient_view)
FROM etl_patient_view p
WHERE p.patient_db_uid IN (SELECT patient_db_uid FROM sjh_st)
  AND NOT EXISTS (SELECT 1 FROM etl_didb_studies s
                  WHERE s.patient_db_uid = p.patient_db_uid
                    AND COALESCE(s.site_id, -1) <> (SELECT id FROM sites WHERE code = 'SJH'))
UNION ALL
SELECT 'etl_patient_view (seen at both sites)', COUNT(*), (SELECT COUNT(*) FROM etl_patient_view)
FROM etl_patient_view p
WHERE p.patient_db_uid IN (SELECT patient_db_uid FROM sjh_st)
  AND EXISTS (SELECT 1 FROM etl_didb_studies s
              WHERE s.patient_db_uid = p.patient_db_uid
                AND COALESCE(s.site_id, -1) <> (SELECT id FROM sites WHERE code = 'SJH'));

\echo
\echo '== RIS (orders and what hangs off them) =='
WITH sjh_o AS (SELECT order_dbid, accession_number FROM etl_orders
               WHERE order_control IN (SELECT ris_issuer FROM sites WHERE code = 'SJH'))
SELECT 'etl_orders' AS tbl, (SELECT COUNT(*) FROM sjh_o) AS sjh_rows, (SELECT COUNT(*) FROM etl_orders) AS total
UNION ALL
SELECT 'std_worklist_scheduled', COUNT(*), (SELECT COUNT(*) FROM std_worklist_scheduled)
FROM std_worklist_scheduled d WHERE d.site_worklist_key IN (SELECT order_dbid FROM sjh_o)
UNION ALL
SELECT 'std_worklist_arrivals', COUNT(*), (SELECT COUNT(*) FROM std_worklist_arrivals)
FROM std_worklist_arrivals d WHERE d.site_worklist_key IN (SELECT order_dbid FROM sjh_o)
UNION ALL
SELECT 'std_worklist_exam_done', COUNT(*), (SELECT COUNT(*) FROM std_worklist_exam_done)
FROM std_worklist_exam_done d WHERE d.site_worklist_key IN (SELECT order_dbid FROM sjh_o)
UNION ALL
SELECT 'std_worklist_cancellations', COUNT(*), (SELECT COUNT(*) FROM std_worklist_cancellations)
FROM std_worklist_cancellations d WHERE d.site_worklist_key IN (SELECT order_dbid FROM sjh_o)
UNION ALL
SELECT 'worklist_status_history', COUNT(*), (SELECT COUNT(*) FROM worklist_status_history)
FROM worklist_status_history d WHERE d.site_worklist_key IN (SELECT order_dbid FROM sjh_o)
UNION ALL
SELECT 'std_reports', COUNT(*), (SELECT COUNT(*) FROM std_reports)
FROM std_reports r WHERE r.reported_acc_number IN (SELECT accession_number FROM sjh_o WHERE accession_number IS NOT NULL);

\echo
\echo '== PACS steps (std_pps) =='
SELECT COUNT(*) AS sjh_rows, (SELECT COUNT(*) FROM std_pps) AS total
FROM std_pps p
WHERE EXISTS (SELECT 1 FROM aetitle_modality_map m
              WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(p.performing_ae_title))
                AND COALESCE(m.modality, '') <> 'PACS'
                AND m.site_id = (SELECT id FROM sites WHERE code = 'SJH'))
   OR EXISTS (SELECT 1 FROM etl_didb_studies s
              WHERE s.study_db_uid = p.study_db_uid
                AND s.site_id = (SELECT id FROM sites WHERE code = 'SJH'));

\echo
\echo '== HL7 ORU reports (and the NLP rows built on them) =='
WITH sjh_r AS (
    SELECT r.id FROM hl7_oru_reports r
    WHERE EXISTS (SELECT 1 FROM etl_orders o
                  WHERE o.accession_number = r.accession_number
                    AND o.order_control IN (SELECT ris_issuer FROM sites WHERE code = 'SJH'))
       OR EXISTS (SELECT 1 FROM etl_didb_studies s
                  WHERE s.accession_number = r.accession_number
                    AND s.site_id = (SELECT id FROM sites WHERE code = 'SJH')))
SELECT 'hl7_oru_reports' AS tbl, (SELECT COUNT(*) FROM sjh_r) AS sjh_rows, (SELECT COUNT(*) FROM hl7_oru_reports) AS total
UNION ALL
SELECT 'hl7_oru_analysis', COUNT(*), (SELECT COUNT(*) FROM hl7_oru_analysis)
FROM hl7_oru_analysis a WHERE a.report_id IN (SELECT id FROM sjh_r)
UNION ALL
SELECT 'ai_nlp_cache', COUNT(*), (SELECT COUNT(*) FROM ai_nlp_cache)
FROM ai_nlp_cache c WHERE c.source_id IN (SELECT id FROM sjh_r);

\echo
\echo '== Devices (AE mapping) =='
SELECT COALESCE(s.code, '(no site)') AS site, COUNT(*) AS devices,
       COUNT(*) FILTER (WHERE m.modality = 'PACS') AS gateways
FROM aetitle_modality_map m LEFT JOIN sites s ON s.id = m.site_id
GROUP BY 1 ORDER BY 1;

\echo
\echo '== Every table with a site_id column: rows per site =='
-- query_to_xml runs one COUNT per table without creating anything.
SELECT c.table_name,
       (xpath('/row/n/text()', query_to_xml(format(
            'SELECT COUNT(*) AS n FROM public.%I WHERE site_id = (SELECT id FROM public.sites WHERE code = %L)',
            c.table_name, 'SJH'), false, true, '')))[1]::text::bigint AS sjh_rows,
       (xpath('/row/n/text()', query_to_xml(format(
            'SELECT COUNT(*) AS n FROM public.%I WHERE site_id = (SELECT id FROM public.sites WHERE is_default)',
            c.table_name), false, true, '')))[1]::text::bigint AS rh_rows,
       (xpath('/row/n/text()', query_to_xml(format(
            'SELECT COUNT(*) AS n FROM public.%I WHERE site_id IS NULL', c.table_name), false, true, '')))[1]::text::bigint AS no_site,
       (xpath('/row/n/text()', query_to_xml(format(
            'SELECT COUNT(*) AS n FROM public.%I', c.table_name), false, true, '')))[1]::text::bigint AS total
FROM information_schema.columns c
JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE c.table_schema = 'public' AND c.column_name = 'site_id' AND t.table_type = 'BASE TABLE'
  AND c.table_name NOT IN ('user_sites', 'user_page_permissions', 'report_access_control')
ORDER BY sjh_rows DESC, c.table_name;

\echo
\echo '== Optional, heavy on a full install: images of SJH studies (uncomment to run) =='
-- SELECT COUNT(*) AS sjh_images FROM etl_didb_raw_images i
-- WHERE i.study_db_uid IN (SELECT study_db_uid FROM etl_didb_studies
--                          WHERE site_id = (SELECT id FROM sites WHERE code = 'SJH'));
