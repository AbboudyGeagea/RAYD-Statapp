-- Migration 0163: RIS orders and reports get their site (bug found 2026-11-18).
-- *** LAUMC BRANCH ONLY ***
--
-- Report 27's RIS-vs-PACS card read 0 RIS exams in every month (operator, screenshot of
-- 2026-11-18): it keeps "this site's" orders with etl_orders.site_id, and nothing ever filled
-- that column; neither did std_reports.site_id, which the RIS reports copy inherits from the
-- order. The order's RIS issuer says the site (sites.ris_issuer: SAP_PROD = RH, SAP_SJH = SJH);
-- ETL_JOBS/etl_orders.py now sets it every night, and this fills what is already loaded.

UPDATE public.etl_orders o
SET site_id = s.id
FROM public.sites s
WHERE s.ris_issuer = o.order_control
  AND o.site_id IS DISTINCT FROM s.id;

-- ETL_JOBS/etl_ris_reports.py's enrichment, once, now that the orders carry their site
UPDATE public.std_reports r
SET site_id = o.site_id
FROM public.etl_orders o
WHERE o.accession_number = r.reported_acc_number
  AND r.reported_acc_number IS NOT NULL
  AND r.is_max_version
  AND r.site_id IS NULL
  AND o.site_id IS NOT NULL;
