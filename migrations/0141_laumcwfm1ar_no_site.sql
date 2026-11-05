-- Migration 0141: LAUMCWFM1AR belongs to no site, and fact_exam's site follows the device
-- *** LAUMC BRANCH ONLY ***
--
-- Reconciled against production PACS for October 2026 (operator, 2026-11-05):
--
-- LAUMCWFM1AR carries PACS SITE_ID '2', which is not a real site, and the operator wants
-- its studies counted aside. Migration 0083 seeded it as RH, so every report that resolves
-- site through this map counted its 118 studies that month as RH volume. Clearing the site
-- removes it from all of them in one place (reports 25, 31-34, 36, the Super Report, the
-- home dashboards and the storage rollup), and it fails closed. The RIS modality ETL
-- inserts with ON CONFLICT DO NOTHING, so it never writes a site back. Reports 22 and 23
-- also exclude it by name and show the count aside.
--
-- fact_exam took its site from the PACS label, which double reading gets wrong for RH
-- mammography. ETL_JOBS/etl_fact_exam.py now uses the device; the rows below already
-- disagree with their device, so they are marked for the next phase 20 to derive again.

UPDATE aetitle_modality_map
SET site_id     = NULL,
    description = 'Legacy specialty router - reports without images on PACS site 2, which is not a real site. Keep the site blank so it stays out of site totals.'
WHERE UPPER(TRIM(aetitle)) = 'LAUMCWFM1AR';

UPDATE fact_exam f
SET derived_at = NULL
WHERE EXISTS (
    SELECT 1
    FROM aetitle_modality_map m
    WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(f.device))
      AND m.site_id IS NOT NULL
      AND m.modality <> 'PACS'
      AND f.site_id IS DISTINCT FROM m.site_id
);
