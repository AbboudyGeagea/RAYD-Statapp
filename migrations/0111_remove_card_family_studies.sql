-- LAUMC: one-time removal of Cardiology-archive (CARD-family) studies.
--
-- Two groups, confirmed against live production data (see plan discussion):
--   Group A — 13 dedicated Cardiology/Vascular-Lab PACS-only-archival devices (echo/
--   cath/angio workstations). They archive straight to PACS and never go through the
--   RIS ordering workflow, so every study from these AEs (any modality tag) inflates
--   studies-vs-orders counts with no way to ever reconcile. Delete unconditionally.
--
--   Group B — SJHCSAPWFMFIR / LAUMCWFM2FIR are shared SJH workflow-manager/gateway AEs
--   (per migration 0083's own device notes), NOT dedicated devices. Their full modality
--   distribution (RF/CT/SR/MR/DX/MG/XA/US/PR/etc.) shows the CARD-family tag is only
--   ~7% of SJHCSAPWFMFIR's ~182,000 studies and ~1% of LAUMCWFM2FIR's ~792 — an AE-based
--   delete here would wipe out essentially all of SJH's imaging archive. Delete only
--   their CARD/SJH_CARD/CARDUS/SJHCARD-tagged rows.
--
-- Going forward: Group A is excluded at ETL extract time (ETL_JOBS/etl_didb_studies.py's
-- _EXCLUDED_AE_SQL); Group B is purged every ETL cycle post-modality-backfill
-- (ETL_JOBS/etl_runner.py Phase 2c). This migration only cleans up what's already loaded.

BEGIN;

-- Clear dangling order references before the studies they point to disappear.
-- etl_orders.study_db_uid has no FK constraint (application-level join), so this
-- wouldn't error if skipped -- but the order row would silently point at a deleted
-- study and keep has_study=TRUE.
UPDATE etl_orders
SET study_db_uid = NULL, study_instance_uid = NULL, has_study = FALSE
WHERE study_db_uid IN (
    SELECT study_db_uid FROM etl_didb_studies
    WHERE UPPER(TRIM(storing_ae)) IN (
        'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
        'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01',
        'DEFINIUM1'
    )
    OR (
        UPPER(TRIM(storing_ae)) IN ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR')
        AND UPPER(TRIM(study_modality)) IN ('CARD', 'SJH_CARD', 'CARDUS', 'SJHCARD')
    )
);

-- Cascades to etl_didb_serieses / etl_didb_raw_images / etl_image_locations via their
-- existing ON DELETE CASCADE FKs (fk_series_to_study, fk_raw_images_to_study,
-- fk_location_to_raw_image) -- no need to delete from those tables separately.
DELETE FROM etl_didb_studies
WHERE UPPER(TRIM(storing_ae)) IN (
    'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
    'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01', 'DEFINIUM1'
)
OR (
    UPPER(TRIM(storing_ae)) IN ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR')
    AND UPPER(TRIM(study_modality)) IN ('CARD', 'SJH_CARD', 'CARDUS', 'SJHCARD')
);

COMMIT;
