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
--
-- FK guards, same pattern as migration 0073 (delete_laumc_svsm_duplicate_studies):
--   - etl_didb_serieses / etl_didb_raw_images / etl_image_locations cascade automatically
--     (ON DELETE CASCADE, init-db/schema.sql:1499/1507 + fk_location_to_raw_image).
--   - etl_orders.study_db_uid has no FK (application-level join per etl_orders.py's own
--     docstring) but would be left dangling — null it out first.
--   - std_pps.study_db_uid (migration 0065) DOES have an FK with no ON DELETE clause
--     (default NO ACTION) — this is what 0073 flagged as a future risk if std_pps was
--     ever non-empty, and it's non-empty now (confirmed live: this migration failed on
--     std_pps_study_db_uid_fkey on first attempt, e.g. study_db_uid=1348775). Same
--     enrichment-target shape as etl_orders (ETL_JOBS/etl_ris_pps.py fills it in by
--     matching STUDY_INSTANCE_UID, no has_study-style flag) — null it out first too.
--
-- BATCHED, same reason as 0073: a single un-batched DELETE FROM etl_didb_studies
-- previously ran 70+ minutes as one transaction and had to be aborted via a Postgres
-- restart, taking the app down with it (cascade into etl_didb_raw_images, "100M+ rows"
-- per etl_runner.py's own phase description, is the actual cost). A DO block commits
-- once per 5,000-row batch instead of once for the whole job.
--
-- Idempotent: re-running finds 0 matching rows in every step and does nothing.

-- Clear dangling RIS enrichment references before the studies they point to disappear.
UPDATE std_pps
SET study_db_uid = NULL
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

-- Group A: dedicated Cardiology/Vascular-Lab devices — every study, any modality.
-- Group B: shared SJH gateway AEs — CARD-family-tagged rows only (leaves their other
-- ~182,700 legitimate multi-modality studies untouched). Cascades to
-- etl_didb_serieses/etl_didb_raw_images/etl_image_locations via their existing
-- ON DELETE CASCADE FKs.
DO $$
DECLARE
    deleted_count INT;
    total_deleted INT := 0;
BEGIN
    LOOP
        DELETE FROM etl_didb_studies
        WHERE study_db_uid IN (
            SELECT study_db_uid FROM etl_didb_studies
            WHERE UPPER(TRIM(storing_ae)) IN (
                'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514',
                'TERRA', 'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO',
                'LOGIQV2-01', 'DEFINIUM1'
            )
            OR (
                UPPER(TRIM(storing_ae)) IN ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR')
                AND UPPER(TRIM(study_modality)) IN ('CARD', 'SJH_CARD', 'CARDUS', 'SJHCARD')
            )
            LIMIT 5000
        );
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        total_deleted := total_deleted + deleted_count;
        RAISE NOTICE 'CARD-family cleanup: % rows this batch, % total', deleted_count, total_deleted;
        COMMIT;
        EXIT WHEN deleted_count = 0;
    END LOOP;
END $$;
