-- Migration 0125: fact_exam -- one row per PACS study, the single counting definition.
-- *** LAUMC BRANCH ONLY ***
--
-- Every report today restates its own counting rules in its own SQL (report_22's
-- _BASE_EXCLUSIONS, report_widgets' _WHERE, the ETL extract filters), and they have
-- drifted apart. fact_exam carries the rules ONCE, so the Explorer (and anything built
-- on it later) is a view of one definition instead of yet another copy.
--
-- Rules (operator, 2026-10-06, validated against medistore.didb_studies for
-- 2026-03-01 .. 2026-05-31 with the Oracle "query 0"):
--   * Linked studies count, each accession as one exam -- including the SR-only
--     partner of a linked group (CT abdomen + SR-only CT pelvis = 2 CT exams).
--     IS_LINKED_STUDY = 'Y' marks them; WORKITEM_DB_UID groups them (checked on live
--     data: every SR-only linked accession shares its work item with the study that
--     holds the images).
--   * Modality comes from PACS STUDY_MODALITY ('\CT\SR\PR\'), with non-exam objects
--     (SR, OT, PR, KO, DOC, SC, REG, SEG, FID, RWV, PLAN) removed. An SR-only linked
--     partner takes the modality, device, scan time and patient type of the study in
--     its work item that holds the images.
--   * Not counted (kept, with `reason` set, so screens can show what was left out):
--     accession starting RH / SJ (imported outside studies, e.g. RH20260918..,
--     SJ20260714..), CARD-family modality, the cardiology devices,
--     LAUMC/SVSM duplicate copies, '@dn' records, SR/OT-only studies that are not
--     linked.
--
-- Loaded by ETL phase 20 (ETL_JOBS/etl_fact_exam.py) straight from PACS, NOT from
-- etl_didb_studies: that table's extract drops studies by storing AE and '@dn' signer
-- before RAYD ever sees them, which would silently lose linked partners stored under
-- an excluded AE, and its study_modality is the series MODE rather than PACS's own
-- STUDY_MODALITY. Existing reports are untouched.
--
-- Raw PACS columns are upserted by the extract; the derived block (own_modality ..
-- reason) is recomputed by the same phase for every work-item group with a row newer
-- than its last derivation (derived_at < synced_at), so a partner arriving in a later
-- run still picks up its group's values.
--
-- Plain table, not a materialized view: row-level security (migration 0050) cannot
-- be enabled on a materialized view, and the Explorer reads this through rayd_app.

CREATE TABLE IF NOT EXISTS fact_exam (
    study_db_uid         BIGINT PRIMARY KEY,

    -- raw, from medistore.didb_studies
    workitem_db_uid      BIGINT,
    is_linked            BOOLEAN   NOT NULL DEFAULT FALSE,
    accession_number     TEXT,
    study_ts             TIMESTAMP,            -- STUDY_DATE with its time part
    insert_time          TIMESTAMP,
    storing_ae           TEXT,
    pacs_site_id         VARCHAR(32),          -- raw SITE_ID ('0' RH, '1' SJH)
    patient_db_uid       TEXT,
    patient_class        TEXT,
    patient_location     TEXT,
    study_description    TEXT,
    procedure_code       TEXT,
    number_of_images     INTEGER,
    study_modality_raw   TEXT,                 -- e.g. '\CT\SR\PR\'
    report_status        TEXT,
    composed_by          TEXT,                 -- REP_STUDY_LAST_COMPOSED_BY
    composed_ts          TIMESTAMP,            -- REP_STUDY_LAST_COMPOSED_TS
    referring_physician  TEXT,
    dn_signer            BOOLEAN   NOT NULL DEFAULT FALSE,
    synced_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    -- link group: the work item, or the study alone when it has none
    grp_key              BIGINT GENERATED ALWAYS AS (COALESCE(workitem_db_uid, -study_db_uid)) STORED,

    -- derived by phase 20
    own_modality         TEXT,                 -- cleaned modality of this study alone
    modality             TEXT,                 -- own, else the group's imaging study's
    device               TEXT,
    exam_ts              TIMESTAMP,            -- scan time (weekday / hour)
    exam_date            DATE,                 -- this accession's own study date (month)
    main_accession       TEXT,                 -- accession of the study holding the images
    patient_type         TEXT,                 -- ER / Inpatient / Outpatient / raw code
    site_id              INTEGER,              -- canonical sites.id, for RLS
    reason               TEXT,                 -- NULL = counted
    derived_at           TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_exam_exam_date ON fact_exam (exam_date);
CREATE INDEX IF NOT EXISTS idx_fact_exam_grp_key   ON fact_exam (grp_key);
CREATE INDEX IF NOT EXISTS idx_fact_exam_site_date ON fact_exam (site_id, exam_date);

-- Same site-scope policy as migration 0050's tables: rayd_app sees only its scope,
-- the owner (ETL) bypasses. Guarded so an install without rayd_app still migrates.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rayd_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON public.fact_exam TO rayd_app;
        ALTER TABLE public.fact_exam ENABLE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS fact_exam_site_scope ON public.fact_exam;
        CREATE POLICY fact_exam_site_scope ON public.fact_exam
            FOR ALL TO rayd_app
            USING (
                site_id = ANY (
                    string_to_array(current_setting('rayd.site_scope', true), ',')::int[]
                )
            );
    END IF;
END $$;
