-- Migration 0119: RIS REPORT -> std_reports (LAUMC).
--
-- WHY THIS TABLE EXISTS. Report 36's "KPI Detailed Reading" (the resident/radiologist
-- TAT sheet) needs three legs: Exam Done -> Signed 1 -> Approved. Two of the three had
-- no source anywhere in RAYD, and the search was exhaustive:
--
--   * PACS etl_didb_studies -- REP_FINAL_TIMESTAMP and REP_FINAL_SIGNED_BY are 100%
--     EMPTY at RH (0 of 38,033 studies, Jan-Aug 2026). REP_PRELIM_* is 3.4% and almost
--     entirely an SJH user. Only REP_STUDY_LAST_COMPOSED_TS is real (98.3%). Every
--     other date column on DIDB_STUDIES was checked: REPORTS_UPDATE_DATE and
--     ORDER_STATUS_TIMESTAMP sit BEFORE composition (median -12h / -25h),
--     DATE_SUBMITTED and REP_DRAFT_TIMESTAMP have 2 rows each, and STUDY_LOCK_TIME is
--     a scheduled auto-lock (median 0.26h, p90 0.27h -- no variance, so not a human act).
--     PACS records THAT a study is approved, never WHEN.
--   * RIS SITE_WORKLIST.APPROVED_DATE -- the column exists but is NULL on all 43,566
--     approved RH exams.
--   * worklist_status_history (migration 0048) -- empty, and its UNIQUE is per canonical
--     stage, so Signed 1/2/3 would collapse into one 'signed' row anyway.
--
-- The RIS REPORT table is the only source that carries the chain, and it carries it
-- cleanly. Measured Jan-Aug 2026: VERIFIED1_DATE 116,519 rows, APPROVED_DATE 64,766,
-- both *_BY_RESOURCE_ID_KEY populated to match, and VERIFIED1 -> APPROVED has a median
-- of 18.7h against a p90 of 69.7h. That 3.7x spread is what a human attending queue
-- looks like -- and it is the test STUDY_LOCK_TIME failed.
--
-- VERIFIED1/2/3 line up with worklist_status_map's Signed 1 (130) / Signed 2 (140) /
-- Signed 3 (150). docs/LAUMC_RIS_TABLES.md tags these columns "-> TAT / KPI Detailed
-- Reading" -- this sheet was scoped against this table before RAYD ever loaded it.
--
-- VERSIONING IS LOAD-BEARING. 134,213 rows for 63,827 distinct REPORT_KEYs (~2.1
-- versions each). Every consumer MUST reduce to the current version or all three
-- measures double-count. Per vendor answer Qr1 the flag is IS_MAX_VERSION; the partial
-- index below is what consumers should drive off.
--
-- Per vendor answer Qr3 ("pull all date/status/people columns RAW; PACS<->RIS status
-- mapping done later") no interpretation happens at load time -- every timestamp and
-- every resource key lands verbatim and the report layer decides what they mean.
--
-- Report CONTENT (DOCUMENT_PLAIN_TEXT, the NLP/CRN feed described in Qr4) is
-- deliberately NOT in scope here. This migration is the TAT chain only; adding a
-- multi-KB text column per version would multiply the table's size for a consumer that
-- does not exist yet. It can be added later without touching what is here.

CREATE TABLE IF NOT EXISTS std_reports (
    -- PK is (report_key, version): Qr1 confirms one row per version, so report_key
    -- alone is not unique.
    report_key                    BIGINT   NOT NULL,
    version                       INTEGER  NOT NULL DEFAULT 0,
    is_max_version                BOOLEAN,

    -- Join keys. reported_acc_number = accession = SPS_ID. WATCH (Qr2): an amended
    -- report gets a NEW accession sequence per version, so joins to the worklist or to
    -- etl_didb_studies must use the max-version row.
    reported_acc_number           TEXT,
    body_key                      BIGINT,
    cr_message_key                BIGINT,
    report_template_key           BIGINT,

    -- Status / lifecycle
    version_status_key            BIGINT,
    finalization_state            TEXT,
    interpretation_type_key       BIGINT,
    addendum                      TEXT,

    -- ── Report-chain timestamps (the reason this table exists) ────────────────
    -- Populations measured on LAUMC RH, Jan-Aug 2026, 134,213 rows:
    draft_date                    TIMESTAMP,   -- 17,071  partial
    wet_read_date                 TIMESTAMP,   --      0  unused at LAUMC
    transcription_date            TIMESTAMP,   --      0  unused at LAUMC
    verified1_date                TIMESTAMP,   -- 116,519  == "Signed 1"
    verified2_date                TIMESTAMP,   --  4,926  == "Signed 2", rare
    verified3_date                TIMESTAMP,   -- 64,766  moves with approved_date
    approved_date                 TIMESTAMP,   -- 64,766  == "Approved"
    reviewed_date                 TIMESTAMP,
    returned_date                 TIMESTAMP,
    report_time                   TIMESTAMP,
    report_created_date           TIMESTAMP,
    last_modified_date            TIMESTAMP,   -- ETL watermark

    -- ── People (RIS resource keys -> std_resources_ris.resource_id_key) ───────
    -- Resolve to a username, never show the key: std_resources_ris.resource_id holds
    -- 'tamina.rizk@ad.umcrh.com_2061750785', and
    --     UPPER(SUBSTRING(resource_id FROM '^[^@]+@[^.]+'))
    -- reduces it to 'TAMINA.RIZK@AD' -- byte-identical to PACS
    -- rep_study_last_composed_by, so the two systems cross-check each other with no
    -- mapping table. Fall back to first_name || ' ' || last_name: some rows carry a
    -- numeric HIS-style resource_id ('20000127') with no AD login.
    -- Do NOT key on names -- 9 Karams, 5 Rizks, 4 Semaans, 3 Ferzlis, and Tamina
    -- Elias-Rizk's surname is spelled two different ways in PERSON.
    reported_by                   BIGINT,
    transcribed_by_resource_id_key BIGINT,
    verified1_by_resource_id_key  BIGINT,
    verified2_by_resource_id_key  BIGINT,
    verified3_by_resource_id_key  BIGINT,
    approved_by_resource_id_key   BIGINT,
    created_by_resource_id_key    BIGINT,
    last_modified_resource_id_key BIGINT,
    signed_behalf_resource_id_key BIGINT,
    wet_read_by_resource_id_key   BIGINT,
    reviewed_by_resource_id_key   BIGINT,
    returned_by_resource_id_key   BIGINT,
    draft_by_resource_id_key      BIGINT,
    report_to                     TEXT,

    -- Effort metrics — free productivity signal, already on the row
    character_count               INTEGER,
    word_count                    INTEGER,
    line_count                    INTEGER,
    total_lines_in_document       INTEGER,
    minutes_of_editing_for_session NUMERIC,

    -- Provenance
    site_id                       INTEGER REFERENCES sites(id),
    source_last_updated           TIMESTAMP,
    last_update                   TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT std_reports_pkey PRIMARY KEY (report_key, version)
);

-- The index every report query drives off: current version only.
CREATE INDEX IF NOT EXISTS idx_std_reports_max_version
    ON std_reports (report_key) WHERE is_max_version;

-- Accession join to SITE_WORKLIST / etl_didb_studies, current version only (Qr2).
CREATE INDEX IF NOT EXISTS idx_std_reports_acc_max
    ON std_reports (reported_acc_number) WHERE is_max_version AND reported_acc_number IS NOT NULL;

-- Report 36 scans a date range and groups by signer; these two carry that.
CREATE INDEX IF NOT EXISTS idx_std_reports_verified1
    ON std_reports (verified1_date) WHERE verified1_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_std_reports_approved
    ON std_reports (approved_date) WHERE approved_date IS NOT NULL;

-- Incremental ETL watermark.
CREATE INDEX IF NOT EXISTS idx_std_reports_last_modified
    ON std_reports (last_modified_date DESC);

COMMENT ON TABLE std_reports IS
    'RIS REPORT mirror — report-chain timestamps and signer identities for Report 36 '
    'KPI Detailed Reading. One row per (report_key, version); filter is_max_version. '
    'Report body text deliberately excluded — TAT chain only.';
