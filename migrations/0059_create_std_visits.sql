-- Migration 0059: create std_visits (RIS VISIT table -> RAYD, LAUMC).
--
-- Column set matches ETL_JOBS/system_type_registry.py's SYSTEM_TYPES['RIS']['tables']
-- ['std_visits'] exactly (the vendor-confirmed real schema, docs/LAUMC_RIS_TABLES.md).
-- Created here directly in the main etl_db — NOT via the generic adapter "provision a
-- separate database" path (routes/db_manager.py's ensure_database(), which spins up a
-- standalone rayd_ris database per system type). That path is right for onboarding an
-- arbitrary new client's LIS/HIS, but wrong here: LAUMC's whole multi-site model (RLS,
-- site_id, joins to etl_didb_studies/sites/site_org_map) assumes everything lives in one
-- database, exactly like etl_orders / aetitle_modality_map / procedure_duration_map
-- already do this session — a separate rayd_ris database couldn't join any of that
-- without cross-database plumbing (FDW/dblink) nobody has asked for.
--
-- Feeds case-mix (IP/OP/ER), payer mix, length-of-stay, hospital service. patient_class_
-- key / financial_class_key / hospital_service_key / mobility_status_key are pulled RAW
-- (unresolved) — their lookup tables (PATIENT_CLASS, FINANCIAL_CLASS, HOSPITAL_SERVICE,
-- MOBILITY_STATUS) haven't been provided yet; same "pull raw now, resolve labels later"
-- approach already used for RIS report signature dates (migration/vendor ruling Qr3).
--
-- site_id is NOT resolved by the loader: VISIT carries no org/issuer column of its own
-- (unlike SITE_WORKLIST.org_structure_key or ORDERS.issuer_of_placer_order_number) — it
-- would need a join through etl_orders.visit_dbid, deferred rather than guessed at today.
--
-- deleted='Y' rows are imported, not dropped — per-vendor guidance is to exclude them
-- from STATS (queries should filter deleted != 'Y'), not to lose the data on load.

CREATE TABLE IF NOT EXISTS std_visits (
    visit_key                     BIGINT PRIMARY KEY,
    patient_person_key            BIGINT,
    patient_class_key             BIGINT,   -- -> IP/OP/ER lookup (pending)
    preadmit_number                TEXT,
    visit_number                   TEXT,    -- = HL7 PV1.19
    financial_class_key            BIGINT,   -- -> payer/TPA lookup (pending)
    admit_date_time                 TIMESTAMP,
    discharge_date_time             TIMESTAMP,
    expected_admit_date_time        TIMESTAMP,
    expected_discharge_date_time    TIMESTAMP,
    visit_description               TEXT,
    visit_priority_key              BIGINT,
    hospital_service_key            BIGINT,   -- -> ward/service lookup (pending)
    visit_indicator                 TEXT,
    issuer_of_visit_number          TEXT,
    issuer_of_preadmit_number       TEXT,
    alternate_visit_id              TEXT,
    mobility_status_key             BIGINT,
    created_by_person_key           BIGINT,
    created_on_date                 TIMESTAMP,   -- incremental watermark
    patient_account_number          TEXT,
    is_master                       TEXT,
    deleted                         TEXT,        -- 'Y' rows excluded from stats, not from load
    deleted_date                    TIMESTAMP,
    site_id                         INTEGER REFERENCES sites(id),  -- unresolved for now, see above
    last_update                     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_visits_patient   ON std_visits (patient_person_key);
CREATE INDEX IF NOT EXISTS idx_std_visits_visit_num ON std_visits (visit_number);
CREATE INDEX IF NOT EXISTS idx_std_visits_created   ON std_visits (created_on_date);
