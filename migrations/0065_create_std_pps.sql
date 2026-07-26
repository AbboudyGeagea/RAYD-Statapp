-- Migration 0065: create std_pps (RIS PPS -> Performed Procedure Step, LAUMC).
--
-- The "treasure table" — a performed-procedure-step-level record (as opposed to
-- SITE_WORKLIST's scheduled/order-level view). Columns confirmed by operator,
-- 2026-07-27, connects to peer_review/orders/patient/status/sps_code/site_pps/
-- procedure_priority/dictation/modality.
--
-- Excluded per operator instruction ("we're not going there yet"): TECHNURSE_NOTES,
-- DEMONSTRATION_NOTES — free-text clinical notes, same category as the patient-names
-- exclusion (migration 0060). Not fetched from Oracle at all.
--
-- procedure_code/procedure_name resolved via a live SPS_CODE join at extract time
-- (matches procedure_duration_map.procedure_code — same pattern as etl_orders.py).
-- performing_ae_title resolved via a live MODALITY join (PPS.MODALITY_KEY = the DEVICE,
-- i.e. MODALITY.MODALITY_KEY — not just a modality TYPE) — matches
-- aetitle_modality_map.ris_modality_key / .aetitle, more precise than the modality-type
-- level resolution used elsewhere.
--
-- status_key/priority_key/status_reason_key/dictation_key/created_by_person_key/
-- primary_tech_person_key/protocol_key/protocol_status_key are all pulled RAW (no FK
-- enforced) — resolve via std_status_ris/std_procedure_priorities/std_dictations/
-- std_resources_ris at query time. primary_tech_person_key joins std_resources_ris on
-- person_key specifically (it's a *_PERSON_KEY column, not *_RESOURCE_ID_KEY).
--
-- study_db_uid: NOT populated by the loader — resolved by a separate enrichment pass
-- (ETL_JOBS/etl_ris_pps.py) matching PPS.STUDY_INSTANCE_UID against
-- etl_didb_studies.study_instance_uid. This is the empirical test of whether PPS gives
-- us the clean RIS<->PACS join that accession/linked_id matching could only
-- approximate (blocked item #4, docs/LAUMC_NEXT_SESSION.md) — a real DICOM UID, not an
-- inferred value. The match rate reported by that job tells us whether it worked.
--
-- No site_id: PPS carries no org/issuer column of its own (SITE_PPS, despite the name,
-- turned out to be a QA/compliance extension table, not a site lookup — see migration
-- 0066). Deferred, same as std_visits.

CREATE TABLE IF NOT EXISTS std_pps (
    pps_key                        BIGINT PRIMARY KEY,
    patient_person_key             BIGINT,
    performed_procedure_step_id    TEXT,
    pps_instance_uid               TEXT,
    study_instance_uid             TEXT,
    study_db_uid                   BIGINT REFERENCES etl_didb_studies(study_db_uid),  -- enrichment target
    pps_code_key                   BIGINT,
    procedure_code                 TEXT,   -- resolved via SPS_CODE join
    procedure_name                 TEXT,   -- resolved via SPS_CODE join
    description                    TEXT,   -- PPS's own description (may differ from procedure_name)
    start_datetime                 TIMESTAMP,
    end_datetime                   TIMESTAMP,
    discontinued_reason            TEXT,
    modality_key                   BIGINT,   -- raw source key (device-level, not modality-type)
    performing_ae_title            TEXT,     -- resolved via MODALITY join — matches aetitle_modality_map.aetitle
    patient_folder_key             BIGINT,
    report_key                     BIGINT,
    dictation_key                  BIGINT,
    status_key                     BIGINT,
    priority_key                   BIGINT,
    status_reason_key              BIGINT,
    created_by_person_key          BIGINT,
    created_date                   TIMESTAMP,   -- incremental watermark
    worklist_flagset                TEXT,
    linked_id                        BIGINT,     -- cross-check vs SITE_WORKLIST.LINKED_ID later
    birads_key                       BIGINT,
    created_by_mpps                   TEXT,
    considered_for_review              TEXT,     -- peer-review flag; full PEER_REVIEW table not built yet
    followup_override                   TEXT,
    radiation_dose                       NUMERIC(10,4),
    radiation_dose_units                  TEXT,
    inv_auto_populate_flag                 TEXT,
    external_reporting_action               TEXT,
    cr_message_key                           BIGINT,
    primary_tech_person_key                   BIGINT,  -- joins std_resources_ris.person_key
    protocol_key                               BIGINT,
    protocol_status_key                         BIGINT,
    last_update                                  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_pps_patient        ON std_pps (patient_person_key);
CREATE INDEX IF NOT EXISTS idx_std_pps_study_uid       ON std_pps (study_instance_uid);
CREATE INDEX IF NOT EXISTS idx_std_pps_study_db_uid    ON std_pps (study_db_uid);
CREATE INDEX IF NOT EXISTS idx_std_pps_tech            ON std_pps (primary_tech_person_key);
CREATE INDEX IF NOT EXISTS idx_std_pps_status          ON std_pps (status_key);
CREATE INDEX IF NOT EXISTS idx_std_pps_created_date    ON std_pps (created_date);
CREATE INDEX IF NOT EXISTS idx_std_pps_linked_id       ON std_pps (linked_id);
