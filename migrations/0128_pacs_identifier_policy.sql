-- 0128 — What happens to the identifiers PACS used to supply
--
-- The Oracle ETL filled a set of columns that HL7 has no equivalent for. The
-- question this migration answers is what those columns should now contain.
-- The answer is NULL, not a generated number, and the reasoning differs by
-- column, so the columns are sorted into three groups.
--
--
-- GROUP 1 — THE SURROGATE PRIMARY KEYS. Already solved; nothing here changes them.
--
--   etl_didb_studies.study_db_uid, etl_patient_view.patient_db_uid,
--   etl_orders.order_dbid, std_pps.pps_key and the worklist keys.
--
-- These are minted by hl7_surrogate_id() (migration 0116) against a persisted
-- natural-key map, from a sequence starting at 9,000,000,000. That IS an
-- auto-increment — but a DETERMINISTIC one, and the difference is the whole
-- point. A plain BIGSERIAL default assigns a new number every time a row is
-- inserted. Replay re-inserts every archived message on purpose, so a BIGSERIAL
-- would produce a second study row for the same accession on every replay
-- instead of updating the first. The map is what makes replay idempotent.
--
--
-- GROUP 2 — GENUINE DICOM/PACS IDENTIFIERS. No HL7 source. They stay NULL.
--
--   study_instance_uid, series_instance_uid, study_id, series_db_uid,
--   raw_image_db_uid, image_number, source_db_uid, linked_id, mdl_patient_dbid,
--   number_of_study_series, number_of_study_images,
--   number_of_patient_series, number_of_patient_images
--
-- Two reasons, one per kind:
--
--   UIDs. A Study Instance UID is an OID-rooted, globally unique DICOM
--   identifier. Generating one would put a value in the column that LOOKS
--   authoritative — it would pass a format check, and anything that later
--   compared it against a real PACS, or exported it, would be trusting a number
--   this system invented. A NULL is unambiguous and cannot be mistaken for the
--   real UID of some other study.
--
--   Counts. NULL means "not known"; 0 means "this study has no images", which is
--   false. SUM and AVG skip NULL and would be dragged toward zero by a zero.
--   These feed the storage report, which is the one report the cutover gives up.
--
--
-- GROUP 3 — NOT PACS IDENTIFIERS AT ALL. HL7 carries them; they were NULL by
-- omission, and the code changes shipped with this migration fill them:
--
--   etl_patient_view rows     — were written only from ADT, so 106 of 108 studies
--                               pointed at a patient row that did not exist.
--                               Now written from any message carrying PID-3.
--   etl_patient_view.fallback_id — reports 22 and 23 display it AS patient_id.
--   etl_patient_view.age_group   — report 22's demographics axis, a super_report filter.
--
-- Also fixed alongside: reports 25, 35 and 36 joined std_pps to etl_didb_studies
-- on study_instance_uid. That column is group 2 — NULL on both sides — and
-- NULL = NULL is never true, so those sections returned no rows at all. They now
-- join on study_db_uid, which is group 1 and populated on both sides.

-- Visible in \d+ and to anyone reading the schema directly.
COMMENT ON COLUMN etl_didb_studies.study_instance_uid IS
    'DICOM Study Instance UID. No HL7 source on this branch unless the site sends '
    'one (IHE puts it in ZDS-1); intentionally NULL rather than generated. '
    'Join on study_db_uid instead. See migration 0128.';
COMMENT ON COLUMN etl_didb_studies.study_id IS
    'DICOM Study ID. No HL7 source; intentionally NULL. See migration 0128.';
COMMENT ON COLUMN etl_didb_studies.number_of_study_series IS
    'Series count from PACS. No HL7 source; NULL means unknown, not zero. See migration 0128.';
COMMENT ON COLUMN etl_didb_studies.number_of_study_images IS
    'Image count from PACS. No HL7 source; NULL means unknown, not zero. See migration 0128.';
COMMENT ON COLUMN etl_didb_studies.site_id IS
    'Never populated, on this branch or the Oracle one. Site scoping goes through '
    'aetitle_modality_map.site_id.';
COMMENT ON COLUMN etl_patient_view.number_of_patient_series IS
    'Series count from PACS. No HL7 source; NULL means unknown, not zero. See migration 0128.';
COMMENT ON COLUMN etl_patient_view.number_of_patient_images IS
    'Image count from PACS. No HL7 source; NULL means unknown, not zero. See migration 0128.';
COMMENT ON COLUMN etl_patient_view.fallback_id IS
    'Same PID-3 as id. Two DIDB columns on the Oracle branch; HL7 has one patient '
    'identifier, and reports 22/23 display this one. See migration 0128.';
COMMENT ON COLUMN etl_orders.study_instance_uid IS
    'DICOM Study Instance UID. No HL7 source; intentionally NULL. See migration 0128.';
COMMENT ON COLUMN etl_orders.linked_id IS
    'PACS-internal study linking. No HL7 equivalent; intentionally NULL.';

-- The same warning where an implementation engineer will actually meet it: the
-- mapping explorer reads these descriptions for the columns it derives from
-- information_schema. Someone pointing a mapping at study_instance_uid is
-- probably about to synthesise one, and this is the moment to say so.
INSERT INTO hl7_field_targets
    (target_kind, target_field, data_type, label, description, is_dangerous, sort_order)
VALUES
    ('study', 'study_instance_uid', 'text', 'etl_didb_studies.study_instance_uid',
     'DICOM Study Instance UID. Map this ONLY if the site genuinely sends one — IHE '
     'carries it in ZDS-1. Never map a value that is not a real UID: it would look '
     'authoritative and could shadow a real study. RAYD joins on study_db_uid and '
     'does not need this.', true, 900),
    ('study', 'study_id', 'text', 'etl_didb_studies.study_id',
     'DICOM Study ID. No HL7 source; left NULL by design.', true, 901),
    ('study', 'number_of_study_series', 'integer', 'etl_didb_studies.number_of_study_series',
     'Series count. NULL means unknown. Do not map a placeholder — 0 is a false '
     'claim that the study has no series, and it drags SUM and AVG down.', true, 902),
    ('study', 'number_of_study_images', 'integer', 'etl_didb_studies.number_of_study_images',
     'Image count. NULL means unknown. Do not map a placeholder — 0 is a false '
     'claim that the study has no images, and it drags SUM and AVG down.', true, 903),
    ('study', 'site_id', 'integer', 'etl_didb_studies.site_id',
     'Not used. Site scoping goes through aetitle_modality_map.site_id, so mapping '
     'this changes nothing on any report.', true, 904),
    ('patient', 'fallback_id', 'text', 'etl_patient_view.fallback_id',
     'Already filled with PID-3 by the projector, same as id. Reports 22 and 23 '
     'display this column as the Patient ID.', true, 905),
    ('patient', 'age_group', 'text', 'etl_patient_view.age_group',
     'Already filled by the projector: current age in whole years, as text — not a '
     'bucket label. report_22 groups on it directly.', true, 906)
ON CONFLICT (target_kind, target_field) DO UPDATE SET
    description  = EXCLUDED.description,
    label        = EXCLUDED.label,
    data_type    = EXCLUDED.data_type,
    is_dangerous = EXCLUDED.is_dangerous,
    sort_order   = EXCLUDED.sort_order;
