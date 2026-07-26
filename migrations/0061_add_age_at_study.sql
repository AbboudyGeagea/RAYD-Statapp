-- Migration 0061: add age_at_study to etl_didb_studies (LAUMC).
--
-- Neither PACS nor RIS stores a trustworthy per-study age value on its own (PACS's own
-- age_at_exam is computed from PACS's patient birth_date, which can be a placeholder —
-- e.g. ER quick-registrations use DOB 9999-11-11). age_at_study is computed at ETL time
-- instead, from the RIS's birth_date (std_patients_ris, migration 0060) via the
-- study<->patient bridge etl_orders already provides (study_db_uid -> the study,
-- patient_dbid -> std_patients_ris.patient_person_key). See
-- ETL_JOBS/etl_ris_patients.py's enrichment step.
--
-- Kept as a SEPARATE column from the existing age_at_exam (PACS-computed) rather than
-- overwriting it — same "pull/compute both, don't silently replace" caution already
-- applied elsewhere this session (e.g. reading_physician_id widened, not reinterpreted).

ALTER TABLE etl_didb_studies
    ADD COLUMN IF NOT EXISTS age_at_study NUMERIC(5,2);
