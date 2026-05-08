-- Patient CD Print Log
-- Stores every CD burn event pulled from the CD surf Oracle DB.
-- task_id is the unique burn event key from CDSURF.TASKS.

CREATE TABLE IF NOT EXISTS cd_print_log (
    id                  SERIAL PRIMARY KEY,
    task_id             BIGINT UNIQUE NOT NULL,
    patient_name        TEXT,
    burned_at           TIMESTAMP,
    task_status         INTEGER,
    burn_identifier     TEXT,
    study_instance_uid  TEXT,
    cd_id               BIGINT,
    number_of_copies    INTEGER,
    cd_status           INTEGER,
    media_type          TEXT,
    cd_folder           TEXT,
    order_id            TEXT,
    study_db_uid        BIGINT,
    accession_number    TEXT,
    study_date          DATE,
    study_modality      TEXT,
    reading_physician   TEXT,
    synced_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cd_print_log_burned_at      ON cd_print_log(burned_at);
CREATE INDEX IF NOT EXISTS idx_cd_print_log_study_db_uid   ON cd_print_log(study_db_uid);
CREATE INDEX IF NOT EXISTS idx_cd_print_log_patient_name   ON cd_print_log(patient_name);
CREATE INDEX IF NOT EXISTS idx_cd_print_log_study_instance ON cd_print_log(study_instance_uid);
