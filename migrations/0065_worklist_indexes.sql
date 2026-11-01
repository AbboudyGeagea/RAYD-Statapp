-- Migration 0065: indexes the Dynamic Worklist views (0064) rely on.
--
--   hl7_oru_reports(accession_number)      rpt.studies finds a study's ORU reports
--   etl_didb_serieses(study_db_uid)        rpt.series filtered by the study's date
--   etl_patient_view(mdl_patient_dbid)     rpt.orders: the patient of an order with no study
--   etl_didb_studies(study_date)           every study date filter (the compiler bounds
--                                          study_date). Same name as optimize.sql's, so
--                                          whichever runs first creates it.
--
-- Not CONCURRENTLY: db_migrations.run_migrations cannot run a CONCURRENTLY statement
-- under SQLAlchemy 2 (it refuses to change the isolation level of a connection that has
-- begun), so optimize.sql only ever applies through update.sh's psql. A plain build
-- blocks writes to each table while it runs (seconds); inserts wait, nothing fails.

CREATE INDEX IF NOT EXISTS idx_oru_reports_accession
    ON hl7_oru_reports (accession_number);

CREATE INDEX IF NOT EXISTS idx_etl_didb_serieses_study_db_uid
    ON etl_didb_serieses (study_db_uid);

CREATE INDEX IF NOT EXISTS idx_etl_patient_view_mdl_patient_dbid
    ON etl_patient_view (mdl_patient_dbid);

CREATE INDEX IF NOT EXISTS idx_etl_didb_studies_study_date
    ON etl_didb_studies (study_date);
