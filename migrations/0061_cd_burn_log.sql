-- Migration 0061: CD burn log, filled by the burning station's REST API.
--
-- The burning station POSTs one JSON event per burn to /api/cd-burn
-- (routes/cd_log_route.py) instead of RAYD pulling CDSURF.TASKS from the burning
-- app's Oracle DB. Report 30 and the CD Burn Summary widget read this table.
-- cd_print_log is left in place, no longer written.
--
-- Ported from the HL7 branch (its migration 0132).

CREATE TABLE IF NOT EXISTS cd_burn_log (
  id SERIAL PRIMARY KEY,
  event_type VARCHAR(50) NOT NULL DEFAULT 'cd_burned',
  timestamp TIMESTAMP NOT NULL,
  burn_mode VARCHAR(50),
  burn_location VARCHAR(100),

  -- Patient information
  patient_id VARCHAR(50),
  patient_name VARCHAR(255),
  patient_dob DATE,

  -- Studies (JSONB array of {"study_uid", "accession_number", "modality", ...})
  studies JSONB NOT NULL DEFAULT '[]',

  -- Burn details
  copies_count INT DEFAULT 1,
  disc_format VARCHAR(20),
  disc_size_mb NUMERIC(10,2),
  disc_label VARCHAR(255),
  burn_duration_seconds INT,

  -- Result tracking
  status VARCHAR(20) NOT NULL DEFAULT 'success',
  error_message TEXT,

  -- Source information
  operator_id VARCHAR(100),
  facility_code VARCHAR(50),
  app_version VARCHAR(20),

  -- Orthanc validation
  orthanc_validated BOOLEAN DEFAULT FALSE,
  orthanc_validation_result JSONB,  -- {"study_uid": {"exists": bool, "instance_count": int, ...}, ...}
  orthanc_validated_at TIMESTAMP,

  -- Metadata
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cd_burn_log_timestamp ON cd_burn_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_cd_burn_log_patient_id ON cd_burn_log(patient_id);
CREATE INDEX IF NOT EXISTS idx_cd_burn_log_status ON cd_burn_log(status);
CREATE INDEX IF NOT EXISTS idx_cd_burn_log_facility ON cd_burn_log(facility_code);
CREATE INDEX IF NOT EXISTS idx_cd_burn_log_orthanc_validated ON cd_burn_log(orthanc_validated);

-- Report 30's dashboard card still said "from CD surf" (0031)
UPDATE report_template
SET long_description = 'CD and DVD burn history sent by the burning station. Shows burn volume by month, disc format (CD vs DVD), and modality. Useful for understanding physical media demand and identifying which study types are burned most frequently.'
WHERE report_id = 30;

-- View: CD burn summary by patient
CREATE OR REPLACE VIEW cd_burn_summary_by_patient AS
SELECT
  patient_id,
  patient_name,
  COUNT(DISTINCT id) as burn_events,
  COUNT(DISTINCT DATE(timestamp)) as burn_days,
  SUM(copies_count) as total_copies,
  SUM(COALESCE(jsonb_array_length(studies), 0)) as total_studies,
  COUNT(DISTINCT facility_code) as facilities,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
  SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns,
  MAX(timestamp) as last_burn_date,
  MIN(timestamp) as first_burn_date
FROM cd_burn_log
WHERE patient_id IS NOT NULL
GROUP BY patient_id, patient_name;

-- View: CD burn summary by facility
CREATE OR REPLACE VIEW cd_burn_summary_by_facility AS
SELECT
  facility_code,
  DATE(timestamp) as burn_date,
  COUNT(*) as burn_events,
  SUM(copies_count) as total_copies,
  COUNT(DISTINCT patient_id) as unique_patients,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
  SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns,
  ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 1) as success_rate_pct
FROM cd_burn_log
GROUP BY facility_code, DATE(timestamp);

-- View: Orthanc validation status
CREATE OR REPLACE VIEW cd_burn_orthanc_validation AS
SELECT
  id,
  patient_id,
  patient_name,
  facility_code,
  timestamp,
  orthanc_validated,
  orthanc_validation_result,
  orthanc_validated_at,
  CASE
    WHEN orthanc_validation_result IS NULL THEN 'pending'
    WHEN orthanc_validation_result->>'all_found' = 'true' THEN 'valid'
    ELSE 'mismatch'
  END as validation_status
FROM cd_burn_log;
