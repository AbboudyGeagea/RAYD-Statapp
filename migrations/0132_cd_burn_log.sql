-- Migration 0132: CD Burn Log Table
-- Stores DICOM CD/DVD burn events from external media burning applications

BEGIN TRANSACTION;

CREATE TABLE cd_burn_log (
  id SERIAL PRIMARY KEY,
  event_type VARCHAR(50) NOT NULL DEFAULT 'cd_burned',
  timestamp TIMESTAMP NOT NULL,
  burn_mode VARCHAR(50),
  burn_location VARCHAR(100),

  -- Patient information
  patient_id VARCHAR(50),
  patient_name VARCHAR(255),
  patient_dob DATE,

  -- Studies (stored as JSONB array)
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

CREATE INDEX idx_cd_burn_log_timestamp ON cd_burn_log(timestamp);
CREATE INDEX idx_cd_burn_log_patient_id ON cd_burn_log(patient_id);
CREATE INDEX idx_cd_burn_log_status ON cd_burn_log(status);
CREATE INDEX idx_cd_burn_log_facility ON cd_burn_log(facility_code);
CREATE INDEX idx_cd_burn_log_orthanc_validated ON cd_burn_log(orthanc_validated);

-- View: CD burn summary by patient
CREATE VIEW cd_burn_summary_by_patient AS
SELECT
  patient_id,
  patient_name,
  COUNT(DISTINCT id) as burn_events,
  COUNT(DISTINCT DATE(timestamp)) as burn_days,
  SUM(copies_count) as total_copies,
  SUM(COALESCE(json_array_length(studies), 0)) as total_studies,
  COUNT(DISTINCT facility_code) as facilities,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
  SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns,
  MAX(timestamp) as last_burn_date,
  MIN(timestamp) as first_burn_date
FROM cd_burn_log
WHERE patient_id IS NOT NULL
GROUP BY patient_id, patient_name;

-- View: CD burn summary by facility
CREATE VIEW cd_burn_summary_by_facility AS
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
CREATE VIEW cd_burn_orthanc_validation AS
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

COMMIT;
