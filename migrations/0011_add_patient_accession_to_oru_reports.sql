-- Migration 0011: Add patient_id and accession_number to hl7_oru_reports
-- These fields are extracted from HL7 ORU^R01 PID and OBR segments going forward.
-- Existing rows will have NULL values.

ALTER TABLE hl7_oru_reports
    ADD COLUMN IF NOT EXISTS patient_id       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS accession_number VARCHAR(100);
