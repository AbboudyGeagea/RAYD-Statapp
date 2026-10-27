-- Migration 0047: lock a patient-portal account after repeated wrong passwords
-- The username is the MRN, which is not secret. routes/portal_bp.py counts
-- failures and locks the account for 15 minutes after 10 in a row.

ALTER TABLE patient_portal_users
    ADD COLUMN IF NOT EXISTS failed_login_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE patient_portal_users
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP;
