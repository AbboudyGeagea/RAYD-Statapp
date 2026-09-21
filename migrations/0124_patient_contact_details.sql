-- Migration 0124: patient contact details from RIS site_person (LAUMC).
--
-- WHY THESE COLUMNS AND NOT THE EXISTING ONES
-- std_patients_ris already has mobile_phone_number / primary_email_address /
-- secondary_email_address / pager_number, sourced from PERSON. Measured 2026-09-21:
-- all four are NULL on all 469,791 rows. They are not sparsely filled -- PERSON simply
-- does not carry contact data at this site, so the ETL has been faithfully copying
-- empty columns since it was written.
--
-- The real values live on a different table: site_person, keyed by the SAME person_key
-- (confirmed by the RIS-side reminder service, which joins both and takes names from
-- PERSON while taking the phone from site_person). Measured on the RIS:
--     475,076 rows / 475,076 distinct person_key   -- exactly one row per person
--     434,595 rows with PATIENT_PHONE_NUMBER       -- 91.5% filled
-- The 1:1 grain is why this is three more columns on std_patients_ris rather than a
-- child table the way std_patient_ids was split out.
--
-- The old PERSON-sourced columns are deliberately LEFT IN PLACE, not dropped or
-- backfilled into: they are a faithful record of what PERSON holds (nothing), other
-- sites may populate them, and silently repointing a column that means "PERSON's
-- mobile" at a different table would make the two impossible to tell apart later.
--
-- SOURCE COLUMNS
--   site_person.PATIENT_PHONE_NUMBER  VARCHAR2(40)  -> patient_phone_raw
--   site_person.OTHER_PHONE_NUMBER    VARCHAR2(64)  -> other_phone_raw
--   site_person.EMAIL_ID              VARCHAR2(60)  -> email_id
--
-- patient_phone_raw is stored VERBATIM alongside the normalised form because the field
-- is free text and carries meaning the digits alone lose:
--   * 1,962 rows hold TWO numbers in one field (17-18 chars)
--   * staff write an 'R' into it to mark "this is intentionally a relative's number"
--   * landlines share the column with mobiles (01: 2,028 rows, 07: 1,458)
--   * ~19,000 rows are junk -- a bare '0', '00', '000', '0000'
-- Normalisation is utils/phone_lb.py, a deliberate verbatim port of the RIS reminder
-- service's own parser, so both systems resolve a given patient to the same number.
--
-- WHAT IS DELIBERATELY NOT PULLED
-- site_person also carries SSN, NATIONAL_ID, INTERNAL_ID, MC_NUMBER, RELIGION,
-- MARITAL_STATUS, CITIZENSHIP, NATIONALITY, ETHNICITY, PERSON_BIRTHPLACE,
-- IS_ALLERGIC/ALLERGY_COMMENT and Arabic name fields (ARABIC_FIRST_NAME,
-- ARABIC_LAST_NAME, FAMILYARABICNAME, GRAND_FATHER_NAME...). None of it is copied.
-- The name fields in particular stay excluded under migration 0060's standing rule
-- that patient names are never copied into Postgres; the rest is sensitive personal
-- data with no reporting use.
--
-- NOTE ON THAT RULE: 0060 excludes names but this adds a phone number and an email,
-- which reach a patient at least as directly. That is intentional and operator-
-- requested (2026-09-21) -- these feed patient-facing workflows that need a way to
-- contact someone about their own study. Recorded here so the asymmetry reads as a
-- decision rather than an oversight.

ALTER TABLE std_patients_ris
    ADD COLUMN IF NOT EXISTS patient_phone_raw         TEXT,
    ADD COLUMN IF NOT EXISTS patient_phone_normalized  TEXT,
    ADD COLUMN IF NOT EXISTS patient_phone_is_landline BOOLEAN,
    ADD COLUMN IF NOT EXISTS patient_phone_is_relative BOOLEAN,
    ADD COLUMN IF NOT EXISTS other_phone_raw           TEXT,
    ADD COLUMN IF NOT EXISTS email_id                  TEXT;

COMMENT ON COLUMN std_patients_ris.patient_phone_raw IS
    'site_person.PATIENT_PHONE_NUMBER verbatim. Free text: may hold two numbers, an '
    '"R" relative marker, a landline, or junk. Never parse this directly -- use '
    'patient_phone_normalized, or utils/phone_lb.resolve_phone if you need the raw.';
COMMENT ON COLUMN std_patients_ris.patient_phone_normalized IS
    'Bare international form, e.g. 96170123456 (no +). NULL when the raw value is a '
    'landline or unusable. Produced by utils/phone_lb.py, a verbatim port of the RIS '
    'reminder service parser so both systems agree on a patient''s number.';
COMMENT ON COLUMN std_patients_ris.patient_phone_is_relative IS
    'Staff wrote an "R" into the phone field: the number intentionally belongs to a '
    'relative (e.g. a parent''s number on a child''s record). Distinguishes an '
    'explained shared number from an unexplained collision between two patients.';

-- Lookup by number (support asking "whose number is this", and the shared-number check
-- the reminder service performs before sending). Partial: 8.5% of rows have no phone
-- and ~19,000 more normalise to NULL, and none of those are ever searched for.
CREATE INDEX IF NOT EXISTS idx_std_patients_ris_phone_norm
    ON std_patients_ris (patient_phone_normalized)
    WHERE patient_phone_normalized IS NOT NULL;
