-- Migration 0124: allow 'worklist' and 'pps' surrogate entities.
--
-- The RIS worklist tables (std_worklist_arrivals, std_worklist_exam_done,
-- std_pps) are keyed on site_worklist_key and pps_key — RIS-internal bigints that
-- HL7 has no equivalent for, exactly as with study_db_uid and patient_db_uid
-- before them. They need minting the same way, so the entity CHECK from 0116 has
-- to widen.
--
-- WHY NOT JUST REUSE THE STUDY SURROGATE
-- On this branch one accession produces one worklist entry and one performed
-- procedure step, so pps_key could simply BE study_db_uid and the joins would all
-- work. Rejected: it silently asserts a 1:1 that is a property of today's HL7
-- feed rather than of the domain. A multi-step procedure — one order, several
-- performed steps — is ordinary radiology, and the day one arrives the shared key
-- would collapse those steps into each other with no error. Distinct key spaces
-- cost one lookup and keep the model honest about what it is claiming.
--
-- Recreating the constraint rather than adding a second one: two overlapping
-- CHECKs on the same column is how you end up with a row that satisfies neither
-- obviously. Guarded so the file stays re-runnable, since bare ADD CONSTRAINT is
-- what permanently broke migration 0089.

DO $ck$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_hl7_surrogate_entity') THEN
        ALTER TABLE hl7_surrogate_keys DROP CONSTRAINT ck_hl7_surrogate_entity;
    END IF;

    ALTER TABLE hl7_surrogate_keys
        ADD CONSTRAINT ck_hl7_surrogate_entity
        CHECK (entity IN ('study', 'patient', 'order', 'worklist', 'pps'));
END
$ck$;
