-- 0129 — the seeded PID-7 mapping was undoing parse_birth_date()
--
-- Migration 0123 seeded hl7_field_mappings with rows reproducing the hardcoded
-- parser positions, so that the overlay started out as a no-op and configuring a
-- field meant editing a row rather than adding one. One of those rows was
--
--     parsed.birth_date <- PID-7, transform 'date'
--
-- and 'date' is a dumber thing than the parser it was supposed to reproduce.
-- parse_birth_date() drops the quick-registration sentinels — 99991111, 00000000,
-- 18000101, the values an ER uses when a patient is admitted before being
-- identified — and anything outside living memory. The 'date' transform just
-- parsed the digits.
--
-- The overlay runs in parse_message()'s finally block, AFTER the hardcoded parse.
-- So parse_birth_date() correctly returned None, and the overlay then put
-- 9999-11-11 back. hl7_patients has a CHECK constraining birth_date to
-- 1880..2100, so the INSERT was rejected — and because the patient row is what
-- etl_patient_view is built from, the study kept a patient_db_uid pointing at a
-- patient that did not exist.
--
-- Found by replaying the archive after widening the hl7_patients write beyond ADT
-- (migration 0128's group 3). The unit tests did not catch it: they test
-- parse_birth_date and they test the overlay, and each is correct on its own. The
-- defect is that the second runs after the first.
--
-- 'birth_date' is now a transform in its own right, calling the real parser.
-- The generic 'date' transform also gained the same plausibility window, so the
-- next column someone maps a sentinel onto fails safe rather than repeating this.

-- ck_hl7_fm_transform enumerates the allowed transforms, so it has to admit the
-- new one before the UPDATE below can set it.
ALTER TABLE hl7_field_mappings DROP CONSTRAINT IF EXISTS ck_hl7_fm_transform;
ALTER TABLE hl7_field_mappings ADD CONSTRAINT ck_hl7_fm_transform
    CHECK (transform IN ('text', 'upper', 'datetime', 'date', 'birth_date',
                         'name_xpn', 'name_xcn', 'number'));

UPDATE hl7_field_mappings
   SET transform = 'birth_date'
 WHERE target_kind = 'parsed'
   AND target_field = 'birth_date'
   AND transform = 'date';
