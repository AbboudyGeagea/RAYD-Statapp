-- Migration 0122: ray7_study_state gains patient_location and procedure_text.
--
-- Both were omitted from 0119 by oversight, and both are needed by the projector
-- rather than by RAY7 itself:
--
--   patient_location  -> etl_didb_studies.patient_location, which the ER dashboard
--                        and every location-scoped report read. Without it the ER
--                        panel cannot tell an emergency study from any other,
--                        since ER detection at this site keys on the location
--                        prefix rather than on patient class.
--   procedure_text    -> etl_didb_studies.study_description, the human-readable
--                        procedure name shown in report listings. The code alone
--                        leaves every row displaying something like "ECABDPEL".
--
-- They sit alongside patient_class and procedure_code, which 0119 did include —
-- these are simply their other halves, and the pairing is why the omission was
-- easy to miss.
--
-- Caught the first time the projector ran against real data: the study upsert
-- failed with "column st.procedure_text does not exist" while the archive, the
-- lifecycle events and the patient projection all still committed, which is the
-- per-statement SAVEPOINT design behaving exactly as intended.

ALTER TABLE ray7_study_state
    ADD COLUMN IF NOT EXISTS patient_location TEXT,
    ADD COLUMN IF NOT EXISTS procedure_text   TEXT;
