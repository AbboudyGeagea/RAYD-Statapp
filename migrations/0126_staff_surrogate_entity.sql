-- Migration 0126: allow the 'staff' surrogate entity, and index the roster
-- lookup the projector performs.
--
-- std_resources_ris.resource_id_key is a RIS-internal bigint. An imported roster
-- has no such thing — it has the identifier the HL7 messages actually carry, an
-- LDAP account at this site — so the key is minted the same way every other
-- foreign identity on this branch is.
--
-- The lookup that needs the index runs for every study with a performer:
--
--     lower(std_resources_ris.resource_id) = lower(events.performed_by_id)
--
-- Case-insensitive because an LDAP account arriving as RIMA.HADDAD in one
-- message and rima.haddad in another is the same person, and a roster typed by
-- hand will not agree with either consistently. Without a matching functional
-- index that comparison cannot use the column's own index and degrades to a scan
-- of the whole roster per study — small today, linear in staff count forever.

DO $ck$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_hl7_surrogate_entity') THEN
        ALTER TABLE hl7_surrogate_keys DROP CONSTRAINT ck_hl7_surrogate_entity;
    END IF;

    ALTER TABLE hl7_surrogate_keys
        ADD CONSTRAINT ck_hl7_surrogate_entity
        CHECK (entity IN ('study', 'patient', 'order', 'worklist', 'pps', 'staff'));
END
$ck$;

CREATE INDEX IF NOT EXISTS idx_std_resources_resource_id_lower
    ON std_resources_ris (lower(resource_id));

-- Report 35 filters role_code = 'TEC' on every technician query. Partial, because
-- the roster holds every role and the reports that care ask for one at a time.
CREATE INDEX IF NOT EXISTS idx_std_resources_role
    ON std_resources_ris (role_code)
    WHERE role_code IS NOT NULL;

-- The projector deletes and re-inserts a study's person references on every
-- event, so this lookup happens constantly.
CREATE INDEX IF NOT EXISTS idx_pps_person_ref_pps
    ON std_pps_person_reference (pps_key);
