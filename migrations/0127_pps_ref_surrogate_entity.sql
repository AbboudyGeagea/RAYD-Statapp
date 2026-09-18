-- Migration 0127: allow the 'pps_ref' surrogate entity.
--
-- std_pps_person_reference.pps_person_reference_key is NOT NULL with no default
-- and no sequence — it is the RIS's own key, and an HL7-sourced install has to
-- mint it like every other foreign identity here. The projector's insert omitted
-- it and failed on the not-null constraint; the row was skipped, its savepoint
-- rolled back, and every technologist name stayed unresolved while the rest of
-- the projection carried on looking healthy.
--
-- Fourth entity added in three migrations (worklist, pps, staff, now pps_ref),
-- which is a pattern worth naming: the list grows every time the projector
-- covers another RIS table, and it will keep growing. The CHECK is kept anyway.
-- entity values are code constants rather than user input, so it catches little,
-- but a typo'd entity silently mints a PARALLEL key space — the surrogate would
-- be perfectly valid and simply refer to nothing anyone else looks up, which is
-- exactly the kind of fault that surfaces as an empty report months later. A
-- migration per entity is a cheap price for that failing loudly instead.

DO $ck$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_hl7_surrogate_entity') THEN
        ALTER TABLE hl7_surrogate_keys DROP CONSTRAINT ck_hl7_surrogate_entity;
    END IF;

    ALTER TABLE hl7_surrogate_keys
        ADD CONSTRAINT ck_hl7_surrogate_entity
        CHECK (entity IN ('study', 'patient', 'order', 'worklist', 'pps',
                          'staff', 'pps_ref'));
END
$ck$;
