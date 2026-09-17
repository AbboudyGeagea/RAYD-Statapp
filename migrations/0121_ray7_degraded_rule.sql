-- Migration 0121: RAY7_DEGRADED, and an explicit duplicate window.
--
-- RAY7_DEGRADED closes a hole in the engine's own honesty.
--
-- RAY7 fails open: when a lookup times out or errors, the rules that depend on it
-- simply find nothing and raise nothing. That is the right behaviour — a screening
-- engine must never block ingestion — but on its own it makes two very different
-- outcomes indistinguishable in the data:
--
--     "RAY7 screened this message and found no problems"
--     "RAY7 could not look"
--
-- Both previously produced an accepted message with no findings. This rule makes the
-- second one say so. It matters most during exactly the conditions that cause it:
-- database pressure, which is when messages are most likely to need screening and
-- least likely to get it.
--
-- Warning rather than critical: a degraded screen is not evidence the message is
-- bad, so it must not quarantine anything. It is a statement about RAY7's coverage,
-- and it belongs in the queue where someone will see a run of them and investigate
-- the database rather than the message.
--
--
-- LOGICAL_DUPLICATE.threshold_minutes gives the duplicate window its own home.
--
-- LOGICAL_DUPLICATE and REPEATED_TRANSITION are two halves of one decision: the same
-- rung reported twice is a duplicate inside the window and a genuine repeat outside
-- it. Both rules therefore read one number, but the engine previously read it from
-- REPEATED_TRANSITION's row only, so an operator tuning that row silently moved a
-- boundary the other rule never advertised it depended on. Seeding the value on
-- LOGICAL_DUPLICATE too gives the studio an obvious place to edit it and makes the
-- coupling visible instead of surprising.
--
-- Guarded by IS NULL so this only fills an unset value and never overwrites a
-- threshold a site has already tuned.

INSERT INTO ray7_rules (rule_code, category, title, description, default_severity)
VALUES
    ('RAY7_DEGRADED', 'referential', 'Screening was degraded',
     'RAY7 could not complete one or more of its lookups for this message, so the rules depending on them did not run. The message was accepted and stored, but it was screened with less information than usual. A run of these points at the database, not at the messages.',
     'warning')
ON CONFLICT (rule_code, modality) DO NOTHING;

UPDATE ray7_rules
   SET threshold_minutes = 10,
       updated_at = NOW()
 WHERE rule_code = 'LOGICAL_DUPLICATE'
   AND modality = ''
   AND threshold_minutes IS NULL;
