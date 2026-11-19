-- Migration 0178: "Scheduled, never arrived" counts 2 hours from the appointment (LAUMC to-do item
-- 17, 2026-11-20). *** LAUMC BRANCH ONLY *** BRAYN's own tables only.
--
-- The sweep now measures STALLED_SCHEDULED from the appointment the RIS sends (OBR-36, migration
-- 0173) instead of the booking, and skips the HIS's duplicate copy whose twin went ahead
-- (utils/brayn_sweep._stall_statement). The seeded 48 hours fitted a clock started at booking; from
-- the appointment, 2 hours late with no arrival is the no-show nobody cancelled (production dry run,
-- 20 Nov: 41). The rule stays OFF (0133); it is switched on in the BRAYN console.

UPDATE public.brayn_rules
   SET threshold_minutes = 120, updated_at = NOW(),
       description = 'The appointment passed 2 hours ago and the patient never arrived: a no-show nobody cancelled, or the arrival was not sent. Counts from the appointment the RIS sends; skips the HIS''s duplicate copy whose twin went ahead.'
 WHERE rule_code = 'STALLED_SCHEDULED' AND modality = '' AND threshold_minutes = 2880;
