-- Migration 0086: delete studies with an '@dn' signer in ANY signer field.
--
-- Migration 0084 only checked rep_final_signed_by. '@dn' values were then found
-- leaking through rep_prelim_signed_by and rep_study_last_composed_by too (e.g.
-- EVA.D@DN surfacing via rep_prelim_signed_by while rep_final_signed_by was clean) —
-- operator instruction 2026-07-31: same policy as before, not real LAUMC data,
-- delete regardless of which field it showed up in.
--
-- Single plain DELETE, no batching needed at this volume (same reasoning as 0084).
-- Idempotent: re-running finds 0 matching rows and deletes nothing.

DELETE FROM etl_didb_studies
WHERE UPPER(rep_final_signed_by) LIKE '%@DN'
   OR UPPER(rep_prelim_signed_by) LIKE '%@DN'
   OR UPPER(rep_study_last_composed_by) LIKE '%@DN';
