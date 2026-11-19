-- Migration 0170: BRAYN learns the HIS's and the RIS's status codes (item 17, 2026-11-19).
-- *** LAUMC BRANCH ONLY ***
--
-- The first day of the live feeds (18-19 Nov, production): BRAYN could not classify 225 RIS and
-- 589 HIS messages (UNKNOWN_STATUS_CODE, "not identified") and no study ever reached "arrived":
--   RIS   SC / A                 225   the patient arrived (between SC scheduled and IP started)
--   HIS   RQ / ^E0001..^E0006    532   the order changed on the HIS; ORC-5 holds SAP's own code
--   HIS   CA / c^E0001, c^E0003   57   the order cancelled on the HIS
--   PACS  FAP (OBX-11)         4,694   final approved: the radiologist's final signature
--   PACS  AAP                     11   amended after approval (item 30): a new final report
-- The HIS's SAP codes vary, so its rows use the wildcard status '*' (utils/brayn.resolve_status,
-- this date): any status for that sender and order control; an exact row still wins.
-- Rows only added (rule 7: the seeded maps are never truncated). Every message held so far is
-- classified again with the BRAYN console's Re-check.

INSERT INTO public.hl7_status_map (sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes)
VALUES ('RIS',   'SC', 'A', 'arrived',   60, TRUE, 'LAUMC RIS lifecycle feed: patient arrived (SC, A, IP, CM; 2026-11-19)'),
       ('SAP_P', 'RQ', '*', 'ordered',   20, TRUE, 'LAUMC HIS: order changed; ORC-5 is a SAP status code (^E0001...)'),
       ('SAP_P', 'CA', '*', 'cancelled', -1, TRUE, 'LAUMC HIS: order cancelled; ORC-5 is a SAP status code (c^E0001...)')
ON CONFLICT (sending_app, order_control, order_status) DO NOTHING;

INSERT INTO public.hl7_result_status_map (sending_app, result_status, canonical_state, ladder_rank, active, notes)
VALUES ('CARESTREAM', 'FAP', 'signed_final', 140, TRUE, 'LAUMC PACS: final approved, the final signature (2026-11-19)'),
       ('CARESTREAM', 'AAP', 'signed_final', 140, TRUE, 'LAUMC PACS: amended after approval (item 30); a new final report')
ON CONFLICT (sending_app, result_status) DO NOTHING;
