-- Migration 0176: BRAYN reads the RIS's SC with ORC-5 empty as a booking change (LAUMC to-do
-- item 17, 2026-11-20). *** LAUMC BRANCH ONLY *** BRAYN's own tables only.
--
-- Production, 19 Nov: the only status BRAYN still could not identify after 0170 was RIS SC with
-- ORC-5 empty, 3 messages, all on one RIS number (SC/SC > SC/ > SC/ > SC/), each with another room
-- (SJH-MRI, SJH-MAMO) and appointment: the RIS re-sends the booking when it changes. The study is
-- still scheduled; held, its new room and time never reached the Live Department board.
-- utils/brayn.resolve_status matches a row with order_status '' to an empty ORC-5 (this date).
-- Messages already held: the BRAYN console's Re-check.

INSERT INTO public.hl7_status_map (sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes)
VALUES ('RIS', 'SC', '', 'scheduled', 40, TRUE,
        'LAUMC RIS: SC with ORC-5 empty = the booking changed (room, appointment); still scheduled (2026-11-20)')
ON CONFLICT (sending_app, order_control, order_status) DO NOTHING;
