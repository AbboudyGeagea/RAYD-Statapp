-- Migration 0168: the RIS lifecycle feed's first messages, filed as orders, removed (2026-11-18).
-- *** LAUMC BRANCH ONLY ***
--
-- The HIS's orders (MSH-3 SAP_P) and the RIS lifecycle feed (MSH-3 RIS, MSH-4 CARESTREAM) reached
-- RAYD on 2026-11-18 from 11:32. The listener stored each RIS status message (ORC-1 SC, ORC-5
-- SC/A/IP/CM) as a row of hl7_orders, so the pages joining hl7_orders by accession counted a study
-- once per status, and took each RIS "CM" for a PACS completion: it closed orders and wrote
-- hl7_scn_studies with storing_ae = MSH-4 (CARESTREAM). hl7_listener.py no longer does either
-- (RIS_SENDING_APPS); this removes what it wrote. BRAYN's archive (hl7_message_archive) keeps
-- every one of these messages. order_status_log lets go of a deleted order (ON DELETE SET NULL).
-- The HIS's orders carry no accession, so the completions changed none of them.

DELETE FROM public.hl7_orders WHERE UPPER(BTRIM(split_part(raw_message, '|', 3))) = 'RIS';

DELETE FROM public.hl7_scn_studies WHERE UPPER(BTRIM(storing_ae)) = 'CARESTREAM';
