-- Migration 0174: BRAYN reads the RIS's modality from OBR-18 (LAUMC to-do item 17, the BRAYN
-- enhancements). *** LAUMC BRANCH ONLY ***
--
-- LAUMC's RIS lifecycle messages carry the modality in OBR-18 (MG, RF, MR, US; production sample
-- of 19 Nov, every ORC-5), where BRAYN never looks: its parser reads OBR-24, then OBR-19, then
-- OBR-17, all empty here, so every RIS study in brayn_study_state had no modality. A field mapping
-- for sender RIS, after the product's OBR-24 row (priority 20 against 10): the first non-empty
-- value wins, so a RIS that one day fills OBR-24 keeps that. Editable in BRAYN's mapping studio.

INSERT INTO public.hl7_field_mappings
    (sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, priority, transform, notes)
VALUES ('RIS', 'status', 'parsed', 'modality', 'OBR', 18, 1, 20, 'upper',
        'LAUMC RIS: OBR-18 = the modality (migration 0174)')
ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority) DO NOTHING;

-- The RIS studies already in BRAYN: their modality from the latest RIS message since 18 Nov.
WITH latest AS (
    SELECT DISTINCT ON (acc) acc, mod
      FROM (SELECT NULLIF(btrim(split_part(split_part(substring(a.raw_message from 'ORC\|[^\r]*'), '|', 4), '^', 1)), '') AS acc,
                   NULLIF(upper(btrim(split_part(split_part(substring(a.raw_message from 'OBR\|[^\r]*'), '|', 19), '^', 1))), '') AS mod,
                   a.received_at, a.id
              FROM public.hl7_message_archive a
             WHERE a.sending_app = 'RIS') m
     WHERE acc IS NOT NULL AND mod IS NOT NULL
     ORDER BY acc, received_at DESC, id DESC
)
UPDATE public.brayn_study_state s
   SET modality = l.mod
  FROM latest l
 WHERE s.accession_number = l.acc
   AND s.modality IS NULL;
