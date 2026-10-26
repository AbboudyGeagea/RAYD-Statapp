-- 0134: move BRAYN's id sequences past the rows 0133 seeded.
--
-- 0133 seeded hl7_status_map, hl7_result_status_map and hl7_field_mappings with
-- their ids (copied from the HL7 branch) but left the sequences at 1, so the first
-- row added from the BRAYN console - a status code, a result status, an accession
-- field - failed on id 1. Safe to run on any state: an empty table sets 1.

SELECT setval('public.hl7_status_map_id_seq',
              GREATEST((SELECT MAX(id) FROM public.hl7_status_map), 1));
SELECT setval('public.hl7_result_status_map_id_seq',
              GREATEST((SELECT MAX(id) FROM public.hl7_result_status_map), 1));
SELECT setval('public.hl7_field_mappings_id_seq',
              GREATEST((SELECT MAX(id) FROM public.hl7_field_mappings), 1));
