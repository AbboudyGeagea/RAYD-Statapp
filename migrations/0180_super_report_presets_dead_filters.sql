-- Migration 0180: the Super Report's saved presets lose the filters the report never applied
-- (review of 2026-11-20). *** LAUMC BRANCH ONLY ***
--
-- routes/super_report._request_filters read series_modality, body_part_series and protocol_name
-- and no query applied them; age_group was applied but nothing on the page offered it (the age
-- filter is the min / max range). A preset holding one of them looked as if it filtered and did
-- not. The route no longer reads them; here they leave the presets (Super Report presets are the
-- saved_reports rows with no base report). updated_at is left alone: it orders the list.

UPDATE public.saved_reports
   SET filter_json = filter_json - ARRAY['series_modality', 'body_part_series', 'protocol_name', 'age_group']
 WHERE base_report_id IS NULL
   AND filter_json ?| ARRAY['series_modality', 'body_part_series', 'protocol_name', 'age_group'];
