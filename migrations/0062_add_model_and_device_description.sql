-- Migration 0062: add manufacturer_model_name to etl_didb_serieses
-- and device_description to aetitle_modality_map.
--
-- manufacturer_model_name (DICOM 0008,1090) is pulled from Oracle's
-- medistore.didb_serieses so each AE title can display its full
-- human-readable device name (e.g. "GE MEDICAL SYSTEMS Optima CT520 Series").
--
-- device_description on aetitle_modality_map is populated by ETL Phase 8
-- and shown in the mapping config UI.

ALTER TABLE etl_didb_serieses
    ADD COLUMN IF NOT EXISTS manufacturer_model_name TEXT;

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS device_description TEXT;
