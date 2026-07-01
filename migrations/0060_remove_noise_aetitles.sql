-- Migration 0060: remove NonDICOMAgent and SVSM from aetitle_modality_map.
-- These AEs are PACS internal noise and must never appear in modality counts.
-- ETL Phase 8 now filters them at insert time; this cleans up existing rows.

DELETE FROM device_weekly_schedule
WHERE UPPER(TRIM(aetitle)) IN ('NONDICOMAGENT', 'SVSM');

DELETE FROM device_exceptions
WHERE UPPER(TRIM(aetitle)) IN ('NONDICOMAGENT', 'SVSM');

DELETE FROM aetitle_modality_map
WHERE UPPER(TRIM(aetitle)) IN ('NONDICOMAGENT', 'SVSM');
