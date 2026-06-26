-- Migration 0053: fully remove NonDICOMAAgent AE title
-- NonDICOMAAgent is the PACS internal non-DICOM document importer.
-- It produces noise studies with mixed modalities and has been excluded
-- from stats since 0048. This migration removes it entirely.

DELETE FROM device_exceptions      WHERE aetitle = 'NonDICOMAAgent';
DELETE FROM device_weekly_schedule WHERE aetitle = 'NonDICOMAAgent';
DELETE FROM summary_storage_daily  WHERE original_storing_ae = 'NonDICOMAAgent';
DELETE FROM etl_didb_studies       WHERE original_storing_ae = 'NonDICOMAAgent';
DELETE FROM aetitle_modality_map   WHERE aetitle = 'NonDICOMAAgent';
