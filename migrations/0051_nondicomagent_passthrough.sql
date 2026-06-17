-- 0051: Make NonDICOMAGENT a passthrough AE (no modality override)
--
-- Problem: real CT/MR/US studies arrive in the PACS with STORING_AE = 'NonDICOMAGENT'
-- because not all imaging devices are individually registered in the PACS device registry.
-- The ETL faithfully copies this value. Phase 2b then correctly derives study_modality
-- from etl_didb_serieses (series-level DICOM modality tags), giving us the right modality
-- per study.
--
-- However, Phase 6 (AE auto-mapper) saw NonDICOMAGENT in etl_didb_studies and inserted
-- it into aetitle_modality_map with the most common series modality (OT or MR). All stat
-- queries use COALESCE(m.modality, s.study_modality), so the mapped modality overrides
-- the correct study_modality — either stamping every study with the wrong modality, or
-- (via exclude_from_stats = TRUE from migration 0050) dropping real studies entirely.
--
-- Fix:
--   1. Allow modality to be NULL in aetitle_modality_map (passthrough rows).
--   2. Keep exactly one NonDICOMAGENT row with modality = NULL and exclude_from_stats = FALSE.
--      - COALESCE(m.modality, s.study_modality) falls through to study_modality (correct).
--      - The existing NOT IN ('SR','OT') modality filter handles OT/SR garbage cleanup.
--      - The ON CONFLICT (aetitle) DO NOTHING in Phase 6 preserves this row permanently.

BEGIN;

-- 1. Allow NULL modality for passthrough AEs
ALTER TABLE aetitle_modality_map ALTER COLUMN modality DROP NOT NULL;

-- 2. Remove all existing NonDICOMAGENT rows (migration 0050 may have left one with OT/MR)
DELETE FROM aetitle_modality_map WHERE UPPER(TRIM(aetitle)) = 'NONDICOMAGENT';

-- 3. Insert the passthrough sentinel row:
--    modality = NULL        → COALESCE falls through to study_modality (from Phase 2b series data)
--    exclude_from_stats = FALSE → real studies are no longer excluded
INSERT INTO aetitle_modality_map (aetitle, modality, exclude_from_stats, daily_capacity_minutes)
VALUES ('NonDICOMAGENT', NULL, FALSE, 0);

COMMIT;
