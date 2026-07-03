-- Migration 0063: convert the site to MODALITY-ONLY analytics.
--
-- Background: the PACS overwrites original_storing_ae with 'NonDicomAgent'
-- via a scheduled "Update DICOM tags" job, permanently destroying device
-- identity for ~24% of studies. No automated recovery exists (station_name,
-- audit trail, and AUTOROUTER.COMMANDS_DATA were all investigated and ruled
-- out). Decision: ditch AE-title tracking entirely and run all analytics on
-- modality type.
--
-- Mechanism: original_storing_ae now holds the MODALITY GROUP (CT, MR, US...)
-- instead of a device AE. aetitle_modality_map gets exactly one row per
-- modality (aetitle = modality). Every existing join/report/dropdown keeps
-- working unchanged, but is now modality-based, and NO study is hidden.
-- ETL Phase 2b maintains this normalization on every run.

BEGIN;

-- ────────────────────────────────────────────────────────────────────
-- STEP 1: Recompute study_modality from series, preferring REAL
-- clinical modalities over OT (scanned docs) and SR (structured reports).
-- ────────────────────────────────────────────────────────────────────

-- Tier 1: studies with at least one real-modality series
UPDATE etl_didb_studies s
SET study_modality = sub.modality
FROM (
    SELECT study_db_uid,
           MODE() WITHIN GROUP (ORDER BY modality) AS modality
    FROM etl_didb_serieses
    WHERE modality IS NOT NULL AND TRIM(modality) != ''
      AND modality NOT IN ('SR', 'OT')
    GROUP BY study_db_uid
) sub
WHERE s.study_db_uid = sub.study_db_uid
  AND s.study_modality IS DISTINCT FROM sub.modality;

-- Tier 2: document-only studies (no real-modality series) -> OT
UPDATE etl_didb_studies s
SET study_modality = sub.modality
FROM (
    SELECT ser.study_db_uid,
           MODE() WITHIN GROUP (ORDER BY ser.modality) AS modality
    FROM etl_didb_serieses ser
    WHERE ser.modality IS NOT NULL AND TRIM(ser.modality) != ''
      AND ser.modality != 'SR'
      AND NOT EXISTS (
          SELECT 1 FROM etl_didb_serieses x
          WHERE x.study_db_uid = ser.study_db_uid
            AND x.modality IS NOT NULL AND TRIM(x.modality) != ''
            AND x.modality NOT IN ('SR', 'OT')
      )
    GROUP BY ser.study_db_uid
) sub
WHERE s.study_db_uid = sub.study_db_uid
  AND s.study_modality IS DISTINCT FROM sub.modality;

-- Tier 3: SR-only studies stay SR (filtered out by the SR convention)
UPDATE etl_didb_studies s
SET study_modality = 'SR'
WHERE s.study_modality IS DISTINCT FROM 'SR'
  AND EXISTS (
      SELECT 1 FROM etl_didb_serieses x WHERE x.study_db_uid = s.study_db_uid
  )
  AND NOT EXISTS (
      SELECT 1 FROM etl_didb_serieses x
      WHERE x.study_db_uid = s.study_db_uid
        AND x.modality IS NOT NULL AND TRIM(x.modality) != ''
        AND x.modality != 'SR'
  );

-- ────────────────────────────────────────────────────────────────────
-- STEP 2: Normalize original_storing_ae = modality group.
-- Studies with no series at all fall back to 'OT' so nothing is hidden.
-- ────────────────────────────────────────────────────────────────────
UPDATE etl_didb_studies
SET original_storing_ae = COALESCE(NULLIF(TRIM(study_modality), ''), 'OT')
WHERE original_storing_ae IS DISTINCT FROM
      COALESCE(NULLIF(TRIM(study_modality), ''), 'OT');

-- ────────────────────────────────────────────────────────────────────
-- STEP 3: Aggregate existing per-device config per modality BEFORE
-- rebuilding the map, so manually tuned capacities are preserved.
-- Multiple devices of one modality sum their capacity (3 MR x 480 = 1440).
-- ────────────────────────────────────────────────────────────────────
CREATE TEMP TABLE _mod_cap ON COMMIT DROP AS
SELECT UPPER(TRIM(modality)) AS modality,
       SUM(COALESCE(daily_capacity_minutes, 480)) AS cap_min
FROM aetitle_modality_map
WHERE modality IS NOT NULL AND TRIM(modality) != ''
  AND COALESCE(exclude_from_stats, FALSE) = FALSE
  AND UPPER(TRIM(aetitle)) NOT IN ('NONDICOMAGENT', 'SVSM')
GROUP BY UPPER(TRIM(modality));

CREATE TEMP TABLE _mod_sched ON COMMIT DROP AS
SELECT UPPER(TRIM(m.modality)) AS modality,
       ws.day_of_week,
       SUM(COALESCE(ws.std_opening_minutes, 720)) AS mins
FROM device_weekly_schedule ws
JOIN aetitle_modality_map m ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle))
WHERE m.modality IS NOT NULL AND TRIM(m.modality) != ''
  AND COALESCE(m.exclude_from_stats, FALSE) = FALSE
GROUP BY UPPER(TRIM(m.modality)), ws.day_of_week;

CREATE TEMP TABLE _mod_exc ON COMMIT DROP AS
SELECT UPPER(TRIM(m.modality)) AS modality,
       e.exception_date,
       SUM(e.actual_opening_minutes) AS mins,
       STRING_AGG(DISTINCT e.reason, '; ') AS reason
FROM device_exceptions e
JOIN aetitle_modality_map m ON UPPER(TRIM(e.aetitle)) = UPPER(TRIM(m.aetitle))
WHERE m.modality IS NOT NULL AND TRIM(m.modality) != ''
GROUP BY UPPER(TRIM(m.modality)), e.exception_date;

-- ────────────────────────────────────────────────────────────────────
-- STEP 4: Rebuild the map: one row per modality, nothing excluded.
-- ────────────────────────────────────────────────────────────────────
DELETE FROM device_weekly_schedule;
DELETE FROM device_exceptions;
DELETE FROM aetitle_modality_map;

INSERT INTO aetitle_modality_map
    (aetitle, modality, daily_capacity_minutes, exclude_from_stats, display_aetitle)
SELECT mods.mod, mods.mod, COALESCE(c.cap_min, 480), FALSE, mods.mod
FROM (
    SELECT DISTINCT UPPER(TRIM(study_modality)) AS mod
    FROM etl_didb_studies
    WHERE study_modality IS NOT NULL AND TRIM(study_modality) != ''
    UNION
    SELECT modality FROM _mod_cap
) mods
LEFT JOIN _mod_cap c ON c.modality = mods.mod
WHERE mods.mod != 'SR'
ON CONFLICT (aetitle) DO NOTHING;

INSERT INTO device_weekly_schedule (aetitle, day_of_week, std_opening_minutes)
SELECT s.modality, s.day_of_week, s.mins
FROM _mod_sched s
JOIN aetitle_modality_map m ON m.aetitle = s.modality
ON CONFLICT (aetitle, day_of_week) DO NOTHING;

INSERT INTO device_exceptions (aetitle, exception_date, actual_opening_minutes, reason)
SELECT e.modality, e.exception_date, e.mins, e.reason
FROM _mod_exc e
JOIN aetitle_modality_map m ON m.aetitle = e.modality;

-- ────────────────────────────────────────────────────────────────────
-- STEP 5: Clear derived storage rollup rows keyed by old device AEs.
-- ETL Phase 7 rebuilds them from the normalized data on the next run.
-- ────────────────────────────────────────────────────────────────────
DELETE FROM summary_storage_daily
WHERE original_storing_ae NOT IN (SELECT aetitle FROM aetitle_modality_map);

COMMIT;
