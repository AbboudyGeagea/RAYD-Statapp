-- Migration 0120: give bone densitometry its own modality (BMD) and stop reporting on it.
--
-- OPERATOR INSTRUCTION (2026-09-19): remove BMD from all reports.
--
-- 'BMD' did not exist as a value anywhere in this system, so it could not be excluded by
-- name. GELUNAR (the GE Lunar bone-densitometry unit) was mapped to modality 'OT' --
-- migration 0083 seeded 'GELUNAR11' that way, and 0117 confirmed the real 'GELUNAR' AE
-- the same way ("map says OT and PACS agrees (99.9%)"). That produced two problems:
--
--   1. DEXA's fate depended on whether a given report happened to filter 'OT'. It was
--      dropped from report_22 / report_23 / viewer_controller (which exclude 'OT') and
--      counted in report_25 / report_35 / report_36 (which exclude only 'SR', or
--      'SR'+'PACS'). The same study was in one report and not another.
--   2. Excluding it via 'OT' also discards every genuinely-unclassified study, which is
--      collateral damage -- 'OT' is a dumping ground, not a device type.
--
-- Naming it BMD makes the exclusion explicit and auditable: the WHERE clauses now say
-- what they exclude instead of catching it by accident, and a future decision to report
-- on bone densitometry again is a one-line change rather than an archaeology exercise.
--
-- NET EFFECT ON EXISTING NUMBERS: none for report_22 / report_23 / viewer_controller --
-- those already dropped GELUNAR as 'OT' and now drop it as 'BMD'. report_25 / 31 / 32 /
-- 34 / 35 / 36 / super_report / financial_dashboard and the rest will each lose the
-- GELUNAR volume they had been silently including. That is the requested change, not a
-- regression, but it does mean those reports' totals shift the day this lands.
--
-- Studies keep loading. This changes reporting only -- nothing is deleted, the ETL still
-- ingests GELUNAR, and etl_didb_studies.study_modality is left alone (every report
-- resolves modality as COALESCE(aetitle_modality_map.modality, study_modality), so the
-- map wins and the raw PACS value stays available for audit).
--
-- Idempotent: re-running is a no-op once the rows already read 'BMD'.

UPDATE aetitle_modality_map
SET    modality = 'BMD'
WHERE  UPPER(TRIM(aetitle)) IN ('GELUNAR', 'GELUNAR11')
  AND  COALESCE(UPPER(TRIM(modality)), '') <> 'BMD';

DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM   aetitle_modality_map
    WHERE  UPPER(TRIM(modality)) = 'BMD';

    RAISE NOTICE '0120: % AE title(s) now mapped to BMD', n;

    IF n = 0 THEN
        -- Not fatal: a non-LAUMC install has no GELUNAR, and the report-side exclusions
        -- are harmless no-ops there. Worth a notice so it is not mistaken for success
        -- on an install that DOES have the device under another AE title.
        RAISE NOTICE '0120: no GELUNAR/GELUNAR11 row found — if this install has a bone '
                     'densitometry unit under a different AE title, map it to BMD by hand.';
    END IF;
END $$;
