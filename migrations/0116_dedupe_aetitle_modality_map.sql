-- Migration 0116: collapse case-duplicate rows in aetitle_modality_map.
--
-- aetitle_modality_map has UNIQUE (aetitle) on the RAW string, but every consumer joins
-- on UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae)). So 'ct99' and 'CT99' are both
-- legal rows AND both match the same studies -- every study on that device is counted
-- TWICE, in report 22, report 23, report 25, the capacity ladder and the yesterday tab
-- alike.
--
-- Confirmed against production 2026-09-18: the yesterday tab showed CT99 = 30 studies
-- where PACS has 15. Exactly 2x, because the map holds both 'ct99' and 'CT99'.
--
-- Nine pairs exist, and in every one the two rows carry IDENTICAL modality and
-- room_name -- they are accidental duplicates from the RIS import (Phase 10,
-- run_ris_modality_etl) landing a differently-cased AE_TITLE alongside an existing
-- manual or seeded row, not two different devices:
--
--     bay87ct/BAY87CT   ct99/CT99   DR300_StoreSCU/DR300_STORESCU
--     fluorostar/FLUOROSTAR   fluorostare/FLUOROSTARE   kizuna/KIZUNA
--     Philips/PHILIPS   rhpacslaumcrh/RHPACSLAUMCRH   Ziehm-Solo/ZIEHM-SOLO
--
-- WHY THIS IS NOT A BLIND "DELETE THE LOWERCASE ONE". device_exceptions.aetitle and
-- device_weekly_schedule.aetitle are FKs to aetitle_modality_map(aetitle) with
-- ON DELETE CASCADE. If the spelling a device's schedule happens to reference is the
-- one deleted, that schedule silently disappears -- and a device with no schedule falls
-- back to a default capacity, quietly changing every utilisation figure for it.
--
-- So the keeper is chosen by DEPENDENTS first (operator decision 2026-09-18), uppercase
-- only as a tie-break:
--     1. the spelling with the most device_weekly_schedule + device_exceptions rows
--     2. then the already-uppercase spelling
--     3. then the lowest id, so the result is deterministic on a re-run
-- Dependents on the losing spelling are RE-POINTED to the keeper before the delete,
-- except where the keeper already has an equivalent row (same day_of_week, or same
-- exception_date) -- those would violate the target's own uniqueness, and they are
-- duplicates of a row the keeper already has, so letting them cascade away is correct.
--
-- Written as a loop over whatever duplicates actually exist rather than a hardcoded
-- list of the nine: the RIS import runs every cycle and can create a new pair at any
-- time, so re-running this migration after a fresh import should clean it again. It is
-- idempotent -- once there are no duplicate groups the loop body never executes.
--
-- Does NOT change any modality value. Modality corrections are migration 0117, kept
-- separate so this purely structural fix can ship without waiting on sign-off.

DO $$
DECLARE
    grp     RECORD;
    keeper  TEXT;
    removed INT := 0;
    moved   INT := 0;
    n       INT;
BEGIN
    FOR grp IN
        SELECT UPPER(TRIM(aetitle)) AS norm
        FROM aetitle_modality_map
        GROUP BY UPPER(TRIM(aetitle))
        HAVING COUNT(*) > 1
    LOOP
        SELECT m.aetitle INTO keeper
        FROM aetitle_modality_map m
        WHERE UPPER(TRIM(m.aetitle)) = grp.norm
        ORDER BY
            (  (SELECT COUNT(*) FROM device_weekly_schedule d WHERE d.aetitle = m.aetitle)
             + (SELECT COUNT(*) FROM device_exceptions     e WHERE e.aetitle = m.aetitle)
            ) DESC,
            (m.aetitle = UPPER(TRIM(m.aetitle))) DESC,
            m.id ASC
        LIMIT 1;

        UPDATE device_weekly_schedule d
        SET    aetitle = keeper
        WHERE  UPPER(TRIM(d.aetitle)) = grp.norm
          AND  d.aetitle <> keeper
          AND  NOT EXISTS (SELECT 1 FROM device_weekly_schedule k
                           WHERE k.aetitle = keeper AND k.day_of_week = d.day_of_week);
        GET DIAGNOSTICS n = ROW_COUNT; moved := moved + n;

        UPDATE device_exceptions e
        SET    aetitle = keeper
        WHERE  UPPER(TRIM(e.aetitle)) = grp.norm
          AND  e.aetitle <> keeper
          AND  NOT EXISTS (SELECT 1 FROM device_exceptions k
                           WHERE k.aetitle = keeper AND k.exception_date = e.exception_date);
        GET DIAGNOSTICS n = ROW_COUNT; moved := moved + n;

        DELETE FROM aetitle_modality_map
        WHERE UPPER(TRIM(aetitle)) = grp.norm
          AND aetitle <> keeper;
        GET DIAGNOSTICS n = ROW_COUNT; removed := removed + n;

        RAISE NOTICE '0116: % -> kept [%], removed % duplicate row(s)', grp.norm, keeper, n;
    END LOOP;

    RAISE NOTICE '0116 done: % duplicate map rows removed, % dependent rows re-pointed',
                 removed, moved;
END $$;
