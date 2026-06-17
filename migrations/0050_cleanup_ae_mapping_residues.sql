-- 0050: Clean up AETitle mapping residues
--
-- Two issues found during audit:
--
-- 1. NonDICOMAAgent: migration 0048 used ON CONFLICT (aetitle) which only sets
--    exclude_from_stats=TRUE without changing the modality. If the UNIQUE constraint
--    on aetitle was not enforced at DB creation time, a second residue row (MR)
--    may coexist alongside the OT row. This migration marks ALL NonDICOMAAgent rows
--    as excluded and removes any duplicates (keeping the highest-id row).
--
-- 2. SVSM: internal PACS AETitle, not a real imaging device. Excluded and deleted.
--    device_exceptions and device_weekly_schedule have ON DELETE CASCADE so related
--    rows in those tables are removed automatically.

BEGIN;

-- 1. Mark every NonDICOMAAgent row as excluded (handles both OT and MR residues)
UPDATE aetitle_modality_map
SET exclude_from_stats = TRUE
WHERE UPPER(TRIM(aetitle)) = 'NONDICOMAGENT';

-- 2. Remove duplicate NonDICOMAAgent rows, keeping only the one with the highest id
--    (no-op if only one row exists)
DELETE FROM aetitle_modality_map
WHERE UPPER(TRIM(aetitle)) = 'NONDICOMAGENT'
  AND id NOT IN (
      SELECT MAX(id) FROM aetitle_modality_map
      WHERE UPPER(TRIM(aetitle)) = 'NONDICOMAGENT'
  );

-- 3. Remove SVSM entirely (internal PACS AETitle, not a real imaging device)
DELETE FROM aetitle_modality_map
WHERE UPPER(TRIM(aetitle)) = 'SVSM';

COMMIT;
