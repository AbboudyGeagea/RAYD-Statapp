-- Migration 0053: extend the EXISTING modality/procedure tables to receive RIS data
-- *** LAUMC ***
--
-- Decision (2026-07): the RIS MODALITY table loads into the existing `aetitle_modality_map`,
-- and RIS SPS_CODE loads into the existing `procedure_duration_map` — NOT into separate
-- std_devices / std_procedure_codes tables. This keeps every report, the capacity ladder,
-- the device grid and the mapping tab working UNCHANGED (they already read these tables),
-- instead of migrating the whole codebase to new tables.
--
-- These columns carry the extra richness the RIS registry has (room name/code, active flag,
-- body part) plus a back-reference to the RIS source key.
--
-- RIS import policy (vendor-confirmed): FILL-ONLY. New devices/procedures are inserted;
-- existing rows are never overwritten (manual mapping-tab edits and RAYD-owned fields like
-- daily_capacity_minutes are preserved), and the RIS import never DELETES rows.
-- => the importer uses ON CONFLICT (aetitle) / (procedure_code) DO NOTHING.

-- ── Devices: RIS MODALITY -> aetitle_modality_map ─────────────────────────────
-- Existing cols already cover the essentials: aetitle (<- AE_TITLE), modality
-- (<- resolved MODALITY_TYPE), description (<- DESCRIPTION), site_id (<- via site_org_map),
-- daily_capacity_minutes (RAYD-owned). Add the rest:
ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS station_name      TEXT,        -- MODALITY.STATION_NAME (room display / floor-map label)
    ADD COLUMN IF NOT EXISTS room_code         VARCHAR(64), -- MODALITY.CODE (e.g. CT64, MAMO1)
    ADD COLUMN IF NOT EXISTS active            BOOLEAN DEFAULT TRUE,  -- MODALITY active flag (exclude demo/dead devices)
    ADD COLUMN IF NOT EXISTS ris_modality_key  BIGINT;      -- MODALITY.MODALITY_KEY (source back-reference)

-- ── Procedures: RIS SPS_CODE -> procedure_duration_map ────────────────────────
-- Existing cols: procedure_code (<- CODE), procedure_name (<- DESCRIPTION),
-- duration_minutes (<- DURATION), modality (resolved), clinical/technical_rvu (RAYD-owned).
ALTER TABLE procedure_duration_map
    ADD COLUMN IF NOT EXISTS active            BOOLEAN DEFAULT TRUE,  -- SPS_CODE.ACTIVE
    ADD COLUMN IF NOT EXISTS body_part         TEXT,        -- resolved from SPS_CODE.BODY_PART_KEY
    ADD COLUMN IF NOT EXISTS ris_sps_code_key  BIGINT;      -- SPS_CODE.SPS_CODE_KEY (source back-reference)
