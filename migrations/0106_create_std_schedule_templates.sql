-- Migration 0106: RIS SCHEDULE_TEMPLATE + MODALITY_SCHEDULE_GROUP (LAUMC device
-- availability groundwork).
--
-- SCHEDULE_TEMPLATE_KEY names are literal device/room names (RH-CT64, RH-MRI_3T,
-- RH-US Room 1...) for device templates (RESOURCE_TEMPLATE_FLAG='N'), and physician
-- names for resource/physician availability templates (RESOURCE_TEMPLATE_FLAG='Y',
-- e.g. "Dr. Sahar Semaan") -- NOT devices, must not be resolved onto aetitle_modality_map.
--
-- MODALITY_SCHEDULE_GROUP_KEY is a scheduling group/site discriminator (Radiology/
-- NonRad/Default/SJH/Vascular Lab) -- SJH and Vascular Lab match the ORG_STRUCTURE site
-- split already documented in docs/LAUMC_RIS_TABLES.md (org 5320=SJH, org 5120=VASC).
--
-- Each row of MODALITY_SCHEDULE pairs exactly one SCHEDULE_TEMPLATE_KEY with exactly one
-- MODALITY_KEY (confirmed against the sample export), so for device-type templates this
-- is a direct, unambiguous device<->template link -- resolved onto aetitle_modality_map
-- as a back-reference, same convention as ris_modality_key / ris_sps_code_key (migration
-- 0053). Only set when unambiguous (ETL_JOBS/etl_ris_modality_schedule.py) -- consistent
-- with the "don't guess on a mapping conflict" fix applied to procedure_duration_map.modality.
--
-- Still does NOT resolve device open/closing times: std_schedule_template_items (migration
-- 0067) joins on SCHEDULE_TEMPLATE_VERSION_KEY / SCHEDULE_SCHEME_KEY, neither of which has
-- a confirmed relationship to SCHEDULE_TEMPLATE_KEY yet -- not guessed at here.

CREATE TABLE IF NOT EXISTS std_schedule_templates (
    schedule_template_key   BIGINT PRIMARY KEY,
    name                    TEXT,
    description             TEXT,
    resource_template_flag  BOOLEAN,             -- Y = physician/resource template, not a device
    source_last_updated     TIMESTAMP,
    last_update             TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS std_modality_schedule_groups (
    modality_schedule_group_key  BIGINT PRIMARY KEY,
    name                         TEXT,
    source_last_updated          TIMESTAMP,
    last_update                  TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS ris_schedule_template_key BIGINT;  -- MODALITY_SCHEDULE.SCHEDULE_TEMPLATE_KEY, resolved only when unambiguous
