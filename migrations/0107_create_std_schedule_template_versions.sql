-- Migration 0107: RIS SCHEDULE_TEMPLATE_VERSION (LAUMC) -- the version bridge that lets
-- std_schedule_template_items resolve to a specific device.
--
-- Confirmed against the operator-provided export (2026-08-01): SCHEDULE_TEMPLATE_KEY is
-- one-to-many to SCHEDULE_TEMPLATE_VERSION_KEY (a template can have several named versions,
-- e.g. "Base" vs "Base 2019"), with DEFAULT_VERSION='Y' marking the currently-active one.
-- SCHEDULE_TEMPLATE_ITEM.SCHEDULE_TEMPLATE_VERSION_KEY (migration 0067) is what actually
-- carries the day/time rows -- confirmed against a real SCHEDULE_TEMPLATE_ITEM sample whose
-- SCHEDULE_TEMPLATE_VERSION_KEY values (5052, 3520, 5041, 5040...) are real keys from this
-- table. So the full chain to a device is now:
--   SCHEDULE_TEMPLATE_ITEM.schedule_template_version_key
--     -> SCHEDULE_TEMPLATE_VERSION (WHERE default_version = TRUE)
--     -> schedule_template_key
--     -> aetitle_modality_map.ris_schedule_template_key (migration 0106)
--     -> aetitle
-- Only the DEFAULT_VERSION='Y' row per template is treated as "current" -- non-default
-- versions (e.g. "Base 2019") are historical/alternate and are imported faithfully but
-- never resolved onto a device.
--
-- NOT resolved by this migration: DAY_OF_WEEK convention (0=Mon vs 0=Sun) and what
-- AVAILABILITY_INDICATOR_KEY values (1/2/8/2100 observed) actually mean (Available/
-- Unavailable/Partial/On-call?) -- both still unconfirmed, imported raw exactly as
-- migration 0067 already does, not guessed at here.

CREATE TABLE IF NOT EXISTS std_schedule_template_versions (
    schedule_template_version_key  BIGINT PRIMARY KEY,
    schedule_template_key          BIGINT,
    version                        TEXT,
    description                    TEXT,
    default_version                BOOLEAN,
    source_last_updated            TIMESTAMP,
    last_update                    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_std_sched_tmpl_versions_template ON std_schedule_template_versions (schedule_template_key);

-- Resolved device for the currently-active version's schedule rows. std_schedule_template_items
-- is a pure RIS mirror, never manually edited (migration 0067), so this is safe to overwrite
-- unconditionally on every ETL pass -- no fill-only/conflict handling needed here.
ALTER TABLE std_schedule_template_items
    ADD COLUMN IF NOT EXISTS aetitle TEXT;
CREATE INDEX IF NOT EXISTS idx_std_sched_tmpl_items_aetitle ON std_schedule_template_items (aetitle);
