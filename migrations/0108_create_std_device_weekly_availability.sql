-- Migration 0108: simple Available/Unavailable weekly availability per device (LAUMC).
--
-- Per operator instruction (2026-08-01): keep it simple -- Available vs Unavailable only
-- (not the richer Reserved-for-IP/Maintenance/Holiday/etc breakdown AVAILABILITY_INDICATOR
-- actually carries -- see docs/LAUMC_RIS_TABLES.md). Only availability_indicator_key = 1
-- ("Available", confirmed against the real AVAILABILITY_INDICATOR export) counts as
-- available time; everything else (Unavailable, Closed, Reserved-for-IP, ED-reserved,
-- Maintenance, ...) counts as not-available for this simple utilization denominator.
--
-- std_schedule_template_items carries OVERLAPPING, SUPERSEDED rows within the same device/
-- day -- confirmed against a real RH-CT64 export (2026-08-01): an old single 07:00-17:44
-- Available block sits alongside a newer split (08:00-12:59 Available / 13:00-14:59
-- Reserved-for-IP / 15:00-17:59 Available) for the same day. run_device_weekly_availability_etl
-- (ETL_JOBS/etl_ris_modality_availability.py) resolves this via an interval sweep keyed on
-- source_last_updated (most recent write wins per overlapping time slot) -- these two new
-- TIME columns are what make that interval math possible (from_time/to_time keep the raw
-- captured value as before; from_time_of_day/to_time_of_day are the parsed HH:MM:SS used
-- for the sweep).
--
-- DAY_OF_WEEK convention confirmed 2026-08-01 (RH-CT64 export cross-checked against the
-- vendor's own scheduling-grid screenshot, Su first/Sa last, plus the data's own structure:
-- days 1-5 share one weekday pattern, days 0 and 6 both carry a full-day-Unavailable
-- override -- the weekend): RIS is 0=Sunday...6=Saturday. RAYD's device_weekly_schedule
-- convention (0=Monday...6=Sunday, see CLAUDE.md) is different, so
-- run_device_weekly_availability_etl converts: rayd_day = (ris_day + 6) % 7.
--
-- std_device_weekly_availability is the RIS-derived counterpart to device_weekly_schedule
-- (RAYD-owned/manually-editable) -- same separation-of-concerns already used for
-- std_modality_exceptions vs device_exceptions (migration 0067). Fully rebuilt every ETL
-- pass (TRUNCATE + insert, not upsert) -- operator instruction: schedules "change almost
-- weekly," so this must never carry a stale row forward.

ALTER TABLE std_schedule_template_items
    ADD COLUMN IF NOT EXISTS from_time_of_day TIME,
    ADD COLUMN IF NOT EXISTS to_time_of_day   TIME;

CREATE TABLE IF NOT EXISTS std_device_weekly_availability (
    aetitle           TEXT    NOT NULL,
    day_of_week       INTEGER NOT NULL,   -- RAYD convention: 0=Mon .. 6=Sun (matches device_weekly_schedule)
    available_minutes INTEGER NOT NULL DEFAULT 0,
    last_update       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aetitle, day_of_week)
);
