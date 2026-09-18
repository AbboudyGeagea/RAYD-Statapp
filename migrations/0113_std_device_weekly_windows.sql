-- Migration 0113: persist the RESOLVED opening-hour windows per device/weekday.
--
-- std_device_weekly_availability (migration 0108) already answers "how many minutes is
-- this device available on a Tuesday", which is all the utilization DENOMINATOR needed.
-- It cannot answer "was this 16:50 exam inside opening hours", because it collapses the
-- schedule to a single minute count and discards the windows themselves.
--
-- Report 25's opening-hours / after-hours split (operator request 2026-09-18) needs those
-- windows. The interval sweep that produces them already exists and is already correct --
-- ETL_JOBS/etl_ris_modality_availability.py's _COMPUTE_WEEKLY_AVAILABILITY_SQL resolves
-- std_schedule_template_items' OVERLAPPING, SUPERSEDED rows (an old 07:00-17:44 Available
-- block coexisting with a newer 08:00-12:59 / 13:00-14:59 / 15:00-17:59 split for the same
-- device and day) into non-overlapping slots, most-recent-write-wins by
-- source_last_updated. Its `winning` CTE holds exactly the rows this table stores; the
-- job simply summed them and threw the intervals away. This migration gives them
-- somewhere to live, so the sweep is not reimplemented (and not re-derived per request)
-- anywhere else.
--
-- is_available mirrors 0108's deliberately simple rule: availability_indicator_key = 1
-- ("Available") is open, everything else (Unavailable, Closed, Reserved-for-IP,
-- ED-reserved, Maintenance, ...) is closed. Non-available slots are still STORED rather
-- than filtered out, so a future report can distinguish "closed" from "reserved for
-- inpatients" without another migration.
--
-- day_of_week is the RAYD convention (0=Mon..6=Sun, per CLAUDE.md), already converted from
-- the RIS's 0=Sunday by the ETL -- same as std_device_weekly_availability, so the two
-- tables join directly.
--
-- Rebuilt in full (TRUNCATE + insert) on every ETL pass, same as
-- std_device_weekly_availability -- operator instruction (2026-08-01): schedules "change
-- almost weekly", so a stale row must never survive a pass.

CREATE TABLE IF NOT EXISTS std_device_weekly_windows (
    aetitle       TEXT    NOT NULL,
    day_of_week   INTEGER NOT NULL,   -- RAYD convention: 0=Mon .. 6=Sun
    from_time     TIME    NOT NULL,
    to_time       TIME    NOT NULL,
    is_available  BOOLEAN NOT NULL,
    last_update   TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aetitle, day_of_week, from_time)
);

-- The report looks up "which window contains this timestamp" per device/day, so the
-- lookup is (aetitle, day_of_week) then a range test on from_time.
CREATE INDEX IF NOT EXISTS idx_std_device_weekly_windows_lookup
    ON std_device_weekly_windows (aetitle, day_of_week, from_time, to_time)
    WHERE is_available;
