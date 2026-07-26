-- Migration 0068: std_schedule_schemes + std_availability_indicators (LAUMC).
--
-- Resolve std_schedule_template_items.schedule_scheme_key and
-- .availability_indicator_key / std_modality_exceptions.availability_indicator_key
-- (migration 0067). Small reference tables, full pull, no date filter.
--
-- SCHEDULE_SCHEME turned out to be a generic template CATEGORY (received 2026-07-27:
-- "Scheme 1"/"Scheme 2"/"All Green"/"Emerg"/"OutPatient"/"InPatient"/"Normal"), not
-- device-specific — it carries no modality/device reference of its own. So resolving
-- this key gives a readable scheme NAME, but does NOT yet answer "which devices use
-- this scheme" — that assignment lives somewhere else, not yet identified (candidate:
-- an undocumented column on MODALITY, given the vendor's own note that MODALITY has
-- ~19 columns and only a subset have been confirmed/named so far).
--
-- AVAILABILITY_INDICATOR (received 2026-07-27, 12 rows) is a scheduling-calendar
-- display config table (COLOR/ALTERNATE_COLOR are "R,G,B" text triplets, stored raw —
-- not parsed) but its DESCRIPTION values give real semantic meaning: 03=Available,
-- 04=Unavailable, 07=Holiday, 11=Maintenance, 2100=Closed are the "device NOT
-- available" states for utilization purposes; 01/02/05/06/08/09/10 (Reserved for
-- head/ER/IP, Overtime, N-day-advance booking limits, External people) are booking-rule
-- nuances on an otherwise physically-open device, not closures.

CREATE TABLE IF NOT EXISTS std_schedule_schemes (
    schedule_scheme_key   BIGINT PRIMARY KEY,
    code                  TEXT,
    description           TEXT,
    default_flag          BOOLEAN,
    active                BOOLEAN,
    source_last_updated   TIMESTAMP,
    last_update           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS std_availability_indicators (
    availability_indicator_key   BIGINT PRIMARY KEY,
    code                          TEXT,
    description                   TEXT,
    color                          TEXT,   -- "R,G,B" text triplet, stored raw
    alternate_color                 TEXT,
    allow_days_in_advance             INTEGER,
    allow_n_next_days                  INTEGER,
    default_search                      BOOLEAN,
    source_last_updated                  TIMESTAMP,
    last_update                           TIMESTAMP NOT NULL DEFAULT NOW()
);
