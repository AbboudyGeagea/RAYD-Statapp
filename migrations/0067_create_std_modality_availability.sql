-- Migration 0067: create std_modality_exceptions + std_schedule_template_items (RIS
-- device availability, LAUMC).
--
-- RIS-authoritative counterparts to RAYD's existing manually-editable tables:
--   MODALITY_AVAIL_EXCEPTION  ~ device_exceptions (per-day capacity overrides)
--   SCHEDULE_TEMPLATE_ITEM    ~ device_weekly_schedule (standard weekly hours)
-- Built as NEW, SEPARATE tables — not merged into or overwriting the existing
-- manually-editable ones, so nothing an admin has already entered there is disturbed.
--
-- *** OPERATOR INSTRUCTION: not editable from RAYD *** — these are pure RIS mirrors.
-- Enforced simply by never building an admin-edit route for them, not by a DB-level
-- lock (matches the read-only-in-practice treatment already given to every other
-- RIS-sourced catalog table this session).
--
-- Feeds device utilization: std_pps.START_DATETIME/END_DATETIME + MODALITY_KEY
-- (Phase 14) already gives actual usage time per device (the numerator); these two
-- tables give available time (the denominator).
--
-- std_modality_exceptions: MODALITY_KEY resolved to aetitle via a live MODALITY join
-- at extract time (matches aetitle_modality_map.aetitle) — same pattern as std_pps's
-- performing_ae_title. Ready to use as-is.
--
-- std_schedule_template_items: captured RAW/faithful, but NOT yet attributable to a
-- specific device — SCHEDULE_SCHEME_KEY is the only link back to... something, and the
-- SCHEDULE_SCHEME table hasn't been provided. Until then this table can't answer "what
-- are THIS device's weekly hours" on its own. SCHEDULE_TEMPLATE_VERSION_KEY suggests
-- schedules are versioned over time — which version is currently effective is also
-- unconfirmed. Both are open questions, not guessed at.
--
-- availability_indicator_key (both tables) pulled RAW — no lookup table provided yet
-- for what an indicator value actually means (Available/Unavailable/Partial/On-call?).

CREATE TABLE IF NOT EXISTS std_modality_exceptions (
    modality_avail_exception_key   BIGINT PRIMARY KEY,
    from_date                       TIMESTAMP,
    to_date                          TIMESTAMP,
    reason                            TEXT,
    exception_created_person_key      BIGINT,   -- resolves via std_resources_ris.person_key
    exception_created_date             TIMESTAMP,
    modality_key                        BIGINT,  -- raw source key
    aetitle                               TEXT,   -- resolved via MODALITY join
    availability_indicator_key            BIGINT, -- unresolved, no lookup provided yet
    priority                               TEXT,
    source_last_updated                     TIMESTAMP,
    last_update                              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_modality_exceptions_aetitle ON std_modality_exceptions (aetitle);
CREATE INDEX IF NOT EXISTS idx_std_modality_exceptions_dates   ON std_modality_exceptions (from_date, to_date);

CREATE TABLE IF NOT EXISTS std_schedule_template_items (
    schedule_template_item_key      BIGINT PRIMARY KEY,
    day_of_week                      INTEGER,  -- convention (0=Mon vs 1=Sun etc.) unconfirmed
    from_time                         TEXT,     -- stored raw; exact source type unconfirmed
    to_time                            TEXT,
    availability_indicator_key          BIGINT,  -- unresolved, shared lookup with the table above
    schedule_scheme_key                  BIGINT,  -- device link — SCHEDULE_SCHEME table not yet provided
    schedule_template_version_key         BIGINT,  -- which version is "current" — unconfirmed
    source_last_updated                    TIMESTAMP,
    last_update                             TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_std_sched_tmpl_items_scheme ON std_schedule_template_items (schedule_scheme_key);
