-- Migration 0184: the Super Report sent by email on a schedule (to-do item 43, 2026-11-26).
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 25 Nov: "include the feature of emailing the SU like the custom reports" (Report
-- Composer's scheduled emails, migration 0138). A schedule keeps the page's filters and blocks
-- when it was saved, and a period worked out on the day it runs (yesterday, the last 7 days,
-- last week Monday to Sunday, last month); the mail carries the written analysis (item 42) and
-- the key figures in its body, and an .xlsx with one sheet per block. utils/super_report_email.py
-- sends it through the one mail server (settings smtp_*), only to smtp_allowed_domains.
--
-- In every database, like Report Composer's: an SJH schedule lives in SJH's database and sends
-- SJH's figures through SJH's mail server. No foreign key to users (site databases mirror them).

CREATE TABLE IF NOT EXISTS super_report_email_schedules (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(120) NOT NULL,
    filters      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    blocks       JSONB        NOT NULL,
    period       VARCHAR(20)  NOT NULL CHECK (period IN ('yesterday', 'last_7_days', 'last_week', 'last_month')),
    recipients   TEXT[]       NOT NULL,
    frequency    VARCHAR(10)  NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly')),
    send_time    TIME         NOT NULL,
    weekday      SMALLINT     CHECK (weekday BETWEEN 0 AND 6),
    month_day    SMALLINT     CHECK (month_day BETWEEN 1 AND 31),
    enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
    next_run_at  TIMESTAMP,
    created_by   INTEGER,
    updated_by   INTEGER,
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    CHECK (frequency <> 'weekly'  OR weekday IS NOT NULL),
    CHECK (frequency <> 'monthly' OR month_day IS NOT NULL),
    CHECK (jsonb_typeof(blocks) = 'array')
);

CREATE TABLE IF NOT EXISTS super_report_email_runs (
    id            SERIAL PRIMARY KEY,
    schedule_id   INTEGER REFERENCES super_report_email_schedules (id) ON DELETE SET NULL,
    schedule_name VARCHAR(120),
    started_by    INTEGER,
    started_at    TIMESTAMP   NOT NULL,
    finished_at   TIMESTAMP,
    status        VARCHAR(10) NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'sent', 'partial', 'failed')),
    recipients    TEXT[]      NOT NULL,
    period_from   DATE,
    period_to     DATE,
    error         TEXT
);

CREATE INDEX IF NOT EXISTS idx_sr_email_runs_schedule ON super_report_email_runs (schedule_id, started_at DESC);
