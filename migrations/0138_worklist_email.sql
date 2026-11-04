-- Migration 0138: scheduled email of saved Report Composer reports.
--
-- An admin picks saved worklists, recipients and a daily / weekly / monthly time; the
-- app mails them as Excel attachments through the SMTP relay held in settings
-- (utils/worklist_email.py). Nothing goes out until an admin has set the relay.
--
-- Every time here is naive hospital time written from Python, like the worklists' own
-- date criteria: LAUMC's Postgres runs on UTC, so NOW() would be hours off.
--
-- A worklist in a schedule cannot be deleted (RESTRICT): recipients would silently stop
-- getting it. A run outlives its schedule: it is the record of what was sent to whom.


CREATE TABLE IF NOT EXISTS worklist_email_schedules (
    id           SERIAL    PRIMARY KEY,
    name         TEXT      NOT NULL,
    recipients   TEXT[]    NOT NULL,
    frequency    TEXT      NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly')),
    send_time    TIME      NOT NULL,
    weekday      SMALLINT  CHECK (weekday BETWEEN 0 AND 6),      -- 0 = Monday
    month_day    SMALLINT  CHECK (month_day BETWEEN 1 AND 31),   -- a shorter month sends on its last day
    enabled      BOOLEAN   NOT NULL DEFAULT TRUE,
    next_run_at  TIMESTAMP NOT NULL,
    created_by   INTEGER   REFERENCES users(id) ON DELETE SET NULL,
    updated_by   INTEGER   REFERENCES users(id) ON DELETE SET NULL,
    CHECK (frequency <> 'weekly'  OR weekday IS NOT NULL),
    CHECK (frequency <> 'monthly' OR month_day IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS worklist_email_schedule_items (
    schedule_id  INTEGER   NOT NULL REFERENCES worklist_email_schedules(id) ON DELETE CASCADE,
    worklist_id  INTEGER   NOT NULL REFERENCES worklists(id) ON DELETE RESTRICT,
    PRIMARY KEY (schedule_id, worklist_id)
);

CREATE INDEX IF NOT EXISTS idx_worklist_email_items_worklist
    ON worklist_email_schedule_items (worklist_id);

-- status: running → sent (all of it went) | partial (the mail went, something is missing
-- from it) | failed (no mail). detail holds one entry per worklist; no row of data.
CREATE TABLE IF NOT EXISTS worklist_email_runs (
    id            SERIAL    PRIMARY KEY,
    schedule_id   INTEGER   REFERENCES worklist_email_schedules(id) ON DELETE SET NULL,
    schedule_name TEXT      NOT NULL,
    started_by    INTEGER   REFERENCES users(id) ON DELETE SET NULL,   -- NULL = the schedule itself
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,
    status        TEXT      NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'sent', 'partial', 'failed')),
    recipients    TEXT[]    NOT NULL,
    detail        JSONB     NOT NULL DEFAULT '[]',
    error         TEXT
);

CREATE INDEX IF NOT EXISTS idx_worklist_email_runs_schedule
    ON worklist_email_runs (schedule_id, started_at DESC);


-- The relay. smtp_password is encrypted with utils/crypto. smtp_allowed_domains is the
-- comma-separated list of domains these attachments may go to: empty means none.
INSERT INTO settings (key, value) VALUES ('smtp_host', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_port', '587') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_security', 'starttls') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_username', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_password', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_from', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_allowed_domains', '') ON CONFLICT (key) DO NOTHING;
