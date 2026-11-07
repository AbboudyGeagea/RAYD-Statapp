-- Migration 0146: the monthly JCI urgent emergent report (utils/sticky_notes_monthly.py).
--
-- The customer asked for an automated report of the preceding month. Each month is kept as
-- the workbook it was generated as, because JCI evidence must not change when a department
-- is ticked later or a study is reopened. An admin can generate a month again on purpose.
--
-- The workbook holds patient IDs and RH studies only. Nothing here holds SJH rows, so this
-- table needs no view in rh_refresh_views().

CREATE TABLE IF NOT EXISTS public.sticky_notes_monthly_reports (
    report_month        DATE PRIMARY KEY,       -- first day of the month covered
    generated_at        TIMESTAMP NOT NULL,
    case_count          INTEGER NOT NULL,
    noted_count         INTEGER NOT NULL,
    open_seen_count     INTEGER NOT NULL,
    workbook            BYTEA NOT NULL,
    email_attempted_at  TIMESTAMP,
    emailed_at          TIMESTAMP,
    emailed_to          TEXT[],
    email_error         TEXT
);
