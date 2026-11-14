-- Migration 0149: one start date for every report's date filter
-- *** LAUMC BRANCH ONLY ***
--
-- Meeting of 2026-11-13, item 11: a global "report start date", 2026-01-01, editable on
-- the ETL Control Room, used as the starting value of every report's date filter (the
-- Super Report and Report Composer included). Users can still pick earlier dates; it only
-- spares them changing the start date each time. Until now every report started on the
-- ETL go-live date (2019-01-01 on LAUMC). The ER dashboard (last 30 days) and Report 37
-- (previous month) keep their own defaults. Read by db.get_report_start_date().
--
-- A value already set on the ETL Control Room is kept.

INSERT INTO settings (key, value)
VALUES ('report_start_date', '2026-01-01')
ON CONFLICT (key) DO NOTHING;
