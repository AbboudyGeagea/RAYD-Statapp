-- Migration 0167: Report 40, Patient Schedule Lead Time (LAUMC to-do item 21).
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 2026-11-17: add it. How far ahead exams are booked: from when the order was created
-- on the RIS (ORDERS.CREATED_ON_DATE, etl_orders.order_created_at, migration 0160) to the
-- appointment, for the exams whose appointment falls in the period, with the breakdowns the RIS
-- gives (routes/report_40.py, utils/schedule_lead_time.py).
--
-- A report like every other (CLAUDE.md rule 8): the catalogue row and the licence entry; access
-- per user and site (rule 9, nothing granted here: Super user, CHT and Administrator open every
-- report).

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query,
                             required_parameters, visualization_type, is_base)
VALUES (
    40,
    'Patient Schedule Lead Time',
    'How far ahead exams are booked: from when the order was created on the RIS to the appointment, for the appointments in the period. Median and 90th percentile lead time, same-day and week-ahead shares, cancelled and rescheduled exams, by modality, procedure, room, patient class, ordering department, referring physician, priority, age band, sex and when the booking was made. Excel download of every exam.',
    NULL,   -- routes/report_40.py and utils/schedule_lead_time.py run their own queries
    'start_date,end_date',
    'table',
    TRUE
)
ON CONFLICT (report_id) DO UPDATE
    SET report_name      = EXCLUDED.report_name,
        long_description = EXCLUDED.long_description,
        is_base          = TRUE;

UPDATE settings
SET value = jsonb_set(
    value::jsonb,
    '{reports}',
    (
        SELECT jsonb_agg(DISTINCT elem::int ORDER BY elem::int)
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '40'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '40'::jsonb);
