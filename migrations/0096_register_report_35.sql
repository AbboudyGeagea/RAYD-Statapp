-- Migration 0096: register Report 35 (Technician TAT & Compliance Monitoring) in the
-- report catalogue, and add report_id 35 to settings.license's 'reports' entitlement
-- array.
--
-- Root cause found 2026-07-31 while investigating "report 35 has no data, and i have
-- no idea where to access this one": report_35.py/report_35.html were already fully
-- built in an earlier session (a standalone extraction of report_25's old Technicians
-- tab, see routes/report_35.py's own docstring) and self-register their blueprint
-- correctly (routes/registry.py imports routes.report_35) -- but the report_template
-- catalogue row this migration adds was never created, and without it the report is
-- invisible in the Viewer Dashboard (viewer_controller.seed_report_access() grants
-- default access via is_base=TRUE) -- same failure mode migration 0093 documented for
-- Report 36, and the same fix pattern.
--
-- Separately, report_35's own "no data" symptom is NOT a bug -- its docstring confirms
-- it runs the exact same hl7_orders.scheduled_datetime query ported verbatim from
-- report_25's old tab, which is empty because the R2I HL7 ORM feed isn't flowing yet
-- (same known gap as report_25's now-removed Technicians tab). Registering the report
-- makes it reachable; it will start showing data once that feed goes live.

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES (
    35,
    'Technician TAT & Compliance Monitoring',
    'Per-technician and per-modality exam-completion TAT against HL7 order schedule: flagged exams (before-scheduled, too-early, overlap, too-late), never-marked-done overdue exams, and daily TAT trend. Standalone extraction of report_25''s former Technicians tab.',
    NULL,   -- report_35.py runs its own queries against hl7_orders directly; no shared SQL stored here
    'start_date,end_date',
    'bar',
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
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '35'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '35'::jsonb);
