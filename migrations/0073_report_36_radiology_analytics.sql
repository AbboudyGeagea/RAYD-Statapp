-- Migration 0073: register Report 36 (Radiology Reporting Analytics).
--
-- Report 36 is Report 25's former "Radiologists" tab as its own report: statistical
-- insights, TAT heatmap, peer ranking, reports per radiologist (modality / AE / month /
-- procedure) and reporting cadence. Reports are counted by finalization date.
-- PACS data only. Same split as LAUMC/HL7 (their 0093), without their RIS panels.
--
-- report_sql_query stays NULL: routes/report_36.py runs its own queries.
-- is_base = TRUE makes it appear on the Viewer Dashboard, and non-admin users get
-- access automatically (viewer_controller.seed_report_access on dashboard load).

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES (
    36,
    'Radiology Reporting Analytics',
    'Radiologist performance by finalization date: TAT heatmap and peer ranking, reports per radiologist by modality, AE title, month and procedure, reporting cadence, and statistical insights.',
    NULL,
    'start_date,end_date',
    'bar',
    TRUE
)
ON CONFLICT (report_id) DO UPDATE
    SET report_name      = EXCLUDED.report_name,
        long_description = EXCLUDED.long_description,
        is_base          = TRUE;

-- Keep the id sequence ahead of the explicit id inserted above.
SELECT setval('public.report_template_report_id_seq',
              GREATEST((SELECT MAX(report_id) FROM public.report_template),
                       (SELECT last_value FROM public.report_template_report_id_seq)));

-- A stored license limits which report blueprints are registered at startup, so add
-- 36 to its 'reports' array. No-op when there is no license row, no 'reports' key,
-- or 36 is already listed. Without a license row the default license is built from
-- the registered report modules, which already include 36.
UPDATE settings
SET value = jsonb_set(
    value::jsonb,
    '{reports}',
    (
        SELECT jsonb_agg(DISTINCT elem::int ORDER BY elem::int)
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '36'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '36'::jsonb);
