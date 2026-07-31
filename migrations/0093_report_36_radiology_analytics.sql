-- Migration 0093: register Report 36 (Radiology Reporting Analytics) in the report
-- catalogue, and add report_id 36 to the settings.license 'reports' entitlement array.
--
-- Report 36 is the former "Radiologists" tab of Report 25 (tab-clinical), converted to
-- a standalone report per operator instruction 2026-07-31: KPI Detailed Reading,
-- Resident vs Radiologist TAT, Patient Wait Time (Scheduled -> Arrived), Technician
-- Efficiency (Arrived -> Exam Done), Radiologist Workload Matrix, Reporting Cadence
-- Analysis, Technician TAT by AE Station. No single base SQL applies to every chart
-- in it (confirmed by reading each chart's data source) -- most run their own bespoke
-- RIS/PPS-anchored queries; Workload Matrix, Technician TAT by AE Station, and the
-- insights panel reuse report_25's own report_template-driven get_gold_standard_data()
-- result (a Python-level function-call dependency, not a stored SQL row) -- so
-- report_sql_query stays NULL here, same as report_27/report_30.
--
-- Without the report_template row, the report is invisible in the Viewer Dashboard
-- (viewer_controller.seed_report_access() grants default access via is_base=TRUE) --
-- see migration 0069's cautionary note about report_25's own row having been missing
-- for years on fresh installs.

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES (
    36,
    'Radiology Reporting Analytics',
    'Radiologist and resident performance: KPI detailed reading, TAT per radiologist, patient wait time (scheduled to arrived), technician efficiency (arrived to exam done vs. expected duration), workload matrix, reporting cadence, and technician TAT by AE station.',
    NULL,   -- report_36.py runs its own queries / reuses report_25.get_gold_standard_data(); no shared SQL stored here
    'start_date,end_date',
    'bar',
    TRUE
)
ON CONFLICT (report_id) DO UPDATE
    SET report_name      = EXCLUDED.report_name,
        long_description = EXCLUDED.long_description,
        is_base          = TRUE;

-- Add 36 to settings.license's 'reports' array if a license row already exists and
-- doesn't have it yet -- a fresh install with no license row uses DEFAULT_LICENSE
-- (routes/registry.py), which is computed from whatever's self-registered in code, so
-- no action is needed there. Guarded/idempotent: no-op if already present or if no
-- license row/no 'reports' key exists.
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
