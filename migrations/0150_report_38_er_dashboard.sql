-- Migration 0150: the ER dashboard becomes Report 38, a report like every other.
-- *** LAUMC BRANCH ONLY ***
--
-- To-do item 25 (2026-11-14, ER only; the ORU tab stays as it is): the standalone page /er,
-- behind the licence feature 'er_dashboard', follows CLAUDE.md rule 8 the way Report 37 did
-- in migration 0147. routes/er_dashboard.py became routes/report_38.py, which registers
-- itself; this adds what the code cannot: the catalogue row (without it the report is
-- invisible on the Viewer Dashboard and cannot be granted), the licence entry (blueprints of
-- unlicensed reports are not registered), and access for the people who had the page.
--
-- Licence: 38 is added to 'reports' unless the licence switched 'er_dashboard' off.
-- Access carries over the page's own rule: every role but tec could open /er (it had no page
-- permission). Admins see every report anyway. An existing row is left as it is.

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query,
                             required_parameters, visualization_type, is_base)
VALUES (
    38,
    'ER Analytics Dashboard',
    'Turnaround time, reporting target compliance, unread studies of the day and workload for emergency patients (patient location ER), by modality, hour and radiologist. Opens on the last 30 days; the target is set in hours on the left panel.',
    NULL,   -- routes/report_38.py runs its own queries
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
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '38'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '38'::jsonb)
  AND COALESCE(value::jsonb->>'er_dashboard', '') <> 'false';

INSERT INTO report_access_control (user_id, report_template_id, is_enabled)
SELECT u.id, 38, u.role <> 'tec'
FROM users u
ON CONFLICT (user_id, report_template_id) DO NOTHING;
