-- Migration 0147: the Sticky Notes page becomes Report 37, a report like every other.
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 2026-11-09: the JCI urgent emergent report must follow the other reports (the
-- shared left filter panel, a Generate button, a row in report_template) instead of being a
-- standalone page with its own permission: "i really don't want the application to be like a
-- workaround for every feature". routes/sticky_notes.py became routes/report_37.py, which
-- registers itself; this adds what the code cannot: the catalogue row (without it the report
-- is invisible on the Viewer Dashboard and cannot be granted, as 0093 and 0096 found for
-- Reports 36 and 35), the licence entry (blueprints of unlicensed reports are not
-- registered), and access for the people who had the page.
--
-- Access carries over the page permission it replaces ('sticky_notes' in
-- user_page_permissions): an explicit grant or block stays as it was; otherwise the role
-- decides as that permission did (viewer and viewer2 had it by default; tec, finance and
-- secretary did not). Admins see every report anyway. The old permission rows are left in
-- place, unread, so going back to the previous code finds them as they were.

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query,
                             required_parameters, visualization_type, is_base)
VALUES (
    37,
    'Urgent Emergent: First Open & Sticky Note',
    'JCI measure for urgent emergent exams (ER and the departments an admin ticks): when each study was first opened, when the sticky note was inserted, by whom, against an optional limit; with the automated monthly workbook. Reads the nightly copy of PACS, one day late. Built in utils/sticky_notes.py.',
    NULL,   -- routes/report_37.py and utils/sticky_notes.py run their own queries
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
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '37'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '37'::jsonb);

INSERT INTO report_access_control (user_id, report_template_id, is_enabled)
SELECT u.id, 37, COALESCE(p.is_enabled, u.role IN ('admin', 'viewer', 'viewer2'))
FROM users u
LEFT JOIN user_page_permissions p ON p.user_id = u.id AND p.page_key = 'sticky_notes'
ON CONFLICT (user_id, report_template_id) DO NOTHING;
