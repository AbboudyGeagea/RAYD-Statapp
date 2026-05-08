-- Register Report 30 (Patient CD/DVD Distribution) in the report catalogue.
-- Without this row the report is invisible in the Viewer Dashboard.

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES (
    30,
    'Patient Media Distribution',
    'CD and DVD burn history from CD surf. Shows burn volume by month, media type (CD vs DVD), and modality. Useful for understanding physical media demand and identifying which study types are burned most frequently.',
    NULL,   -- report_30.py runs its own queries; no SQL stored here
    'start_date,end_date',
    'bar',
    TRUE
)
ON CONFLICT (report_id) DO UPDATE
    SET report_name      = EXCLUDED.report_name,
        long_description = EXCLUDED.long_description,
        is_base          = TRUE;
