-- 0065_restore_base_reports_and_page_permissions.sql
-- Repairs a database created from the broken init-db/schema.sql: a stray blank line
-- inside the report_template COPY aborted the init script at that point, so
--   * report_template was left empty (only report 30 came back, via 0031):
--     admins saw no reports, and reports 23/25/29 had no stored SQL to run;
--   * user_page_permissions was never created: /admin/users, /admin/dashboard and
--     every page rendered for a non-admin user failed with a 500.
--
-- Safe on any database: the table is created only when missing, a report row is
-- inserted only when its report_id is absent (existing rows are never changed), and
-- the two UPDATEs at the end only touch SQL that cannot run on this schema.
--
-- Report SQL is the schema.sql version with 0001 (report 23) applied, plus:
--   * report 25: 0045 can never run on this branch (schema.sql already has
--     clinical_rvu/technical_rvu, so its ALTER fails), so its rewrite is repeated here;
--   * report 27: etl_orders.patient_dbid is TEXT, so the bigint join needs a cast.
-- No blank or "--" lines inside the SQL literals: db_migrations._split_sql drops them.

CREATE TABLE IF NOT EXISTS public.user_page_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    page_key VARCHAR(50) NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (user_id, page_key)
);

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES
(22, 'Studies Fact',
 'Counts all studies between the selected date range. If start date is empty, uses Go-Live Date.',
'SELECT
    s.study_db_uid,
    s.procedure_code,
    s.study_date,
    s.original_storing_ae,
    m.modality,
    s.study_status,
    s.patient_db_uid,
    p.fallback_id as patient_id,
    p.sex,
    p.age_group,
    s.last_update,
    s.patient_class,
    TRIM(CONCAT(s.referring_physician_first_name, '' '', s.referring_physician_last_name)) as referring_physician,
    s.signing_physician_first_name,
    s.signing_physician_last_name,
    s.signing_physician_id,
    s.patient_location
FROM etl_didb_studies s
LEFT JOIN aetitle_modality_map m ON s.original_storing_ae = m.aetitle
LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
WHERE 1=1',
 'start_date,end_date', 'table', TRUE),
(23, 'Patient Demographics',
 'Base report for Patients Fact',
'SELECT
    p.patient_db_uid,
    p.birth_date,
    p.sex,
    p.age_group,
    s.age_at_exam,
    p.fallback_id,
    s.study_db_uid,
    s.study_date,
    s.original_storing_ae,
    m.modality,
    s.patient_class,
    s.procedure_code AS proc_id
FROM etl_didb_studies s
LEFT JOIN etl_patient_view p ON s.patient_db_uid::text = p.patient_db_uid::text
LEFT JOIN aetitle_modality_map m ON s.original_storing_ae = m.aetitle
WHERE 1=1',
 'start_date,end_date', 'bar', TRUE),
(25, 'Modality Device Fact',
 'Calculates average turnaround time (TAT) in minutes per signing physician.',
'SELECT
    UPPER(TRIM(s.original_storing_ae)) as aetitle,
    COALESCE(UPPER(m.modality), ''N/A'') as modality,
    s.study_date,
    s.patient_class,
    s.patient_location,
    s.rep_final_signed_by as reading_radiologist,
    s.procedure_code,
    EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date))/60 as total_tat_min,
    COALESCE(pm.duration_minutes, 15) as proc_duration,
    COALESCE(pm.clinical_rvu,  1.0) AS clinical_rvu,
    COALESCE(pm.technical_rvu, 1.0) AS technical_rvu,
    COALESCE(m.daily_capacity_minutes, 480) as base_daily_capacity,
    s.patient_db_uid as patient_id
FROM etl_didb_studies s
LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.original_storing_ae)) = UPPER(TRIM(m.aetitle))
LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
WHERE s.study_date BETWEEN :start AND :end
  AND s.rep_final_timestamp IS NOT NULL
  AND s.rep_final_signed_by IS NOT NULL',
 'start_date,end_date', 'bar', TRUE),
(27, 'Order base Reports',
 'Patient-level demographics and utilization base report',
'SELECT
    o.order_dbid,
    o.order_status,
    o.proc_id,
    o.proc_text,
    o.scheduled_datetime,
    o.has_study,
    s.study_date,
    s.original_storing_ae,
    s.procedure_code,
    p.birth_date,
    p.sex,
    COALESCE(m.duration_minutes, 0) as duration_minutes
FROM etl_orders o
LEFT JOIN etl_didb_studies s
    ON s.study_db_uid = o.study_db_uid
LEFT JOIN etl_patient_view p
    ON p.patient_db_uid::text = o.patient_dbid
LEFT JOIN procedure_duration_map m
    ON m.procedure_code = s.procedure_code OR m.procedure_code = o.proc_id',
 'start_date,end_date', 'table', TRUE),
(29, 'Storage Calculation',
 'Calculate the Storage usage for different Modalities, Patients, Procedures in a specific period of time.',
'SELECT
    study_date,
    COALESCE(modality, ''N/A'')            AS modality,
    COALESCE(procedure_code, ''UNKNOWN'')   AS procedure_code,
    COALESCE(original_storing_ae, ''Unknown'')       AS original_storing_ae,
    SUM(total_gb)                           AS total_gb,
    SUM(study_count)                        AS study_count
FROM summary_storage_daily
WHERE study_date BETWEEN :start AND :end
GROUP BY study_date, modality, procedure_code, original_storing_ae
ORDER BY total_gb DESC',
 'start_date,end_date', 'table', TRUE)
ON CONFLICT (report_id) DO NOTHING;

SELECT setval('public.report_template_report_id_seq',
              GREATEST((SELECT MAX(report_id) FROM public.report_template),
                       (SELECT last_value FROM public.report_template_report_id_seq)));

UPDATE report_template
SET report_sql_query = replace(
    report_sql_query,
    'COALESCE(pm.rvu_value, 1.0) as rvu,',
    E'COALESCE(pm.clinical_rvu,  1.0) AS clinical_rvu,\n    COALESCE(pm.technical_rvu, 1.0) AS technical_rvu,'
)
WHERE report_id = 25
  AND strpos(report_sql_query, 'pm.rvu_value') > 0;

UPDATE report_template
SET report_sql_query = replace(
    report_sql_query,
    'ON p.patient_db_uid = o.patient_dbid',
    'ON p.patient_db_uid::text = o.patient_dbid'
)
WHERE report_id = 27
  AND strpos(report_sql_query, 'ON p.patient_db_uid = o.patient_dbid') > 0;
