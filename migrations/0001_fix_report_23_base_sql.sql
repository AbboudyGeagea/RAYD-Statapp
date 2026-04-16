-- Fix report 23 total count inflated by etl_orders join
-- Old query drove from etl_patient_view and joined etl_orders, producing
-- one row per order per study instead of one row per study.
UPDATE report_template
SET report_sql_query =
'SELECT
    p.patient_db_uid,
    p.birth_date,
    p.sex,
    p.age_group,
    s.age_at_exam,
    p.fallback_id,
    s.study_db_uid,
    s.study_date,
    s.storing_ae,
    m.modality,
    s.patient_class,
    s.procedure_code AS proc_id
FROM etl_didb_studies s
LEFT JOIN etl_patient_view p ON s.patient_db_uid::text = p.patient_db_uid::text
LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
WHERE 1=1'
WHERE report_id = 23;
