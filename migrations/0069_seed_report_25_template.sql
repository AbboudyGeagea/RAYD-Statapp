-- Migration 0069: seed report_template row for report_id=25 (Modality Device Fact).
--
-- report_25's SQL was never actually seeded by any migration or init-db/schema.sql —
-- only report_id=30 has a proper INSERT (migration 0031). The row report_25.py depends
-- on (routes/report_25.py: get_gold_standard_data -> "SELECT report_sql_query FROM
-- report_template WHERE report_id = 25") only ever existed because it was inserted
-- manually at some point in the past and carried forward in the live DB / backup dumps.
-- On a genuine fresh install or full volume wipe (docker compose down -v + rebuild —
-- exactly what happened on LAUMC, 2026-07-26), that row simply does not exist:
-- template_res is None, get_gold_standard_data returns immediately, and report 25
-- shows no data at all — not a filtering bug, the query never ran.
--
-- Content restored from init-db/rayd_backup.sql's report_template dump, already
-- upgraded to migration 0045's clinical_rvu/technical_rvu split (rather than seeding
-- the pre-0045 single `rvu` column and relying on 0045's string-replace UPDATE, which
-- has already run by migration-number order and would never see this new row).

INSERT INTO report_template (report_id, report_name, long_description, report_sql_query, required_parameters, visualization_type, is_base)
VALUES (
    25,
    'Modality Device Fact',
    'Calculates average turnaround time (TAT) in minutes per signing physician.',
    '
    SELECT
        UPPER(TRIM(s.storing_ae)) as aetitle,
        COALESCE(UPPER(m.modality), ''N/A'') as modality,
        s.study_date,
        s.patient_class,
        s.patient_location,
        s.rep_final_signed_by as reading_radiologist,
        s.procedure_code,
        -- TAT Calculation
        EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date))/60 as total_tat_min,
        -- Work Duration from Procedure Map (Default to 15 if missing)
        COALESCE(pm.duration_minutes, 15) as proc_duration,
        -- RVU values from Procedure Map (Default to 1.0 if missing)
        COALESCE(pm.clinical_rvu,  1.0) AS clinical_rvu,
        COALESCE(pm.technical_rvu, 1.0) AS technical_rvu,
        -- Base Daily Capacity from Modality Map (Default to 480 if missing)
        COALESCE(m.daily_capacity_minutes, 480) as base_daily_capacity,
        s.patient_db_uid as patient_id
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
    LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
    WHERE s.study_date BETWEEN :start AND :end
      AND s.rep_final_timestamp IS NOT NULL
      AND s.rep_final_signed_by IS NOT NULL
    ',
    'start_date,end_date',
    'bar',
    TRUE
)
ON CONFLICT (report_id) DO NOTHING;
-- DO NOTHING, not DO UPDATE: unlike report_30 (whose Python route owns the query),
-- report_25's SQL is sometimes hand-tuned live in report_template by an admin. A
-- re-run of this migration must never clobber a working, already-customized query —
-- it only fills the gap when the row is missing entirely.
