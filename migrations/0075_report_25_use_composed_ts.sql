-- Migration 0075: report_25 prefers REP_STUDY_LAST_COMPOSED_BY/_TS over
-- rep_final_signed_by/rep_final_timestamp — the fields that actually have data on
-- this (older/sparser) PACS install. See migration 0074 for the source column add.
--
-- Priority order, most to least reliable on THIS install:
--   1. rep_study_last_composed_ts / _by   -- confirmed populated (operator sample data)
--   2. rep_final_timestamp / rep_final_signed_by -- PACS-native, unreliable here, kept
--      as a fallback in case some rows do have it
--   3. hl7_oru_reports.result_datetime / physician_id -- RIS-sourced (migration 0070)
--
-- reading_radiologist will now often show composed_by's raw login-style value (e.g.
-- "abdallah.noufaily@ad") rather than a name -- known, accepted for now; resolving to
-- a real name and radiologist-vs-resident role is explicitly deferred by the operator.
--
-- Guarded to only touch the row if it still matches exactly what migration 0070 left
-- (a no-op if an admin has since hand-tuned report_template for report_id=25).

UPDATE report_template
SET report_sql_query = '
    SELECT
        UPPER(TRIM(s.storing_ae)) as aetitle,
        COALESCE(UPPER(m.modality), ''N/A'') as modality,
        s.study_date,
        s.patient_class,
        s.patient_location,
        COALESCE(s.rep_study_last_composed_by, s.rep_final_signed_by, o.physician_id) as reading_radiologist,
        s.procedure_code,
        -- TAT Calculation: prefer REP_STUDY_LAST_COMPOSED_TS (what actually has data on
        -- this install), then PACS''s own rep_final_timestamp, then the RIS-sourced
        -- report (hl7_oru_reports) when neither PACS field was ever synced.
        EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) - s.study_date))/60 as total_tat_min,
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
    LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
    WHERE s.study_date BETWEEN :start AND :end
      AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NOT NULL
      AND COALESCE(s.rep_study_last_composed_by, s.rep_final_signed_by, o.physician_id) IS NOT NULL
    '
WHERE report_id = 25
  AND report_sql_query LIKE '%hl7_oru_reports%'
  AND report_sql_query NOT LIKE '%rep_study_last_composed%';
