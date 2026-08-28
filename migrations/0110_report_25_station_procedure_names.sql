-- Migration 0110: report_25's stored template (report_template.report_id = 25) adds two
-- resolved-display columns alongside its existing raw aetitle/procedure_code columns --
-- aetitle_display (RIS station/room name, falling back to the raw AE title) and
-- procedure_display (RIS procedure description, falling back to the raw procedure code).
--
-- Both joins (aetitle_modality_map m, procedure_duration_map pm) already exist in the
-- stored query (used today for modality/daily_capacity_minutes and duration/RVU) -- this
-- only adds two more SELECTed columns from tables already in the FROM clause.
--
-- Guarded exactly like migration 0091 (idempotent, no-op if already applied or if an
-- admin has since hand-tuned report_template for report_id=25): only fires on the exact
-- SQL text 0091 left behind (contains 'insert_time', the marker of the last change) and
-- not already containing this migration's own marker.

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
        COALESCE(NULLIF(TRIM(m.station_name),''), UPPER(TRIM(s.storing_ae))) as aetitle_display,
        COALESCE(NULLIF(TRIM(pm.procedure_name),''), s.procedure_code) as procedure_display,
        -- TAT Calculation: prefer REP_STUDY_LAST_COMPOSED_TS (what actually has data on
        -- this install), then PACS''s own rep_final_timestamp, then the RIS-sourced
        -- report (hl7_oru_reports) when neither PACS field was ever synced. Anchored on
        -- insert_time (real time-of-day), not study_date (bare date == midnight) --
        -- see migration 0091.
        EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) - COALESCE(s.insert_time, s.study_date)))/60 as total_tat_min,
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
  AND report_sql_query LIKE '%insert_time%'
  AND report_sql_query NOT LIKE '%aetitle_display%';
