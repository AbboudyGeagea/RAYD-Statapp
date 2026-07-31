-- Migration 0091: report_25's stored template (report_template.report_id = 25) anchors
-- total_tat_min on s.study_date, a bare DATE (implicitly midnight) -- inflates every TAT
-- by however many hours had already passed since midnight when the exam actually
-- happened, not a fixed offset. Found 2026-07-31 comparing against PPS-based numbers.
--
-- Fix: anchor on COALESCE(s.insert_time, s.study_date) instead. insert_time is Oracle
-- DIDB_STUDIES.INSERT_TIME (ETL_JOBS/etl_didb_studies.py) -- PACS's own ingestion
-- timestamp, a real time-of-day, not the ETL's last_update sync time. Validated
-- 2026-07-31 against production (see project memory / this session): 100% populated,
-- 0 rows at midnight, ~0 rows land after signing, consistently lower/more-plausible TAT
-- than study_date across all 3 site buckets, both mean and median. COALESCE fallback
-- keeps today's behavior for any historical row where insert_time is null.
--
-- Only the TAT calc line changes -- s.study_date is left in the SELECT list as-is
-- (still used elsewhere, e.g. report_25.py's df['study_date_dt']).
--
-- Guarded to only touch the row if it still matches exactly what migration 0075 left
-- (a no-op if an admin has since hand-tuned report_template for report_id=25, or if this
-- fix was already applied).

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
  AND report_sql_query LIKE '%hl7_oru_reports%'
  AND report_sql_query NOT LIKE '%insert_time%';
