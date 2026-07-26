-- Migration 0070: report_25's "is reported" detection was checking the wrong source.
--
-- Confirmed live on LAUMC, 2026-07-26: radiologists sign reports in the RIS, and RIS
-- correctly delivers them into hl7_oru_reports (report_source='ris', via Phase 9 / the
-- live HL7 listener) -- but PACS's own etl_didb_studies.REP_FINAL_TIMESTAMP /
-- REP_FINAL_SIGNED_BY are NOT reliably synced back for recent studies. Spot-checked
-- directly against Oracle (medistore.didb_studies): a study scanned 2026-07-16 04:35
-- had a complete, signed report in hl7_oru_reports dated 2026-07-16 08:56 (same-day
-- turnaround, completely normal) -- yet PACS's REP_FINAL_TIMESTAMP was still NULL over
-- a week later. report_25's query required s.rep_final_timestamp/rep_final_signed_by
-- IS NOT NULL, so it was silently excluding effectively all current studies -- not a
-- reporting backlog, not an ETL bug pulling data wrong, just checking a field PACS
-- itself doesn't keep current.
--
-- Fix: LEFT JOIN hl7_oru_reports by accession_number, COALESCE the PACS field with the
-- RIS-sourced one for both the completion filter and the TAT calculation, preferring
-- PACS's own value where it does exist (older bulk-loaded data had some -- 172 rows --
-- so don't discard it, just fall back when PACS is null).
--
-- Known limitation, not fixed here: hl7_oru_reports.physician_id is very likely a raw
-- HL7 provider code/ID, not a display name like PACS's rep_final_signed_by -- radiologist
-- attribution for RIS-only-sourced reports may show a code instead of a name until this
-- is resolved through std_resources_ris (same composite-ID join pattern Phase 13 already
-- uses for etl_didb_studies.reading/signing_physician_id). Tracked in
-- docs/LAUMC_NEXT_SESSION.md, not blocking this fix -- the "no data at all" problem is
-- far worse than "some names show as codes."
--
-- Guarded to only touch the row if it still matches exactly what migration 0069 seeded
-- (the same content this session inserted minutes earlier) -- a no-op if an admin has
-- since hand-tuned report_template for report_id=25.

UPDATE report_template
SET report_sql_query = '
    SELECT
        UPPER(TRIM(s.storing_ae)) as aetitle,
        COALESCE(UPPER(m.modality), ''N/A'') as modality,
        s.study_date,
        s.patient_class,
        s.patient_location,
        COALESCE(s.rep_final_signed_by, o.physician_id) as reading_radiologist,
        s.procedure_code,
        -- TAT Calculation: prefer PACS''s own completion timestamp; fall back to the
        -- RIS-sourced report (hl7_oru_reports) when PACS was never synced with it.
        EXTRACT(EPOCH FROM (COALESCE(s.rep_final_timestamp, o.result_datetime) - s.study_date))/60 as total_tat_min,
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
      AND COALESCE(s.rep_final_timestamp, o.result_datetime) IS NOT NULL
      AND COALESCE(s.rep_final_signed_by, o.physician_id) IS NOT NULL
    '
WHERE report_id = 25
  AND report_sql_query LIKE '%AND s.rep_final_signed_by IS NOT NULL%'
  AND report_sql_query NOT LIKE '%hl7_oru_reports%';
