-- Migration 0064: Dynamic Worklists, Bekaa's PACS data layer.
--
-- Replaces the Custom Reports composer. The editor (routes/worklists.py) comes from
-- the HL7 branch (19d63c56 / 9401bdda / 5bdaee02); its HL7 views read the RIS order
-- lifecycle (ray7_study_state, hl7_study_events ...), which Bekaa does not have. These
-- views read what Bekaa does have:
--
--   rpt.studies    one row per PACS study            etl_didb_studies (+ patient, AE map,
--                                                    PACS order, ORU report, NLP, CRN)
--   rpt.series     one row per PACS series           etl_didb_serieses
--   rpt.orders     one row per PACS order            etl_orders
--   rpt.reports    one row per ORU report            hl7_oru_reports + hl7_oru_analysis
--   rpt.findings   one row per NLP finding           hl7_oru_analysis.affirmed_labels
--   rpt.storage    one row per day / AE / procedure  summary_storage_daily
--   rpt.crn        one row per CRN notification      crn_notifications (+ events)
--
--   worklists, worklist_versions                     saved definitions, every save kept
--   rpt_<database> login role                        the ONLY identity worklists run as
--
-- Every view leaves SR out (COALESCE(m.modality, study_modality, '') != 'SR') and takes
-- the modality from the AE map first. The definitions match the reports they sit
-- beside: referring name as utils/referring.py (Super Report), patient type from the
-- pc_inpatient / pc_outpatient / pc_emergency settings (Super Report), shift from the
-- shift_* settings set on Report 25, report turnaround from images stored in PACS
-- (insert_time) to final signature, as the Super Report measures it.
--
-- THE ROLE IS THE SANDBOX. Worklist SQL, generated or written by an admin, never runs
-- on the app's own connection: that one logs in as the table owner, a superuser in the
-- postgres image, and SET ROLE (or set_config('role', ...)) would switch straight back.
-- A separate LOGIN role is a boundary: it can read rpt.* and nothing else, every
-- transaction is read-only, every statement time-limited. A view reads its tables with
-- its owner's rights, so the role needs no grant on them. One role PER DATABASE
-- (rpt_<database name>): roles are cluster-wide and the password lives in this
-- database's settings, so a test database on the same server cannot rotate the live one.
--
-- RUNNER NOTE: db_migrations._split_sql ends a statement at every line that ends in a
-- semicolon. Inside the DO block below every line that has one ends with a comment
-- instead, so the block reaches Postgres whole. Keep it that way when editing.


CREATE SCHEMA IF NOT EXISTS rpt;


-- ── rpt.studies — one row per PACS study ──────────────────────────────────────
--
-- study_at is the PACS study date plus the DICOM study time (HHMMSS or HH:MM); hour,
-- shift and the delays are empty when PACS has no usable time. The study date stays
-- the index-backed filter (the worklist compiler bounds study_date as well), so a
-- date range never scans the whole table.
CREATE OR REPLACE VIEW rpt.studies AS
SELECT
    s.study_db_uid                                             AS study_uid,
    NULLIF(btrim(s.accession_number), '')                      AS accession,
    COALESCE(NULLIF(btrim(p.id), ''), NULLIF(btrim(p.fallback_id), '')) AS patient_id,
    upper(left(COALESCE(NULLIF(btrim(p.sex), ''), NULLIF(btrim(p.gender), '')), 1)) AS sex,
    ag.years                                                   AS age,
    CASE WHEN ag.years IS NULL THEN NULL
         WHEN ag.years < 18 THEN '0-17'
         WHEN ag.years < 40 THEN '18-39'
         WHEN ag.years < 65 THEN '40-64'
         ELSE '65+' END                                        AS age_band,
    NULLIF(btrim(s.patient_class), '')                         AS patient_class,
    CASE WHEN s.patient_class = ANY(pc.er)  THEN 'Emergency'
         WHEN s.patient_class = ANY(pc.inp) THEN 'Inpatient'
         WHEN s.patient_class = ANY(pc.outp) THEN 'Outpatient'
         ELSE NULLIF(btrim(s.patient_class), '') END           AS patient_type,
    NULLIF(btrim(s.patient_location), '')                      AS patient_location,
    COALESCE(m.modality, NULLIF(btrim(s.study_modality), ''))  AS modality,
    NULLIF(btrim(s.study_modality), '')                        AS pacs_modality,
    NULLIF(btrim(s.storing_ae), '')                            AS device,
    NULLIF(btrim(m.display_aetitle), '')                       AS device_name,
    NULLIF(btrim(m.room_name), '')                             AS room,
    NULLIF(btrim(s.procedure_code), '')                        AS procedure_code,
    COALESCE(NULLIF(btrim(s.study_description), ''), NULLIF(btrim(o.proc_text), ''),
             NULLIF(btrim(s.procedure_code), ''))              AS procedure,
    NULLIF(btrim(s.study_description), '')                     AS study_description,
    NULLIF(upper(btrim(s.study_body_part)), '')                AS body_part,
    NULLIF(btrim(s.study_status), '')                          AS study_status,
    NULLIF(btrim(s.report_status), '')                         AS report_status,
    NULLIF(btrim(s.order_status), '')                          AS order_status,
    t.study_at,
    s.study_date,
    t.hour,
    CASE WHEN t.hour IS NULL THEN NULL
         WHEN t.hour >= sh.ms AND t.hour < sh.me THEN 'Morning'
         WHEN t.hour >= sh.a_s AND t.hour < sh.a_e THEN 'Afternoon'
         ELSE 'Night' END                                      AS shift,
    extract(isodow FROM s.study_date)::int                     AS weekday,
    (extract(day FROM s.study_date)::int - 1) / 7 + 1          AS week_of_month,
    s.insert_time                                              AS stored_at,
    o.scheduled_datetime                                       AS scheduled_at,
    s.rep_transcribed_timestamp                                AS transcribed_at,
    s.rep_prelim_timestamp                                     AS prelim_signed_at,
    s.rep_final_timestamp                                      AS final_signed_at,
    s.rep_addendum_timestamp                                   AS addendum_at,
    CASE WHEN s.rep_final_timestamp IS NOT NULL THEN 'Final'
         WHEN s.rep_prelim_timestamp IS NOT NULL THEN 'Preliminary'
         WHEN s.rep_transcribed_timestamp IS NOT NULL THEN 'Transcribed'
         WHEN rs.has_report THEN 'Reported'
         ELSE 'Not reported' END                               AS report_stage,
    rs.has_report,
    (COALESCE(s.rep_has_addendum, FALSE) OR COALESCE(s.has_addendum, FALSE)
     OR s.rep_addendum_timestamp IS NOT NULL)                  AS has_addendum,
    (COALESCE(s.is_linked_study, FALSE) OR COALESCE(s.is_linked, FALSE)) AS linked,
    nm.technologist,
    nm.performing_physician,
    COALESCE(pa_r.canonical_name, nm.reader)                   AS reading_radiologist,
    COALESCE(pa_s.canonical_name, nm.signer)                   AS signing_radiologist,
    CASE WHEN upper(nm.ref_first) = 'EXTERNAL DOC' OR upper(nm.ref_last) = 'EXTERNAL DOC'
         THEN 'External referrals'
         ELSE NULLIF(concat_ws(' ', nm.ref_first, nm.ref_last), '') END AS referring,
    s.number_of_study_images                                   AS images,
    s.number_of_study_series                                   AS series,
    round(extract(epoch FROM (s.rep_final_timestamp - s.insert_time)) / 3600.0, 2)  AS report_tat_hours,
    round(extract(epoch FROM (s.rep_prelim_timestamp - s.insert_time)) / 3600.0, 2) AS prelim_tat_hours,
    CASE WHEN t.hour IS NOT NULL
         THEN round(extract(epoch FROM (s.insert_time - t.study_at)) / 60.0, 1) END AS store_delay_minutes,
    CASE WHEN t.hour IS NOT NULL
         THEN round(extract(epoch FROM (t.study_at - o.scheduled_datetime)) / 60.0, 1) END AS start_delay_minutes,
    (ru.reports > 0)                                           AS oru_report,
    COALESCE(ru.critical, FALSE)                               AS critical,
    ru.findings,
    EXISTS (SELECT 1 FROM crn_notifications c
             WHERE c.accession_number = NULLIF(btrim(s.accession_number), '')) AS crn
FROM etl_didb_studies s
CROSS JOIN (
    SELECT ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_inpatient'), 'I,IP,INPAT,INPATIENT,INN'), ',')) x
                  WHERE btrim(x) <> '') AS inp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_outpatient'), 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY'), ',')) x
                  WHERE btrim(x) <> '') AS outp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_emergency'), 'E,EP,ER,EMERGENCY,URG,URGENT'), ',')) x
                  WHERE btrim(x) <> '') AS er
      FROM settings WHERE key IN ('pc_inpatient', 'pc_outpatient', 'pc_emergency')
) pc
CROSS JOIN (
    SELECT COALESCE(MAX(CASE WHEN key = 'shift_morning_start'   AND value ~ '^[0-9]{1,2}$' THEN value::int END), 7)  AS ms,
           COALESCE(MAX(CASE WHEN key = 'shift_morning_end'     AND value ~ '^[0-9]{1,2}$' THEN value::int END), 15) AS me,
           COALESCE(MAX(CASE WHEN key = 'shift_afternoon_start' AND value ~ '^[0-9]{1,2}$' THEN value::int END), 15) AS a_s,
           COALESCE(MAX(CASE WHEN key = 'shift_afternoon_end'   AND value ~ '^[0-9]{1,2}$' THEN value::int END), 23) AS a_e
      FROM settings WHERE key ~ '^shift_'
) sh
LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.room_name, mm.display_aetitle
      FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(btrim(s.storing_ae))
     ORDER BY (mm.aetitle = s.storing_ae) DESC, mm.id
     LIMIT 1
) m ON TRUE
LEFT JOIN LATERAL (
    SELECT eo.proc_text, eo.scheduled_datetime
      FROM etl_orders eo
     WHERE eo.study_db_uid = s.study_db_uid
     ORDER BY eo.scheduled_datetime DESC NULLS LAST, eo.order_dbid DESC
     LIMIT 1
) o ON TRUE
CROSS JOIN LATERAL (
    SELECT CASE
             WHEN s.study_time ~ '^([01][0-9]|2[0-3])[0-5][0-9]'
             THEN make_interval(hours => substr(s.study_time, 1, 2)::int,
                                mins  => substr(s.study_time, 3, 2)::int,
                                secs  => COALESCE(substring(s.study_time FROM '^[0-9]{4}([0-5][0-9])'), '0')::int)
             WHEN s.study_time ~ '^([01]?[0-9]|2[0-3]):[0-5][0-9]'
             THEN make_interval(hours => split_part(s.study_time, ':', 1)::int,
                                mins  => substr(split_part(s.study_time, ':', 2), 1, 2)::int)
           END AS tod
) tm
CROSS JOIN LATERAL (
    SELECT s.study_date::timestamp + COALESCE(tm.tod, INTERVAL '0') AS study_at,
           extract(hour FROM tm.tod)::int                           AS hour
) t
CROSS JOIN LATERAL (
    SELECT CASE WHEN p.birth_date IS NOT NULL AND s.study_date IS NOT NULL
                THEN COALESCE(floor(s.age_at_exam)::int,
                              date_part('year', age(s.study_date, p.birth_date))::int)
                ELSE floor(s.age_at_exam)::int END AS years
) ag
CROSS JOIN LATERAL (
    SELECT (COALESCE(s.study_has_report, FALSE) OR COALESCE(s.has_report, FALSE)
            OR s.rep_final_timestamp IS NOT NULL OR s.rep_prelim_timestamp IS NOT NULL) AS has_report
) rs
CROSS JOIN LATERAL (
    SELECT NULLIF(btrim(regexp_replace(regexp_replace(s.operators_name, '\\', ', ', 'g'), '\^+', ' ', 'g')), '') AS technologist,
           NULLIF(btrim(regexp_replace(regexp_replace(s.performing_physicians_name, '\\', ', ', 'g'), '\^+', ' ', 'g')), '') AS performing_physician,
           NULLIF(concat_ws(' ', NULLIF(btrim(s.reading_physician_first_name), ''),
                                 NULLIF(btrim(s.reading_physician_last_name), '')), '') AS reader,
           COALESCE(NULLIF(concat_ws(' ', NULLIF(btrim(s.signing_physician_first_name), ''),
                                          NULLIF(btrim(s.signing_physician_last_name), '')), ''),
                    NULLIF(btrim(s.rep_final_signed_by), ''))                   AS signer,
           NULLIF(btrim(s.referring_physician_first_name), '')                   AS ref_first,
           NULLIF(btrim(s.referring_physician_last_name), '')                    AS ref_last
) nm
LEFT JOIN physician_alias_map pa_r ON pa_r.alias = nm.reader AND NOT pa_r.dismissed
LEFT JOIN physician_alias_map pa_s ON pa_s.alias = nm.signer AND NOT pa_s.dismissed
LEFT JOIN LATERAL (
    SELECT COUNT(DISTINCT r.id)                                 AS reports,
           bool_or(a.is_critical)                               AS critical,
           string_agg(DISTINCT f.label, ', ' ORDER BY f.label)  AS findings
      FROM hl7_oru_reports r
      LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
      LEFT JOIN LATERAL unnest(a.affirmed_labels) AS f(label) ON TRUE
     WHERE r.accession_number = NULLIF(btrim(s.accession_number), '')
) ru ON TRUE
WHERE COALESCE(m.modality, s.study_modality, '') != 'SR';


-- ── rpt.series — one row per PACS series ──────────────────────────────────────
-- The series' own modality (an MR study can hold an SR series: left out), its study's
-- date, device and mapped modality.
CREATE OR REPLACE VIEW rpt.series AS
SELECT
    se.series_db_uid                                           AS series_uid,
    se.study_db_uid                                            AS study_uid,
    NULLIF(btrim(s.accession_number), '')                      AS accession,
    COALESCE(NULLIF(btrim(p.id), ''), NULLIF(btrim(p.fallback_id), '')) AS patient_id,
    s.study_date::timestamp + COALESCE(tm.tod, INTERVAL '0')   AS study_at,
    s.study_date,
    extract(hour FROM tm.tod)::int                             AS hour,
    extract(isodow FROM s.study_date)::int                     AS weekday,
    NULLIF(btrim(se.modality), '')                             AS modality,
    COALESCE(m.modality, NULLIF(btrim(s.study_modality), ''))  AS study_modality,
    NULLIF(btrim(s.storing_ae), '')                            AS device,
    se.series_number,
    NULLIF(btrim(se.series_description), '')                   AS series_description,
    NULLIF(btrim(se.protocol_name), '')                        AS protocol,
    NULLIF(upper(btrim(se.body_part_examined)), '')            AS body_part,
    se.number_of_series_images                                 AS images,
    NULLIF(btrim(se.station_name), '')                         AS station,
    NULLIF(btrim(se.manufacturer), '')                         AS manufacturer,
    NULLIF(btrim(se.institution_name), '')                     AS institution,
    NULLIF(btrim(se.institutional_department_name), '')        AS department,
    NULLIF(btrim(s.study_description), '')                     AS study_description
FROM etl_didb_serieses se
JOIN etl_didb_studies s ON s.study_db_uid = se.study_db_uid
LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
LEFT JOIN LATERAL (
    SELECT mm.modality FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(btrim(s.storing_ae))
     ORDER BY (mm.aetitle = s.storing_ae) DESC, mm.id LIMIT 1
) m ON TRUE
CROSS JOIN LATERAL (
    SELECT CASE
             WHEN s.study_time ~ '^([01][0-9]|2[0-3])[0-5][0-9]'
             THEN make_interval(hours => substr(s.study_time, 1, 2)::int,
                                mins  => substr(s.study_time, 3, 2)::int)
             WHEN s.study_time ~ '^([01]?[0-9]|2[0-3]):[0-5][0-9]'
             THEN make_interval(hours => split_part(s.study_time, ':', 1)::int,
                                mins  => substr(split_part(s.study_time, ':', 2), 1, 2)::int)
           END AS tod
) tm
WHERE COALESCE(m.modality, s.study_modality, '') != 'SR'
  AND COALESCE(NULLIF(btrim(se.modality), ''), '') != 'SR';


-- ── rpt.orders — one row per PACS order (etl_orders) ──────────────────────────
-- The order's patient is its study's, else the PACS patient the order names
-- (etl_patient_view.mdl_patient_dbid). scheduled_at is the PACS scheduled time, else
-- the order time (the ETL keeps COALESCE(SCHEDULED_DATETIME, ORDER_DATETIME)).
CREATE OR REPLACE VIEW rpt.orders AS
SELECT
    o.order_dbid                                               AS order_id,
    o.study_db_uid                                             AS study_uid,
    NULLIF(btrim(s.accession_number), '')                      AS accession,
    COALESCE(NULLIF(btrim(p.id), ''), NULLIF(btrim(p.fallback_id), ''),
             NULLIF(btrim(pm.id), ''), NULLIF(btrim(pm.fallback_id), '')) AS patient_id,
    NULLIF(btrim(s.patient_class), '')                         AS patient_class,
    CASE WHEN s.patient_class = ANY(pc.er)  THEN 'Emergency'
         WHEN s.patient_class = ANY(pc.inp) THEN 'Inpatient'
         WHEN s.patient_class = ANY(pc.outp) THEN 'Outpatient'
         ELSE NULLIF(btrim(s.patient_class), '') END           AS patient_type,
    NULLIF(btrim(o.proc_id), '')                               AS procedure_code,
    COALESCE(NULLIF(btrim(o.proc_text), ''), NULLIF(btrim(s.study_description), ''),
             NULLIF(btrim(o.proc_id), ''))                     AS procedure,
    COALESCE(m.modality, NULLIF(btrim(s.study_modality), ''), NULLIF(btrim(o.modality), '')) AS modality,
    NULLIF(btrim(s.storing_ae), '')                            AS device,
    o.scheduled_datetime                                       AS scheduled_at,
    o.scheduled_datetime::date                                 AS scheduled_date,
    extract(hour FROM o.scheduled_datetime)::int               AS hour,
    extract(isodow FROM o.scheduled_datetime)::int             AS weekday,
    NULLIF(btrim(o.order_status), '')                          AS order_status,
    NULLIF(btrim(o.order_control), '')                         AS order_control,
    (s.study_db_uid IS NOT NULL OR COALESCE(o.has_study, FALSE)) AS performed,
    (upper(btrim(COALESCE(o.order_status, ''))) IN ('CA', 'DC')
     OR upper(btrim(COALESCE(o.order_control, ''))) IN ('CA', 'OC', 'DC', 'OD')) AS cancelled,
    t.study_at,
    CASE WHEN s.rep_final_timestamp IS NOT NULL THEN 'Final'
         WHEN s.rep_prelim_timestamp IS NOT NULL THEN 'Preliminary'
         WHEN s.rep_transcribed_timestamp IS NOT NULL THEN 'Transcribed'
         WHEN s.study_db_uid IS NULL THEN NULL
         WHEN COALESCE(s.study_has_report, FALSE) OR COALESCE(s.has_report, FALSE) THEN 'Reported'
         ELSE 'Not reported' END                               AS report_stage,
    s.rep_final_timestamp                                      AS final_signed_at,
    CASE WHEN upper(nm.ref_first) = 'EXTERNAL DOC' OR upper(nm.ref_last) = 'EXTERNAL DOC'
         THEN 'External referrals'
         ELSE NULLIF(concat_ws(' ', nm.ref_first, nm.ref_last), '') END AS referring,
    CASE WHEN tm.tod IS NOT NULL
         THEN round(extract(epoch FROM (t.study_at - o.scheduled_datetime)) / 60.0, 1) END AS start_delay_minutes
FROM etl_orders o
CROSS JOIN (
    SELECT ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_inpatient'), 'I,IP,INPAT,INPATIENT,INN'), ',')) x
                  WHERE btrim(x) <> '') AS inp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_outpatient'), 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY'), ',')) x
                  WHERE btrim(x) <> '') AS outp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_emergency'), 'E,EP,ER,EMERGENCY,URG,URGENT'), ',')) x
                  WHERE btrim(x) <> '') AS er
      FROM settings WHERE key IN ('pc_inpatient', 'pc_outpatient', 'pc_emergency')
) pc
LEFT JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
LEFT JOIN LATERAL (
    SELECT x.id, x.fallback_id FROM etl_patient_view x
     WHERE s.study_db_uid IS NULL AND x.mdl_patient_dbid = NULLIF(btrim(o.patient_dbid), '')
     ORDER BY x.patient_db_uid LIMIT 1
) pm ON TRUE
LEFT JOIN LATERAL (
    SELECT mm.modality FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(btrim(s.storing_ae))
     ORDER BY (mm.aetitle = s.storing_ae) DESC, mm.id LIMIT 1
) m ON TRUE
CROSS JOIN LATERAL (
    SELECT CASE
             WHEN s.study_time ~ '^([01][0-9]|2[0-3])[0-5][0-9]'
             THEN make_interval(hours => substr(s.study_time, 1, 2)::int,
                                mins  => substr(s.study_time, 3, 2)::int)
             WHEN s.study_time ~ '^([01]?[0-9]|2[0-3]):[0-5][0-9]'
             THEN make_interval(hours => split_part(s.study_time, ':', 1)::int,
                                mins  => substr(split_part(s.study_time, ':', 2), 1, 2)::int)
           END AS tod
) tm
CROSS JOIN LATERAL (
    SELECT s.study_date::timestamp + COALESCE(tm.tod, INTERVAL '0') AS study_at
) t
CROSS JOIN LATERAL (
    SELECT NULLIF(btrim(s.referring_physician_first_name), '') AS ref_first,
           NULLIF(btrim(s.referring_physician_last_name), '')  AS ref_last
) nm
WHERE COALESCE(m.modality, s.study_modality, o.modality, '') != 'SR';


-- ── rpt.reports — one row per ORU report ──────────────────────────────────────
-- report_at is the report's own time (OBR-22/7), else when RAYD received it: the same
-- COALESCE the ORU page filters on and idx_oru_reports_coalesce_date indexes. The
-- study is the PACS study with the report's accession.
CREATE OR REPLACE VIEW rpt.reports AS
SELECT
    r.id                                                       AS report_id,
    NULLIF(btrim(r.accession_number), '')                      AS accession,
    COALESCE(NULLIF(btrim(r.patient_id), ''), st.patient_id)   AS patient_id,
    NULLIF(btrim(r.physician_id), '')                          AS radiologist,
    COALESCE(st.modality, NULLIF(btrim(r.modality), ''))       AS modality,
    COALESCE(NULLIF(btrim(r.procedure_name), ''), st.study_description,
             NULLIF(btrim(r.procedure_code), ''))              AS procedure,
    NULLIF(btrim(r.procedure_code), '')                        AS procedure_code,
    st.device,
    st.patient_class,
    CASE WHEN st.patient_class = ANY(pc.er)  THEN 'Emergency'
         WHEN st.patient_class = ANY(pc.inp) THEN 'Inpatient'
         WHEN st.patient_class = ANY(pc.outp) THEN 'Outpatient'
         ELSE st.patient_class END                             AS patient_type,
    COALESCE(r.result_datetime, r.received_at)                 AS report_at,
    COALESCE(r.result_datetime, r.received_at)::date           AS report_date,
    r.received_at,
    st.study_date,
    st.stored_at,
    round(extract(epoch FROM (COALESCE(r.result_datetime, r.received_at) - st.stored_at)) / 3600.0, 2)
                                                               AS report_tat_hours,
    (a.id IS NOT NULL)                                         AS analysed,
    COALESCE(a.is_critical, FALSE)                             AS critical,
    COALESCE(cardinality(a.affirmed_labels), 0)                AS finding_count,
    NULLIF(array_to_string(a.affirmed_labels, ', '), '')       AS findings,
    (NULLIF(btrim(r.impression_text), '') IS NOT NULL)         AS has_impression,
    COALESCE(array_length(regexp_split_to_array(NULLIF(btrim(r.report_text), ''), '\s+'), 1), 0)
                                                               AS report_words,
    EXISTS (SELECT 1 FROM crn_notifications c WHERE c.report_id = r.id) AS crn
FROM hl7_oru_reports r
CROSS JOIN (
    SELECT ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_inpatient'), 'I,IP,INPAT,INPATIENT,INN'), ',')) x
                  WHERE btrim(x) <> '') AS inp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_outpatient'), 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY'), ',')) x
                  WHERE btrim(x) <> '') AS outp,
           ARRAY(SELECT btrim(x) FROM unnest(string_to_array(COALESCE(
               MAX(value) FILTER (WHERE key = 'pc_emergency'), 'E,EP,ER,EMERGENCY,URG,URGENT'), ',')) x
                  WHERE btrim(x) <> '') AS er
      FROM settings WHERE key IN ('pc_inpatient', 'pc_outpatient', 'pc_emergency')
) pc
LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
LEFT JOIN LATERAL (
    SELECT COALESCE(mm.modality, NULLIF(btrim(s.study_modality), '')) AS modality,
           NULLIF(btrim(s.storing_ae), '')        AS device,
           NULLIF(btrim(s.patient_class), '')     AS patient_class,
           NULLIF(btrim(s.study_description), '') AS study_description,
           s.study_date, s.insert_time AS stored_at,
           COALESCE(NULLIF(btrim(p.id), ''), NULLIF(btrim(p.fallback_id), '')) AS patient_id
      FROM etl_didb_studies s
      LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
      LEFT JOIN LATERAL (
          SELECT x.modality FROM aetitle_modality_map x
           WHERE upper(btrim(x.aetitle)) = upper(btrim(s.storing_ae))
           ORDER BY (x.aetitle = s.storing_ae) DESC, x.id LIMIT 1
      ) mm ON TRUE
     WHERE s.accession_number = NULLIF(btrim(r.accession_number), '')
     ORDER BY s.study_db_uid DESC
     LIMIT 1
) st ON TRUE
WHERE COALESCE(st.modality, r.modality, '') != 'SR';


-- ── rpt.findings — one row per NLP finding of a report ────────────────────────
-- The labels the NLP worker affirmed (negation and history removed, benign labels
-- left out), the same ones the ORU page's word cloud and critical log count.
CREATE OR REPLACE VIEW rpt.findings AS
SELECT
    rr.report_id, rr.accession, rr.patient_id, rr.radiologist, rr.modality, rr.procedure,
    rr.device, rr.patient_type, rr.report_at, rr.report_date,
    f.label                                                    AS finding
FROM hl7_oru_analysis a
CROSS JOIN LATERAL unnest(a.affirmed_labels) AS f(label)
JOIN rpt.reports rr ON rr.report_id = a.report_id;


-- ── rpt.storage — one row per day / device / procedure (summary_storage_daily) ──
CREATE OR REPLACE VIEW rpt.storage AS
SELECT
    ss.study_date::timestamp                                   AS study_at,
    ss.study_date,
    COALESCE(m.modality, NULLIF(btrim(ss.modality), ''))       AS modality,
    NULLIF(btrim(ss.storing_ae), '')                           AS device,
    NULLIF(btrim(ss.procedure_code), '')                       AS procedure_code,
    ss.total_gb::numeric                                       AS gb,
    ss.study_count                                             AS studies
FROM summary_storage_daily ss
LEFT JOIN LATERAL (
    SELECT mm.modality FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(btrim(ss.storing_ae))
     ORDER BY (mm.aetitle = ss.storing_ae) DESC, mm.id LIMIT 1
) m ON TRUE
WHERE COALESCE(m.modality, ss.modality, '') != 'SR';


-- ── rpt.crn — one row per Critical Result Notification ───────────────────────
-- The same reading as Admin > CRN > Report (utils/crn_admin_ops.report): acknowledgement
-- time from the report's arrival, overdue / resent / fallback / by phone from the events.
CREATE OR REPLACE VIEW rpt.crn AS
SELECT
    n.id                                                       AS crn_id,
    n.ref_code,
    NULLIF(btrim(n.accession_number), '')                      AS accession,
    NULLIF(btrim(n.patient_id), '')                            AS patient_id,
    NULLIF(btrim(n.signing_radiologist), '')                   AS radiologist,
    NULLIF(btrim(n.doctor_code), '')                           AS doctor_code,
    COALESCE(NULLIF(btrim(n.doctor_name), ''), NULLIF(btrim(n.doctor_code), '')) AS doctor,
    n.status,
    n.detected_at,
    n.detected_at::date                                        AS detected_date,
    n.report_received_at,
    n.first_sent_at,
    n.acknowledged_at,
    (n.acknowledged_at IS NOT NULL)                            AS acknowledged,
    round(extract(epoch FROM (n.acknowledged_at - COALESCE(n.report_received_at, n.detected_at))) / 60.0, 1)
                                                               AS ack_minutes,
    ev.overdue, ev.resent, ev.fallback, ev.by_phone,
    CASE WHEN n.acknowledged_at IS NULL THEN 'not acknowledged'
         WHEN ev.by_phone THEN 'by phone'
         WHEN rc.role = 'fallback' THEN 'fallback contact'
         WHEN ev.resent THEN 'after resend'
         ELSE 'first SMS' END                                  AS acknowledged_how,
    st.modality,
    st.procedure,
    st.device
FROM crn_notifications n
LEFT JOIN crn_recipients rc ON rc.id = n.acknowledged_by
CROSS JOIN LATERAL (
    SELECT COALESCE(bool_or(e.event_type = 'overdue'), FALSE)                                   AS overdue,
           COALESCE(bool_or(e.event_type = 'resent'), FALSE)                                    AS resent,
           COALESCE(bool_or(e.event_type IN ('escalated_fallback', 'sent_to_fallback')), FALSE) AS fallback,
           COALESCE(bool_or(e.event_type = 'acknowledged_by_phone'), FALSE)                     AS by_phone
      FROM crn_events e WHERE e.notification_id = n.id
) ev
LEFT JOIN LATERAL (
    SELECT COALESCE(mm.modality, NULLIF(btrim(s.study_modality), '')) AS modality,
           COALESCE(NULLIF(btrim(s.study_description), ''), NULLIF(btrim(s.procedure_code), '')) AS procedure,
           NULLIF(btrim(s.storing_ae), '') AS device
      FROM etl_didb_studies s
      LEFT JOIN LATERAL (
          SELECT x.modality FROM aetitle_modality_map x
           WHERE upper(btrim(x.aetitle)) = upper(btrim(s.storing_ae))
           ORDER BY (x.aetitle = s.storing_ae) DESC, x.id LIMIT 1
      ) mm ON TRUE
     WHERE s.accession_number = NULLIF(btrim(n.accession_number), '')
     ORDER BY s.study_db_uid DESC
     LIMIT 1
) st ON TRUE;


-- ── Saved worklists ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS worklists (
    id           SERIAL    PRIMARY KEY,
    name         TEXT      NOT NULL,
    description  TEXT,
    folder       TEXT      NOT NULL DEFAULT 'General',
    visibility   TEXT      NOT NULL DEFAULT 'personal'
                 CHECK (visibility IN ('personal', 'shared')),
    owner_id     INTEGER   REFERENCES users(id) ON DELETE SET NULL,
    dataset      TEXT      NOT NULL,
    definition   JSONB     NOT NULL,
    version      INTEGER   NOT NULL DEFAULT 1,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by   INTEGER   REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_worklists_owner ON worklists (owner_id);

-- Every save, kept. A shared worklist edited into nonsense can be put back.
CREATE TABLE IF NOT EXISTS worklist_versions (
    worklist_id  INTEGER   NOT NULL REFERENCES worklists(id) ON DELETE CASCADE,
    version      INTEGER   NOT NULL,
    name         TEXT      NOT NULL,
    definition   JSONB     NOT NULL,
    saved_by     INTEGER   REFERENCES users(id) ON DELETE SET NULL,
    saved_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (worklist_id, version)
);


-- ── A starting library ────────────────────────────────────────────────────────
-- Shared examples, ownerless so any admin may adapt them; tests/test_worklists.py
-- runs every one.
INSERT INTO worklists (name, description, folder, visibility, dataset, definition)
SELECT v.name, v.description, 'Examples', 'shared', v.dataset, v.definition::jsonb
  FROM (VALUES
    ('Studies per modality, by month',
     'Volume per modality over the last three complete months.', 'studies',
     '{"dataset":"studies","criteria":{"study_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"chart","rows":"study_at:month","cols":"modality","measures":["count"]}}'),
    ('Technologists: morning shift, first week of the month',
     'Studies per technologist (DICOM operator) on the morning shift during the first week of each month.', 'studies',
     '{"dataset":"studies","criteria":{"study_at":{"mode":"quick","quick":"last_3_months"},"shift":{"values":["Morning"]},"week_of_month":{"values":[1]}},"output":{"kind":"table","rows":"technologist","cols":"study_at:month","measures":["count"]}}'),
    ('Report turnaround by signing radiologist',
     'Median and average hours from images in PACS to final signature, last month.', 'studies',
     '{"dataset":"studies","criteria":{"study_at":{"mode":"quick","quick":"last_month"},"report_stage":{"values":["Final"]}},"output":{"kind":"table","rows":"signing_radiologist","measures":["count","median_report_tat","avg_report_tat"]}}'),
    ('Unreported studies, last 7 days',
     'Studies of the last seven days with no report yet, oldest first.', 'studies',
     '{"dataset":"studies","criteria":{"study_at":{"mode":"quick","quick":"last_7_days"},"has_report":{"value":"no"}},"output":{"kind":"list","columns":["study_at","accession","patient_id","modality","procedure","device","patient_type","referring"],"sort":"study_at","desc":false}}'),
    ('Emergency studies today',
     'Every ER study since midnight, newest first.', 'studies',
     '{"dataset":"studies","criteria":{"study_at":{"mode":"quick","quick":"today"},"patient_type":{"values":["Emergency"]}},"output":{"kind":"list","columns":["study_at","accession","patient_id","modality","procedure","device","report_stage","technologist"],"sort":"study_at","desc":true}}'),
    ('Critical findings, last 30 days',
     'ORU reports the NLP marked critical, with their findings.', 'reports',
     '{"dataset":"reports","criteria":{"report_at":{"mode":"quick","quick":"last_30_days"},"critical":{"value":"yes"}},"output":{"kind":"list","columns":["report_at","accession","patient_id","modality","procedure","radiologist","findings","crn"],"sort":"report_at","desc":true}}'),
    ('Most frequent findings by modality',
     'NLP findings counted per modality over the last three complete months.', 'findings',
     '{"dataset":"findings","criteria":{"report_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"table","rows":"finding","cols":"modality","measures":["count"]}}'),
    ('Storage per modality, by month',
     'Gigabytes stored per modality over the last three complete months.', 'storage',
     '{"dataset":"storage","criteria":{"study_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"chart","rows":"study_at:month","cols":"modality","measures":["gb"]}}'),
    ('Protocols per device',
     'Series per acquisition protocol and device, last 30 days.', 'series',
     '{"dataset":"series","criteria":{"study_at":{"mode":"quick","quick":"last_30_days"}},"output":{"kind":"table","rows":"protocol","cols":"device","measures":["count"]}}'),
    ('Orders not performed',
     'PACS orders scheduled in the last 30 days that have no study yet and were not cancelled.', 'orders',
     '{"dataset":"orders","criteria":{"scheduled_at":{"mode":"quick","quick":"last_30_days"},"performed":{"value":"no"},"cancelled":{"value":"no"}},"output":{"kind":"list","columns":["scheduled_at","order_id","patient_id","procedure","modality","order_status"],"sort":"scheduled_at","desc":true}}'),
    ('CRN acknowledgement',
     'Critical Result Notifications of the last 3 months by how they were acknowledged.', 'crn',
     '{"dataset":"crn","criteria":{"detected_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"table","rows":"acknowledged_how","measures":["count","median_ack_minutes"]}}')
  ) AS v(name, description, dataset, definition)
 WHERE NOT EXISTS (SELECT 1 FROM worklists w WHERE w.folder = 'Examples' AND w.name = v.name);

INSERT INTO worklist_versions (worklist_id, version, name, definition)
SELECT w.id, 1, w.name, w.definition FROM worklists w
 WHERE w.folder = 'Examples'
   AND NOT EXISTS (SELECT 1 FROM worklist_versions v WHERE v.worklist_id = w.id);


-- ── The role worklists run as ─────────────────────────────────────────────────
-- (Every line inside the block that ends a statement carries a comment: see the
-- RUNNER NOTE at the top.)
DO $wl$
DECLARE
    r text := 'rpt_' || regexp_replace(lower(current_database()), '[^a-z0-9_]', '_', 'g');  -- one role per database
    p text := md5(random()::text || clock_timestamp()::text) || md5(random()::text);       -- fresh password each run
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
        EXECUTE 'CREATE ROLE ' || quote_ident(r) || ' LOGIN PASSWORD ' || quote_literal(p)
             || ' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION';                 -- new role
    ELSE
        EXECUTE 'ALTER ROLE ' || quote_ident(r) || ' LOGIN PASSWORD ' || quote_literal(p);    -- re-run: rotate
    END IF;                                                                                    -- role exists
    EXECUTE 'ALTER ROLE ' || quote_ident(r) || ' SET default_transaction_read_only = on';     -- read-only
    EXECUTE 'ALTER ROLE ' || quote_ident(r) || ' SET statement_timeout = ' || quote_literal('30s');  -- time limit
    EXECUTE 'ALTER ROLE ' || quote_ident(r) || ' SET search_path = rpt';                      -- FROM studies
    EXECUTE 'GRANT USAGE ON SCHEMA rpt TO ' || quote_ident(r);                                -- see the schema
    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA rpt TO ' || quote_ident(r);                 -- read the views
    EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA rpt GRANT SELECT ON TABLES TO ' || quote_ident(r);  -- and later ones
    INSERT INTO settings (key, value) VALUES ('worklist_db_role', r)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;                                -- who
    INSERT INTO settings (key, value) VALUES ('worklist_db_password', p)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;                                -- its password
END
$wl$;
