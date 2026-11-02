-- Migration 0136: Dynamic Worklists on LAUMC — the RIS and PACS data layer.
--
-- Replaces the Custom Reports composer (operator, 2026-11-02). The editor
-- (routes/worklists.py) is Bekaa's (5d06f2cc), itself the HL7 branch's RAYD V2.0 editor.
-- The HL7 views read the RIS lifecycle from HL7 messages, which LAUMC does not receive
-- yet (R2I); Bekaa's read PACS only. LAUMC has both through the Oracle ETL, so its views
-- are Bekaa's PACS ones plus the RIS exam:
--
--   rpt.exams      one row per RIS exam              etl_orders (RIS SITE_WORKLIST) +
--                                                    status history, PPS, RIS report
--   rpt.studies    one row per PACS study            etl_didb_studies (+ patient, AE map,
--                                                    RIS order, ORU report, NLP, CRN)
--   rpt.series     one row per PACS series           etl_didb_serieses
--   rpt.reports    one row per ORU report            hl7_oru_reports + hl7_oru_analysis
--   rpt.findings   one row per NLP finding           hl7_oru_analysis.affirmed_labels
--   rpt.storage    one row per day / AE / procedure  summary_storage_daily
--   rpt.crn        one row per CRN notification      crn_notifications (+ events)
--
--   worklists, worklist_versions                     saved definitions, every save kept
--   rpt_<database> login role                        the ONLY identity worklists run as
--
-- Bekaa's rpt.orders (PACS orders) is not here: on LAUMC etl_orders holds the RIS
-- worklist (ETL_JOBS/etl_orders.py), which rpt.exams reads properly.
--
-- Every view leaves SR out and takes the modality from the AE map first. A PACS study
-- is counted as the reports count it (etl_didb_studies, operator 2026-11-02), not by
-- fact_exam. The definitions match the reports beside them: the RIS lifecycle as
-- Report 35 reads it, the patient MRN from the RIS identity as Report 27 picks it
-- (etl_patient_view.id is PACS-internal at LAUMC), referring name and patient type as
-- the Super Report, shift from the shift_* settings set on Report 25, report
-- turnaround from images stored in PACS (insert_time) to final signature.
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
-- RLS NOTE: the views belong to the table owner, which bypasses the site policies of
-- migration 0050. Those are not wired yet (utils/site_scope.py); once they are,
-- worklists still see every site — filter on the site column, or scope the views.


CREATE SCHEMA IF NOT EXISTS rpt;


-- ── rpt.exams — one row per RIS exam ──────────────────────────────────────────
--
-- One row per RIS worklist entry (SITE_WORKLIST: one scheduled procedure step, one
-- accession), as etl_orders holds it on LAUMC (order_dbid = SITE_WORKLIST_KEY). The
-- lifecycle is the RIS's own status history (migrations 0088 / 0090 / 0092 / 0122) and
-- performed step (std_pps), read as Report 35 reads them:
--   booked_at        first Scheduled transition (status 40)
--   arrived_at       first Arrived transition (60)
--   started_at       scan start: the performed step's start (the modality's MPPS)
--   scanner_done_at  scan end: the performed step's end
--   exam_done_at     last Exam Done transition (100)
--   technologist     the performed step's first person reference whose RIS role is TEC
-- The performed step is the one a transition names (exam done, else arrival, else
-- scheduling). The report chain is the RIS report's current version (std_reports,
-- is_max_version, migration 0119): Signed 1 and Approved, and who did each. The status
-- is the RIS status in words (worklist_status_map's stage). Device, images, patient
-- class and referring physician come from the PACS study the ETL matched
-- (etl_orders.study_db_uid), so they stay empty until the exam is in PACS.
--
-- scheduled_at (SITE_WORKLIST.SCHEDULED_DATE) is a column of etl_orders and indexed:
-- filtered on, it bounds the rows before any lookup. The other dates are looked up per
-- exam, so filtering on them alone reads every exam.
CREATE OR REPLACE VIEW rpt.exams AS
SELECT
    o.order_dbid                                               AS exam_id,
    NULLIF(btrim(o.accession_number), '')                      AS accession,
    mrn.patient_id,
    upper(left(NULLIF(btrim(pr.gender_code), ''), 1))          AS sex,
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
    COALESCE(m.modality, NULLIF(btrim(o.modality), ''))        AS modality,
    NULLIF(btrim(o.modality), '')                              AS ris_modality,
    dv.device,
    NULLIF(btrim(m.display_aetitle), '')                       AS device_name,
    NULLIF(btrim(m.room_name), '')                             AS room,
    si.name                                                    AS site,
    NULLIF(btrim(o.proc_id), '')                               AS procedure_code,
    COALESCE(NULLIF(btrim(o.proc_text), ''), NULLIF(btrim(o.proc_id), '')) AS procedure,
    COALESCE(NULLIF(btrim(pri.description), ''), NULLIF(btrim(pri.code), '')) AS priority,
    st.exam_status,
    NULLIF(btrim(wsm.status_name), '')                         AS ris_status,
    st.cancelled,
    st.performed,
    o.scheduled_datetime                                       AS scheduled_at,
    o.scheduled_datetime::date                                 AS scheduled_date,
    sc.booked_at,
    ar.arrived_at,
    pps.start_datetime                                         AS started_at,
    pps.end_datetime                                           AS scanner_done_at,
    ed.exam_done_at,
    CASE WHEN st.cancelled THEN ca.cancelled_at END            AS cancelled_at,
    t.exam_at,
    t.hour,
    CASE WHEN t.hour IS NULL THEN NULL
         WHEN t.hour >= sh.ms AND t.hour < sh.me THEN 'Morning'
         WHEN t.hour >= sh.a_s AND t.hour < sh.a_e THEN 'Afternoon'
         ELSE 'Night' END                                      AS shift,
    extract(isodow FROM t.exam_at)::int                        AS weekday,
    (extract(day FROM t.exam_at)::int - 1) / 7 + 1             AS week_of_month,
    tr.technologist,
    rep.verified1_date                                         AS signed_at,
    rep.approved_date                                          AS approved_at,
    CASE WHEN rep.approved_date IS NOT NULL THEN 'Approved'
         WHEN rep.verified1_date IS NOT NULL THEN 'Signed'
         WHEN rep.report_key IS NOT NULL THEN 'Draft'
         ELSE 'Not reported' END                               AS ris_report,
    rp.signed_by,
    rp.approved_by,
    CASE WHEN upper(nm.ref_first) = 'EXTERNAL DOC' OR upper(nm.ref_last) = 'EXTERNAL DOC'
         THEN 'External referrals'
         ELSE NULLIF(concat_ws(' ', nm.ref_first, nm.ref_last), '') END AS referring,
    s.study_db_uid                                             AS study_uid,
    (s.study_db_uid IS NOT NULL)                               AS in_pacs,
    s.insert_time                                              AS stored_at,
    s.number_of_study_images                                   AS images,
    round(extract(epoch FROM (pps.start_datetime - ar.arrived_at)) / 60.0, 1)       AS wait_minutes,
    round(extract(epoch FROM (pps.start_datetime - o.scheduled_datetime)) / 60.0, 1) AS start_delay_minutes,
    round(extract(epoch FROM (pps.end_datetime - pps.start_datetime)) / 60.0, 1)     AS exam_minutes,
    round(extract(epoch FROM (ed.exam_done_at - ar.arrived_at)) / 60.0, 1)          AS arrival_to_done_minutes,
    round(extract(epoch FROM (rep.verified1_date - ed.exam_done_at)) / 3600.0, 2)   AS signed_tat_hours,
    round(extract(epoch FROM (rep.approved_date - ed.exam_done_at)) / 3600.0, 2)    AS report_tat_hours
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
CROSS JOIN (
    SELECT COALESCE(MAX(CASE WHEN key = 'shift_morning_start'   AND value ~ '^[0-9]{1,2}$' THEN value::int END), 7)  AS ms,
           COALESCE(MAX(CASE WHEN key = 'shift_morning_end'     AND value ~ '^[0-9]{1,2}$' THEN value::int END), 15) AS me,
           COALESCE(MAX(CASE WHEN key = 'shift_afternoon_start' AND value ~ '^[0-9]{1,2}$' THEN value::int END), 15) AS a_s,
           COALESCE(MAX(CASE WHEN key = 'shift_afternoon_end'   AND value ~ '^[0-9]{1,2}$' THEN value::int END), 23) AS a_e
      FROM settings WHERE key ~ '^shift_'
) sh
-- Each transition, with the performed step it names, read as ORDER BY time LIMIT 1 so
-- it walks the (site_worklist_key, time) index of its table. Not MIN()/MAX(): with two
-- aggregates Postgres may read MAX(pps_key) down the pps_key index instead, filtering
-- on the exam — the whole table, for every exam.
LEFT JOIN LATERAL (
    SELECT x.scheduled_at AS booked_at, x.pps_key
      FROM std_worklist_scheduled x WHERE x.site_worklist_key = o.order_dbid
     ORDER BY x.scheduled_at LIMIT 1
) sc ON TRUE
LEFT JOIN LATERAL (
    SELECT x.arrived_at, x.pps_key
      FROM std_worklist_arrivals x WHERE x.site_worklist_key = o.order_dbid
     ORDER BY x.arrived_at LIMIT 1
) ar ON TRUE
LEFT JOIN LATERAL (
    SELECT x.exam_done_at, x.pps_key
      FROM std_worklist_exam_done x WHERE x.site_worklist_key = o.order_dbid
     ORDER BY x.exam_done_at DESC LIMIT 1
) ed ON TRUE
LEFT JOIN LATERAL (
    SELECT x.cancelled_at
      FROM std_worklist_cancellations x WHERE x.site_worklist_key = o.order_dbid
     ORDER BY x.cancelled_at DESC LIMIT 1
) ca ON TRUE
LEFT JOIN std_pps pps ON pps.pps_key = COALESCE(ed.pps_key, ar.pps_key, sc.pps_key)
LEFT JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
CROSS JOIN LATERAL (
    SELECT COALESCE(NULLIF(btrim(pps.performing_ae_title), ''), NULLIF(btrim(s.storing_ae), '')) AS device
) dv
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.room_name, mm.display_aetitle
      FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(dv.device)
     ORDER BY (mm.aetitle = dv.device) DESC, mm.id
     LIMIT 1
) m ON TRUE
-- The site: the order's own, else the one whose RIS issuer is the order's
-- (etl_orders.order_control keeps ISSUER_OF_PLACER_ORDER_NUMBER: SAP_PROD, SAP_SJH).
LEFT JOIN LATERAL (
    SELECT x.name FROM sites x
     WHERE x.id = o.site_id OR x.ris_issuer = NULLIF(btrim(o.order_control), '')
     ORDER BY (x.id = o.site_id) DESC NULLS LAST, x.id
     LIMIT 1
) si ON TRUE
LEFT JOIN worklist_status_map wsm ON wsm.status_key = o.status_key
LEFT JOIN std_procedure_priorities pri ON pri.priority_key = pps.priority_key
LEFT JOIN std_patients_ris pr
       ON pr.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$' THEN o.patient_dbid::bigint END
-- One MRN per patient, chosen as Report 27 chooses it: a flag that LOOKS primary wins
-- the sort but is never required.
LEFT JOIN LATERAL (
    SELECT pi.patient_id
      FROM std_patient_ids pi
     WHERE pi.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$' THEN o.patient_dbid::bigint END
       AND NULLIF(btrim(pi.patient_id), '') IS NOT NULL
     ORDER BY (upper(btrim(COALESCE(pi.is_primary, ''))) IN ('Y', '1', 'T', 'TRUE')) DESC,
              pi.display_sort_order NULLS LAST, pi.sequence_id NULLS LAST, pi.patient_id_list_key
     LIMIT 1
) mrn ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(NULLIF(btrim(concat(res.first_name, ' ', res.last_name)), ''),
                    NULLIF(btrim(res.common_name), ''))        AS technologist
      FROM std_pps_person_reference ppr
      JOIN std_resources_ris res ON res.resource_id_key = ppr.resource_id_key
     WHERE ppr.pps_key = pps.pps_key AND res.role_code = 'TEC'
     ORDER BY ppr.display_sort_order ASC NULLS LAST
     LIMIT 1
) tr ON TRUE
LEFT JOIN LATERAL (
    SELECT r.report_key, r.verified1_date, r.approved_date,
           r.verified1_by_resource_id_key, r.approved_by_resource_id_key
      FROM std_reports r
     WHERE r.reported_acc_number = NULLIF(btrim(o.accession_number), '') AND r.is_max_version
     ORDER BY r.report_key DESC
     LIMIT 1
) rep ON TRUE
LEFT JOIN std_resources_ris r1 ON r1.resource_id_key = rep.verified1_by_resource_id_key
LEFT JOIN std_resources_ris r2 ON r2.resource_id_key = rep.approved_by_resource_id_key
CROSS JOIN LATERAL (
    SELECT COALESCE(NULLIF(btrim(concat(r1.first_name, ' ', r1.last_name)), ''),
                    NULLIF(btrim(r1.common_name), ''))         AS signed_by,
           COALESCE(NULLIF(btrim(concat(r2.first_name, ' ', r2.last_name)), ''),
                    NULLIF(btrim(r2.common_name), ''))         AS approved_by
) rp
CROSS JOIN LATERAL (
    SELECT COALESCE(pps.start_datetime, ed.exam_done_at, ar.arrived_at, o.scheduled_datetime) AS exam_at,
           extract(hour FROM COALESCE(pps.start_datetime, ed.exam_done_at, ar.arrived_at,
                                      o.scheduled_datetime))::int                            AS hour
) t
CROSS JOIN LATERAL (
    SELECT COALESCE(wsm.stage,
                    CASE upper(btrim(o.order_status)) WHEN 'CA' THEN 'cancelled' WHEN 'CM' THEN 'exam_done'
                         ELSE lower(NULLIF(btrim(o.order_status), '')) END) AS stage
) sg
CROSS JOIN LATERAL (
    SELECT CASE sg.stage WHEN 'requested' THEN 'Requested' WHEN 'scheduled' THEN 'Scheduled'
                         WHEN 'arrived' THEN 'Arrived' WHEN 'in_progress' THEN 'In progress'
                         WHEN 'exam_done' THEN 'Exam done' WHEN 'dictated' THEN 'Dictated'
                         WHEN 'prelim' THEN 'Preliminary' WHEN 'signed' THEN 'Signed'
                         WHEN 'approved' THEN 'Approved' WHEN 'cancelled' THEN 'Cancelled'
                         WHEN 'discontinued' THEN 'Discontinued'
                         ELSE initcap(replace(sg.stage, '_', ' ')) END                     AS exam_status,
           (COALESCE(wsm.is_cancel, FALSE) OR sg.stage IN ('cancelled', 'discontinued'))  AS cancelled,
           (pps.start_datetime IS NOT NULL OR ed.exam_done_at IS NOT NULL
            OR s.study_db_uid IS NOT NULL
            OR sg.stage IN ('in_progress', 'exam_done', 'dictated', 'prelim', 'signed', 'approved')) AS performed
) st
CROSS JOIN LATERAL (
    SELECT CASE WHEN pr.birth_date IS NOT NULL AND t.exam_at IS NOT NULL
                THEN date_part('year', age(t.exam_at::date, pr.birth_date))::int END AS years
) ag
CROSS JOIN LATERAL (
    SELECT NULLIF(btrim(s.referring_physician_first_name), '') AS ref_first,
           NULLIF(btrim(s.referring_physician_last_name), '')  AS ref_last
) nm
WHERE COALESCE(m.modality, NULLIF(btrim(o.modality), ''), '') != 'SR';


-- ── rpt.studies — one row per PACS study ──────────────────────────────────────
--
-- study_at is the PACS study date plus the DICOM study time (HHMMSS or HH:MM); hour,
-- shift and the delays are empty when PACS has no usable time. The study date stays
-- the index-backed filter (the worklist compiler bounds study_date as well), so a
-- date range never scans the whole table. The patient ID is the RIS MRN of the study's
-- RIS order, else PACS's fallback ID (what Report 22 shows), else its own.
CREATE OR REPLACE VIEW rpt.studies AS
SELECT
    s.study_db_uid                                             AS study_uid,
    NULLIF(btrim(s.accession_number), '')                      AS accession,
    COALESCE(mrn.patient_id, NULLIF(btrim(p.fallback_id), ''), NULLIF(btrim(p.id), '')) AS patient_id,
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
    si.name                                                    AS site,
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
LEFT JOIN sites si ON si.id = s.site_id
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.room_name, mm.display_aetitle
      FROM aetitle_modality_map mm
     WHERE upper(btrim(mm.aetitle)) = upper(btrim(s.storing_ae))
     ORDER BY (mm.aetitle = s.storing_ae) DESC, mm.id
     LIMIT 1
) m ON TRUE
-- The study's RIS order (the ETL matches it on the accession): its appointment, its
-- procedure text, and the RIS patient behind it.
LEFT JOIN LATERAL (
    SELECT eo.proc_text, eo.scheduled_datetime, eo.patient_dbid
      FROM etl_orders eo
     WHERE eo.study_db_uid = s.study_db_uid
     ORDER BY eo.scheduled_datetime DESC NULLS LAST, eo.order_dbid DESC
     LIMIT 1
) o ON TRUE
LEFT JOIN LATERAL (
    SELECT pi.patient_id
      FROM std_patient_ids pi
     WHERE pi.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$' THEN o.patient_dbid::bigint END
       AND NULLIF(btrim(pi.patient_id), '') IS NOT NULL
     ORDER BY (upper(btrim(COALESCE(pi.is_primary, ''))) IN ('Y', '1', 'T', 'TRUE')) DESC,
              pi.display_sort_order NULLS LAST, pi.sequence_id NULLS LAST, pi.patient_id_list_key
     LIMIT 1
) mrn ON TRUE
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
-- date, device, mapped modality and patient (as rpt.studies finds it).
CREATE OR REPLACE VIEW rpt.series AS
SELECT
    se.series_db_uid                                           AS series_uid,
    se.study_db_uid                                            AS study_uid,
    NULLIF(btrim(s.accession_number), '')                      AS accession,
    COALESCE(mrn.patient_id, NULLIF(btrim(p.fallback_id), ''), NULLIF(btrim(p.id), '')) AS patient_id,
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
LEFT JOIN LATERAL (
    SELECT eo.patient_dbid FROM etl_orders eo
     WHERE eo.study_db_uid = s.study_db_uid
     ORDER BY eo.scheduled_datetime DESC NULLS LAST, eo.order_dbid DESC LIMIT 1
) o ON TRUE
LEFT JOIN LATERAL (
    SELECT pi.patient_id
      FROM std_patient_ids pi
     WHERE pi.patient_person_key = CASE WHEN o.patient_dbid ~ '^[0-9]+$' THEN o.patient_dbid::bigint END
       AND NULLIF(btrim(pi.patient_id), '') IS NOT NULL
     ORDER BY (upper(btrim(COALESCE(pi.is_primary, ''))) IN ('Y', '1', 'T', 'TRUE')) DESC,
              pi.display_sort_order NULLS LAST, pi.sequence_id NULLS LAST, pi.patient_id_list_key
     LIMIT 1
) mrn ON TRUE
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


-- ── rpt.reports — one row per ORU report ──────────────────────────────────────
-- report_at is the report's own time (OBR-22/7), else when RAYD received it: the same
-- COALESCE the ORU page filters on and idx_oru_reports_coalesce_date indexes. The
-- study is the PACS study with the report's accession. On LAUMC the RIS report text
-- (report_source 'ris') and the listener's ORU share this table (fill-only merge).
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
           COALESCE(NULLIF(btrim(p.fallback_id), ''), NULLIF(btrim(p.id), '')) AS patient_id
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
    ('RIS exams per technologist, last month',
     'Exams each RIS technologist performed last month, with the median minutes from arrival to Exam Done (Report 35''s turnaround).', 'exams',
     '{"dataset":"exams","criteria":{"scheduled_at":{"mode":"quick","quick":"last_month"},"performed":{"value":"yes"}},"output":{"kind":"table","rows":"technologist","measures":["count","median_arrival_to_done"]}}'),
    ('Patient wait by modality, last 30 days',
     'Median minutes from arrival to scan start, per modality.', 'exams',
     '{"dataset":"exams","criteria":{"scheduled_at":{"mode":"quick","quick":"last_30_days"}},"output":{"kind":"table","rows":"modality","measures":["count","median_wait"]}}'),
    ('Scheduled today, not done yet',
     'Today''s RIS exams that are neither performed nor cancelled, by appointment time.', 'exams',
     '{"dataset":"exams","criteria":{"scheduled_at":{"mode":"quick","quick":"today"},"performed":{"value":"no"},"cancelled":{"value":"no"}},"output":{"kind":"list","columns":["scheduled_at","accession","patient_id","modality","procedure","exam_status","arrived_at","priority"],"sort":"scheduled_at","desc":false}}'),
    ('RIS report approval by radiologist, last month',
     'Exams approved per approving radiologist, with the median hours from Exam Done to approval.', 'exams',
     '{"dataset":"exams","criteria":{"scheduled_at":{"mode":"quick","quick":"last_month"},"ris_report":{"values":["Approved"]}},"output":{"kind":"table","rows":"approved_by","measures":["count","median_report_tat"]}}'),
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
    ('CRN acknowledgement',
     'Critical Result Notifications of the last 3 months by how they were acknowledged.', 'crn',
     '{"dataset":"crn","criteria":{"detected_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"table","rows":"acknowledged_how","measures":["count","median_ack_minutes"]}}')
  ) AS v(name, description, dataset, definition)
 WHERE NOT EXISTS (SELECT 1 FROM worklists w WHERE w.folder = 'Examples' AND w.name = v.name);

INSERT INTO worklist_versions (worklist_id, version, name, definition)
SELECT w.id, 1, w.name, w.definition FROM worklists w
 WHERE w.folder = 'Examples'
   AND NOT EXISTS (SELECT 1 FROM worklist_versions v WHERE v.worklist_id = w.id);


-- ── The AI assistant's navigation help ────────────────────────────────────────
-- Migration 0032 seeded a Custom Reports entry. Only that seeded row is rewritten: an
-- operator's own wording (Admin > AI Docs) is left alone.
UPDATE ai_doc_chunks
   SET section    = 'Dynamic Worklists',
       keywords   = 'worklist,dynamic worklist,custom report,ad hoc,custom,build report,custom query,sql',
       content    = 'Dynamic Worklists (sidebar: Analytics) replaced Custom Reports. Pick a dataset (RIS exams, PACS studies or series, reports, NLP findings, storage or CRN), add criteria on its tabs, and show it as a list, a table or a chart; save it, export it as CSV. Admins can share worklists and write their SQL by hand.',
       updated_at = NOW()
 WHERE section = 'Custom Reports'
   AND content = 'Custom Reports lets you build ad-hoc queries on study data by selecting fields, filters, and date ranges without writing SQL.';


-- ── The role worklists run as ─────────────────────────────────────────────────
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
