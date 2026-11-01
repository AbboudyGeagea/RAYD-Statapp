-- Migration 0139: NLP findings in Dynamic Worklists (from Bekaa 5d06f2cc).
--
--   rpt.findings              one row per finding the NLP worker affirmed in a report
--                             (negation and history removed, benign labels left out):
--                             "most frequent findings by modality", "every pneumothorax
--                             last month"
--   rpt.exams.findings        the report's findings, comma-separated
--   rpt.reports.findings      the same, per report
--
-- rpt.exams and rpt.reports are restated from 0136 with 0137's table names
-- (ray7_* -> brayn_*) and one column added at the end: CREATE OR REPLACE VIEW keeps
-- every existing column, so nothing that reads them changes.


CREATE OR REPLACE VIEW rpt.exams AS
SELECT
    s.accession_number                                         AS accession,
    s.patient_id,
    p.sex,
    age.years                                                  AS age,
    CASE WHEN age.years IS NULL THEN NULL
         WHEN age.years < 18 THEN '0-17'
         WHEN age.years < 40 THEN '18-39'
         WHEN age.years < 65 THEN '40-64'
         ELSE '65+' END                                        AS age_band,
    s.patient_class,
    CASE upper(s.patient_class) WHEN 'I' THEN 'Inpatient' WHEN 'O' THEN 'Outpatient'
         WHEN 'E' THEN 'Emergency' ELSE s.patient_class END    AS patient_type,
    s.patient_location,
    COALESCE(m.modality, s.modality)                           AS modality,
    s.aetitle                                                  AS device,
    COALESCE(s.room_name, m.room_name)                         AS room,
    si.name                                                    AS site,
    s.procedure_code,
    s.procedure_text                                           AS procedure,
    CASE WHEN s.cancelled_at     IS NOT NULL THEN 'Cancelled'
         WHEN s.signed_final_at  IS NOT NULL THEN 'Signed final'
         WHEN s.signed_prelim_at IS NOT NULL THEN 'Signed preliminary'
         WHEN s.reported_at      IS NOT NULL THEN 'Reported'
         WHEN s.completed_at IS NOT NULL OR s.pacs_completed_at IS NOT NULL THEN 'Completed'
         WHEN s.started_at       IS NOT NULL THEN 'Started'
         WHEN s.arrived_at       IS NOT NULL THEN 'Arrived'
         WHEN s.scheduled_at     IS NOT NULL THEN 'Scheduled'
         ELSE 'Ordered' END                                    AS status,
    s.ordered_at, s.scheduled_at, s.arrived_at, s.started_at, s.completed_at,
    s.pacs_completed_at                                        AS images_at,
    s.signed_prelim_at                                         AS prelim_signed_at,
    s.signed_final_at                                          AS final_signed_at,
    t.final_at                                                 AS report_final_at,
    s.cancelled_at,
    t.exam_at,
    t.exam_at::date                                            AS exam_date,
    t.performed_at,
    sh.name                                                    AS shift,
    (extract(day FROM t.exam_at)::int - 1) / 7 + 1             AS week_of_month,
    extract(isodow FROM t.exam_at)::int                        AS weekday,
    extract(hour FROM t.performed_at)::int                     AS hour,
    tech.who                                                   AS technician,
    r.physician_id                                             AS radiologist,
    NULLIF(btrim(concat_ws(' ', d.referring_physician_first_name,
                                d.referring_physician_last_name)), '') AS referring,
    (r.id IS NOT NULL OR s.reported_at IS NOT NULL)            AS has_report,
    COALESCE(a.is_critical, FALSE)                             AS critical,
    COALESCE(n.quality, 0)                                     AS open_quality_notes,
    COALESCE(n.workflow, 0) > 0                                AS stalled,
    round(extract(epoch FROM (s.started_at - s.arrived_at)) / 60.0, 1)     AS wait_minutes,
    round(extract(epoch FROM (s.completed_at - s.started_at)) / 60.0, 1)   AS exam_minutes,
    round(extract(epoch FROM (t.final_at - COALESCE(s.completed_at, s.pacs_completed_at)))
          / 3600.0, 2)                                         AS report_tat_hours,
    s.placer_order_number,
    s.visit_number,
    NULLIF(array_to_string(a.affirmed_labels, ', '), '')       AS findings
FROM brayn_study_state s
LEFT JOIN hl7_oru_reports r ON r.accession_number = s.accession_number
CROSS JOIN LATERAL (
    SELECT COALESCE(s.completed_at, s.pacs_completed_at, s.started_at, s.arrived_at,
                    s.scheduled_at, s.signed_final_at, s.signed_prelim_at, s.reported_at) AS exam_at,
           COALESCE(s.started_at, s.completed_at, s.pacs_completed_at)                   AS performed_at,
           COALESCE(s.signed_final_at,
                    CASE WHEN s.signed_prelim_at IS NULL
                         THEN COALESCE(s.reported_at, r.result_datetime) END)            AS final_at
) t
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.room_name, mm.site_id
      FROM aetitle_modality_map mm
     WHERE upper(mm.aetitle) = upper(s.aetitle)
     ORDER BY mm.id LIMIT 1
) m ON TRUE
LEFT JOIN sites si ON si.id = m.site_id
LEFT JOIN hl7_patients p ON p.patient_id = s.patient_id
LEFT JOIN etl_didb_studies d ON d.accession_number = s.accession_number
LEFT JOIN LATERAL (
    SELECT CASE WHEN p.birth_date IS NOT NULL AND t.exam_at IS NOT NULL
                THEN date_part('year', age(t.exam_at::date, p.birth_date))::int END AS years
) age ON TRUE
LEFT JOIN LATERAL (
    SELECT w.name FROM worklist_shifts w
     WHERE CASE WHEN w.start_hour < w.end_hour
                THEN extract(hour FROM t.performed_at) >= w.start_hour
                 AND extract(hour FROM t.performed_at) <  w.end_hour
                ELSE extract(hour FROM t.performed_at) >= w.start_hour
                  OR extract(hour FROM t.performed_at) <  w.end_hour END
     ORDER BY w.sort_order LIMIT 1
) sh ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(e.performed_by_name, e.performed_by_id) AS who
      FROM hl7_study_events e
     WHERE e.accession_number = s.accession_number
       AND e.canonical_state IN ('started', 'completed')
       AND COALESCE(e.performed_by_name, e.performed_by_id) IS NOT NULL
     ORDER BY (e.canonical_state = 'started') DESC, e.event_time
     LIMIT 1
) tech ON TRUE
LEFT JOIN LATERAL (
    SELECT x.is_critical, x.affirmed_labels FROM hl7_oru_analysis x
     WHERE x.report_id = r.id
     ORDER BY x.analyzed_at DESC NULLS LAST, x.id DESC LIMIT 1
) a ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*) FILTER (WHERE rr.category = 'quality')  AS quality,
           COUNT(*) FILTER (WHERE rr.category = 'workflow') AS workflow
      FROM brayn_findings f
      JOIN brayn_rules rr ON rr.rule_code = f.rule_code AND rr.modality = ''
     WHERE f.accession_number = s.accession_number
       AND f.message_archive_id IS NULL AND f.resolved_at IS NULL
) n ON TRUE
WHERE NOT s.is_provisional
  AND t.exam_at IS NOT NULL
  AND COALESCE(m.modality, s.modality, '') <> 'SR';


CREATE OR REPLACE VIEW rpt.reports AS
SELECT
    r.id                                                       AS report_id,
    r.accession_number                                         AS accession,
    COALESCE(r.patient_id, s.patient_id)                       AS patient_id,
    r.physician_id                                             AS radiologist,
    COALESCE(m.modality, s.modality, r.modality)               AS modality,
    COALESCE(s.procedure_text, r.procedure_name)               AS procedure,
    s.aetitle                                                  AS device,
    si.name                                                    AS site,
    s.patient_class,
    CASE upper(s.patient_class) WHEN 'I' THEN 'Inpatient' WHEN 'O' THEN 'Outpatient'
         WHEN 'E' THEN 'Emergency' ELSE s.patient_class END    AS patient_type,
    r.result_datetime                                          AS report_at,
    r.result_datetime::date                                    AS report_date,
    CASE WHEN s.signed_final_at  IS NOT NULL THEN 'Final'
         WHEN s.signed_prelim_at IS NOT NULL THEN 'Preliminary'
         ELSE 'Unspecified' END                                AS signature,
    s.signed_prelim_at                                         AS prelim_signed_at,
    s.signed_final_at                                          AS final_signed_at,
    COALESCE(s.completed_at, s.pacs_completed_at)              AS completed_at,
    round(extract(epoch FROM (COALESCE(s.signed_final_at,
                                       CASE WHEN s.signed_prelim_at IS NULL
                                            THEN COALESCE(s.reported_at, r.result_datetime) END)
                              - COALESCE(s.completed_at, s.pacs_completed_at))) / 3600.0, 2)
                                                               AS report_tat_hours,
    COALESCE(a.is_critical, FALSE)                             AS critical,
    NULLIF(btrim(concat_ws(' ', r.referring_physician_first_name,
                                r.referring_physician_last_name)), '') AS referring,
    NULLIF(array_to_string(a.affirmed_labels, ', '), '')       AS findings
FROM hl7_oru_reports r
LEFT JOIN brayn_study_state s ON s.accession_number = r.accession_number
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.site_id FROM aetitle_modality_map mm
     WHERE upper(mm.aetitle) = upper(s.aetitle)
     ORDER BY mm.id LIMIT 1
) m ON TRUE
LEFT JOIN sites si ON si.id = m.site_id
LEFT JOIN LATERAL (
    SELECT x.is_critical, x.affirmed_labels FROM hl7_oru_analysis x
     WHERE x.report_id = r.id
     ORDER BY x.analyzed_at DESC NULLS LAST, x.id DESC LIMIT 1
) a ON TRUE
WHERE COALESCE(m.modality, s.modality, r.modality, '') <> 'SR';


-- ── rpt.findings — one row per NLP finding of a report ────────────────────────
CREATE OR REPLACE VIEW rpt.findings AS
SELECT
    rr.report_id, rr.accession, rr.patient_id, rr.radiologist, rr.modality, rr.procedure,
    rr.device, rr.site, rr.patient_type, rr.report_at, rr.report_date,
    f.label                                                    AS finding
FROM hl7_oru_analysis a
CROSS JOIN LATERAL unnest(a.affirmed_labels) AS f(label)
JOIN rpt.reports rr ON rr.report_id = a.report_id;


-- The worklist role reads it like the other views (0136's default privileges cover a
-- view created by the same owner; said explicitly in case another role created it).
DO $$
DECLARE r text;
BEGIN
    SELECT value INTO r FROM settings WHERE key = 'worklist_db_role';
    IF r IS NOT NULL AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
        EXECUTE format('GRANT SELECT ON rpt.findings TO %I', r);
    END IF;
END $$;


-- ── One more shared example ───────────────────────────────────────────────────
INSERT INTO worklists (name, description, folder, visibility, dataset, definition)
SELECT 'Most frequent findings by modality',
       'NLP findings counted per modality over the last three complete months.',
       'Examples', 'shared', 'findings',
       '{"dataset":"findings","criteria":{"report_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"table","rows":"finding","cols":"modality","measures":["count"]}}'::jsonb
 WHERE NOT EXISTS (SELECT 1 FROM worklists w
                    WHERE w.folder = 'Examples' AND w.name = 'Most frequent findings by modality');

INSERT INTO worklist_versions (worklist_id, version, name, definition)
SELECT w.id, 1, w.name, w.definition FROM worklists w
 WHERE w.folder = 'Examples'
   AND NOT EXISTS (SELECT 1 FROM worklist_versions v WHERE v.worklist_id = w.id);
