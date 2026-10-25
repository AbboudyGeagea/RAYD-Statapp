-- Migration 0136: Dynamic Worklists — RAYD V2.0's report editor, HL7 data layer.
--
-- Replaces the Custom Reports composer. Modelled on the Carestream RIS "Dynamic
-- Worklist Editor": a saved-worklist tree, criteria on tabs, date modes, pickers,
-- generated SQL an IT user can take over, and list / table / chart output.
--
--   rpt.exams / rpt.orders / rpt.reports   flat datasets the editor queries
--   worklist_shifts                        the shift hours the "shift" field uses
--   worklists, worklist_versions           saved definitions, every save kept
--   rpt_<database> login role              the ONLY identity worklists run as
--
-- THE rpt VIEWS READ THE HL7 TABLES DIRECTLY (ray7_study_state, hl7_study_events,
-- hl7_orders, hl7_oru_reports ...), an explicit exception to "reports read etl_*
-- only", decided when the editor was designed (2026-10-06): the technician of the
-- Started event, the open RAY7 notes and the order-to-exam chain exist nowhere in
-- etl_*. The 124 report queries are untouched; only this module reads the views.
--
-- THE ROLE IS THE SANDBOX. Worklist SQL — generated, or written by hand by an IT
-- user — never runs on the app's own connection. That connection logs in as the
-- table owner, a superuser in the stock postgres image, and from there SET ROLE (or
-- a plain SELECT set_config('role', ...)) switches straight back, so no in-session
-- privilege drop is a boundary. A separate LOGIN role is: it can read rpt.* and
-- nothing else, every transaction is read-only, every statement time-limited.
--
-- One role PER DATABASE (rpt_<database name>), because roles are cluster-wide and
-- its password is kept in this database's settings: a test database on the same
-- server must not rotate the live one's credentials.


CREATE SCHEMA IF NOT EXISTS rpt;


-- ── Shift hours ───────────────────────────────────────────────────────────────
-- An hour belongs to the first shift (sort_order) whose window holds it; a window
-- whose end is not after its start wraps midnight (23 -> 7 is the night).
CREATE TABLE IF NOT EXISTS worklist_shifts (
    name        TEXT    PRIMARY KEY,
    start_hour  INTEGER NOT NULL CHECK (start_hour BETWEEN 0 AND 23),
    end_hour    INTEGER NOT NULL CHECK (end_hour BETWEEN 0 AND 24),
    sort_order  INTEGER NOT NULL DEFAULT 0
);

INSERT INTO worklist_shifts (name, start_hour, end_hour, sort_order) VALUES
    ('AM', 7, 15, 1), ('PM', 15, 23, 2), ('NIGHT', 23, 7, 3)
ON CONFLICT (name) DO NOTHING;


-- ── rpt.exams — one row per study ─────────────────────────────────────────────
--
-- exam_at is the study's date anchor, the same chain the projector files
-- etl_didb_studies.study_date under (exam events first, then the report), so a
-- worklist and a report count a study on the same day. performed_at is when the
-- technologist had the patient (started, else completed): hour and shift use it.
-- Undated lifecycles (an order and nothing else) are orders, not exams.
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
    s.visit_number
FROM ray7_study_state s
LEFT JOIN hl7_oru_reports r ON r.accession_number = s.accession_number
CROSS JOIN LATERAL (
    SELECT COALESCE(s.completed_at, s.pacs_completed_at, s.started_at, s.arrived_at,
                    s.scheduled_at, s.signed_final_at, s.signed_prelim_at, s.reported_at) AS exam_at,
           COALESCE(s.started_at, s.completed_at, s.pacs_completed_at)                   AS performed_at,
           -- The same TAT end the projector writes to rep_final_timestamp.
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
-- The technologist is whoever the Started event names, else the Completed one.
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
    SELECT x.is_critical FROM hl7_oru_analysis x
     WHERE x.report_id = r.id
     ORDER BY x.analyzed_at DESC NULLS LAST, x.id DESC LIMIT 1
) a ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*) FILTER (WHERE rr.category = 'quality')  AS quality,
           COUNT(*) FILTER (WHERE rr.category = 'workflow') AS workflow
      FROM ray7_findings f
      JOIN ray7_rules rr ON rr.rule_code = f.rule_code AND rr.modality = ''
     WHERE f.accession_number = s.accession_number
       AND f.message_archive_id IS NULL AND f.resolved_at IS NULL
) n ON TRUE
WHERE NOT s.is_provisional
  AND t.exam_at IS NOT NULL
  AND COALESCE(m.modality, s.modality, '') <> 'SR';


-- ── rpt.orders — one row per HIS order received ───────────────────────────────
--
-- Matched to its lifecycle by accession when the order carried one, else by placer
-- number (a scheduled study first, then the provisional lifecycle of an order the
-- RIS has not scheduled yet).
CREATE OR REPLACE VIEW rpt.orders AS
SELECT
    o.id                                                       AS order_id,
    COALESCE(o.message_datetime, o.received_at)                AS ordered_at,
    COALESCE(o.message_datetime, o.received_at)::date          AS order_date,
    o.placer_order_number,
    COALESCE(o.accession_number,
             CASE WHEN NOT st.is_provisional THEN st.accession_number END) AS accession,
    o.patient_id,
    o.patient_class,
    CASE upper(o.patient_class) WHEN 'I' THEN 'Inpatient' WHEN 'O' THEN 'Outpatient'
         WHEN 'E' THEN 'Emergency' ELSE o.patient_class END    AS patient_type,
    o.patient_location,
    o.procedure_code,
    COALESCE(o.procedure_text, st.procedure_text)              AS procedure,
    COALESCE(m.modality, st.modality, o.modality)              AS modality,
    si.name                                                    AS site,
    NULLIF(btrim(concat_ws(' ', o.referring_physician_first_name,
                                o.referring_physician_last_name)), '') AS referring,
    o.ordering_physician,
    CASE WHEN st.cancelled_at     IS NOT NULL THEN 'Cancelled'
         WHEN st.signed_final_at  IS NOT NULL OR st.signed_prelim_at IS NOT NULL
              OR st.reported_at   IS NOT NULL THEN 'Reported'
         WHEN st.completed_at IS NOT NULL OR st.pacs_completed_at IS NOT NULL THEN 'Completed'
         WHEN st.started_at       IS NOT NULL THEN 'Started'
         WHEN st.arrived_at       IS NOT NULL THEN 'Arrived'
         WHEN st.scheduled_at     IS NOT NULL THEN 'Scheduled'
         ELSE 'Not scheduled' END                              AS status,
    st.scheduled_at,
    COALESCE(st.completed_at, st.pacs_completed_at, st.started_at) AS performed_at,
    (COALESCE(st.scheduled_at, st.arrived_at, st.started_at,
              st.completed_at, st.pacs_completed_at) IS NOT NULL)  AS scheduled,
    (COALESCE(st.completed_at, st.pacs_completed_at, st.started_at) IS NOT NULL) AS performed,
    (COALESCE(st.reported_at, st.signed_prelim_at, st.signed_final_at) IS NOT NULL) AS reported,
    (st.cancelled_at IS NOT NULL)                              AS cancelled,
    round(extract(epoch FROM (st.scheduled_at - COALESCE(o.message_datetime, o.received_at)))
          / 3600.0, 2)                                         AS order_to_schedule_hours,
    round(extract(epoch FROM (COALESCE(st.completed_at, st.pacs_completed_at)
                              - COALESCE(o.message_datetime, o.received_at))) / 3600.0, 2)
                                                               AS order_to_exam_hours
FROM hl7_orders o
LEFT JOIN LATERAL (
    SELECT x.* FROM (
        SELECT s.*, 0 AS pri FROM ray7_study_state s
         WHERE s.accession_number = o.accession_number
        UNION ALL
        SELECT s.*, 1 FROM ray7_study_state s
         WHERE s.placer_order_number = o.placer_order_number
    ) x
    ORDER BY x.pri, x.is_provisional, x.first_seen_at DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.site_id FROM aetitle_modality_map mm
     WHERE upper(mm.aetitle) = upper(st.aetitle)
     ORDER BY mm.id LIMIT 1
) m ON TRUE
LEFT JOIN sites si ON si.id = m.site_id
WHERE COALESCE(m.modality, st.modality, o.modality, '') <> 'SR';


-- ── rpt.reports — one row per report ──────────────────────────────────────────
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
                                r.referring_physician_last_name)), '') AS referring
FROM hl7_oru_reports r
LEFT JOIN ray7_study_state s ON s.accession_number = r.accession_number
LEFT JOIN LATERAL (
    SELECT mm.modality, mm.site_id FROM aetitle_modality_map mm
     WHERE upper(mm.aetitle) = upper(s.aetitle)
     ORDER BY mm.id LIMIT 1
) m ON TRUE
LEFT JOIN sites si ON si.id = m.site_id
LEFT JOIN LATERAL (
    SELECT x.is_critical FROM hl7_oru_analysis x
     WHERE x.report_id = r.id
     ORDER BY x.analyzed_at DESC NULLS LAST, x.id DESC LIMIT 1
) a ON TRUE
WHERE COALESCE(m.modality, s.modality, r.modality, '') <> 'SR';


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
-- Shared examples, the questions the editor was designed around. Ownerless, so any
-- author may adapt them; tests/test_worklists.py runs every one.
INSERT INTO worklists (name, description, folder, visibility, dataset, definition)
SELECT v.name, v.description, 'Examples', 'shared', v.dataset, v.definition::jsonb
  FROM (VALUES
    ('Exams per modality, by month',
     'Volume per modality over the last three complete months.', 'exams',
     '{"dataset":"exams","criteria":{"exam_at":{"mode":"quick","quick":"last_3_months"}},"output":{"kind":"chart","rows":"exam_at:month","cols":"modality","measures":["count"]}}'),
    ('Technicians: AM shift, first week of the month',
     'Exams per technician on the AM shift during the first week of each month.', 'exams',
     '{"dataset":"exams","criteria":{"exam_at":{"mode":"quick","quick":"last_3_months"},"shift":{"values":["AM"]},"week_of_month":{"values":[1]}},"output":{"kind":"table","rows":"technician","cols":"exam_at:month","measures":["count"]}}'),
    ('Report turnaround by radiologist',
     'Median hours from exam completion to final signature, last month.', 'exams',
     '{"dataset":"exams","criteria":{"exam_at":{"mode":"quick","quick":"last_month"},"has_report":{"value":"yes"}},"output":{"kind":"table","rows":"radiologist","measures":["count","median_report_tat"]}}'),
    ('Orders not performed',
     'HIS orders from the last 30 days that never reached the exam.', 'orders',
     '{"dataset":"orders","criteria":{"ordered_at":{"mode":"quick","quick":"last_30_days"},"performed":{"value":"no"},"cancelled":{"value":"no"}},"output":{"kind":"list","columns":["ordered_at","placer_order_number","patient_id","procedure","modality","referring","status"],"sort":"ordered_at","desc":true}}'),
    ('Emergency exams today',
     'Every ER exam since midnight, newest first.', 'exams',
     '{"dataset":"exams","criteria":{"exam_at":{"mode":"quick","quick":"today"},"patient_type":{"values":["Emergency"]}},"output":{"kind":"list","columns":["exam_at","accession","patient_id","modality","procedure","device","status","technician"],"sort":"exam_at","desc":true}}'),
    ('Exams with open data-quality notes',
     'Studies RAY7 has an open quality note on — contradictory times, skipped rungs, no order.', 'exams',
     '{"dataset":"exams","criteria":{"open_quality_notes":{"min":1}},"output":{"kind":"list","columns":["exam_at","accession","modality","status","open_quality_notes"],"sort":"exam_at","desc":true}}')
  ) AS v(name, description, dataset, definition)
 WHERE NOT EXISTS (SELECT 1 FROM worklists w WHERE w.folder = 'Examples' AND w.name = v.name);

INSERT INTO worklist_versions (worklist_id, version, name, definition)
SELECT w.id, 1, w.name, w.definition FROM worklists w
 WHERE w.folder = 'Examples'
   AND NOT EXISTS (SELECT 1 FROM worklist_versions v WHERE v.worklist_id = w.id);


-- ── The role worklists run as ─────────────────────────────────────────────────
DO $wl$
DECLARE
    r text := 'rpt_' || regexp_replace(lower(current_database()), '[^a-z0-9_]', '_', 'g');
    p text := md5(random()::text || clock_timestamp()::text) || md5(random()::text);
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB '
                       'NOCREATEROLE NOINHERIT NOREPLICATION', r, p);
    ELSE
        EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', r, p);
    END IF;

    EXECUTE format('ALTER ROLE %I SET default_transaction_read_only = on', r);
    EXECUTE format('ALTER ROLE %I SET statement_timeout = %L', r, '30s');
    -- So an IT user can write FROM exams rather than FROM rpt.exams.
    EXECUTE format('ALTER ROLE %I SET search_path = rpt', r);

    EXECUTE format('GRANT USAGE ON SCHEMA rpt TO %I', r);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA rpt TO %I', r);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA rpt GRANT SELECT ON TABLES TO %I', r);

    INSERT INTO settings (key, value) VALUES ('worklist_db_role', r)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    INSERT INTO settings (key, value) VALUES ('worklist_db_password', p)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
END
$wl$;
