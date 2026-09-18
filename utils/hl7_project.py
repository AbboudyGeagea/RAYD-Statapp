"""
utils/hl7_project.py
────────────────────────────────────────────────────────────────
The projector: HL7-derived state into the tables the reports already read.

    ray7_study_state  ─┐
    hl7_patients       ├─▶  etl_didb_studies
    hl7_oru_reports    │    etl_patient_view
    hl7_orders        ─┘    etl_orders
                                  │
                                  ▼
                     124 report queries, UNCHANGED

This is the piece that makes anything appear on a dashboard. Everything before it
fills HL7-shaped tables no report has ever heard of.

PER-ACCESSION, NOT A REBUILD. Each call projects one study, driven by the message
that just arrived. A nightly full rebuild would be simpler to write and wrong for
this feed: the reports are expected to be live, and rebuilding a table that 124
queries read while they read it is a lock problem nobody needs.

IDEMPOTENT BY CONSTRUCTION. Every write is an upsert keyed on the surrogate ID,
which is stable for a given accession (see migration 0116). So projecting the same
study after every event — which is exactly what happens — converges rather than
duplicating, and a replay of the whole archive produces the same rows as the
original live run.


THE TAT ANCHOR, which is the most consequential line in this module
───────────────────────────────────────────────────────────────────
etl_didb_studies.insert_time is set to the COMPLETED timestamp.

On the Oracle branches insert_time meant "when the PACS ingested the images", and
report 25 computes turnaround as `rep_final_timestamp - insert_time`. That was
always a proxy: PACS ingestion is not when the exam finished, merely the closest
observable event, and the better anchors (std_pps.end_datetime, worklist status
100) were LAUMC-only so the shared report could not use them.

Here the RIS tells us outright when the exam completed. Putting that value in
insert_time hands every existing report the anchor the cutover document actually
asked for — "turnaround time start point moves from PACS ingest time to scan
time" — without editing one report query. The column name is now a misnomer, which
is a fair price for not touching 124 queries to rename it.

Falls back down the ladder (started, arrived, scheduled) so a study that never
reported a completion still gets a defensible anchor rather than a NULL that
silently drops it out of every TAT calculation.
"""
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("HL7_PROJECT")


# ── etl_didb_studies ──────────────────────────────────────────────────────────
#
# On patient_db_uid: the column is NOT NULL, and a study can legitimately reach us
# with no patient identifier. Rather than collapsing every such study onto a single
# sentinel patient — which would inflate per-patient counts and quietly corrupt
# "studies per patient" on every report that computes it — each gets its own
# synthetic patient keyed on its accession. Wrong in an obvious, countable way
# instead of wrong in an invisible one.
#
# patient_location is VARCHAR(3) in this schema, so it is truncated explicitly.
# Letting Postgres raise on a longer value would fail the whole projection over a
# display field.
_STUDY_SQL = """
INSERT INTO etl_didb_studies (
    study_db_uid, patient_db_uid, accession_number, storing_ae,
    study_date, study_time, insert_time, last_update,
    study_description, procedure_code, study_modality,
    patient_class, patient_location, order_status, study_status,
    study_has_report, rep_final_timestamp, rep_final_signed_by,
    reading_physician_id, age_at_exam, is_linked_study
)
SELECT
    hl7_surrogate_id('study', st.accession_number),
    COALESCE(
        hl7_surrogate_id('patient', st.patient_id),
        hl7_surrogate_id('patient', 'NOPATIENT:' || st.accession_number)
    ),
    st.accession_number,
    st.aetitle,
    COALESCE(st.completed_at, st.started_at, st.arrived_at, st.scheduled_at)::date,
    to_char(COALESCE(st.completed_at, st.started_at, st.arrived_at, st.scheduled_at),
            'HH24:MI:SS'),
    -- See the module docstring: this is the exam-completion time, not an ingest
    -- time, and that is deliberate.
    COALESCE(st.completed_at, st.started_at, st.arrived_at, st.scheduled_at),
    NOW(),
    st.procedure_text,
    st.procedure_code,
    st.modality,
    st.patient_class,
    LEFT(st.patient_location, 3),
    CASE WHEN st.cancelled_at IS NOT NULL THEN 'CA'
         WHEN st.completed_at IS NOT NULL THEN 'CM'
         WHEN st.started_at   IS NOT NULL THEN 'IP'
         WHEN st.arrived_at   IS NOT NULL THEN 'AR'
         WHEN st.scheduled_at IS NOT NULL THEN 'SC'
         ELSE NULL END,
    CASE WHEN st.cancelled_at IS NOT NULL THEN 'cancelled'
         WHEN st.is_closed THEN 'closed' ELSE 'open' END,
    (r.result_datetime IS NOT NULL),
    r.result_datetime,
    r.physician_id,
    r.physician_id,
    -- Fractional years, matching the NUMERIC(5,2) the column declares. AGE() would
    -- give an interval that still needs converting, and dividing days by 365.25
    -- keeps leap years from accumulating a visible error on paediatric ages.
    CASE WHEN p.birth_date IS NOT NULL
              AND COALESCE(st.completed_at, st.started_at, st.arrived_at, st.scheduled_at) IS NOT NULL
         THEN ROUND(
              (COALESCE(st.completed_at, st.started_at, st.arrived_at, st.scheduled_at)::date
               - p.birth_date)::numeric / 365.25, 2)
         END,
    FALSE
FROM ray7_study_state st
LEFT JOIN hl7_patients p ON p.patient_id = st.patient_id
LEFT JOIN LATERAL (
    SELECT o.result_datetime, o.physician_id
      FROM hl7_oru_reports o
     WHERE o.accession_number = st.accession_number
     ORDER BY o.result_datetime DESC NULLS LAST, o.id DESC
     LIMIT 1
) r ON TRUE
WHERE st.accession_number = :acc
ON CONFLICT (study_db_uid) DO UPDATE SET
    patient_db_uid       = EXCLUDED.patient_db_uid,
    accession_number     = EXCLUDED.accession_number,
    -- COALESCE(EXCLUDED, existing) everywhere a later message might carry less
    -- than an earlier one did: the projection runs after every event, and a
    -- Completed that omits the AE title must not erase what Started reported.
    storing_ae           = COALESCE(EXCLUDED.storing_ae,        etl_didb_studies.storing_ae),
    study_date           = COALESCE(EXCLUDED.study_date,        etl_didb_studies.study_date),
    study_time           = COALESCE(EXCLUDED.study_time,        etl_didb_studies.study_time),
    insert_time          = COALESCE(EXCLUDED.insert_time,       etl_didb_studies.insert_time),
    study_description    = COALESCE(EXCLUDED.study_description, etl_didb_studies.study_description),
    procedure_code       = COALESCE(EXCLUDED.procedure_code,    etl_didb_studies.procedure_code),
    study_modality       = COALESCE(EXCLUDED.study_modality,    etl_didb_studies.study_modality),
    patient_class        = COALESCE(EXCLUDED.patient_class,     etl_didb_studies.patient_class),
    patient_location     = COALESCE(EXCLUDED.patient_location,  etl_didb_studies.patient_location),
    order_status         = COALESCE(EXCLUDED.order_status,      etl_didb_studies.order_status),
    study_status         = COALESCE(EXCLUDED.study_status,      etl_didb_studies.study_status),
    -- Never un-report a study: study_has_report only ever goes false -> true.
    study_has_report     = etl_didb_studies.study_has_report OR EXCLUDED.study_has_report,
    rep_final_timestamp  = COALESCE(EXCLUDED.rep_final_timestamp,  etl_didb_studies.rep_final_timestamp),
    rep_final_signed_by  = COALESCE(EXCLUDED.rep_final_signed_by,  etl_didb_studies.rep_final_signed_by),
    reading_physician_id = COALESCE(EXCLUDED.reading_physician_id, etl_didb_studies.reading_physician_id),
    age_at_exam          = COALESCE(EXCLUDED.age_at_exam,       etl_didb_studies.age_at_exam),
    last_update          = NOW()
"""

_PATIENT_SQL = """
INSERT INTO etl_patient_view (
    patient_db_uid, id, birth_date, sex, gender, last_update,
    number_of_patient_studies
)
SELECT
    hl7_surrogate_id('patient', p.patient_id),
    p.patient_id,
    p.birth_date,
    p.sex,
    p.sex,
    NOW(),
    (SELECT count(*) FROM ray7_study_state s WHERE s.patient_id = p.patient_id)
FROM hl7_patients p
WHERE p.patient_id = :pid
ON CONFLICT (patient_db_uid) DO UPDATE SET
    id                        = COALESCE(EXCLUDED.id,         etl_patient_view.id),
    birth_date                = COALESCE(EXCLUDED.birth_date, etl_patient_view.birth_date),
    sex                       = COALESCE(EXCLUDED.sex,        etl_patient_view.sex),
    gender                    = COALESCE(EXCLUDED.gender,     etl_patient_view.gender),
    number_of_patient_studies = EXCLUDED.number_of_patient_studies,
    last_update               = NOW()
"""

# etl_orders is keyed on the PLACER order number where one exists, because that is
# the identity the HIS owns and reuses across the order's life. Falling back to the
# accession keeps RIS-originated studies that never carried a placer number
# representable rather than dropping them.
_ORDER_SQL = """
INSERT INTO etl_orders (
    order_dbid, patient_dbid, study_db_uid, accession_number,
    proc_id, proc_text, scheduled_datetime, order_status, modality,
    has_study, last_update
)
SELECT
    hl7_surrogate_id('order', COALESCE(st.placer_order_number, st.accession_number)),
    st.patient_id,
    hl7_surrogate_id('study', st.accession_number),
    st.accession_number,
    st.procedure_code,
    st.procedure_text,
    st.scheduled_at,
    CASE WHEN st.cancelled_at IS NOT NULL THEN 'CA'
         WHEN st.completed_at IS NOT NULL THEN 'CM'
         WHEN st.started_at   IS NOT NULL THEN 'IP'
         WHEN st.arrived_at   IS NOT NULL THEN 'AR'
         WHEN st.scheduled_at IS NOT NULL THEN 'SC'
         ELSE NULL END,
    st.modality,
    TRUE,
    NOW()
FROM ray7_study_state st
WHERE st.accession_number = :acc
ON CONFLICT (order_dbid) DO UPDATE SET
    patient_dbid       = COALESCE(EXCLUDED.patient_dbid,       etl_orders.patient_dbid),
    study_db_uid       = COALESCE(EXCLUDED.study_db_uid,       etl_orders.study_db_uid),
    accession_number   = COALESCE(EXCLUDED.accession_number,   etl_orders.accession_number),
    proc_id            = COALESCE(EXCLUDED.proc_id,            etl_orders.proc_id),
    proc_text          = COALESCE(EXCLUDED.proc_text,          etl_orders.proc_text),
    scheduled_datetime = COALESCE(EXCLUDED.scheduled_datetime, etl_orders.scheduled_datetime),
    order_status       = COALESCE(EXCLUDED.order_status,       etl_orders.order_status),
    modality           = COALESCE(EXCLUDED.modality,           etl_orders.modality),
    has_study          = TRUE,
    last_update        = NOW()
"""


def project_study(accession_number):
    """
    Project one accession into etl_didb_studies and etl_orders.

    Each statement gets its own SAVEPOINT, same reasoning as everywhere else on
    this path: a projection failure must cost the dashboard row, never the archived
    message or the lifecycle event it was derived from. Those are recoverable by
    replay precisely because they are written first and independently.
    """
    if not accession_number:
        return []
    done = []
    for label, sql in (('study', _STUDY_SQL), ('order', _ORDER_SQL)):
        try:
            with db.session.begin_nested():
                db.session.execute(text(sql), {'acc': accession_number})
            done.append('etl_' + label)
        except Exception:
            logger.exception("projector: %s upsert failed | acc=%s", label, accession_number)
    return done


def project_patient(patient_id):
    """Project one patient into etl_patient_view."""
    if not patient_id:
        return []
    try:
        with db.session.begin_nested():
            db.session.execute(text(_PATIENT_SQL), {'pid': patient_id})
        return ['etl_patient']
    except Exception:
        logger.exception("projector: patient upsert failed | pid=%s", patient_id)
        return []


# ── The RIS worklist tables ───────────────────────────────────────────────────
#
# Reports 35 and 36 (technician TAT), and parts of 25, 33 and 34, read the RIS
# worklist tables rather than etl_didb_studies. On the Oracle branches those were
# filled by ETL phases 14 and 17 from WORKLIST_STATUS_HISTORY and PPS. With Oracle
# gone they have no source — which is why those reports were on the "blank until
# further notice" list.
#
# They do not need importing. The lifecycle events already carry arrived, started
# and completed with the performing device, which is precisely what those tables
# hold; the data has been arriving all along, just not in the shape the reports
# read. So this projects it rather than asking a site to supply what it has
# already sent.
#
# WHAT THIS RECOVERS AND WHAT IT DOES NOT. Arrived -> exam-done turnaround comes
# back in full. The technologist NAME does not: report 35 resolves that through
# std_pps_person_reference joined to std_resources_ris.role_code = 'TEC', and the
# staff roster is import-only master data. So the timings return now and the names
# return when the roster is loaded — the event does carry performed_by_id, so
# nothing is lost in the meantime, it is simply unresolved.

_WORKLIST_ARRIVAL_SQL = """
INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, arrived_at, sps_id, last_update)
SELECT hl7_surrogate_id('worklist', s.accession_number),
       hl7_surrogate_id('pps', s.accession_number),
       s.arrived_at,
       -- sps_id IS the accession at this site, per the RIS documentation, so the
       -- reports' accession COALESCE chain resolves without the 'WL#' fallback.
       s.accession_number,
       NOW()
FROM ray7_study_state s
WHERE s.accession_number = :acc AND s.arrived_at IS NOT NULL
ON CONFLICT (site_worklist_key, arrived_at) DO NOTHING
"""

# No natural unique constraint here, so re-projection is delete-then-insert rather
# than an upsert. Scoped to the one study, and the projector runs after every
# event for that study, so without this a four-event lifecycle would leave four
# identical exam-done rows and double-count every completion.
_WORKLIST_DONE_SQL = """
WITH gone AS (
    DELETE FROM std_worklist_exam_done
     WHERE site_worklist_key = hl7_surrogate_id('worklist', :acc)
)
INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at, last_update)
SELECT hl7_surrogate_id('worklist', s.accession_number),
       hl7_surrogate_id('pps', s.accession_number),
       s.completed_at,
       NOW()
FROM ray7_study_state s
WHERE s.accession_number = :acc AND s.completed_at IS NOT NULL
"""

_PPS_SQL = """
INSERT INTO std_pps (pps_key, study_db_uid, procedure_code, procedure_name,
                     start_datetime, end_datetime, performing_ae_title)
SELECT hl7_surrogate_id('pps', s.accession_number),
       hl7_surrogate_id('study', s.accession_number),
       s.procedure_code, s.procedure_text,
       -- The MPPS equivalents: the exam ran from started to completed.
       s.started_at, s.completed_at, s.aetitle
FROM ray7_study_state s
WHERE s.accession_number = :acc
  AND (s.started_at IS NOT NULL OR s.completed_at IS NOT NULL)
ON CONFLICT (pps_key) DO UPDATE SET
    study_db_uid        = COALESCE(EXCLUDED.study_db_uid,        std_pps.study_db_uid),
    procedure_code      = COALESCE(EXCLUDED.procedure_code,      std_pps.procedure_code),
    procedure_name      = COALESCE(EXCLUDED.procedure_name,      std_pps.procedure_name),
    start_datetime      = COALESCE(EXCLUDED.start_datetime,      std_pps.start_datetime),
    end_datetime        = COALESCE(EXCLUDED.end_datetime,        std_pps.end_datetime),
    performing_ae_title = COALESCE(EXCLUDED.performing_ae_title, std_pps.performing_ae_title)
"""


# Who performed this study, in the shape report 35 resolves names through.
#
# The report does not read a performer off the study. It goes
# std_pps_person_reference -> std_resources_ris and filters role_code = 'TEC',
# because the obvious column on std_pps was confirmed 100% NULL at this RIS and
# person_reference_type_key mixes technologists with receptionists and nurses.
# That indirection is preserved rather than short-circuited, so the report works
# unmodified and an Oracle-sourced install and an HL7 one resolve identically.
#
# Only performers already in the roster are linked. An unknown ID is not invented
# as a person: it stays unresolved, and RAY7's UNKNOWN_PERFORMER is the place
# that gets reported. Inserting a placeholder would put a fictional member of
# staff into the technician reports.
_PERSON_REF_SQL = """
WITH gone AS (
    DELETE FROM std_pps_person_reference
     WHERE pps_key = hl7_surrogate_id('pps', :acc)
)
INSERT INTO std_pps_person_reference
    (pps_person_reference_key, pps_key, resource_id_key, display_sort_order, last_update)
SELECT
       -- NOT NULL with no default or sequence: it is the RIS's own key, so an
       -- HL7 install mints it like every other foreign identity. Keyed on the
       -- pair, so re-projecting the same study reuses the same row identity.
       hl7_surrogate_id('pps_ref', :acc || ':' || r.resource_id_key),
       hl7_surrogate_id('pps', :acc),
       r.resource_id_key,
       -- Earliest rung first, so report 35's "lowest display_sort_order is the
       -- primary" tie-break picks whoever actually ran the exam rather than
       -- whoever last touched it.
       MIN(e.ladder_rank), NOW()
  FROM hl7_study_events e
  JOIN std_resources_ris r
    ON lower(r.resource_id) = lower(e.performed_by_id)
 WHERE e.accession_number = :acc
   AND e.performed_by_id IS NOT NULL
 GROUP BY r.resource_id_key
"""


def project_worklist(accession_number):
    """Project the lifecycle into the RIS-shaped worklist tables."""
    if not accession_number:
        return []
    done = []
    for label, sql in (('arrival', _WORKLIST_ARRIVAL_SQL),
                       ('exam_done', _WORKLIST_DONE_SQL),
                       ('pps', _PPS_SQL),
                       ('person_ref', _PERSON_REF_SQL)):
        try:
            with db.session.begin_nested():
                db.session.execute(text(sql), {'acc': accession_number})
            done.append('std_' + label)
        except Exception:
            logger.exception("projector: std_%s failed | acc=%s", label, accession_number)
    return done


_OVERRIDE_TARGETS = {
    'study':   ('etl_didb_studies', 'study_db_uid',   'study'),
    'patient': ('etl_patient_view', 'patient_db_uid', 'patient'),
    'order':   ('etl_orders',       'order_dbid',     'order'),
}


def _override_whitelist():
    """
    Column names the catalogue actually permits, per target kind.

    Not decoration. A mapping's target_field is operator-supplied text that has to
    be interpolated as a column name — it cannot be bound as a parameter — so it
    is checked against hl7_field_targets before it reaches any SQL string. The
    table also has a CHECK on target_kind, but defence at the point of
    interpolation is the one that matters.
    """
    try:
        rows = db.session.execute(text(
            "SELECT target_kind, target_field FROM hl7_field_targets")).fetchall()
        allowed = {}
        for kind, field in rows:
            allowed.setdefault(kind, set()).add(field)
        return allowed
    except Exception:
        logger.exception("projector: could not read the target catalogue; "
                         "refusing all direct overrides")
        return {}


def apply_direct_overrides(msg, segments):
    """
    Write operator-configured values straight into the etl_* tables.

    The deliberately risky half of the field-mapping feature, enabled by operator
    decision. A value written here is indistinguishable downstream from one the
    lifecycle produced, so a wrong mapping corrupts a report with no error
    anywhere — which is why the catalogue marks every one of these targets
    dangerous and the editor warns before saving.

    Runs LAST, after the normal projection, so an override genuinely overrides
    rather than racing it.
    """
    try:
        from utils.hl7_fieldmap import direct_overrides
        overrides = direct_overrides(msg, segments)
    except Exception:
        logger.exception("projector: could not evaluate direct overrides")
        return []

    if not any(overrides.values()):
        return []

    allowed = _override_whitelist()
    done = []

    for kind, values in overrides.items():
        if not values or kind not in _OVERRIDE_TARGETS:
            continue
        table, key_col, entity = _OVERRIDE_TARGETS[kind]

        natural = msg.patient_id if kind == 'patient' else msg.accession_number
        if kind == 'order':
            natural = msg.placer_order_number or msg.accession_number
        if not natural:
            continue

        safe = {c: v for c, v in values.items() if c in allowed.get(kind, set())}
        for rejected in set(values) - set(safe):
            logger.warning("projector: refusing override of unknown column %s.%s",
                           table, rejected)
        if not safe:
            continue

        sets = ', '.join(f"{c} = :v_{i}" for i, c in enumerate(safe))
        params = {f'v_{i}': v for i, v in enumerate(safe.values())}
        params['nat'] = natural
        try:
            with db.session.begin_nested():
                db.session.execute(text(
                    f"UPDATE {table} SET {sets} "
                    f"WHERE {key_col} = hl7_surrogate_id('{entity}', :nat)"
                ), params)
            done.append(f"override:{table}({','.join(safe)})")
        except Exception:
            logger.exception("projector: direct override failed | %s | %s",
                             table, list(safe))
    return done


def project_message(msg):
    """
    Project whatever the message just changed.

    A status event moves the study forward, so the study and its order are
    reprojected. ADT changes demographics, which alters age_at_exam on every study
    that patient has — so those are reprojected too rather than left stale until
    the patient's next exam.
    """
    done = []

    if msg.accession_number:
        done += project_study(msg.accession_number)
        done += project_worklist(msg.accession_number)
        if msg.patient_id:
            done += project_patient(msg.patient_id)

    elif msg.kind == 'adt' and msg.patient_id:
        done += project_patient(msg.patient_id)
        try:
            rows = db.session.execute(text(
                "SELECT accession_number FROM ray7_study_state WHERE patient_id = :pid"
            ), {'pid': msg.patient_id}).fetchall()
            for (acc,) in rows:
                done += project_study(acc)
        except Exception:
            logger.exception("projector: could not reproject studies for patient %s",
                             msg.patient_id)

    return done
