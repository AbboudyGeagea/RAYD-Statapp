"""
utils/hl7_ingest.py
────────────────────────────────────────────────────────────────
The write side of ingestion: archive a message, then persist what was parsed out
of it. Sits between the listener (sockets, framing, ACK) and RAY7 (screening).

    listener ─▶ archive() ─▶ RAY7.screen() ─▶ persist_parsed() ─▶ projector
                    │                              │
              raw, deduped                hl7_study_events
                                          hl7_patients

ORDER MATTERS AND IS NOT NEGOTIABLE: archive() runs before parsing is trusted and
before screening. The archive is the only copy a message will ever have on this
branch — there is no source database to re-extract from — so it is written first
and everything else is derived. A parse failure, a screening failure or a projector
bug all become replayable; none of them can lose the message.

WHAT THIS MODULE DELIBERATELY DOES NOT WRITE
hl7_orders and hl7_oru_reports keep their existing write paths in hl7_listener.
Orders and reports were never the gap — the lifecycle was. Re-routing two working
inserts through here would risk live behaviour for no benefit, so the order and the
report continue where they are, and the projector reads ordered_at from hl7_orders
and reported_at from hl7_oru_reports when it assembles ray7_study_state.

That is also why hl7_study_events carries only the four RIS lifecycle states and
cancellation: those are the transitions with no home anywhere else.
"""
import json
import logging
import time

from sqlalchemy import text
from db import db

logger = logging.getLogger("HL7_INGEST")


_PACS_APP = {'at': 0.0, 'value': None}


def is_pacs_sender(sending_app):
    """
    Is this message from the PACS rather than the RIS?

    The two send byte-identical completion messages — ORM^O01, ORC-1=SC,
    ORC-5=CM — that mean entirely different things: the RIS means the exam
    finished, the PACS means the images arrived. Nothing in the message
    distinguishes them, so settings.hl7_pacs_sending_app names the PACS by MSH-3.

    Unset returns False, so an unconfigured install treats every completion as a
    RIS exam-done. That is the conservative direction: turnaround time stays
    correct and the PACS transfer-lag figure is simply absent, rather than
    turnaround being silently computed from an image-arrival timestamp.
    """
    now = time.time()
    if _PACS_APP['value'] is not None and (now - _PACS_APP['at']) < 300:
        configured = _PACS_APP['value']
    else:
        try:
            row = db.session.execute(text(
                "SELECT value FROM settings WHERE key = 'hl7_pacs_sending_app'")).first()
            configured = (row[0] if row else '') or ''
            _PACS_APP['value'] = configured
            _PACS_APP['at'] = now
        except Exception:
            configured = _PACS_APP['value'] or ''
    if not configured:
        return False
    return (sending_app or '').strip().upper() == configured.strip().upper()


_ARCHIVE_SQL = """
    INSERT INTO hl7_message_archive
        (message_control_id, sending_app, sending_facility, message_type,
         message_datetime, hl7_version, raw_message, content_hash, source_ip,
         parse_status)
    VALUES
        (:control_id, :sending_app, :sending_facility, :message_type,
         :message_datetime, :hl7_version, :raw_message, :content_hash, :source_ip,
         :parse_status)
    ON CONFLICT (sending_app, message_control_id, content_hash) DO NOTHING
    RETURNING id
"""


def archive(msg):
    """
    Store the raw message. Returns (archive_id, is_redelivery).

    The conflict IS the redelivery detection. The archive's unique key is
    (sending_app, message_control_id, content_hash), so a byte-identical
    redelivery — the permanent SAP Mirth behaviour, not a fault — collides and
    returns no row. A control ID reused for DIFFERENT content does not collide, so
    that message is stored and RAY7 flags the reuse afterwards. Keeping both
    behaviours in one index is why neither needs a special case here.

    (None, False) means the insert itself failed, which the caller must treat as
    "do not proceed": without an archive row there is nothing to attach findings or
    events to, and projecting anyway would produce data we cannot trace back.
    """
    params = {
        'control_id':       msg.control_id,
        'sending_app':      msg.sending_app or '',
        'sending_facility': msg.sending_facility,
        'message_type':     msg.message_type,
        'message_datetime': msg.message_datetime,
        'hl7_version':      msg.hl7_version,
        'raw_message':      msg.raw_message,
        'content_hash':     msg.content_hash,
        'source_ip':        msg.source_ip,
        # 'other' means the dispatcher recognised no clinical shape. Still archived,
        # marked so it can be found later rather than silently blending in.
        'parse_status':     'ignored' if msg.kind == 'other' else 'ok',
    }
    try:
        row = db.session.execute(text(_ARCHIVE_SQL), params).first()
    except Exception:
        logger.exception("RAY7/ingest: archive insert failed for control_id=%s",
                         msg.control_id)
        return None, False

    if row is None:
        return None, True          # identical redelivery
    return row[0], False


_PATIENT_SQL = """
    INSERT INTO hl7_patients
        (patient_id, patient_name, birth_date, sex, patient_class, patient_location,
         last_adt_event, last_message_at, message_archive_id, updated_at)
    VALUES
        (:patient_id, :patient_name, :birth_date, :sex, :patient_class,
         :patient_location, :last_adt_event, :last_message_at, :archive_id, NOW())
    ON CONFLICT (patient_id) DO UPDATE SET
        -- LAST WRITE WINS, not fill-only. ADT A08 exists specifically to correct
        -- demographics, so an update must be allowed to overwrite. COALESCE on the
        -- incoming value only, so a message that omits a field leaves the stored
        -- one alone rather than blanking it.
        patient_name       = COALESCE(EXCLUDED.patient_name,     hl7_patients.patient_name),
        birth_date         = COALESCE(EXCLUDED.birth_date,       hl7_patients.birth_date),
        sex                = COALESCE(EXCLUDED.sex,              hl7_patients.sex),
        patient_class      = COALESCE(EXCLUDED.patient_class,    hl7_patients.patient_class),
        patient_location   = COALESCE(EXCLUDED.patient_location, hl7_patients.patient_location),
        last_adt_event     = COALESCE(EXCLUDED.last_adt_event,   hl7_patients.last_adt_event),
        last_message_at    = COALESCE(EXCLUDED.last_message_at,  hl7_patients.last_message_at),
        message_archive_id = EXCLUDED.message_archive_id,
        updated_at         = NOW()
    WHERE hl7_patients.last_message_at IS NULL
       OR EXCLUDED.last_message_at IS NULL
       OR EXCLUDED.last_message_at >= hl7_patients.last_message_at
"""

_EVENT_SQL = """
    INSERT INTO hl7_study_events
        (message_archive_id, accession_number, placer_order_number, patient_id,
         canonical_state, ladder_rank, event_time,
         performed_by_id, performed_by_name, performed_by_role,
         aetitle, room_name, modality, procedure_code, procedure_text,
         patient_class, patient_location, raw_order_control, raw_order_status)
    VALUES
        (:archive_id, :accession_number, :placer_order_number, :patient_id,
         :canonical_state, :ladder_rank, :event_time,
         :performed_by_id, :performed_by_name, :performed_by_role,
         :aetitle, :room_name, :modality, :procedure_code, :procedure_text,
         :patient_class, :patient_location, :raw_order_control, :raw_order_status)
"""


def persist_parsed(msg, archive_id):
    """
    Write the parsed content into the staging tables. Returns a short label of what
    was written, for the log line.

    Each write is wrapped in its own SAVEPOINT for the same reason RAY7's are: a
    constraint violation here must cost this one row, not the archived message and
    not the other writes in the same transaction. Does not commit — the caller owns
    the transaction boundary so that archive, findings and events land together.
    """
    if not archive_id:
        return 'nothing (no archive row)'

    written = []

    # ANY message that names a patient, not just ADT. This used to be ADT-only, on
    # the reasoning that ADT is the demographics feed — true, but the patient ROW is
    # not the same thing as the demographics. Every ORM, status and ORU carries PID-3,
    # and etl_didb_studies.patient_db_uid is minted from it whether or not an ADT ever
    # arrived. Restricting the write to ADT left 106 of 108 studies pointing at a
    # patient row that did not exist, so every report joining studies to
    # etl_patient_view lost them. A site that sends no ADT at all is a supported
    # configuration; it must still get patients.
    #
    # Safe because the upsert COALESCEs on the INCOMING value: a status message
    # carrying nothing but the ID creates the row, then leaves every demographic
    # field alone. last_adt_event stays ADT-only — it means what it says.
    if msg.patient_id:
        try:
            with db.session.begin_nested():
                db.session.execute(text(_PATIENT_SQL), {
                    'patient_id':       msg.patient_id,
                    'patient_name':     msg.patient_name,
                    'birth_date':       msg.birth_date,
                    'sex':              msg.sex,
                    'patient_class':    msg.patient_class,
                    'patient_location': msg.patient_location,
                    'last_adt_event':   msg.message_type if msg.kind == 'adt' else None,
                    'last_message_at':  msg.message_datetime,
                    'archive_id':       archive_id,
                })
            written.append('patient')
        except Exception:
            logger.exception("RAY7/ingest: hl7_patients upsert failed for patient_id=%s",
                             msg.patient_id)

    # Lifecycle events only. An unclassified status message (no canonical_state) is
    # deliberately NOT written: RAY7 has already raised UNKNOWN_STATUS_CODE as
    # critical and quarantined it, and inventing a rung for a code we cannot read
    # would put a guess into the lifecycle log. The raw message is archived, so the
    # event can be recovered by replay once the code is mapped.
    if msg.kind == 'status' and msg.canonical_state and msg.ladder_rank is not None:
        if msg.accession_number or msg.placer_order_number:
            try:
                with db.session.begin_nested():
                    db.session.execute(text(_EVENT_SQL), {
                        'archive_id':          archive_id,
                        'accession_number':    msg.accession_number,
                        'placer_order_number': msg.placer_order_number,
                        'patient_id':          msg.patient_id,
                        'canonical_state':     msg.canonical_state,
                        'ladder_rank':         msg.ladder_rank,
                        'event_time':          msg.event_time,
                        'performed_by_id':     msg.performed_by_id,
                        'performed_by_name':   msg.performed_by_name,
                        'performed_by_role':   msg.performed_by_role,
                        'aetitle':             msg.aetitle,
                        'room_name':           msg.room_name,
                        'modality':            msg.modality,
                        'procedure_code':      msg.procedure_code,
                        'procedure_text':      msg.procedure_text,
                        'patient_class':       msg.patient_class,
                        'patient_location':    msg.patient_location,
                        'raw_order_control':   msg.raw_order_control,
                        'raw_order_status':    msg.raw_order_status,
                    })
                written.append('event:' + msg.canonical_state)
            except Exception:
                logger.exception("RAY7/ingest: hl7_study_events insert failed for %s",
                                 msg.accession_number)

            # Roll the event into the per-study state RAY7 screens the NEXT message
            # against. Order matters: screening has already happened by this point,
            # so it saw the state as it was before this event.
            update_study_state(msg)

    # A report closes the loop. Without this, ray7_study_state.reported_at stays
    # NULL forever and the UNREPORTED sweep rule fires on every completed study —
    # the rule would be pure noise rather than a signal. The report itself lives in
    # hl7_oru_reports; this only records THAT one exists, so the absence sweep can
    # answer "completed but never reported" from one indexed table.
    if msg.kind == 'result' and msg.accession_number and msg.event_time:
        try:
            with db.session.begin_nested():
                db.session.execute(text("""
                    UPDATE ray7_study_state
                       SET reported_at = COALESCE(reported_at, :reported_at),
                           is_closed   = TRUE,
                           updated_at  = NOW()
                     WHERE accession_number = :acc
                """), {'acc': msg.accession_number, 'reported_at': msg.event_time})
            written.append('reported')
        except Exception:
            logger.exception("RAY7/ingest: reported_at update failed | acc=%s",
                             msg.accession_number)

    return ', '.join(written) if written else 'nothing'


# Which ray7_study_state column each rung stamps. Values come from a CHECK-
# constrained set, so this can be interpolated into SQL safely — but it is a
# whitelist lookup rather than a format of the incoming value, and must stay that
# way.
_RUNG_COLUMN = {
    'scheduled': 'scheduled_at',
    'arrived':   'arrived_at',
    'started':   'started_at',
    'completed': 'completed_at',
    'cancelled': 'cancelled_at',
}


def update_study_state(msg):
    """
    Maintain ray7_study_state — the per-accession lifecycle RAY7's sequence rules
    read.

    WITHOUT THIS THE SEQUENCE RULES ARE NOT MERELY BLIND, THEY ARE WRONG. Confirmed
    on the first live run: with the table unpopulated, every study looked unknown,
    so SKIPPED_RUNG reported "arrived and started missing" on a study whose arrived
    and started events were sitting in hl7_study_events two rows away, and
    ORPHAN_EVENT fired on every message. False positives on everything are worse
    than silence, because they train people to stop reading the queue.

    Called AFTER RAY7 has screened, never before: screening the current message
    must see the state as it was BEFORE that message, or every event would find
    itself already recorded and the comparisons would be meaningless.

    Each rung keeps its FIRST timestamp (COALESCE on the stored value), because a
    repeated Arrived is a real occurrence here and the clinically meaningful moment
    is when the patient first arrived. current_rank only ever moves forward via
    GREATEST, so a late-delivered earlier event cannot walk the study backwards —
    the ladder records the furthest point reached, not the most recent message.
    """
    if not msg.accession_number:
        return
    column = _RUNG_COLUMN.get(msg.canonical_state or '')
    if not column or msg.ladder_rank is None:
        return

    # A completion from the PACS means "images stored", not "exam done". Both
    # arrive as ORC-5=CM and only the sender tells them apart, so it is redirected
    # to its own column here. Without this the PACS timestamp would overwrite the
    # clinical one and every turnaround figure would quietly be measuring image
    # transfer instead of radiology.
    if column == 'completed_at' and is_pacs_sender(msg.sending_app):
        column = 'pacs_completed_at'

    sql = """
        INSERT INTO ray7_study_state
            (accession_number, placer_order_number, patient_id, modality, aetitle,
             room_name, procedure_code, procedure_text, patient_class,
             patient_location, {col},
             current_rank, event_count, is_closed, first_seen_at, last_event_at, updated_at)
        VALUES
            (:accession, :placer, :patient_id, :modality, :aetitle,
             :room, :procedure_code, :procedure_text, :patient_class,
             :patient_location, :event_time,
             -- is_closed must be computed on INSERT too, not only on conflict.
             -- A study whose FIRST event is the completion — an exam whose earlier
             -- rungs never arrived, which is exactly the case the absence sweep
             -- cares about — would otherwise stay open forever and be re-reported
             -- as stalled every time the sweep ran.
             :rank, 1, (:rank >= 100 AND :col <> 'pacs_completed_at')
                       OR :state = 'cancelled', NOW(), :event_time, NOW())
        ON CONFLICT (accession_number) DO UPDATE SET
            {col}               = COALESCE(ray7_study_state.{col}, EXCLUDED.{col}),
            placer_order_number = COALESCE(ray7_study_state.placer_order_number, EXCLUDED.placer_order_number),
            patient_id          = COALESCE(ray7_study_state.patient_id,     EXCLUDED.patient_id),
            modality            = COALESCE(ray7_study_state.modality,       EXCLUDED.modality),
            aetitle             = COALESCE(EXCLUDED.aetitle,  ray7_study_state.aetitle),
            room_name           = COALESCE(EXCLUDED.room_name, ray7_study_state.room_name),
            procedure_code      = COALESCE(ray7_study_state.procedure_code, EXCLUDED.procedure_code),
            procedure_text      = COALESCE(ray7_study_state.procedure_text, EXCLUDED.procedure_text),
            patient_location    = COALESCE(EXCLUDED.patient_location, ray7_study_state.patient_location),
            patient_class       = COALESCE(EXCLUDED.patient_class, ray7_study_state.patient_class),
            current_rank        = GREATEST(ray7_study_state.current_rank, EXCLUDED.current_rank),
            event_count         = ray7_study_state.event_count + 1,
            last_event_at       = GREATEST(COALESCE(ray7_study_state.last_event_at, EXCLUDED.last_event_at),
                                           COALESCE(EXCLUDED.last_event_at, ray7_study_state.last_event_at)),
            is_closed           = ray7_study_state.is_closed
                                  OR (EXCLUDED.current_rank >= 100
                                      AND '{col}' <> 'pacs_completed_at')
                                  OR EXCLUDED.cancelled_at IS NOT NULL,
            updated_at          = NOW()
    """.replace('{col}', column)

    try:
        with db.session.begin_nested():
            db.session.execute(text(sql), {
                'accession':      msg.accession_number,
                'placer':         msg.placer_order_number,
                'patient_id':     msg.patient_id,
                'modality':       msg.modality,
                # aetitle and room come from the Started event and should win when
                # present — they describe where the exam actually happened, which a
                # later message has no reason to overwrite with nothing.
                'aetitle':        msg.aetitle,
                'room':           msg.room_name,
                'procedure_code': msg.procedure_code,
                'procedure_text': msg.procedure_text,
                'patient_class':  msg.patient_class,
                'patient_location': msg.patient_location,
                'event_time':     msg.event_time,
                'rank':           msg.ladder_rank,
                'state':          msg.canonical_state,
                'col':            column,
            })
    except Exception:
        logger.exception("RAY7/ingest: ray7_study_state upsert failed | acc=%s",
                         msg.accession_number)


def mark_quarantined(archive_id):
    """
    Record that a message was parsed but held out of the reporting tables.

    Distinct from RAY7's own ray7_status stamp: this marks the DATA-flow outcome on
    the archive row, so a replay can find every message whose content never reached
    etl_* and re-offer it once the finding is resolved.
    """
    if not archive_id:
        return
    try:
        with db.session.begin_nested():
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET parse_status = 'ok', parsed_at = NOW(), projected_at = NULL
                 WHERE id = :id
            """), {'id': archive_id})
    except Exception:
        logger.exception("RAY7/ingest: could not mark archive_id=%s quarantined", archive_id)


def mark_parsed(archive_id, projected):
    """Stamp the archive row once ingestion has finished with it."""
    if not archive_id:
        return
    try:
        with db.session.begin_nested():
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET parsed_at = NOW(),
                       projected_at = CASE WHEN :projected THEN NOW() ELSE projected_at END
                 WHERE id = :id
            """), {'id': archive_id, 'projected': bool(projected)})
    except Exception:
        logger.exception("RAY7/ingest: could not stamp archive_id=%s", archive_id)


def mark_parse_error(archive_id, error):
    """Dead-letter a message whose processing raised, without losing it."""
    if not archive_id:
        return
    try:
        with db.session.begin_nested():
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET parse_status = 'error', parse_error = :err, parsed_at = NOW()
                 WHERE id = :id
            """), {'id': archive_id, 'err': str(error)[:2000]})
    except Exception:
        logger.exception("RAY7/ingest: could not dead-letter archive_id=%s", archive_id)
