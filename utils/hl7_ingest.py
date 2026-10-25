"""
utils/hl7_ingest.py
────────────────────────────────────────────────────────────────
The write side of ingestion: archive a message, then persist what was parsed out
of it. Sits between the listener (sockets, framing, ACK) and BRAYN (screening).

    listener ─▶ archive() ─▶ BRAYN.screen() ─▶ persist_parsed() ─▶ projector
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
and reported_at from hl7_oru_reports when it assembles brayn_study_state.

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
    that message is stored and BRAYN flags the reuse afterwards. Keeping both
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
        logger.exception("BRAYN/ingest: archive insert failed for control_id=%s",
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
        (message_archive_id, accession_number, placer_order_number, visit_number,
         patient_id, canonical_state, ladder_rank, event_time,
         performed_by_id, performed_by_name, performed_by_role,
         aetitle, room_name, modality, procedure_code, procedure_text,
         patient_class, patient_location, raw_order_control, raw_order_status)
    VALUES
        (:archive_id, :accession_number, :placer_order_number, :visit_number,
         :patient_id, :canonical_state, :ladder_rank, :event_time,
         :performed_by_id, :performed_by_name, :performed_by_role,
         :aetitle, :room_name, :modality, :procedure_code, :procedure_text,
         :patient_class, :patient_location, :raw_order_control, :raw_order_status)
"""


def persist_parsed(msg, archive_id):
    """
    Write the parsed content into the staging tables. Returns a short label of what
    was written, for the log line.

    Each write is wrapped in its own SAVEPOINT for the same reason BRAYN's are: a
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
                    # The last-write-wins clock is for DEMOGRAPHICS, so only the
                    # messages that carry them may move it. An ORM or ORU stamped by
                    # a RIS whose clock runs ahead of the HIS used to advance it, and
                    # the A08 correction that followed was then rejected as "older".
                    'last_message_at':  msg.message_datetime if msg.kind == 'adt' else None,
                    'archive_id':       archive_id,
                })
            written.append('patient')
        except Exception:
            logger.exception("BRAYN/ingest: hl7_patients upsert failed for patient_id=%s",
                             msg.patient_id)

    # Lifecycle events only. An unclassified status message (no canonical_state) is
    # deliberately NOT written: BRAYN has already raised UNKNOWN_STATUS_CODE as
    # critical and quarantined it, and inventing a rung for a code we cannot read
    # would put a guess into the lifecycle log. The raw message is archived, so the
    # event can be recovered by replay once the code is mapped.
    # 'order' and 'result' join 'status' here as of the sequence-enforcement work.
    #
    # An ORM NW used to write no event and no state row at all, so the ordered rung
    # existed in the schema and never in the data — ordered_at was filled only by the
    # absence sweep, reading hl7_orders. An ORU likewise had no rung until OBX-11 was
    # parsed. Both now carry a canonical_state and a rank, so both belong in the
    # lifecycle log on exactly the same terms as a status message.
    if (msg.kind in ('status', 'order', 'result')
            and msg.canonical_state and msg.ladder_rank is not None):
        # A placer-only message (the HIS order) for an order the RIS has ALREADY
        # scheduled: it belongs to that study. Without this an ORM NW delivered after
        # its own SC opened a second, provisional lifecycle that nothing would ever
        # rekey — the SC that does the rekeying had already been and gone — and the
        # study never acquired its ordered_at.
        #
        # The accession is written back onto the message deliberately, so that the
        # event row, the state upsert, the projection and the direct overrides that
        # follow all address the real study. Screening has already run; it found the
        # same study through the same order number (_Context._load_state).
        if not msg.accession_number and msg.placer_order_number:
            known = accession_for_placer(msg.placer_order_number)
            if known:
                msg.accession_number = known

        if msg.accession_number or msg.placer_order_number:
            try:
                with db.session.begin_nested():
                    db.session.execute(text(_EVENT_SQL), {
                        'archive_id':          archive_id,
                        'accession_number':    msg.accession_number,
                        'placer_order_number': msg.placer_order_number,
                        'visit_number':        msg.visit_number,
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
                logger.exception("BRAYN/ingest: hl7_study_events insert failed for %s",
                                 msg.accession_number)

            # Roll the event into the per-study state BRAYN screens the NEXT message
            # against. Order matters: screening has already happened by this point,
            # so it saw the state as it was before this event.
            update_study_state(msg)

    # A report closes the loop. Without this, brayn_study_state.reported_at stays
    # NULL forever and the UNREPORTED sweep rule fires on every completed study —
    # the rule would be pure noise rather than a signal. The report itself lives in
    # hl7_oru_reports; this only records THAT one exists, so the absence sweep can
    # answer "completed but never reported" from one indexed table.
    if msg.kind == 'result' and msg.accession_number and msg.event_time:
        try:
            with db.session.begin_nested():
                # reported_at means "a report exists", which is what the UNREPORTED
                # sweep asks about, and any ORU answers it regardless of signature
                # level. The signature RUNGS are set by update_study_state above,
                # from the OBX-11 mapping.
                #
                # is_closed is deliberately NOT set here any more. Closing on any ORU
                # meant a preliminary report closed the study, which would make
                # STALLED_UNSIGNED unsatisfiable in exactly the way completion once
                # made UNREPORTED unsatisfiable — the study stops being tracked at the
                # precise moment it still needs its final signature. Closure is now
                # rank 140 or cancellation, and nowhere else.
                # LEAST, not COALESCE: "when was it first reported" is the earliest
                # report time, whichever report happened to be delivered first.
                db.session.execute(text("""
                    UPDATE brayn_study_state
                       SET reported_at = LEAST(reported_at, :reported_at),
                           updated_at  = NOW()
                     WHERE accession_number = :acc
                """), {'acc': msg.accession_number, 'reported_at': msg.event_time})
            written.append('reported')
        except Exception:
            logger.exception("BRAYN/ingest: reported_at update failed | acc=%s",
                             msg.accession_number)

    return ', '.join(written) if written else 'nothing'


# Which brayn_study_state column each rung stamps. Values come from a CHECK-
# constrained set, so this can be interpolated into SQL safely — but it is a
# whitelist lookup rather than a format of the incoming value, and must stay that
# way.
_RUNG_COLUMN = {
    'ordered':       'ordered_at',
    'scheduled':     'scheduled_at',
    'arrived':       'arrived_at',
    'started':       'started_at',
    'completed':     'completed_at',
    'signed_prelim': 'signed_prelim_at',
    'signed_final':  'signed_final_at',
    'cancelled':     'cancelled_at',
}

# Marks a lifecycle opened before the RIS minted an accession. The tilde cannot occur
# in an accession issued by any of these systems, so a provisional key is recognisable
# on sight and can never collide with a real one.
_PROVISIONAL_PREFIX = '~ORD:'


def provisional_key(placer_order_number):
    return f"{_PROVISIONAL_PREFIX}{placer_order_number}"


def is_provisional_key(key):
    return bool(key) and key.startswith(_PROVISIONAL_PREFIX)


def _rekey_provisional(msg):
    """
    Move a provisional lifecycle onto the accession the RIS has just minted.

    An ORM NW arrives with only a placer order number, so its state row is keyed
    ~ORD:<order>. The scheduling message is the first to carry BOTH identifiers, and
    this is where the two halves become one study.

    Done as an UPDATE of the key rather than a merge because brayn_study_state is the
    only table keyed on the accession as a primary key; brayn_findings and
    hl7_study_events hold it as plain TEXT with no foreign key, so they are rewritten
    alongside rather than cascading.

    If a row already exists under the real accession — the scheduling message arrived
    twice, or out of order relative to itself — the provisional row is folded into it
    and deleted, taking the earlier ordered_at with it. COALESCE order matters: the
    real row wins on every column except the rungs the provisional row uniquely holds.
    """
    acc = msg.accession_number
    placer = msg.placer_order_number
    if not acc or not placer:
        return False
    prov = provisional_key(placer)

    try:
        with db.session.begin_nested():
            merged = db.session.execute(text("""
                UPDATE brayn_study_state real_row
                   SET ordered_at    = COALESCE(real_row.ordered_at, p.ordered_at),
                       placer_order_number = COALESCE(real_row.placer_order_number,
                                                      p.placer_order_number),
                       visit_number  = COALESCE(real_row.visit_number, p.visit_number),
                       event_count   = real_row.event_count + p.event_count,
                       first_seen_at = LEAST(real_row.first_seen_at, p.first_seen_at),
                       updated_at    = NOW()
                  FROM brayn_study_state p
                 WHERE real_row.accession_number = :acc
                   AND p.accession_number = :prov
                RETURNING real_row.accession_number
            """), {'acc': acc, 'prov': prov}).first()

            if merged:
                db.session.execute(
                    text("DELETE FROM brayn_study_state WHERE accession_number = :prov"),
                    {'prov': prov})
            else:
                db.session.execute(text("""
                    UPDATE brayn_study_state
                       SET accession_number = :acc,
                           is_provisional   = FALSE,
                           updated_at       = NOW()
                     WHERE accession_number = :prov
                """), {'acc': acc, 'prov': prov})

            # Findings raised against the provisional key have to follow it.
            db.session.execute(text("""
                UPDATE brayn_findings SET accession_number = :acc
                 WHERE accession_number = :prov
            """), {'acc': acc, 'prov': prov})

            # And so does the order's own event — which is stored with NO accession
            # (the ORM NW had none), not under the provisional key. Matching it on
            # the key, as this did, never found it: every scheduled study's lifecycle
            # log began at Scheduled and the study read as never having been ordered.
            db.session.execute(text("""
                UPDATE hl7_study_events SET accession_number = :acc
                 WHERE accession_number IS NULL AND placer_order_number = :placer
            """), {'acc': acc, 'placer': placer})
        return True
    except Exception:
        logger.exception("BRAYN/ingest: rekey of %s to %s failed", prov, acc)
        return False


def accession_for_placer(placer_order_number):
    """
    The accession the RIS minted for this order, if a scheduled study already holds
    it. The most recent wins when one order became several exams. Indexed
    (idx_brayn_state_placer), and None when anything goes wrong — the caller then
    opens a provisional lifecycle, which the next scheduling message folds in.
    """
    try:
        row = db.session.execute(text("""
            SELECT accession_number FROM brayn_study_state
             WHERE placer_order_number = :p AND NOT is_provisional
             ORDER BY first_seen_at DESC
             LIMIT 1
        """), {'p': placer_order_number}).first()
        return row[0] if row else None
    except Exception:
        logger.exception("BRAYN/ingest: placer lookup failed | placer=%s", placer_order_number)
        return None


# Attributes a later rung legitimately refines — the device and room the exam ran
# on, the procedure as finally performed, where the patient was. Each takes its value
# from the HIGHEST-ranked ORDER OR EXAM event that carried one (ranks 0–100), latest
# first on a tie; the report and cancellation rungs are used only when no exam event
# has a value. The report comes from another system with its own, often abbreviated,
# coding ("CT ABDO" for the RIS's "CT ABDOMEN AND PELVIS"), and it describes the
# exam rather than taking part in it.
#
# Read from the study's own lifecycle log rather than decided by arrival order:
# "the most recent message wins" made the projected device depend on which message
# the queue happened to deliver last, so the same study could come out on two
# different AE titles from two delivery orders — and differently again on replay.
# A handful of rows on an index per study.
_REFINED = ('aetitle', 'room_name', 'patient_class', 'patient_location', 'modality',
            'procedure_code', 'procedure_text')
_REFINED_ORDER = ('(e.ladder_rank BETWEEN 0 AND 100) DESC, e.ladder_rank DESC, '
                  'e.event_time DESC, e.id DESC')

_REFINE_SQL = """
UPDATE brayn_study_state s
   SET {sets}
  FROM (
        SELECT {aggs}
          FROM hl7_study_events e
         WHERE e.accession_number = :acc
       ) x
 WHERE s.accession_number = :acc
""".format(
    sets=',\n       '.join(f"{c} = COALESCE(x.{c}, s.{c})" for c in _REFINED),
    aggs=',\n               '.join(
        f"(array_agg(e.{c} ORDER BY {_REFINED_ORDER}) "
        f"FILTER (WHERE e.{c} IS NOT NULL))[1] AS {c}" for c in _REFINED),
)


def update_study_state(msg):
    """
    Maintain brayn_study_state — the per-accession lifecycle BRAYN's sequence rules
    read.

    WITHOUT THIS THE SEQUENCE RULES ARE NOT MERELY BLIND, THEY ARE WRONG. Confirmed
    on the first live run: with the table unpopulated, every study looked unknown,
    so SKIPPED_RUNG reported "arrived and started missing" on a study whose arrived
    and started events were sitting in hl7_study_events two rows away, and
    ORPHAN_EVENT fired on every message. False positives on everything are worse
    than silence, because they train people to stop reading the queue.

    Called AFTER BRAYN has screened, never before: screening the current message
    must see the state as it was BEFORE that message, or every event would find
    itself already recorded and the comparisons would be meaningless.

    Each rung keeps its FIRST timestamp — the EARLIEST one (LEAST), because a
    repeated Arrived is a real occurrence here and the clinically meaningful moment
    is when the patient first arrived. It used to be COALESCE on the stored value,
    which is the first one DELIVERED: a repeat stamped earlier but queued behind a
    later one then lost, and the same study came out differently depending on
    delivery order and differently again on replay, which takes MIN. current_rank
    only ever moves forward via GREATEST, so a late-delivered earlier event cannot
    walk the study backwards — the ladder records the furthest point reached, not
    the most recent message.

    Every column is now order-independent: rung times by LEAST, rank by GREATEST,
    identity by first-seen, and the refined attributes from the highest-ranked
    event (_REFINED). That is what lets replay rebuild state by simply re-walking
    the archive, and what makes the projected study independent of the queue.
    """
    column = _RUNG_COLUMN.get(msg.canonical_state or '')
    if not column or msg.ladder_rank is None:
        return

    # IDENTITY, in precedence order. The accession once the RIS has minted it;
    # otherwise a provisional key derived from the order number, which is all an ORM
    # NW carries. Without this the ordered rung could never have a state row at all.
    #
    # A message holding BOTH identifiers is the scheduling message that mints the
    # accession, so it is also the moment the provisional lifecycle is folded into the
    # real one. Done before the upsert, or the upsert would create a second row under
    # the accession and the order's own rung would be stranded on the old key.
    if msg.accession_number and msg.placer_order_number:
        _rekey_provisional(msg)

    key = msg.accession_number or (
        provisional_key(msg.placer_order_number) if msg.placer_order_number else None)
    if not key:
        return

    # A completion from the PACS means "images stored", not "exam done". Both
    # arrive as ORC-5=CM and only the sender tells them apart, so it is redirected
    # to its own column here. Without this the PACS timestamp would overwrite the
    # clinical one and every turnaround figure would quietly be measuring image
    # transfer instead of radiology.
    if column == 'completed_at' and is_pacs_sender(msg.sending_app):
        column = 'pacs_completed_at'

    sql = """
        INSERT INTO brayn_study_state
            (accession_number, placer_order_number, visit_number, is_provisional,
             patient_id, modality, aetitle,
             room_name, procedure_code, procedure_text, patient_class,
             patient_location, {col},
             current_rank, event_count, is_closed, first_seen_at, last_event_at, updated_at)
        VALUES
            (:accession, :placer, :visit_number, :provisional,
             :patient_id, :modality, :aetitle,
             :room, :procedure_code, :procedure_text, :patient_class,
             :patient_location, :event_time,
             -- is_closed must be computed on INSERT too, not only on conflict.
             -- A study whose FIRST event is the completion — an exam whose earlier
             -- rungs never arrived, which is exactly the case the absence sweep
             -- cares about — would otherwise stay open forever and be re-reported
             -- as stalled every time the sweep ran.
             --
             -- CLOSURE IS THE FINAL SIGNATURE (rank 140), NOT COMPLETION. Closing at
             -- 100 made UNREPORTED unsatisfiable — completed_at can only be set by a
             -- rank-100 event, which also set is_closed, and the sweep requires NOT
             -- is_closed — so the rule and its index covered an empty set from 0119
             -- until 0130. A study whose images are done but whose report is unsigned
             -- is the one the queue most needs to show.
             :rank, 1, :rank >= 140 OR :state = 'cancelled', NOW(), :event_time, NOW())
        ON CONFLICT (accession_number) DO UPDATE SET
            {col}               = LEAST(brayn_study_state.{col}, EXCLUDED.{col}),
            placer_order_number = COALESCE(brayn_study_state.placer_order_number, EXCLUDED.placer_order_number),
            patient_id          = COALESCE(brayn_study_state.patient_id,     EXCLUDED.patient_id),
            modality            = COALESCE(brayn_study_state.modality,       EXCLUDED.modality),
            aetitle             = COALESCE(EXCLUDED.aetitle,  brayn_study_state.aetitle),
            room_name           = COALESCE(EXCLUDED.room_name, brayn_study_state.room_name),
            procedure_code      = COALESCE(brayn_study_state.procedure_code, EXCLUDED.procedure_code),
            procedure_text      = COALESCE(brayn_study_state.procedure_text, EXCLUDED.procedure_text),
            patient_location    = COALESCE(EXCLUDED.patient_location, brayn_study_state.patient_location),
            patient_class       = COALESCE(EXCLUDED.patient_class, brayn_study_state.patient_class),
            current_rank        = GREATEST(brayn_study_state.current_rank, EXCLUDED.current_rank),
            event_count         = brayn_study_state.event_count + 1,
            last_event_at       = GREATEST(COALESCE(brayn_study_state.last_event_at, EXCLUDED.last_event_at),
                                           COALESCE(EXCLUDED.last_event_at, brayn_study_state.last_event_at)),
            visit_number        = COALESCE(brayn_study_state.visit_number, EXCLUDED.visit_number),
            is_closed           = brayn_study_state.is_closed
                                  OR EXCLUDED.current_rank >= 140
                                  OR EXCLUDED.cancelled_at IS NOT NULL,
            updated_at          = NOW()
    """.replace('{col}', column)

    try:
        with db.session.begin_nested():
            db.session.execute(text(sql), {
                'accession':      key,
                'placer':         msg.placer_order_number,
                'visit_number':   msg.visit_number,
                # From the MESSAGE, never from the key's spelling: an accession that
                # merely begins with '~ORD:' is still an accession, and reading the
                # prefix hid such a study from the reports as an unscheduled order.
                'provisional':    not msg.accession_number,
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
            if msg.accession_number:
                db.session.execute(text(_REFINE_SQL), {'acc': key})
    except Exception:
        logger.exception("BRAYN/ingest: brayn_study_state upsert failed | acc=%s",
                         msg.accession_number)


def mark_quarantined(archive_id):
    """
    Record that a message was parsed but held out of the reporting tables.

    Distinct from BRAYN's own brayn_status stamp: this marks the DATA-flow outcome on
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
        logger.exception("BRAYN/ingest: could not mark archive_id=%s quarantined", archive_id)


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
        logger.exception("BRAYN/ingest: could not stamp archive_id=%s", archive_id)


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
        logger.exception("BRAYN/ingest: could not dead-letter archive_id=%s", archive_id)
