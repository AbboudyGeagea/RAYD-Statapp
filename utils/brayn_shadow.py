"""
utils/brayn_shadow.py
────────────────────────────────────────────────────────────────
BRAYN on LAUMC, observing only — the "orchestrator" between LAUMC's listener and
the BRAYN engine (operator, 2026-10-26: bring BRAYN to LAUMC "without breaking
anything").

On the HL7 branch BRAYN sits IN the ingest path: it archives, screens, and decides
what reaches the reports. LAUMC's reports come from the Oracle ETL, and its
listener already stores reports (fill-only, the RIS text wins), orders and PACS
completions. So here BRAYN only watches:

    LAUMC listener: parse -> its own writes -> ACK      (unchanged)
                                               |
                                               v  after the ACK has gone out
    observe():  archive the raw message (hl7_message_archive, own transaction)
                                               |
                                               v  background worker, in order
    screen_one(): parse -> BRAYN screen -> BRAYN's own tables
                  (findings, study events, study state, patients, quality notes,
                   per-sender counters) — never etl_*, hl7_orders, hl7_oru_reports

Nothing is ever held: a message BRAYN cannot identify is marked 'quarantined' in
the archive (shown as "not identified" in the console) while LAUMC stored it as
usual. Every entry point is fail-open — an error here is logged and swallowed, and
can never reach the listener, its ACK, or its data.

OFF until settings.brayn_enabled = 'true' (migration 0133 seeds 'false').

HOSPITAL CLOCK. HL7 timestamps are naive hospital time, and BRAYN compares them
with NOW() (stalled studies, future events, 15-minute counter buckets). The HL7
branch runs every DB session on that zone; LAUMC's Postgres runs on UTC, and
switching it globally would move the "today" of every existing report. So BRAYN's
work runs inside clock(): while it is active on a thread, every transaction that
thread begins — its own commits included, and the counters' separate connection —
starts with SET LOCAL TIME ZONE, which ends with that transaction. LAUMC's own
transactions never see it.
"""
import logging
import os
import queue
import re
import threading
import time
from contextlib import contextmanager

from sqlalchemy import event, text

from db import db

logger = logging.getLogger("BRAYN_SHADOW")

_SWITCH_TTL = 30                     # seconds the on/off switch is cached
_switch = {'at': 0.0, 'on': False}
_wake = queue.Queue(maxsize=1000)
_worker = {'thread': None, 'stop': threading.Event()}
BATCH = 200

# Postgres TEXT cannot hold U+0000; stored as HL7's own hex escape, as on HL7.
_NUL = '\x00'
_NUL_ESCAPED = '\\X00\\'


# ── the switch ────────────────────────────────────────────────────────────────

def enabled():
    """settings.brayn_enabled == 'true'. Fails CLOSED: if it cannot be read, BRAYN
    stays off — observing is optional, LAUMC's own path is not."""
    now = time.monotonic()
    if now - _switch['at'] < _SWITCH_TTL:
        return _switch['on']
    try:
        value = db.session.execute(
            text("SELECT value FROM settings WHERE key = 'brayn_enabled'")).scalar()
        db.session.rollback()
        _switch['on'] = str(value or '').strip().lower() == 'true'
    except Exception:
        db.session.rollback()
        _switch['on'] = False
    _switch['at'] = now
    return _switch['on']


def reset_switch():
    """Forget the cached switch, so a change takes effect on the next message."""
    _switch['at'] = 0.0


# ── the hospital clock ────────────────────────────────────────────────────────

_ZONE_NAME = re.compile(r'^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$')
_local = threading.local()


def hospital_zone():
    """The zone HL7 timestamps are written in (TZ, default Asia/Beirut), or None.
    Validated: it is inlined into SET LOCAL, and an unknown name would make every
    BRAYN transaction fail."""
    zone = (os.environ.get('TZ') or 'Asia/Beirut').strip()
    if not _ZONE_NAME.match(zone):
        return None
    try:
        import zoneinfo
        known = zoneinfo.available_timezones()
    except Exception:
        known = set()
    return zone if not known or zone in known else None


def install_clock(engine):
    """Hook the engine once: a transaction begun on a thread inside clock() runs on
    hospital time. Outside clock() the hook does nothing at all."""
    if event.contains(engine, 'begin', _on_begin):
        return
    event.listen(engine, 'begin', _on_begin)


def _on_begin(conn):
    zone = getattr(_local, 'zone', None)
    if zone:
        conn.exec_driver_sql(f"SET LOCAL TIME ZONE '{zone}'")


@contextmanager
def clock():
    """Run BRAYN's transactions on this thread on hospital time. A transaction that
    is already open (a request that has queried before) is switched too."""
    prev = getattr(_local, 'zone', None)
    zone = hospital_zone()
    _local.zone = zone
    try:
        if zone and db.session().in_transaction():
            db.session.execute(text("SELECT set_config('timezone', :z, true)"), {'z': zone})
        yield
    finally:
        _local.zone = prev


# ── 1. after the ACK: archive ─────────────────────────────────────────────────

def observe(raw_message, source_ip, app):
    """
    Archive one message for BRAYN. Called by the listener AFTER it has stored the
    message its own way and sent the ACK. Never raises.

    Only the archive insert happens here (one indexed INSERT); screening is the
    worker's, so a slow or failing BRAYN can never hold up the listener.
    """
    try:
        with app.app_context():
            if not enabled():
                return
            from utils import brayn
            from utils.hl7_ingest import archive
            from utils.hl7_parse import parse_message
            if _NUL in raw_message:
                raw_message = raw_message.replace(_NUL, _NUL_ESCAPED)
            with clock():
                try:
                    msg = parse_message(raw_message, source_ip=source_ip)
                    archive_id, redelivered = archive(msg)
                    if redelivered:
                        db.session.rollback()
                        brayn.note_redelivery(msg.sending_app, msg.control_id, msg.content_hash)
                        return
                    if archive_id is None:
                        db.session.rollback()
                        return
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                    raise
        wake()
    except Exception:
        logger.exception("BRAYN shadow: could not archive a message (LAUMC unaffected)")


def wake():
    try:
        _wake.put_nowait(1)
    except queue.Full:
        pass


# ── 2. the worker: screen what was archived ───────────────────────────────────

_PENDING_SQL = """
    SELECT id FROM hl7_message_archive
     WHERE brayn_status = 'pending'
     ORDER BY id
     LIMIT :n
"""


_ROOM_BOOKING_SQL = text("""
    UPDATE brayn_study_state
       SET ris_room       = COALESCE(:room, ris_room),
           appointment_at = COALESCE(:appt, appointment_at),
           booked_minutes = COALESCE(:mins, booked_minutes)
     WHERE accession_number = :acc
""")


def _store_room_booking(msg):
    """The RIS room, appointment and booked minutes onto the study (item 38, migration 0173),
    the latest message's winning: a rebooking moves the room and the time. BRAYN's own table;
    fail-open like every step here."""
    room = (msg.ris_room or '').strip() or None
    mins = msg.booked_minutes
    try:
        mins = int(round(float(mins))) if mins is not None else None
    except (TypeError, ValueError):
        mins = None
    if mins is not None and not 0 < mins <= 600:
        mins = None
    if not msg.accession_number or not (room or msg.appointment_at or mins):
        return
    try:
        with db.session.begin_nested():
            db.session.execute(_ROOM_BOOKING_SQL, {'acc': msg.accession_number, 'room': room,
                                                   'appt': msg.appointment_at, 'mins': mins})
    except Exception:
        logger.exception("BRAYN shadow: room/appointment not stored | acc=%s", msg.accession_number)


_PACS_ACCESSION_SQL = text("""
    UPDATE brayn_study_state SET pacs_accession = :pacs
     WHERE accession_number = :acc AND accession_number <> :pacs
       AND pacs_accession IS DISTINCT FROM :pacs
""")

# A RIS study whose PACS accession has a report: reported_at from the PACS study's earliest
# report or signature (any of them answers UNREPORTED), earliest kept.
_REPORT_LINK_SQL = text("""
    UPDATE brayn_study_state r
       SET reported_at = LEAST(r.reported_at, p.t), updated_at = NOW()
      FROM (SELECT accession_number, LEAST(reported_at, signed_prelim_at, signed_final_at) AS t
              FROM brayn_study_state WHERE accession_number = :pacs) p
     WHERE r.pacs_accession = p.accession_number
       AND r.accession_number <> p.accession_number
       AND p.t IS NOT NULL
       AND (r.reported_at IS NULL OR r.reported_at > p.t)
""")


# A report arriving: the RIS studies under its accession get its time, whatever its OBX-11 says.
# An ORU whose status BRAYN does not map opens no study row of its own (no rung), so linking
# through the PACS row alone missed it (production, 20 Nov: none of the 110 UNREPORTED had one).
_ORU_LINK_SQL = text("""
    UPDATE brayn_study_state
       SET reported_at = LEAST(reported_at, :t), updated_at = NOW()
     WHERE pacs_accession = :acc AND accession_number <> :acc
       AND (reported_at IS NULL OR reported_at > :t)
""")

# Every sweep: a RIS study whose report RAYD holds (hl7_oru_reports: the live ORU, stored by the
# listener, or the RIS's report copied each night) is reported, however it got there.
_STORED_REPORTS_SQL = text("""
    UPDATE brayn_study_state r
       SET reported_at = COALESCE(o.result_datetime, o.received_at), updated_at = NOW()
      FROM hl7_oru_reports o
     WHERE o.accession_number = r.pacs_accession
       AND r.pacs_accession IS NOT NULL AND r.reported_at IS NULL
       AND COALESCE(o.result_datetime, o.received_at) IS NOT NULL
""")


def _link_report(msg):
    """The PACS report onto the RIS study (migration 0175). The RIS's completion names the PACS
    accession (OBR-20) and the report arrives under it, on its own study row; whichever comes
    second links the two. BRAYN's own table; fail-open."""
    if not msg.accession_number:
        return
    pacs = (msg.pacs_accession or '').strip() or None
    try:
        with db.session.begin_nested():
            if msg.kind == 'status' and pacs:
                db.session.execute(_PACS_ACCESSION_SQL, {'acc': msg.accession_number, 'pacs': pacs})
                db.session.execute(_REPORT_LINK_SQL, {'pacs': pacs})
            elif msg.kind == 'result':
                t = msg.event_time or msg.message_datetime
                if t:
                    db.session.execute(_ORU_LINK_SQL, {'acc': msg.accession_number, 't': t})
    except Exception:
        logger.exception("BRAYN shadow: report not linked to the RIS study | acc=%s", msg.accession_number)


# Every sweep: the PACS's own signatures onto the RIS study, from the audit trail copy the
# nightly ETL keeps (etl_pacs_audit_events, migrations 0155/0156; never the PACS itself): a
# resident's wet read = the first signature, a radiologist's approval = the final one. The ORU
# feed carries only final approvals (FAP) and amendments (AAP), so a study wet-read and waiting
# for its approval read as "no report" (production, 20 Nov: 65 of the 110 UNREPORTED). A day
# late, like the copy; earliest kept; times only, no staff.
_PACS_SIGNATURES_SQL = text("""
    WITH todo AS (
        SELECT accession_number, pacs_accession FROM brayn_study_state
         WHERE pacs_accession IS NOT NULL AND signed_final_at IS NULL AND cancelled_at IS NULL
    ), e AS (
        SELECT t.accession_number,
               MIN(a.event_at) FILTER (WHERE a.kind = 'wetread') AS wet,
               MIN(a.event_at) FILTER (WHERE a.kind = 'approve') AS appr
          FROM todo t
          JOIN etl_pacs_audit_events a
            ON a.accession_number = t.pacs_accession AND a.kind IN ('wetread', 'approve')
         GROUP BY 1
    )
    UPDATE brayn_study_state r
       SET signed_prelim_at = LEAST(r.signed_prelim_at, e.wet),
           signed_final_at  = LEAST(r.signed_final_at, e.appr),
           updated_at = NOW()
      FROM e
     WHERE r.accession_number = e.accession_number
       AND (e.appr IS NOT NULL
            OR (e.wet IS NOT NULL AND (r.signed_prelim_at IS NULL OR r.signed_prelim_at > e.wet)))
""")


def link_pacs_signatures():
    """The PACS's wet reads and approvals onto the RIS studies (see _PACS_SIGNATURES_SQL).
    Returns how many studies changed; fail-open. Runs before the sweep."""
    try:
        with db.session.begin_nested():
            n = db.session.execute(_PACS_SIGNATURES_SQL).rowcount
        db.session.commit()
        return n
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN shadow: PACS signatures not linked")
        return 0


def link_stored_reports():
    """RIS studies whose report RAYD already stores (see _STORED_REPORTS_SQL). Returns how many;
    fail-open. Runs before the sweep, so UNREPORTED closes in the same pass."""
    try:
        with db.session.begin_nested():
            n = db.session.execute(_STORED_REPORTS_SQL).rowcount
        db.session.commit()
        return n
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN shadow: stored reports not linked")
        return 0


def screen_one(archive_id, live=True):
    """
    Screen one archived message and write BRAYN's own tables, in one transaction.
    Call inside an app context and clock(). Returns True if it was screened, False
    if another worker had it or it failed.

    Same order as the HL7 branch's listener, minus everything that writes report
    data (legacy writes, projection, direct overrides). A message that fails is
    marked (parse_status 'error', brayn_status 'flagged') so the worker never loops
    on it.

    live=False is the console's re-check of a message BRAYN could not identify:
    its old message findings go first, so a check that no longer fires leaves
    nothing behind, and it is not counted again per sender — that traffic was
    counted when it arrived.
    """
    from utils import brayn
    from utils.brayn_quality import evaluate_study
    from utils.hl7_ingest import mark_parsed, mark_quarantined, persist_parsed
    from utils.hl7_parse import parse_message
    try:
        row = db.session.execute(text("""
            SELECT id, raw_message, source_ip FROM hl7_message_archive
             WHERE id = :id AND brayn_status = :status
             FOR UPDATE SKIP LOCKED
        """), {'id': archive_id, 'status': 'pending' if live else 'quarantined'}).first()
        if row is None:
            db.session.rollback()
            return False
        if not live:
            db.session.execute(text("DELETE FROM brayn_findings WHERE message_archive_id = :id"),
                               {'id': archive_id})
        msg = parse_message(row.raw_message, source_ip=row.source_ip)
        verdict = brayn.screen(msg, archive_id)
        brayn.persist(verdict, msg, archive_id)
        if verdict.may_project:
            persist_parsed(msg, archive_id)           # events, study state, patients
            _store_room_booking(msg)                  # RIS room, appointment (0173)
            _link_report(msg)                         # PACS report onto the RIS study (0175)
            evaluate_study(msg.accession_number)      # quality notes on the study
            mark_parsed(archive_id, projected=False)  # LAUMC's reports come from Oracle
        else:
            mark_quarantined(archive_id)              # "not identified"; LAUMC stored it anyway
        db.session.commit()
        if live:
            brayn.record_observations(verdict, msg, archive_id)
        return True
    except Exception as exc:
        db.session.rollback()
        logger.exception("BRAYN shadow: screening failed | archive_id=%s", archive_id)
        try:
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET parse_status = 'error', parse_error = :err, parsed_at = NOW(),
                       brayn_status = 'flagged', brayn_screened_at = NOW()
                 WHERE id = :id
            """), {'id': archive_id, 'err': str(exc)[:500]})
            db.session.commit()
        except Exception:
            db.session.rollback()
        return False


def process_pending(app, limit=None):
    """Screen every pending archived message, oldest first. Returns how many."""
    done = 0
    with app.app_context():
        if not enabled():
            return 0
        with clock():
            while True:
                try:
                    ids = [r[0] for r in db.session.execute(text(_PENDING_SQL), {'n': BATCH})]
                    db.session.rollback()
                except Exception:
                    db.session.rollback()
                    logger.exception("BRAYN shadow: could not read pending messages")
                    return done
                if not ids:
                    return done
                for archive_id in ids:
                    if screen_one(archive_id):
                        done += 1
                    if limit and done >= limit:
                        return done


def recheck(archive_ids):
    """
    Screen messages BRAYN could not identify again, under today's configuration —
    after a status code or the accession field was mapped. Call inside an app
    context. Returns how many were re-checked.
    """
    done = 0
    with clock():
        for archive_id in archive_ids:
            if screen_one(archive_id, live=False):
                done += 1
    return done


def set_enabled(on):
    """The console's switch. Takes effect on the next message."""
    db.session.execute(text("""
        INSERT INTO settings (key, value) VALUES ('brayn_enabled', :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """), {'v': 'true' if on else 'false'})
    db.session.commit()
    reset_switch()
    if on:
        wake()


def start_worker(app):
    """One daemon thread, started by the listener in the process that owns port 6661,
    so two gunicorn workers never screen side by side (and FOR UPDATE SKIP LOCKED
    makes it harmless if they ever did)."""
    if _worker['thread'] and _worker['thread'].is_alive():
        return _worker['thread']
    stop = _worker['stop'] = threading.Event()

    def loop():
        while not stop.is_set():
            try:
                _wake.get(timeout=30)          # a new message, or every 30 s regardless
            except queue.Empty:
                pass
            try:
                while True:
                    _wake.get_nowait()
            except queue.Empty:
                pass
            if stop.is_set():
                return
            try:
                process_pending(app)
            except Exception:
                logger.exception("BRAYN shadow: worker pass failed")

    t = threading.Thread(target=loop, daemon=True, name='BRAYN-shadow')
    t.start()
    _worker['thread'] = t
    return t


def stop_worker(timeout=10):
    """Stop the worker after its current pass."""
    t = _worker['thread']
    _worker['stop'].set()
    wake()
    if t:
        t.join(timeout)
    _worker['thread'] = None


# ── 3. the 15-minute sweep (delays, sender alerts) ────────────────────────────

def sweep(app):
    """BRAYN's absence sweep, on hospital time, only while BRAYN is on."""
    with app.app_context():
        if not enabled():
            return {}
        try:
            with clock():
                from utils.brayn_sweep import run_sweep
                link_stored_reports()
                link_pacs_signatures()
                return run_sweep()
        except Exception:
            db.session.rollback()
            logger.exception("BRAYN shadow: sweep failed")
            return {}
