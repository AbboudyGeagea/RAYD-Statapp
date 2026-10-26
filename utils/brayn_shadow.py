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


def screen_one(archive_id):
    """
    Screen one archived message and write BRAYN's own tables, in one transaction.
    Call inside an app context and clock(). Returns True if it was screened, False
    if another worker had it or it failed.

    Same order as the HL7 branch's listener, minus everything that writes report
    data (legacy writes, projection, direct overrides). A message that fails is
    marked (parse_status 'error', brayn_status 'flagged') so the worker never loops
    on it.
    """
    from utils import brayn
    from utils.brayn_quality import evaluate_study
    from utils.hl7_ingest import mark_parsed, mark_quarantined, persist_parsed
    from utils.hl7_parse import parse_message
    try:
        row = db.session.execute(text("""
            SELECT id, raw_message, source_ip FROM hl7_message_archive
             WHERE id = :id AND brayn_status = 'pending'
             FOR UPDATE SKIP LOCKED
        """), {'id': archive_id}).first()
        if row is None:
            db.session.rollback()
            return False
        msg = parse_message(row.raw_message, source_ip=row.source_ip)
        verdict = brayn.screen(msg, archive_id)
        brayn.persist(verdict, msg, archive_id)
        if verdict.may_project:
            persist_parsed(msg, archive_id)           # events, study state, patients
            evaluate_study(msg.accession_number)      # quality notes on the study
            mark_parsed(archive_id, projected=False)  # LAUMC's reports come from Oracle
        else:
            mark_quarantined(archive_id)              # "not identified"; LAUMC stored it anyway
        db.session.commit()
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
                return run_sweep()
        except Exception:
            db.session.rollback()
            logger.exception("BRAYN shadow: sweep failed")
            return {}
