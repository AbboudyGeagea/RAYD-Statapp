"""
utils/ray7.py
────────────────────────────────────────────────────────────────
RAY7 — the screening engine between the HL7 listener and the database.

Every inbound message passes through here after it is archived and before anything
reaches the reporting tables. RAY7 inspects it, raises findings, and returns a
verdict that decides whether the message's data is allowed to project.

    MLLP ─▶ archive (raw) ─▶ RAY7.screen() ─▶ persist events ─▶ project to etl_*
                                    │
                                    └─▶ ray7_findings ─▶ studio queue


RAY7 NEVER DROPS A MESSAGE
──────────────────────────
The archive is the only copy a message will ever have — there is no source database
to re-extract from on this branch — so rejecting is deleting. There is deliberately
no reject verdict: ACCEPTED, FLAGGED and QUARANTINED all keep the message, and a
quarantined one is still parsed and stored, just withheld from etl_* until someone
clears it. The listener answers AA either way; NAK would only make a sender we do
not control retry something we have already stored safely.


RUNS INLINE, BEFORE THE ACK (operator decision, 2026-09-17)
───────────────────────────────────────────────────────────
Everything in this module is paid inside the sender's timeout window, which imposes
three non-negotiable properties:

  1. Every rule is an indexed lookup or pure computation. No rule scans
     hl7_study_events. That is what ray7_study_state is for — one PK-keyed row
     carrying the whole lifecycle, so the sequence rules cost one fetch between them.
  2. The whole pass runs under a time budget. If the budget is blown the remaining
     rules are skipped and a warning is logged, rather than holding the socket open.
     Rules are ordered so the critical ones run first and cheap ones run last.
  3. FAIL-OPEN, always. A rule that raises is logged and produces no finding; the
     message still projects. A screening engine that can block ingestion by crashing
     is a bigger risk to the data than the anomalies it was built to catch.

Two lookups per message hit the database (duplicate probe, study state); everything
else is served from short-lived in-process caches, same pattern as the listener's
field-map cache.


TWO TRAPS THIS ENGINE IS SHAPED AROUND
──────────────────────────────────────
"CM before AR" has two causes and only one is a fault. If the messages merely
arrived out of order on the wire but their timestamps are consistent, that is
routine — the event-sourced design already handles it and flagging it would be pure
noise. The finding is when the TIMESTAMPS THEMSELVES contradict the ladder. See
_rule_ladder_regression vs _rule_time_contradiction, which is the whole distinction.

A repeated Arrived is not automatically a duplicate. This RIS genuinely emits more
than one Arrived transition for a single worklist entry — the reason the arrivals
ETL upserts on (site_worklist_key, arrived_at) and not on the key alone. So a repeat
outside the suppression window is informational, not an error.
"""
import json
import time
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import text
from db import db

logger = logging.getLogger("RAY7")

# ── Severity and disposition ──────────────────────────────────────────────────
INFO     = 'info'
WARNING  = 'warning'
CRITICAL = 'critical'

_SEVERITY_RANK = {INFO: 0, WARNING: 1, CRITICAL: 2}

ACCEPTED    = 'accepted'
FLAGGED     = 'flagged'
QUARANTINED = 'quarantined'

# How long the whole inline pass may take before remaining rules are abandoned.
# Generous relative to a socket timeout, tight relative to a human noticing.
_TIME_BUDGET_MS = 250

# Tolerance for sender clock skew before an event counts as "in the future".
_FUTURE_SKEW_MINUTES = 5

# Config caches. Small, slow-changing, read on every message.
_CACHE_TTL = 300
_cache = {'rules': (0.0, None), 'status_map': (0.0, None), 'aetitles': (0.0, None)}


# ── The contract RAY7 screens ─────────────────────────────────────────────────
#
# Defined here rather than in the parsers deliberately: RAY7 is the consumer, so its
# needs constrain what the parsers must produce, not the other way round. A parser
# for a new message type is correct when it can fill this in.
@dataclass
class ParsedMessage:
    # Envelope
    control_id:        str  = ''          # MSH-10, or a content hash if absent
    sending_app:       str  = ''          # MSH-3
    sending_facility:  str  = None        # MSH-4
    message_type:      str  = None        # MSH-9
    message_datetime:  datetime = None    # MSH-7
    hl7_version:       str  = None        # MSH-12
    raw_message:       str  = ''
    content_hash:      str  = ''
    source_ip:         str  = None

    # What sort of thing this is: order | status | result | adt | other
    kind:              str  = 'other'

    # Identity
    accession_number:    str = None
    placer_order_number: str = None
    patient_id:          str = None

    # Lifecycle (status messages)
    canonical_state:   str  = None
    ladder_rank:       int  = None
    event_time:        datetime = None
    raw_order_control: str  = None        # ORC-1
    raw_order_status:  str  = None        # ORC-5

    # Attribution — this feed carries it per transition
    performed_by_id:   str  = None
    performed_by_name: str  = None
    performed_by_role: str  = None

    # Where
    aetitle:           str  = None
    room_name:         str  = None

    modality:          str  = None
    procedure_code:    str  = None
    procedure_text:    str  = None
    patient_class:     str  = None
    patient_location:  str  = None

    # Demographics (ADT)
    patient_name:      str  = None
    birth_date:        object = None
    sex:               str  = None

    # Set by the parser when it recognised a placeholder it had to null out, so
    # PLACEHOLDER_DATA can report it without re-deriving the judgement.
    placeholders:      list = field(default_factory=list)


@dataclass
class Finding:
    rule_code: str
    severity:  str
    detail:    dict = field(default_factory=dict)


@dataclass
class Verdict:
    status:   str = ACCEPTED
    severity: str = None
    findings: list = field(default_factory=list)

    @property
    def may_project(self):
        """Quarantined messages are stored and parsed, but withheld from etl_*."""
        return self.status != QUARANTINED


def content_hash(raw_message):
    """
    Stable hash of a message body, used both as the dedupe discriminator and as a
    fallback control ID when the sender leaves MSH-10 blank.

    Line endings are normalised first: the same message re-framed with \\r\\n instead
    of \\r is the same message, and letting that change the hash would defeat dedupe
    against exactly the redelivery path most likely to re-frame it.
    """
    normalised = (raw_message or '').replace('\r\n', '\r').replace('\n', '\r')
    return hashlib.sha256(normalised.encode('utf-8', 'replace')).hexdigest()


# ── Configuration ─────────────────────────────────────────────────────────────

def _cached(key, loader):
    stamp, value = _cache.get(key, (0.0, None))
    now = time.time()
    if value is None or (now - stamp) > _CACHE_TTL:
        try:
            value = loader()
            _cache[key] = (now, value)
        except Exception:
            logger.exception("RAY7 could not refresh cache '%s'; using previous value", key)
    return value


def _rules():
    """{(rule_code, modality): {...}} — operator-tuned config from ray7_rules."""
    def load():
        rows = db.session.execute(text("""
            SELECT rule_code, modality, enabled,
                   COALESCE(severity, default_severity) AS severity,
                   threshold_minutes
              FROM ray7_rules
        """)).mappings().all()
        return {(r['rule_code'], r['modality'] or ''): dict(r) for r in rows}
    return _cached('rules', load) or {}


def _rule_config(code, modality=None):
    """
    Most-specific-first: a row for this modality beats the '' catch-all.

    An unknown rule code returns enabled/warning rather than silently disappearing —
    a rule whose seed row was never applied should still be heard from.
    """
    rules = _rules()
    if modality:
        hit = rules.get((code, modality))
        if hit:
            return hit
    return rules.get((code, ''), {'enabled': True, 'severity': WARNING, 'threshold_minutes': None})


def status_map():
    """{(sending_app, order_control, order_status): (canonical_state, ladder_rank)}"""
    def load():
        rows = db.session.execute(text("""
            SELECT sending_app, order_control, order_status, canonical_state, ladder_rank
              FROM hl7_status_map WHERE active
        """)).mappings().all()
        return {
            (r['sending_app'] or '', r['order_control'] or '', (r['order_status'] or '').upper()):
                (r['canonical_state'], r['ladder_rank'])
            for r in rows
        }
    return _cached('status_map', load) or {}


def resolve_status(sending_app, order_control, order_status):
    """
    Map a raw ORC pair onto the canonical ladder, most-specific match first.

    Returns (canonical_state, ladder_rank) or (None, None) when the code is unmapped
    — which UNKNOWN_STATUS_CODE treats as critical, because an unclassified
    transition would otherwise be dropped on the floor without anyone noticing.
    """
    if not order_status:
        return (None, None)
    smap = status_map()
    app  = sending_app or ''
    ctl  = order_control or ''
    st   = order_status.upper()
    for key in ((app, ctl, st), (app, '', st), ('', ctl, st), ('', '', st)):
        if key in smap:
            return smap[key]
    return (None, None)


def _known_aetitles():
    def load():
        rows = db.session.execute(
            text("SELECT UPPER(aetitle) FROM aetitle_modality_map")
        ).fetchall()
        return {r[0] for r in rows if r[0]}
    return _cached('aetitles', load) or set()


# ── Context: everything the rules need, fetched once ──────────────────────────

class _Context:
    """
    One duplicate probe and one state lookup, shared by every rule.

    Rules must never query on their own. With twenty of them inside the ACK window,
    per-rule queries would turn a sub-millisecond pass into a visible stall on the
    sender's socket.
    """

    # Ladder rungs, in order, mapped to their column in ray7_study_state.
    RUNGS = [(40, 'scheduled_at'), (60, 'arrived_at'), (70, 'started_at'), (100, 'completed_at')]

    def __init__(self, msg, archive_id):
        self.msg        = msg
        self.archive_id = archive_id
        self.siblings   = []     # other archive rows sharing sender + control ID
        self.state      = None   # ray7_study_state row, if the study is known

        self._load_siblings()
        self._load_state()

    def _load_siblings(self):
        if not self.msg.control_id:
            return
        try:
            self.siblings = db.session.execute(text("""
                SELECT id, content_hash, received_at
                  FROM hl7_message_archive
                 WHERE sending_app = :app
                   AND message_control_id = :cid
                   AND id <> :self_id
                 LIMIT 5
            """), {
                'app': self.msg.sending_app or '',
                'cid': self.msg.control_id,
                'self_id': self.archive_id or -1,
            }).mappings().all()
        except Exception:
            logger.exception("RAY7 duplicate probe failed; treating as no siblings")

    def _load_state(self):
        acc = self.msg.accession_number
        if not acc:
            return
        try:
            self.state = db.session.execute(
                text("SELECT * FROM ray7_study_state WHERE accession_number = :acc"),
                {'acc': acc},
            ).mappings().first()
        except Exception:
            logger.exception("RAY7 state lookup failed; treating study as unknown")

    def rung_time(self, rank):
        if not self.state:
            return None
        for r, col in self.RUNGS:
            if r == rank:
                return self.state.get(col)
        return None

    def recorded_rungs(self):
        """[(rank, timestamp)] for every rung this study already has a time for."""
        if not self.state:
            return []
        return [(r, self.state.get(c)) for r, c in self.RUNGS if self.state.get(c)]


# ── Rules ─────────────────────────────────────────────────────────────────────
#
# Registered in execution order: critical and cheap first, so that if the time
# budget is blown the rules abandoned are the ones that mattered least.

# SEEDED BUT NOT IMPLEMENTED HERE, on purpose:
#
#   STALLED_SCHEDULED, STALLED_ARRIVED, STALLED_STARTED, UNREPORTED, ORPHAN_ORDER
#       Absence rules. They cannot run on message arrival — the whole point is that
#       the next message never came — so they belong to the periodic sweep, which
#       reads ray7_study_state through its partial stall indexes.
#
#   UNKNOWN_PROCEDURE, UNKNOWN_PERFORMER
#       Both need master data RAYD does not have yet: a procedure catalogue and a
#       staff roster, which arrive by MFN. Implementing them now would mean checking
#       every code against an empty table and flagging all of them, which is noise,
#       not information. The catalogue rows exist so the rules can be switched on the
#       day MFN lands without a migration.

_RULES = []


def _rule(code, kinds=None):
    def register(fn):
        _RULES.append({'code': code, 'kinds': kinds, 'fn': fn})
        return fn
    return register


@_rule('MISSING_IDENTIFIER', kinds={'order', 'status', 'result'})
def _rule_missing_identifier(msg, ctx):
    if not msg.accession_number and not msg.placer_order_number:
        return [Finding('MISSING_IDENTIFIER', CRITICAL, {
            'message_type': msg.message_type,
            'reason': 'neither an accession number nor a placer order number is present',
        })]


@_rule('UNKNOWN_STATUS_CODE', kinds={'status'})
def _rule_unknown_status(msg, ctx):
    # The parser resolves the code; an unresolved one arrives here with no rank.
    if msg.raw_order_status and msg.ladder_rank is None:
        return [Finding('UNKNOWN_STATUS_CODE', CRITICAL, {
            'order_control': msg.raw_order_control,
            'order_status': msg.raw_order_status,
            'sending_app': msg.sending_app,
            'hint': 'add a row to hl7_status_map for this code',
        })]


@_rule('CONTROL_ID_REUSE')
def _rule_control_id_reuse(msg, ctx):
    """
    Another message from the same sender carries this control ID but different
    content. Both are stored — the dedupe key includes the hash precisely so this
    one could not be lost — but one of them is almost certainly a sender bug, and
    whichever downstream consumer keys on control ID will see only one of them.
    """
    # Every sibling necessarily differs in content: an identical one would have
    # collided on the archive's (sender, control_id, hash) key and never become a
    # separate row. So the mere existence of a sibling is the finding.
    if ctx.siblings:
        return [Finding('CONTROL_ID_REUSE', CRITICAL, {
            'control_id': msg.control_id,
            'sending_app': msg.sending_app,
            'conflicting_archive_ids': [s['id'] for s in ctx.siblings],
        })]


# NOTE — EXACT_REDELIVERY is deliberately NOT an inline rule; see note_redelivery()
# near the bottom of this module. It cannot be one: the archive's unique key is
# (sending_app, message_control_id, content_hash), so a byte-identical redelivery
# collides on INSERT and never produces a second archive row for screen() to be
# called with. A rule here looking for a same-hash sibling would be unreachable code,
# because such a sibling cannot exist by construction. The redelivery is detected at
# the insert instead, where the conflict actually surfaces.


@_rule('LOGICAL_DUPLICATE', kinds={'status'})
def _rule_logical_duplicate(msg, ctx):
    """
    This rung already has a timestamp and the new one lands on or very near it —
    the same real-world event reaching us by two routes.

    Split from REPEATED_TRANSITION by the suppression window rather than by exact
    equality: two routes rarely agree to the second, and treating a four-second
    difference as a genuine second arrival would be wrong.
    """
    if msg.ladder_rank is None or not msg.event_time:
        return
    existing = ctx.rung_time(msg.ladder_rank)
    if not existing:
        return
    window = _rule_config('REPEATED_TRANSITION', msg.modality).get('threshold_minutes') or 10
    if abs((msg.event_time - existing).total_seconds()) <= window * 60:
        return [Finding('LOGICAL_DUPLICATE', WARNING, {
            'state': msg.canonical_state,
            'existing_time': existing.isoformat(),
            'incoming_time': msg.event_time.isoformat(),
        })]


@_rule('REPEATED_TRANSITION', kinds={'status'})
def _rule_repeated_transition(msg, ctx):
    """
    Same rung again, well outside the suppression window. Legitimate here — a
    worklist entry can genuinely arrive more than once — so this is informational
    and exists to make the pattern visible, not to call it an error.
    """
    if msg.ladder_rank is None or not msg.event_time:
        return
    existing = ctx.rung_time(msg.ladder_rank)
    if not existing:
        return
    window = _rule_config('REPEATED_TRANSITION', msg.modality).get('threshold_minutes') or 10
    gap = abs((msg.event_time - existing).total_seconds())
    if gap > window * 60:
        return [Finding('REPEATED_TRANSITION', INFO, {
            'state': msg.canonical_state,
            'previous_time': existing.isoformat(),
            'incoming_time': msg.event_time.isoformat(),
            'gap_minutes': round(gap / 60, 1),
        })]


@_rule('LADDER_REGRESSION', kinds={'status'})
def _rule_ladder_regression(msg, ctx):
    """
    THE OUT-OF-ORDER DISTINCTION.

    A lower-ranked event arriving after a higher-ranked one is not by itself a
    problem — that is ordinary late delivery, and the projector is built to absorb
    it. It becomes a finding only when the event also CLAIMS TO HAVE HAPPENED LATER
    than the higher rung it sits below: an Arrived stamped after the Completed that
    supposedly followed it. That is a contradiction in the source, not in transit.
    """
    if msg.ladder_rank is None or not msg.event_time or not ctx.state:
        return
    current = ctx.state.get('current_rank') or 0
    if msg.ladder_rank >= current:
        return
    current_time = ctx.rung_time(current)
    if current_time and msg.event_time > current_time:
        return [Finding('LADDER_REGRESSION', WARNING, {
            'incoming_state': msg.canonical_state,
            'incoming_rank': msg.ladder_rank,
            'incoming_time': msg.event_time.isoformat(),
            'reached_rank': current,
            'reached_time': current_time.isoformat(),
        })]


@_rule('TIME_CONTRADICTION', kinds={'status'})
def _rule_time_contradiction(msg, ctx):
    """
    The incoming event is stamped earlier than a rung BELOW it on the ladder —
    completed before arrived, started before scheduled. Left alone this produces
    negative turnaround times, which is how it usually gets noticed: as a nonsense
    number in a report rather than as a data problem.
    """
    if msg.ladder_rank is None or not msg.event_time:
        return
    offenders = [
        {'rank': rank, 'time': ts.isoformat()}
        for rank, ts in ctx.recorded_rungs()
        if rank < msg.ladder_rank and ts > msg.event_time
    ]
    if offenders:
        return [Finding('TIME_CONTRADICTION', WARNING, {
            'incoming_state': msg.canonical_state,
            'incoming_rank': msg.ladder_rank,
            'incoming_time': msg.event_time.isoformat(),
            'earlier_rungs_stamped_later': offenders,
        })]


@_rule('SKIPPED_RUNG', kinds={'status'})
def _rule_skipped_rung(msg, ctx):
    """
    Completion arriving for a study with no arrival and no start on record.

    Deliberately narrow: only completion triggers it, and only arrived/started
    count as missing. A study with no Scheduled is routine (walk-ins and emergencies
    are never scheduled), so demanding the full ladder would flag normal ER work all
    day and bury everything else.
    """
    if msg.ladder_rank is None or msg.ladder_rank < 100:
        return
    missing = []
    for rank, label in ((60, 'arrived'), (70, 'started')):
        if not ctx.rung_time(rank):
            missing.append(label)
    if missing:
        return [Finding('SKIPPED_RUNG', WARNING, {
            'completed_at': msg.event_time.isoformat() if msg.event_time else None,
            'missing_rungs': missing,
        })]


@_rule('FUTURE_EVENT')
def _rule_future_event(msg, ctx):
    if not msg.event_time:
        return
    skew = _rule_config('FUTURE_EVENT', msg.modality).get('threshold_minutes') or _FUTURE_SKEW_MINUTES
    horizon = datetime.now() + timedelta(minutes=skew)
    if msg.event_time > horizon:
        return [Finding('FUTURE_EVENT', WARNING, {
            'event_time': msg.event_time.isoformat(),
            'local_now': datetime.now().isoformat(),
            'tolerance_minutes': skew,
        })]


@_rule('ORPHAN_EVENT', kinds={'status'})
def _rule_orphan_event(msg, ctx):
    """
    A lifecycle event for an accession nothing has ever introduced.

    Scheduling is exempt because that is the message that mints the accession in the
    first place — it is supposed to be the first thing we see for a study.
    """
    if ctx.state or not msg.accession_number:
        return
    if msg.ladder_rank is not None and msg.ladder_rank <= 40:
        return
    return [Finding('ORPHAN_EVENT', WARNING, {
        'accession_number': msg.accession_number,
        'state': msg.canonical_state,
        'note': 'no order or scheduling message has introduced this accession',
    })]


@_rule('UNKNOWN_AETITLE', kinds={'status'})
def _rule_unknown_aetitle(msg, ctx):
    if not msg.aetitle:
        return
    if msg.aetitle.upper() not in _known_aetitles():
        return [Finding('UNKNOWN_AETITLE', INFO, {
            'aetitle': msg.aetitle,
            'note': 'device utilisation will not be attributed until this is mapped',
        })]


@_rule('PLACEHOLDER_DATA')
def _rule_placeholder_data(msg, ctx):
    """
    Reports what the parser already recognised and neutralised, rather than
    re-deriving it. The parser has to null a 9999-11-11 birth date anyway to keep it
    out of the age bands; this makes that silent correction visible and countable.
    """
    if msg.placeholders:
        return [Finding('PLACEHOLDER_DATA', INFO, {
            'placeholders': list(msg.placeholders),
            'patient_id': msg.patient_id,
        })]


# ── The pass ──────────────────────────────────────────────────────────────────

def screen(msg, archive_id=None):
    """
    Run every applicable rule and return a Verdict. Does not write anything.

    Never raises. A failure anywhere in here resolves to "accept" — see the
    fail-open note at the top of the module.
    """
    started = time.monotonic()
    findings = []

    try:
        ctx = _Context(msg, archive_id)
    except Exception:
        logger.exception("RAY7 could not build context; accepting message unscreened")
        return Verdict(status=ACCEPTED)

    budget_blown = False
    for rule in _RULES:
        if rule['kinds'] and msg.kind not in rule['kinds']:
            continue

        elapsed_ms = (time.monotonic() - started) * 1000
        if elapsed_ms > _TIME_BUDGET_MS:
            budget_blown = True
            logger.warning(
                "RAY7 time budget exceeded (%.0fms) — skipping %s and later rules "
                "for control_id=%s", elapsed_ms, rule['code'], msg.control_id,
            )
            break

        cfg = _rule_config(rule['code'], msg.modality)
        if not cfg.get('enabled', True):
            continue

        try:
            produced = rule['fn'](msg, ctx) or []
        except Exception:
            # Fail-open, per rule: one broken rule must not cost us the message.
            logger.exception("RAY7 rule %s raised; ignoring it for this message", rule['code'])
            continue

        for f in produced:
            # Operator severity override wins over whatever the rule proposed.
            f.severity = cfg.get('severity') or f.severity
            findings.append(f)

    if budget_blown:
        findings.append(Finding('RAY7_BUDGET_EXCEEDED', INFO, {
            'elapsed_ms': round((time.monotonic() - started) * 1000, 1),
            'budget_ms': _TIME_BUDGET_MS,
        }))

    return _verdict(findings)


def _verdict(findings):
    if not findings:
        return Verdict(status=ACCEPTED, severity=None, findings=[])
    worst = max(findings, key=lambda f: _SEVERITY_RANK.get(f.severity, 0)).severity
    if worst == CRITICAL:
        status = QUARANTINED
    elif worst == WARNING:
        status = FLAGGED
    else:
        status = ACCEPTED       # info-only findings are recorded but change nothing
    return Verdict(status=status, severity=worst, findings=findings)


def persist(verdict, msg, archive_id):
    """
    Write the verdict onto the archive row and record its findings.

    Separate from screen() so the engine can be exercised without writing, and so a
    caller already inside a transaction controls when this lands. Does not commit.

    Findings upsert on (message_archive_id, rule_code): re-screening a message during
    a replay refreshes its findings instead of stacking a second copy of each.
    """
    if not archive_id:
        return

    try:
        db.session.execute(text("""
            UPDATE hl7_message_archive
               SET ray7_status = :status,
                   ray7_severity = :severity,
                   ray7_screened_at = NOW()
             WHERE id = :id
        """), {'status': verdict.status, 'severity': verdict.severity, 'id': archive_id})

        for f in verdict.findings:
            db.session.execute(text("""
                INSERT INTO ray7_findings
                    (message_archive_id, rule_code, severity,
                     accession_number, patient_id, detail)
                VALUES
                    (:archive_id, :rule_code, :severity,
                     :accession, :patient_id, CAST(:detail AS jsonb))
                ON CONFLICT (message_archive_id, rule_code)
                WHERE message_archive_id IS NOT NULL
                DO UPDATE SET severity   = EXCLUDED.severity,
                              detail     = EXCLUDED.detail,
                              created_at = NOW()
            """), {
                'archive_id': archive_id,
                'rule_code': f.rule_code,
                'severity': f.severity,
                'accession': msg.accession_number,
                'patient_id': msg.patient_id,
                'detail': _json(f.detail),
            })
    except Exception:
        # Recording the verdict must never cost us the message either.
        logger.exception("RAY7 could not persist verdict for archive_id=%s", archive_id)


def note_redelivery(sending_app, control_id, hash_value):
    """
    Record a byte-identical redelivery, which the archive INSERT rejected.

    The listener calls this when its ON CONFLICT DO NOTHING returns no row: that
    conflict IS the detection, and it is the only place the event is visible, since
    no second archive row exists to screen. The finding is attached to the original
    row and carries a count, so a hundred redeliveries of one message stay one
    finding with seen=100 rather than a hundred rows nobody can read past.

    Harmless in itself — the SAP Mirth hub redelivers and there is no fix coming —
    but the RATE is a real signal about the interface, and a rate is only
    measurable if each instance is counted somewhere.
    """
    try:
        row = db.session.execute(text("""
            SELECT id FROM hl7_message_archive
             WHERE sending_app = :app
               AND message_control_id = :cid
               AND content_hash = :hash
             LIMIT 1
        """), {'app': sending_app or '', 'cid': control_id, 'hash': hash_value}).first()
        if not row:
            return

        db.session.execute(text("""
            INSERT INTO ray7_findings
                (message_archive_id, rule_code, severity, detail)
            VALUES
                (:archive_id, 'EXACT_REDELIVERY', :severity,
                 CAST(:detail AS jsonb))
            ON CONFLICT (message_archive_id, rule_code)
            WHERE message_archive_id IS NOT NULL
            DO UPDATE SET
                detail = jsonb_set(
                    ray7_findings.detail, '{seen}',
                    to_jsonb(COALESCE((ray7_findings.detail ->> 'seen')::int, 1) + 1)
                ),
                created_at = NOW()
        """), {
            'archive_id': row[0],
            'severity': _rule_config('EXACT_REDELIVERY').get('severity') or INFO,
            'detail': _json({'control_id': control_id, 'seen': 1}),
        })
    except Exception:
        logger.exception("RAY7 could not record redelivery for control_id=%s", control_id)


def _json(obj):
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return '{}'


def summary(verdict):
    """One-line log form: RAY7 quarantined: CONTROL_ID_REUSE, UNKNOWN_STATUS_CODE"""
    if not verdict.findings:
        return "RAY7 accepted"
    codes = ', '.join(f.rule_code for f in verdict.findings)
    return f"RAY7 {verdict.status}: {codes}"
