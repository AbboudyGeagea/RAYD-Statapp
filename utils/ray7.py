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

Database cost per message: two reads in screen() (duplicate probe, study state), then
in persist() one UPDATE plus one INSERT per finding — so a clean message costs three
statements and a flagged one a few more. Rule config, the status map and the AE title
set come from in-process caches, warmed at startup by warm_caches() and refreshed on
a TTL; a refresh adds three reads to whichever message happens to trip it.

Every one of those statements is time-bounded and runs inside its own SAVEPOINT. See
_bounded_query() for why the savepoint is the load-bearing part.


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
import threading
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

# How long the whole inline pass may take before remaining work is abandoned. This
# covers the database lookups too, not just the rules — see screen().
_TIME_BUDGET_MS = 250

# Ceiling on any single RAY7 query. THE IMPORTANT ONE.
#
# Every RAY7 statement runs inside the sender's ACK window, so an unbounded query is
# not a slow query — it is a stalled HL7 feed. Without this, a lock held on
# ray7_study_state or a Postgres hiccup blocks the MLLP socket for as long as it
# lasts, and back-pressures into the hospital's interface engine. With it, the query
# is cancelled, RAY7 degrades to screening that message with less information, and
# the message still gets through. Degraded screening is a bad day; a stalled feed is
# an incident.
_STATEMENT_TIMEOUT_MS = 150

# Tolerance for sender clock skew before an event counts as "in the future".
_FUTURE_SKEW_MINUTES = 5

# Config caches. Small, slow-changing, read on every message.
#
# Guarded by a lock because the listener runs one thread per connection: without it,
# every thread that finds the cache expired starts its own reload, so a busy moment
# turns one refresh into a dozen simultaneous ones inside as many ACK windows.
_CACHE_TTL = 300
_CACHE_LOCK = threading.Lock()
_cache = {'rules': (0.0, None), 'status_map': (0.0, None), 'aetitles': (0.0, None),
          'result_map': (0.0, None), 'ladder': (0.0, None)}


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

    # Identity.
    #
    # The accession does not exist at the ordered rung — the RIS mints it at
    # scheduling — so these three are a PRECEDENCE CHAIN, not three spellings of one
    # key. resolve_identity() tries accession, then placer order number, and uses
    # visit_number only to break a tie when one order number matches more than one
    # open lifecycle. Visit is deliberately never an identity on its own: PACS
    # messages frequently carry no PV1 at all, and matching on a field that is often
    # absent splits one study into two.
    accession_number:    str = None
    placer_order_number: str = None
    visit_number:        str = None   # PV1-19
    patient_id:          str = None

    # Lifecycle (status messages)
    canonical_state:   str  = None
    ladder_rank:       int  = None
    event_time:        datetime = None
    raw_order_control: str  = None        # ORC-1
    raw_order_status:  str  = None        # ORC-5
    raw_result_status: str  = None        # OBX-11, or OBR-25 as fallback

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
    """
    TTL cache with single-flight refresh.

    The fast path takes no lock — a stale read for the microseconds it takes another
    thread to finish refreshing is harmless, and locking every message to read a dict
    would be worse than the problem. Only the refresh serialises, and the second
    thread in re-checks the stamp and finds the work already done.
    """
    stamp, value = _cache.get(key, (0.0, None))
    now = time.time()
    if value is not None and (now - stamp) <= _CACHE_TTL:
        return value

    with _CACHE_LOCK:
        stamp, value = _cache.get(key, (0.0, None))
        now = time.time()
        if value is not None and (now - stamp) <= _CACHE_TTL:
            return value
        try:
            value = loader()
            _cache[key] = (now, value)
        except Exception:
            # Keep serving the stale value. Config that is five minutes out of date
            # screens better than config that is missing.
            logger.exception("RAY7 could not refresh cache '%s'; using previous value", key)
    return value


def warm_caches():
    """
    Populate every cache up front, called once at listener startup.

    Without this the first message after boot — and one message every TTL after that
    — pays three extra queries inside its own ACK window. Warming moves that cost off
    the hot path to a moment when nothing is waiting on it.
    """
    try:
        _rules()
        status_map()
        _known_aetitles()
        result_status_map()
        ladder_profile()
        logger.info(
            "RAY7 caches warmed: %d rules, %d status mappings, %d AE titles, "
            "%d result statuses, %d ladder rungs (%d enforced)",
            len(_rules() or {}), len(status_map() or {}), len(_known_aetitles() or set()),
            len(result_status_map() or {}), len(ladder_profile() or {}),
            len(enforced_ranks()))
    except Exception:
        logger.exception("RAY7 cache warm failed; caches will fill lazily instead")


def _as_naive(value):
    """
    Coerce a datetime to naive local time.

    Every timestamp in this schema is `timestamp without time zone`, and
    hl7_listener._parse_hl7_datetime strips offsets, so parsers should never hand us
    an aware value. If one ever does, comparing it against datetime.now() raises
    TypeError — which fail-open would swallow, silently disabling FUTURE_EVENT for
    good. A rule that stops working quietly is worse than one that never existed, so
    normalise rather than trust.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


# The cache loaders go through _bounded_query for the same reason every other read
# does. They are called lazily from inside screen(), which runs in the listener's
# transaction, so an unguarded refresh that timed out would abort that transaction
# and discard the archived message — the cache being stale is not worth that.
def _rules():
    """{(rule_code, modality): {...}} — operator-tuned config from ray7_rules."""
    def load():
        rows, err = _bounded_query('rule config load', """
            SELECT rule_code, modality, enabled,
                   COALESCE(severity, default_severity) AS severity,
                   threshold_minutes
              FROM ray7_rules
        """)
        if err:
            raise RuntimeError(err)     # _cached keeps serving the previous value
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
        rows, err = _bounded_query('status map load', """
            SELECT sending_app, order_control, order_status, canonical_state, ladder_rank
              FROM hl7_status_map WHERE active
        """)
        if err:
            raise RuntimeError(err)
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


def result_status_map():
    """{(sending_app, result_status): (canonical_state, ladder_rank)} from OBX-11."""
    def load():
        rows, err = _bounded_query('result status map load', """
            SELECT sending_app, result_status, canonical_state, ladder_rank
              FROM hl7_result_status_map WHERE active
        """)
        if err:
            raise RuntimeError(err)
        return {
            ((r['sending_app'] or ''), (r['result_status'] or '').upper()):
                (r['canonical_state'], r['ladder_rank'])
            for r in rows
        }
    return _cached('result_map', load) or {}


def resolve_result_status(sending_app, result_status):
    """
    Map an OBX-11 (or OBR-25) value onto a signature rung, per-sender first.

    Returns (None, None) for an unmapped value, which UNKNOWN_RESULT_STATUS reports
    as a warning rather than a critical: the report still reaches hl7_oru_reports and
    still reaches the radiologist, so only the signature rung is lost. Quarantining
    would remove more information than it protects.
    """
    if not result_status:
        return (None, None)
    rmap = result_status_map()
    app = sending_app or ''
    st  = result_status.upper()
    for key in ((app, st), ('', st)):
        if key in rmap:
            return rmap[key]
    return (None, None)


def ladder_profile():
    """
    {rung: {'ladder_rank', 'expected', 'enforce_order', 'label'}} — the per-site
    statement of which rungs this install actually has.

    Everything ships off (migration 0130), so on an unconfigured install every lookup
    below reports "not expected, not enforced" and the sequence layer stays inert.
    That is the intended state until an implementation engineer has mapped the site.
    """
    def load():
        rows, err = _bounded_query('ladder profile load', """
            SELECT rung, ladder_rank, label, expected, enforce_order
              FROM ray7_ladder_profile
        """)
        if err:
            raise RuntimeError(err)
        return {r['rung']: dict(r) for r in rows}
    return _cached('ladder', load) or {}


def enforced_ranks():
    """
    Ranks that participate in the sequence judgement, ascending.

    A rung must be BOTH expected and enforce_order to appear here. Expected-but-not-
    enforced is the deliberate middle case: a rung the site does receive over an
    unreliable path, which should be recorded and time-tracked without its lateness
    quarantining anything.
    """
    return sorted(
        p['ladder_rank'] for p in ladder_profile().values()
        if p.get('expected') and p.get('enforce_order')
    )


def profile_configured():
    """
    Whether anyone has actually configured this install's ladder.

    THE DISTINCTION THIS DRAWS IS LOAD-BEARING. Migration 0130 seeds every rung
    expected = FALSE, which literally reads as "this site sends none of these" — and
    taken literally it would make UNEXPECTED_RUNG fire on every single message the
    moment the migration lands, at every install, which is precisely the queue flood
    the opt-in default exists to prevent.

    An all-off profile therefore means UNCONFIGURED, not "configured as absent", and
    the profile-driven rules stay silent until at least one rung is switched on.
    """
    return any(p.get('expected') for p in ladder_profile().values())


def rung_by_rank(rank):
    for rung, p in ladder_profile().items():
        if p.get('ladder_rank') == rank:
            return rung
    return None


def is_expected(rank):
    """
    Whether a rung is declared present at this install.

    An UNKNOWN rank counts as expected. A rank RAY7 has no profile row for is a gap
    in configuration, not a statement that the site does not send it, and treating it
    as unexpected would make a missing row silently suppress rules.
    """
    rung = rung_by_rank(rank)
    if rung is None:
        return True
    return bool(ladder_profile().get(rung, {}).get('expected'))


def _known_aetitles():
    def load():
        rows, err = _bounded_query(
            'AE title load',
            "SELECT UPPER(aetitle) AS ae FROM aetitle_modality_map",
        )
        if err:
            raise RuntimeError(err)
        return {r['ae'] for r in rows if r['ae']}
    return _cached('aetitles', load) or set()


# ── Context: everything the rules need, fetched once ──────────────────────────

def _bounded_query(label, sql, params=None, one=False):
    """
    Run one RAY7 statement under a time limit, inside its own SAVEPOINT.

    Returns (rows, error) — never raises, never leaves the caller's transaction in a
    state the caller did not ask for.

    THE SAVEPOINT IS NOT OPTIONAL, and the reason is easy to miss. When Postgres
    cancels a statement for exceeding statement_timeout, it does not just fail that
    statement — it ABORTS THE TRANSACTION. RAY7 shares the listener's transaction,
    which also holds the archive INSERT. So a naive timeout here would roll back the
    very row whose survival is the point of the entire design: the engine built to
    guarantee no message is ever lost would become the thing that loses it, and only
    under load, which is exactly when nobody is watching closely.

    begin_nested() issues a SAVEPOINT, so a cancelled query rolls back only to that
    point. The archive row, and anything else the caller has already written,
    survives untouched.

    SET LOCAL is scoped to the savepoint too, so the 150ms ceiling reverts with it
    and cannot leak onto the projection writes that follow — those are legitimately
    larger than a point read and must not inherit a limit meant for one.
    """
    try:
        with db.session.begin_nested():
            db.session.execute(
                text("SET LOCAL statement_timeout = :ms"),
                {'ms': str(_STATEMENT_TIMEOUT_MS)},
            )
            result = db.session.execute(text(sql), params or {})
            rows = result.mappings().first() if one else result.mappings().all()
        return rows, None
    except Exception as exc:
        logger.warning("RAY7 %s failed or timed out: %s", label, exc)
        return None, str(exc)


class _Context:
    """
    One duplicate probe and one state lookup, shared by every rule.

    Rules must never query on their own. With twenty of them inside the ACK window,
    per-rule queries would turn a sub-millisecond pass into a visible stall on the
    sender's socket.

    Both lookups are time-bounded. When one is cancelled or fails, the context is
    marked DEGRADED rather than the failure being swallowed: the rules that depend on
    it will quietly find nothing and raise no findings, and "RAY7 saw no problems"
    must never be indistinguishable from "RAY7 could not look". screen() turns that
    flag into a finding of its own.
    """

    # Ladder rungs, in order, mapped to their column in ray7_study_state.
    #
    # This maps RANK to COLUMN, which is schema structure, not clinical policy — it
    # is not the hardcoded status ladder the standing rule warns against. The part
    # that varies per site, CODE to rank, lives in hl7_status_map where an operator
    # can reach it.
    RUNGS = [(20, 'ordered_at'), (40, 'scheduled_at'), (60, 'arrived_at'),
             (70, 'started_at'), (100, 'completed_at'),
             (120, 'signed_prelim_at'), (140, 'signed_final_at')]

    def __init__(self, msg, archive_id):
        self.msg        = msg
        self.archive_id = archive_id
        self.siblings   = []     # other archive rows sharing sender + control ID
        self.state      = None   # ray7_study_state row, if the study is known
        self.degraded   = []     # names of lookups that failed or timed out
        self.matched_by = None   # which link in the identity chain found the study
        self.ambiguous  = []     # accessions an order number matched when >1 survived

        self._load_siblings()
        self._load_state()

    def _load_siblings(self):
        if not self.msg.control_id:
            return
        rows, err = _bounded_query('duplicate probe', """
            SELECT id, content_hash, received_at
              FROM hl7_message_archive
             WHERE sending_app = :app
               AND message_control_id = :cid
               AND id <> :self_id
             LIMIT 5
        """, {
            'app': self.msg.sending_app or '',
            'cid': self.msg.control_id,
            'self_id': self.archive_id or -1,
        })
        if err:
            self.degraded.append('duplicate_probe')
        else:
            self.siblings = rows or []

    def _load_state(self):
        """
        Resolve this message to one study, by precedence: accession, then order
        number, with visit number breaking a tie.

        At most two lookups, both on an index, because this runs inside the sender's
        ACK window. The accession probe is the primary key; the order-number probe
        uses idx_ray7_state_placer. The visit tiebreak is applied in Python over the
        handful of rows that came back rather than as a third query.
        """
        msg = self.msg

        # 1. Accession — authoritative once the RIS has minted it.
        if msg.accession_number:
            row, err = _bounded_query(
                'study state lookup',
                "SELECT * FROM ray7_study_state WHERE accession_number = :acc",
                {'acc': msg.accession_number}, one=True,
            )
            if err:
                self.degraded.append('study_state')
                return
            if row:
                self.state = row
                self.matched_by = 'accession'
                return

        # 2. Order number. Either this message predates the accession (an ORM NW), or
        #    it carries one the RIS minted after we already opened a provisional row
        #    for the order — the scheduling message that triggers the rekey.
        if msg.placer_order_number:
            rows, err = _bounded_query('study state by order', """
                SELECT * FROM ray7_study_state
                 WHERE placer_order_number = :placer
                 ORDER BY is_provisional DESC, first_seen_at DESC
                 LIMIT 5
            """, {'placer': msg.placer_order_number})
            if err:
                self.degraded.append('study_state')
                return
            self.state = self._disambiguate(rows or [])

    def _disambiguate(self, rows):
        """
        Pick one lifecycle when an order number matches several.

        Visit number is the tiebreaker and never an identity of its own. A message
        with no visit falls back to the most recent open lifecycle rather than
        matching nothing, because PACS messages routinely carry no PV1 at all and
        refusing to match would start a second lifecycle for a study we already know.
        """
        if not rows:
            return None
        if len(rows) == 1:
            self.matched_by = 'order_number'
            return rows[0]

        visit = self.msg.visit_number
        if visit:
            narrowed = [r for r in rows if r.get('visit_number') == visit]
            if len(narrowed) == 1:
                self.matched_by = 'order_number+visit'
                return narrowed[0]
            if narrowed:
                rows = narrowed

        self.ambiguous = [r['accession_number'] for r in rows]
        self.matched_by = 'order_number(ambiguous)'
        return rows[0]

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

_DEFAULT_DUPLICATE_WINDOW_MIN = 10


def _duplicate_window(modality=None):
    """
    The boundary between LOGICAL_DUPLICATE and REPEATED_TRANSITION, in minutes.

    One window, read by both rules, because they are two halves of one decision: the
    same rung reported twice is a duplicate inside the window and a genuine repeat
    outside it, and there is no coherent configuration where those two numbers
    differ. LOGICAL_DUPLICATE's own row is authoritative so the studio has an obvious
    place to edit it; REPEATED_TRANSITION's value is the fallback, which is where the
    seed migration happens to put it.

    Previously both rules read REPEATED_TRANSITION's threshold directly, so an
    operator tuning that row silently moved a boundary the other rule never
    advertised it depended on.
    """
    own = _rule_config('LOGICAL_DUPLICATE', modality).get('threshold_minutes')
    if own:
        return own
    shared = _rule_config('REPEATED_TRANSITION', modality).get('threshold_minutes')
    return shared or _DEFAULT_DUPLICATE_WINDOW_MIN


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
    window = _duplicate_window(msg.modality)
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
    window = _duplicate_window(msg.modality)
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


@_rule('OUT_OF_SEQUENCE_DELIVERY', kinds={'status', 'result'})
def _rule_out_of_sequence(msg, ctx):
    """
    THE MESSAGES ARE SOUND; THE QUEUE THAT DELIVERED THEM WAS NOT.

    This is the third of the three cases that all look like "CM before AR", and the
    only one nothing caught before:

        TIME_CONTRADICTION        the timestamps disagree — the source is wrong
        SKIPPED_RUNG              the rung is genuinely absent — no event existed
        OUT_OF_SEQUENCE_DELIVERY  the timestamps agree, the ARRIVAL ORDER did not

    LADDER_REGRESSION deliberately exempts this case, on the reasoning that the
    projector absorbs late delivery so flagging it is noise. That holds for the DATA
    and not for the INTERFACE: a sender whose queue is disturbed is telling us
    something, and it stays invisible until the day it also loses a message. This
    rule is that signal, and it is mutually exclusive with LADDER_REGRESSION by
    construction — that one takes the contradictory-timestamp case, this one takes
    the consistent-timestamp case, and between them they cover every rank < current.

    Critical, so the study is held out of the reports until a human acknowledges it.
    Gated on enforced_ranks(), so on an unconfigured install it never fires at all.
    """
    if msg.ladder_rank is None or not msg.event_time or not ctx.state:
        return

    enforced = enforced_ranks()
    if msg.ladder_rank not in enforced:
        return

    current = ctx.state.get('current_rank') or 0
    if msg.ladder_rank >= current:
        return
    # A rung the site does not hold to an order cannot be the thing this message is
    # late relative to, or disabling a rung would still quarantine its neighbours.
    if current not in enforced:
        return

    current_time = ctx.rung_time(current)

    # Hand the contradictory case to LADDER_REGRESSION rather than raising both. A
    # single event producing two findings of different severities would quarantine on
    # the strength of a rule whose own docstring says it is describing something else.
    if current_time and msg.event_time > current_time:
        return

    return [Finding('OUT_OF_SEQUENCE_DELIVERY', CRITICAL, {
        'incoming_state':      msg.canonical_state,
        'incoming_rank':       msg.ladder_rank,
        'incoming_time':       msg.event_time.isoformat(),
        'already_reached_rank': current,
        'already_reached_rung': rung_by_rank(current),
        'already_reached_time': current_time.isoformat() if current_time else None,
        # False when the higher rung has no timestamp to compare against — the
        # arrival order is still wrong, but consistency could not be confirmed, and
        # saying so is cheaper than someone re-deriving it from the queue later.
        'timestamps_compared': bool(current_time),
        'matched_by':          ctx.matched_by,
        'sending_app':         msg.sending_app,
        'note': 'events are self-consistent; they were delivered in the wrong order',
    })]


@_rule('UNEXPECTED_RUNG', kinds={'status', 'result'})
def _rule_unexpected_rung(msg, ctx):
    """
    A status for a rung this site declared it does not send.

    Info, not a fault: the likeliest explanation is that the ladder profile is out of
    date, and the useful response is to update it, not to hold traffic. Without this
    a wrong profile is invisible — and a profile nobody has noticed is wrong silently
    suppresses the absence rules it governs.
    """
    if msg.ladder_rank is None or not profile_configured():
        return
    if is_expected(msg.ladder_rank):
        return
    return [Finding('UNEXPECTED_RUNG', INFO, {
        'state': msg.canonical_state,
        'rank':  msg.ladder_rank,
        'hint':  'enable this rung in the ladder profile, or ignore if genuinely unused',
    })]


@_rule('UNKNOWN_RESULT_STATUS', kinds={'result'})
def _rule_unknown_result_status(msg, ctx):
    """
    An OBX-11 value with no row in hl7_result_status_map.

    Only raised where the site actually uses the signature rungs; elsewhere the value
    is genuinely irrelevant and reporting it would be noise.
    """
    if not msg.raw_result_status or msg.ladder_rank is not None:
        return
    profile = ladder_profile()
    if not any(profile.get(r, {}).get('expected')
               for r in ('signed_prelim', 'signed_final')):
        return
    return [Finding('UNKNOWN_RESULT_STATUS', WARNING, {
        'result_status': msg.raw_result_status,
        'sending_app':   msg.sending_app,
        'hint':          'add a row to hl7_result_status_map for this value',
        'effect':        'the report still lands; only its signature rung is lost',
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
    # A rung the site does not send is not a rung the exam skipped. Without this
    # gate, switching started off in the profile would flag every completed study as
    # missing a start for as long as the setting stood — turning a configuration
    # choice into a permanent queue of false findings.
    missing = []
    for rank, label in ((60, 'arrived'), (70, 'started')):
        if not is_expected(rank):
            continue
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

    # Normalise once, here, rather than in each rule that compares timestamps.
    msg.event_time = _as_naive(msg.event_time)

    try:
        ctx = _Context(msg, archive_id)
    except Exception:
        logger.exception("RAY7 could not build context; accepting message unscreened")
        return _verdict([Finding('RAY7_DEGRADED', WARNING, {
            'phase': 'context',
            'effect': 'message accepted without being screened at all',
        })])

    # The budget covers the database work, not just the rules.
    #
    # An earlier version started the clock and then checked it only between rules —
    # which measured nothing that could actually be slow, since every rule is pure
    # computation over data the context already fetched. The two lookups above are
    # the only part that can block, so they are what the budget has to bound.
    elapsed_ms = (time.monotonic() - started) * 1000
    budget_blown = elapsed_ms > _TIME_BUDGET_MS

    if ctx.degraded:
        findings.append(Finding('RAY7_DEGRADED', WARNING, {
            'failed_lookups': list(ctx.degraded),
            'effect': 'the rules depending on those lookups could not run',
        }))

    for rule in _RULES:
        if budget_blown:
            logger.warning(
                "RAY7 budget exceeded (%.0fms) — skipping %s and later rules for control_id=%s",
                elapsed_ms, rule['code'], msg.control_id,
            )
            break
        if rule['kinds'] and msg.kind not in rule['kinds']:
            continue

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
            # Keyed on the code the finding actually carries, not the code the rule
            # was registered under. They are the same for every rule today, but a
            # rule that emits a second, related finding would otherwise be given the
            # wrong rule's operator severity.
            override = _rule_config(f.rule_code, msg.modality).get('severity')
            f.severity = override or f.severity
            findings.append(f)

        elapsed_ms = (time.monotonic() - started) * 1000
        budget_blown = elapsed_ms > _TIME_BUDGET_MS

    if budget_blown:
        findings.append(Finding('RAY7_BUDGET_EXCEEDED', INFO, {
            'elapsed_ms': round(elapsed_ms, 1),
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

    # Same savepoint reasoning as the reads: recording a verdict must never be able
    # to abort the caller's transaction and take the archived message down with it.
    # A finding we failed to write is a gap in the audit trail; a message we failed
    # to keep is unrecoverable.
    try:
        with db.session.begin_nested():
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET ray7_status = :status,
                       ray7_severity = :severity,
                       ray7_screened_at = NOW()
                 WHERE id = :id
            """), {'status': verdict.status, 'severity': verdict.severity, 'id': archive_id})
    except Exception:
        logger.exception("RAY7 could not stamp verdict on archive_id=%s", archive_id)

    # One savepoint per finding, not one around the batch: a single malformed detail
    # payload should cost that one finding, not every other finding on the message.
    for f in verdict.findings:
        try:
            with db.session.begin_nested():
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
            logger.exception(
                "RAY7 could not record finding %s for archive_id=%s", f.rule_code, archive_id)


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
    row, err = _bounded_query('redelivery lookup', """
        SELECT id FROM hl7_message_archive
         WHERE sending_app = :app
           AND message_control_id = :cid
           AND content_hash = :hash
         LIMIT 1
    """, {'app': sending_app or '', 'cid': control_id, 'hash': hash_value}, one=True)
    if err or not row:
        return

    try:
        with db.session.begin_nested():
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
                'archive_id': row['id'],
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
