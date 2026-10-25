"""
utils/brayn.py
────────────────────────────────────────────────────────────────
BRAYN — the screening engine between the HL7 listener and the database.

REBUILT IN FOUR LAYERS (operator decision 2026-10-25, migration 0135)
─────────────────────────────────────────────────────────────────────
The first BRAYN judged every message as it arrived and could quarantine it for
eight different reasons. The end-to-end suite showed what that cost: on the
2026-09-24 load it held 360 of 660 messages (55%) for control-ID reuse although
every one of them was valid; holding an out-of-order message protected nothing
once the lifecycle state was made order-independent; and a held message left its
study half-projected with nothing in the reports saying so. BRAYN now does four
different jobs, and only the first can keep data out of the reports:

    MLLP ─▶ archive ─▶ ① INTAKE (this module, inline) ──held──▶ stored, nothing derived
                              │                                    fix config + replay
                              ├─accepted─▶ accept path ─▶ ② QUALITY (utils/brayn_quality)
                              │                             notes on the STUDY, judged on
                              │                             its whole lifecycle, self-closing
                              └─▶ ③ INTERFACE (utils/brayn_health)
                                    per-sender rates; an alert only past threshold_pct
    ④ WORKFLOW (utils/brayn_sweep), every 15 minutes: stalled, unreported, orphan

  ① INTAKE asks two questions: can this message be attached to a study (an
    accession or a placer order number), and can it be classified (a rung)? A
    message that fails either cannot produce data — there is nothing to attach it to
    or nothing to say about it — so holding it costs nothing and loses nothing.
    MISSING_IDENTIFIER and UNKNOWN_STATUS_CODE; held only at severity critical, so
    an operator can still downgrade either. PLACEHOLDER_DATA and FUTURE_EVENT are
    recorded here too because they are facts about the message as received, but
    neither ever holds.

  ③ The transport rules — redelivery, control-ID reuse, out-of-order delivery,
    logical duplicates, repeated transitions — and the configuration gaps (unknown
    AE title, result status, unexpected rung) are OBSERVATIONS: counted per sender,
    never rows per study. The values themselves go to brayn_unknown_values, which is
    the implementation engineer's mapping to-do list.


BRAYN NEVER DROPS A MESSAGE
──────────────────────────
Unchanged. The archive is the only copy a message will ever have — there is no
source database to re-extract from on this branch — so rejecting is deleting.
There is no reject verdict: a held message is stored and parsed, only not
projected, and the listener answers AA either way; NAK would only make a sender we
do not control retry something we have already stored safely.


RUNS INLINE, BEFORE THE ACK (operator decision, 2026-09-17)
───────────────────────────────────────────────────────────
Everything in screen() is paid inside the sender's timeout window:

  1. The intake rules are pure computation over the parsed message; they cannot
     be degraded and cost nothing. The observations need two indexed lookups (the
     duplicate probe and the study state), fetched once in _Context.
  2. The lookups run under a time budget and a per-statement ceiling. A blown
     budget or a failed lookup is itself an observation (BRAYN_BUDGET_EXCEEDED,
     BRAYN_DEGRADED): it costs measurement, never the message.
  3. FAIL-OPEN, always. A rule that raises is logged and produces nothing; the
     message still projects.

Every statement is time-bounded and runs inside its own SAVEPOINT. See
_bounded_query() for why the savepoint is the load-bearing part.
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

logger = logging.getLogger("BRAYN")

# ── Severity and disposition ──────────────────────────────────────────────────
INFO     = 'info'
WARNING  = 'warning'
CRITICAL = 'critical'

_SEVERITY_RANK = {INFO: 0, WARNING: 1, CRITICAL: 2}

ACCEPTED = 'accepted'
# A HELD message is stored and parsed but not projected. The stored value is the
# pre-0135 word, kept so the archive's CHECK constraint and its history read on.
HELD = 'quarantined'
QUARANTINED = HELD

# How long the whole inline pass may take before remaining work is abandoned. This
# covers the database lookups too, not just the rules — see screen().
_TIME_BUDGET_MS = 250

# Ceiling on any single BRAYN query. THE IMPORTANT ONE.
#
# Every BRAYN statement runs inside the sender's ACK window, so an unbounded query is
# not a slow query — it is a stalled HL7 feed. Without this, a lock held on
# brayn_study_state or a Postgres hiccup blocks the MLLP socket for as long as it
# lasts, and back-pressures into the hospital's interface engine. With it, the query
# is cancelled, BRAYN degrades to screening that message with less information, and
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


# ── The contract BRAYN screens ─────────────────────────────────────────────────
#
# Defined here rather than in the parsers deliberately: BRAYN is the consumer, so its
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
    findings: list = field(default_factory=list)   # message findings (intake, placeholder)
    notes:    list = field(default_factory=list)   # sticky study notes (FUTURE_EVENT)
    observed: list = field(default_factory=list)   # interface rule codes, counted per sender
    unknowns: list = field(default_factory=list)   # (kind, value) for brayn_unknown_values

    @property
    def may_project(self):
        """Held messages are stored and parsed, but withheld from etl_*."""
        return self.status != HELD


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
            logger.exception("BRAYN could not refresh cache '%s'; using previous value", key)
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
            "BRAYN caches warmed: %d rules, %d status mappings, %d AE titles, "
            "%d result statuses, %d ladder rungs (%d marked measure-order)",
            len(_rules() or {}), len(status_map() or {}), len(_known_aetitles() or set()),
            len(result_status_map() or {}), len(ladder_profile() or {}),
            len(enforced_ranks()))
    except Exception:
        logger.exception("BRAYN cache warm failed; caches will fill lazily instead")


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
    """{(rule_code, modality): {...}} — operator-tuned config from brayn_rules."""
    def load():
        rows, err = _bounded_query('rule config load', """
            SELECT rule_code, modality, enabled,
                   COALESCE(severity, default_severity) AS severity,
                   threshold_minutes
              FROM brayn_rules
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
              FROM brayn_ladder_profile
        """)
        if err:
            raise RuntimeError(err)
        return {r['rung']: dict(r) for r in rows}
    return _cached('ladder', load) or {}


def enforced_ranks():
    """
    Ranks explicitly marked "measure order" (expected AND enforce_order), ascending.
    Reported at startup; measured_ranks() below decides what is actually measured.
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

    An UNKNOWN rank counts as expected. A rank BRAYN has no profile row for is a gap
    in configuration, not a statement that the site does not send it, and treating it
    as unexpected would make a missing row silently suppress rules.

    So does every rank on an UNCONFIGURED profile — the all-off seed means "nobody
    has said", per profile_configured(). Reading it literally retired SKIPPED_RUNG at
    every install that had not mapped its ladder: it skips any rung that is not
    expected, and on the shipped seed no rung was, so the rule could never fire.
    """
    if not profile_configured():
        return True
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
    Run one BRAYN statement under a time limit, inside its own SAVEPOINT.

    Returns (rows, error) — never raises, never leaves the caller's transaction in a
    state the caller did not ask for.

    THE SAVEPOINT IS NOT OPTIONAL, and the reason is easy to miss. When Postgres
    cancels a statement for exceeding statement_timeout, it does not just fail that
    statement — it ABORTS THE TRANSACTION. BRAYN shares the listener's transaction,
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
        logger.warning("BRAYN %s failed or timed out: %s", label, exc)
        return None, str(exc)


class _Context:
    """
    One duplicate probe and one state lookup, shared by every rule.

    Rules must never query on their own. With twenty of them inside the ACK window,
    per-rule queries would turn a sub-millisecond pass into a visible stall on the
    sender's socket.

    Both lookups are time-bounded. When one is cancelled or fails, the context is
    marked DEGRADED rather than the failure being swallowed: the rules that depend on
    it will quietly find nothing and raise no findings, and "BRAYN saw no problems"
    must never be indistinguishable from "BRAYN could not look". screen() counts that
    flag as the sender's BRAYN_DEGRADED rate.
    """

    # Ladder rungs, in order, mapped to their column in brayn_study_state.
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
        self.state      = None   # brayn_study_state row, if the study is known
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
        uses idx_brayn_state_placer. The visit tiebreak is applied in Python over the
        handful of rows that came back rather than as a third query.
        """
        msg = self.msg

        # 1. Accession — authoritative once the RIS has minted it.
        if msg.accession_number:
            row, err = _bounded_query(
                'study state lookup',
                "SELECT * FROM brayn_study_state WHERE accession_number = :acc",
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
                SELECT * FROM brayn_study_state
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


# ── ① Intake: the only rules that can hold a message ──────────────────────────
#
# Pure functions of the parsed message. Each returns a Finding or None.

def _intake_missing_identifier(msg):
    if msg.kind in ('order', 'status', 'result') \
            and not msg.accession_number and not msg.placer_order_number:
        return Finding('MISSING_IDENTIFIER', CRITICAL, {
            'message_type': msg.message_type,
            'reason': 'neither an accession number nor a placer order number is present',
            'effect': 'held: there is no study to attach this message to',
        })


def _intake_unknown_status(msg):
    # Any status message without a rung, not only one whose ORC-5 is filled. An
    # ORC-1 = XO with ORC-5 empty used to pass every rule and vanish as "accepted".
    if msg.kind == 'status' and msg.ladder_rank is None:
        return Finding('UNKNOWN_STATUS_CODE', CRITICAL, {
            'order_control': msg.raw_order_control,
            'order_status': msg.raw_order_status,
            'sending_app': msg.sending_app,
            'hint': 'add a row to hl7_status_map for this code, then replay held messages',
            'effect': 'held: the transition cannot be classified',
        })


_INTAKE = (('MISSING_IDENTIFIER', _intake_missing_identifier),
           ('UNKNOWN_STATUS_CODE', _intake_unknown_status))


def _placeholder(msg):
    """
    Reports what the parser already recognised and neutralised, rather than
    re-deriving it. The parser has to null a 9999-11-11 birth date anyway to keep it
    out of the age bands; this makes that silent correction visible and countable.
    """
    if msg.placeholders:
        return Finding('PLACEHOLDER_DATA', INFO, {
            'placeholders': list(msg.placeholders),
            'patient_id': msg.patient_id,
        })


def _future(msg):
    """
    A timestamp ahead of our clock, beyond the skew tolerance — the sender's clock or
    a mistyped date. A fact about the message as it arrived, so it is judged here
    and kept as a STICKY study note: once the clock catches up the time looks
    plausible, which is exactly when nobody would notice it was wrong.
    """
    if not msg.event_time:
        return None
    skew = _rule_config('FUTURE_EVENT', msg.modality).get('threshold_minutes') \
        or _FUTURE_SKEW_MINUTES
    if msg.event_time > datetime.now() + timedelta(minutes=skew):
        return Finding('FUTURE_EVENT', WARNING, {
            'event_time': msg.event_time.isoformat(),
            'state': msg.canonical_state,
            'local_now': datetime.now().isoformat(timespec='seconds'),
            'tolerance_minutes': skew,
        })


# ── ③ Interface observations ──────────────────────────────────────────────────
#
# Each returns True (count it), a (kind, value) unknown to register as well, or a
# falsy value. They read the context the two bounded lookups fetched; none queries.

_DEFAULT_DUPLICATE_WINDOW_MIN = 10


def _duplicate_window(modality=None):
    """
    The boundary between LOGICAL_DUPLICATE and REPEATED_TRANSITION, in minutes —
    two halves of one decision, so one window: LOGICAL_DUPLICATE's own row first,
    then REPEATED_TRANSITION's (where the 0119 seed put it), then the default.
    """
    own = _rule_config('LOGICAL_DUPLICATE', modality).get('threshold_minutes')
    if own:
        return own
    shared = _rule_config('REPEATED_TRANSITION', modality).get('threshold_minutes')
    return shared or _DEFAULT_DUPLICATE_WINDOW_MIN


def measured_ranks():
    """
    The rungs whose delivery ORDER is measured for OUT_OF_SEQUENCE_DELIVERY.

    The ladder profile's enforce_order switch used to mean "quarantine if late"; it
    now means "count it if late". With none marked, every expected rung is measured
    — and every rung on an unconfigured profile, since measuring holds nothing.
    """
    profile = ladder_profile()
    marked = {p['ladder_rank'] for p in profile.values() if p.get('enforce_order')}
    if marked:
        return marked
    if profile_configured():
        return {p['ladder_rank'] for p in profile.values() if p.get('expected')}
    return {p['ladder_rank'] for p in profile.values()}


def _obs_control_id_reuse(msg, ctx):
    # Every sibling necessarily differs in content: an identical one would have
    # collided on the archive's (sender, control_id, hash) key.
    return bool(ctx.siblings)


def _obs_logical_duplicate(msg, ctx):
    """The same rung again, within the window: one event reaching us by two routes."""
    if msg.kind != 'status' or msg.ladder_rank is None or not msg.event_time:
        return False
    existing = ctx.rung_time(msg.ladder_rank)
    return bool(existing) and \
        abs((msg.event_time - existing).total_seconds()) <= _duplicate_window(msg.modality) * 60


def _obs_repeated_transition(msg, ctx):
    """The same rung again, well outside the window. This RIS genuinely emits more
    than one Arrived per worklist entry, so this is a rate worth seeing, not a fault."""
    if msg.kind != 'status' or msg.ladder_rank is None or not msg.event_time:
        return False
    existing = ctx.rung_time(msg.ladder_rank)
    return bool(existing) and \
        abs((msg.event_time - existing).total_seconds()) > _duplicate_window(msg.modality) * 60


def _obs_out_of_sequence(msg, ctx):
    """Delivered after a later rung of the same study had already arrived."""
    if msg.ladder_rank is None or msg.ladder_rank < 0 or not ctx.state:
        return False
    measured = measured_ranks()
    current = ctx.state.get('current_rank') or 0
    return msg.ladder_rank in measured and current in measured and msg.ladder_rank < current


def _obs_unexpected_rung(msg, ctx):
    if msg.ladder_rank is None or msg.ladder_rank < 0 or not profile_configured():
        return False
    if is_expected(msg.ladder_rank):
        return False
    return ('rung', msg.canonical_state or str(msg.ladder_rank))


def _obs_unknown_aetitle(msg, ctx):
    if msg.aetitle and msg.aetitle.upper() not in _known_aetitles():
        return ('aetitle', msg.aetitle)
    return False


def _obs_unknown_result_status(msg, ctx):
    if msg.kind == 'result' and msg.raw_result_status and msg.ladder_rank is None:
        return ('result_status', msg.raw_result_status)
    return False


_OBSERVERS = (
    ('CONTROL_ID_REUSE',         _obs_control_id_reuse),
    ('OUT_OF_SEQUENCE_DELIVERY', _obs_out_of_sequence),
    ('LOGICAL_DUPLICATE',        _obs_logical_duplicate),
    ('REPEATED_TRANSITION',      _obs_repeated_transition),
    ('UNEXPECTED_RUNG',          _obs_unexpected_rung),
    ('UNKNOWN_AETITLE',          _obs_unknown_aetitle),
    ('UNKNOWN_RESULT_STATUS',    _obs_unknown_result_status),
)


# ── The pass ──────────────────────────────────────────────────────────────────

def _configured(code, finding, modality):
    """Apply the operator's switch and severity override to one finding."""
    cfg = _rule_config(code, modality)
    if not cfg.get('enabled', True) or finding is None:
        return None
    finding.severity = cfg.get('severity') or finding.severity
    return finding


def screen(msg, archive_id=None):
    """
    Run the intake gate and the observations. Returns a Verdict; writes nothing.

    Never raises. A failure anywhere in here resolves to "accept" — see the
    fail-open note at the top of the module.
    """
    started = time.monotonic()
    findings, notes, observed, unknowns = [], [], [], []

    # Normalise once, here, rather than in each rule that compares timestamps.
    msg.event_time = _as_naive(msg.event_time)

    # ① intake, plus the two facts about the message as received.
    for code, fn in _INTAKE + (('PLACEHOLDER_DATA', _placeholder),):
        try:
            f = _configured(code, fn(msg), msg.modality)
        except Exception:
            logger.exception("BRAYN intake rule %s raised; ignoring it for this message", code)
            continue
        if f:
            findings.append(f)
    if any(f.rule_code == 'UNKNOWN_STATUS_CODE' for f in findings):
        unknowns.append(('status_code', '%s/%s' % (msg.raw_order_control or '',
                                                   msg.raw_order_status or '')))
    try:
        f = _configured('FUTURE_EVENT', _future(msg), msg.modality)
        if f:
            # A study note when there is a study to attach it to; a message
            # finding otherwise (a placer-only order stamped in the future).
            (notes if msg.accession_number else findings).append(f)
    except Exception:
        logger.exception("BRAYN FUTURE_EVENT raised; ignoring it for this message")

    # ③ observations. The two lookups are the only part that can be slow, so they
    # are what the budget bounds.
    try:
        ctx = _Context(msg, archive_id)
    except Exception:
        logger.exception("BRAYN could not build context; observations skipped")
        ctx = None
    if ctx is None or ctx.degraded:
        observed.append('BRAYN_DEGRADED')
    else:
        if (time.monotonic() - started) * 1000 > _TIME_BUDGET_MS:
            logger.warning("BRAYN budget exceeded before observations | control_id=%s",
                           msg.control_id)
            observed.append('BRAYN_BUDGET_EXCEEDED')
        else:
            for code, fn in _OBSERVERS:
                if not _rule_config(code, msg.modality).get('enabled', True):
                    continue
                try:
                    hit = fn(msg, ctx)
                except Exception:
                    logger.exception("BRAYN observation %s raised; ignoring it", code)
                    continue
                if hit:
                    observed.append(code)
                    if isinstance(hit, tuple):
                        unknowns.append(hit)

    return _verdict(findings, notes, observed, unknowns)


def _verdict(findings, notes=(), observed=(), unknowns=()):
    v = Verdict(findings=list(findings), notes=list(notes), observed=list(observed),
                unknowns=list(unknowns))
    if findings:
        v.severity = max(findings, key=lambda f: _SEVERITY_RANK.get(f.severity, 0)).severity
    # ONLY a critical INTAKE finding holds. Severity overrides still apply, so an
    # operator who rates UNKNOWN_STATUS_CODE a warning lets those messages through.
    v.status = HELD if v.severity == CRITICAL else ACCEPTED
    return v


def persist(verdict, msg, archive_id):
    """
    Write the verdict onto the archive row, its message findings, and — for an
    accepted message — its sticky study notes. Does not commit, and does not record
    the observations: those describe the live wire, so record_observations() is
    called by the listener only, never by a replay.

    Findings upsert on (message_archive_id, rule_code): re-screening a message during
    a replay refreshes its findings instead of stacking a second copy of each.
    """
    if not archive_id:
        return

    # Same savepoint reasoning as the reads: recording a verdict must never be able
    # to abort the caller's transaction and take the archived message down with it.
    try:
        with db.session.begin_nested():
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET brayn_status = :status,
                       brayn_severity = :severity,
                       brayn_screened_at = NOW()
                 WHERE id = :id
            """), {'status': verdict.status, 'severity': verdict.severity, 'id': archive_id})
    except Exception:
        logger.exception("BRAYN could not stamp verdict on archive_id=%s", archive_id)

    # One savepoint per finding: a malformed detail payload costs that finding only.
    for f in verdict.findings:
        try:
            with db.session.begin_nested():
                db.session.execute(text("""
                    INSERT INTO brayn_findings
                        (message_archive_id, rule_code, severity,
                         accession_number, patient_id, sending_app, detail)
                    VALUES
                        (:archive_id, :rule_code, :severity,
                         :accession, :patient_id, :app, CAST(:detail AS jsonb))
                    ON CONFLICT (message_archive_id, rule_code)
                    WHERE message_archive_id IS NOT NULL
                    DO UPDATE SET severity   = EXCLUDED.severity,
                                  detail     = EXCLUDED.detail,
                                  created_at = NOW()
                """), {
                    'archive_id': archive_id, 'rule_code': f.rule_code,
                    'severity': f.severity, 'accession': msg.accession_number,
                    'patient_id': msg.patient_id, 'app': msg.sending_app,
                    'detail': _json(f.detail),
                })
        except Exception:
            logger.exception(
                "BRAYN could not record finding %s for archive_id=%s", f.rule_code, archive_id)

    if not verdict.may_project or not msg.accession_number:
        return
    for f in verdict.notes:
        try:
            with db.session.begin_nested():
                db.session.execute(text("""
                    INSERT INTO brayn_findings
                        (message_archive_id, rule_code, severity,
                         accession_number, patient_id, sending_app, detail)
                    VALUES (NULL, :rule_code, :severity, :accession, :patient_id, :app,
                            CAST(:detail AS jsonb))
                    ON CONFLICT (rule_code, accession_number)
                        WHERE message_archive_id IS NULL AND resolved_at IS NULL
                    DO UPDATE SET detail = EXCLUDED.detail
                """), {
                    'rule_code': f.rule_code, 'severity': f.severity,
                    'accession': msg.accession_number, 'patient_id': msg.patient_id,
                    'app': msg.sending_app, 'detail': _json(f.detail),
                })
        except Exception:
            logger.exception("BRAYN could not record note %s | acc=%s",
                             f.rule_code, msg.accession_number)


def record_observations(verdict, msg, archive_id):
    """
    Count this message and what was observed about it, per sender — ③ interface.

    Called by the LISTENER after its commit, in a transaction of its own: the
    counters are hot rows (every message from one sender in one quarter-hour adds
    to the same MESSAGES row), and holding their locks for the length of the
    ingest transaction would serialise every connection behind every other.
    Never by replay, which would count the same wire traffic twice.
    """
    metrics = ['MESSAGES'] + list(verdict.observed)
    if not verdict.may_project:
        metrics.append('HELD')
    from utils import brayn_health
    brayn_health.record(msg.sending_app, metrics,
                       [(kind, value, archive_id) for kind, value in verdict.unknowns])


def note_redelivery(sending_app, control_id, hash_value):
    """
    Count a byte-identical redelivery, which the archive INSERT rejected.

    The listener calls this when its ON CONFLICT DO NOTHING returns no row: that
    conflict IS the detection, and it is the only place the event is visible. It
    used to become a finding on the original message with a seen= counter; it is a
    RATE now, per sender — harmless in itself (the SAP Mirth hub redelivers and
    there is no fix coming), worth watching as a trend.
    """
    from utils import brayn_health
    brayn_health.record(sending_app, ['MESSAGES', 'EXACT_REDELIVERY'], [])


def _json(obj):
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return '{}'


def summary(verdict):
    """One-line log form: BRAYN held: UNKNOWN_STATUS_CODE | observed: UNKNOWN_AETITLE"""
    parts = []
    if verdict.findings:
        parts.append(', '.join(f.rule_code for f in verdict.findings))
    if verdict.notes:
        parts.append('notes: ' + ', '.join(f.rule_code for f in verdict.notes))
    if verdict.observed:
        parts.append('observed: ' + ', '.join(verdict.observed))
    word = 'held' if verdict.status == HELD else 'accepted'
    return f"BRAYN {word}" + (': ' + ' | '.join(parts) if parts else '')
