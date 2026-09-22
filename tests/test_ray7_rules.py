"""
tests/test_ray7_rules.py
────────────────────────────────────────────────────────────────
Offline exercise of RAY7's rule logic. Run it directly:

    python tests/test_ray7_rules.py

No database, no Flask, no container — which is the point. This sandbox has no PACS,
no RIS and no usable stack, so the rules cannot be verified by sending a message
through a running system. They can be verified here, because each rule is a plain
function over a context object and a stub with the same shape is enough.

What this is actually protecting: the distinction between a message that merely
ARRIVED out of order, which is routine and must stay silent, and one whose
TIMESTAMPS contradict the lifecycle, which is a real fault. Getting that wrong in
either direction is how a screening engine becomes useless — either it misses real
problems, or it produces a queue so noisy that nobody reads the real problems in it.
"""
import os, sys, types, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub out the db import so utils.ray7 loads without Flask/SQLAlchemy config.
fake_db = types.ModuleType("db")
fake_db.db = types.SimpleNamespace(session=None)
sys.modules["db"] = fake_db

import utils.ray7 as ray7

# Rules read config through _rule_config, which hits the DB. Pin it.
ray7._rule_config = lambda code, modality=None: {
    'enabled': True, 'severity': None, 'threshold_minutes': 10
}

T = lambda h, m=0: dt.datetime(2026, 9, 17, h, m)

class Ctx:
    """Same surface as ray7._Context, without the database."""
    RUNGS = ray7._Context.RUNGS
    def __init__(self, **rungs):
        self.siblings = []
        self.matched_by = 'accession'
        self.ambiguous = []
        self.state = dict(rungs) if rungs else None
        if self.state is not None:
            ranks = [r for r, c in self.RUNGS if self.state.get(c)]
            self.state.setdefault('current_rank', max(ranks) if ranks else 0)
    def rung_time(self, rank):
        if not self.state: return None
        for r, c in self.RUNGS:
            if r == rank: return self.state.get(c)
    def recorded_rungs(self):
        if not self.state: return []
        return [(r, self.state[c]) for r, c in self.RUNGS if self.state.get(c)]

def msg(rank, state, when, **kw):
    return ray7.ParsedMessage(kind='status', ladder_rank=rank, canonical_state=state,
                              event_time=when, accession_number='ACC1', **kw)

def codes(result):
    return sorted(f.rule_code for f in (result or []))

passed = failed = 0
def check(label, got, want):
    global passed, failed
    ok = got == want
    passed, failed = passed + (1 if ok else 0), failed + (0 if ok else 1)
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok: print(f"        got {got!r}, want {want!r}")

print("\nLADDER_REGRESSION — the out-of-order distinction")
# Arrived (60) turns up after Completed (100) was recorded, but is stamped EARLIER.
# Ordinary late delivery. Must NOT fire.
check("late delivery, timestamps consistent -> silent",
      codes(ray7._rule_ladder_regression(msg(60, 'arrived', T(9)),
                                         Ctx(arrived_at=None, completed_at=T(11)))), [])
# Arrived stamped AFTER the Completed that supposedly followed it. Real contradiction.
check("arrived stamped after completed -> flags",
      codes(ray7._rule_ladder_regression(msg(60, 'arrived', T(13)),
                                         Ctx(completed_at=T(11)))), ['LADDER_REGRESSION'])
# Forward progress is never a regression.
check("forward progress -> silent",
      codes(ray7._rule_ladder_regression(msg(100, 'completed', T(12)),
                                         Ctx(arrived_at=T(9)))), [])

print("\nTIME_CONTRADICTION")
check("completed before arrived -> flags",
      codes(ray7._rule_time_contradiction(msg(100, 'completed', T(8)),
                                          Ctx(arrived_at=T(10)))), ['TIME_CONTRADICTION'])
check("completed after arrived -> silent",
      codes(ray7._rule_time_contradiction(msg(100, 'completed', T(12)),
                                          Ctx(arrived_at=T(10)))), [])

print("\nSKIPPED_RUNG")
check("completed with no arrival or start -> flags",
      codes(ray7._rule_skipped_rung(msg(100, 'completed', T(12)), Ctx(scheduled_at=T(8)))),
      ['SKIPPED_RUNG'])
check("completed with full ladder -> silent",
      codes(ray7._rule_skipped_rung(msg(100, 'completed', T(12)),
                                    Ctx(arrived_at=T(9), started_at=T(10)))), [])


# ── The ladder profile and the sequence layer it governs ──────────────────────
#
# Everything below depends on ray7_ladder_profile, which is a table. Pinning the
# cache directly is how these stay offline: _cached() serves a fresh entry without
# ever calling its loader, so no database is touched.

_RUNGS = [('ordered', 20), ('scheduled', 40), ('arrived', 60), ('started', 70),
          ('completed', 100), ('signed_prelim', 120), ('signed_final', 140)]

def profile(expected=(), enforced=()):
    """Pin a ladder profile. Empty = the unconfigured state 0130 ships."""
    import time
    ray7._cache['ladder'] = (time.time(), {
        rung: {'rung': rung, 'ladder_rank': rank, 'label': rung,
               'expected': rung in expected, 'enforce_order': rung in enforced}
        for rung, rank in _RUNGS
    })

ALL = [r for r, _ in _RUNGS]

print("\nOUT_OF_SEQUENCE_DELIVERY — arrival order, not timestamps")
profile(expected=ALL, enforced=ALL)
# Arrived (60) delivered after Completed (100) landed, stamped EARLIER so the events
# themselves are sound. LADDER_REGRESSION stays silent on this; the new rule is
# precisely what catches it.
check("late delivery, timestamps consistent -> quarantines",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)),
                                       Ctx(completed_at=T(11)))),
      ['OUT_OF_SEQUENCE_DELIVERY'])
check("  ...and it is critical",
      [f.severity for f in ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)),
                                                      Ctx(completed_at=T(11)))],
      ['critical'])
# The contradictory case belongs to LADDER_REGRESSION. Both firing would quarantine
# on the strength of a rule that says it is describing something else.
check("timestamps contradict -> silent (LADDER_REGRESSION owns it)",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(13)),
                                       Ctx(completed_at=T(11)))), [])
check("forward progress -> silent",
      codes(ray7._rule_out_of_sequence(msg(100, 'completed', T(12)),
                                       Ctx(arrived_at=T(9)))), [])
check("unknown study -> silent",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)), Ctx())), [])

print("\nOUT_OF_SEQUENCE_DELIVERY — the off switch")
profile()                       # nothing configured: the state on arrival
check("unconfigured profile -> silent",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)),
                                       Ctx(completed_at=T(11)))), [])
profile(expected=ALL, enforced=[r for r in ALL if r != 'arrived'])
check("rung expected but not enforced -> silent",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)),
                                       Ctx(completed_at=T(11)))), [])
# The rung it is late RELATIVE TO must also be enforced, or disabling one rung would
# still quarantine its neighbours.
profile(expected=ALL, enforced=[r for r in ALL if r != 'completed'])
check("reached rung not enforced -> silent",
      codes(ray7._rule_out_of_sequence(msg(60, 'arrived', T(9)),
                                       Ctx(completed_at=T(11)))), [])

print("\nUNEXPECTED_RUNG — and the all-off flood guard")
profile()
# THE REGRESSION THIS GUARDS. 0130 seeds every rung expected=FALSE, which read
# literally means "this site sends none of these" — and would fire on every message
# at every install the moment the migration landed.
check("unconfigured profile -> silent, not a flood",
      codes(ray7._rule_unexpected_rung(msg(70, 'started', T(10)), Ctx())), [])
profile(expected=['scheduled', 'arrived', 'completed'])
check("rung the site says it does not send -> info",
      codes(ray7._rule_unexpected_rung(msg(70, 'started', T(10)), Ctx())),
      ['UNEXPECTED_RUNG'])
check("rung the site does send -> silent",
      codes(ray7._rule_unexpected_rung(msg(60, 'arrived', T(10)), Ctx())), [])

print("\nSKIPPED_RUNG respects the profile")
profile(expected=['scheduled', 'arrived', 'completed'])   # no 'started' at this site
check("disabled rung is not a skipped rung",
      codes(ray7._rule_skipped_rung(msg(100, 'completed', T(12)),
                                    Ctx(arrived_at=T(9)))), [])
check("an expected rung that IS missing still flags",
      codes(ray7._rule_skipped_rung(msg(100, 'completed', T(12)),
                                    Ctx(scheduled_at=T(8)))), ['SKIPPED_RUNG'])
print("\nSTALL RULE BRIDGING — a disabled rung must not blind its segment")
import utils.ray7_sweep as sweep

def bridge(expected):
    profile(expected=expected)
    return {r['code']: r['where'] for r in sweep._stall_rules()}

# Full ladder, no signature feed: completion still waits on reported_at, which is
# what preserves UNREPORTED at a site that never sends W or F.
b = bridge(['scheduled', 'arrived', 'started', 'completed'])
check("arrived waits on started",
      b.get('STALLED_ARRIVED'), "s.arrived_at IS NOT NULL AND s.started_at IS NULL")
check("completed falls back to reported_at",
      b.get('UNREPORTED'), "s.completed_at IS NOT NULL AND s.reported_at IS NULL")

# THE BRIDGE. With started disabled the static rules would ask two useless
# questions — arrived->started fires forever, started->completed can never fire —
# leaving a patient able to arrive and never be scanned with nothing noticing.
b = bridge(['scheduled', 'arrived', 'completed'])
check("started disabled -> arrived waits on completed instead",
      b.get('STALLED_ARRIVED'), "s.arrived_at IS NOT NULL AND s.completed_at IS NULL")
check("started disabled -> no rule named for it",
      'STALLED_STARTED' in b, False)

# With signature rungs on, completion waits on the first signature rather than on
# "a report of any kind", and the prelim->final gap becomes its own question.
b = bridge(['completed', 'signed_prelim', 'signed_final'])
check("completed waits on the first signature",
      b.get('UNREPORTED'), "s.completed_at IS NOT NULL AND s.signed_prelim_at IS NULL")
check("prelim waits on final",
      b.get('STALLED_UNSIGNED'),
      "s.signed_prelim_at IS NOT NULL AND s.signed_final_at IS NULL")

# An unconfigured profile must keep the original four rules. Silently disabling every
# absence rule on merge would be worse than anything this design guards against.
profile()
check("unconfigured -> falls back to the static rules",
      [r['code'] for r in sweep._stall_rules()],
      ['STALLED_SCHEDULED', 'STALLED_ARRIVED', 'STALLED_STARTED', 'UNREPORTED'])

profile()   # leave the cache unconfigured for anything after this
check("walk-in: no scheduled rung is fine -> silent",
      codes(ray7._rule_skipped_rung(msg(100, 'completed', T(12)),
                                    Ctx(arrived_at=T(9), started_at=T(10)))), [])

print("\nDUPLICATE vs REPEAT (10-minute window)")
check("same rung 4 min apart -> logical duplicate",
      codes(ray7._rule_logical_duplicate(msg(60, 'arrived', T(9, 4)), Ctx(arrived_at=T(9)))),
      ['LOGICAL_DUPLICATE'])
check("same rung 4 min apart -> not a repeat",
      codes(ray7._rule_repeated_transition(msg(60, 'arrived', T(9, 4)), Ctx(arrived_at=T(9)))), [])
check("same rung 3 h apart -> genuine repeat, info",
      codes(ray7._rule_repeated_transition(msg(60, 'arrived', T(12)), Ctx(arrived_at=T(9)))),
      ['REPEATED_TRANSITION'])
check("same rung 3 h apart -> not a duplicate",
      codes(ray7._rule_logical_duplicate(msg(60, 'arrived', T(12)), Ctx(arrived_at=T(9)))), [])

print("\nORPHAN_EVENT")
check("completion for unknown accession -> flags",
      codes(ray7._rule_orphan_event(msg(100, 'completed', T(12)), Ctx())), ['ORPHAN_EVENT'])
check("scheduling for unknown accession -> silent (it mints it)",
      codes(ray7._rule_orphan_event(msg(40, 'scheduled', T(8)), Ctx())), [])

print("\nMISSING_IDENTIFIER / FUTURE_EVENT")
check("no accession and no placer -> critical",
      codes(ray7._rule_missing_identifier(ray7.ParsedMessage(kind='status'), Ctx())),
      ['MISSING_IDENTIFIER'])
check("placer only -> acceptable",
      codes(ray7._rule_missing_identifier(
          ray7.ParsedMessage(kind='status', placer_order_number='P1'), Ctx())), [])
check("event 2 days ahead -> flags",
      codes(ray7._rule_future_event(
          msg(60, 'arrived', dt.datetime.now() + dt.timedelta(days=2)), Ctx())), ['FUTURE_EVENT'])
check("event 1 min ahead (clock skew) -> silent",
      codes(ray7._rule_future_event(
          msg(60, 'arrived', dt.datetime.now() + dt.timedelta(minutes=1)), Ctx())), [])

print("\nVERDICT mapping")
check("info only -> accepted",
      ray7._verdict([ray7.Finding('X', ray7.INFO)]).status, ray7.ACCEPTED)
check("warning -> flagged",
      ray7._verdict([ray7.Finding('X', ray7.WARNING)]).status, ray7.FLAGGED)
check("critical wins over warning -> quarantined",
      ray7._verdict([ray7.Finding('X', ray7.WARNING),
                     ray7.Finding('Y', ray7.CRITICAL)]).status, ray7.QUARANTINED)
check("quarantined blocks projection",
      ray7._verdict([ray7.Finding('Y', ray7.CRITICAL)]).may_project, False)
check("flagged still projects",
      ray7._verdict([ray7.Finding('X', ray7.WARNING)]).may_project, True)

print("\n_as_naive — a parser emitting tz-aware timestamps must not disable a rule")
import datetime as _dt
_aware = _dt.datetime(2026, 9, 17, 9, 0, tzinfo=_dt.timezone(_dt.timedelta(hours=3)))
check("aware datetime is coerced to naive",
      ray7._as_naive(_aware).tzinfo, None)
check("naive datetime passes through unchanged",
      ray7._as_naive(T(9)), T(9))
check("None passes through", ray7._as_naive(None), None)
# The failure this guards: comparing aware vs naive raises TypeError, which
# fail-open would swallow, silently retiring FUTURE_EVENT forever.
_m = msg(60, 'arrived', _aware)
_m.event_time = ray7._as_naive(_m.event_time)
try:
    ray7._rule_future_event(_m, Ctx())
    check("FUTURE_EVENT survives an aware input once normalised", True, True)
except TypeError:
    check("FUTURE_EVENT survives an aware input once normalised", False, True)

print("\nduplicate window resolution")
_cfgs = {}
ray7._rule_config = lambda code, modality=None: _cfgs.get(
    code, {'enabled': True, 'severity': None, 'threshold_minutes': None})
_cfgs['LOGICAL_DUPLICATE'] = {'enabled': True, 'severity': None, 'threshold_minutes': 25}
_cfgs['REPEATED_TRANSITION'] = {'enabled': True, 'severity': None, 'threshold_minutes': 10}
check("LOGICAL_DUPLICATE's own threshold wins", ray7._duplicate_window(), 25)
_cfgs['LOGICAL_DUPLICATE'] = {'enabled': True, 'severity': None, 'threshold_minutes': None}
check("falls back to REPEATED_TRANSITION", ray7._duplicate_window(), 10)
_cfgs.clear()
check("falls back to the built-in default", ray7._duplicate_window(),
      ray7._DEFAULT_DUPLICATE_WINDOW_MIN)
ray7._rule_config = lambda code, modality=None: {
    'enabled': True, 'severity': None, 'threshold_minutes': 10}

print("\ncontent_hash")
check("CRLF vs CR reframing hashes the same",
      ray7.content_hash("MSH|a\r\nPID|b"), ray7.content_hash("MSH|a\rPID|b"))
check("different content hashes differently",
      ray7.content_hash("MSH|a") != ray7.content_hash("MSH|b"), True)

print(f"\n{'='*52}\n  {passed} passed, {failed} failed\n{'='*52}")
sys.exit(1 if failed else 0)
