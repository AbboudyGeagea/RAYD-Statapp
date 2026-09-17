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
