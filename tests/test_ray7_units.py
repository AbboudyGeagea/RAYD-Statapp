"""
RAY7's pure functions, without the pipeline: the quality conditions, which rungs are
measured, the verdict mapping, stall bridging, hashing. Replaces the offline
tests/test_ray7_rules.py script, whose rule functions the four-layer rebuild retired.

The ladder profile is pinned straight into RAY7's cache: _cached() serves a fresh
entry without calling its loader.
"""
import datetime as dt
import time

import pytest

from utils import ray7
from utils import ray7_quality as quality
from utils import ray7_sweep as sweep

T = lambda h, m=0: dt.datetime(2026, 9, 17, h, m)
_RUNGS = [('ordered', 20), ('scheduled', 40), ('arrived', 60), ('started', 70),
          ('completed', 100), ('signed_prelim', 120), ('signed_final', 140)]
ALL = [r for r, _ in _RUNGS]


def profile(expected=(), enforced=()):
    ray7._cache['ladder'] = (time.time(), {
        rung: {'rung': rung, 'ladder_rank': rank, 'label': rung,
               'expected': rung in expected, 'enforce_order': rung in enforced}
        for rung, rank in _RUNGS})


def cond(**state):
    return quality.conditions(state, ray7.is_expected)


@pytest.fixture(autouse=True)
def unconfigured(app):
    profile()
    yield


# ── TIME_CONTRADICTION ────────────────────────────────────────────────────────

@pytest.mark.parametrize('state,want', [
    ({'arrived_at': T(10), 'completed_at': T(8)}, [('arrived', 'completed')]),
    ({'scheduled_at': T(9), 'started_at': T(8)}, [('scheduled', 'started')]),
    ({'completed_at': T(10), 'signed_final_at': T(9)}, [('completed', 'signed_final')]),
    ({'ordered_at': T(9), 'scheduled_at': T(8)}, [('ordered', 'scheduled')]),
    ({'arrived_at': T(8), 'completed_at': T(10)}, []),
    ({'arrived_at': T(10), 'completed_at': T(10)}, []),
    ({'signed_final_at': T(9), 'signed_prelim_at': T(10)}, []),     # addendum shape
    ({'arrived_at': T(10), 'started_at': T(9), 'completed_at': T(8)},
     [('arrived', 'started'), ('arrived', 'completed'), ('started', 'completed')]),
])
def test_time_contradiction(state, want):
    got = cond(**state).get('TIME_CONTRADICTION', {}).get('pairs', [])
    assert [(p['earlier_rung'], p['later_rung']) for p in got] == want


def test_cancelled_study_has_no_conditions():
    assert cond(arrived_at=T(10), completed_at=T(8), cancelled_at=T(11)) == {}


# ── SKIPPED_RUNG ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize('state,missing', [
    ({'completed_at': T(12)}, ['arrived', 'started']),
    ({'arrived_at': T(9), 'completed_at': T(12)}, ['started']),
    ({'started_at': T(10), 'completed_at': T(12)}, ['arrived']),
    ({'arrived_at': T(9), 'started_at': T(10), 'completed_at': T(12)}, None),
    ({'arrived_at': T(9)}, None),                                    # not completed yet
])
def test_skipped_rung(state, missing):
    got = cond(**state).get('SKIPPED_RUNG')
    assert (got['missing_rungs'] if got else None) == missing


def test_skipped_rung_ignores_a_rung_the_site_does_not_send():
    profile(expected=['scheduled', 'arrived', 'completed'])
    assert 'SKIPPED_RUNG' not in cond(arrived_at=T(9), completed_at=T(12))


# ── ORPHAN_EVENT ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize('state,orphan', [
    ({'arrived_at': T(9)}, True),
    ({'completed_at': T(9)}, True),
    ({'scheduled_at': T(8), 'arrived_at': T(9)}, False),
    ({'ordered_at': T(8), 'completed_at': T(9)}, False),
    ({'signed_final_at': T(9)}, False),                              # report only
    ({}, False),
])
def test_orphan_event(state, orphan):
    assert ('ORPHAN_EVENT' in cond(**state)) is orphan


def test_orphan_event_off_without_an_introducing_rung():
    profile(expected=['arrived', 'started', 'completed'])
    assert 'ORPHAN_EVENT' not in cond(arrived_at=T(9))


# ── the ladder profile ────────────────────────────────────────────────────────

def test_unconfigured_profile_expects_every_rung():
    assert all(ray7.is_expected(rank) for _, rank in _RUNGS)
    assert ray7.profile_configured() is False


def test_configured_profile_expects_only_what_is_switched_on():
    profile(expected=['arrived'])
    assert ray7.is_expected(60) and not ray7.is_expected(70)


def test_unknown_rank_counts_as_expected():
    profile(expected=['arrived'])
    assert ray7.is_expected(999)


def test_measured_ranks_unconfigured_is_everything():
    assert ray7.measured_ranks() == {20, 40, 60, 70, 100, 120, 140}


def test_measured_ranks_follow_the_expected_rungs():
    profile(expected=['arrived', 'completed'])
    assert ray7.measured_ranks() == {60, 100}


def test_measured_ranks_marked_explicitly():
    profile(expected=ALL, enforced=['completed'])
    assert ray7.measured_ranks() == {100}


# ── the verdict ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize('severities,status', [
    ([], 'accepted'), (['info'], 'accepted'), (['warning'], 'accepted'),
    (['critical'], 'quarantined'), (['info', 'critical'], 'quarantined'),
])
def test_only_critical_holds(severities, status):
    v = ray7._verdict([ray7.Finding('X', s) for s in severities])
    assert v.status == status and v.may_project is (status == 'accepted')


def test_observations_never_hold():
    v = ray7._verdict([], observed=['OUT_OF_SEQUENCE_DELIVERY', 'CONTROL_ID_REUSE'])
    assert v.status == 'accepted'


def test_summary_line():
    v = ray7._verdict([ray7.Finding('UNKNOWN_STATUS_CODE', 'critical')], observed=['X'])
    assert ray7.summary(v) == 'RAY7 held: UNKNOWN_STATUS_CODE | observed: X'
    assert ray7.summary(ray7._verdict([])) == 'RAY7 accepted'


# ── intake as pure functions ──────────────────────────────────────────────────

def test_missing_identifier():
    assert ray7._intake_missing_identifier(ray7.ParsedMessage(kind='status')).rule_code \
        == 'MISSING_IDENTIFIER'
    assert ray7._intake_missing_identifier(
        ray7.ParsedMessage(kind='status', placer_order_number='P1')) is None
    assert ray7._intake_missing_identifier(ray7.ParsedMessage(kind='adt')) is None


def test_unknown_status_needs_only_a_missing_rung():
    assert ray7._intake_unknown_status(ray7.ParsedMessage(kind='status')).rule_code \
        == 'UNKNOWN_STATUS_CODE'
    assert ray7._intake_unknown_status(ray7.ParsedMessage(kind='status', ladder_rank=60)) is None


# ── stall bridging (the sweep) ────────────────────────────────────────────────

def bridge(expected):
    profile(expected=expected)
    return {r['code']: r['where'] for r in sweep._stall_rules()}


def test_full_ladder_without_signatures_waits_on_any_report():
    b = bridge(['scheduled', 'arrived', 'started', 'completed'])
    assert b['STALLED_ARRIVED'] == "s.arrived_at IS NOT NULL AND s.started_at IS NULL"
    assert b['UNREPORTED'] == "s.completed_at IS NOT NULL AND s.reported_at IS NULL"


def test_disabled_started_bridges_arrived_to_completed():
    b = bridge(['scheduled', 'arrived', 'completed'])
    assert b['STALLED_ARRIVED'] == "s.arrived_at IS NOT NULL AND s.completed_at IS NULL"
    assert 'STALLED_STARTED' not in b


def test_signature_rungs():
    b = bridge(['completed', 'signed_prelim', 'signed_final'])
    assert b['UNREPORTED'] == "s.completed_at IS NOT NULL AND s.signed_prelim_at IS NULL"
    assert b['STALLED_UNSIGNED'] == \
        "s.signed_prelim_at IS NOT NULL AND s.signed_final_at IS NULL"


def test_unconfigured_keeps_the_original_four_stall_rules():
    profile()
    assert [r['code'] for r in sweep._stall_rules()] == [
        'STALLED_SCHEDULED', 'STALLED_ARRIVED', 'STALLED_STARTED', 'UNREPORTED']


# ── odds and ends ─────────────────────────────────────────────────────────────

def test_content_hash_ignores_reframing():
    assert ray7.content_hash("MSH|a\r\nPID|b") == ray7.content_hash("MSH|a\rPID|b")
    assert ray7.content_hash("MSH|a") != ray7.content_hash("MSH|b")


def test_as_naive():
    aware = dt.datetime(2026, 9, 17, 9, tzinfo=dt.timezone(dt.timedelta(hours=3)))
    assert ray7._as_naive(aware).tzinfo is None
    assert ray7._as_naive(T(9)) == T(9)
    assert ray7._as_naive(None) is None


def test_duplicate_window_resolution(monkeypatch):
    cfgs = {}
    monkeypatch.setattr(ray7, '_rule_config', lambda code, modality=None: cfgs.get(
        code, {'enabled': True, 'severity': None, 'threshold_minutes': None}))
    cfgs['LOGICAL_DUPLICATE'] = {'threshold_minutes': 25}
    cfgs['REPEATED_TRANSITION'] = {'threshold_minutes': 10}
    assert ray7._duplicate_window() == 25
    cfgs['LOGICAL_DUPLICATE'] = {'threshold_minutes': None}
    assert ray7._duplicate_window() == 10
    cfgs.clear()
    assert ray7._duplicate_window() == ray7._DEFAULT_DUPLICATE_WINDOW_MIN
