"""
Delivery order must not change the data.

The interface is not ours: queues back up, a Mirth hub redelivers, two systems race
each other. Whatever order a study's messages arrive in, the projected study has
to come out the same — that is what makes the reports trustworthy at all. These
tests deliver every permutation of a lifecycle and compare the result against what
in-order delivery produces.

With the ladder enforced, out-of-order delivery IS reported (quarantine, by design),
so there the property is different: the number of OUT_OF_SEQUENCE_DELIVERY
findings is exactly predictable, and acknowledging them releases a study identical
to the in-order one.
"""
import itertools
import random
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()
ACC, PLACER = 'ACC-PERM', 'PL-PERM'
RIS = ['SC', 'AR', 'IP', 'CM']
MIN = {'NW': 320, 'SC': 300, 'AR': 280, 'IP': 260, 'CM': 240, 'W': 200, 'F': 150}
RANK = {'NW': 20, 'SC': 40, 'AR': 60, 'IP': 70, 'CM': 100, 'W': 120, 'F': 140}
ALL_RUNGS = ['ordered', 'scheduled', 'arrived', 'started', 'completed',
             'signed_prelim', 'signed_final']


def at(m):
    return BASE - timedelta(minutes=m)


def build(code, ctrl=None):
    """One message of the lifecycle, deterministic given its code."""
    ctrl = ctrl or f'PERM-{code}'
    if code == 'NW':
        return k.order(PLACER, when=k.hl7(at(MIN['NW'])), ctrl=ctrl)
    if code in ('W', 'F'):
        return k.report(ACC, k.hl7(at(MIN[code])), result_status=code, ctrl=ctrl,
                        text=f'{code} report')
    placer = PLACER if code == 'SC' else None
    return k.status(code, ACC, k.hl7(at(MIN[code])), placer=placer, ctrl=ctrl)


def summary(pipe):
    st = pipe.state(ACC) or {}
    s = pipe.study(ACC) or {}
    keep_st = ('ordered_at', 'scheduled_at', 'arrived_at', 'started_at', 'completed_at',
               'signed_prelim_at', 'signed_final_at', 'reported_at', 'current_rank',
               'is_closed', 'is_provisional', 'placer_order_number', 'patient_id',
               'modality', 'aetitle')
    keep_s = ('study_date', 'insert_time', 'order_status', 'study_status',
              'study_has_report', 'rep_final_timestamp', 'storing_ae', 'study_modality')
    return {
        'state': {c: st.get(c) for c in keep_st},
        'study': {c: s.get(c) for c in keep_s},
        'state_rows': pipe.count('ray7_study_state'),
        'provisional_rows': pipe.count('ray7_study_state', 'is_provisional'),
        'study_rows': pipe.count('etl_didb_studies'),
        'order_rows': pipe.count('etl_orders'),
        'unlinked_events': pipe.count('hl7_study_events', 'accession_number IS NULL'),
    }


def deliver(pipe, codes):
    for c in codes:
        assert pipe.send(build(c))


def expected(codes):
    """What in-order delivery of `codes` produces, from a clean database."""
    return _EXPECTED[tuple(sorted(codes, key=RANK.get))]


_EXPECTED = {}


@pytest.fixture
def baseline(pipe, request):
    def get(codes):
        key = tuple(sorted(codes, key=RANK.get))
        if key not in _EXPECTED:
            deliver(pipe, key)
            _EXPECTED[key] = summary(pipe)
            from conftest import DATA_TABLES
            for t in DATA_TABLES:
                pipe.exec(f'DELETE FROM {t}')
        return _EXPECTED[key]
    return get


# ── unconfigured ladder: order must not matter at all ─────────────────────────

@pytest.mark.parametrize('order', list(itertools.permutations(RIS)), ids='-'.join)
def test_ris_lifecycle_any_order(pipe, baseline, order):
    want = baseline(RIS)
    deliver(pipe, order)
    assert summary(pipe) == want


@pytest.mark.parametrize('order', list(itertools.permutations(RIS)), ids='-'.join)
def test_premature_findings_are_cleared_once_the_rest_arrives(pipe, order):
    """SKIPPED_RUNG and ORPHAN_EVENT are true when raised against a half-delivered
    lifecycle; the sweep closes them once the missing rungs turn up. Nothing above
    info may be left open for a study whose data is in fact complete."""
    deliver(pipe, order)
    pipe.sweep()
    left = [f['rule_code'] for f in pipe.findings(open_only=True) if f['severity'] != 'info']
    assert left == []


@pytest.mark.parametrize('order', list(itertools.permutations(['NW', 'SC', 'AR', 'CM'])),
                         ids='-'.join)
def test_provisional_order_any_order(pipe, baseline, order):
    """The HIS order (placer only) and the scheduling message (placer + accession)
    in every relative order: always ONE study, ordered_at kept, nothing provisional
    left behind, no event without an accession."""
    want = baseline(['NW', 'SC', 'AR', 'CM'])
    assert want['state_rows'] == 1 and want['provisional_rows'] == 0
    deliver(pipe, order)
    assert summary(pipe) == want


@pytest.mark.parametrize('order', list(itertools.permutations(['CM', 'W', 'F'])), ids='-'.join)
def test_completion_and_signatures_any_order(pipe, baseline, order):
    want = baseline(['SC', 'AR', 'IP', 'CM', 'W', 'F'])
    deliver(pipe, ['SC', 'AR', 'IP'] + list(order))
    assert summary(pipe) == want


# ── enforced ladder: delivery order is reported, exactly ─────────────────────

def _expected_oos(order):
    """Messages delivered below the highest rank already ACCEPTED. Quarantined ones
    never advance the state, so the running maximum only counts accepted rungs."""
    best, n = 0, 0
    for c in order:
        if RANK[c] < best:
            n += 1
        else:
            best = RANK[c]
    return n


@pytest.mark.parametrize('order', list(itertools.permutations(RIS)), ids='-'.join)
def test_enforced_ladder_reports_each_late_message(pipe, order):
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    deliver(pipe, order)
    assert len(pipe.findings('OUT_OF_SEQUENCE_DELIVERY')) == _expected_oos(order)
    held = pipe.count('hl7_message_archive', "ray7_status = 'quarantined'")
    assert held == _expected_oos(order)


@pytest.mark.parametrize('order', list(itertools.permutations(RIS)), ids='-'.join)
def test_acknowledging_releases_the_complete_study(pipe, baseline, client, order):
    want = baseline(RIS)
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    deliver(pipe, order)
    r = client.post(f'/ray7/study/{ACC}/acknowledge', json={'note': 'test'},
                    headers={'X-Test-Role': 'su'})
    assert r.status_code == 200, r.get_json()
    assert pipe.count('hl7_message_archive', "ray7_status = 'quarantined'") == 0
    assert summary(pipe) == want


# ── chaos: shuffled, redelivered, repeated ────────────────────────────────────

@pytest.mark.parametrize('seed', range(30))
def test_chaos_converges(pipe, baseline, seed):
    """Random order, random byte-identical redeliveries, and a second Arrived stamped
    later than the first (this RIS genuinely emits repeats). The first arrival is
    the one that counts, whichever is delivered first."""
    codes = ['NW', 'SC', 'AR', 'IP', 'CM', 'W', 'F']
    want = baseline(codes)
    rnd = random.Random(seed)
    stream = [build(c) for c in codes]
    stream.append(k.status('AR', ACC, k.hl7(at(MIN['AR'] - 30)), ctrl='PERM-AR-REPEAT'))
    stream += [rnd.choice(stream) for _ in range(rnd.randint(0, 5))]
    rnd.shuffle(stream)
    for raw in stream:
        assert pipe.send(raw)
    assert summary(pipe) == want
