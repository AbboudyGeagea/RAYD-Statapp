"""
The absence sweep: findings that can only be raised by waiting.

Thresholds are minutes measured on the HOSPITAL's clock — the same clock the HL7
timestamps are written in. The database server's own time zone must not move
them: a Postgres container left on UTC while the hospital is on Beirut time would
otherwise delay every stall by two or three hours.
"""
from datetime import timedelta

import pytest

import hl7kit as k

ALL_RUNGS = ['ordered', 'scheduled', 'arrived', 'started', 'completed',
             'signed_prelim', 'signed_final']


def stuck(pipe, acc, code, minutes, **kw):
    pipe.send(k.status(code, acc, k.ts(minutes), **kw))


# rule, rung it is stuck at, seeded threshold
STALLS = [
    ('STALLED_SCHEDULED', 'SC', 2880),
    ('STALLED_ARRIVED', 'AR', 240),
    ('STALLED_STARTED', 'IP', 180),
    ('UNREPORTED', 'CM', 1440),
]


@pytest.mark.parametrize('rule,code,threshold', STALLS)
def test_stall_fires_past_the_threshold(pipe, rule, code, threshold):
    stuck(pipe, 'A1', code, threshold + 20)
    raised = pipe.sweep()
    assert raised.get(rule) == 1
    f = pipe.findings(rule)[0]
    assert f['accession_number'] == 'A1' and f['message_archive_id'] is None


@pytest.mark.parametrize('rule,code,threshold', STALLS)
def test_stall_is_silent_before_the_threshold(pipe, rule, code, threshold):
    stuck(pipe, 'A1', code, threshold - 20)
    pipe.sweep()
    assert pipe.findings(rule) == []


@pytest.mark.parametrize('rule,code,threshold', STALLS)
def test_stall_minutes_are_reported_on_the_hospital_clock(pipe, rule, code, threshold):
    stuck(pipe, 'A1', code, threshold + 20)
    pipe.sweep()
    f = pipe.findings(rule)
    assert f, 'stall not raised at all'
    assert abs(float(f[0]['detail']['stalled_minutes']) - (threshold + 20)) <= 2


@pytest.mark.parametrize('rule,code,threshold', STALLS)
def test_one_open_finding_per_study_per_rule(pipe, rule, code, threshold):
    stuck(pipe, 'A1', code, threshold + 20)
    for _ in range(3):
        pipe.sweep()
    assert len(pipe.findings(rule)) == 1


@pytest.mark.parametrize('rule,code,next_msg', [
    ('STALLED_SCHEDULED', 'SC', lambda: k.status('AR', 'A1', k.ts(1))),
    ('STALLED_ARRIVED', 'AR', lambda: k.status('IP', 'A1', k.ts(1))),
    ('STALLED_STARTED', 'IP', lambda: k.status('CM', 'A1', k.ts(1))),
    ('UNREPORTED', 'CM', lambda: k.report('A1', k.ts(1))),
])
def test_stall_auto_resolves_when_the_awaited_event_arrives(pipe, rule, code, next_msg):
    threshold = dict((r, t) for r, _, t in STALLS)[rule]
    stuck(pipe, 'A1', code, threshold + 20)
    pipe.sweep()
    pipe.send(next_msg())
    pipe.sweep()
    f = pipe.findings(rule)
    assert len(f) == 1 and f[0]['resolved_at'] is not None
    assert f[0]['resolution'] == 'cleared'


def test_a_study_that_stalls_again_raises_a_fresh_finding(pipe):
    stuck(pipe, 'A1', 'AR', 260)
    pipe.sweep()
    pipe.exec("UPDATE ray7_findings SET resolved_at = NOW(), resolution = 'ignored'")
    pipe.sweep()
    assert len(pipe.findings('STALLED_ARRIVED')) == 2


@pytest.mark.parametrize('code', ['SC', 'AR', 'IP', 'CM'])
def test_cancelled_studies_never_stall(pipe, code):
    stuck(pipe, 'A1', code, 3000)
    pipe.send(k.status('CA', 'A1', k.ts(2999)))
    pipe.sweep()
    assert [f for f in pipe.findings() if f['message_archive_id'] is None] == []


def test_cancellation_resolves_an_open_stall(pipe):
    stuck(pipe, 'A1', 'AR', 260)
    pipe.sweep()
    pipe.send(k.status('CA', 'A1', k.ts(1)))
    pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED')[0]['resolved_at'] is not None


def test_completed_and_unreported_is_not_closed(pipe):
    """UNREPORTED was dead code until 0130: closure at completion made it
    unsatisfiable. A completed study must stay open until its final signature."""
    stuck(pipe, 'A1', 'CM', 1500)
    assert pipe.state('A1')['is_closed'] is False
    pipe.sweep()
    assert len(pipe.findings('UNREPORTED')) == 1


def test_finally_signed_study_is_never_unreported(pipe):
    stuck(pipe, 'A1', 'CM', 1500)
    pipe.send(k.report('A1', k.ts(1490)))
    pipe.sweep()
    assert pipe.findings('UNREPORTED') == []


def test_per_modality_threshold(pipe):
    pipe.exec("INSERT INTO ray7_rules (rule_code, modality, category, title, default_severity, "
              "threshold_minutes) VALUES ('STALLED_STARTED', 'DX', 'absence', 'x', 'warning', 30)")
    stuck(pipe, 'A-DX', 'IP', 60, mod='DX', ae='DR_RH')
    stuck(pipe, 'A-CT', 'IP', 60, mod='CT')
    pipe.sweep()
    assert [f['accession_number'] for f in pipe.findings('STALLED_STARTED')] == ['A-DX']


def test_disabled_absence_rule(pipe):
    pipe.rule('STALLED_ARRIVED', enabled=False)
    stuck(pipe, 'A1', 'AR', 300)
    pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED') == []


def test_threshold_is_operator_config(pipe):
    pipe.rule('STALLED_ARRIVED', threshold_minutes=30)
    stuck(pipe, 'A1', 'AR', 45)
    pipe.sweep()
    assert len(pipe.findings('STALLED_ARRIVED')) == 1


# ── the ladder profile bridges a disabled rung ────────────────────────────────

def test_disabled_started_rung_bridges_arrived_to_completed(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    stuck(pipe, 'A1', 'AR', 300)
    pipe.sweep()
    f = pipe.findings('STALLED_ARRIVED')
    assert len(f) == 1
    pipe.send(k.status('CM', 'A1', k.ts(1)))
    pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED')[0]['resolved_at'] is not None


def test_stalled_unsigned(pipe):
    pipe.profile(expected=['completed', 'signed_prelim', 'signed_final'])
    stuck(pipe, 'A1', 'CM', 4000)
    pipe.send(k.report('A1', k.ts(2900), result_status='W'))
    pipe.sweep()
    assert len(pipe.findings('STALLED_UNSIGNED')) == 1
    pipe.send(k.report('A1', k.ts(1), result_status='F'))
    pipe.sweep()
    assert pipe.findings('STALLED_UNSIGNED')[0]['resolved_at'] is not None


def test_with_signature_rungs_completion_waits_on_the_first_signature(pipe):
    pipe.profile(expected=['completed', 'signed_prelim', 'signed_final'])
    stuck(pipe, 'A1', 'CM', 1500)
    pipe.sweep()
    assert len(pipe.findings('UNREPORTED')) == 1


# ── orders the RIS never scheduled ────────────────────────────────────────────

def test_orphan_order_with_an_accession(pipe):
    pipe.send(k.order('PL1', acc='A1', when=k.ts(1500), msh_when=k.ts(1500)))
    pipe.sweep()
    assert len(pipe.findings('ORPHAN_ORDER')) == 1


def test_orphan_order_without_an_accession(pipe):
    """The normal shape of a HIS order is placer-only. An unscheduled one is still an
    orphan, and must be raised once — not once per sweep."""
    pipe.send(k.order('PL1', when=k.ts(1500), msh_when=k.ts(1500)))
    pipe.sweep()
    pipe.sweep()
    f = pipe.findings('ORPHAN_ORDER')
    assert len(f) == 1


def test_orphan_order_resolves_when_scheduled(pipe):
    pipe.send(k.order('PL1', when=k.ts(1500), msh_when=k.ts(1500)))
    pipe.sweep()
    pipe.send(k.status('SC', 'A1', k.ts(1), placer='PL1'))
    pipe.sweep()
    f = pipe.findings('ORPHAN_ORDER')
    assert f and all(x['resolved_at'] is not None for x in f)


def test_recent_order_is_not_an_orphan(pipe):
    pipe.send(k.order('PL1', acc='A1', when=k.ts(30), msh_when=k.ts(30)))
    pipe.sweep()
    assert pipe.findings('ORPHAN_ORDER') == []


# ── premature message findings ────────────────────────────────────────────────

def test_skipped_rung_resolves_once_the_rungs_turn_up(pipe):
    pipe.send(k.status('CM', 'A1', k.ts(100)))
    assert 'SKIPPED_RUNG' in pipe.codes()
    pipe.send(k.status('AR', 'A1', k.ts(140)))
    pipe.send(k.status('IP', 'A1', k.ts(120)))
    pipe.sweep()
    assert pipe.findings('SKIPPED_RUNG', open_only=True) == []


def test_orphan_event_resolves_once_introduced(pipe):
    pipe.send(k.status('AR', 'A1', k.ts(100)))
    pipe.send(k.status('SC', 'A1', k.ts(140)))
    pipe.sweep()
    assert pipe.findings('ORPHAN_EVENT', open_only=True) == []


def test_out_of_sequence_is_never_auto_resolved(pipe):
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    pipe.send(k.status('CM', 'A1', k.ts(100)))
    pipe.send(k.status('AR', 'A1', k.ts(140)))
    pipe.sweep()
    assert len(pipe.findings('OUT_OF_SEQUENCE_DELIVERY', open_only=True)) == 1


def test_human_resolution_is_left_alone(pipe):
    stuck(pipe, 'A1', 'AR', 300)
    pipe.sweep()
    pipe.exec("UPDATE ray7_findings SET resolved_at = NOW(), resolution = 'ignored', "
              "resolution_note = 'by hand'")
    pipe.send(k.status('IP', 'A1', k.ts(1)))
    pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED')[0]['resolution'] == 'ignored'


def test_sweep_on_an_empty_database(pipe):
    assert pipe.sweep() == {}
