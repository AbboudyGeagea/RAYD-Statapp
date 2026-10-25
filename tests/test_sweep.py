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


@pytest.mark.parametrize('rule,first,later', [
    ('STALLED_ARRIVED', 'AR', 'CM'),        # never started, but completed
    ('STALLED_SCHEDULED', 'SC', 'IP'),      # never arrived, but started
    ('STALLED_SCHEDULED', 'SC', 'CM'),
])
def test_a_study_past_a_skipped_rung_is_not_stalled_there(pipe, rule, first, later):
    """It used to flap: raised because the NEXT rung was missing, auto-resolved in the
    same pass because a LATER one was there — a fresh closed note every sweep (20 on
    one study in five hours on localhost, 2026-10-25)."""
    threshold = dict((r, t) for r, _, t in STALLS)[rule]
    stuck(pipe, 'A1', first, threshold + 20)
    pipe.send(k.status(later, 'A1', k.ts(threshold + 10)))
    for _ in range(3):
        pipe.sweep()
    assert pipe.findings(rule) == []


def test_skipped_rung_does_not_flap_with_a_configured_ladder(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'started', 'completed'])
    stuck(pipe, 'A1', 'AR', 300)
    pipe.send(k.status('CM', 'A1', k.ts(290)))
    for _ in range(3):
        pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED') == []


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
    assert pipe.findings(open_only=True) == []


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
              "threshold_minutes) VALUES ('STALLED_STARTED', 'DX', 'workflow', 'x', 'warning', 30)")
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


# ── ③ interface alerts: a sender's rate against its threshold ────────────────

def seed_rate(pipe, metric, n, messages, app='RIS'):
    """Counters as the listener writes them, in the current quarter-hour."""
    bucket = ("date_trunc('hour', LOCALTIMESTAMP) + floor(extract(minute FROM LOCALTIMESTAMP)"
              " / 15) * interval '15 minutes'")
    for m, v in (('MESSAGES', messages), (metric, n)):
        pipe.exec(f"INSERT INTO ray7_interface_stats (bucket_start, sending_app, metric, n) "
                  f"VALUES ({bucket}, :app, :m, :v) ON CONFLICT (bucket_start, sending_app, metric) "
                  f"DO UPDATE SET n = EXCLUDED.n", app=app, m=m, v=v)


def alerts(pipe, rule=None):
    return [f for f in pipe.findings(rule=rule)
            if f['message_archive_id'] is None and f['accession_number'] is None]


def test_alert_is_raised_past_the_threshold(pipe):
    seed_rate(pipe, 'OUT_OF_SEQUENCE_DELIVERY', 30, 100)        # 30% vs 10%
    pipe.sweep()
    a = alerts(pipe, 'OUT_OF_SEQUENCE_DELIVERY')
    assert len(a) == 1 and a[0]['sending_app'] == 'RIS'
    assert (a[0]['detail']['rate_pct'], a[0]['detail']['threshold_pct']) == (30.0, 10.0)


def test_no_alert_under_the_threshold(pipe):
    seed_rate(pipe, 'OUT_OF_SEQUENCE_DELIVERY', 5, 100)
    pipe.sweep()
    assert alerts(pipe) == []


def test_no_alert_on_too_little_traffic(pipe):
    seed_rate(pipe, 'CONTROL_ID_REUSE', 5, 10)                  # 50%, but 10 messages
    pipe.sweep()
    assert alerts(pipe) == []


def test_one_open_alert_per_sender_per_rule(pipe):
    seed_rate(pipe, 'CONTROL_ID_REUSE', 10, 100)
    for _ in range(3):
        pipe.sweep()
    assert len(alerts(pipe, 'CONTROL_ID_REUSE')) == 1


def test_alerts_are_per_sender(pipe):
    seed_rate(pipe, 'CONTROL_ID_REUSE', 10, 100, app='RIS_A')
    seed_rate(pipe, 'CONTROL_ID_REUSE', 0, 100, app='RIS_B')
    pipe.sweep()
    assert [a['sending_app'] for a in alerts(pipe)] == ['RIS_A']


def test_alert_clears_when_the_rate_recovers(pipe):
    seed_rate(pipe, 'OUT_OF_SEQUENCE_DELIVERY', 30, 100)
    pipe.sweep()
    seed_rate(pipe, 'OUT_OF_SEQUENCE_DELIVERY', 1, 200)
    pipe.sweep()
    a = alerts(pipe, 'OUT_OF_SEQUENCE_DELIVERY')
    assert len(a) == 1 and a[0]['resolved_at'] is not None


def test_alert_stays_open_while_the_sender_is_quiet(pipe):
    seed_rate(pipe, 'OUT_OF_SEQUENCE_DELIVERY', 30, 100)
    pipe.sweep()
    pipe.exec("DELETE FROM ray7_interface_stats")
    pipe.sweep()
    assert alerts(pipe)[0]['resolved_at'] is None


def test_redelivery_is_measured_not_alerted_by_default(pipe):
    seed_rate(pipe, 'EXACT_REDELIVERY', 90, 100)
    pipe.sweep()
    assert alerts(pipe) == []


def test_alert_threshold_is_operator_config(pipe):
    pipe.exec("UPDATE ray7_rules SET threshold_pct = 50 WHERE rule_code = 'EXACT_REDELIVERY'")
    seed_rate(pipe, 'EXACT_REDELIVERY', 90, 100)
    pipe.sweep()
    assert len(alerts(pipe, 'EXACT_REDELIVERY')) == 1


def test_disabled_rule_raises_no_alert(pipe):
    pipe.rule('CONTROL_ID_REUSE', enabled=False)
    seed_rate(pipe, 'CONTROL_ID_REUSE', 50, 100)
    pipe.sweep()
    assert alerts(pipe) == []


def test_real_traffic_raises_the_alert(pipe):
    """End to end: 25 studies whose completion overtakes their arrival."""
    for i in range(25):
        pipe.send(k.status('CM', f'O{i:02d}', k.ts(100)))
        pipe.send(k.status('AR', f'O{i:02d}', k.ts(140)))
    pipe.sweep()
    a = alerts(pipe, 'OUT_OF_SEQUENCE_DELIVERY')
    assert len(a) == 1 and a[0]['detail']['rate_pct'] == 50.0


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
