"""
Every inline RAY7 rule, driven through the real ingest path rather than called as a
function. What matters is not only that a rule fires, but what its verdict then does
to the data: accepted and flagged project, quarantined is held back, and nothing is
ever lost.
"""
import threading
import time
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


ALL_RUNGS = ['ordered', 'scheduled', 'arrived', 'started', 'completed',
             'signed_prelim', 'signed_final']
RIS_RUNGS = ['scheduled', 'arrived', 'started', 'completed']


def verdicts(pipe):
    return [r['ray7_status'] for r in pipe.archive()]


# ── referential: no identity, no classification ──────────────────────────────

def test_missing_identifier_is_quarantined_and_kept(pipe):
    raw = k.message(k.msh('RIS'), k.pid(), k.orc('SC', status='AR', when=k.ts(5)),
                    k.obr(code='X'))
    assert pipe.send(raw) is True
    assert verdicts(pipe) == ['quarantined']
    assert pipe.codes() == ['MISSING_IDENTIFIER']
    assert pipe.count('hl7_study_events') == 0
    assert pipe.archive()[0]['raw_message'] == raw


def test_order_with_no_identifier_is_quarantined(pipe):
    raw = k.message(k.msh('SAP_HIS'), k.pid(), k.orc('NW', when=k.ts(5)), k.obr())
    pipe.send(raw)
    assert pipe.codes() == ['MISSING_IDENTIFIER']


def test_unknown_status_code_is_quarantined(pipe):
    pipe.send(k.status('ZZ', 'A1', k.ts(5)))
    f = pipe.findings('UNKNOWN_STATUS_CODE')
    assert len(f) == 1 and f[0]['severity'] == 'critical'
    assert f[0]['detail']['order_status'] == 'ZZ'
    assert verdicts(pipe) == ['quarantined']
    assert pipe.state('A1') is None and pipe.study('A1') is None


def test_unknown_status_code_recovers_by_mapping_and_replay(pipe):
    pipe.send(k.status('ZZ', 'A1', k.hl7(at(5))))
    pipe.exec("INSERT INTO hl7_status_map (order_status, canonical_state, ladder_rank) "
              "VALUES ('ZZ', 'arrived', 60)")
    from conftest import _reset_caches
    _reset_caches()
    pipe.replay()
    assert pipe.state('A1')['arrived_at'] == at(5)
    assert pipe.findings('UNKNOWN_STATUS_CODE') == []
    assert verdicts(pipe) != ['quarantined']


def test_status_message_nothing_can_classify_is_not_silently_dropped(pipe):
    """ORC-1 XO (order changed) with ORC-5 empty: no rung, no state — it must at
    least be visible as a finding, not vanish as an accepted no-op."""
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    raw = k.message(k.msh('RIS'), k.pid(), k.orc('XO', acc='A1', when=k.ts(5)), k.obr(acc='A1'))
    pipe.send(raw)
    assert 'UNKNOWN_STATUS_CODE' in pipe.codes()


def test_sender_specific_mapping_beats_the_global_one(pipe):
    pipe.exec("INSERT INTO hl7_status_map (sending_app, order_status, canonical_state, ladder_rank) "
              "VALUES ('RIS2', 'AR', 'started', 70)")
    pipe.send(k.status('AR', 'A1', k.hl7(at(10)), app='RIS2'))
    pipe.send(k.status('AR', 'A2', k.hl7(at(10)), app='RIS'))
    assert pipe.state('A1')['started_at'] == at(10)
    assert pipe.state('A2')['arrived_at'] == at(10)


def test_inactive_mapping_is_ignored(pipe):
    pipe.exec("UPDATE hl7_status_map SET active = FALSE WHERE order_status = 'AR'")
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.codes() == ['UNKNOWN_STATUS_CODE']


# ── duplicates ────────────────────────────────────────────────────────────────

def test_exact_redelivery_is_one_archive_row(pipe):
    raw = k.status('AR', 'A1', k.ts(10))
    assert all(pipe.send(raw) for _ in range(4))
    assert pipe.count('hl7_message_archive') == 1
    f = pipe.findings('EXACT_REDELIVERY')
    assert len(f) == 1 and f[0]['detail']['seen'] == 3 and f[0]['severity'] == 'info'
    assert pipe.count('hl7_study_events') == 1


def test_redelivery_reframed_with_crlf_is_still_a_redelivery(pipe):
    raw = k.status('AR', 'A1', k.ts(10))
    pipe.send(raw)
    pipe.send(raw.replace('\r', '\r\n'))
    assert pipe.count('hl7_message_archive') == 1


def test_redelivery_never_changes_the_verdict(pipe):
    raw = k.status('SC', 'A1', k.ts(10))
    pipe.send(raw)
    pipe.send(raw)
    assert verdicts(pipe) == ['accepted']


def test_control_id_reuse_with_different_content_is_flagged(pipe):
    pipe.send(k.status('AR', 'A1', k.ts(10), ctrl='SAME-1'))
    pipe.send(k.status('AR', 'A2', k.ts(9), ctrl='SAME-1', pat='P2'))
    assert pipe.count('hl7_message_archive') == 2
    f = pipe.findings('CONTROL_ID_REUSE')
    assert len(f) == 1 and f[0]['accession_number'] == 'A2'


def test_control_id_reuse_across_senders_is_not_reuse(pipe):
    pipe.send(k.status('AR', 'A1', k.ts(10), ctrl='SAME-1', app='RIS'))
    pipe.send(k.status('AR', 'A2', k.ts(9), ctrl='SAME-1', app='OTHER_RIS'))
    assert pipe.findings('CONTROL_ID_REUSE') == []


def test_two_senders_sharing_a_control_id_keep_both_orders(pipe):
    """hl7_orders is keyed on MSH-10 alone: two systems that both number from 1
    overwrite each other's orders."""
    pipe.send(k.order('PL-A', when=k.ts(10), ctrl='1001', app='HIS_A'))
    pipe.send(k.order('PL-B', when=k.ts(9), ctrl='1001', app='HIS_B', pat='P2'))
    assert {r['placer_order_number'] for r in pipe.q("SELECT placer_order_number FROM hl7_orders")} \
        == {'PL-A', 'PL-B'}


def test_logical_duplicate_within_the_window(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(30))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(27))))
    f = pipe.findings('LOGICAL_DUPLICATE')
    assert len(f) == 1 and f[0]['severity'] == 'warning'
    assert verdicts(pipe) == ['accepted', 'flagged']
    assert pipe.state('A1')['scheduled_at'] == at(30)


def test_repeat_outside_the_window_is_informational(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(100))))
    assert pipe.codes() == ['REPEATED_TRANSITION']
    assert verdicts(pipe) == ['accepted', 'accepted']
    assert pipe.findings('REPEATED_TRANSITION')[0]['detail']['gap_minutes'] == 200.0


def test_duplicate_window_is_operator_config(pipe):
    pipe.rule('LOGICAL_DUPLICATE', threshold_minutes=300)
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(100))))
    assert pipe.codes() == ['LOGICAL_DUPLICATE']


# ── sequence ──────────────────────────────────────────────────────────────────

def test_late_delivery_with_consistent_times_is_silent_when_unconfigured(pipe):
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    assert 'LADDER_REGRESSION' not in pipe.codes()
    assert 'OUT_OF_SEQUENCE_DELIVERY' not in pipe.codes()


def test_ladder_regression_when_the_timestamps_contradict(pipe):
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(50))))       # arrived AFTER completion
    assert 'LADDER_REGRESSION' in pipe.codes()
    assert verdicts(pipe)[-1] == 'flagged'


def test_time_contradiction(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))      # completed BEFORE arrival
    f = pipe.findings('TIME_CONTRADICTION')
    assert len(f) == 1
    assert f[0]['detail']['earlier_rungs_stamped_later'][0]['rank'] == 60


def test_skipped_rung(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(200))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    f = pipe.findings('SKIPPED_RUNG')
    assert len(f) == 1 and f[0]['detail']['missing_rungs'] == ['arrived', 'started']
    assert pipe.study('A1')['order_status'] == 'CM'          # flagged still projects


def test_skipped_rung_respects_a_disabled_rung(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    pipe.send(k.status('AR', 'A1', k.hl7(at(200))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    assert 'SKIPPED_RUNG' not in pipe.codes()


def test_future_event(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(days=2))))
    assert pipe.codes() == ['FUTURE_EVENT']


def test_a_minute_of_clock_skew_is_tolerated(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(minutes=1))))
    assert pipe.codes() == []


def test_future_tolerance_is_operator_config(pipe):
    pipe.rule('FUTURE_EVENT', threshold_minutes=60 * 24 * 3)
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(days=2))))
    assert pipe.codes() == []


def test_orphan_event(pipe):
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.codes() == ['ORPHAN_EVENT']


@pytest.mark.parametrize('code', ['SC', 'CA'])
def test_orphan_event_exempts_rungs_that_can_open_a_study(pipe, code):
    pipe.send(k.status(code, 'A1', k.ts(10)))
    assert 'ORPHAN_EVENT' not in pipe.codes()


# ── referential: devices, placeholders ────────────────────────────────────────

def test_unknown_aetitle_is_informational(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(20)))
    pipe.send(k.status('IP', 'A1', k.ts(10), ae='NOT_A_DEVICE'))
    assert 'UNKNOWN_AETITLE' in pipe.codes()
    assert verdicts(pipe)[-1] == 'accepted'


@pytest.mark.parametrize('ae', ['EBW', 'ebw', 'Ebw'])
def test_known_aetitle_any_case(pipe, ae):
    pipe.send(k.status('SC', 'A1', k.ts(10), ae=ae))
    assert 'UNKNOWN_AETITLE' not in pipe.codes()


def test_placeholder_patient(pipe):
    pipe.send(k.adt('A01', pat='QR001', dob='99991111', sex='U'))
    assert pipe.codes() == ['PLACEHOLDER_DATA']
    p = pipe.one("SELECT * FROM hl7_patients WHERE patient_id = 'QR001'")
    assert p['birth_date'] is None and p['sex'] == 'U'


# ── the ladder profile ────────────────────────────────────────────────────────

def test_unexpected_rung(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    pipe.send(k.status('IP', 'A1', k.ts(10)))
    assert 'UNEXPECTED_RUNG' in pipe.codes()


def test_unconfigured_profile_never_floods_unexpected_rung(pipe):
    for _, raw in k.lifecycle('A1', 100):
        pipe.send(raw)
    assert 'UNEXPECTED_RUNG' not in pipe.codes()


def test_unknown_result_status_when_signatures_are_tracked(pipe):
    pipe.profile(expected=RIS_RUNGS + ['signed_prelim', 'signed_final'])
    pipe.send(k.report('A1', k.ts(5), obx_statuses=['X']))
    f = pipe.findings('UNKNOWN_RESULT_STATUS')
    assert len(f) == 1 and f[0]['detail']['result_status'] == 'X'
    assert pipe.one("SELECT * FROM hl7_oru_reports WHERE accession_number='A1'") is not None


def test_unknown_result_status_silent_without_signature_rungs(pipe):
    pipe.send(k.report('A1', k.ts(5), obx_statuses=['X']))
    assert 'UNKNOWN_RESULT_STATUS' not in pipe.codes()


def test_out_of_sequence_delivery_is_silent_by_default(pipe):
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    assert 'OUT_OF_SEQUENCE_DELIVERY' not in pipe.codes()


def test_out_of_sequence_delivery_quarantines_when_enforced(pipe):
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    f = pipe.findings('OUT_OF_SEQUENCE_DELIVERY')
    assert len(f) == 1 and f[0]['severity'] == 'critical'
    assert verdicts(pipe)[1] == 'quarantined'
    assert pipe.state('A1')['arrived_at'] is None          # withheld


def test_out_of_sequence_and_regression_are_mutually_exclusive(pipe):
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(50))))
    codes = pipe.codes()
    assert 'LADDER_REGRESSION' in codes and 'OUT_OF_SEQUENCE_DELIVERY' not in codes


@pytest.mark.parametrize('unenforced', ['arrived', 'completed'])
def test_out_of_sequence_needs_both_rungs_enforced(pipe, unenforced):
    pipe.profile(expected=ALL_RUNGS, enforced=[r for r in ALL_RUNGS if r != unenforced])
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    assert 'OUT_OF_SEQUENCE_DELIVERY' not in pipe.codes()


def test_signature_delivered_out_of_order(pipe):
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    for _, raw in k.lifecycle('A1', 300):
        pipe.send(raw)
    pipe.send(k.report('A1', k.hl7(at(100)), result_status='F'))
    pipe.send(k.report('A1', k.hl7(at(150)), result_status='W'))
    assert 'OUT_OF_SEQUENCE_DELIVERY' in pipe.codes()


# ── configuration: the operator's knobs actually turn ─────────────────────────

def test_disabled_rule_stays_silent(pipe):
    pipe.rule('ORPHAN_EVENT', enabled=False)
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.codes() == []


def test_severity_override_changes_the_verdict(pipe):
    pipe.rule('UNKNOWN_STATUS_CODE', severity='warning')
    pipe.send(k.status('ZZ', 'A1', k.ts(10)))
    assert verdicts(pipe) == ['flagged']
    assert pipe.findings('UNKNOWN_STATUS_CODE')[0]['severity'] == 'warning'


def test_severity_override_to_info_accepts(pipe):
    pipe.rule('ORPHAN_EVENT', severity='info')
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert verdicts(pipe) == ['accepted']


def test_modality_specific_rule_row_wins(pipe):
    pipe.exec("INSERT INTO ray7_rules (rule_code, modality, category, title, default_severity, "
              "enabled) VALUES ('ORPHAN_EVENT', 'MR', 'sequence', 'x', 'warning', FALSE)")
    pipe.send(k.status('AR', 'A-MR', k.ts(10), mod='MR'))
    pipe.send(k.status('AR', 'A-CT', k.ts(10), mod='CT'))
    assert [f['accession_number'] for f in pipe.findings('ORPHAN_EVENT')] == ['A-CT']


def test_verdict_and_worst_severity_are_stamped_on_the_archive(pipe):
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    a = pipe.archive()[0]
    assert (a['ray7_status'], a['ray7_severity']) == ('flagged', 'warning')
    assert a['ray7_screened_at'] is not None


# ── fail-open: RAY7 must never cost a message ─────────────────────────────────

def test_a_rule_that_raises_is_ignored(pipe, monkeypatch):
    from utils import ray7
    rule = next(r for r in ray7._RULES if r['code'] == 'ORPHAN_EVENT')

    def boom(msg, ctx):
        raise RuntimeError('rule bug')
    monkeypatch.setitem(rule, 'fn', boom)
    assert pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.state('A1') is not None


def test_context_failure_degrades_but_accepts(pipe, monkeypatch):
    from utils import ray7

    def boom(*a, **kw):
        raise RuntimeError('context bug')
    monkeypatch.setattr(ray7, '_Context', boom)
    assert pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.codes() == ['RAY7_DEGRADED']
    assert pipe.state('A1') is not None


def test_a_locked_state_row_degrades_screening_not_the_feed(pipe, app):
    """Another session holds the study's row. RAY7's 150 ms read ceiling must turn
    that into RAY7_DEGRADED instead of a stalled socket, and the message must
    still be archived."""
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    import psycopg2
    dsn = app.config['SQLALCHEMY_DATABASE_URI'].replace('+psycopg2', '')
    blocker = psycopg2.connect(dsn)
    cur = blocker.cursor()
    cur.execute("LOCK TABLE ray7_study_state IN ACCESS EXCLUSIVE MODE")
    release = threading.Timer(1.5, blocker.rollback)
    release.start()
    t0 = time.time()
    try:
        assert pipe.send(k.status('AR', 'A1', k.ts(10)))
    finally:
        release.cancel()
        blocker.rollback()
        blocker.close()
    assert 'RAY7_DEGRADED' in pipe.codes()
    assert pipe.count('hl7_message_archive') == 2
    assert time.time() - t0 < 10
