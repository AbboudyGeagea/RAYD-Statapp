"""
BRAYN, the four layers (migration 0135), driven through the real ingest path.

    ① intake     only an unidentifiable or unclassifiable message is HELD
    ② quality    notes on the study, judged on its whole lifecycle, self-closing
    ③ interface  per-sender counters + the unknown-values registry; never rows
    ④ workflow   the sweep — see test_sweep.py

What matters is not only that a rule fires but what its verdict does to the data:
nothing but intake may keep data out of the reports, and nothing is ever lost.
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
    return [r['brayn_status'] for r in pipe.archive()]


def full_exam(pipe, acc='A1', start=300):
    for i, code in enumerate(('SC', 'AR', 'IP', 'CM')):
        pipe.send(k.status(code, acc, k.hl7(at(start - 20 * i))))


# ── ① intake: the only reasons a message is held ─────────────────────────────

def test_missing_identifier_is_held_and_kept(pipe):
    raw = k.message(k.msh('RIS'), k.pid(), k.orc('SC', status='AR', when=k.ts(5)),
                    k.obr(code='X'))
    assert pipe.send(raw) is True
    assert verdicts(pipe) == ['quarantined']
    assert pipe.codes() == ['MISSING_IDENTIFIER']
    assert pipe.count('hl7_study_events') == 0
    assert pipe.archive()[0]['raw_message'] == raw
    assert pipe.archive()[0]['projected_at'] is None


def test_order_with_no_identifier_is_held(pipe):
    pipe.send(k.message(k.msh('SAP_HIS'), k.pid(), k.orc('NW', when=k.ts(5)), k.obr()))
    assert pipe.codes() == ['MISSING_IDENTIFIER'] and pipe.held() == 1


def test_report_with_no_identifier_is_held(pipe):
    pipe.send(k.message(k.msh('PACS', None, 'ORU^R01'), k.pid(), k.obr(result_dt=k.ts(5)),
                        k.obx('text')))
    assert pipe.codes() == ['MISSING_IDENTIFIER'] and pipe.held() == 1


def test_adt_needs_no_study_identifier(pipe):
    pipe.send(k.adt('A08'))
    assert pipe.held() == 0


def test_unknown_status_code_is_held(pipe):
    pipe.send(k.status('ZZ', 'A1', k.ts(5)))
    f = pipe.findings('UNKNOWN_STATUS_CODE')
    assert len(f) == 1 and f[0]['severity'] == 'critical'
    assert f[0]['detail']['order_status'] == 'ZZ'
    assert pipe.held() == 1
    assert pipe.state('A1') is None and pipe.study('A1') is None


def test_unknown_status_code_registers_the_code_to_map(pipe):
    pipe.send(k.status('ZZ', 'A1', k.ts(5)))
    pipe.send(k.status('ZZ', 'A2', k.ts(4)))
    assert pipe.unknown('status_code') == [
        {'kind': 'status_code', 'sending_app': 'RIS', 'value': 'SC/ZZ', 'n': 2}]


def test_unknown_status_code_recovers_by_mapping_and_replay(pipe):
    pipe.send(k.status('ZZ', 'A1', k.hl7(at(5))))
    pipe.exec("INSERT INTO hl7_status_map (order_status, canonical_state, ladder_rank) "
              "VALUES ('ZZ', 'arrived', 60)")
    from conftest import _reset_caches
    _reset_caches()
    pipe.replay()
    assert pipe.state('A1')['arrived_at'] == at(5)
    assert pipe.findings('UNKNOWN_STATUS_CODE') == []
    assert verdicts(pipe) == ['accepted']


def test_status_message_nothing_can_classify_is_held(pipe):
    """ORC-1 XO (order changed) with ORC-5 empty: no rung. It used to be stamped
    accepted and vanish."""
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    pipe.send(k.message(k.msh('RIS'), k.pid(), k.orc('XO', acc='A1', when=k.ts(5)),
                        k.obr(acc='A1')))
    assert 'UNKNOWN_STATUS_CODE' in pipe.codes() and pipe.held() == 1


def test_sender_specific_mapping_beats_the_global_one(pipe):
    pipe.exec("INSERT INTO hl7_status_map (sending_app, order_status, canonical_state, ladder_rank) "
              "VALUES ('RIS2', 'AR', 'started', 70)")
    pipe.send(k.status('AR', 'A1', k.hl7(at(10)), app='RIS2'))
    pipe.send(k.status('AR', 'A2', k.hl7(at(10)), app='RIS'))
    assert pipe.state('A1')['started_at'] == at(10)
    assert pipe.state('A2')['arrived_at'] == at(10)


def test_inactive_mapping_is_unknown(pipe):
    pipe.exec("UPDATE hl7_status_map SET active = FALSE WHERE order_status = 'AR'")
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.codes() == ['UNKNOWN_STATUS_CODE']


def test_intake_downgraded_by_the_operator_does_not_hold(pipe):
    pipe.rule('UNKNOWN_STATUS_CODE', severity='warning')
    pipe.send(k.status('ZZ', 'A1', k.ts(10)))
    assert verdicts(pipe) == ['accepted'] and pipe.held() == 0
    assert pipe.findings('UNKNOWN_STATUS_CODE')[0]['severity'] == 'warning'


def test_intake_rule_switched_off_does_not_hold(pipe):
    pipe.rule('MISSING_IDENTIFIER', enabled=False)
    pipe.send(k.message(k.msh('RIS'), k.pid(), k.orc('SC', status='AR', when=k.ts(5)), k.obr()))
    assert pipe.held() == 0 and pipe.codes() == []


def test_verdict_is_stamped_on_the_archive(pipe):
    pipe.send(k.status('ZZ', 'A1', k.ts(10)))
    a = pipe.archive()[0]
    assert (a['brayn_status'], a['brayn_severity']) == ('quarantined', 'critical')
    assert a['brayn_screened_at'] is not None


def test_a_clean_message_is_accepted_with_no_severity(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(10)))
    a = pipe.archive()[0]
    assert (a['brayn_status'], a['brayn_severity']) == ('accepted', None)


@pytest.mark.parametrize('sender_noise', [
    'redelivery', 'reuse', 'out_of_order', 'duplicate', 'unknown_ae', 'future', 'placeholder',
    'contradiction',
])
def test_nothing_but_intake_ever_holds(pipe, sender_noise):
    """Every non-intake rule, provoked: each message is accepted and projects."""
    pipe.profile(expected=ALL_RUNGS, enforced=ALL_RUNGS)
    if sender_noise == 'redelivery':
        raw = k.status('SC', 'A1', k.ts(10))
        pipe.send(raw); pipe.send(raw)
    elif sender_noise == 'reuse':
        pipe.send(k.status('SC', 'A1', k.ts(10), ctrl='X'))
        pipe.send(k.status('SC', 'A2', k.ts(9), ctrl='X'))
    elif sender_noise == 'out_of_order':
        pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
        pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    elif sender_noise == 'duplicate':
        pipe.send(k.status('SC', 'A1', k.hl7(at(30))))
        pipe.send(k.status('SC', 'A1', k.hl7(at(28))))
    elif sender_noise == 'unknown_ae':
        pipe.send(k.status('IP', 'A1', k.ts(10), ae='NOT_A_DEVICE'))
    elif sender_noise == 'future':
        pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(days=2))))
    elif sender_noise == 'placeholder':
        pipe.send(k.adt('A01', dob='99991111', sex='U'))
        pipe.send(k.status('SC', 'A1', k.ts(10)))
    elif sender_noise == 'contradiction':
        pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
        pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    assert pipe.held() == 0
    assert set(verdicts(pipe)) == {'accepted'}
    assert pipe.state('A1') is not None


# ── facts about the message as received ──────────────────────────────────────

def test_placeholder_patient_is_an_info_finding(pipe):
    pipe.send(k.adt('A01', pat='QR001', dob='99991111', sex='U'))
    assert pipe.codes() == ['PLACEHOLDER_DATA']
    p = pipe.one("SELECT * FROM hl7_patients WHERE patient_id = 'QR001'")
    assert p['birth_date'] is None and p['sex'] == 'U'


def test_future_event_is_a_sticky_study_note(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(days=2))))
    notes = pipe.notes('FUTURE_EVENT')
    assert len(notes) == 1 and notes[0]['accession_number'] == 'A1'
    pipe.send(k.status('AR', 'A1', k.ts(1)))           # more traffic for the study
    assert len(pipe.notes('FUTURE_EVENT', open_only=True)) == 1


def test_a_minute_of_clock_skew_is_tolerated(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(minutes=1))))
    assert pipe.codes() == []


def test_future_tolerance_is_operator_config(pipe):
    pipe.rule('FUTURE_EVENT', threshold_minutes=60 * 24 * 3)
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(days=2))))
    assert pipe.codes() == []


def test_future_order_without_accession_is_a_message_finding(pipe):
    pipe.send(k.order('PL1', when=k.hl7(k.now() + timedelta(days=2))))
    f = pipe.findings('FUTURE_EVENT')
    assert len(f) == 1 and f[0]['message_archive_id'] is not None


# ── ③ interface: counted per sender, never rows ──────────────────────────────

def test_every_message_is_counted(pipe):
    full_exam(pipe)
    assert pipe.stat('MESSAGES', 'RIS') == 4


def test_exact_redelivery_is_a_rate(pipe):
    raw = k.status('SC', 'A1', k.ts(10))
    assert all(pipe.send(raw) for _ in range(4))
    assert pipe.count('hl7_message_archive') == 1
    assert (pipe.stat('EXACT_REDELIVERY'), pipe.stat('MESSAGES')) == (3, 4)
    assert pipe.findings() == []
    assert pipe.count('hl7_study_events') == 1


def test_redelivery_reframed_with_crlf_is_still_a_redelivery(pipe):
    raw = k.status('SC', 'A1', k.ts(10))
    pipe.send(raw)
    pipe.send(raw.replace('\r', '\r\n'))
    assert pipe.count('hl7_message_archive') == 1 and pipe.stat('EXACT_REDELIVERY') == 1


def test_control_id_reuse_ingests_both_and_counts_it(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(10), ctrl='SAME-1'))
    pipe.send(k.status('SC', 'A2', k.ts(9), ctrl='SAME-1', pat='P2'))
    assert pipe.count('hl7_message_archive') == 2
    assert pipe.state('A1') and pipe.state('A2')
    assert pipe.stat('CONTROL_ID_REUSE') == 1 and pipe.findings() == []


def test_control_id_reuse_across_senders_is_not_reuse(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(10), ctrl='SAME-1', app='RIS'))
    pipe.send(k.status('SC', 'A2', k.ts(9), ctrl='SAME-1', app='OTHER_RIS'))
    assert pipe.stat('CONTROL_ID_REUSE') == 0


def test_two_senders_sharing_a_control_id_keep_both_orders(pipe):
    pipe.send(k.order('PL-A', when=k.ts(10), ctrl='1001', app='HIS_A'))
    pipe.send(k.order('PL-B', when=k.ts(9), ctrl='1001', app='HIS_B', pat='P2'))
    assert {r['placer_order_number'] for r in pipe.q("SELECT placer_order_number FROM hl7_orders")} \
        == {'PL-A', 'PL-B'}


def test_one_sender_reusing_a_control_id_keeps_both_orders(pipe):
    pipe.send(k.order('PL-A', when=k.ts(10), ctrl='1001'))
    pipe.send(k.order('PL-B', when=k.ts(9), ctrl='1001', pat='P2'))
    assert pipe.count('hl7_orders') == 2 and pipe.count('etl_orders') == 2


def test_logical_duplicate_is_a_rate(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(30))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(27))))
    assert pipe.stat('LOGICAL_DUPLICATE') == 1 and pipe.stat('REPEATED_TRANSITION') == 0
    assert pipe.state('A1')['scheduled_at'] == at(30)


def test_repeat_outside_the_window_is_a_repeated_transition(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(100))))
    assert pipe.stat('REPEATED_TRANSITION') == 1 and pipe.stat('LOGICAL_DUPLICATE') == 0


def test_duplicate_window_is_operator_config(pipe):
    pipe.rule('LOGICAL_DUPLICATE', threshold_minutes=300)
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(100))))
    assert pipe.stat('LOGICAL_DUPLICATE') == 1


def test_out_of_order_delivery_is_measured_on_an_unconfigured_ladder(pipe):
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(140))))
    assert pipe.stat('OUT_OF_SEQUENCE_DELIVERY') == 1
    assert pipe.state('A1')['arrived_at'] == at(140) and pipe.held() == 0


def test_in_order_delivery_is_not_out_of_order(pipe):
    full_exam(pipe)
    assert pipe.stat('OUT_OF_SEQUENCE_DELIVERY') == 0


@pytest.mark.parametrize('late,measured,counted', [
    ('AR', ['arrived', 'completed'], 1),      # both rungs measured
    ('IP', ['arrived', 'completed'], 0),      # the late rung is not measured
    ('AR', ['arrived'], 0),                   # the rung it is late to is not measured
])
def test_measure_order_selects_the_rungs(pipe, late, measured, counted):
    pipe.profile(expected=ALL_RUNGS, enforced=measured)
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status(late, 'A1', k.hl7(at(140))))
    assert pipe.stat('OUT_OF_SEQUENCE_DELIVERY') == counted


def test_cancellation_after_progress_is_not_out_of_order(pipe):
    full_exam(pipe)
    pipe.send(k.status('CA', 'A1', k.ts(10)))
    assert pipe.stat('OUT_OF_SEQUENCE_DELIVERY') == 0


def test_unexpected_rung(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    pipe.send(k.status('IP', 'A1', k.ts(10)))
    assert pipe.stat('UNEXPECTED_RUNG') == 1
    assert [u['value'] for u in pipe.unknown('rung')] == ['started']


def test_unconfigured_profile_never_counts_unexpected_rungs(pipe):
    full_exam(pipe)
    assert pipe.stat('UNEXPECTED_RUNG') == 0


def test_unknown_aetitle(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(20)))
    pipe.send(k.status('IP', 'A1', k.ts(10), ae='NOT_A_DEVICE'))
    pipe.send(k.status('CM', 'A1', k.ts(5), ae='NOT_A_DEVICE'))
    assert pipe.stat('UNKNOWN_AETITLE') == 2
    assert pipe.unknown('aetitle') == [
        {'kind': 'aetitle', 'sending_app': 'RIS', 'value': 'NOT_A_DEVICE', 'n': 2}]
    assert pipe.findings('UNKNOWN_AETITLE') == []         # a count, never a row


@pytest.mark.parametrize('ae', ['EBW', 'ebw', 'Ebw'])
def test_known_aetitle_any_case(pipe, ae):
    pipe.send(k.status('SC', 'A1', k.ts(10), ae=ae))
    assert pipe.stat('UNKNOWN_AETITLE') == 0


def test_unknown_result_status(pipe):
    full_exam(pipe)
    pipe.send(k.report('A1', k.ts(5), obx_statuses=['X']))
    assert pipe.stat('UNKNOWN_RESULT_STATUS', 'PACS') == 1
    assert [u['value'] for u in pipe.unknown('result_status')] == ['X']
    assert pipe.one("SELECT * FROM hl7_oru_reports WHERE accession_number='A1'") is not None


def test_counters_are_per_sender(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(10), app='RIS_A'))
    pipe.send(k.status('SC', 'A2', k.ts(10), app='RIS_B'))
    pipe.send(k.status('SC', 'A3', k.ts(10), app='RIS_B'))
    assert (pipe.stat('MESSAGES', 'RIS_A'), pipe.stat('MESSAGES', 'RIS_B')) == (1, 2)


def test_held_messages_are_counted(pipe):
    pipe.send(k.status('ZZ', 'A1', k.ts(10)))
    assert pipe.stat('HELD', 'RIS') == 1


def test_disabled_interface_rule_is_not_counted(pipe):
    pipe.rule('UNKNOWN_AETITLE', enabled=False)
    pipe.send(k.status('IP', 'A1', k.ts(10), ae='NOT_A_DEVICE'))
    assert pipe.stat('UNKNOWN_AETITLE') == 0 and pipe.unknown('aetitle') == []


def test_replay_never_counts_the_wire_twice(pipe):
    full_exam(pipe)
    raw = k.status('SC', 'A9', k.ts(10))
    pipe.send(raw); pipe.send(raw)
    before = {m: pipe.stat(m) for m in ('MESSAGES', 'EXACT_REDELIVERY')}
    pipe.replay()
    assert {m: pipe.stat(m) for m in before} == before


# ── ② quality: notes on the study, judged on its whole lifecycle ─────────────

def test_time_contradiction_completed_before_arrived(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    notes = pipe.notes('TIME_CONTRADICTION', open_only=True)
    assert len(notes) == 1
    pair = notes[0]['detail']['pairs'][0]
    assert (pair['earlier_rung'], pair['later_rung']) == ('arrived', 'completed')


def test_arrival_stamped_after_completion_is_a_contradiction(pipe):
    """What LADDER_REGRESSION used to catch per message is one case of the state rule."""
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(50))))
    assert len(pipe.notes('TIME_CONTRADICTION', open_only=True)) == 1
    assert pipe.findings('LADDER_REGRESSION') == []


def test_report_signed_before_the_exam_finished(pipe):
    full_exam(pipe, start=300)                      # CM at -240
    pipe.send(k.report('A1', k.hl7(at(260))))      # signed at -260, before completion
    assert len(pipe.notes('TIME_CONTRADICTION', open_only=True)) == 1


def test_preliminary_after_final_is_not_a_contradiction(pipe):
    full_exam(pipe)
    pipe.send(k.report('A1', k.hl7(at(150)), result_status='F'))
    pipe.send(k.report('A1', k.hl7(at(100)), result_status='W'))   # an addendum
    assert pipe.notes('TIME_CONTRADICTION') == []


def test_consistent_study_has_no_contradiction(pipe):
    full_exam(pipe)
    pipe.send(k.report('A1', k.hl7(at(200))))
    assert pipe.open_notes('A1') == []


def test_skipped_rung_opens_and_closes_itself(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(200))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    notes = pipe.notes('SKIPPED_RUNG', open_only=True)
    assert len(notes) == 1 and notes[0]['detail']['missing_rungs'] == ['arrived', 'started']
    assert pipe.study('A1')['order_status'] == 'CM'            # a note never withholds
    pipe.send(k.status('AR', 'A1', k.hl7(at(180))))
    assert pipe.notes('SKIPPED_RUNG', open_only=True)[0]['detail']['missing_rungs'] == ['started']
    pipe.send(k.status('IP', 'A1', k.hl7(at(150))))
    assert pipe.notes('SKIPPED_RUNG', open_only=True) == []
    closed = pipe.notes('SKIPPED_RUNG')[0]
    assert closed['resolution'] == 'cleared' and 'auto' in closed['resolution_note']


def test_skipped_rung_fires_on_an_unconfigured_ladder(pipe):
    pipe.send(k.status('CM', 'A1', k.ts(10)))
    assert pipe.open_notes('A1') == ['ORPHAN_EVENT', 'SKIPPED_RUNG']


def test_skipped_rung_respects_a_disabled_rung(pipe):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    pipe.send(k.status('SC', 'A1', k.hl7(at(200))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(180))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))
    assert pipe.notes('SKIPPED_RUNG') == []


def test_walk_in_skips_scheduling_without_a_note(pipe):
    pipe.send(k.order('PL1', acc='A1', when=k.hl7(at(150))))
    for code, m in (('AR', 140), ('IP', 120), ('CM', 100)):
        pipe.send(k.status(code, 'A1', k.hl7(at(m))))
    assert pipe.open_notes('A1') == []


def test_orphan_event_opens_and_closes_itself(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    assert pipe.open_notes('A1') == ['ORPHAN_EVENT']
    pipe.send(k.status('SC', 'A1', k.hl7(at(140))))
    assert pipe.open_notes('A1') == []


def test_orphan_event_off_when_the_site_sends_neither_order_nor_schedule(pipe):
    pipe.profile(expected=['arrived', 'started', 'completed'])
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.notes('ORPHAN_EVENT') == []


def test_out_of_order_delivery_leaves_no_open_note(pipe):
    for code, m in (('CM', 240), ('IP', 260), ('AR', 280), ('SC', 300)):
        pipe.send(k.status(code, 'A1', k.hl7(at(m))))
    assert pipe.open_notes('A1') == []


def test_cancellation_closes_the_notes(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    assert pipe.open_notes('A1')
    pipe.send(k.status('CA', 'A1', k.ts(1)))
    assert pipe.open_notes('A1') == []


def test_a_note_ignored_by_a_person_does_not_reopen(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    pipe.exec("UPDATE brayn_findings SET resolved_at = NOW(), resolution = 'ignored' "
              "WHERE rule_code = 'TIME_CONTRADICTION'")
    pipe.send(k.status('IP', 'A1', k.hl7(at(120))))
    assert pipe.notes('TIME_CONTRADICTION', open_only=True) == []


def test_a_note_cleared_by_a_person_reopens_while_still_true(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    pipe.exec("UPDATE brayn_findings SET resolved_at = NOW(), resolution = 'cleared' "
              "WHERE rule_code = 'TIME_CONTRADICTION'")
    pipe.send(k.status('IP', 'A1', k.hl7(at(120))))
    assert len(pipe.notes('TIME_CONTRADICTION', open_only=True)) == 1


def test_one_open_note_per_study_per_rule(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    for m in (150, 149, 148):
        pipe.send(k.status('CM', 'A1', k.hl7(at(m))))
    assert len(pipe.notes('TIME_CONTRADICTION')) == 1


def test_disabled_quality_rule_stays_silent(pipe):
    pipe.rule('ORPHAN_EVENT', enabled=False)
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.notes('ORPHAN_EVENT') == []


def test_modality_specific_quality_rule_row_wins(pipe):
    pipe.exec("INSERT INTO brayn_rules (rule_code, modality, category, title, default_severity, "
              "enabled) VALUES ('ORPHAN_EVENT', 'MR', 'quality', 'x', 'warning', FALSE)")
    pipe.send(k.status('AR', 'A-MR', k.ts(10), mod='MR', ae='MR1'))
    pipe.send(k.status('AR', 'A-CT', k.ts(10), mod='CT'))
    assert [f['accession_number'] for f in pipe.notes('ORPHAN_EVENT')] == ['A-CT']


def test_quality_severity_is_operator_config(pipe):
    pipe.rule('ORPHAN_EVENT', severity='info')
    pipe.send(k.status('AR', 'A1', k.ts(10)))
    assert pipe.notes('ORPHAN_EVENT')[0]['severity'] == 'info'


def test_provisional_orders_get_no_notes(pipe):
    pipe.send(k.order('PL1', when=k.ts(10)))
    assert pipe.notes() == []


# ── fail-open: BRAYN must never cost a message ─────────────────────────────────

def test_an_intake_rule_that_raises_is_ignored(pipe, monkeypatch):
    from utils import brayn

    def boom(msg):
        raise RuntimeError('rule bug')
    monkeypatch.setattr(brayn, '_INTAKE', (('MISSING_IDENTIFIER', boom),))
    assert pipe.send(k.message(k.msh('RIS'), k.pid(), k.orc('SC', status='AR'), k.obr()))
    assert pipe.held() == 0


def test_an_observer_that_raises_is_ignored(pipe, monkeypatch):
    from utils import brayn

    def boom(msg, ctx):
        raise RuntimeError('observer bug')
    monkeypatch.setattr(brayn, '_OBSERVERS', (('UNKNOWN_AETITLE', boom),))
    assert pipe.send(k.status('SC', 'A1', k.ts(10)))
    assert pipe.state('A1') is not None


def test_context_failure_is_counted_and_accepted(pipe, monkeypatch):
    from utils import brayn

    def boom(*a, **kw):
        raise RuntimeError('context bug')
    monkeypatch.setattr(brayn, '_Context', boom)
    assert pipe.send(k.status('SC', 'A1', k.ts(10)))
    assert pipe.stat('BRAYN_DEGRADED') == 1
    assert pipe.state('A1') is not None and pipe.held() == 0


def test_quality_failure_costs_only_the_note(pipe, monkeypatch):
    from utils import brayn_quality

    def boom(state, expected):
        raise RuntimeError('quality bug')
    monkeypatch.setattr(brayn_quality, 'conditions', boom)
    assert pipe.send(k.status('CM', 'A1', k.ts(10)))
    assert pipe.study('A1') is not None


def test_counter_failure_costs_only_the_count(pipe, monkeypatch):
    from utils import brayn_health
    monkeypatch.setattr(brayn_health, '_STATS_SQL', 'SELECT no_such_function()')
    assert pipe.send(k.status('SC', 'A1', k.ts(10)))
    assert pipe.state('A1') is not None


def test_a_locked_state_row_degrades_measurement_not_the_feed(pipe, app):
    """Another session holds the table. BRAYN's 150 ms read ceiling must turn that
    into a BRAYN_DEGRADED count instead of a stalled socket, and the message must
    still be archived."""
    pipe.send(k.status('SC', 'A1', k.ts(30)))
    import psycopg2
    dsn = app.config['SQLALCHEMY_DATABASE_URI'].replace('+psycopg2', '')
    blocker = psycopg2.connect(dsn)
    cur = blocker.cursor()
    cur.execute("LOCK TABLE brayn_study_state IN ACCESS EXCLUSIVE MODE")
    release = threading.Timer(1.5, blocker.rollback)
    release.start()
    t0 = time.time()
    try:
        assert pipe.send(k.status('AR', 'A1', k.ts(10)))
    finally:
        release.cancel()
        blocker.rollback()
        blocker.close()
    assert pipe.stat('BRAYN_DEGRADED') == 1
    assert pipe.count('hl7_message_archive') == 2
    assert time.time() - t0 < 10
