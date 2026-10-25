"""
utils/brayn_explain.py: BRAYN in the operator's words.

Every sentence here is held to the detail shapes the engine actually stores (taken
from real findings on the local stack, 2026-10-25), so a change to a rule's detail
that would leave the console printing JSON or a blank fails here first.
"""
from datetime import datetime, timedelta

import pytest

from utils import brayn_explain as X

NOW = datetime(2026, 10, 25, 14, 12, 0)


# ── words for every rule ──────────────────────────────────────────────────────

def test_every_seeded_rule_has_plain_words(pipe):
    codes = {r['rule_code'] for r in pipe.q("SELECT DISTINCT rule_code FROM brayn_rules")}
    assert codes - set(X.RULES) == set()


def test_every_layer_has_a_title(pipe):
    cats = {r['category'] for r in pipe.q("SELECT DISTINCT category FROM brayn_rules")}
    assert cats <= {k for k, _, _ in X.GROUPS}


def test_an_unknown_rule_still_reads(pipe):
    assert X.rule_title('SOME_NEW_RULE') == 'Some new rule'


# ── time in words ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize('minutes,text', [(0, '0 min'), (45, '45 min'), (60, '1 h'), (260, '4 h 20 min'),
                                          (1500, '25 h'), (2880, '2 days'), (3000, '2 days 2 h')])
def test_duration(minutes, text):
    assert X.duration(minutes) == text


@pytest.mark.parametrize('delta,text', [(timedelta(seconds=20), 'just now'), (timedelta(minutes=5), '5 min ago'),
                                        (timedelta(hours=3), '3 h ago'), (timedelta(minutes=-90), 'in 1 h 30 min')])
def test_ago(delta, text):
    assert X.ago(NOW - delta, NOW) == text


def test_clock():
    assert X.clock(NOW - timedelta(hours=2), NOW) == '12:12'
    assert X.clock(NOW - timedelta(days=1), NOW) == 'yesterday 14:12'
    assert X.clock(datetime(2026, 10, 2, 9, 5), NOW) == '2 Oct 09:05'
    assert X.clock('2026-10-25T11:14:47', NOW) == '11:14'
    assert X.clock(None, NOW) == ''


# ── notes as sentences ────────────────────────────────────────────────────────

def test_stall_wait_is_computed_now_not_when_raised():
    """The stored stalled_minutes is how long the study had waited when the note was
    raised; reading it hours later must not show the stale number."""
    detail = {'since': (NOW - timedelta(hours=9, minutes=41)).isoformat(), 'stalled_minutes': 260,
              'threshold_minutes': 240, 'modality': 'CT', 'aetitle': 'CT64_RH'}
    assert X.note_sentence('STALLED_ARRIVED', detail, NOW) == 'Arrived 9 h 41 min ago, not started. Limit 4 h.'


@pytest.mark.parametrize('code,lead', [('STALLED_SCHEDULED', 'Scheduled'), ('STALLED_STARTED', 'Started'),
                                       ('UNREPORTED', 'Completed'), ('STALLED_UNSIGNED', 'Preliminary report')])
def test_every_stall_reads(code, lead):
    s = X.note_sentence(code, {'since': (NOW - timedelta(hours=30)).isoformat(), 'threshold_minutes': 1440}, NOW)
    assert s.startswith(f'{lead} 30 h ago,') and s.endswith('Limit 24 h.')


def test_orphan_order():
    s = X.note_sentence('ORPHAN_ORDER', {'ordered_at': (NOW - timedelta(days=2)).isoformat(),
                                         'placer_order_number': 'PL-1', 'threshold_minutes': 1440}, NOW)
    assert s == 'Order PL-1 placed 2 days ago, never scheduled. Limit 24 h.'


def test_time_contradiction():
    detail = {'pairs': [{'later_rung': 'completed', 'earlier_rung': 'arrived',
                         'later_rung_time': '2026-10-25T11:14:47', 'earlier_rung_time': '2026-10-25T12:14:47'}],
              'effect': 'negative turnaround times downstream'}
    s = X.note_sentence('TIME_CONTRADICTION', detail, NOW)
    assert s.startswith('Completed (11:14) is earlier than Arrived (12:14).')


def test_skipped_rung():
    s = X.note_sentence('SKIPPED_RUNG', {'completed_at': '2026-10-24T09:34:49',
                                         'missing_rungs': ['arrived', 'started']}, NOW)
    assert s.startswith('Completed came in with no Arrived and no Started.')


def test_future_event():
    s = X.note_sentence('FUTURE_EVENT', {'event_time': '2026-10-25T23:40:00', 'state': 'arrived',
                                         'local_now': '2026-10-25T14:09:12', 'tolerance_minutes': 5}, NOW)
    assert s == "Arrived is stamped 23:40, later than the message arrived (14:09). Check the sender's clock."


def test_placeholder():
    s = X.note_sentence('PLACEHOLDER_DATA', {'placeholders': ['birth_date=99991111 (quick-registration placeholder)']}, NOW)
    assert s == 'Placeholder data: birth_date=99991111 (quick-registration placeholder).'


def test_intake_reasons():
    assert X.note_sentence('UNKNOWN_STATUS_CODE', {'order_control': 'SC', 'order_status': 'ZZ'}, NOW) == \
        'ORC-5 = ZZ with ORC-1 = SC is not in the status map.'
    assert 'ORC-3 / OBR-3' in X.note_sentence('MISSING_IDENTIFIER', {}, NOW)
    assert X.note_sentence('UNKNOWN_STATUS_CODE', {'order_control': None, 'order_status': None}, NOW) \
        .startswith('No status code: ORC-5 and ORC-1 are both empty')
    assert X.note_sentence('UNKNOWN_STATUS_CODE', {'order_control': 'XO', 'order_status': None}, NOW) \
        .startswith('ORC-5 is empty and ORC-1 = XO is not a code BRAYN can classify')


def test_interface_rate():
    s = X.note_sentence('CONTROL_ID_REUSE', {'count': 1, 'messages': 43, 'rate_pct': 2.33,
                                             'threshold_pct': 1.0, 'window_minutes': 60}, NOW)
    assert s == '2.33% of messages in the last 1 h (1 of 43); alert above 1.0%.'


def test_a_note_never_reads_as_blank_or_json():
    for code in X.RULES:
        s = X.note_sentence(code, {}, NOW)
        assert s and '{' not in s, code


def test_a_broken_detail_falls_back_to_words():
    assert X.note_sentence('TIME_CONTRADICTION', {'pairs': 'not a list'}, NOW)


# ── a message, in a few words ─────────────────────────────────────────────────

class M:
    def __init__(self, **kw):
        self.__dict__.update({'kind': None, 'placer_order_number': None, 'ladder_rank': None,
                              'raw_order_status': None, 'raw_order_control': None,
                              'canonical_state': None, 'raw_result_status': None,
                              'message_type': None}, **kw)


@pytest.mark.parametrize('msg,text', [
    (M(kind='order', placer_order_number='PL-9'), 'New order PL-9'),
    (M(kind='status', ladder_rank=60, canonical_state='arrived'), 'Arrived'),
    (M(kind='status', raw_order_status='ZZ'), 'ORC-5 = ZZ (not mapped)'),
    (M(kind='result', canonical_state='signed_final'), 'Report, final'),
    (M(kind='result', canonical_state='signed_prelim'), 'Report, preliminary'),
    (M(kind='result', raw_result_status='X'), 'Report, status X'),
    (M(kind='adt', message_type='ADT^A08'), 'Patient update (A08)'),
    (M(kind='adt', message_type='ADT^A40'), 'Merge patient (A40)'),
    (M(kind='other', message_type='QRY^A19'), 'QRY^A19'),
])
def test_event_text(msg, text):
    assert X.event_text(msg) == text


def test_segments_number_msh_the_hl7_way():
    segs = X.segments_of('MSH|^~\\&|RIS|RH\rPID|1||P1^^^HIS')
    msh = dict(segs[0][1])
    assert msh[1] == '|' and msh[2] == '^~\\&' and msh[3] == 'RIS' and msh[4] == 'RH'
    assert dict(segs[1][1])[3] == 'P1^^^HIS'
