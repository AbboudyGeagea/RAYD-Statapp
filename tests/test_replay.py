"""
Replay: rebuild everything downstream of the archive, from the archive.

THE PROPERTY: live ingestion and a full replay of the same archive produce the same
derived data. Replay is how a parser fix becomes retroactive and how a quarantined
study is released, so a replay that drifts from the live result silently rewrites
history — for every study, every time someone presses "acknowledge".
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


def lifecycle(pipe, acc, start=300, pat='P0001', **kw):
    for i, code in enumerate(('SC', 'AR', 'IP', 'CM')):
        pipe.send(k.status(code, acc, k.hl7(at(start - 20 * i)), pat=pat, **kw))


# ── corpora: each one a realistic slice of traffic ────────────────────────────

def c_happy(pipe):
    pipe.send(k.adt('A08', when=k.hl7(at(340))))
    pipe.send(k.order('PL1', when=k.hl7(at(320))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(300)), placer='PL1'))
    for i, code in enumerate(('AR', 'IP', 'CM')):
        pipe.send(k.status(code, 'A1', k.hl7(at(280 - 20 * i))))
    pipe.send(k.report('A1', k.hl7(at(200))))


def c_unreported(pipe):
    lifecycle(pipe, 'A1')


def c_signatures(pipe):
    lifecycle(pipe, 'A1')
    pipe.send(k.report('A1', k.hl7(at(200)), result_status='W', text='prelim'))
    pipe.send(k.report('A1', k.hl7(at(150)), result_status='F', text='final'))


def c_signatures_reversed(pipe):
    lifecycle(pipe, 'A1')
    pipe.send(k.report('A1', k.hl7(at(150)), result_status='F', text='final'))
    pipe.send(k.report('A1', k.hl7(at(200)), result_status='W', text='prelim'))


def c_prelim_only(pipe):
    lifecycle(pipe, 'A1')
    pipe.send(k.report('A1', k.hl7(at(200)), result_status='W'))


def c_provisional(pipe):
    pipe.send(k.order('PL1', when=k.hl7(at(320))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(300)), placer='PL1'))
    pipe.send(k.status('CM', 'A1', k.hl7(at(240))))


def c_unscheduled_order(pipe):
    pipe.send(k.order('PL1', when=k.hl7(at(320))))


def c_cancelled(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('CA', 'A1', k.hl7(at(290))))


def c_out_of_order(pipe):
    for code, m in (('CM', 240), ('IP', 260), ('AR', 280), ('SC', 300)):
        pipe.send(k.status(code, 'A1', k.hl7(at(m))))


def c_repeated_arrival(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(200))))
    pipe.send(k.status('AR', 'A1', k.hl7(at(280))))       # earlier, delivered later
    pipe.send(k.status('CM', 'A1', k.hl7(at(100))))


def c_interleaved(pipe):
    for i, code in enumerate(('SC', 'AR', 'IP', 'CM')):
        pipe.send(k.status(code, 'A1', k.hl7(at(300 - 20 * i)), pat='PA'))
        pipe.send(k.status(code, 'A2', k.hl7(at(290 - 20 * i)), pat='PB', ae='MR1', mod='MR'))
    pipe.send(k.report('A2', k.hl7(at(100)), pat='PB'))


def c_device_changes(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(300)), ae='CT64_RH'))
    pipe.send(k.status('IP', 'A1', k.hl7(at(260)), ae='MR1'))
    pipe.send(k.status('CM', 'A1', k.hl7(at(240)), ae='CT64_RH'))


def c_pacs(pipe):
    pipe.setting('hl7_pacs_sending_app', 'PACS')
    pipe.send(k.status('CM', 'A1', k.hl7(at(240)), app='RIS'))
    pipe.send(k.status('CM', 'A1', k.hl7(at(230)), app='PACS'))


def c_report_only(pipe):
    pipe.send(k.report('A1', k.hl7(at(200))))


def c_adt_corrections(pipe):
    lifecycle(pipe, 'A1')
    pipe.send(k.adt('A01', dob='19700101', when=k.hl7(at(400))))
    pipe.send(k.adt('A08', dob='19800101', when=k.hl7(at(100))))


def c_unknown_code(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('ZZ', 'A1', k.hl7(at(280))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(240))))


CORPORA = [c_happy, c_unreported, c_signatures, c_signatures_reversed, c_prelim_only,
           c_provisional, c_unscheduled_order, c_cancelled, c_out_of_order,
           c_repeated_arrival, c_interleaved, c_device_changes, c_pacs, c_report_only,
           c_adt_corrections, c_unknown_code]


def message_findings(pipe):
    return sorted((f['accession_number'] or '', f['rule_code']) for f in pipe.findings()
                  if f['message_archive_id'] is not None
                  and f['rule_code'] not in ('CONTROL_ID_REUSE', 'EXACT_REDELIVERY'))


@pytest.mark.parametrize('corpus', CORPORA, ids=lambda f: f.__name__[2:])
def test_full_replay_reproduces_the_live_data(pipe, corpus):
    corpus(pipe)
    live = pipe.snapshot()
    pipe.replay()
    assert pipe.snapshot() == live


@pytest.mark.parametrize('corpus', CORPORA, ids=lambda f: f.__name__[2:])
def test_full_replay_reproduces_the_live_findings(pipe, corpus):
    corpus(pipe)
    live = message_findings(pipe)
    pipe.replay()
    assert message_findings(pipe) == live


@pytest.mark.parametrize('corpus', [c_happy, c_signatures, c_interleaved, c_unknown_code],
                         ids=lambda f: f.__name__[2:])
def test_replay_is_idempotent(pipe, corpus):
    corpus(pipe)
    pipe.replay()
    once = pipe.snapshot()
    pipe.replay()
    assert pipe.snapshot() == once


def test_replay_never_touches_the_archive(pipe):
    c_happy(pipe)
    before = pipe.q("SELECT id, message_control_id, sending_app, raw_message, content_hash, "
                    "received_at FROM hl7_message_archive ORDER BY id")
    pipe.replay()
    after = pipe.q("SELECT id, message_control_id, sending_app, raw_message, content_hash, "
                   "received_at FROM hl7_message_archive ORDER BY id")
    assert after == before


def test_replay_leaves_the_interface_counters_alone(pipe):
    """The counters describe the live wire; a replay is not wire traffic."""
    raw = k.status('SC', 'A1', k.ts(10))
    for _ in range(3):
        pipe.send(raw)
    pipe.replay()
    assert (pipe.stat('EXACT_REDELIVERY'), pipe.stat('MESSAGES')) == (2, 3)


def test_dry_run_changes_nothing(pipe):
    c_happy(pipe)
    before = pipe.snapshot()
    stats = pipe.replay(dry_run=True)
    assert stats['read'] == 7
    assert pipe.snapshot() == before


def test_partial_replay_leaves_other_studies_alone(pipe):
    lifecycle(pipe, 'A1')
    pipe.send(k.report('A1', k.hl7(at(200))))
    last = pipe.scalar("SELECT MAX(id) FROM hl7_message_archive")
    lifecycle(pipe, 'A2', start=150, pat='P2')
    before = pipe.snapshot()
    pipe.replay(since_id=last, rescreen=False)
    assert pipe.snapshot() == before


def test_releasing_one_study_does_not_rewrite_the_others(pipe):
    """The console's acknowledge replays one study's messages. It must not reach
    into every other study's state — closing studies that are still unreported."""
    lifecycle(pipe, 'OTHER', pat='P2')                       # completed, unreported
    pipe.profile(expected=['scheduled', 'arrived', 'started', 'completed'],
                 enforced=['scheduled', 'arrived', 'started', 'completed'])
    pipe.send(k.status('CM', 'HELD', k.hl7(at(240))))
    pipe.send(k.status('AR', 'HELD', k.hl7(at(280))))       # quarantined
    other_before = pipe.state('OTHER')
    ids = [r['id'] for r in pipe.q("SELECT id FROM hl7_message_archive "
                                    "WHERE ray7_status = 'quarantined'")]
    pipe.replay(archive_ids=ids, rescreen=False)
    other_after = pipe.state('OTHER')
    for col in ('is_closed', 'completed_at', 'reported_at', 'current_rank', 'ordered_at',
                'signed_final_at', 'is_provisional'):
        assert other_after[col] == other_before[col], col
    assert pipe.state('HELD')['arrived_at'] == at(280)


def test_parser_fix_is_retroactive(pipe):
    """The reason the archive exists: map a code after the fact, replay, and the
    study is whole — no resend from the hospital."""
    pipe.send(k.status('SC', 'A1', k.hl7(at(300))))
    pipe.send(k.status('XQ', 'A1', k.hl7(at(280))))
    assert pipe.state('A1')['arrived_at'] is None
    pipe.exec("INSERT INTO hl7_status_map (order_status, canonical_state, ladder_rank) "
              "VALUES ('XQ', 'arrived', 60)")
    from conftest import _reset_caches
    _reset_caches()
    pipe.replay()
    assert pipe.state('A1')['arrived_at'] == at(280)
    assert pipe.study('A1')['order_status'] == 'AR'
