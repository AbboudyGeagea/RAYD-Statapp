"""
The RAY7 console: who may open it, what it shows, and the acknowledge button that
releases a quarantined study into the reports.

A release that clears the findings but leaves the data out is worse than no button:
the operator believes the study is back.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()
RIS_RUNGS = ['scheduled', 'arrived', 'started', 'completed']
ALL_RUNGS = ['ordered'] + RIS_RUNGS + ['signed_prelim', 'signed_final']


def at(m):
    return BASE - timedelta(minutes=m)


def su(client, method, url, **kw):
    return getattr(client, method)(url, headers={'X-Test-Role': 'su'}, **kw)


def hold_one(pipe, acc='HELD'):
    pipe.profile(expected=RIS_RUNGS, enforced=RIS_RUNGS)
    pipe.send(k.status('CM', acc, k.hl7(at(240))))
    pipe.send(k.status('AR', acc, k.hl7(at(280))))


# ── access ────────────────────────────────────────────────────────────────────

def test_anonymous_is_refused(pipe, client):
    assert client.get('/ray7').status_code == 401


@pytest.mark.parametrize('role,code', [('su', 200), ('administrator', 200),
                                       ('implementation', 403), ('user', 403)])
def test_role_access(pipe, client, role, code):
    assert client.get('/ray7', headers={'X-Test-Role': role}).status_code == code


@pytest.mark.parametrize('role', ['implementation', 'user'])
def test_acknowledge_is_refused_without_access(pipe, client, role):
    hold_one(pipe)
    r = client.post('/ray7/study/HELD/acknowledge', json={},
                    headers={'X-Test-Role': role})
    assert r.status_code == 403
    assert pipe.count('hl7_message_archive', "ray7_status = 'quarantined'") == 1


# ── the views render, with and without data ───────────────────────────────────

@pytest.mark.parametrize('view', ['summary', 'findings', 'held', 'messages'])
def test_views_render_empty(pipe, client, view):
    assert su(client, 'get', f'/ray7?view={view}').status_code == 200


@pytest.mark.parametrize('view', ['summary', 'findings', 'held', 'messages'])
def test_views_render_with_data(pipe, client, view):
    hold_one(pipe)
    pipe.send(k.status('ZZ', 'A2', k.ts(5)))
    r = su(client, 'get', f'/ray7?view={view}')
    assert r.status_code == 200
    if view == 'held':
        assert b'HELD' in r.data


@pytest.mark.parametrize('qs', ['view=findings&severity=critical', 'view=findings&rule=ZZ',
                                'view=findings&show=resolved', 'view=findings&q=HELD',
                                'view=messages&q=RIS', 'view=findings&page=abc',
                                'view=findings&page=999', "view=findings&q=%27%3B--"])
def test_filters_and_hostile_query_strings(pipe, client, qs):
    hold_one(pipe)
    assert su(client, 'get', f'/ray7?{qs}').status_code == 200


def test_message_detail(pipe, client):
    hold_one(pipe)
    aid = pipe.scalar("SELECT id FROM hl7_message_archive WHERE ray7_status = 'quarantined'")
    r = su(client, 'get', f'/ray7/message/{aid}')
    body = r.get_json()
    assert r.status_code == 200
    assert body['segments'][0].startswith('MSH|')
    assert [f['rule_code'] for f in body['findings']] == ['OUT_OF_SEQUENCE_DELIVERY']


def test_message_detail_not_found(pipe, client):
    assert su(client, 'get', '/ray7/message/999999').status_code == 404


# ── resolve: closes a flag, moves no data ─────────────────────────────────────

def test_resolve_a_finding(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.findings('ORPHAN_EVENT')[0]['id']
    r = su(client, 'post', f'/ray7/finding/{fid}/resolve', json={'resolution': 'ignored',
                                                                  'note': 'known'})
    assert r.status_code == 200
    f = pipe.findings('ORPHAN_EVENT')[0]
    assert (f['resolution'], f['resolution_note'], f['resolved_by']) == ('ignored', 'known', 4242)


def test_resolve_rejects_an_invented_resolution(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.findings('ORPHAN_EVENT')[0]['id']
    r = su(client, 'post', f'/ray7/finding/{fid}/resolve', json={'resolution': 'deleted'})
    assert r.status_code == 400


def test_resolving_does_not_release_held_data(pipe, client):
    hold_one(pipe)
    fid = pipe.findings('OUT_OF_SEQUENCE_DELIVERY')[0]['id']
    su(client, 'post', f'/ray7/finding/{fid}/resolve', json={'resolution': 'cleared'})
    assert pipe.state('HELD')['arrived_at'] is None


# ── acknowledge: the release ──────────────────────────────────────────────────

def test_acknowledge_releases_the_study(pipe, client):
    hold_one(pipe)
    r = su(client, 'post', '/ray7/study/HELD/acknowledge', json={'note': 'checked'})
    assert r.status_code == 200 and r.get_json()['messages_released'] == 1
    assert pipe.state('HELD')['arrived_at'] == at(280)
    assert pipe.count('hl7_message_archive', "ray7_status = 'quarantined'") == 0
    f = pipe.findings('OUT_OF_SEQUENCE_DELIVERY')[0]
    assert f['resolved_at'] and f['resolved_by'] == 4242 and f['resolution_note'] == 'checked'


def test_acknowledge_projects_the_released_rung(pipe, client):
    hold_one(pipe)
    su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    assert pipe.q("SELECT arrived_at FROM std_worklist_arrivals") == [{'arrived_at': at(280)}]


def test_released_report_reaches_the_report_table(pipe, client):
    """A report held back (here: its sender reused a control ID) never wrote its
    hl7_oru_reports row. Releasing it has to write that row, or the study is back
    in the reports with no report attached."""
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'HELD', k.hl7(at(m)), app='RAD', ctrl=f'C-{code}'))
    pipe.send(k.report('HELD', k.hl7(at(100)), app='RAD', ctrl='C-CM', text='Final read.'))
    assert pipe.findings('CONTROL_ID_REUSE')
    assert pipe.count('hl7_oru_reports') == 0
    su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    r = pipe.one("SELECT * FROM hl7_oru_reports WHERE accession_number = 'HELD'")
    assert r is not None and r['report_text'].startswith('Final read.')
    assert pipe.study('HELD')['study_has_report'] is True


def test_released_order_reaches_the_order_table(pipe, client):
    pipe.send(k.order('PL-1', when=k.ts(60), ctrl='SAME'))
    pipe.send(k.order('PL-2', acc='HELD', when=k.ts(50), ctrl='SAME', pat='P2'))
    assert pipe.findings('CONTROL_ID_REUSE')
    su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    assert pipe.count('hl7_orders', "placer_order_number = 'PL-2'") == 1


def test_acknowledge_unknown_study(pipe, client):
    r = su(client, 'post', '/ray7/study/NOPE/acknowledge', json={})
    assert r.status_code == 200 and r.get_json()['messages_released'] == 0


def test_acknowledge_is_idempotent(pipe, client):
    hold_one(pipe)
    su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    before = pipe.snapshot()
    r = su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    assert r.status_code == 200
    assert pipe.snapshot() == before


def test_acknowledge_leaves_other_studies_untouched(pipe, client):
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'OTHER', k.hl7(at(m)), pat='P2'))
    hold_one(pipe)
    before = pipe.state('OTHER')
    su(client, 'post', '/ray7/study/HELD/acknowledge', json={})
    after = pipe.state('OTHER')
    assert {c: after[c] for c in ('is_closed', 'current_rank', 'completed_at')} == \
           {c: before[c] for c in ('is_closed', 'current_rank', 'completed_at')}


def test_accession_with_a_slash_can_be_acknowledged(pipe, client):
    hold_one(pipe, acc='2026/00042')
    r = su(client, 'post', '/ray7/study/2026/00042/acknowledge', json={})
    assert r.status_code == 200 and r.get_json()['messages_released'] == 1
