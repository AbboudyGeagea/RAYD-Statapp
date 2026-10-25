"""
The RAY7 console: who may open it, what it shows, and the release of held messages.

Since the four-layer rebuild a message is held only when it cannot be identified or
classified, so the release is: fix the configuration, then "Replay held messages",
which re-screens them. A release that cleared a flag but left the data out would be
worse than no button — the operator would believe the study is back.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


def su(client, method, url, **kw):
    return getattr(client, method)(url, headers={'X-Test-Role': 'su'}, **kw)


def hold_unknown_code(pipe, acc='HELD', code='ZZ', n=1):
    pipe.send(k.status('SC', acc, k.hl7(at(300))))
    for i in range(n):
        pipe.send(k.status(code, acc, k.hl7(at(280 - i)), ctrl=f'HOLD-{acc}-{i}'))


def map_code(pipe, code='ZZ', state='arrived', rank=60):
    pipe.exec("INSERT INTO hl7_status_map (order_status, canonical_state, ladder_rank) "
              "VALUES (:c, :s, :r)", c=code, s=state, r=rank)
    from conftest import _reset_caches
    _reset_caches()


# ── access ────────────────────────────────────────────────────────────────────

def test_anonymous_is_refused(pipe, client):
    assert client.get('/ray7').status_code == 401


@pytest.mark.parametrize('role,code', [('su', 200), ('administrator', 200),
                                       ('implementation', 200), ('user', 403)])
def test_role_access(pipe, client, role, code):
    assert client.get('/ray7', headers={'X-Test-Role': role}).status_code == code


@pytest.mark.parametrize('role', ['user'])
def test_replay_is_refused_without_access(pipe, client, role):
    """Implementation has the console since 2026-10-25 (it maps what RAY7 lists)."""
    hold_unknown_code(pipe)
    r = client.post('/ray7/held/replay', json={}, headers={'X-Test-Role': role})
    assert r.status_code == 403
    assert pipe.held() == 1


# ── the views render, with and without data ───────────────────────────────────

VIEWS = ['summary', 'held', 'findings', 'health', 'messages']


@pytest.mark.parametrize('view', VIEWS)
def test_views_render_empty(pipe, client, view):
    assert su(client, 'get', f'/ray7?view={view}').status_code == 200


@pytest.mark.parametrize('view', VIEWS)
def test_views_render_with_data(pipe, client, view):
    hold_unknown_code(pipe)
    pipe.send(k.status('AR', 'A2', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A2', k.hl7(at(150))))           # a quality note
    pipe.send(k.status('IP', 'A3', k.ts(5), ae='NOT_A_DEVICE'))  # an unknown value
    r = su(client, 'get', f'/ray7?view={view}')
    assert r.status_code == 200
    if view == 'held':
        assert b'UNKNOWN_STATUS_CODE' in r.data and b'SC / ZZ' in r.data
    if view == 'health':
        assert b'NOT_A_DEVICE' in r.data and b'RIS' in r.data
    if view == 'summary':
        assert b'quality' in r.data and b'intake' in r.data


@pytest.mark.parametrize('qs', ['view=findings&severity=critical', 'view=findings&rule=ZZ',
                                'view=findings&show=resolved', 'view=findings&q=HELD',
                                'view=findings&layer=quality', 'view=findings&layer=nonsense',
                                'view=messages&q=RIS', 'view=messages&severity=quarantined',
                                'view=findings&page=abc', 'view=findings&page=999',
                                "view=findings&q=%27%3B--", 'view=bogus'])
def test_filters_and_hostile_query_strings(pipe, client, qs):
    hold_unknown_code(pipe)
    assert su(client, 'get', f'/ray7?{qs}').status_code == 200


def test_page_carries_a_csrf_token_and_sends_it(pipe, client):
    """Production runs CSRFProtect. The console's POSTs used to carry no token, so its
    Resolve button failed with a CSRF error and bounced the operator to the login page."""
    body = su(client, 'get', '/ray7').data
    assert b'name="csrf-token"' in body and b'X-CSRFToken' in body


def test_held_is_grouped_by_the_thing_to_fix(pipe, client):
    hold_unknown_code(pipe, acc='H1', n=2)
    hold_unknown_code(pipe, acc='H2', n=1)
    r = su(client, 'get', '/ray7?view=held')
    assert r.data.count(b'SC / ZZ') == 1
    assert pipe.held() == 3


def test_message_detail(pipe, client):
    hold_unknown_code(pipe)
    aid = pipe.scalar("SELECT id FROM hl7_message_archive WHERE ray7_status = 'quarantined'")
    body = su(client, 'get', f'/ray7/message/{aid}').get_json()
    assert body['segments'][0].startswith('MSH|')
    assert [f['rule_code'] for f in body['findings']] == ['UNKNOWN_STATUS_CODE']


def test_message_detail_not_found(pipe, client):
    assert su(client, 'get', '/ray7/message/999999').status_code == 404


# ── resolve: closes a flag, moves no data ─────────────────────────────────────

def test_resolve_a_note(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.notes('ORPHAN_EVENT')[0]['id']
    r = su(client, 'post', f'/ray7/finding/{fid}/resolve', json={'resolution': 'ignored',
                                                                  'note': 'known'})
    assert r.status_code == 200
    f = pipe.notes('ORPHAN_EVENT')[0]
    assert (f['resolution'], f['resolution_note'], f['resolved_by']) == ('ignored', 'known', 4242)


def test_resolve_rejects_an_invented_resolution(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.notes('ORPHAN_EVENT')[0]['id']
    assert su(client, 'post', f'/ray7/finding/{fid}/resolve',
              json={'resolution': 'deleted'}).status_code == 400


def test_resolving_does_not_release_a_held_message(pipe, client):
    hold_unknown_code(pipe)
    fid = pipe.findings('UNKNOWN_STATUS_CODE')[0]['id']
    su(client, 'post', f'/ray7/finding/{fid}/resolve', json={'resolution': 'cleared'})
    assert pipe.held() == 1 and pipe.state('HELD')['arrived_at'] is None


# ── replay held: the release ──────────────────────────────────────────────────

def test_replay_without_a_fix_keeps_them_held(pipe, client):
    hold_unknown_code(pipe)
    body = su(client, 'post', '/ray7/held/replay', json={}).get_json()
    assert (body['replayed'], body['released'], body['still_held']) == (1, 0, 1)
    assert pipe.held() == 1


def test_fix_then_replay_releases_the_message(pipe, client):
    hold_unknown_code(pipe)
    map_code(pipe)
    body = su(client, 'post', '/ray7/held/replay', json={}).get_json()
    assert (body['released'], body['still_held']) == (1, 0)
    assert pipe.held() == 0
    assert pipe.state('HELD')['arrived_at'] == at(280)
    assert pipe.study('HELD')['order_status'] == 'AR'
    assert pipe.q("SELECT arrived_at FROM std_worklist_arrivals") == [{'arrived_at': at(280)}]
    assert pipe.findings('UNKNOWN_STATUS_CODE') == []


def test_replay_can_be_limited_to_one_rule(pipe, client):
    hold_unknown_code(pipe, acc='H1')
    pipe.send(k.message(k.msh('RIS'), k.pid(), k.orc('SC', status='AR', when=k.ts(5)), k.obr()))
    map_code(pipe)
    body = su(client, 'post', '/ray7/held/replay',
              json={'rule': 'UNKNOWN_STATUS_CODE'}).get_json()
    assert (body['replayed'], body['released']) == (1, 1)
    assert pipe.held() == 1                     # the MISSING_IDENTIFIER one is untouched


def test_replay_with_nothing_held(pipe, client):
    body = su(client, 'post', '/ray7/held/replay', json={}).get_json()
    assert body == {'status': 'ok', 'replayed': 0, 'released': 0, 'still_held': 0}


def test_released_report_reaches_the_report_table(pipe, client):
    """A report held for want of an accession — this PACS puts it in a Z-segment.
    Map the field, replay, and the report row and the study's report arrive."""
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'R1', k.hl7(at(m))))
    raw = k.message(k.msh('PACS', None, 'ORU^R01', k.hl7(at(100))), k.pid(),
                    k.obr(result_dt=k.hl7(at(100)), mod='CT'),
                    k.obx('Final read.', 'F'), 'ZDS|R1^PACS')
    pipe.send(raw)
    assert pipe.held() == 1 and pipe.count('hl7_oru_reports') == 0
    pipe.exec("INSERT INTO hl7_field_mappings (message_kind, target_kind, target_field, segment, "
              "field_index, component_index, priority) VALUES ('result', 'parsed', "
              "'accession_number', 'ZDS', 1, 1, 1)")
    from conftest import _reset_caches
    _reset_caches()
    body = su(client, 'post', '/ray7/held/replay', json={}).get_json()
    assert body['released'] == 1
    r = pipe.one("SELECT * FROM hl7_oru_reports WHERE accession_number = 'R1'")
    assert r is not None and r['report_text'].startswith('Final read.')
    assert pipe.study('R1')['study_has_report'] is True
    assert pipe.state('R1')['signed_final_at'] == at(100)


def test_released_order_reaches_the_order_table(pipe, client):
    pipe.send(k.message(k.msh('SAP_HIS', None, 'ORM^O01', k.ts(60)), k.pid(), k.pv1(),
                        k.orc('NW', when=k.ts(60)), k.obr(), 'ZPL|PL-Z^HIS'))
    assert pipe.held() == 1
    pipe.exec("INSERT INTO hl7_field_mappings (message_kind, target_kind, target_field, segment, "
              "field_index, component_index, priority) VALUES ('order', 'parsed', "
              "'placer_order_number', 'ZPL', 1, 1, 1)")
    from conftest import _reset_caches
    _reset_caches()
    su(client, 'post', '/ray7/held/replay', json={})
    assert pipe.held() == 0
    assert pipe.count('hl7_orders') == 1
    assert pipe.order('PL-Z')['order_status'] == 'NW'


def test_release_leaves_other_studies_untouched(pipe, client):
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'OTHER', k.hl7(at(m)), pat='P2'))
    hold_unknown_code(pipe)
    before = pipe.state('OTHER')
    map_code(pipe)
    su(client, 'post', '/ray7/held/replay', json={})
    after = pipe.state('OTHER')
    assert {c: after[c] for c in ('is_closed', 'current_rank', 'completed_at', 'event_count')} == \
           {c: before[c] for c in ('is_closed', 'current_rank', 'completed_at', 'event_count')}


def test_release_is_idempotent(pipe, client):
    hold_unknown_code(pipe)
    map_code(pipe)
    su(client, 'post', '/ray7/held/replay', json={})
    before = pipe.snapshot()
    body = su(client, 'post', '/ray7/held/replay', json={}).get_json()
    assert body['replayed'] == 0 and pipe.snapshot() == before
