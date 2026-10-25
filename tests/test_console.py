"""
The BRAYN console: who may open it, what each screen shows, and every action on it.

Rebuilt 2026-10-25 around the operator's questions (Feed status, To fix, Messages,
Study timeline, Study notes, Settings). Every function of the first console is
still here and still tested: the release is fix the configuration, then
"Reprocess", which re-screens the held messages. A release that cleared a flag but
left the data out would be worse than no button — the operator would believe the
study is back.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


def as_role(client, role, method, url, **kw):
    return getattr(client, method)(url, headers={'X-Test-Role': role}, **kw)


def su(client, method, url, **kw):
    return as_role(client, 'su', method, url, **kw)


def page(client, qs, role='su'):
    r = as_role(client, role, 'get', '/brayn?' + qs)
    assert r.status_code == 200, r.data[-500:]
    return r.data.decode('utf-8')


def hold_unknown_code(pipe, acc='HELD', code='ZZ', n=1, app='RIS'):
    pipe.send(k.status('SC', acc, k.hl7(at(300)), app=app))
    for i in range(n):
        pipe.send(k.status(code, acc, k.hl7(at(280 - i)), ctrl=f'HOLD-{acc}-{i}', app=app))


def map_code(pipe, code='ZZ', state='arrived', rank=60):
    pipe.exec("INSERT INTO hl7_status_map (order_status, canonical_state, ladder_rank) "
              "VALUES (:c, :s, :r)", c=code, s=state, r=rank)
    from conftest import _reset_caches
    _reset_caches()


def held_report(pipe, z='ACC20261025', ctrl=None):
    """An ORU whose accession sits in a Z-segment, so it is held for want of one."""
    pipe.send(k.message(k.msh('PACS', ctrl, 'ORU^R01', k.hl7(at(100))), k.pid(),
                        k.obr(result_dt=k.hl7(at(100)), mod='CT'),
                        k.obx('Final read.', 'F'), f'ZDS|{z}^PACS'))
    return pipe.scalar("SELECT MAX(id) FROM hl7_message_archive")


def detail(client, aid, role='su'):
    r = as_role(client, role, 'get', f'/brayn/message/{aid}')
    assert r.status_code == 200
    return r.get_json()


def field(d, key):
    return next(f for s in d['segments'] for f in s['fields'] if f['key'] == key)


# ── access ────────────────────────────────────────────────────────────────────

def test_anonymous_is_refused(pipe, client):
    assert client.get('/brayn').status_code == 401


@pytest.mark.parametrize('role,code', [('su', 200), ('administrator', 200),
                                       ('implementation', 200), ('user', 403)])
def test_role_access(pipe, client, role, code):
    assert as_role(client, role, 'get', '/brayn').status_code == code


@pytest.mark.parametrize('old,new', [
    ('/ray7', '/brayn'),
    ('/ray7?view=held', '/brayn?view=held'),
    ('/ray7/message/12', '/brayn/message/12'),
])
def test_old_ray7_addresses_redirect(pipe, client, old, new):
    """RAY7 was renamed BRAYN (0137); a bookmark must not land on a 404."""
    r = su(client, 'get', old)
    assert r.status_code == 301 and r.headers['Location'].endswith(new)


def test_replay_is_refused_without_access(pipe, client):
    """Implementation has the console since 2026-10-25 (it maps what BRAYN lists)."""
    hold_unknown_code(pipe)
    r = as_role(client, 'user', 'post', '/brayn/held/replay', json={})
    assert r.status_code == 403
    assert pipe.held() == 1


# ── every screen renders, with and without data ───────────────────────────────

VIEWS = ['status', 'fix', 'messages', 'study', 'notes', 'settings']


def busy(pipe):
    hold_unknown_code(pipe)
    for code, m in (('SC', 300), ('AR', 200), ('CM', 250)):        # completed before arrived
        pipe.send(k.status(code, 'A2', k.hl7(at(m))))
    pipe.send(k.report('A2', k.hl7(at(150))))
    pipe.send(k.status('IP', 'A3', k.ts(5), ae='NOT_A_DEVICE'))     # an unknown value
    held_report(pipe)


@pytest.mark.parametrize('view', VIEWS)
def test_views_render_empty(pipe, client, view):
    body = page(client, f'view={view}')
    assert 'No HL7 message has been received yet' in body


@pytest.mark.parametrize('view', VIEWS)
def test_views_render_with_data(pipe, client, view):
    busy(pipe)
    body = page(client, f'view={view}&acc=A2')
    assert 'BRAYN' in body and 'No HL7 message has been received yet' not in body


@pytest.mark.parametrize('old,expect', [('summary', 'Senders'), ('held', 'Blocks data'),
                                        ('findings', 'Data quality'), ('health', 'Senders')])
def test_old_view_names_land_on_their_successor(pipe, client, old, expect):
    busy(pipe)
    assert expect in page(client, f'view={old}')


@pytest.mark.parametrize('qs', ['view=notes&tab=nonsense', 'view=messages&show=nonsense',
                                'view=messages&type=XYZ', 'view=messages&q=%27%3B--',
                                'view=messages&page=abc', 'view=messages&page=99999999',
                                'view=notes&q=%27%3B--', 'view=study&acc=%27%3B--',
                                'view=bogus', 'view=find&q=', 'view=messages&sender=%00'])
def test_hostile_query_strings(pipe, client, qs):
    busy(pipe)
    r = su(client, 'get', '/brayn?' + qs, follow_redirects=True)
    assert r.status_code == 200


def test_page_carries_a_csrf_token_and_sends_it(pipe, client):
    """Production runs CSRFProtect. The console's POSTs once carried no token, so its
    buttons failed with a CSRF error and bounced the operator to the login page."""
    body = page(client, 'view=status')
    assert 'name="csrf-token"' in body and 'X-CSRFToken' in body


def test_tab_badges_count_what_needs_a_look(pipe, client):
    busy(pipe)
    body = page(client, 'view=status')
    assert 'messages are not in the reports' in body or 'message is not in the reports' in body


# ── Feed status ───────────────────────────────────────────────────────────────

def test_feed_status_lists_each_sender(pipe, client):
    busy(pipe)
    body = page(client, 'view=status')
    for app in ('RIS', 'PACS'):
        assert f'<b class="mono">{app}</b>' in body
    assert 'Data held' in body                     # both senders have held messages
    assert 'MSA-1 = AA' in body                    # the ACK is explained


def test_feed_status_all_clear(pipe, client):
    pipe.send(k.status('SC', 'OK1', k.ts(30)))
    body = page(client, 'view=status')
    assert 'Every message received is in the reports' in body and 'Receiving' in body


# ── To fix ────────────────────────────────────────────────────────────────────

def test_unmapped_code_is_one_card_with_the_fix_on_it(pipe, client):
    hold_unknown_code(pipe, acc='H1', n=2)
    hold_unknown_code(pipe, acc='H2', n=1)
    body = page(client, 'view=fix')
    assert body.count('Status code not mapped') == 1
    assert '3 messages' in body and '2 studies' in body
    assert '<form class="act" data-statusmap' in body and 'Save mapping' in body
    assert pipe.held() == 3


def test_administrator_sees_who_can_fix_it(pipe, client):
    """Mapping writes are su / implementation, as in the mapping studio."""
    hold_unknown_code(pipe)
    body = page(client, 'view=fix', role='administrator')
    assert '<form class="act" data-statusmap' not in body and 'An implementation engineer can map' in body


def test_mapped_code_offers_reprocess(pipe, client):
    hold_unknown_code(pipe)
    map_code(pipe)
    body = page(client, 'view=fix')
    assert 'Mapped since: ZZ' in body and 'data-replay="UNKNOWN_STATUS_CODE"' in body


def test_empty_status_is_its_own_card(pipe, client):
    """No ORC-5 at all cannot be mapped as a code: the card says what to do instead."""
    pipe.send(k.message(k.msh('RIS'), k.pid(), k.orc('XO', acc='E1', when=k.ts(5)), k.obr(acc='E1')))
    body = page(client, 'view=fix')
    assert 'No status code in ORC-5' in body and 'is not a new-order or cancel code' in body
    assert '<form class="act" data-statusmap' not in body


def test_missing_identifier_card_offers_the_field_picker(pipe, client):
    held_report(pipe)
    body = page(client, 'view=fix')
    assert 'No accession or order number in ORU^R01 from PACS' in body
    assert 'Open a message and pick the field' in body


def test_unknown_device_card_and_its_fix(pipe, client):
    pipe.send(k.status('IP', 'A3', k.ts(5), ae='NOT_A_DEVICE'))
    assert 'Unknown device' in page(client, 'view=fix')
    r = su(client, 'post', '/brayn/device/save', json={'aetitle': ' not_a_device ', 'modality': 'ct'})
    assert r.status_code == 200 and r.get_json()['modality'] == 'CT'
    assert pipe.one("SELECT modality FROM aetitle_modality_map WHERE aetitle = 'NOT_A_DEVICE'") \
        == {'modality': 'CT'}
    assert 'Unknown device' not in page(client, 'view=fix')


def test_device_save_updates_an_existing_row(pipe, client):
    su(client, 'post', '/brayn/device/save', json={'aetitle': 'CT64_RH', 'modality': 'MR'})
    assert pipe.count('aetitle_modality_map', "UPPER(aetitle) = 'CT64_RH'") == 1
    assert pipe.scalar("SELECT modality FROM aetitle_modality_map WHERE aetitle = 'CT64_RH'") == 'MR'


@pytest.mark.parametrize('body,code', [({'aetitle': '', 'modality': 'CT'}, 400),
                                       ({'aetitle': 'X1', 'modality': 'computed tomography'}, 400),
                                       ({'aetitle': 'X1', 'modality': ''}, 400)])
def test_device_save_validates(pipe, client, body, code):
    assert su(client, 'post', '/brayn/device/save', json=body).status_code == code


def test_device_save_is_implementation_work(pipe, client):
    r = as_role(client, 'administrator', 'post', '/brayn/device/save',
                json={'aetitle': 'X1', 'modality': 'CT'})
    assert r.status_code == 403 and pipe.count('aetitle_modality_map', "aetitle = 'X1'") == 0


def test_unknown_result_status_card_and_its_fix(pipe, client):
    pipe.send(k.status('CM', 'R1', k.ts(60)))
    pipe.send(k.report('R1', k.ts(30), result_status='X'))
    assert 'Result status not mapped' in page(client, 'view=fix')
    r = su(client, 'post', '/brayn/result-status/save',
           json={'result_status': 'x', 'canonical_state': 'signed_final', 'sending_app': ''})
    assert r.status_code == 200
    assert pipe.one("SELECT canonical_state, ladder_rank FROM hl7_result_status_map "
                    "WHERE sending_app = '' AND result_status = 'X'") == \
        {'canonical_state': 'signed_final', 'ladder_rank': 140}
    assert 'Result status not mapped' not in page(client, 'view=fix')


def test_result_status_save_validates(pipe, client):
    assert su(client, 'post', '/brayn/result-status/save',
              json={'result_status': 'X', 'canonical_state': 'completed'}).status_code == 400


def test_step_switched_off_but_sent_card(pipe, client):
    pipe.profile(expected=['scheduled', 'arrived', 'completed'])
    pipe.send(k.status('IP', 'A4', k.ts(5)))
    body = page(client, 'view=fix')
    assert 'Started received from RIS, but switched off for this site' in body
    pipe.profile(expected=['scheduled', 'arrived', 'started', 'completed'])
    assert 'switched off for this site' not in page(client, 'view=fix')


def test_sender_alert_card(pipe, client):
    pipe.exec("""INSERT INTO brayn_findings (rule_code, severity, sending_app, detail)
                 VALUES ('OUT_OF_SEQUENCE_DELIVERY', 'warning', 'RIS',
                         '{"count": 15, "messages": 120, "rate_pct": 12.5, "threshold_pct": 10, "window_minutes": 60}')""")
    pipe.send(k.status('SC', 'S1', k.ts(5)))
    body = page(client, 'view=fix')
    assert 'RIS: Delivered out of order' in body
    assert '12.5% of messages in the last 1 h (15 of 120); alert above 10%.' in body


# ── replay held: the release ──────────────────────────────────────────────────

def test_replay_without_a_fix_keeps_them_held(pipe, client):
    hold_unknown_code(pipe)
    body = su(client, 'post', '/brayn/held/replay', json={}).get_json()
    assert (body['replayed'], body['released'], body['still_held']) == (1, 0, 1)
    assert pipe.held() == 1


def test_fix_then_replay_releases_the_message(pipe, client):
    hold_unknown_code(pipe)
    map_code(pipe)
    body = su(client, 'post', '/brayn/held/replay', json={}).get_json()
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
    body = su(client, 'post', '/brayn/held/replay', json={'rule': 'UNKNOWN_STATUS_CODE'}).get_json()
    assert (body['replayed'], body['released']) == (1, 1)
    assert pipe.held() == 1                     # the MISSING_IDENTIFIER one is untouched


def test_replay_can_be_limited_to_one_sender(pipe, client):
    """What a To-fix card sends: its rule and its sender."""
    hold_unknown_code(pipe, acc='H1', app='RIS')
    hold_unknown_code(pipe, acc='H2', app='RIS2')
    map_code(pipe)
    body = su(client, 'post', '/brayn/held/replay',
              json={'rule': 'UNKNOWN_STATUS_CODE', 'sender': 'RIS2'}).get_json()
    assert (body['replayed'], body['released']) == (1, 1)
    assert pipe.held() == 1


def test_replay_with_nothing_held(pipe, client):
    body = su(client, 'post', '/brayn/held/replay', json={}).get_json()
    assert body == {'status': 'ok', 'replayed': 0, 'released': 0, 'still_held': 0}


def test_released_report_reaches_the_report_table(pipe, client):
    """A report held for want of an accession — this PACS puts it in a Z-segment.
    Map the field, replay, and the report row and the study's report arrive."""
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'R1', k.hl7(at(m))))
    held_report(pipe, z='R1')
    assert pipe.held() == 1 and pipe.count('hl7_oru_reports') == 0
    pipe.exec("INSERT INTO hl7_field_mappings (sending_app, message_kind, target_kind, "
              "target_field, segment, field_index, component_index, priority) VALUES "
              "('PACS', 'result', 'parsed', 'accession_number', 'ZDS', 1, 1, 1)")
    from conftest import _reset_caches
    _reset_caches()
    body = su(client, 'post', '/brayn/held/replay', json={'rule': 'MISSING_IDENTIFIER',
                                                          'sender': 'PACS'}).get_json()
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
    su(client, 'post', '/brayn/held/replay', json={})
    assert pipe.held() == 0
    assert pipe.count('hl7_orders') == 1
    assert pipe.order('PL-Z')['order_status'] == 'NW'


def test_release_leaves_other_studies_untouched(pipe, client):
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, 'OTHER', k.hl7(at(m)), pat='P2'))
    hold_unknown_code(pipe)
    before = pipe.state('OTHER')
    map_code(pipe)
    su(client, 'post', '/brayn/held/replay', json={})
    after = pipe.state('OTHER')
    assert {c: after[c] for c in ('is_closed', 'current_rank', 'completed_at', 'event_count')} == \
           {c: before[c] for c in ('is_closed', 'current_rank', 'completed_at', 'event_count')}


def test_release_is_idempotent(pipe, client):
    hold_unknown_code(pipe)
    map_code(pipe)
    su(client, 'post', '/brayn/held/replay', json={})
    before = pipe.snapshot()
    body = su(client, 'post', '/brayn/held/replay', json={}).get_json()
    assert body['replayed'] == 0 and pipe.snapshot() == before


# ── Messages ──────────────────────────────────────────────────────────────────

def test_messages_say_whether_each_is_in_the_reports(pipe, client):
    busy(pipe)
    body = page(client, 'view=messages')
    assert 'Not in reports' in body and 'In reports' in body
    assert 'ORC-5 = ZZ (not mapped)' in body and 'Report, final' in body


def test_messages_filter_held(pipe, client):
    busy(pipe)
    body = page(client, 'view=messages&show=held')
    assert 'In reports</span>' not in body and 'Status code not mapped' in body


def test_messages_filter_by_sender_and_type(pipe, client):
    busy(pipe)
    body = page(client, 'view=messages&sender=PACS')
    assert 'ORU^R01' in body and 'ORC-5 = ZZ' not in body
    body = page(client, 'view=messages&type=ORU')
    assert 'ORM^O01' not in body.split('<tbody>', 1)[1]


def test_messages_search_by_accession(pipe, client):
    busy(pipe)
    body = page(client, 'view=messages&q=A2')
    rows = body.split('<tbody>', 1)[1]
    assert 'A2' in rows and 'NOT_A_DEVICE' not in rows and 'HELD' not in rows


def test_messages_page_older(pipe, client):
    for i in range(55):
        pipe.send(k.status('SC', f'P{i}', k.ts(300 - i)))
    first = page(client, 'view=messages')
    assert 'Older' in first
    assert 'Newer' in page(client, 'view=messages&page=2')


def test_find_opens_a_study_or_searches_messages(pipe, client):
    pipe.send(k.status('SC', 'F1', k.ts(30), placer='PL-F1'))
    for q in ('F1', 'PL-F1'):
        r = su(client, 'get', f'/brayn?view=find&q={q}')
        assert r.status_code == 302 and 'view=study' in r.headers['Location']
    r = su(client, 'get', '/brayn?view=find&q=RIS-NOPE')
    assert 'view=messages' in r.headers['Location'] and 'q=RIS-NOPE' in r.headers['Location']


# ── one message, explained ────────────────────────────────────────────────────

def test_held_message_shows_its_problem_field(pipe, client):
    hold_unknown_code(pipe)
    aid = pipe.scalar("SELECT id FROM hl7_message_archive WHERE brayn_status = 'quarantined'")
    d = detail(client, aid)
    assert d['verdict']['state'] == 'held' and d['ack'] == 'AA'
    assert 'ORC-5 = ZZ with ORC-1 = SC is not in the status map' in d['verdict']['text']
    assert 'not in the status map' in field(d, 'ORC-5')['problem']
    step = next(x for x in d['read'] if x['label'] == 'Step')
    assert step['problem'] and step['value'] == 'ZZ → unknown' and step['source'] == 'ORC-5'
    assert [f['rule_code'] for f in d['findings']] == ['UNKNOWN_STATUS_CODE']


def test_accepted_message_shows_what_brayn_read(pipe, client):
    pipe.send(k.status('AR', 'OK1', k.ts(30)))
    aid = pipe.scalar("SELECT MAX(id) FROM hl7_message_archive")
    d = detail(client, aid)
    assert d['verdict'] == {'state': 'in', 'title': 'In reports',
                            'text': 'Applied to study OK1: Arrived.'}
    assert field(d, 'ORC-3')['used'] == 'Accession'
    assert field(d, 'ORC-9')['used'] == 'Time of this step'
    assert field(d, 'MSH-10')['used'].startswith('Control ID')
    read = {x['label']: x for x in d['read']}
    assert read['Study (accession)']['value'] == 'OK1' and read['Study (accession)']['source'] == 'ORC-3.1'
    assert read['Step']['value'] == 'AR → Arrived'
    assert read['Device']['value'] == 'CT64_RH → CT'
    assert d['segments'][0]['name'] == 'MSH' and field(d, 'MSH-3')['name']


def test_unknown_device_is_the_problem_field(pipe, client):
    pipe.send(k.status('IP', 'A3', k.ts(5), ae='NOT_A_DEVICE'))
    d = detail(client, pipe.scalar("SELECT MAX(id) FROM hl7_message_archive"))
    assert d['verdict']['state'] == 'in'
    assert 'no modality yet' in field(d, 'OBR-21')['problem']


def test_missing_accession_suggests_the_field_holding_a_known_study(pipe, client):
    pipe.send(k.status('SC', 'ACC20261025', k.ts(300)))
    aid = held_report(pipe, z='ACC20261025')
    d = detail(client, aid)
    assert d['verdict']['state'] == 'held' and d['message_kind'] == 'result'
    top = d['suggestions'][0]
    assert (top['key'], top['segment'], top['field'], top['component']) == ('ZDS-1.1', 'ZDS', 1, 1)
    assert 'Same value as study ACC20261025' in top['reason']
    assert field(d, 'OBR-3')['problem'].startswith('Empty')
    assert field(d, 'ZDS-1')['suggested']


def test_missing_accession_suggests_by_pattern(pipe, client):
    for acc in ('2610250013', '2610250014', '2610250015'):
        pipe.send(k.status('SC', acc, k.ts(300)))
    d = detail(client, held_report(pipe, z='2610259999'))
    assert d['suggestions'][0]['key'] == 'ZDS-1.1'
    assert "Same pattern as this site's accessions (9999999999)" in d['suggestions'][0]['reason']


def test_message_detail_not_found(pipe, client):
    assert su(client, 'get', '/brayn/message/999999').status_code == 404


# ── Study timeline ────────────────────────────────────────────────────────────

def test_study_timeline_shows_steps_sources_and_contradictions(pipe, client):
    for code, m in (('SC', 300), ('AR', 200), ('CM', 250)):
        pipe.send(k.status(code, 'T1', k.hl7(at(m))))
    pipe.send(k.report('T1', k.hl7(at(150))))
    body = page(client, 'view=study&acc=T1')
    for label in ('Scheduled', 'Arrived', 'Started', 'Completed', 'Report final'):
        assert f'<b>{label}</b>' in body
    assert 'contradicts another step' in body and 'never received' in body
    assert 'is earlier than Arrived' in body              # the note, in a sentence
    assert 'What the reports show' in body and 'TAT starts at' in body
    assert 'RIS · CM' in body                            # a step links to its message


def test_study_found_by_order_number(pipe, client):
    pipe.send(k.status('SC', 'T2', k.ts(30), placer='PL-T2'))
    assert '<h2>T2</h2>' in page(client, 'view=study&acc=PL-T2')


def test_study_with_only_held_messages(pipe, client):
    pipe.send(k.status('ZZ', 'ONLYHELD', k.ts(30)))
    assert 'has only held messages' in page(client, 'view=study&acc=ONLYHELD')


def test_unknown_study(pipe, client):
    pipe.send(k.status('SC', 'T4', k.ts(30)))
    assert 'No study or order' in page(client, 'view=study&acc=NOPE')


def test_settings_before_the_first_message(pipe, client):
    """An engineer prepares the checks before the senders are pointed at RAYD."""
    body = page(client, 'view=settings')
    assert 'No HL7 message has been received yet' in body and 'Arrived, not started' in body


def test_study_list_when_no_accession(pipe, client):
    pipe.send(k.status('SC', 'T3', k.ts(30)))
    assert 'Recently active studies' in page(client, 'view=study') and 'T3' in page(client, 'view=study')


# ── Study notes ───────────────────────────────────────────────────────────────

def test_notes_are_sentences_with_their_study(pipe, client):
    for code, m in (('SC', 300), ('AR', 200), ('CM', 250)):
        pipe.send(k.status(code, 'N1', k.hl7(at(m))))
    body = page(client, 'view=notes&tab=quality')
    assert 'Times contradict the lifecycle' in body and 'is earlier than Arrived' in body
    assert 'view=study&acc=N1' in body and 'Closes by itself when fixed' in body


def test_delays_tab(pipe, client):
    pipe.send(k.status('AR', 'D1', k.ts(300)))
    pipe.sweep()
    body = page(client, 'view=notes&tab=delays')
    assert 'Arrived, not started' in body and 'Limit 4 h.' in body


def test_resolve_a_note(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.notes('ORPHAN_EVENT')[0]['id']
    r = su(client, 'post', f'/brayn/finding/{fid}/resolve', json={'resolution': 'ignored',
                                                                   'note': 'known'})
    assert r.status_code == 200
    f = pipe.notes('ORPHAN_EVENT')[0]
    assert (f['resolution'], f['resolution_note'], f['resolved_by']) == ('ignored', 'known', 4242)
    assert 'known' in page(client, 'view=notes&tab=closed')


def test_resolve_rejects_an_invented_resolution(pipe, client):
    pipe.send(k.status('AR', 'A1', k.ts(5)))
    fid = pipe.notes('ORPHAN_EVENT')[0]['id']
    assert su(client, 'post', f'/brayn/finding/{fid}/resolve',
              json={'resolution': 'deleted'}).status_code == 400


def test_resolving_does_not_release_a_held_message(pipe, client):
    hold_unknown_code(pipe)
    fid = pipe.findings('UNKNOWN_STATUS_CODE')[0]['id']
    su(client, 'post', f'/brayn/finding/{fid}/resolve', json={'resolution': 'cleared'})
    assert pipe.held() == 1 and pipe.state('HELD')['arrived_at'] is None


# ── Settings ──────────────────────────────────────────────────────────────────

def test_settings_list_every_check_in_plain_words(pipe, client):
    body = page(client, 'view=settings')
    for words in ('Held until fixed', 'Data quality notes', 'Delays', 'Sender behaviour',
                  'Arrived, not started', 'Same MSH-10, different content', 'Status codes: ORC-5'):
        assert words in body
    assert 'LADDER_REGRESSION' not in body          # retired in 0135


def test_settings_are_read_only_for_administrators(pipe, client):
    body = page(client, 'view=settings', role='administrator')
    assert 'Read-only: only implementation engineers can change checks' in body
    assert 'data-rulefield="enabled"' not in body


def test_switching_a_delay_check_off_stops_it(pipe, client):
    r = su(client, 'post', '/brayn/rules/save', json={'rule_code': 'STALLED_ARRIVED', 'enabled': False})
    assert r.status_code == 200
    pipe.send(k.status('AR', 'A1', k.ts(300)))
    pipe.sweep()
    assert pipe.findings('STALLED_ARRIVED') == []


def test_a_limit_saved_in_the_console_is_the_one_the_sweep_uses(pipe, client):
    su(client, 'post', '/brayn/rules/save', json={'rule_code': 'STALLED_ARRIVED', 'threshold_minutes': 30})
    pipe.send(k.status('AR', 'A1', k.ts(45)))
    pipe.sweep()
    assert len(pipe.findings('STALLED_ARRIVED')) == 1
    assert pipe.scalar("SELECT threshold_minutes FROM brayn_rules WHERE rule_code = 'STALLED_ARRIVED' "
                       "AND modality = ''") == 30


def test_rate_limit_and_level(pipe, client):
    assert su(client, 'post', '/brayn/rules/save',
              json={'rule_code': 'OUT_OF_SEQUENCE_DELIVERY', 'threshold_pct': 25, 'severity': 'info'}
              ).status_code == 200
    assert pipe.one("SELECT threshold_pct, severity FROM brayn_rules WHERE rule_code = "
                    "'OUT_OF_SEQUENCE_DELIVERY' AND modality = ''") == {'threshold_pct': 25, 'severity': 'info'}


@pytest.mark.parametrize('body,code', [
    ({'rule_code': 'UNKNOWN_STATUS_CODE', 'enabled': False}, 400),        # intake: always on
    ({'rule_code': 'MISSING_IDENTIFIER', 'severity': 'warning'}, 400),
    ({'rule_code': 'STALLED_ARRIVED', 'severity': 'critical'}, 400),      # only intake holds
    ({'rule_code': 'STALLED_ARRIVED', 'threshold_minutes': 'soon'}, 400),
    ({'rule_code': 'STALLED_ARRIVED', 'threshold_minutes': 0}, 400),
    ({'rule_code': 'STALLED_ARRIVED', 'threshold_pct': 5}, 400),          # has no rate
    ({'rule_code': 'OUT_OF_SEQUENCE_DELIVERY', 'threshold_pct': 250}, 400),
    ({'rule_code': 'STALLED_ARRIVED'}, 400),                              # nothing to change
    ({'rule_code': 'NO_SUCH_RULE', 'enabled': True}, 404),
])
def test_rule_save_validates(pipe, client, body, code):
    assert su(client, 'post', '/brayn/rules/save', json=body).status_code == code


def test_rule_save_is_implementation_work(pipe, client):
    r = as_role(client, 'administrator', 'post', '/brayn/rules/save',
                json={'rule_code': 'STALLED_ARRIVED', 'enabled': False})
    assert r.status_code == 403
    assert pipe.scalar("SELECT enabled FROM brayn_rules WHERE rule_code = 'STALLED_ARRIVED' "
                       "AND modality = ''") is True
