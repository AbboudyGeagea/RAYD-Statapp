"""
The BRAYN console on LAUMC (routes/brayn_console.py): admin only, in LAUMC's words
("not identified", never "held" or "not in reports"), every fix writes BRAYN's own
tables only, and a fixed message becomes identified when re-checked.
"""
from datetime import datetime

import pytest
from sqlalchemy import text

import hl7kit as k

OTHER_ROLES = ('viewer', 'viewer2', 'tec', 'finance', 'secretary')
# The HL7 branch's words, which are untrue on LAUMC: nothing is held, and the
# reports never depended on BRAYN.
HL7_ONLY = ('Not in reports', 'In reports', 'in the reports', 'Reprocess', 'held message',
            'Held until fixed', 'HL7 Mapping', 'Data held')
LAUMC_TABLES = ['hl7_orders', 'hl7_oru_reports', 'hl7_scn_studies', 'aetitle_modality_map']


def get(client, url, role='admin'):
    return client.get(url, headers={'X-Test-Role': role} if role else {})


def post(client, url, body=None, role='admin'):
    return client.post(url, json=body or {}, headers={'X-Test-Role': role} if role else {})


def _snapshot(pipe):
    return {t: sorted(map(repr, pipe.q(f'SELECT * FROM {t}'))) for t in LAUMC_TABLES}


@pytest.fixture
def traffic(pipe):
    """BRAYN on, an afternoon of messages screened, one of each kind of trouble."""
    pipe.brayn()
    pipe.send(k.order('PL-10', acc='ACC10', when=k.ts(200)))
    for _, raw in k.lifecycle('ACC10', start_minutes_ago=150, placer='PL-10'):
        pipe.send(raw)
    pipe.send(k.report('ACC10', k.ts(20)))
    pipe.send(k.status('ZZ', 'ACC40', k.ts(10)))                         # unmapped code
    pipe.send(k.message(k.msh('PACS', None, 'ORU^R01', k.ts(5)), k.pid(),  # no identifier
                        k.obr(result_dt=k.ts(5), extra={18: 'ACC77'}), k.obx('Free text.')))
    pipe.screen()
    return pipe


# ── who may see it ────────────────────────────────────────────────────────────

def test_only_an_administrator_opens_the_console(client, pipe):
    assert get(client, '/brayn').status_code == 200
    for role in OTHER_ROLES:
        assert get(client, '/brayn', role).status_code == 403, role
        assert post(client, '/brayn/switch', {'enabled': True}, role).status_code == 403, role
        assert get(client, '/brayn/message/1', role).status_code == 403, role
    assert get(client, '/brayn', None).status_code in (401, 302)
    assert pipe.scalar("SELECT value FROM settings WHERE key = 'brayn_enabled'") == 'false'


# ── the switch ────────────────────────────────────────────────────────────────

def test_the_console_says_brayn_is_off_and_turns_it_on(client, pipe):
    banner = b'<strong>BRAYN is off</strong>'
    assert banner in get(client, '/brayn').data
    r = post(client, '/brayn/switch', {'enabled': True})
    assert r.status_code == 200 and r.json['enabled'] is True
    assert pipe.scalar("SELECT value FROM settings WHERE key = 'brayn_enabled'") == 'true'
    assert banner not in get(client, '/brayn').data
    pipe.send(k.order('PL-1', when=k.ts(5)))
    assert pipe.count('hl7_message_archive') == 1

    post(client, '/brayn/switch', {'enabled': False})
    pipe.send(k.order('PL-2', when=k.ts(4)))
    assert pipe.count('hl7_message_archive') == 1
    assert pipe.count('hl7_orders') == 2


# ── every screen, in LAUMC's words ────────────────────────────────────────────

SCREENS = ['/brayn', '/brayn?view=fix', '/brayn?view=messages',
           '/brayn?view=messages&show=in', '/brayn?view=messages&show=held',
           '/brayn?view=messages&show=error', '/brayn?view=messages&q=ACC10',
           '/brayn?view=study', '/brayn?view=study&acc=ACC10', '/brayn?view=study&acc=NOPE',
           '/brayn?view=notes', '/brayn?view=notes&tab=delays', '/brayn?view=notes&tab=closed',
           '/brayn?view=settings']


def test_every_screen_renders_in_laumc_words(client, traffic):
    for url in SCREENS:
        r = get(client, url)
        assert r.status_code == 200, url
        page = r.get_data(as_text=True)
        assert [w for w in HL7_ONLY if w in page] == [], url
    fix = get(client, '/brayn?view=fix').get_data(as_text=True)
    assert 'Status code not mapped' in fix and 'No accession or order number' in fix
    assert 'Not identified' in fix


def test_find_opens_a_study_or_searches_messages(client, traffic):
    assert get(client, '/brayn?view=find&q=ACC10').headers['Location'].endswith(
        '/brayn?view=study&acc=ACC10')
    assert 'view=messages' in get(client, '/brayn?view=find&q=XYZ').headers['Location']


def test_a_message_brayn_cannot_place_is_explained_as_not_identified(client, traffic):
    aid = traffic.scalar("SELECT id FROM hl7_message_archive WHERE brayn_status = 'quarantined' "
                         "AND message_type LIKE 'ORM%'")
    d = get(client, f'/brayn/message/{aid}').json
    assert d['verdict']['title'] == 'Not identified'
    assert 'LAUMC stored the message' in d['verdict']['text']
    assert d['ack'] == ''
    assert [f['rule_code'] for f in d['findings']] == ['UNKNOWN_STATUS_CODE']
    ok = traffic.scalar("SELECT MIN(id) FROM hl7_message_archive WHERE brayn_status = 'accepted'")
    assert get(client, f'/brayn/message/{ok}').json['verdict']['title'] == 'Screened'


# ── fixes: BRAYN's tables only ────────────────────────────────────────────────

def test_mapping_a_status_code_and_rechecking_identifies_the_message(client, traffic):
    before = _snapshot(traffic)
    counted = traffic.scalar("SELECT SUM(n) FROM brayn_interface_stats WHERE metric = 'MESSAGES'")

    r = post(client, '/brayn/status-code/save', {'order_status': 'ZZ', 'canonical_state': 'arrived'})
    assert r.status_code == 200
    r = post(client, '/brayn/held/replay', {'rule': 'UNKNOWN_STATUS_CODE', 'sender': 'RIS'})
    assert r.json == {'status': 'ok', 'replayed': 1, 'released': 1, 'still_held': 0}

    assert traffic.scalar("SELECT arrived_at IS NOT NULL FROM brayn_study_state "
                          "WHERE accession_number = 'ACC40'") is True
    assert traffic.count('brayn_findings', "rule_code = 'UNKNOWN_STATUS_CODE'") == 0
    assert traffic.scalar("SELECT SUM(n) FROM brayn_interface_stats "
                          "WHERE metric = 'MESSAGES'") == counted       # not counted twice
    assert _snapshot(traffic) == before                                  # LAUMC untouched


def test_picking_the_accession_field_and_rechecking_identifies_the_message(client, traffic):
    before = _snapshot(traffic)
    r = post(client, '/brayn/field-map/save', {
        'sending_app': 'PACS', 'message_kind': 'result', 'target_field': 'accession_number',
        'segment': 'OBR', 'field_index': 18})
    assert r.status_code == 200
    r = post(client, '/brayn/held/replay', {'rule': 'MISSING_IDENTIFIER', 'sender': 'PACS'})
    assert r.json['released'] == 1
    assert traffic.count('brayn_findings', "rule_code = 'MISSING_IDENTIFIER'") == 0
    assert traffic.count('hl7_message_archive', "brayn_status = 'quarantined'") == 1   # ZZ still
    assert traffic.count('brayn_study_state', "accession_number = 'ACC77'") == 1
    assert traffic.count('hl7_study_events', "accession_number = 'ACC77'") == 1
    assert _snapshot(traffic) == before


def test_rechecking_without_a_fix_changes_nothing(client, traffic):
    r = post(client, '/brayn/held/replay', {'rule': 'UNKNOWN_STATUS_CODE', 'sender': 'RIS'})
    assert r.json == {'status': 'ok', 'replayed': 1, 'released': 0, 'still_held': 1}
    assert traffic.count('brayn_findings', "rule_code = 'UNKNOWN_STATUS_CODE'") == 1


def test_the_field_save_only_sets_brayns_identifiers(client, pipe):
    for body in ({'target_field': 'patient_id', 'segment': 'PID', 'field_index': 3},
                 {'target_field': 'accession_number', 'segment': 'MSH', 'field_index': 10},
                 {'target_field': 'accession_number', 'segment': 'OBR', 'field_index': 0}):
        assert post(client, '/brayn/field-map/save', body).status_code == 400, body
    assert pipe.count('hl7_field_mappings', "notes = 'set from the BRAYN console'") == 0


def test_checks_save_but_the_identifying_ones_stay_on(client, pipe):
    assert post(client, '/brayn/rules/save',
                {'rule_code': 'TIME_CONTRADICTION', 'enabled': False}).status_code == 200
    assert pipe.scalar("SELECT enabled FROM brayn_rules WHERE rule_code = 'TIME_CONTRADICTION' "
                       "AND modality = ''") is False
    assert post(client, '/brayn/rules/save',
                {'rule_code': 'MISSING_IDENTIFIER', 'enabled': False}).status_code == 400
    assert post(client, '/brayn/rules/save',
                {'rule_code': 'UNREPORTED', 'threshold_minutes': 600}).status_code == 200
    assert pipe.scalar("SELECT threshold_minutes FROM brayn_rules WHERE rule_code = 'UNREPORTED' "
                       "AND modality = ''") == 600


def test_a_step_switched_off_stops_counting_its_order(client, pipe):
    r = post(client, '/brayn/ladder-profile/save',
             {'rung': 'started', 'expected': False, 'enforce_order': True})
    assert r.json == {'status': 'ok', 'rung': 'started', 'expected': False, 'enforce_order': False}
    assert post(client, '/brayn/ladder-profile/save', {'rung': 'nope'}).status_code == 400


def test_the_result_status_save(client, pipe):
    r = post(client, '/brayn/result-status/save',
             {'result_status': 'Q', 'sending_app': 'PACS', 'canonical_state': 'signed_final'})
    assert r.status_code == 200
    assert pipe.scalar("SELECT ladder_rank FROM hl7_result_status_map "
                       "WHERE sending_app = 'PACS' AND result_status = 'Q'") == 140


# ── the console runs on hospital time ─────────────────────────────────────────

def test_console_requests_run_on_hospital_time(app):
    from db import db
    from routes import brayn_console
    from utils.brayn_shadow import hospital_zone
    with app.test_request_context('/brayn', headers={'X-Test-Role': 'admin'}):
        app.preprocess_request()
        assert db.session.execute(text('SHOW timezone')).scalar() == hospital_zone()
        now = brayn_console._now()
        app.do_teardown_request()
    assert abs((now - datetime.now()).total_seconds()) < 120
