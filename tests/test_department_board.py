"""
The Live Department board (item 38, operator 2026-11-19): one card per RIS room of one
hospital, from BRAYN's ladder, built from RIS messages shaped like LAUMC's (room code in
ORC-16, appointment in OBR-36, booked minutes in OBR-27) sent through the real listener
(hl7kit) and screened by the BRAYN shadow.

Times are minutes before now, as the RIS would stamp its events. The day start is moved to
midnight where utilization is checked, so the figures do not depend on the hour the suite
runs at.
"""
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

import hl7kit as k
from utils import department_board as board

ADMIN = 4242
# RIS rooms: (AE, room code, room name, modality, site)
ROOMS = (('CT64_RH', 'CT64', 'CT Room - 750 HD (GE)', 'CT', 'RH'),
         ('CT2_RH', 'CT128 ED', 'CT128 Emergency', 'CT', 'RH'),
         ('DEFINIUM1', 'XR1', 'X-Ray Room 1 - Definium (GE)', 'DX', 'RH'),
         ('IDLEXR', 'PORT2', 'Portable 2', 'DX', 'RH'),
         ('VIVIDE9-003168', 'US CARDIO 2', 'ECHO 10TH', 'US', 'RH'),
         ('RHPACSLAUMCRH', 'RH-PACS', 'PACS External Upload', 'OT', 'RH'),
         ('AFFINITI1', 'SJH-US2', 'Ultrasound-2', 'US', 'SJH'))
USED = ('CT64', 'CT128 ED', 'XR1', 'US CARDIO 2', 'SJH-US2')    # booked on in the last 30 days


@pytest.fixture
def rooms(pipe):
    """BRAYN on, every message screened once sent; the RIS rooms above, open today on the
    RIS (except PORT2), durations for two CT procedures."""
    pipe.brayn()
    send = pipe.send

    def send_and_screen(raw):
        ack = send(raw)
        pipe.screen()
        return ack

    pipe.send = send_and_screen
    for i, (ae, code, name, mod, site) in enumerate(ROOMS):
        pipe.exec("""INSERT INTO aetitle_modality_map (aetitle, modality, station_name, room_code, ris_modality_key,
                                                       daily_capacity_minutes, site_id)
                     SELECT :ae, :mod, :name, :code, :key, 480, id FROM sites WHERE code = :site""",
                  ae=ae, mod=mod, name=name, code=code, key=990100 + i, site=site)
        if ae != 'IDLEXR':
            pipe.exec("INSERT INTO std_device_weekly_availability (aetitle, day_of_week, available_minutes) "
                      "VALUES (:ae, :d, 480) ON CONFLICT DO NOTHING", ae=ae, d=datetime.now().weekday())
    pipe.exec("""INSERT INTO procedure_duration_map (procedure_code, duration_minutes, modality) VALUES
                 ('CTABD', 30, 'CT'), ('CTHEAD', 15, 'CT')""")
    for code in USED:                       # an exam booked five days ago, long closed
        pipe.exec("""INSERT INTO brayn_study_state (accession_number, ris_room, appointment_at, current_rank,
                                                    event_count, is_closed, first_seen_at, updated_at)
                     VALUES (:acc, :room, now() - interval '5 days', 140, 1, TRUE, now(), now())""",
                  acc='OLD-' + code, room=code)
    yield pipe
    pipe.exec("DELETE FROM std_device_weekly_availability WHERE aetitle = ANY(:aes)", aes=[r[0] for r in ROOMS])


def study(pipe, acc, room='CT64', proc='CTABD', desc='CT ABDOMEN AND PELVIS', pat='P0001',
          appt=0, minutes=None, **steps):
    """RIS events for one accession on a room; steps = scheduled / arrived / started /
    completed / cancelled, each in minutes ago; appt = the appointment, minutes from now
    (None: no appointment); minutes = booked minutes (OBR-27)."""
    when = (datetime.now() + timedelta(minutes=appt)).strftime('%Y%m%d%H%M%S') if appt is not None else None
    for code, step in (('SC', 'scheduled'), ('AR', 'arrived'), ('IP', 'started'),
                       ('CM', 'completed'), ('CA', 'cancelled')):
        if step in steps:
            pipe.send(k.status(code, acc, k.ts(steps[step]), pat=pat, ae=None, mod=None,
                               proc=proc, desc=desc, room=room, appt=when, minutes=minutes))


def get_board(app, site='RH', **kw):
    with app.app_context():
        return board.build_board(site=site, **kw)


def card(b, key):
    return next(r for lane in b['lanes'] for r in lane['rooms'] if r['key'] == key)


def keys(b):
    return {r['key'] for lane in b['lanes'] for r in lane['rooms']}


# ── the pieces ────────────────────────────────────────────────────────────────

T0 = datetime(2026, 11, 2, 7, 0)


def test_busy_minutes_merge_overlaps_and_clip_to_the_window():
    m = lambda h, mi: T0.replace(hour=h, minute=mi)
    intervals = [(m(6, 30), m(7, 20)),          # starts before the window: from 07:00
                 (m(8, 0), m(8, 30)), (m(8, 15), m(8, 45)),   # overlap: 08:00-08:45 once
                 (m(9, 0), m(9, 0)),            # empty
                 (m(9, 50), m(11, 0))]          # runs past now: to 10:00
    assert board.merged_busy_minutes(intervals, m(7, 0), m(10, 0)) == 20 + 45 + 10


def test_opening_window():
    day0 = T0.replace(hour=0)
    assert board.window_start_for(day0, 480, 7) == T0
    assert board.window_start_for(day0, 1440, 7) == day0          # a 24-hour room
    assert board.open_minutes_so_far(T0, T0 + timedelta(hours=3), 480) == 180
    assert board.open_minutes_so_far(T0, T0 + timedelta(hours=10), 480) == 480
    assert board.open_minutes_so_far(T0, T0 - timedelta(hours=1), 480) == 0


# ── BRAYN keeps the room, the appointment and the booked minutes (migration 0173) ──

def test_brayn_stores_the_ris_room_appointment_and_booked_minutes(rooms):
    study(rooms, 'A1', room='XR1', minutes=10, appt=30, scheduled=5)
    row = rooms.one("SELECT ris_room, appointment_at, booked_minutes FROM brayn_study_state WHERE accession_number = 'A1'")
    assert row['ris_room'] == 'XR1' and row['booked_minutes'] == 10
    assert abs((row['appointment_at'] - (datetime.now() + timedelta(minutes=30))).total_seconds()) < 90
    # rebooked on another room for another time: the latest message wins
    study(rooms, 'A1', room='CT64', minutes=30, appt=90, scheduled=1)
    row = rooms.one("SELECT ris_room, booked_minutes FROM brayn_study_state WHERE accession_number = 'A1'")
    assert (row['ris_room'], row['booked_minutes']) == ('CT64', 30)


def test_brayn_reads_the_ris_modality_from_obr_18(rooms):
    """Migration 0174: LAUMC's RIS sends the modality in OBR-18, where the parser never looked."""
    rooms.send(k.status('SC', 'M18', k.ts(5), ae=None, mod=None, room='XR1', obr18='rf'))
    assert rooms.scalar("SELECT modality FROM brayn_study_state WHERE accession_number = 'M18'") == 'RF'
    rooms.send(k.status('SC', 'M24', k.ts(5), ae=None, mod='CT', room='CT64', obr18='RF'))   # OBR-24 first
    assert rooms.scalar("SELECT modality FROM brayn_study_state WHERE accession_number = 'M24'") == 'CT'


def test_migration_0174_fills_the_modality_of_studies_already_stored(rooms):
    rooms.exec("""INSERT INTO brayn_study_state (accession_number, current_rank, event_count, is_closed, first_seen_at, updated_at)
                  VALUES ('1003000000009', 40, 1, FALSE, now(), now())""")
    raw = ('MSH|^~\\&|RIS||RAYD||20261119155000||ORM^O01|M9|P|2.3\r'
           'ORC|SC|0019400009|1003000000009||A|||||||||||XR1\r'
           'OBR|04587294|0019400009|1003000000009|C11-03^XR: SPINE|R|||||||||||x^Y^Z||RF|||||||||10|||||||||20261119161500\r')
    rooms.exec("""INSERT INTO hl7_message_archive (message_control_id, sending_app, raw_message, content_hash)
                  VALUES ('M9', 'RIS', :raw, 'h9')""", raw=raw)
    sql = open(os.path.join(os.path.dirname(__file__), '..', 'migrations', '0174_brayn_ris_modality.sql'),
               encoding='utf-8').read()
    rooms.exec(sql.split('-- The RIS studies already in BRAYN', 1)[1].split('\n', 1)[1])
    assert rooms.scalar("SELECT modality FROM brayn_study_state WHERE accession_number = '1003000000009'") == 'RF'


def test_the_migration_fills_the_studies_already_open_from_the_archive(rooms):
    rooms.exec("""INSERT INTO brayn_study_state (accession_number, current_rank, event_count, is_closed, first_seen_at, updated_at)
                  VALUES ('1003000000001', 40, 1, FALSE, now(), now())""")
    raw = ('MSH|^~\\&|RIS||RAYD||20261119155000||ORM^O01|M1|P|2.3\r'
           'ORC|SC|0019400001|1003000000001||A|||||||||||XR1\r'
           'OBR|04587294|0019400001|1003000000001|C11-03^XR: SPINE|R|||||||||||x^Y^Z||RF|||||||||10|||||||||20261119161500\r')
    rooms.exec("""INSERT INTO hl7_message_archive (message_control_id, sending_app, raw_message, content_hash)
                  VALUES ('M1', 'RIS', :raw, 'h1')""", raw=raw)
    sql = open(os.path.join(os.path.dirname(__file__), '..', 'migrations', '0173_brayn_room_booking.sql'),
               encoding='utf-8').read()
    rooms.exec(sql.split('-- The studies already open', 1)[1].split('\n', 1)[1])
    row = rooms.one("SELECT ris_room, appointment_at, booked_minutes FROM brayn_study_state "
                    "WHERE accession_number = '1003000000001'")
    assert (row['ris_room'], row['appointment_at'], row['booked_minutes']) == \
           ('XR1', datetime(2026, 11, 19, 16, 15), 10)


# ── what a room shows ─────────────────────────────────────────────────────────

def test_one_card_per_ris_room_named_by_its_room_and_one_hospital(rooms):
    study(rooms, 'S1', room='SJH-US2', proc='US1', desc='US BREAST', scheduled=5)
    b = get_board(rooms.app)
    assert {'CT64', 'CT128 ED', 'XR1'} <= keys(b)
    assert card(b, 'CT64')['room'] == 'CT Room - 750 HD (GE)'        # the room name, not the AE title
    assert 'SJH-US2' not in keys(b)                                  # the other hospital
    assert 'US CARDIO 2' not in keys(b)                              # cardiology stays out
    assert 'PORT2' not in keys(b)                                    # closed today, nothing booked
    assert 'RH-PACS' not in keys(b)                                  # open on paper, never booked on
    assert card(get_board(rooms.app, site='SJH'), 'SJH-US2')['scheduled_today'] == 1
    assert b['unknown_rooms'] == []                                  # SJH's room is not "unknown"


def test_a_room_closed_on_the_ris_shows_once_something_is_booked_on_it(rooms):
    study(rooms, 'P1', room='PORT2', proc='XRCH', desc='CHEST', scheduled=5)
    assert card(get_board(rooms.app), 'PORT2')['scheduled_today'] == 1


def test_scheduled_today_and_still_to_come(rooms):
    study(rooms, 'A1', appt=30, scheduled=60)
    study(rooms, 'A2', appt=60, scheduled=60, arrived=5)
    study(rooms, 'A3', appt=60 * 24 * 3, scheduled=60)              # in three days
    study(rooms, 'A4', appt=45, scheduled=60, cancelled=10)
    c = card(get_board(rooms.app), 'CT64')
    if (datetime.now() + timedelta(minutes=60)).date() != datetime.now().date():
        pytest.skip('the appointments would fall tomorrow')
    assert (c['scheduled_today'], c['to_come']) == (2, 1)
    assert [x['accession'] for x in c['coming']] == ['A1']
    assert [x['accession'] for x in c['waiting']] == ['A2']


def test_a_wait_turns_red_past_its_modalitys_limit(rooms):
    with rooms.app.app_context():
        board.save_limits({'CT': 20}, 30, 50)
    study(rooms, 'A1', scheduled=60, arrived=25)
    study(rooms, 'A2', scheduled=60, arrived=10, proc='CTHEAD', desc='CT HEAD')
    study(rooms, 'X1', room='XR1', proc='XRCH', desc='CHEST', scheduled=60, arrived=25)
    b = get_board(rooms.app)
    ct = card(b, 'CT64')
    assert [(w['accession'], w['over']) for w in ct['waiting']] == [('A1', True), ('A2', False)]
    assert (ct['waiting_over'], ct['wait_limit_min']) == (1, 20)
    xr = card(b, 'XR1')
    assert (xr['wait_limit_min'], xr['waiting'][0]['over']) == (30, False)   # DX: the default
    assert b['totals']['waiting_over'] == 1 and b['limits']['wait_min']['CT'] == 20


def test_the_running_exam_ends_at_its_booked_minutes_and_turns_late(rooms):
    study(rooms, 'A1', minutes=30, scheduled=60, arrived=40, started=10)
    study(rooms, 'X1', room='XR1', proc='XRCH', desc='CHEST', minutes=5, scheduled=60, arrived=30, started=12)
    b = get_board(rooms.app)
    ct = card(b, 'CT64')
    c = ct['current']
    assert (ct['status'], c['accession'], c['expected_minutes'], c['expected_source'], c['late']) == \
           ('busy', 'A1', 30, 'booked', False)
    assert datetime.fromisoformat(c['expected_end']) == datetime.fromisoformat(c['started_at']) + timedelta(minutes=30)
    xr = card(b, 'XR1')
    assert (xr['status'], xr['current']['late']) == ('late', True)
    assert b['totals']['late'] == 1


def test_without_booked_minutes_the_procedure_then_the_modality_then_the_default(rooms):
    study(rooms, 'A1', proc='CTHEAD', desc='CT HEAD', scheduled=30, started=5)
    study(rooms, 'A2', room='CT128 ED', proc='CTXXX', desc='CT OTHER', scheduled=30, started=5)
    study(rooms, 'X1', room='XR1', proc='XRCH', desc='CHEST', scheduled=30, started=5)
    b = get_board(rooms.app)
    assert (card(b, 'CT64')['current']['expected_minutes'], card(b, 'CT64')['current']['expected_source']) == (15, 'procedure')
    assert (card(b, 'CT128 ED')['current']['expected_minutes'], card(b, 'CT128 ED')['current']['expected_source']) == (22, 'modality')
    assert card(b, 'XR1')['current']['expected_source'] == 'default'


def test_an_exam_done_too_early_is_yellow_until_the_next_one_starts(rooms):
    study(rooms, 'A1', minutes=30, scheduled=60, arrived=40, started=20, completed=17)   # 3 of 30 min
    e = card(get_board(rooms.app), 'CT64')['early']
    assert e['accession'] == 'A1' and 2 <= e['took_min'] <= 4 and e['expected_minutes'] == 30
    assert get_board(rooms.app)['totals']['early'] == 1
    study(rooms, 'A2', minutes=30, scheduled=60, arrived=30, started=5)
    c = card(get_board(rooms.app), 'CT64')
    assert c['early'] is None and c['current']['accession'] == 'A2'


def test_an_exam_done_in_its_time_is_not_early(rooms):
    study(rooms, 'A1', minutes=30, scheduled=90, arrived=70, started=60, completed=35)   # 25 of 30
    assert card(get_board(rooms.app), 'CT64')['early'] is None


def test_the_early_share_is_a_setting(rooms):
    with rooms.app.app_context():
        board.save_limits({}, 30, 90)
    study(rooms, 'A1', minutes=30, scheduled=90, arrived=70, started=60, completed=35)   # 25 of 30 < 90 %
    assert card(get_board(rooms.app), 'CT64')['early'] is not None


def test_done_today_and_utilization(rooms):
    now = datetime.now()
    if now - timedelta(minutes=110) < datetime.combine(now.date(), datetime.min.time()):
        pytest.skip('the exams would start before midnight')
    rooms.exec("INSERT INTO settings (key, value) VALUES ('shift_morning_start', '0') "
               "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    study(rooms, 'A1', scheduled=120, arrived=110, started=100, completed=70)   # 30 min
    study(rooms, 'A2', scheduled=70, arrived=65, started=60, completed=40)      # 20 min
    study(rooms, 'A3', scheduled=70, arrived=60, started=55, completed=45)      # inside A2
    b = get_board(rooms.app)
    c = card(b, 'CT64')
    assert c['done_today'] == 3 and c['status'] == 'free'
    assert abs(c['busy_min'] - 50) <= 1
    assert c['utilization_pct'] == round(100 * c['busy_min'] / c['open_min'])


def test_a_room_code_rayd_does_not_know_is_listed(rooms):
    study(rooms, 'Z1', room='NEWROOM9', scheduled=5)
    assert get_board(rooms.app)['unknown_rooms'] == [{'code': 'NEWROOM9', 'studies': 1}]


def test_patient_name_comes_from_the_patient_record(rooms):
    study(rooms, 'A1', scheduled=20, arrived=10, pat='P0042')
    rooms.send(k.adt('A08', pat='P0042', name='HADDAD^RIMA'))
    w = card(get_board(rooms.app), 'CT64')['waiting'][0]
    assert (w['patient'], w['patient_id']) == ('RIMA HADDAD', 'P0042')


def test_limits_are_validated(app):
    with app.app_context():
        with pytest.raises(ValueError):
            board.save_limits({'CT': 0}, 30, 50)
        with pytest.raises(ValueError):
            board.save_limits({}, 30, 100)
        from db import db
        db.session.rollback()
        assert board.load_limits() == {'wait_min': {}, 'default_wait_min': 30, 'early_pct': 50}


# ── LAUMC ─────────────────────────────────────────────────────────────────────

def test_now_is_hospital_time_on_a_utc_server(app):
    """LAUMC's Postgres runs on UTC; the ladder is hospital time, and so is the board."""
    from db import db
    from utils.brayn_shadow import hospital_zone
    with app.app_context():
        db.session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        b = board.build_board()
        hospital = db.session.execute(text("SELECT (now() AT TIME ZONE :z)::timestamp(0)"),
                                      {'z': hospital_zone()}).scalar()
        db.session.rollback()
    assert abs((datetime.fromisoformat(b['now']) - hospital).total_seconds()) <= 5


def test_the_day_starts_with_the_morning_shift(app, pipe):
    get = lambda: get_board(app)['day_start'][11:16]
    assert get() == '07:00'
    pipe.exec("INSERT INTO settings (key, value) VALUES ('shift_morning_start', '6') "
              "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    assert get() == '06:00'


def test_while_brayn_is_off_the_board_shows_the_rooms_and_no_exam(rooms):
    rooms.brayn(False)
    study(rooms, 'A1', scheduled=20, arrived=10)
    b = get_board(rooms.app)
    assert 'CT64' in keys(b)
    assert (b['totals']['waiting'], b['totals']['scheduled_today'], b['totals']['done_today']) == (0, 0, 0)


# ── page and API ──────────────────────────────────────────────────────────────

def h(client, url, role='administrator', uid=ADMIN):
    return client.get(url, headers={'X-Test-Role': role, 'X-Test-Uid': str(uid)})


def test_api_returns_the_chosen_hospital(client, rooms):
    study(rooms, 'A1', scheduled=20, arrived=10)
    study(rooms, 'S1', room='SJH-US2', proc='US1', desc='US BREAST', scheduled=20, arrived=5)
    get = lambda url: h(client, url, role='superuser').get_json()      # a Super user holds both
    d = get('/viewer/department/api/board?site=RH')
    assert {'now', 'lanes', 'totals', 'limits', 'sites', 'site'} <= set(d)
    assert d['site'] == 'RH' and d['totals']['waiting'] == 1 and d['can_edit_limits'] is True
    assert [s['code'] for s in d['sites']] == ['RH', 'SJH']
    d = get('/viewer/department/api/board?site=SJH')
    assert d['site'] == 'SJH' and d['totals']['waiting'] == 1
    assert get('/viewer/department/api/board')['site'] == 'RH'           # the header's, else the first


def test_an_account_sees_only_its_hospitals(app, client, rooms):
    from db import db
    with app.app_context():
        for t in ('user_page_permissions', 'user_sites'):
            db.session.execute(text(f"DELETE FROM {t} WHERE user_id = 9102"))
        db.session.execute(text("DELETE FROM users WHERE id = 9102"))
        db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (9102, 'db-9102', 'x', 'user')"))
        db.session.execute(text("INSERT INTO user_sites (user_id, site_id) VALUES (9102, rayd_default_site_id())"))
        db.session.execute(text("INSERT INTO user_page_permissions (user_id, page_key, is_enabled) VALUES (9102, 'live_feed', TRUE)"))
        db.session.commit()
    d = h(client, '/viewer/department/api/board?site=SJH', role='user', uid=9102).get_json()
    assert d['site'] == 'RH' and [s['code'] for s in d['sites']] == ['RH'] and d['can_edit_limits'] is False


def test_the_limits_are_saved_by_settings_roles_only(client, app):
    body = {'wait_min': {'CT': 25, 'MR': 40}, 'default_wait_min': 35, 'early_pct': 40}
    r = client.post('/viewer/department/api/limits', json=body,
                    headers={'X-Test-Role': 'administrator', 'X-Test-Uid': str(ADMIN)})
    assert r.status_code == 200 and r.get_json()['limits']['wait_min'] == {'CT': 25, 'MR': 40}
    with app.app_context():
        from db import db
        saved = json.loads(db.session.execute(text("SELECT value FROM settings WHERE key = :k"),
                                              {'k': board.LIMITS_KEY}).scalar())
    assert saved['early_pct'] == 40
    r = client.post('/viewer/department/api/limits', json={'wait_min': {'CT': 0}},
                    headers={'X-Test-Role': 'administrator', 'X-Test-Uid': str(ADMIN)})
    assert r.status_code == 400 and 'between 1 and 600' in r.get_json()['error']


@pytest.mark.parametrize('role,live,code', [('administrator', None, 200), ('cht', None, 200),
                                            ('user', True, 200), ('user', False, 403),
                                            ('user', None, 403)])
def test_access_follows_the_live_feed_permission(app, client, role, live, code):
    """Administrator and CHT open every page; a User the live page only when it is ticked
    for it on RH (item 13f: no role default any more, migration 0152 wrote them out)."""
    from db import db
    with app.app_context():
        db.session.execute(text("DELETE FROM user_page_permissions WHERE user_id = 9101"))
        db.session.execute(text("DELETE FROM user_sites WHERE user_id = 9101"))
        db.session.execute(text("DELETE FROM users WHERE id = 9101"))
        db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (9101, 'db-9101', 'x', :r)"),
                           {'r': role})
        db.session.execute(text("INSERT INTO user_sites (user_id, site_id) VALUES (9101, rayd_default_site_id())"))
        if live is not None:
            db.session.execute(text("""INSERT INTO user_page_permissions (user_id, page_key, is_enabled)
                                       VALUES (9101, 'live_feed', :on)"""), {'on': live})
        db.session.commit()
    assert h(client, '/viewer/department/api/board', role=role, uid=9101).status_code == code


def test_a_user_cannot_save_the_limits(client, app):
    from db import db
    with app.app_context():
        db.session.execute(text("DELETE FROM user_page_permissions WHERE user_id = 9103"))
        db.session.execute(text("DELETE FROM user_sites WHERE user_id = 9103"))
        db.session.execute(text("DELETE FROM users WHERE id = 9103"))
        db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (9103, 'db-9103', 'x', 'user')"))
        db.session.execute(text("INSERT INTO user_sites (user_id, site_id) VALUES (9103, rayd_default_site_id())"))
        db.session.execute(text("INSERT INTO user_page_permissions (user_id, page_key, is_enabled) VALUES (9103, 'live_feed', TRUE)"))
        db.session.commit()
    r = client.post('/viewer/department/api/limits', json={'wait_min': {'CT': 10}},
                    headers={'X-Test-Role': 'user', 'X-Test-Uid': '9103'})
    assert r.status_code == 403


def test_anonymous_is_refused(client):
    assert client.get('/viewer/department/api/board').status_code in (401, 302)


def test_the_old_live_page_opens_the_board(client):
    r = h(client, '/viewer/live')
    assert r.status_code == 302 and r.headers['Location'].endswith('/viewer/department')


def test_page_renders_and_its_script_is_valid(client):
    r = h(client, '/viewer/department')
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and 'id="board"' in html and 'Live Department' in html
    assert 'id="limits-btn"' in html and 'data-explain="live-board"' in html
    assert 'id="full-btn"' in html and 'overflow: hidden' in html          # a wall screen: no scrolling
    if not shutil.which('node'):
        pytest.skip('node not installed')
    script = re.search(r'<script>\n(.*?)</script>', html, re.S).group(1)
    path = os.path.join(os.path.dirname(__file__), '_department_board_page.js')
    try:
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(script)
        done = subprocess.run(['node', '--check', path], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    finally:
        os.remove(path)
