"""
The Live Department board: what each device shows, built from RIS messages sent
through the real listener (hl7kit), and the page / API around it.

Times are minutes before now, as the RIS would stamp its events. The day start is
moved to midnight where utilization is checked, so the figures do not depend on the
hour the suite runs at.
"""
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta

import pytest

import hl7kit as k
from utils import department_board as board

SU = 4242


@pytest.fixture
def devices(pipe):
    """Two CT rooms and an MR, durations for their procedures, open 8 hours."""
    pipe.exec("""UPDATE aetitle_modality_map SET room_name = 'CT Room 1', display_aetitle = 'CT 64',
                        daily_capacity_minutes = 480 WHERE aetitle = 'CT64_RH'""")
    pipe.exec("""INSERT INTO aetitle_modality_map (aetitle, modality, room_name, daily_capacity_minutes)
                 VALUES ('CT2_RH', 'CT', 'CT Room 2', 480)""")
    pipe.exec("""INSERT INTO procedure_duration_map (procedure_code, duration_minutes, modality) VALUES
                 ('CTABD', 30, 'CT'), ('CTHEAD', 15, 'CT'), ('MRBRAIN', 40, 'MR')""")
    return pipe


def study(pipe, acc, ae='CT64_RH', mod='CT', proc='CTABD', desc='CT ABDOMEN AND PELVIS',
          pat='P0001', **steps):
    """RIS events for one accession; steps = scheduled / arrived / started / completed /
    cancelled, each in minutes ago."""
    for code, step in (('SC', 'scheduled'), ('AR', 'arrived'), ('IP', 'started'),
                       ('CM', 'completed'), ('CA', 'cancelled')):
        if step in steps:
            pipe.send(k.status(code, acc, k.ts(steps[step]), pat=pat, ae=ae, mod=mod,
                               proc=proc, desc=desc))


def get_board(app, **kw):
    with app.app_context():
        return board.build_board(**kw)


def device(b, ae):
    return next(d for lane in b['lanes'] for d in lane['devices'] if d['aetitle'] == ae)


def lane(b, mod):
    return next(l for l in b['lanes'] if l['modality'] == mod)


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
    assert board.window_start_for(day0, 1440, 7) == day0          # a 24-hour device
    assert board.open_minutes_so_far(T0, T0 + timedelta(hours=3), 480) == 180
    assert board.open_minutes_so_far(T0, T0 + timedelta(hours=10), 480) == 480
    assert board.open_minutes_so_far(T0, T0 - timedelta(hours=1), 480) == 0
    assert board.open_minutes_so_far(T0, T0 + timedelta(hours=3), 0) == 0


# ── what a device shows ───────────────────────────────────────────────────────

def test_busy_device_current_study_and_expected_finish(app, devices):
    study(devices, 'A1', scheduled=60, arrived=40, started=10)
    d = device(get_board(app), 'CT64_RH')
    assert (d['status'], d['name'], d['room']) == ('busy', 'CT 64', 'CT Room 1')
    c = d['current']
    assert (c['accession'], c['expected_minutes'], c['expected_source']) == ('A1', 30, 'procedure')
    started = datetime.fromisoformat(c['started_at'])
    assert datetime.fromisoformat(c['expected_end']) == started + timedelta(minutes=30)
    assert 19 <= c['remaining_min'] <= 21
    assert d['free_at'] == c['expected_end']


def test_overrun(app, devices):
    study(devices, 'A1', scheduled=90, arrived=60, started=45)
    d = device(get_board(app), 'CT64_RH')
    assert d['status'] == 'overrun' and -16 <= d['current']['remaining_min'] <= -14


def test_newest_start_is_current_older_open_ones_are_counted(app, devices):
    study(devices, 'A1', scheduled=90, arrived=80, started=70)     # never completed
    study(devices, 'A2', scheduled=30, arrived=20, started=5)
    d = device(get_board(app), 'CT64_RH')
    assert d['current']['accession'] == 'A2' and d['also_in_progress'] == 1


def test_waiting_longest_first_with_levels(app, devices):
    study(devices, 'A2', scheduled=40, arrived=25)
    study(devices, 'A3', scheduled=90, arrived=70, proc='CTHEAD', desc='CT HEAD')
    study(devices, 'A4', scheduled=50, arrived=35)
    d = device(get_board(app), 'CT64_RH')
    assert [w['accession'] for w in d['waiting']] == ['A3', 'A4', 'A2']
    assert [w['level'] for w in d['waiting']] == ['alert', 'warn', 'ok']
    assert 69 <= d['waiting'][0]['wait_min'] <= 71
    assert d['status'] == 'free'
    # free now, then the three studies one after the other: 15 + 30 + 30 minutes
    clear = datetime.fromisoformat(d['queue_clear_at'])
    now = datetime.fromisoformat(get_board(app)['now'])
    assert abs((clear - now).total_seconds() / 60 - 75) <= 1
    s = lane(get_board(app), 'CT')['summary']
    assert s['waiting'] == 3 and 69 <= s['longest_wait_min'] <= 71


def test_queue_clears_after_the_current_study(app, devices):
    study(devices, 'A1', scheduled=60, arrived=40, started=10)       # ends in ~20 min
    study(devices, 'A2', scheduled=30, arrived=15)                   # then 30 min
    d = device(get_board(app), 'CT64_RH')
    end = datetime.fromisoformat(d['current']['expected_end'])
    assert datetime.fromisoformat(d['queue_clear_at']) == end + timedelta(minutes=30)


def test_incoming_orders_on_the_device_and_unassigned(app, devices):
    study(devices, 'A5', scheduled=5)
    devices.send(k.order('PL9', when=k.ts(3), mod='CT', code='CTHEAD', desc='CT HEAD'))
    b = get_board(app)
    d = device(b, 'CT64_RH')
    assert [(o['accession'], o['state']) for o in d['incoming']] == [('A5', 'Scheduled')]
    un = lane(b, 'CT')['unassigned']
    assert [(o['order'], o['state'], o['procedure']) for o in un['incoming']] == \
           [('PL9', 'Ordered', 'CT HEAD')]
    # The RIS schedules the order on a device: it moves there and is listed once.
    devices.send(k.status('SC', 'A9', k.ts(1), placer='PL9', ae='CT2_RH', proc='CTHEAD',
                          desc='CT HEAD'))
    b = get_board(app)
    assert [o['accession'] for o in device(b, 'CT2_RH')['incoming']] == ['A9']
    assert lane(b, 'CT')['unassigned'] is None
    assert b['totals']['incoming'] == 2


def test_cancelled_and_old_studies_leave_the_board(app, devices):
    study(devices, 'A6', scheduled=30, cancelled=10)
    study(devices, 'A7', scheduled=60 * 30)                  # scheduled 30 hours ago
    d = device(get_board(app), 'CT64_RH')
    assert d['incoming'] == [] and d['waiting'] == []


def test_done_today_and_utilization(app, devices):
    now = datetime.now()
    if now - timedelta(minutes=110) < datetime.combine(now.date(), datetime.min.time()):
        pytest.skip('the studies would start before midnight')
    devices.exec("UPDATE worklist_shifts SET start_hour = 0")
    study(devices, 'A1', scheduled=120, arrived=110, started=100, completed=70)   # 30 min
    study(devices, 'A2', scheduled=70, arrived=65, started=60, completed=40)      # 20 min
    study(devices, 'A3', scheduled=70, arrived=60, started=55, completed=45)      # inside A2
    b = get_board(app)
    d = device(b, 'CT64_RH')
    assert d['done_today'] == 3 and d['status'] == 'free'
    assert abs(d['busy_min'] - 50) <= 1
    minutes_today = (datetime.fromisoformat(b['now'])
                     - datetime.combine(now.date(), datetime.min.time())).total_seconds() / 60
    assert abs(d['open_min'] - min(minutes_today, 480)) <= 1
    assert d['utilization_pct'] == round(100 * d['busy_min'] / d['open_min'])
    assert lane(b, 'CT')['summary']['done_today'] == 3


def test_an_exception_closes_a_device(app, devices):
    devices.exec("""INSERT INTO device_exceptions (aetitle, exception_date, actual_opening_minutes, reason)
                    VALUES ('CT2_RH', CURRENT_DATE, 0, 'Service visit')""")
    d = device(get_board(app), 'CT2_RH')
    assert (d['status'], d['exception'], d['open_min'], d['utilization_pct']) == \
           ('closed', 'Service visit', 0, None)


def test_duration_falls_back_to_the_modality_then_the_default(app, devices):
    study(devices, 'A1', scheduled=20, arrived=15, started=5, proc='CTXXX', desc='CT OTHER')
    study(devices, 'A2', scheduled=20, arrived=15, started=5, ae='DR_RH', mod='DX',
          proc='DXCHEST', desc='CHEST PA')
    b = get_board(app)
    ct, dx = device(b, 'CT64_RH')['current'], device(b, 'DR_RH')['current']
    assert (ct['expected_minutes'], ct['expected_source']) == (22, 'modality')   # median(30, 15)
    assert (dx['expected_minutes'], dx['expected_source']) == (board.DEFAULT_MINUTES, 'default')


def test_an_unmapped_device_gets_a_flagged_card(app, devices):
    study(devices, 'A1', scheduled=20, arrived=10, ae='MYSTERY_CT')
    d = device(get_board(app), 'MYSTERY_CT')
    assert d['mapped'] is False and [w['accession'] for w in d['waiting']] == ['A1']


def test_lanes_follow_the_department_order(app, devices):
    names = [l['modality'] for l in get_board(app)['lanes']]
    assert names.index('CT') < names.index('MR') < names.index('DX')


def test_patient_name_comes_from_the_patient_record(app, devices):
    study(devices, 'A1', scheduled=20, arrived=10, pat='P0042')
    devices.send(k.adt('A08', pat='P0042', name='HADDAD^RIMA'))
    w = device(get_board(app), 'CT64_RH')['waiting'][0]
    assert (w['patient'], w['patient_id']) == ('RIMA HADDAD', 'P0042')


# ── page and API ──────────────────────────────────────────────────────────────

def h(client, url, role='su', uid=SU):
    return client.get(url, headers={'X-Test-Role': role, 'X-Test-Uid': str(uid)})


def test_api_returns_the_board(client, devices):
    study(devices, 'A1', scheduled=20, arrived=10)
    r = h(client, '/viewer/department/api/board')
    assert r.status_code == 200
    d = r.get_json()
    assert {'now', 'lanes', 'totals', 'thresholds', 'sites'} <= set(d)
    assert d['totals']['waiting'] == 1


@pytest.mark.parametrize('role,code', [('su', 200), ('administrator', 200),
                                       ('implementation', 200), ('user', 403)])
def test_access_follows_the_live_feed_permission(client, role, code):
    assert h(client, '/viewer/department/api/board', role=role, uid=9101).status_code == code


def test_anonymous_is_refused(client):
    assert client.get('/viewer/department/api/board').status_code in (401, 302)


def test_page_renders_and_its_script_is_valid(client):
    r = h(client, '/viewer/department')
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and 'id="board"' in html and 'Live Department' in html
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
