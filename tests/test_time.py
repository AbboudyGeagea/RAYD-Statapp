"""
One clock. HL7 timestamps are naive local hospital time, the app container runs on
TZ (Asia/Beirut by default), and the database must agree with both — otherwise
every NOW() comparison in the sweep, every received_at, every CURRENT_DATE in a
report is off by the UTC offset (two hours in winter, three in summer).
"""
from datetime import datetime, timedelta

import hl7kit as k


def test_database_session_runs_on_the_hospital_time_zone(pipe):
    import os
    want = os.environ.get('TZ', 'Asia/Beirut')
    assert pipe.scalar("SHOW timezone") == want


def test_database_now_matches_the_app_clock(pipe):
    db_now = pipe.scalar("SELECT LOCALTIMESTAMP")
    assert abs((db_now - datetime.now()).total_seconds()) < 120


def test_received_at_is_local_time(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(5)))
    got = pipe.archive()[0]['received_at']
    assert abs((got - datetime.now()).total_seconds()) < 120


def test_current_date_is_the_hospital_date(pipe):
    assert pipe.scalar("SELECT CURRENT_DATE") == datetime.now().date()


def test_offset_timestamps_are_stored_as_written(pipe):
    stamp = (k.now() - timedelta(minutes=60)).strftime('%Y%m%d%H%M%S')
    pipe.send(k.status('SC', 'A1', stamp + '+0300'))
    assert pipe.state('A1')['scheduled_at'] == datetime.strptime(stamp, '%Y%m%d%H%M%S')


def test_future_event_is_judged_on_the_hospital_clock(pipe):
    pipe.send(k.status('SC', 'A1', k.hl7(k.now() + timedelta(minutes=30))))
    assert 'FUTURE_EVENT' in pipe.codes()
