"""
tests/test_report_37_audit_open.py — Report 37's first open from the PACS audit trail (to-do
item 2, 2026-11-17). Where the nightly audit copy covers a study, its first open is the first
time a resident or radiologist displayed the images or locked it for reading, exact to the
second; studies from before the copy keep the nightly-snapshot method.
"""
from datetime import datetime, timedelta

import pytest

from test_sticky_notes import (ADMIN, PERIOD, T0, history, load, m, note, sql, study)  # noqa: F401

UNTIL = datetime.now().replace(microsecond=0)


@pytest.fixture
def audit(sql):  # noqa: F811
    sql("DELETE FROM etl_pacs_audit_events")
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")
    sql("""INSERT INTO pacs_user_role_override (login_id, role)
           VALUES ('RES.ONE', 'residents'), ('RAD.TWO', 'radiologists') ON CONFLICT DO NOTHING""")
    yield sql
    sql("DELETE FROM etl_pacs_audit_events")
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")


def covered_from(sql, day):
    sql("""INSERT INTO settings (key, value) VALUES ('pacs_audit_copied_from', :f),
                                                    ('pacs_audit_copied_until', :u)""",
        f=day.isoformat(), u=UNTIL.strftime('%Y-%m-%d %H:%M:%S'))


def case_study(sql, uid):
    study(sql, uid)
    sql("UPDATE etl_didb_studies SET study_instance_uid = :x WHERE study_db_uid = :u", x=f'1.2.840.{uid}', u=uid)


_next_id = iter(range(1, 10_000))


def opened(sql, uid, minutes, login, kind='open', source='Vue PACS Client'):
    sql("""INSERT INTO etl_pacs_audit_events (log_dbid, event_at, kind, user_name, source, study_instance_uid)
           VALUES (:i, :t, :k, :l, :s, :x)""",
        i=next(_next_id), t=m(minutes), k=kind, l=login, s=source, x=f'1.2.840.{uid}')


def when(minutes):
    return m(minutes).strftime('%Y-%m-%d %H:%M')


def test_the_first_open_is_the_first_reader_in_the_audit(client, audit):
    covered_from(audit, T0.date() - timedelta(days=1))
    case_study(audit, 1)
    note(audit, 1, 30, author='res.one@ad')
    opened(audit, 1, 2, 'his_vue_motion@ad', source='Carestream Vue Motion')   # the HIS, not a reader
    opened(audit, 1, 5, 'tech.one@ad')                                          # a technologist
    opened(audit, 1, 8, 'res.one@ad', kind='lock')
    opened(audit, 1, 8, 'res.one@ad')
    opened(audit, 1, 60, 'rad.two@ad')
    history(audit, 1, 600, 40, 'rad.two@ad')        # the nightly snapshot would say 40 min

    data = load(client)
    case = data['cases'][0]

    assert case['open_at'] == when(8)
    assert case['opened_by_login'] == 'res.one@ad' and case['opened_by'] == 'Res One'
    assert case['minutes_open_to_note'] == 22
    assert case['open_from_audit'] is True and case['open_blank_reason'] is None
    assert data['kpi']['open_seen'] == 1 and data['kpi']['open_from_audit'] == 1


def test_an_open_in_vue_motion_by_a_reader_counts(client, audit):
    covered_from(audit, T0.date())
    case_study(audit, 1)
    opened(audit, 1, 12, 'rad.two@ad', source='Carestream Vue Motion')

    assert load(client)['cases'][0]['open_at'] == when(12)


def test_an_audit_open_after_the_note_is_still_the_first_open(client, audit):
    """The audit holds every open, so nothing earlier was overwritten."""
    covered_from(audit, T0.date())
    case_study(audit, 1)
    note(audit, 1, 30)
    opened(audit, 1, 45, 'rad.two@ad')

    case = load(client)['cases'][0]

    assert case['open_at'] == when(45) and case['open_overwritten'] is False


def test_a_covered_study_no_reader_opened_says_so(client, audit):
    covered_from(audit, T0.date())
    case_study(audit, 1)
    opened(audit, 1, 5, 'tech.one@ad')

    case = load(client)['cases'][0]

    assert case['open_at'] is None
    assert case['open_blank_reason'] == 'Not opened by a resident or radiologist by the last nightly copy'


def test_studies_from_before_the_copy_keep_the_nightly_snapshot(client, audit):
    covered_from(audit, T0.date() + timedelta(days=1))
    case_study(audit, 1)
    opened(audit, 1, 8, 'res.one@ad')
    history(audit, 1, 600, 10, 'rad.two@ad')

    case = load(client)['cases'][0]

    assert case['open_at'] == when(10) and case['opened_by_login'] == 'rad.two@ad'
    assert case['open_from_audit'] is False


def test_the_exports_say_where_first_open_comes_from(client, audit):
    case_study(audit, 1)
    before = client.get('/report/37/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)
    covered_from(audit, T0.date())
    after = client.get('/report/37/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)

    assert 'PACS audit trail' not in before and 'earliest open a nightly copy caught' in before
    assert 'PACS audit trail' in after and f'from {T0.date().isoformat()}' in after
