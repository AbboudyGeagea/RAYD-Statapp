"""
tests/test_report_start_date.py
────────────────────────────────────────────────────────────────
Item 11 of the meeting of 2026-11-13: one "report start date", 2026-01-01, editable on
the ETL Control Room, used as the starting value of every report's date filter, the Super
Report and Report Composer included. Users can still pick earlier dates. Every report used
to start on the ETL go-live date (2019-01-01 on LAUMC).
"""
from datetime import date, timedelta

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

ADMIN = {'X-Test-Role': 'admin'}


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def set_go_live(day):
    sql("DELETE FROM go_live_config")
    sql("INSERT INTO go_live_config (go_live_date) VALUES (:d)", d=day)


def test_it_starts_on_the_first_of_january_2026():
    from db import get_report_start_date
    set_go_live('2019-01-01')
    assert get_report_start_date() == date(2026, 1, 1)


def test_without_a_setting_it_falls_back_to_the_go_live_date():
    from db import get_report_start_date
    set_go_live('2019-01-01')
    sql("DELETE FROM settings WHERE key = 'report_start_date'")
    assert get_report_start_date() == date(2019, 1, 1)


def test_a_report_opens_on_it(client):
    set_go_live('2019-01-01')
    html = client.get('/report/22', headers=ADMIN).get_data(as_text=True)
    assert 'name="start_date" class="input-dark mb-2" value="2026-01-01"' in html


def test_an_earlier_date_can_still_be_picked(client):
    set_go_live('2019-01-01')
    html = client.get('/report/22', headers=ADMIN,
                      query_string={'start_date': '2024-03-01', 'end_date': '2024-03-31'}
                      ).get_data(as_text=True)
    assert 'value="2024-03-01"' in html


def test_the_shared_sidebar_filter_starts_on_it():
    from utils.report_filters import sidebar_filters
    _params, _clause, start, _end = sidebar_filters(MultiDict())
    assert start == '2026-01-01'


def test_the_super_report_opens_on_it(client):
    html = client.get('/viewer/super-report-page', headers=ADMIN).get_data(as_text=True)
    assert 'const reportStart = "2026-01-01";' in html


def test_report_composer_gets_it_for_fixed_dates(client):
    catalog = client.get('/worklists/api/catalog', headers=ADMIN).get_json()
    assert catalog['report_start_date'] == '2026-01-01'


# ── the ETL Control Room ────────────────────────────────────────────────────────

def save(client, value, role='admin'):
    return client.post('/admin/report-start-date', json={'date': value},
                       headers={'X-Test-Role': role})


def test_an_admin_changes_it_and_every_report_follows(client):
    from db import get_report_start_date
    response = save(client, '2025-07-01')
    assert response.status_code == 200 and response.get_json()['date'] == '2025-07-01'
    assert get_report_start_date() == date(2025, 7, 1)
    html = client.get('/report/22', headers=ADMIN).get_data(as_text=True)
    assert 'value="2025-07-01"' in html


@pytest.mark.parametrize('value', ['', 'January', '2026-13-01'])
def test_a_value_that_is_not_a_date_is_refused(client, value):
    from db import get_report_start_date
    assert save(client, value).status_code == 400
    assert get_report_start_date() == date(2026, 1, 1)


def test_a_date_in_the_future_is_refused(client):
    assert save(client, (date.today() + timedelta(days=1)).isoformat()).status_code == 400


def test_only_an_admin_can_change_it(client):
    from db import get_report_start_date
    assert save(client, '2025-07-01', role='viewer').status_code == 403
    assert get_report_start_date() == date(2026, 1, 1)
