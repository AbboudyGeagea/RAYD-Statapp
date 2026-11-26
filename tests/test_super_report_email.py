"""
tests/test_super_report_email.py — the Super Report sent by email on a schedule (item 43,
utils/super_report_email.py, migration 0184). The relay is Report Composer's test relay: real
SMTP on a local socket, so the message and its workbook are read back as production sends them.
"""
import io
from datetime import date, timedelta

import openpyxl
import pytest
from sqlalchemy import text

from test_super_report import ADMIN, device, study
from test_worklist_email import Relay, setting
from utils import super_report_email as sre

URL = '/viewer/super-report/schedules'


@pytest.fixture
def relay(app):
    r = Relay()
    with app.app_context():
        for k, v in (('smtp_host', '127.0.0.1'), ('smtp_port', str(r.port)), ('smtp_security', 'none'),
                     ('smtp_from', 'rayd@laumc.test'), ('smtp_allowed_domains', 'laumc.test')):
            setting(k, v)
    yield r
    r.stop()


def body(**kw):
    return {'name': 'Weekly radiology', 'period': 'last_7_days', 'frequency': 'weekly', 'weekday': 0,
            'send_time': '07:00', 'recipients': 'manager@laumc.test', 'blocks': ['summary', 'volume', 'tat'],
            'filters': {'modality': ['CT']}, **kw}


def test_the_periods_are_worked_out_on_the_day_it_runs():
    thursday = date(2026, 11, 26)
    assert sre.period_dates('yesterday', thursday) == (date(2026, 11, 25), date(2026, 11, 25),
                                                      date(2026, 11, 24), date(2026, 11, 24))
    assert sre.period_dates('last_7_days', thursday) == (date(2026, 11, 19), date(2026, 11, 25),
                                                        date(2026, 11, 12), date(2026, 11, 18))
    assert sre.period_dates('last_week', thursday) == (date(2026, 11, 16), date(2026, 11, 22),
                                                      date(2026, 11, 9), date(2026, 11, 15))
    assert sre.period_dates('last_month', thursday) == (date(2026, 10, 1), date(2026, 10, 31),
                                                       date(2026, 9, 1), date(2026, 9, 30))
    assert sre.period_dates('last_week', date(2026, 11, 23))[0] == date(2026, 11, 16)   # on a Monday


def test_a_schedule_is_checked_saved_and_listed_for_the_settings_capability(client, relay):
    assert client.post(URL, headers={'X-Test-Role': 'user'}, json=body()).status_code == 403
    assert client.get(URL, headers={'X-Test-Role': 'user'}).status_code == 403
    bad = client.post(URL, headers=ADMIN, json=body(recipients='someone@gmail.com'))
    assert bad.status_code == 400 and 'outside the allowed domains' in bad.get_json()['error']
    assert client.post(URL, headers=ADMIN, json=body(blocks=['nope'])).status_code == 400
    assert client.post(URL, headers=ADMIN, json=body(period='forever')).status_code == 400

    r = client.post(URL, headers=ADMIN, json=body(filters={'modality': ['CT'], 'Bad Key!': ['x']}))
    assert r.status_code == 200, r.get_data(as_text=True)
    got = client.get(URL, headers=ADMIN).get_json()
    s = got['schedules'][0]
    assert got['mail_ready'] and got['allowed_domains'] == ['laumc.test']
    assert (s['name'], s['period_label'], s['blocks'], s['filters']) == (
        'Weekly radiology', 'The last 7 days', ['summary', 'volume', 'tat'], {'modality': ['CT']})
    assert s['next_run_at'] and s['last_run'] is None

    assert client.delete(f"{URL}/{s['id']}", headers=ADMIN).get_json()['success']
    assert client.get(URL, headers=ADMIN).get_json()['schedules'] == []


def test_send_now_mails_the_analysis_as_html_and_text_with_a_workbook(client, app, relay):
    today = date.today()
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        device('MRX1', 'MR', 'RH')
        for i in range(12):                                    # the last 7 days: 12 CT
            study(today - timedelta(days=2), 'CTX1', 'CT', tat_min=30)
        for i in range(8):                                     # the week before: 8 CT
            study(today - timedelta(days=9), 'CTX1', 'CT', tat_min=60)
        study(today - timedelta(days=2), 'MRX1', 'MR')         # left out by the CT filter
    sid = client.post(URL, headers=ADMIN, json=body()).get_json()['id']

    run = client.post(f'{URL}/{sid}/send', headers=ADMIN).get_json()

    assert run['status'] == 'sent', run
    mail = relay.mails[-1]
    assert mail.rcpts == ['manager@laumc.test']
    assert mail.msg['Subject'].startswith('Weekly radiology · LAU Medical Center - Rizk Hospital · ')
    html = mail.msg.get_body(('html',)).get_content()
    plain = mail.msg.get_body(('plain',)).get_content()
    assert '12 studies (+4)' in html and '12 studies (+4)' in plain          # 8 before: under 10
    assert 'Internal Use only' in html and 'modality: CT' in html
    assert 'REPORTING' in plain and 'REFERRING PHYSICIANS' not in plain       # only the blocks ticked
    att = next(p for p in mail.msg.iter_attachments())
    book = openpyxl.load_workbook(io.BytesIO(att.get_content()))
    assert book.sheetnames == ['Summary', 'Key figures', 'Volume by modality', 'Studies per day', 'Turnaround']
    assert [c.value for c in book['Volume by modality'][2]] == ['CT', 12, 8]

    runs = client.get(f'{URL}/{sid}/runs', headers=ADMIN).get_json()['runs']
    assert runs[0]['status'] == 'sent' and runs[0]['period_to'] == (today - timedelta(days=1)).isoformat()


def test_a_due_schedule_goes_out_once_and_nothing_goes_out_without_a_mail_server(client, app, relay):
    from db import db
    sid = client.post(URL, headers=ADMIN, json=body(frequency='daily')).get_json()['id']
    with app.app_context():
        db.session.execute(text("UPDATE super_report_email_schedules SET next_run_at = now() - interval '1 day'"))
        db.session.commit()
        assert sre.run_due() == 1 and sre.run_due() == 0               # claimed before sending
        assert len(relay.mails) == 1
        db.session.execute(text("UPDATE super_report_email_schedules SET next_run_at = now() - interval '1 day'"))
        db.session.commit()
        setting('smtp_host', '')
        assert sre.run_due() == 0 and len(relay.mails) == 1


def test_the_page_offers_the_schedules_to_administrators_only(client):
    admin = client.get('/viewer/super-report-page', headers=ADMIN).get_data(as_text=True)
    assert 'id="sr-email-dialog"' in admin and 'data-explain="sr-emails"' in admin
    import os
    js = open(os.path.join(os.path.dirname(__file__), '..', 'static', 'js', 'super_report.js'), encoding='utf-8').read()
    assert 'function srOpenEmails' in js and "fetch('/viewer/super-report/schedules')" in js
