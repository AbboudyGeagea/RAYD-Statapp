"""
tests/test_worklist_email.py — scheduled email of saved Report Composer reports.

Nothing is mocked between the schedule and the wire: the relay below speaks real SMTP on
a local socket, so smtplib and the MIME message are the ones production sends, and the
attachments are read back as real workbooks.

    when         daily / weekly / monthly slots; due, not due, paused, never twice
    what         the rows of the saved worklist, shown as the page shows them
    failures     one worklist failing, relay down, TLS never dropped, recipients refused
    patient data rows only in attachments, only to the allowed domains
    access       admin only; someone else's personal worklist stays theirs
    Microsoft    OAuth over real TLS, against stand-ins for Entra ID and Exchange Online
"""
import base64
import email
import email.policy
import http.server
import io
import json
import os
import re
import socketserver
import ssl
import threading
import urllib.parse
from datetime import datetime, time, timedelta

import openpyxl
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from sqlalchemy import text

from conftest import ROOT
from test_worklists import ADMIN, TEC, VIEWER, ae, h, save, sql, study, users
from utils import worklist_catalog as cat
from utils import worklist_email as mailer
from utils import worklist_engine as engine
from utils.crypto import decrypt, encrypt

OTHER_ADMIN = 9004
PAST = datetime(2020, 1, 1, 7, 0)
LIST = {'dataset': 'studies', 'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'}},
        'output': {'kind': 'list', 'columns': ['accession', 'patient_id', 'procedure']}}
BY_MODALITY = {'dataset': 'studies', 'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'}},
               'output': {'kind': 'table', 'rows': 'modality', 'measures': ['count']}}


@pytest.fixture(autouse=True)
def ctx(app):
    """Inside an app context; what a test made goes afterwards, schedules before the
    worklists they hold on to."""
    from db import db
    with app.app_context():
        yield
        db.session.rollback()
        for stmt in ("DELETE FROM worklist_email_runs",
                     "DELETE FROM worklist_email_schedules",
                     "DELETE FROM worklists WHERE owner_id IS NOT NULL OR folder <> 'Examples'",
                     f"DELETE FROM user_page_permissions WHERE user_id IN ({ADMIN}, {VIEWER}, {TEC})",
                     f"DELETE FROM users WHERE id IN ({ADMIN}, {VIEWER}, {TEC}, {OTHER_ADMIN})"):
            db.session.execute(text(stmt))
        db.session.commit()


class _Smtp(socketserver.StreamRequestHandler):
    def _say(self, line):
        self.wfile.write(line.encode() + b'\r\n')

    def handle(self):
        relay = self.server.relay
        self._say('220 test relay')
        sender, rcpts = None, []
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            line = raw.decode('utf-8', 'replace').strip()
            word = line.upper()
            if word.startswith(('EHLO', 'HELO')):
                self._say('250-test relay\r\n250 8BITMIME')     # no STARTTLS, no AUTH
            elif word.startswith('MAIL FROM'):
                sender = re.search(r'<(.*?)>', line).group(1)
                self._say('250 ok')
            elif word.startswith('RCPT TO'):
                addr = re.search(r'<(.*?)>', line).group(1)
                if addr in relay.refuse:
                    self._say('550 no such user')
                else:
                    rcpts.append(addr)
                    self._say('250 ok')
            elif word == 'DATA':
                self._say('354 go')
                chunks = []
                while True:
                    chunk = self.rfile.readline()
                    if chunk == b'.\r\n':
                        break
                    chunks.append(chunk[1:] if chunk.startswith(b'..') else chunk)
                msg = email.message_from_bytes(b''.join(chunks), policy=email.policy.default)
                relay.mails.append(type('Mail', (), {'sender': sender, 'rcpts': rcpts, 'msg': msg}))
                self._say('250 queued')
            elif word == 'QUIT':
                self._say('221 bye')
                return
            else:
                self._say('250 ok')


class Relay:
    def __init__(self):
        self.mails, self.refuse = [], set()
        self.server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), _Smtp)
        self.server.relay = self
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


def setting(key, value):
    sql("INSERT INTO settings (key, value) VALUES (:k, :v) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", k=key, v=value)


@pytest.fixture
def relay():
    r = Relay()
    for k, v in (('smtp_host', '127.0.0.1'), ('smtp_port', str(r.port)), ('smtp_security', 'none'),
                 ('smtp_from', 'rayd@laumc.test'), ('smtp_allowed_domains', 'laumc.test')):
        setting(k, v)
    yield r
    r.stop()


def worklist(client, name, definition, uid=ADMIN, visibility='shared'):
    return save(client, {'name': name, 'folder': 'Mail', 'visibility': visibility,
                         'definition': definition}, uid=uid).get_json()['id']


def schedule(client, ids, uid=ADMIN, **kw):
    body = {'name': 'Morning pack', 'recipients': 'a@laumc.test, b@laumc.test', 'frequency': 'daily',
            'send_time': '07:00', 'worklist_ids': ids, **kw}
    r = h(client, 'post', '/worklists/api/schedules/save', uid=uid, json=body)
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()['id']


def make_due(sid):
    sql("UPDATE worklist_email_schedules SET next_run_at = :at WHERE id = :id", at=PAST, id=sid)


def run_rows(sid=None):
    where = 'WHERE schedule_id = :id' if sid else ''
    return sql(f"SELECT * FROM worklist_email_runs {where} ORDER BY id", id=sid)


def attachments(mail):
    return {a.get_filename(): openpyxl.load_workbook(io.BytesIO(a.get_content())).active
            for a in mail.msg.iter_attachments()}


def body(mail):
    return mail.msg.get_body(('plain',)).get_content()


def cells(sheet):
    return [list(r) for r in sheet.iter_rows(values_only=True)]


# ── When ──────────────────────────────────────────────────────────────────────

def slot(frequency, at='07:00', weekday=None, month_day=None):
    return {'frequency': frequency, 'send_time': time.fromisoformat(at), 'weekday': weekday,
            'month_day': month_day}


@pytest.mark.parametrize('s, after, expected', [
    (slot('daily'), datetime(2026, 11, 4, 6, 59), datetime(2026, 11, 4, 7, 0)),
    (slot('daily'), datetime(2026, 11, 4, 7, 0), datetime(2026, 11, 5, 7, 0)),       # strictly after
    (slot('weekly', weekday=0), datetime(2026, 11, 4, 12, 0), datetime(2026, 11, 9, 7, 0)),
    (slot('weekly', weekday=2), datetime(2026, 11, 4, 6, 0), datetime(2026, 11, 4, 7, 0)),
    (slot('weekly', weekday=2), datetime(2026, 11, 4, 8, 0), datetime(2026, 11, 11, 7, 0)),
    (slot('monthly', month_day=1), datetime(2026, 11, 4, 12, 0), datetime(2026, 12, 1, 7, 0)),
    (slot('monthly', month_day=31), datetime(2026, 11, 4, 12, 0), datetime(2026, 11, 30, 7, 0)),
    (slot('monthly', month_day=31), datetime(2027, 2, 1, 0, 0), datetime(2027, 2, 28, 7, 0)),
    (slot('monthly', month_day=31), datetime(2026, 12, 31, 7, 0), datetime(2027, 1, 31, 7, 0)),
])
def test_next_run(s, after, expected):
    assert mailer.next_run(s, after) == expected


def test_a_due_schedule_is_sent_and_moves_on(client, relay):
    users()
    ae('CT_1', 'CT')
    study(1, accession_number='ACC-1')
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    before = datetime.now()

    assert mailer.run_due() == 1

    assert len(relay.mails) == 1
    mail = relay.mails[0]
    assert mail.sender == 'rayd@laumc.test' and sorted(mail.rcpts) == ['a@laumc.test', 'b@laumc.test']
    assert mail.msg['Subject'].startswith('Morning pack · ')
    assert mail.msg['Date'] and mail.msg['Message-ID']
    next_at = sql("SELECT next_run_at FROM worklist_email_schedules WHERE id = :id", id=sid)[0]['next_run_at']
    assert next_at > before and next_at.time() == time(7, 0)
    run = run_rows(sid)[0]
    assert run['status'] == 'sent' and run['finished_at'] and run['started_by'] is None
    assert run['recipients'] == ['a@laumc.test', 'b@laumc.test']
    assert run['detail'][0]['rows'] == 1 and run['detail'][0]['version'] == 1 and run['detail'][0]['attached']


def test_never_twice_for_one_slot(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    assert mailer.run_due() == 1
    assert mailer.run_due() == 0
    assert len(relay.mails) == 1


def test_a_second_scheduler_cannot_claim_it_again(client, relay, monkeypatch):
    """Two schedulers read it as due; the claim lets only one of them send."""
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    real = mailer.next_run

    def other_scheduler_gets_in_first(s, after):
        sql("UPDATE worklist_email_schedules SET next_run_at = :at WHERE id = :id",
            at=after + timedelta(days=1), id=sid)
        return real(s, after)
    monkeypatch.setattr(mailer, 'next_run', other_scheduler_gets_in_first)
    assert mailer.run_due() == 0
    assert relay.mails == []


def test_a_slot_missed_while_down_goes_once(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)     # years late
    assert mailer.run_due() == 1 and mailer.run_due() == 0
    assert sql("SELECT next_run_at FROM worklist_email_schedules WHERE id = :id",
               id=sid)[0]['next_run_at'] > datetime.now()


def test_not_due_and_paused_stay_quiet(client, relay):
    users()
    wid = worklist(client, 'Today', LIST)
    schedule(client, [wid])
    paused = schedule(client, [wid], name='Paused', enabled=False)
    make_due(paused)
    assert mailer.run_due() == 0
    assert relay.mails == [] and run_rows() == []


def test_resuming_a_paused_schedule_does_not_send_what_it_missed(client, relay):
    users()
    wid = worklist(client, 'Today', LIST)
    sid = schedule(client, [wid], enabled=False)
    make_due(sid)
    schedule(client, [wid], id=sid, enabled=True)
    assert mailer.run_due() == 0


def test_off_until_the_relay_is_set(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    setting('smtp_host', '')
    assert mailer.run_due() == 0
    assert relay.mails == [] and run_rows() == []
    assert h(client, 'post', f'/worklists/api/schedules/{sid}/send').status_code == 400


def test_quiet_in_demo_mode(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    setting('demo_mode', 'true')
    assert mailer.run_due() == 0
    assert relay.mails == []


def test_a_run_cut_off_by_a_restart_stops_saying_running(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    sql("""INSERT INTO worklist_email_runs (schedule_id, schedule_name, started_at, recipients)
           VALUES (:id, 'x', :at, ARRAY['a@laumc.test'])""", id=sid, at=datetime.now() - timedelta(hours=2))
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'Interrupted' in run['error']


def test_the_job_is_wired_into_the_scheduler():
    src = open(os.path.join(ROOT, 'app.py'), encoding='utf-8').read()
    assert "id='worklist_email'" in src and 'run_due' in src


# ── What ──────────────────────────────────────────────────────────────────────

def test_the_list_arrives_labelled_with_the_rows(client, relay):
    users()
    study(1, accession_number='ACC-1', study_description='CT HEAD')
    sid = schedule(client, [worklist(client, 'Today/ER', LIST)])
    make_due(sid)
    mailer.run_due()
    sheets = attachments(relay.mails[0])
    assert list(sheets) == ['Today_ER.xlsx']
    table = cells(sheets['Today_ER.xlsx'])
    labels = dict(cat.COLUMNS['studies'])
    assert table[0] == [labels['accession'], labels['patient_id'], labels['procedure']]
    assert table[1][0] == 'ACC-1' and table[1][2] == 'CT HEAD'


def test_a_table_arrives_as_the_page_shows_it(client, relay):
    users()
    ae('CT_1', 'CT'), ae('MR_1', 'MR')
    study(1), study(2), study(3, storing_ae='MR_1')
    sid = schedule(client, [worklist(client, 'Volumes', BY_MODALITY)])
    make_due(sid)
    mailer.run_due()
    assert cells(attachments(relay.mails[0])['Volumes.xlsx']) == [
        ['Modality', 'Studies'], ['CT', 2], ['MR', 1], ['Total', 3]]
    assert run_rows(sid)[0]['detail'][0]['rows'] == 2    # the Total is not a row


def test_a_split_table_spreads_across_columns_with_totals(client, relay):
    users()
    ae('CT_1', 'CT'), ae('MR_1', 'MR')
    study(1), study(2, patient_class='I'), study(3, storing_ae='MR_1')
    defn = {'dataset': 'studies', 'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'}},
            'output': {'kind': 'table', 'rows': 'modality', 'cols': 'patient_type', 'measures': ['count']}}
    sid = schedule(client, [worklist(client, 'Split', defn)])
    make_due(sid)
    mailer.run_due()
    assert cells(attachments(relay.mails[0])['Split.xlsx']) == [
        ['Modality · Studies', 'Inpatient', 'Outpatient', 'Total'],
        ['CT', 1, 1, 2],
        ['MR', 0, 1, 1],
        ['Total', 1, 2, 3]]


def test_a_measure_that_does_not_add_up_gets_no_total():
    compiled = {'kind': 'table', 'dims': [{'id': 'm', 'label': 'Modality'}], 'columns': None,
                'measures': [{'id': 'n', 'label': 'Studies', 'additive': True},
                             {'id': 'med', 'label': 'Median', 'additive': False}]}
    headers, rows, total = mailer.table_for(compiled, {'rows': [['CT', 2, 5.5], ['MR', 1, 9.0]], 'columns': []})
    assert headers == ['Modality', 'Studies', 'Median']
    assert rows == [['CT', 2, 5.5], ['MR', 1, 9.0]] and total == ['Total', 3, None]


def test_a_measure_with_no_grouping_is_one_line():
    compiled = {'kind': 'table', 'dims': [], 'columns': None,
                'measures': [{'id': 'n', 'label': 'Studies', 'additive': True}]}
    assert mailer.table_for(compiled, {'rows': [[7]], 'columns': []}) == (['', 'Studies'], [['All', 7]], None)


def test_a_chart_arrives_as_its_table(client, relay):
    users()
    study(1)
    defn = {**BY_MODALITY, 'output': {**BY_MODALITY['output'], 'kind': 'chart'}}
    sid = schedule(client, [worklist(client, 'Bars', defn)])
    make_due(sid)
    mailer.run_due()
    assert cells(attachments(relay.mails[0])['Bars.xlsx'])[0] == ['Modality', 'Studies']


def test_written_sql_runs_with_its_own_headers_and_odd_types(client, relay):
    users()
    defn = {'sql': """SELECT 'x' AS "Label", ARRAY['a','b'] AS "List", INTERVAL '1 day' AS "Gap" """}
    sid = schedule(client, [worklist(client, 'Handmade', defn)])
    make_due(sid)
    mailer.run_due()
    table = cells(attachments(relay.mails[0])['Handmade.xlsx'])
    assert table[0] == ['Label', 'List', 'Gap'] and table[1][0] == 'x' and 'a' in table[1][1]
    assert run_rows(sid)[0]['status'] == 'sent'


def test_the_last_saved_version_is_what_goes(client, relay):
    users()
    study(1, accession_number='ACC-1')
    wid = worklist(client, 'Today', LIST)
    sid = schedule(client, [wid])
    defn = {**LIST, 'output': {'kind': 'list', 'columns': ['accession']}}
    save(client, {'id': wid, 'name': 'Today', 'folder': 'Mail', 'visibility': 'shared', 'definition': defn}, uid=ADMIN)
    make_due(sid)
    mailer.run_due()
    assert len(cells(attachments(relay.mails[0])['Today.xlsx'])[0]) == 1
    assert run_rows(sid)[0]['detail'][0]['version'] == 2


def test_quick_dates_are_worked_out_on_the_day_it_runs(client, relay):
    users()
    study(1, accession_number='TODAY')
    study(2, accession_number='OLD', study_date=(datetime.now() - timedelta(days=40)).date())
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    table = cells(attachments(relay.mails[0])['Today.xlsx'])
    assert [r[0] for r in table[1:]] == ['TODAY']


def test_a_formula_typed_into_the_data_stays_text(client, relay):
    users()
    study(1, study_description='=HYPERLINK("http://evil","click")')
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    cell = attachments(relay.mails[0])['Today.xlsx'].cell(row=2, column=3)
    assert cell.value.startswith('=HYPERLINK') and cell.data_type == 's'


def test_no_rows_means_no_attachment_and_says_so(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    assert attachments(relay.mails[0]) == {}
    assert '- Today: no rows.' in body(relay.mails[0])
    assert run_rows(sid)[0]['status'] == 'sent'


# ── When something goes wrong ─────────────────────────────────────────────────

def test_one_failing_worklist_does_not_stop_the_others(client, relay):
    users()
    study(1)
    good = worklist(client, 'Good', LIST)
    bad = worklist(client, 'Bad', {'sql': 'SELECT password_hash FROM public.users'})   # outside the sandbox
    sid = schedule(client, [good, bad])
    make_due(sid)
    mailer.run_due()
    mail = relay.mails[0]
    assert list(attachments(mail)) == ['Good.xlsx']
    assert '- Bad: could not be produced' in body(mail)
    assert 'permission denied' not in body(mail) and 'password_hash' not in body(mail)
    run = run_rows(sid)[0]
    assert run['status'] == 'partial'
    failed = next(e for e in run['detail'] if e['name'] == 'Bad')
    assert 'permission denied' in failed['error']


def test_relay_down_is_a_failed_run_and_the_next_slot_still_comes(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    setting('smtp_port', '1')           # nothing listens there
    assert mailer.run_due() == 1
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and run['error'] and run['finished_at']
    assert sql("SELECT next_run_at FROM worklist_email_schedules WHERE id = :id",
               id=sid)[0]['next_run_at'] > datetime.now()


def test_asked_for_tls_it_never_falls_back_to_plain(client, relay):
    users()
    study(1)
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    setting('smtp_security', 'starttls')    # this relay does not offer it
    mailer.run_due()
    assert relay.mails == []
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'STARTTLS' in run['error']


def test_a_refused_recipient_is_recorded(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    relay.refuse.add('b@laumc.test')
    mailer.run_due()
    assert relay.mails[0].rcpts == ['a@laumc.test']
    run = run_rows(sid)[0]
    assert run['status'] == 'partial' and 'b@laumc.test' in run['error']


def test_too_large_is_left_out_and_said(client, relay, monkeypatch):
    users()
    study(1)
    monkeypatch.setattr(mailer, 'MAX_ATTACHMENTS_BYTES', 100)
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    assert attachments(relay.mails[0]) == {}
    assert 'too large to email' in body(relay.mails[0])
    assert run_rows(sid)[0]['status'] == 'partial'


def test_more_rows_than_the_cap_says_so(client, relay, monkeypatch):
    users()
    study(1), study(2), study(3)
    monkeypatch.setattr(engine, 'CSV_CAP', 2)
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    assert len(cells(attachments(relay.mails[0])['Today.xlsx'])) == 3     # header + 2
    assert 'first 2 rows only' in body(relay.mails[0])
    run = run_rows(sid)[0]
    assert run['status'] == 'partial' and run['detail'][0]['truncated']


# ── Patient data ──────────────────────────────────────────────────────────────

def test_rows_are_in_the_attachment_never_the_message(client, relay):
    users()
    study(1, accession_number='ACC-SECRET', study_description='CT SECRET')
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    mailer.run_due()
    text_part = body(relay.mails[0])
    assert 'ACC-SECRET' not in text_part and 'CT SECRET' not in text_part
    assert '- Today: 1 row, attached.' in text_part
    assert 'patient information' in text_part
    assert 'ACC-SECRET' not in json.dumps(run_rows(sid)[0]['detail'])


@pytest.mark.parametrize('recipients, word', [
    ('someone@gmail.com', 'outside the allowed domains'),
    ('a@laumc.test, someone@gmail.com', 'outside the allowed domains'),
    ('a@sub.laumc.test', 'outside the allowed domains'),
    ('not-an-address', 'not an email address'),
    ('a@laumc.test\nBcc: x@gmail.com', 'not an email address'),
    ('', 'at least one recipient'),
])
def test_recipients_are_checked_on_save(client, relay, recipients, word):
    users()
    wid = worklist(client, 'Today', LIST)
    r = h(client, 'post', '/worklists/api/schedules/save',
          json={'name': 'x', 'recipients': recipients, 'frequency': 'daily', 'send_time': '07:00',
                'worklist_ids': [wid]})
    assert r.status_code == 400 and word in r.get_json()['error']


def test_recipients_are_checked_again_when_it_runs(client, relay):
    """The domain list tightened after the schedule was saved: nothing goes, not even a query."""
    users()
    study(1)
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    setting('smtp_allowed_domains', 'other.test')
    assert mailer.run_due() == 1
    assert relay.mails == []
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'outside the allowed domains' in run['error'] and run['detail'] == []


def test_no_allowed_domains_means_no_schedule(client, relay):
    users()
    wid = worklist(client, 'Today', LIST)
    setting('smtp_allowed_domains', '')
    r = h(client, 'post', '/worklists/api/schedules/save',
          json={'name': 'x', 'recipients': 'a@laumc.test', 'frequency': 'daily', 'send_time': '07:00',
                'worklist_ids': [wid]})
    assert r.status_code == 400
    assert not mailer.is_ready(mailer.mail_config())


# ── Mail server settings ──────────────────────────────────────────────────────

MAIL = {'smtp_host': 'relay.laumc.test', 'smtp_port': '587', 'smtp_security': 'starttls',
        'smtp_username': 'rayd', 'smtp_password': 's3cret', 'smtp_from': 'rayd@laumc.test',
        'smtp_allowed_domains': 'laumc.test, LAU.edu.test'}


def test_the_password_is_encrypted_and_never_comes_back(client):
    users()
    r = h(client, 'post', '/admin/email-server/api/save', json=MAIL)
    assert r.status_code == 200 and 's3cret' not in r.get_data(as_text=True)
    stored = sql("SELECT value FROM settings WHERE key = 'smtp_password'")[0]['value']
    assert stored and stored != 's3cret'
    assert decrypt(stored) == 's3cret'
    got = h(client, 'get', '/worklists/api/schedules')
    assert got.get_json()['mail']['password_set'] is True and stored not in got.get_data(as_text=True)
    assert 'smtp_password' not in got.get_json()['mail']
    assert got.get_json()['mail']['smtp_allowed_domains'] == 'laumc.test, lau.edu.test'
    own = h(client, 'get', '/admin/email-server/api')
    assert own.get_json()['mail']['password_set'] is True and own.get_json()['ready'] is True
    assert 's3cret' not in own.get_data(as_text=True) and stored not in own.get_data(as_text=True)

    # left empty: kept; asked to: removed
    h(client, 'post', '/admin/email-server/api/save', json={**MAIL, 'smtp_password': ''})
    assert sql("SELECT value FROM settings WHERE key = 'smtp_password'")[0]['value'] == stored
    h(client, 'post', '/admin/email-server/api/save', json={**MAIL, 'smtp_password': '', 'clear_password': True})
    assert sql("SELECT value FROM settings WHERE key = 'smtp_password'")[0]['value'] == ''


@pytest.mark.parametrize('change, word', [
    ({'smtp_port': 'abc'}, 'port'), ({'smtp_port': '70000'}, 'port'), ({'smtp_security': 'magic'}, 'security'),
    ({'smtp_from': 'nope'}, 'From'), ({'smtp_allowed_domains': ''}, 'domain'),
    ({'smtp_allowed_domains': 'not a domain'}, 'domain'),
])
def test_bad_mail_settings_are_refused(client, change, word):
    users()
    r = h(client, 'post', '/admin/email-server/api/save', json={**MAIL, **change})
    assert r.status_code == 400 and word in r.get_json()['error']


def test_an_empty_host_switches_sending_off(client):
    users()
    r = h(client, 'post', '/admin/email-server/api/save', json={'smtp_host': ''})
    assert r.status_code == 200 and r.get_json()['ready'] is False


def test_the_test_email_has_no_data_and_reports_failure(client, relay):
    users()
    assert h(client, 'post', '/admin/email-server/api/test', json={'to': 'me@laumc.test'}).status_code == 200
    assert relay.mails[0].rcpts == ['me@laumc.test'] and attachments(relay.mails[0]) == {}
    outside = h(client, 'post', '/admin/email-server/api/test', json={'to': 'me@gmail.com'})
    assert outside.status_code == 400
    setting('smtp_port', '1')
    down = h(client, 'post', '/admin/email-server/api/test', json={'to': 'me@laumc.test'})
    assert down.status_code == 502 and 'did not take' in down.get_json()['error']


# ── The page's schedules ──────────────────────────────────────────────────────

def test_list_edit_and_history(client, relay):
    users()
    a, b = worklist(client, 'A', LIST), worklist(client, 'B', LIST)
    sid = schedule(client, [a, b], frequency='weekly', weekday=2, send_time='06:30')
    got = h(client, 'get', '/worklists/api/schedules').get_json()
    assert got['ready'] is True
    s = got['schedules'][0]
    assert (s['frequency'], s['weekday'], s['month_day'], s['send_time']) == ('weekly', 2, None, '06:30')
    assert [w['name'] for w in s['worklists']] == ['A', 'B'] and s['last_run'] is None
    assert s['next_run_at'].endswith('06:30')

    schedule(client, [b], id=sid, frequency='monthly', month_day=15)
    s = h(client, 'get', '/worklists/api/schedules').get_json()['schedules'][0]
    assert (s['frequency'], s['weekday'], s['month_day']) == ('monthly', None, 15)
    assert [w['name'] for w in s['worklists']] == ['B']

    r = h(client, 'post', f'/worklists/api/schedules/{sid}/send').get_json()
    assert r['status'] == 'sent'
    s = h(client, 'get', '/worklists/api/schedules').get_json()['schedules'][0]
    assert s['last_run']['status'] == 'sent'
    history = h(client, 'get', f'/worklists/api/schedules/{sid}/runs').get_json()
    assert history[0]['recipients'] == ['a@laumc.test', 'b@laumc.test'] and history[0]['started_by'] == 'wl-admin'


def test_send_now_works_on_a_paused_schedule_and_keeps_its_next_slot(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)], enabled=False)
    before = sql("SELECT next_run_at FROM worklist_email_schedules WHERE id = :id", id=sid)[0]
    assert h(client, 'post', f'/worklists/api/schedules/{sid}/send').get_json()['status'] == 'sent'
    assert len(relay.mails) == 1
    assert sql("SELECT next_run_at FROM worklist_email_schedules WHERE id = :id", id=sid)[0] == before
    assert h(client, 'post', '/worklists/api/schedules/999999/send').status_code == 404


@pytest.mark.parametrize('change, word', [
    ({'name': '  '}, 'name'), ({'frequency': 'hourly'}, 'frequency'), ({'send_time': '25:00'}, 'time'),
    ({'frequency': 'weekly'}, 'weekday'), ({'frequency': 'weekly', 'weekday': 7}, 'weekday'),
    ({'frequency': 'monthly', 'month_day': 32}, 'day of the month'), ({'worklist_ids': []}, 'report'),
    ({'worklist_ids': ['abc']}, 'report'), ({'worklist_ids': [999999]}, 'report'),
    ({'id': 999999}, 'no longer exists'),
])
def test_bad_schedules_are_refused(client, relay, change, word):
    users()
    wid = worklist(client, 'Today', LIST)
    body_ = {'name': 'x', 'recipients': 'a@laumc.test', 'frequency': 'daily', 'send_time': '07:00',
             'worklist_ids': [wid], **change}
    r = h(client, 'post', '/worklists/api/schedules/save', json=body_)
    assert r.status_code == 400 and word in r.get_json()['error']


def test_a_name_cannot_carry_a_header(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)], name='Pack\r\nBcc: x@gmail.com')
    assert h(client, 'post', f'/worklists/api/schedules/{sid}/send').get_json()['status'] == 'sent'
    assert relay.mails[0].msg['Bcc'] is None
    assert sorted(relay.mails[0].rcpts) == ['a@laumc.test', 'b@laumc.test']


def test_a_scheduled_worklist_cannot_be_deleted_until_taken_out(client, relay):
    users()
    a, b = worklist(client, 'A', LIST), worklist(client, 'B', LIST)
    sid = schedule(client, [a, b])
    r = h(client, 'post', f'/worklists/api/{a}/delete')
    assert r.status_code == 409 and 'Morning pack' in r.get_json()['error']
    schedule(client, [b], id=sid)
    assert h(client, 'post', f'/worklists/api/{a}/delete').status_code == 200


def test_deleting_a_schedule_keeps_the_record_of_what_it_sent(client, relay):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    h(client, 'post', f'/worklists/api/schedules/{sid}/send')
    assert h(client, 'post', f'/worklists/api/schedules/{sid}/delete').status_code == 200
    run = run_rows()[0]
    assert run['schedule_id'] is None and run['schedule_name'] == 'Morning pack'
    assert run['recipients'] == ['a@laumc.test', 'b@laumc.test']


# ── Who ───────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('method, url', [
    ('get', '/worklists/api/schedules'), ('post', '/worklists/api/schedules/save'),
    ('post', '/worklists/api/schedules/1/delete'), ('get', '/worklists/api/schedules/1/runs'),
    ('post', '/worklists/api/schedules/1/send'), ('get', '/admin/email-server'),
    ('get', '/admin/email-server/api'), ('post', '/admin/email-server/api/save'),
    ('post', '/admin/email-server/api/test'),
])
def test_only_an_admin_gets_near_it(client, relay, method, url):
    users()
    sql(f"INSERT INTO user_page_permissions (user_id, page_key, is_enabled) VALUES ({TEC}, 'custom_reports', true)")
    assert h(client, method, url, uid=VIEWER).status_code == 403
    assert h(client, method, url, uid=TEC).status_code == 403
    assert getattr(client, method)(url).status_code in (302, 401)


def test_someone_elses_personal_worklist_stays_theirs(client, relay):
    """An admin cannot open a colleague's personal worklist on the page, so a schedule must
    not be a way of having it mailed to them."""
    users()
    mine = worklist(client, 'Viewer private', LIST, uid=VIEWER, visibility='personal')
    r = h(client, 'post', '/worklists/api/schedules/save',
          json={'name': 'x', 'recipients': 'a@laumc.test', 'frequency': 'daily', 'send_time': '07:00',
                'worklist_ids': [mine]})
    assert r.status_code == 400 and 'not yours to send' in r.get_json()['error']


def test_a_colleague_may_keep_what_a_schedule_holds(client, relay):
    users()
    sql(f"INSERT INTO users (id, username, password_hash, role) VALUES ({OTHER_ADMIN}, 'wl-admin2', 'x', 'admin')")
    private = worklist(client, 'Admin private', LIST, visibility='personal')
    shared = worklist(client, 'Shared', LIST)
    sid = schedule(client, [private])
    # the other admin edits it, keeping the private one and adding a shared one
    schedule(client, [private, shared], uid=OTHER_ADMIN, id=sid)
    got = h(client, 'get', '/worklists/api/schedules', uid=OTHER_ADMIN).get_json()['schedules'][0]
    assert {w['id'] for w in got['worklists']} == {private, shared}


def test_the_editor_offers_scheduling_to_admins_only(client):
    users()
    html = h(client, 'get', '/report-composer', uid=VIEWER).get_data(as_text=True)
    assert 'id="btn-sched" type="button" hidden' in html
    assert h(client, 'get', '/worklists/api/catalog', uid=VIEWER).get_json()['me']['can_schedule'] is False


def test_the_mail_server_is_set_in_administration_not_in_report_composer(client):
    """Moved 2026-11-07: Report Composer and the Sticky Notes monthly report both send
    through it, so it is an Administration page of its own, and the composer only links to it."""
    users()
    page = h(client, 'get', '/admin/email-server')
    html = page.get_data(as_text=True)
    assert page.status_code == 200 and 'Email Server' in html and 'id="mx-host"' in html
    assert 'href="/admin/email-server"' in html                      # the sidebar, under Administration
    composer = h(client, 'get', '/report-composer').get_data(as_text=True)
    assert 'mx-host' not in composer and 'mail-settings' not in composer
    assert '"/admin/email-server"' in composer                       # the link where the form was
    assert 'href="/admin/email-server"' not in h(client, 'get', '/report-composer', uid=VIEWER).get_data(as_text=True)
    for gone in ('/worklists/api/mail-settings', '/worklists/api/mail-settings/test'):
        assert h(client, 'post', gone, json=MAIL).status_code == 404


def test_the_email_server_page_script_is_valid_js():
    import shutil
    import subprocess
    import jinja2
    src = open(os.path.join(ROOT, 'templates', 'email_server.html'), encoding='utf-8').read()
    jinja2.Environment().parse(src)
    if not shutil.which('node'):
        pytest.skip('node not installed')
    script = re.sub(r'\{\{.*?\}\}', 'x', re.search(r'<script>\n(.*?)</script>', src, re.S).group(1))
    tmp = os.path.join(os.path.dirname(__file__), '_email_server_page.js')
    try:
        open(tmp, 'w', encoding='utf-8').write(script)
        done = subprocess.run(['node', '--check', tmp], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    finally:
        os.remove(tmp)


# ── Microsoft 365 ─────────────────────────────────────────────────────────────
#
# Stand-ins for Entra ID and Exchange Online, spoken to over real sockets and real TLS.
# The stand-in Exchange is strict in the ways the real one is on port 587: nothing can sign
# in before STARTTLS, EHLO again after it, and no mail without signing in. XOAUTH2 follows
# Microsoft's example: AUTH XOAUTH2, a 334, then the token. None of this is a real tenant.

TENANT = '11111111-2222-3333-4444-555555555555'
CLIENT = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
SECRET = 's3cret-value'
MAILBOX = 'rayd@laumc.test'
TOKEN = 'tok-' + 'x' * 2400     # real ones run to a couple of thousand characters


class _Exchange(socketserver.StreamRequestHandler):
    def _say(self, line):
        self.wfile.write(line.encode() + b'\r\n')

    def _read(self):
        return self.rfile.readline().decode('utf-8', 'replace').strip()

    def handle(self):
        relay = self.server.relay
        secure = greeted = signed_in = False
        rcpts = []
        self._say('220 test exchange')
        while True:
            line = self._read()
            if not line:
                return
            word = line.upper()
            if word.startswith('EHLO'):
                greeted = True
                extra = ['250-AUTH PLAIN XOAUTH2'] if secure else ['250-STARTTLS'] if relay.offer_tls else []
                self._say('\r\n'.join(['250-test exchange', *extra, '250 8BITMIME']))
            elif word == 'STARTTLS':
                self._say('220 go ahead')
                self.request = relay.tls.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile('rb')
                self.wfile = self.request.makefile('wb', buffering=0)
                secure, greeted = True, False
            elif word.startswith('AUTH'):
                if not (secure and greeted):
                    relay.broken_rules.append(line[:40])
                    self._say('503 5.5.1 STARTTLS and EHLO first')
                elif word == 'AUTH XOAUTH2':
                    self._say('334 ')
                    relay.signed_in_with.append(base64.b64decode(self._read()).decode())
                    if relay.refuse_token:
                        self._say('535 5.7.3 Authentication unsuccessful')
                    else:
                        signed_in = True
                        self._say('235 2.7.0 Authentication successful')
                elif word.startswith('AUTH PLAIN '):
                    relay.signed_in_with.append(base64.b64decode(line.split()[2]).decode())
                    signed_in = True
                    self._say('235 2.7.0 Authentication successful')
                else:
                    relay.broken_rules.append(line[:40])
                    self._say('504 5.5.4 unrecognized')
            elif word.startswith('MAIL FROM'):
                self._say('250 ok' if signed_in else '530 5.7.57 Client not authenticated')
            elif word.startswith('RCPT TO'):
                rcpts.append(re.search(r'<(.*?)>', line).group(1))
                self._say('250 ok')
            elif word == 'DATA':
                self._say('354 go')
                chunks = []
                while True:
                    chunk = self.rfile.readline()
                    if chunk == b'.\r\n':
                        break
                    chunks.append(chunk[1:] if chunk.startswith(b'..') else chunk)
                msg = email.message_from_bytes(b''.join(chunks), policy=email.policy.default)
                relay.mails.append(type('Mail', (), {'rcpts': rcpts, 'msg': msg}))
                self._say('250 queued')
            elif word == 'QUIT':
                self._say('221 bye')
                return
            else:
                self._say('250 ok')


class _Entra(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        entra = self.server.entra
        form = urllib.parse.parse_qs(self.rfile.read(int(self.headers['Content-Length'])).decode())
        entra.requests.append((self.path, {k: v[0] for k, v in form.items()}))
        status, answer = entra.answer
        data = answer if isinstance(answer, bytes) else json.dumps(answer).encode()
        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture(scope='module')
def certificate(tmp_path_factory):
    """A certificate for localhost, as (certificate file, key file)."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    now = datetime.now()
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    folder = tmp_path_factory.mktemp('tls')
    cert_file, key_file = folder / 'cert.pem', folder / 'key.pem'
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                           serialization.NoEncryption()))
    return str(cert_file), str(key_file)


class Microsoft:
    """Exchange Online and Entra ID, and RAYD's settings pointing at them."""

    def __init__(self, certificate, monkeypatch):
        self.mails, self.signed_in_with, self.broken_rules = [], [], []
        self.offer_tls, self.refuse_token = True, False
        self.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.tls.load_cert_chain(*certificate)
        self.exchange = socketserver.ThreadingTCPServer(('127.0.0.1', 0), _Exchange)
        self.exchange.relay = self
        self.requests, self.answer = [], (200, {'access_token': TOKEN, 'token_type': 'Bearer', 'expires_in': 3599})
        self.entra = http.server.ThreadingHTTPServer(('127.0.0.1', 0), _Entra)
        self.entra.entra = self
        for server in (self.exchange, self.entra):
            threading.Thread(target=server.serve_forever, daemon=True).start()
        monkeypatch.setenv('NO_PROXY', '127.0.0.1')
        monkeypatch.setattr(mailer, 'O365_TOKEN_URL',
                            f'http://127.0.0.1:{self.entra.server_address[1]}/{{tenant}}/oauth2/v2.0/token')
        self.trust_certificate = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.trust_certificate.load_verify_locations(certificate[0])
        monkeypatch.setattr(mailer.ssl, 'create_default_context', lambda: self.trust_certificate)
        for k, v in (('smtp_host', 'localhost'), ('smtp_port', str(self.exchange.server_address[1])),
                     ('smtp_security', 'starttls'), ('smtp_from', MAILBOX), ('smtp_allowed_domains', 'laumc.test'),
                     ('smtp_auth', 'oauth_o365'), ('smtp_oauth_tenant', TENANT), ('smtp_oauth_client_id', CLIENT),
                     ('smtp_oauth_client_secret', encrypt(SECRET))):
            setting(k, v)

    def stop(self):
        for server in (self.exchange, self.entra):
            server.shutdown()
            server.server_close()


@pytest.fixture
def microsoft(certificate, monkeypatch):
    m = Microsoft(certificate, monkeypatch)
    yield m
    m.stop()


def due_schedule(client):
    users()
    sid = schedule(client, [worklist(client, 'Today', LIST)])
    make_due(sid)
    return sid


def test_o365_signs_in_with_a_token_and_sends(client, microsoft):
    study(1, accession_number='ACC-1')
    sid = due_schedule(client)
    assert mailer.run_due() == 1

    run = run_rows(sid)[0]
    assert run['status'] == 'sent', run['error']
    path, form = microsoft.requests[0]
    assert path == f'/{TENANT}/oauth2/v2.0/token'
    assert form == {'grant_type': 'client_credentials', 'client_id': CLIENT, 'client_secret': SECRET,
                    'scope': 'https://outlook.office365.com/.default'}
    assert microsoft.signed_in_with == [f'user={MAILBOX}\x01auth=Bearer {TOKEN}\x01\x01']
    assert microsoft.broken_rules == []
    mail = microsoft.mails[0]
    assert mail.msg['From'] == MAILBOX and sorted(mail.rcpts) == ['a@laumc.test', 'b@laumc.test']
    assert cells(attachments(mail)['Today.xlsx'])[1][0] == 'ACC-1'


def test_o365_a_refused_secret_is_a_failed_run_with_microsofts_reason(client, microsoft):
    sid = due_schedule(client)
    microsoft.answer = (401, {'error': 'invalid_client', 'error_description':
                              'AADSTS7000215: Invalid client secret provided.\r\nTrace ID: abc\r\nTimestamp: now'})
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed'
    assert run['error'] == 'Microsoft refused the sign-in: AADSTS7000215: Invalid client secret provided.'
    assert microsoft.signed_in_with == [] and microsoft.mails == []


def test_o365_an_answer_that_is_not_json_is_still_a_clear_failure(client, microsoft):
    sid = due_schedule(client)
    microsoft.answer = (502, b'<html>Bad gateway</html>')
    mailer.run_due()
    assert run_rows(sid)[0]['error'] == 'Microsoft refused the sign-in: HTTP 502'


def test_o365_a_token_refused_for_the_mailbox_says_where_to_look(client, microsoft):
    sid = due_schedule(client)
    microsoft.refuse_token = True
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and microsoft.mails == []
    assert 'refused it for ' + MAILBOX in run['error'] and '535' in run['error']
    assert 'docs/LAUMC_O365_EMAIL_SETUP.md' in run['error']
    assert TOKEN not in run['error'] and SECRET not in run['error']


def test_o365_entra_unreachable_is_a_failed_run_without_the_secret(client, microsoft, monkeypatch):
    sid = due_schedule(client)
    monkeypatch.setattr(mailer, 'O365_TOKEN_URL', 'http://127.0.0.1:1/{tenant}/oauth2/v2.0/token')
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and run['error'] and SECRET not in run['error']


@pytest.mark.parametrize('security', ['none', 'starttls_unverified'])
def test_o365_no_token_goes_over_a_plain_or_unverified_connection(client, microsoft, security):
    sid = due_schedule(client)
    setting('smtp_security', security)
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'certificate checked' in run['error']
    assert microsoft.requests == [] and microsoft.signed_in_with == []


def test_o365_a_relay_without_starttls_gets_no_token(client, microsoft):
    sid = due_schedule(client)
    microsoft.offer_tls = False
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'STARTTLS' in run['error']
    assert microsoft.requests == [] and microsoft.signed_in_with == []


def test_o365_an_untrusted_certificate_gets_no_token(client, microsoft, monkeypatch):
    sid = due_schedule(client)
    monkeypatch.setattr(mailer.ssl, 'create_default_context', lambda: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))
    mailer.run_due()
    run = run_rows(sid)[0]
    assert run['status'] == 'failed' and 'CERTIFICATE_VERIFY_FAILED' in run['error']
    assert microsoft.requests == [] and microsoft.signed_in_with == []


def test_password_sign_in_over_real_starttls(client, microsoft):
    """The password path, which only ever met a relay with no TLS and no login above."""
    sid = due_schedule(client)
    setting('smtp_auth', 'password')
    setting('smtp_username', 'relay-user')
    setting('smtp_password', encrypt('relay-pass'))
    mailer.run_due()
    assert run_rows(sid)[0]['status'] == 'sent'
    assert microsoft.signed_in_with == ['\x00relay-user\x00relay-pass']
    assert microsoft.requests == [] and microsoft.broken_rules == []


O365_FORM = {'smtp_host': 'smtp.office365.com', 'smtp_port': '587', 'smtp_security': 'starttls',
             'smtp_from': MAILBOX, 'smtp_allowed_domains': 'laumc.test', 'smtp_auth': 'oauth_o365',
             'smtp_oauth_tenant': TENANT, 'smtp_oauth_client_id': CLIENT, 'smtp_oauth_client_secret': SECRET}


def stored(key):
    return sql("SELECT value FROM settings WHERE key = :k", k=key)[0]['value']


def test_o365_settings_keep_the_secret_encrypted_and_to_themselves(client):
    users()
    r = h(client, 'post', '/admin/email-server/api/save', json=O365_FORM)
    assert r.status_code == 200 and r.get_json()['ready'] is True and SECRET not in r.get_data(as_text=True)
    saved = stored('smtp_oauth_client_secret')
    assert saved != SECRET and decrypt(saved) == SECRET
    page = h(client, 'get', '/worklists/api/schedules')
    assert saved not in page.get_data(as_text=True)
    mail = page.get_json()['mail']
    assert mail['client_secret_set'] is True and 'smtp_oauth_client_secret' not in mail
    assert (mail['smtp_auth'], mail['smtp_oauth_tenant'], mail['smtp_oauth_client_id']) == ('oauth_o365', TENANT, CLIENT)

    # left empty it is kept, and an earlier password does not linger
    setting('smtp_password', encrypt('old'))
    assert h(client, 'post', '/admin/email-server/api/save',
             json={**O365_FORM, 'smtp_oauth_client_secret': ''}).status_code == 200
    assert stored('smtp_oauth_client_secret') == saved and stored('smtp_password') == ''


def test_going_back_to_a_password_drops_the_microsoft_settings(client):
    users()
    h(client, 'post', '/admin/email-server/api/save', json=O365_FORM)
    r = h(client, 'post', '/admin/email-server/api/save',
          json={**O365_FORM, 'smtp_auth': 'password', 'smtp_username': 'u', 'smtp_password': 'p'})
    assert r.status_code == 200
    assert stored('smtp_oauth_client_secret') == '' and stored('smtp_oauth_tenant') == ''
    assert decrypt(stored('smtp_password')) == 'p' and stored('smtp_username') == 'u'


@pytest.mark.parametrize('change, word', [
    ({'smtp_auth': 'magic'}, 'sign-in'),
    ({'smtp_security': 'none'}, 'certificate checked'),
    ({'smtp_security': 'starttls_unverified'}, 'certificate checked'),
    ({'smtp_oauth_tenant': ''}, 'tenant'),
    ({'smtp_oauth_tenant': 'evil.com/../x'}, 'tenant'),      # it goes into the token URL
    ({'smtp_oauth_tenant': 'contoso.com?x=1'}, 'tenant'),
    ({'smtp_oauth_client_id': 'not-a-guid'}, 'client ID'),
    ({'smtp_oauth_client_secret': ''}, 'client secret'),
])
def test_bad_microsoft_settings_are_refused(client, change, word):
    users()
    r = h(client, 'post', '/admin/email-server/api/save', json={**O365_FORM, **change})
    assert r.status_code == 400 and word in r.get_json()['error']


def test_a_tenant_may_be_its_domain(client):
    users()
    r = h(client, 'post', '/admin/email-server/api/save',
          json={**O365_FORM, 'smtp_oauth_tenant': 'laumc.onmicrosoft.com'})
    assert r.status_code == 200 and stored('smtp_oauth_tenant') == 'laumc.onmicrosoft.com'


def test_not_ready_until_every_microsoft_setting_is_there(client, microsoft):
    for key in ('smtp_oauth_tenant', 'smtp_oauth_client_id', 'smtp_oauth_client_secret'):
        before = stored(key)
        setting(key, '')
        assert not mailer.is_ready(mailer.mail_config()), key
        setting(key, before)
    assert mailer.is_ready(mailer.mail_config())


def test_the_o365_test_email_goes_through_the_same_sign_in(client, microsoft):
    users()
    r = h(client, 'post', '/admin/email-server/api/test', json={'to': 'me@laumc.test'})
    assert r.status_code == 200, r.get_data(as_text=True)
    assert microsoft.signed_in_with and attachments(microsoft.mails[0]) == {}
    microsoft.answer = (401, {'error_description': 'AADSTS7000222: The provided client secret keys are expired.'})
    r = h(client, 'post', '/admin/email-server/api/test', json={'to': 'me@laumc.test'})
    assert r.status_code == 502 and 'AADSTS7000222' in r.get_json()['error']
