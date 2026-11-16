"""
tests/test_crn_sms.py — CRN's SMS sender for go-live (to-do item 20, November 2026).

e-tec SMS (e-tecsms.com), ported from the Mazloum branch, where CHT runs it: one POST per
batch of up to 50 numbers; e-tec answers HTTP 200 whatever happens, a refusal is a text
starting with ERR. The password is stored encrypted. And the hospital name on the page a
doctor opens is LAUMC's (migration 0154), no longer the Mazloum seed of migration 0132.
"""
import pytest
from sqlalchemy import text

from utils import crn_sms
from utils.crypto import encrypt

CFG = {'crn_sms_provider': 'etec', 'crn_sms_username': 'laumc', 'crn_sms_sender': 'LAUMC'}


class _Answer:
    def __init__(self, body, status=200):
        self.text, self.status_code = body, status


@pytest.fixture
def posts(monkeypatch):
    sent = []

    def fake_post(url, timeout, data):
        sent.append({'url': url, **data})
        return fake_post.answer
    fake_post.answer = _Answer('OK 4471')
    monkeypatch.setattr(crn_sms.requests, 'post', fake_post)
    return fake_post, sent


@pytest.mark.parametrize('e164, local', [('+9613123456', '03123456'), ('+96170123456', '70123456'),
                                         ('+96181123456', '81123456'), ('+33612345678', '0033612345678')])
def test_numbers_go_in_e_tecs_local_form(e164, local):
    assert crn_sms.etec_number(e164) == local


def test_the_dry_run_records_and_sends_nothing(posts):
    result = crn_sms.send_sms({'crn_sms_provider': 'log'}, '+96170123456', 'x')
    assert result['ok'] and result['dry_run'] and posts[1] == []


def test_e_tec_sends_with_the_decrypted_password(posts):
    result = crn_sms.send_sms({**CFG, 'crn_sms_password': encrypt('s3cret')}, '+96170123456', 'Ref ABC')
    sent = posts[1][0]
    assert result['ok'] and not result['dry_run'] and result['message_id'] == 'OK 4471'
    assert sent['url'] == crn_sms.ETEC_URL
    assert (sent['username'], sent['password'], sent['from'], sent['to'], sent['text'], sent['action']) == \
        ('laumc', 's3cret', 'LAUMC', '70123456', 'Ref ABC', 'sendsms')


@pytest.mark.parametrize('answer, error', [(_Answer('ERR: Not authenticated'), 'refused'),
                                           (_Answer(''), 'refused'),
                                           (_Answer('Bad gateway', 502), 'HTTP 502')])
def test_a_refusal_is_a_failed_send(posts, answer, error):
    posts[0].answer = answer
    result = crn_sms.send_sms({**CFG, 'crn_sms_password': encrypt('x')}, '+96170123456', 'Ref')
    assert not result['ok'] and error in result['error']


def test_many_numbers_go_in_batches_of_fifty(posts):
    numbers = [f'+961701{i:05d}' for i in range(120)]
    result = crn_sms.send_sms({**CFG, 'crn_sms_password': encrypt('x')}, numbers, 'Ref')
    assert result['ok'] and [len(p['to'].split(',')) for p in posts[1]] == [50, 50, 20]


def test_e_tec_without_its_credentials_sends_nothing(posts):
    result = crn_sms.send_sms({'crn_sms_provider': 'etec'}, '+96170123456', 'Ref')
    assert not result['ok'] and 'not set' in result['error'] and posts[1] == []


def test_settings_need_the_credentials_and_keep_the_password_encrypted(app):
    from db import db
    from utils import crn_admin_ops as ops
    base = {'crn_marker': 'CRN', 'crn_resend_min': '15', 'crn_fallback_min': '30', 'crn_overdue_min': '60',
            'crn_link_ttl_hours': '72', 'crn_sms_template': 'Ref {ref} {link}'}
    with app.app_context():
        errors = ops.save_settings({**base, 'crn_sms_provider': 'etec'})
        assert any('e-tec SMS needs' in e for e in errors)
        assert ops.save_settings({**base, 'crn_sms_provider': 'etec', 'crn_sms_username': 'laumc',
                                  'crn_sms_sender': 'LAUMC', 'crn_sms_password': 's3cret'}) == []
        stored = db.session.execute(text("SELECT value FROM settings WHERE key = 'crn_sms_password'")).scalar()
        assert stored and stored != 's3cret'
        rows = {name: (state, detail) for state, name, detail in ops.readiness(ops.load_settings(), check_gateway=False)}
        assert rows['SMS provider'] == ('ok', 'e-tec SMS, sender LAUMC')


def test_the_hospital_name_is_laumcs_in_each_database(app):
    """Migration 0132's Mazloum seed replaced by the served site's name (migration 0154)."""
    from db import db
    from utils import site_db
    with app.app_context():
        assert db.session.execute(text(
            "SELECT value FROM settings WHERE key = 'crn_hospital_name'")).scalar() == 'LAUMC - Rizk Hospital'
        sjh = site_db.engine_for(site_db.each_site_id()[1])
    with sjh.connect() as c:
        assert c.execute(text("SELECT value FROM settings WHERE key = 'crn_hospital_name'")).scalar() \
            == 'LAUMC - Saint John'


# ── email to SMS (LAUMC's gateway, operator 2026-11-16) ───────────────────────

@pytest.fixture
def gateway(app):
    """The Email Server pointed at a test relay that allows the gateway's domain."""
    from db import db
    from test_worklist_email import Relay
    relay = Relay()
    with app.app_context():
        for k, v in (('smtp_host', '127.0.0.1'), ('smtp_port', str(relay.port)), ('smtp_security', 'none'),
                     ('smtp_from', 'rayd@laumc.test'), ('smtp_allowed_domains', 'laumc.test, sms.laumc.test')):
            db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {'k': k, 'v': v})
        db.session.commit()
    yield relay
    relay.stop()


EMAIL = {'crn_sms_provider': 'email', 'crn_sms_email_to': '{number}@sms.laumc.test'}


def test_email_to_sms_sends_one_email_per_mobile(app, gateway):
    with app.app_context():
        result = crn_sms.send_sms(EMAIL, ['+96170123456', '+9613123456'], 'Ref ABC: https://crn/n/x')
    assert result['ok'] and not result['dry_run']
    assert [m.rcpts for m in gateway.mails] == [['70123456@sms.laumc.test'], ['03123456@sms.laumc.test']]
    assert gateway.mails[0].msg.get_content().strip() == 'Ref ABC: https://crn/n/x'


def test_the_mobile_can_go_in_the_subject_of_a_fixed_address(app, gateway):
    cfg = {'crn_sms_provider': 'email', 'crn_sms_email_to': 'sms@sms.laumc.test', 'crn_sms_email_subject': '{intl}'}
    with app.app_context():
        assert crn_sms.send_sms(cfg, '+96170123456', 'Ref')['ok']
    assert gateway.mails[0].rcpts == ['sms@sms.laumc.test'] and gateway.mails[0].msg['Subject'] == '96170123456'


@pytest.mark.parametrize('cfg, words', [
    ({'crn_sms_email_to': ''}, 'enter the gateway address'),
    ({'crn_sms_email_to': 'sms@sms.laumc.test'}, 'put the mobile'),
    ({'crn_sms_email_to': '{phone}@sms.laumc.test'}, 'may only use'),
    ({'crn_sms_email_to': '{number}@elsewhere.test'}, 'add elsewhere.test'),
])
def test_email_to_sms_refuses_settings_that_cannot_send(app, gateway, cfg, words):
    with app.app_context():
        result = crn_sms.send_sms({'crn_sms_provider': 'email', **cfg}, '+96170123456', 'Ref')
    assert not result['ok'] and words in result['error'] and gateway.mails == []


def test_email_to_sms_needs_the_email_server(app):
    with app.app_context():
        result = crn_sms.send_sms(EMAIL, '+96170123456', 'Ref')
    assert not result['ok'] and 'Email Server is not set up' in result['error']


def test_a_refused_address_is_a_failed_send(app, gateway):
    gateway.refuse.add('70123456@sms.laumc.test')
    with app.app_context():
        result = crn_sms.send_sms(EMAIL, '+96170123456', 'Ref')
    assert not result['ok'] and '70123456@sms.laumc.test' in result['error']


def test_email_to_sms_settings_are_checked_and_shown_ready(app, gateway):
    from utils import crn_admin_ops as ops
    base = {'crn_marker': 'CRN', 'crn_resend_min': '15', 'crn_fallback_min': '30', 'crn_overdue_min': '60',
            'crn_link_ttl_hours': '72', 'crn_sms_template': 'Ref {ref} {link}', 'crn_sms_provider': 'email'}
    with app.app_context():
        assert any('put the mobile' in e for e in ops.save_settings({**base, 'crn_sms_email_to': 'sms@sms.laumc.test'}))
        assert ops.save_settings({**base, 'crn_sms_email_to': '{number}@sms.laumc.test'}) == []
        rows = {name: (state, detail) for state, name, detail in ops.readiness(ops.load_settings(), check_gateway=False)}
    assert rows['SMS provider'] == ('ok', 'email to SMS: +96170123456 is emailed to 70123456@sms.laumc.test')
