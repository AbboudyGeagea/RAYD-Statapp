"""
tests/test_report_presets.py — saved filter presets on the shared report panel (operator,
2026-11-26: the Super Report's "Saved presets" on Report 22; routes/report_presets.py).
"""
import os

import pytest
from sqlalchemy import text

URL = '/report/22/presets'
ADMIN_ID, OTHER_ID, USER_ID = 98100, 98101, 98102


@pytest.fixture
def people(app):
    """Three real accounts (saved_reports.owner_user_id points at users): two administrators,
    who open every report, and a User with no report ticked."""
    from db import db
    from utils import site_db
    with app.app_context():
        for uid, name, role in ((ADMIN_ID, 'rp_admin', 'administrator'), (OTHER_ID, 'rp_other', 'administrator'),
                                (USER_ID, 'rp_user', 'user')):
            db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (:i, :n, 'x', :r)"),
                               {'i': uid, 'n': name, 'r': role})
            db.session.execute(text("INSERT INTO user_sites (user_id, site_id) SELECT :i, id FROM sites WHERE is_default"),
                               {'i': uid})
        db.session.commit()
        site_db.mirror_users()
    yield {k: {'X-Test-Uid': str(v)} for k, v in (('admin', ADMIN_ID), ('other', OTHER_ID), ('user', USER_ID))}
    with app.app_context():
        db.session.rollback()
        ids = {'ids': [ADMIN_ID, OTHER_ID, USER_ID]}
        db.session.execute(text("DELETE FROM saved_reports WHERE owner_user_id = ANY(:ids)"), ids)
        db.session.execute(text("DELETE FROM user_audit_log WHERE actor_user_id = ANY(:ids) OR target_user_id = ANY(:ids)"), ids)
        db.session.execute(text("DELETE FROM users WHERE id = ANY(:ids)"), ids)
        db.session.commit()
        site_db.mirror_users()


def test_report_22_shows_the_presets_under_its_panel(client):
    html = client.get('/report/22', headers={'X-Test-Role': 'administrator'}).get_data(as_text=True)
    assert 'id="rp-presets" data-report="22"' in html and 'js/report_presets.js' in html
    js = open(os.path.join(os.path.dirname(__file__), '..', 'static', 'js', 'report_presets.js'), encoding='utf-8').read()
    assert "new FormData(document.getElementById('mainForm'))" in js and 'function load' in js


def test_a_preset_is_saved_listed_replaced_by_name_and_deleted(client, people):
    me = people['admin']
    panel = {'start_date': ['2026-10-01'], 'end_date': ['2026-10-31'], 'modality': ['CT', 'MR'],
             'mod_enabled': ['on'], 'f_mod_mode': ['exclude'], 'bad key!': ['x'], 'empty': ['']}
    r = client.post(URL, headers=me, json={'name': '  October   CT/MR ', 'filters': panel})
    assert r.get_json()['success'] and r.get_json()['name'] == 'October CT/MR'

    got = client.get(URL, headers=me).get_json()
    assert [p['name'] for p in got] == ['October CT/MR']
    assert got[0]['filters'] == {'start_date': ['2026-10-01'], 'end_date': ['2026-10-31'], 'modality': ['CT', 'MR'],
                                 'mod_enabled': ['on'], 'f_mod_mode': ['exclude']}

    again = client.post(URL, headers=me, json={'name': 'October CT/MR', 'filters': {'modality': ['US']}})
    assert again.get_json()['id'] == got[0]['id']                       # same name: replaced
    assert client.get(URL, headers=me).get_json()[0]['filters'] == {'modality': ['US']}

    assert client.post(URL, headers=me, json={'name': ' ', 'filters': {}}).status_code == 400
    assert client.delete(f"{URL}/{got[0]['id']}", headers=me).get_json()['success']
    assert client.get(URL, headers=me).get_json() == []
    assert client.delete(f"{URL}/{got[0]['id']}", headers=me).status_code == 404


def test_each_person_sees_their_own_and_only_for_a_report_they_may_open(client, people):
    mine = client.post(URL, headers=people['admin'], json={'name': 'Mine', 'filters': {'modality': ['CT']}}).get_json()
    assert client.get(URL, headers=people['other']).get_json() == []    # someone else's list
    assert client.delete(f"{URL}/{mine['id']}", headers=people['other']).status_code == 404
    assert client.get(URL, headers=people['user']).status_code == 403   # Report 22 not ticked for them
    assert client.post(URL, headers=people['user'], json={'name': 'x', 'filters': {}}).status_code == 403
    # the Super Report's presets (no base report) stay apart
    assert client.get('/viewer/super-report/saved', headers=people['admin']).get_json() == []
