"""
tests/test_site_databases.py — SJH in its own database, same web app (meeting of
2026-11-13, item 13a; utils/site_db.py, migration 0153).

The suite runs with two databases, as LAUMC will once POSTGRES_DB_SJH is set: the main one
(RH, logins, access) and rayd_pytest_laumc_sjh, which utils/site_db.py created, loaded with
init-db/schema.sql and migrated (tests/conftest.py). Pinned here:
    - the SJH database is set up like the main one and knows it serves SJH;
    - a request reads the database of the site picked in the header, only when the
      account holds that site; logins and access always come from the main database;
    - reports keep the served site's devices in each database, and cached figures are
      never shared between sites;
    - SJH's tables can point at users (a copy of the user list, without passwords);
    - streamed CSV exports read the picked site, and RH's through the RH-only views.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

BASE = 97500
DAY = date(2026, 10, 15)
PERIOD = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}


@pytest.fixture
def ids(app):
    from db import db
    with app.app_context():
        rh = db.session.execute(text("SELECT id FROM sites WHERE is_default")).scalar()
        sjh = db.session.execute(text("SELECT id FROM sites WHERE code = 'SJH'")).scalar()
    return {'RH': rh, 'SJH': sjh}


@pytest.fixture
def sjh(app, ids):
    """Statements on SJH's database, its data cleared before and after each test."""
    from utils import site_db
    engine = site_db.engine_for(ids['SJH'])

    def run(statement, **params):
        with engine.begin() as c:
            res = c.execute(text(statement), params)
            return res.fetchall() if res.returns_rows else None

    def clear():
        for t in ('worklist_versions', 'worklists', 'etl_didb_studies', 'aetitle_modality_map'):
            run(f"DELETE FROM {t}" + (" WHERE folder <> 'Examples' OR owner_id IS NOT NULL" if t == 'worklists' else ''))
    clear()
    yield run
    clear()


@pytest.fixture
def people(app, ids):
    from db import db
    made = []

    def make(name, role, sites=('RH',)):
        uid = BASE + len(made)
        with app.app_context():
            db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (:i, :n, 'x', :r)"),
                               {'i': uid, 'n': f'sd_{name}', 'r': role})
            for code in sites:
                db.session.execute(text("INSERT INTO user_sites (user_id, site_id) VALUES (:u, :s)"),
                                   {'u': uid, 's': ids[code]})
            db.session.commit()
        made.append(uid)
        from utils import site_db
        with app.app_context():
            site_db.mirror_users()
        return uid

    yield make
    with app.app_context():
        db.session.rollback()
        p = {'ids': made + [0]}
        db.session.execute(text("DELETE FROM user_audit_log WHERE actor_user_id = ANY(:ids) OR target_user_id = ANY(:ids)"), p)
        for t in ('user_page_permissions', 'report_access_control'):
            db.session.execute(text(f"DELETE FROM {t} WHERE user_id = ANY(:ids)"), p)
        db.session.execute(text("DELETE FROM users WHERE id = ANY(:ids)"), p)
        db.session.commit()
        from utils import site_db
        site_db.mirror_users()


def hdr(uid):
    return {'X-Test-Uid': str(uid)}


def pick(client, uid, site_id):
    return client.post('/site/switch', data={'site_id': site_id, 'next': '/viewer/dashboard'}, headers=hdr(uid))


def rh_study(app, uid, cls='RHCLASS', ae='CT_RH_T'):
    """A study RH's views keep: on a device mapped to RH, PACS label RH."""
    from db import db
    with app.app_context():
        db.session.execute(text("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
                                   VALUES (:ae, 'CT', rayd_default_site_id()) ON CONFLICT DO NOTHING"""), {'ae': ae})
        db.session.execute(text("""
            INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae, study_date,
                                          patient_class, patient_location, pacs_site_id_raw, insert_time,
                                          rep_study_last_composed_ts)
            VALUES (:u, :u, :a, :ae, :d, :c, 'ER', (SELECT pacs_site_id FROM sites WHERE is_default), :t, :r)"""),
            {'u': uid, 'a': f'RH{uid}', 'ae': ae, 'd': DAY, 'c': cls,
             't': datetime(2026, 10, 15, 10), 'r': datetime(2026, 10, 15, 10, 45)})
        db.session.commit()


def sjh_study(sjh, uid, cls='SJHCLASS', ae='CT_SJH_T'):
    """A study in SJH's database, on a device mapped to SJH."""
    sjh("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           SELECT :ae, 'CT', id FROM sites WHERE code = 'SJH'
             AND NOT EXISTS (SELECT 1 FROM aetitle_modality_map WHERE aetitle = :ae)""", ae=ae)
    sjh("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae, study_date,
                                         patient_class, patient_location, pacs_site_id_raw, insert_time,
                                         rep_study_last_composed_ts)
           VALUES (:u, :u, :a, :ae, :d, :c, 'ER', (SELECT pacs_site_id FROM sites WHERE code = 'SJH'), :t, :r)""",
        u=uid, a=f'SJ{uid}', ae=ae, d=DAY, c=cls, t=datetime(2026, 10, 15, 10), r=datetime(2026, 10, 15, 10, 30))


def options(client, uid):
    r = client.get('/api/filter-options', headers=hdr(uid))
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


# ── the SJH database ──────────────────────────────────────────────────────────

def test_the_sjh_database_carries_every_migration(app, ids, sjh):
    import os
    from conftest import ROOT
    files = {f for f in os.listdir(os.path.join(ROOT, 'migrations')) if f.endswith('.sql')}
    there = {r[0] for r in sjh("SELECT name FROM schema_migrations")}
    assert files and files <= there


def test_each_database_knows_the_site_it_serves(app, ids, sjh):
    from db import db
    with app.app_context():
        assert db.session.execute(text("SELECT rayd_served_site_id()")).scalar() == ids['RH']
    assert sjh("SELECT rayd_served_site_id()")[0][0] == ids['SJH']


def test_the_user_list_is_copied_without_passwords(app, people, sjh):
    uid = people('copied', 'user', ('RH', 'SJH'))
    row = sjh("SELECT username, role, password_hash FROM users WHERE id = :i", i=uid)
    assert row and tuple(row[0]) == ('sd_copied', 'user', '!')
    assert sjh("SELECT COUNT(*) FROM users WHERE password_hash <> '!'")[0][0] == 0


# ── which database a request reads ────────────────────────────────────────────

def test_an_account_with_both_sites_switches_between_them(client, app, people, sjh):
    uid = people('both', 'administrator', ('RH', 'SJH'))
    rh_study(app, BASE + 900)
    sjh_study(sjh, BASE + 901)
    assert options(client, uid)['classes'] == ['RHCLASS']           # RH by default
    assert pick(client, uid, _sjh_id(app)).status_code == 302
    assert options(client, uid)['classes'] == ['SJHCLASS']
    assert pick(client, uid, _rh_id(app)).status_code == 302
    assert options(client, uid)['classes'] == ['RHCLASS']


def test_an_account_cannot_pick_a_site_it_does_not_hold(client, app, people, sjh):
    uid = people('rh_only', 'administrator', ('RH',))
    sjh_study(sjh, BASE + 902)
    rh_study(app, BASE + 903)
    assert pick(client, uid, _sjh_id(app)).status_code == 403
    assert options(client, uid)['classes'] == ['RHCLASS']


def test_an_account_with_sjh_only_reads_sjh(client, app, people, sjh):
    uid = people('sjh_only', 'administrator', ('SJH',))
    sjh_study(sjh, BASE + 904)
    rh_study(app, BASE + 905)
    assert options(client, uid)['classes'] == ['SJHCLASS']


def test_the_header_offers_the_switch_only_with_two_sites(client, app, people):
    both = people('hdr_both', 'administrator', ('RH', 'SJH'))
    one = people('hdr_one', 'administrator', ('RH',))
    page = client.get('/report/38', headers=hdr(both)).get_data(as_text=True)
    assert 'data-site-switch' in page and 'SJH ·' in page
    assert 'data-site-switch' not in client.get('/report/38', headers=hdr(one)).get_data(as_text=True)


def test_logins_and_access_come_from_the_main_database_on_sjh(client, app, people, sjh, ids):
    """A User's report tick lives on the main database, per site: ticked for SJH, Report 38
    opens on SJH and not on RH."""
    uid = people('sjh_reader', 'user', ('RH', 'SJH'))
    from db import db
    with app.app_context():
        db.session.execute(text("""INSERT INTO report_access_control (user_id, site_id, report_template_id, is_enabled)
                                   VALUES (:u, :s, 38, TRUE)"""), {'u': uid, 's': ids['SJH']})
        db.session.commit()
    assert client.get('/report/38', headers=hdr(uid)).status_code == 403          # RH: not ticked
    pick(client, uid, ids['SJH'])
    assert client.get('/report/38', headers=hdr(uid)).status_code == 200          # SJH: ticked
    assert sjh("SELECT COUNT(*) FROM report_access_control")[0][0] == 0           # access lives on main only


def test_a_switch_is_recorded_on_the_main_database(client, app, people, ids):
    uid = people('audited', 'administrator', ('RH', 'SJH'))
    pick(client, uid, ids['SJH'])
    from db import db
    with app.app_context():
        got = db.session.execute(text("""SELECT detail->>'site' FROM user_audit_log
                                         WHERE actor_user_id = :u AND action = 'site_switched'"""), {'u': uid}).scalar()
    assert got == 'SJH'


# ── reports in each database ──────────────────────────────────────────────────

def test_a_report_keeps_the_served_sites_devices(client, app, people, sjh, ids):
    """Report 22 keeps the studies of the served site's devices (m.site_id = the served
    site). In SJH's database that is SJH: a study on a device its migrations mapped to RH
    counts for nothing there."""
    import json
    import re
    uid = people('rep', 'administrator', ('RH', 'SJH'))
    rh_study(app, BASE + 910)
    rh_study(app, BASE + 911)
    sjh_study(sjh, BASE + 912)
    sjh("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id) VALUES ('CT_RH_T', 'CT', rayd_default_site_id())""")
    sjh("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae, study_date,
                                         patient_location, pacs_site_id_raw, insert_time)
           VALUES (:u, :u, 'X1', 'CT_RH_T', :d, 'ER', '0', :t)""", u=BASE + 913, d=DAY, t=datetime(2026, 10, 15, 9))

    def total():
        r = client.get('/report/22', headers=hdr(uid), query_string=PERIOD)
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        return sum(json.loads(re.search(r'const d = (.*);', r.get_data(as_text=True)).group(1))['stat_c'].values())
    assert total() == 2
    pick(client, uid, ids['SJH'])
    assert total() == 1


def test_cached_figures_are_never_shared_between_sites(client, app, people, sjh, ids):
    """The dropdown values are cached for 5 minutes, now per site."""
    uid = people('cache', 'administrator', ('RH', 'SJH'))
    rh_study(app, BASE + 920)
    sjh_study(sjh, BASE + 921)
    first = options(client, uid)
    pick(client, uid, ids['SJH'])
    second = options(client, uid)
    assert first['classes'] == ['RHCLASS'] and second['classes'] == ['SJHCLASS']
    assert 'CT_SJH_T' in second['aetitles'] and 'CT_SJH_T' not in first['aetitles']


def test_a_streamed_export_reads_the_picked_site(client, app, people, sjh, ids):
    """Report 22's CSV streams after the view returns; it reads the picked site, and on RH
    the RH-only views (it used to stream outside its request, past both)."""
    uid = people('csv', 'administrator', ('RH', 'SJH'))
    rh_study(app, BASE + 930)
    sjh_study(sjh, BASE + 931)
    body = {**PERIOD}

    def rows():
        r = client.post('/report/22/export', headers=hdr(uid), data=body)
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        return r.get_data(as_text=True).strip().splitlines()[1:]
    rh_rows = rows()
    pick(client, uid, ids['SJH'])
    sjh_rows = rows()
    assert len(rh_rows) == 1 and 'RHCLASS' in rh_rows[0]
    assert len(sjh_rows) == 1 and 'SJHCLASS' in sjh_rows[0]


# ── SJH's own tables point at users ───────────────────────────────────────────

def test_a_report_saved_on_sjh_is_kept_in_sjh(client, app, people, sjh, ids):
    """Report Composer's saved reports live in the site's database and point at the
    account that saved them (worklists.owner_id -> users): the copy of users holds that."""
    uid = people('composer', 'administrator', ('RH', 'SJH'))
    pick(client, uid, ids['SJH'])
    body = {'name': 'SJH list', 'folder': 'Mine', 'visibility': 'personal',
            'definition': {'dataset': 'studies', 'criteria': {}, 'output': {'kind': 'list', 'columns': ['accession']}}}
    r = client.post('/worklists/api/save', json=body, headers=hdr(uid))
    assert r.status_code == 200, r.get_data(as_text=True)
    assert sjh("SELECT owner_id FROM worklists WHERE name = 'SJH list'")[0][0] == uid
    from db import db
    with app.app_context():
        assert db.session.execute(text("SELECT COUNT(*) FROM worklists WHERE name = 'SJH list'")).scalar() == 0


# ── outside a request ─────────────────────────────────────────────────────────

def test_background_work_reads_the_main_database_unless_told(app, ids, sjh):
    from db import db
    from utils import site_db
    with app.app_context():
        assert db.session.execute(text("SELECT rayd_served_site_id()")).scalar() == ids['RH']
        db.session.rollback()
        with site_db.use_site(ids['SJH']):
            assert db.session.execute(text("SELECT rayd_served_site_id()")).scalar() == ids['SJH']
        db.session.rollback()


def test_a_main_table_stays_on_the_main_database_inside_sjh(app, ids):
    from db import User, db
    from utils import site_db
    with app.app_context():
        with site_db.use_site(ids['SJH']):
            assert db.session.query(User).filter_by(username='admin').one().role == 'superuser'
            assert site_db.route(db.engine, mapper=User) is db.engine
        db.session.rollback()


def test_without_a_site_database_everything_reads_the_main_one(app, ids, monkeypatch):
    from db import db
    from utils import site_db
    monkeypatch.setattr(site_db, '_engines', {})
    with app.app_context():
        with site_db.use_site(ids['SJH']):
            assert db.session.execute(text("SELECT rayd_served_site_id()")).scalar() == ids['RH']
        db.session.rollback()
    assert site_db.database_site_ids() == {ids['RH']}


def _rh_id(app):
    from db import db
    with app.app_context():
        return db.session.execute(text("SELECT id FROM sites WHERE is_default")).scalar()


def _sjh_id(app):
    from db import db
    with app.app_context():
        return db.session.execute(text("SELECT id FROM sites WHERE code = 'SJH'")).scalar()
