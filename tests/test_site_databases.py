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
        for t in ('worklist_email_runs', 'worklist_email_schedules', 'worklist_versions', 'worklists',
                  'etl_didb_studies', 'aetitle_modality_map'):
            run(f"DELETE FROM {t}" + (" WHERE folder <> 'Examples' OR owner_id IS NOT NULL" if t == 'worklists' else ''))
        run("DELETE FROM settings WHERE key LIKE 'smtp\_%'")
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


def test_a_gateway_study_counts_on_its_scanner_in_report_25(app, sjh, ids):
    """Item 13's device rule (migration 0164). SJH's studies come through its middleware,
    mapped as modality 'PACS': Report 25 put every SJH study on that one device under modality
    PACS (and its turnaround sections leave PACS out). With the scanner as the study's device
    (storing_ae) it counts on the scanner, under the scanner's modality."""
    pytest.importorskip('pandas')
    from werkzeug.datastructures import MultiDict
    from db import db
    from utils import site_db
    from ETL_JOBS.etl_didb_studies import apply_gateway_devices
    from routes.report_25 import get_gold_standard_data
    sjh("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           SELECT v.ae, v.modality, s.id FROM sites s, (VALUES ('SJHCSAPWFMFIR', 'PACS'), ('MR_SJH_T', 'MR')) v(ae, modality)
           WHERE s.code = 'SJH'""")
    arrived = datetime.now().replace(microsecond=0) - timedelta(minutes=30)
    sjh("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                original_storing_ae, study_date, study_modality, procedure_code, patient_class,
                patient_location, insert_time, study_status, pacs_site_id_raw,
                rep_study_last_composed_by, rep_study_last_composed_ts)
           VALUES (:u, :u, '1.2.3.97', 'SJGW1', 'SJHCSAPWFMFIR', 'MR_SJH_T', :d, 'MR', 'MR01', 'O',
                   'DEF', :t, 'UNREAD', (SELECT pacs_site_id FROM sites WHERE code = 'SJH'),
                   'dr.sjh@ad', :r)""",
        u=BASE + 990, d=arrived.date(), t=arrived, r=arrived + timedelta(minutes=10))
    form = MultiDict([('start_date', arrived.date().isoformat()), ('end_date', arrived.date().isoformat())])

    def report_25():
        from routes import report_cache
        with report_cache._lock:
            report_cache._store.clear()              # results are cached for 5 minutes
        with app.app_context(), site_db.use_site(ids['SJH']):
            data = get_gold_standard_data(form)[0]
            db.session.rollback()
        return {r['ae'] for r in data['matrix']}, data['unread_aging']

    assert report_25() == ({'SJHCSAPWFMFIR'}, [{'bucket': '0-24h', 'modality': 'PACS', 'cnt': 1}])
    with site_db.engine_for(ids['SJH']).begin() as conn:
        assert apply_gateway_devices(conn) == 1
    assert report_25() == ({'MR_SJH_T'}, [{'bucket': '0-24h', 'modality': 'MR', 'cnt': 1}])


def test_the_sjh_database_reads_its_own_rows_through_the_same_rules(client, app, people, sjh, ids):
    """Migration 0165: the rh views keep the site each database serves. In SJH's database they
    kept RH (so Report Composer saw none of SJH's studies) and requests read the real tables,
    without the rules: test patients (BRAYN's list, copied from the main database), the HIS's
    duplicate orders, another site's rows."""
    from db import db
    from utils import site_db
    uid = people('rules', 'administrator', ('RH', 'SJH'))
    with app.app_context():
        db.session.execute(text("INSERT INTO brayn_placeholders (kind, code, label) VALUES ('patient', 'TESTPAT_SJ', 't')"))
        db.session.commit()
        site_db.sync_shared_tables()
    try:
        sjh_study(sjh, BASE + 930)                                   # an SJH study
        sjh("INSERT INTO etl_patient_view (patient_db_uid, id) VALUES (:p, 'TESTPAT_SJ')", p=BASE + 931)
        sjh_study(sjh, BASE + 931)                                   # the test patient's
        sjh_study(sjh, BASE + 932)
        sjh("UPDATE etl_didb_studies SET site_id = rayd_default_site_id() WHERE study_db_uid = :u", u=BASE + 932)
        yesterday = datetime.now() - timedelta(days=1)
        for key, issuer, status in ((BASE + 940, 'SAP_SJH', 'CM'), (BASE + 941, 'SAP_SJH', 'scheduled'),
                                    (BASE + 942, 'SAP_PROD', 'CM')):
            sjh("""INSERT INTO etl_orders (order_dbid, order_control, order_status, scheduled_datetime)
                   VALUES (:k, :i, :s, :t)""", k=key, i=issuer, s=status, t=yesterday)

        seen = [r[0] for r in sjh("SELECT study_db_uid FROM rh.etl_didb_studies ORDER BY 1")]
        assert seen == [BASE + 930]                     # not the test patient's, not RH's
        assert [r[0] for r in sjh("SELECT accession FROM rpt.studies")] == [f'SJ{BASE + 930}']
        assert [r[0] for r in sjh("SELECT order_dbid FROM rh.etl_orders ORDER BY 1")] == [BASE + 940]

        # a request on SJH reads through them: Report 22 counts the one SJH study
        import json
        import re
        pick(client, uid, ids['SJH'])
        r = client.get('/report/22', headers=hdr(uid), query_string=PERIOD)
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        assert sum(json.loads(re.search(r'const d = (.*);', r.get_data(as_text=True)).group(1))['stat_c'].values()) == 1
    finally:
        sjh("DELETE FROM etl_orders WHERE order_dbid BETWEEN :a AND :b", a=BASE + 940, b=BASE + 942)
        sjh("DELETE FROM etl_patient_view WHERE patient_db_uid = :p", p=BASE + 931)
        with app.app_context():
            db.session.execute(text("DELETE FROM brayn_placeholders WHERE code = 'TESTPAT_SJ'"))
            db.session.commit()
            site_db.sync_shared_tables()


def test_the_brayn_console_reads_the_main_database_whatever_site_is_picked(client, app, people, sjh, ids):
    """BRAYN's tables, its switch and every HL7 message live on the main database until BRAYN
    routes by site (item 13d): with SJH picked, the console must not read SJH's empty copies."""
    from db import db
    uid = people('cht_both', 'cht', ('RH', 'SJH'))
    pick(client, uid, ids['SJH'])
    try:
        r = client.post('/brayn/switch', headers=hdr(uid), json={'enabled': True})
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        with app.app_context():
            assert db.session.execute(text("SELECT value FROM settings WHERE key = 'brayn_enabled'")).scalar() == 'true'
        assert (sjh("SELECT value FROM settings WHERE key = 'brayn_enabled'") or [[None]])[0][0] != 'true'
        assert client.get('/brayn', headers=hdr(uid)).status_code == 200
    finally:
        with app.app_context():
            db.session.execute(text("UPDATE settings SET value = 'false' WHERE key = 'brayn_enabled'"))
            db.session.commit()
        sjh("UPDATE settings SET value = 'false' WHERE key = 'brayn_enabled'")


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


# ── RH and SJH side by side (item 13b) ────────────────────────────────────────

def test_the_super_report_puts_the_sites_side_by_side_per_modality(client, app, people, sjh, ids):
    uid = people('compare', 'administrator', ('RH', 'SJH'))
    rh_study(app, BASE + 940)
    rh_study(app, BASE + 941)
    sjh_study(sjh, BASE + 942)
    r = client.get('/viewer/super-report/sites', headers=hdr(uid),
                   query_string={'start': '2026-10-01', 'end': '2026-10-31'})
    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert [s['code'] for s in d['sites']] == ['RH', 'SJH']
    ct = next(m for m in d['modalities'] if m['modality'] == 'CT')['by_site']
    assert (ct['RH']['studies'], ct['SJH']['studies']) == (2, 1)
    assert (ct['RH']['median_tat_min'], ct['SJH']['median_tat_min']) == (45.0, 30.0)
    assert d['totals']['RH']['studies'] == 2 and d['totals']['SJH']['studies'] == 1


def test_the_side_by_side_is_for_accounts_holding_both_sites(client, app, people):
    one = people('one_site', 'administrator', ('RH',))
    both = people('two_sites', 'administrator', ('RH', 'SJH'))
    q = {'start': '2026-10-01', 'end': '2026-10-31'}
    assert client.get('/viewer/super-report/sites', headers=hdr(one), query_string=q).get_json()['sites'] == []
    assert 'id="super-sites-card"' not in client.get('/viewer/super-report-page', headers=hdr(one)).get_data(as_text=True)
    assert 'id="super-sites-card"' in client.get('/viewer/super-report-page', headers=hdr(both)).get_data(as_text=True)


def test_the_live_board_puts_the_sites_side_by_side_per_modality(client, app, people, ids):
    """Each hospital's RIS rooms, from BRAYN in the main database (item 38: BRAYN's tables stay
    there until item 13d), summarised per modality."""
    from datetime import datetime
    from db import db
    uid = people('board', 'administrator', ('RH', 'SJH'))
    with app.app_context():
        for key, (ae, code, site) in enumerate((('BRDCT_RH', 'BRD CT RH', 'RH'), ('BRDCT_SJH', 'SJH-BRD CT', 'SJH'))):
            db.session.execute(text("""INSERT INTO aetitle_modality_map (aetitle, modality, room_code, ris_modality_key, site_id)
                                       VALUES (:ae, 'CT', :code, :k, :s)"""),
                               {'ae': ae, 'code': code, 'k': 990900 + key, 's': ids[site]})
            db.session.execute(text("""INSERT INTO brayn_study_state (accession_number, ris_room, appointment_at,
                                           current_rank, event_count, is_closed, first_seen_at, updated_at)
                                       VALUES (:a, :r, :t, 40, 1, FALSE, now(), now())"""),
                               {'a': 'ACC-' + ae, 'r': code, 't': datetime.now()})
        db.session.commit()
    r = client.get('/viewer/department/api/sites', headers=hdr(uid))
    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert [s['code'] for s in d['sites']] == ['RH', 'SJH']
    ct = next(m for m in d['modalities'] if m['modality'] == 'CT')['by_site']
    assert ct['RH']['rooms'] == 1 and ct['SJH']['rooms'] == 1
    assert ct['RH']['scheduled_today'] == 1 and ct['SJH']['scheduled_today'] == 1
    assert set(d['totals']) == {'RH', 'SJH'}


def test_the_board_offers_the_sites_only_with_two_sites(client, app, people):
    one = people('board_one', 'administrator', ('RH',))
    both = people('board_two', 'administrator', ('RH', 'SJH'))
    assert client.get('/viewer/department/api/sites', headers=hdr(one)).get_json()['sites'] == []
    assert [s['code'] for s in client.get('/viewer/department/api/board', headers=hdr(one)).get_json()['sites']] == ['RH']
    assert [s['code'] for s in client.get('/viewer/department/api/board', headers=hdr(both)).get_json()['sites']] == ['RH', 'SJH']


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


# ── scheduled work per site ───────────────────────────────────────────────────

def test_a_job_runs_once_per_database(app, ids):
    from db import db
    from utils import site_db
    seen = []

    def job(site_id):
        seen.append((site_id, db.session.execute(text("SELECT rayd_served_site_id()")).scalar()))
        if site_id == ids['RH']:
            raise RuntimeError('one site failing does not stop the next')
        return 'done'
    with app.app_context():
        out = site_db.run_per_site('test job', job)
    assert seen == [(ids['RH'], ids['RH']), (ids['SJH'], ids['SJH'])]
    assert out == {ids['SJH']: 'done'}


def test_an_sjh_schedule_is_sent_through_sjhs_mail_server(client, app, people, sjh, ids):
    """Report Composer schedules live in each site's database, with that site's mail server
    (meeting of 2026-11-13, item 13g: one mail server per site). RH has none set here, so
    only SJH sends, and the run is recorded in SJH's database."""
    from test_worklist_email import Relay
    from utils import site_db
    from utils.worklist_email import run_due
    relay = Relay()
    try:
        for k, v in (('smtp_host', '127.0.0.1'), ('smtp_port', str(relay.port)), ('smtp_security', 'none'),
                     ('smtp_from', 'rayd@sjh.test'), ('smtp_allowed_domains', 'sjh.test')):
            sjh("INSERT INTO settings (key, value) VALUES (:k, :v) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", k=k, v=v)
        uid = people('mailer', 'administrator', ('RH', 'SJH'))
        pick(client, uid, ids['SJH'])
        wid = client.post('/worklists/api/save', headers=hdr(uid), json={
            'name': 'SJH daily', 'folder': 'Mail', 'visibility': 'shared',
            'definition': {'dataset': 'studies', 'criteria': {}, 'output': {'kind': 'list', 'columns': ['accession']}},
        }).get_json()['id']
        r = client.post('/worklists/api/schedules/save', headers=hdr(uid), json={
            'name': 'SJH pack', 'recipients': 'head@sjh.test', 'frequency': 'daily',
            'send_time': '07:00', 'worklist_ids': [wid]})
        assert r.status_code == 200, r.get_data(as_text=True)
        sjh("UPDATE worklist_email_schedules SET next_run_at = :t", t=datetime(2020, 1, 1, 7))
        with app.app_context():
            sent = site_db.run_per_site('Report Composer emails', lambda _s: run_due())
        assert sent == {ids['RH']: 0, ids['SJH']: 1}
        assert len(relay.mails) == 1
        assert sjh("SELECT status FROM worklist_email_runs")[0][0] == 'sent'
    finally:
        relay.stop()


# ── procedure codes, shared by both sites ─────────────────────────────────────

@pytest.fixture
def procedure(app, ids):
    """One procedure on the main database, the copies in line before the test."""
    from db import db
    from utils import site_db
    with app.app_context():
        db.session.execute(text("""INSERT INTO procedure_duration_map (procedure_code, procedure_name, duration_minutes)
                                   VALUES ('SD-CT01', 'CT test', 15)"""))
        db.session.commit()
        site_db.sync_shared_tables()
    yield 'SD-CT01'
    with app.app_context():
        db.session.execute(text("DELETE FROM procedure_duration_map WHERE procedure_code = 'SD-CT01'"))
        db.session.commit()
        site_db.sync_shared_tables()


def test_the_sjh_copy_of_procedure_codes_matches_the_main_one(app, ids, sjh, procedure):
    from utils import site_db
    assert sjh("SELECT procedure_name, duration_minutes FROM procedure_duration_map "
               "WHERE procedure_code = 'SD-CT01'") == [('CT test', 15)]
    with app.app_context():
        assert site_db.sync_shared_tables() == {ids['SJH']: False}     # nothing to copy


def test_a_procedure_changed_on_sjh_is_changed_for_both_sites(client, app, people, sjh, ids, procedure):
    """Mapping's procedure edits write the main database, whatever site is picked, and
    SJH's copy follows at once."""
    uid = people('mapper', 'cht', ('RH', 'SJH'))
    pick(client, uid, ids['SJH'])
    r = client.post('/mapping/procedure/update', json={'code': 'sd-ct01', 'duration': 25}, headers=hdr(uid))
    assert r.status_code == 200, r.get_data(as_text=True)
    from db import db
    with app.app_context():
        assert db.session.execute(text("SELECT duration_minutes FROM procedure_duration_map "
                                       "WHERE procedure_code = 'SD-CT01'")).scalar() == 25
    assert sjh("SELECT duration_minutes FROM procedure_duration_map WHERE procedure_code = 'SD-CT01'") == [(25,)]


def test_a_copy_that_drifted_is_brought_back(app, ids, sjh, procedure):
    from utils import site_db
    sjh("UPDATE procedure_duration_map SET duration_minutes = 99 WHERE procedure_code = 'SD-CT01'")
    sjh("INSERT INTO procedure_duration_map (procedure_code, duration_minutes) VALUES ('SD-STRAY', 5)")
    with app.app_context():
        assert site_db.sync_shared_tables() == {ids['SJH']: True}
    assert sjh("SELECT duration_minutes FROM procedure_duration_map WHERE procedure_code = 'SD-CT01'") == [(15,)]
    assert sjh("SELECT COUNT(*) FROM procedure_duration_map WHERE procedure_code = 'SD-STRAY'")[0][0] == 0


def test_procedure_codes_read_through_the_model_come_from_the_main_database(app, ids, sjh, procedure):
    from db import ProcedureDurationMap, db
    from utils import site_db
    sjh("UPDATE procedure_duration_map SET duration_minutes = 77 WHERE procedure_code = 'SD-CT01'")
    with app.app_context():
        with site_db.use_site(ids['SJH']):
            row = ProcedureDurationMap.query.filter_by(procedure_code='SD-CT01').one()
            assert row.duration_minutes == 15
        db.session.rollback()


# ── outside a request ─────────────────────────────────────────────────────────

def test_the_site_map_loads_with_a_site_database_present(app, ids, caplog):
    """Found starting the real stack: the site map was read through the routed session,
    whose routing asks for the default site, which reads the site map: it recursed about a
    thousand levels deep ("maximum recursion depth exceeded"), each reload of the map."""
    import logging
    from utils import site_resolver
    caplog.set_level(logging.ERROR, logger='SITE_RESOLVER')
    site_resolver.invalidate_cache()
    with app.app_context():
        assert site_resolver.default_site() == ids['RH']
    site_resolver.invalidate_cache()
    with app.test_request_context('/'):
        assert site_resolver.default_site() == ids['RH']
    assert not [r for r in caplog.records if 'site map load failed' in r.getMessage()]


def test_a_failed_site_map_load_is_not_kept(app, ids):
    """Outside an app context the map cannot load; that must not stick for 5 minutes."""
    from utils import site_resolver
    site_resolver.invalidate_cache()
    assert site_resolver.default_site() is None               # no app context: nothing to read
    with app.app_context():
        assert site_resolver.default_site() == ids['RH']       # the next call loads it


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


# ── the setup lock (2026-11-21: the SJH move stopped, "SJH's database not ready") ──

def test_the_setup_lock_is_its_own_not_the_etls():
    """It was 872951065, the nightly ETL's lock (also the site route's while it copies): an app
    starting with SJH on waited for as long as either ran."""
    import db_migrations
    from ETL_JOBS import etl_runner, etl_site_route
    from utils import site_db
    assert site_db._BOOT_LOCK_ID not in (etl_runner._ETL_SYNC_LOCK_ID, etl_site_route._ETL_SYNC_LOCK_ID,
                                         db_migrations._MIGRATION_LOCK_ID)


def test_a_running_etl_does_not_hold_the_setup(app, ids):
    from db import db
    from utils import site_db
    from ETL_JOBS.etl_runner import _ETL_SYNC_LOCK_ID
    url = site_db.engine_for(ids['SJH']).url
    with app.app_context(), db.engine.connect() as etl:
        assert etl.execute(text("SELECT pg_try_advisory_lock(:k)"), {'k': _ETL_SYNC_LOCK_ID}).scalar()
        try:
            site_db._bootstrap(url, 'SJH')                      # returns: nothing waits on the ETL
        finally:
            etl.execute(text("SELECT pg_advisory_unlock(:k)"), {'k': _ETL_SYNC_LOCK_ID})
            etl.commit()


def test_a_start_gives_up_on_a_held_setup_lock_and_says_who_holds_it(app, ids, monkeypatch, caplog):
    import logging
    from db import db
    from utils import site_db
    monkeypatch.setattr(site_db, '_BOOT_WAIT_S', 3)
    url = site_db.engine_for(ids['SJH']).url
    with app.app_context(), db.engine.connect() as other:
        other.execute(text("SELECT pg_advisory_lock(:k)"), {'k': site_db._BOOT_LOCK_ID})
        other.commit()
        try:
            with caplog.at_level(logging.WARNING, logger='SITE_DB'):
                with pytest.raises(RuntimeError, match='still held'):
                    site_db._bootstrap(url, 'SJH')
            assert 'held by pid' in caplog.text
        finally:
            other.execute(text("SELECT pg_advisory_unlock(:k)"), {'k': site_db._BOOT_LOCK_ID})
            other.commit()


def test_the_move_script_keeps_the_apps_log_when_sjh_is_not_ready():
    import os
    src = open(os.path.join(os.path.dirname(__file__), '..', 'scripts', 'sjh_move.sh'), encoding='utf-8').read()
    assert 'up -d --force-recreate rayd-app' in src
    assert 'not set up"; then' in src and src.count('app_log; switch_off') == 2
