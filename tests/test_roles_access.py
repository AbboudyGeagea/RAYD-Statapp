"""
tests/test_roles_access.py — RAYD's new roles and access per site (meeting of 2026-11-13,
item 13f; utils/roles.py, migration 0152).

    superuser      Super user     everything, every site, every account
    cht            CHT            implementation: mapping, ETL, BRAYN console (+ settings)
    administrator  Administrator  the users and settings of the sites granted
    user           User           the pages and reports ticked for it, site by site

Pinned here: what each role may do; that migration 0152 renamed every account and kept
every page; that a User's access is per site and needs the site granted; that an
Administrator manages only Users and Administrators of its own sites and gives only its own
sites and those two roles; and that no code compares role names any more.
"""
import os
import re

import pytest
from sqlalchemy import text

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = 97100        # ids of the accounts these tests make


def hdr(uid):
    """Sign in as a real account (its role read from users), as the app does."""
    return {'X-Test-Uid': str(uid)}


@pytest.fixture
def sites(app):
    from db import db
    with app.app_context():
        rh = db.session.execute(text("SELECT id FROM sites WHERE is_default")).scalar()
        sjh = db.session.execute(text("SELECT id FROM sites WHERE code = 'SJH'")).scalar()
    return {'RH': rh, 'SJH': sjh}


@pytest.fixture
def people(app, sites):
    """Accounts with their sites. Users are not cleared between tests, so these are removed
    after each one, with everything that points at them."""
    from db import db
    made = []

    def make(name, role, site_codes=('RH',), status='active'):
        uid = BASE + len(made)
        with app.app_context():
            db.session.execute(text("""
                INSERT INTO users (id, username, password_hash, role, status)
                VALUES (:id, :n, 'x', :r, :s)"""), {'id': uid, 'n': f'rt_{name}', 'r': role, 's': status})
            for code in site_codes:
                db.session.execute(text("INSERT INTO user_sites (user_id, site_id) VALUES (:u, :s)"),
                                   {'u': uid, 's': sites[code]})
            db.session.commit()
        made.append(uid)
        return uid

    yield make
    with app.app_context():
        db.session.rollback()
        ids = {'ids': made + [0]}
        db.session.execute(text("DELETE FROM user_audit_log WHERE actor_user_id = ANY(:ids) OR target_user_id = ANY(:ids)"), ids)
        extra = [r[0] for r in db.session.execute(text("SELECT id FROM users WHERE username LIKE 'rt\\_new%'"))]
        ids2 = {'ids': made + extra + [0]}
        db.session.execute(text("DELETE FROM user_audit_log WHERE actor_user_id = ANY(:ids) OR target_user_id = ANY(:ids)"), ids2)
        for t in ('user_page_permissions', 'report_access_control', 'active_sessions'):
            db.session.execute(text(f"DELETE FROM {t} WHERE user_id = ANY(:ids)"), ids2)
        db.session.execute(text("DELETE FROM users WHERE id = ANY(:ids)"), ids2)
        db.session.commit()


def tick(app, uid, site_id, page=None, report=None, on=True):
    from db import db
    with app.app_context():
        if page:
            db.session.execute(text("""
                INSERT INTO user_page_permissions (user_id, site_id, page_key, is_enabled) VALUES (:u, :s, :k, :on)
                ON CONFLICT (user_id, site_id, page_key) DO UPDATE SET is_enabled = EXCLUDED.is_enabled"""),
                {'u': uid, 's': site_id, 'k': page, 'on': on})
        if report:
            db.session.execute(text("""
                INSERT INTO report_access_control (user_id, site_id, report_template_id, is_enabled) VALUES (:u, :s, :r, :on)
                ON CONFLICT (user_id, site_id, report_template_id) DO UPDATE SET is_enabled = EXCLUDED.is_enabled"""),
                {'u': uid, 's': site_id, 'r': report, 'on': on})
        db.session.commit()


# ── what each role may do ─────────────────────────────────────────────────────

class _U:
    is_authenticated = True

    def __init__(self, role, uid=1):
        self.role, self.id = role, uid


@pytest.mark.parametrize('role, implementation, settings, users, all_pages, all_sites', [
    ('superuser',     True,  True,  True,  True,  True),
    ('cht',           True,  True,  False, True,  False),
    ('administrator', False, True,  True,  True,  False),
    ('user',          False, False, False, False, False),
])
def test_what_each_role_may_do(role, implementation, settings, users, all_pages, all_sites):
    from utils.roles import can
    u = _U(role)
    assert (can(u, 'implementation'), can(u, 'settings'), can(u, 'users'),
            can(u, 'all_pages'), can(u, 'all_sites')) == (implementation, settings, users, all_pages, all_sites)


@pytest.mark.parametrize('legacy, now', [('admin', 'administrator'), ('viewer', 'user'), ('viewer2', 'user'),
                                         ('tec', 'user'), ('finance', 'user'), ('secretary', 'user'),
                                         (' Admin ', 'administrator')])
def test_an_old_role_name_reads_as_its_new_role(legacy, now):
    """A row the migration did not reach must not lock anyone out (the HL7 branch's db4ce865)."""
    from utils.roles import role_of
    assert role_of(_U(legacy)) == now


def test_an_unknown_role_holds_nothing():
    from utils.roles import can, role_of
    u = _U('report_admin')
    assert role_of(u) is None
    assert not any(can(u, c) for c in ('implementation', 'settings', 'users', 'all_pages', 'all_sites'))


def test_a_misspelt_capability_fails_loudly():
    from utils.roles import can
    with pytest.raises(KeyError):
        can(_U('superuser'), 'implementaton')


def test_who_may_give_which_role():
    from utils.roles import assignable_roles
    assert assignable_roles(_U('superuser')) == ('superuser', 'cht', 'administrator', 'user')
    assert assignable_roles(_U('administrator')) == ('administrator', 'user')
    assert assignable_roles(_U('cht')) == ()
    assert assignable_roles(_U('user')) == ()


# ── migration 0152 on the accounts init-db/schema.sql seeds ───────────────────

def test_migration_renamed_every_account(app):
    from db import db
    with app.app_context():
        roles = dict(db.session.execute(text(
            "SELECT username, role FROM users WHERE username IN ('admin', 'administrator', 'viewer', 'role', 'roles')")).fetchall())
        assert roles == {'admin': 'superuser', 'administrator': 'administrator',
                         'viewer': 'user', 'role': 'user', 'roles': 'user'}
        assert db.session.execute(text(
            "SELECT COUNT(*) FROM users WHERE role NOT IN ('superuser', 'cht', 'administrator', 'user')")).scalar() == 0


def test_the_database_refuses_an_old_role_name(app):
    from db import db
    with app.app_context():
        with pytest.raises(Exception):
            db.session.execute(text("INSERT INTO users (username, password_hash, role) VALUES ('rt_legacy', 'x', 'viewer')"))
            db.session.commit()
        db.session.rollback()


def test_migration_kept_a_viewers_pages_and_reports_on_rh(app, sites):
    """viewer had every page by default, plus Mapping and the CRN board by role; its reports
    were granted by the Viewer Dashboard on each visit. Now all of it is written out, on RH."""
    from db import db
    with app.app_context():
        uid = db.session.execute(text("SELECT id FROM users WHERE username = 'viewer'")).scalar()
        assert db.session.execute(text("SELECT array_agg(site_id) FROM user_sites WHERE user_id = :u"),
                                  {'u': uid}).scalar() == [sites['RH']]
        pages = dict(db.session.execute(text(
            "SELECT page_key, is_enabled FROM user_page_permissions WHERE user_id = :u AND site_id = :s"),
            {'u': uid, 's': sites['RH']}).fetchall())
        assert pages['mapping'] and pages['crn'] and pages['oru'] and pages['live_feed']
        missing = db.session.execute(text("""
            SELECT COUNT(*) FROM report_template t
            WHERE t.is_base AND t.report_id < 39 AND NOT EXISTS (
                SELECT 1 FROM report_access_control a
                WHERE a.user_id = :u AND a.site_id = :s AND a.report_template_id = t.report_id)"""),
            {'u': uid, 's': sites['RH']}).scalar()
        assert missing == 0


def test_migration_gave_a_tec_only_its_pages_and_no_reports(app, sites):
    """A tec never reached the Viewer Dashboard, so it had no reports; its pages were Live
    and HL7 Orders. The migration's own statements, run on one tec, keep exactly that."""
    from db import db
    path = os.path.join(ROOT, 'migrations', '0152_roles_and_site_access.sql')
    sql = open(path, encoding='utf-8').read()
    pages_stmt = sql[sql.index('INSERT INTO user_page_permissions (user_id, site_id, page_key, is_enabled)'):]
    pages_stmt = pages_stmt[:pages_stmt.index('EXCLUDED.is_enabled;') + len('EXCLUDED.is_enabled')]
    reports_stmt = sql[sql.index('INSERT INTO report_access_control (user_id, site_id, report_template_id, is_enabled)'):]
    reports_stmt = reports_stmt[:reports_stmt.index('DO NOTHING;') + len('DO NOTHING')]
    with app.app_context():
        db.session.execute(text("ALTER TABLE users DROP CONSTRAINT users_role_check"))
        try:
            db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (97190, 'rt_tec', 'x', 'tec')"))
            db.session.execute(text("""INSERT INTO user_page_permissions (user_id, site_id, page_key, is_enabled)
                                       VALUES (97190, rayd_default_site_id(), 'oru', TRUE)"""))
            db.session.execute(text(pages_stmt))
            db.session.execute(text(reports_stmt))
            pages = dict(db.session.execute(text(
                "SELECT page_key, is_enabled FROM user_page_permissions WHERE user_id = 97190")).fetchall())
            reports = db.session.execute(text(
                "SELECT COUNT(*) FROM report_access_control WHERE user_id = 97190")).scalar()
        finally:
            db.session.rollback()
    assert {k for k, on in pages.items() if on} == {'live_feed', 'hl7_orders', 'oru'}   # the explicit tick kept
    assert pages['mapping'] is False and pages['crn'] is False
    assert reports == 0


# ── a User's access: per site, and only with the site ─────────────────────────

def test_a_user_opens_only_the_pages_ticked_for_it(client, app, people, sites):
    u = people('reader', 'user')
    assert client.get('/mapping', headers=hdr(u)).status_code == 403
    tick(app, u, sites['RH'], page='mapping')
    assert client.get('/mapping', headers=hdr(u)).status_code == 200


def test_a_tick_on_another_site_opens_nothing_here(client, app, people, sites):
    """This database serves RH: a page ticked for SJH only does not open here."""
    u = people('sjh_reader', 'user', ('RH', 'SJH'))
    tick(app, u, sites['SJH'], page='mapping')
    assert client.get('/mapping', headers=hdr(u)).status_code == 403


def test_a_tick_without_the_site_opens_nothing(client, app, people, sites):
    """Taking RH away keeps the ticks, unread: the account holds nothing on RH."""
    u = people('no_site', 'user', ())
    tick(app, u, sites['RH'], page='mapping')
    assert client.get('/mapping', headers=hdr(u)).status_code == 403


def test_a_user_opens_only_the_reports_ticked_for_it(client, app, people, sites):
    u = people('report_reader', 'user')
    assert client.get('/report/38', headers=hdr(u)).status_code == 403
    tick(app, u, sites['RH'], report=38)
    assert client.get('/report/38', headers=hdr(u)).status_code == 200
    tick(app, u, sites['RH'], report=38, on=False)
    assert client.get('/report/38', headers=hdr(u)).status_code == 403


def test_super_user_cht_and_administrator_open_every_page_and_report(client, people):
    for role in ('superuser', 'cht', 'administrator'):
        u = people(role + '_all', role)
        assert client.get('/mapping', headers=hdr(u)).status_code == 200, role
        assert client.get('/report/38', headers=hdr(u)).status_code == 200, role


def test_the_dashboard_no_longer_grants_every_report(app, people, sites):
    """Until item 13f each visit to the Viewer Dashboard granted every report to every
    non-admin. Now a User reaches only what was ticked."""
    from utils.roles import can_open_report, granted_report_ids
    u = people('dash', 'user')
    tick(app, u, sites['RH'], report=22)
    from db import User, db
    with app.test_request_context('/viewer/dashboard'):
        user = db.session.get(User, u)
        assert granted_report_ids(user) == {22}
        assert can_open_report(user, 22) and not can_open_report(user, 23)


def test_an_account_without_the_site_served_cannot_sign_in(app, people):
    from utils.roles import current_site_id, has_site
    from db import User, db
    with_rh, sjh_only = people('with_rh', 'user'), people('sjh_only', 'administrator', ('SJH',))
    boss = people('boss', 'superuser', ())
    with app.test_request_context('/'):
        assert has_site(db.session.get(User, with_rh), current_site_id())
        assert not has_site(db.session.get(User, sjh_only), current_site_id())
        assert has_site(db.session.get(User, boss), current_site_id())      # a Super user holds every site


# ── what is implementation, what is a setting ─────────────────────────────────

def test_the_brayn_console_is_for_super_user_and_cht(client, people):
    assert client.get('/brayn', headers=hdr(people('c1', 'cht'))).status_code == 200
    assert client.get('/brayn', headers=hdr(people('s1', 'superuser'))).status_code == 200
    assert client.get('/brayn', headers=hdr(people('a1', 'administrator'))).status_code == 403
    assert client.get('/brayn', headers=hdr(people('u1', 'user'))).status_code == 403


def test_the_etl_control_rooms_report_start_date_is_implementation(client, people):
    body = {'date': '2026-01-01'}
    assert client.post('/admin/report-start-date', json=body, headers=hdr(people('c2', 'cht'))).status_code == 200
    assert client.post('/admin/report-start-date', json=body, headers=hdr(people('a2', 'administrator'))).status_code == 403


def test_the_email_server_is_a_setting(client, people):
    assert client.get('/admin/email-server', headers=hdr(people('a3', 'administrator'))).status_code == 200
    assert client.get('/admin/email-server', headers=hdr(people('c3', 'cht'))).status_code == 200
    assert client.get('/admin/email-server', headers=hdr(people('u3', 'user'))).status_code == 403


def test_mapping_edits_are_implementation(client, app, people, sites):
    """Viewing Mapping is a page a User can be granted; changing it is implementation."""
    viewer = people('map_view', 'user')
    tick(app, viewer, sites['RH'], page='mapping')
    admin = people('map_admin', 'administrator')
    for who in (viewer, admin):
        r = client.post('/mapping/ae/update', json={'id': 0}, headers=hdr(who))
        assert r.status_code == 403


# ── User Management ───────────────────────────────────────────────────────────

def test_an_administrator_sees_only_the_accounts_of_its_sites(client, people):
    admin = people('adm', 'administrator')
    mine = people('mine', 'user')
    both = people('both', 'user', ('RH', 'SJH'))
    vendor = people('vendor', 'cht')
    boss = people('boss2', 'superuser')
    fresh = people('fresh', 'user', (), status='pending')
    page = client.get('/admin/users', headers=hdr(admin)).get_data(as_text=True)
    assert 'rt_mine' in page and 'rt_fresh' in page
    assert 'rt_both' not in page and 'rt_vendor' not in page and 'rt_boss2' not in page
    assert 'rt_adm"' not in page            # not its own account either


def test_a_super_user_sees_every_other_account(client, people):
    boss = people('boss3', 'superuser', ())
    others = [people('o_' + r, r, ('RH', 'SJH')) for r in ('cht', 'administrator', 'user')]
    page = client.get('/admin/users', headers=hdr(boss)).get_data(as_text=True)
    assert all(f'rt_o_{r}' in page for r in ('cht', 'administrator', 'user'))


def test_cht_and_users_do_not_manage_accounts(client, people):
    for role in ('cht', 'user'):
        assert client.get('/admin/users', headers=hdr(people('nm_' + role, role))).status_code == 403


def test_an_administrator_gives_only_administrator_and_user(client, people):
    admin, target = people('adm2', 'administrator'), people('t2', 'user')
    for role, ok in (('administrator', True), ('user', True), ('cht', False), ('superuser', False)):
        r = client.post('/admin/users/role', json={'user_id': target, 'role': role}, headers=hdr(admin))
        assert (r.status_code == 200) is ok, (role, r.get_json())


def test_an_administrator_cannot_touch_a_cht_or_super_user_account(client, people):
    admin = people('adm3', 'administrator')
    for victim in (people('v_cht', 'cht'), people('v_su', 'superuser')):
        assert client.post('/admin/users/disable', json={'user_id': victim}, headers=hdr(admin)).status_code == 400
        assert client.post('/admin/users/set-password', json={'user_id': victim, 'password': 'abcdef'},
                           headers=hdr(admin)).status_code == 400


def test_nobody_changes_their_own_account_here(client, people):
    boss = people('self', 'superuser')
    assert client.post('/admin/users/role', json={'user_id': boss, 'role': 'user'}, headers=hdr(boss)).status_code == 400
    assert client.post('/admin/users/disable', json={'user_id': boss}, headers=hdr(boss)).status_code == 400


def test_an_administrator_grants_only_its_own_sites(client, app, people, sites):
    admin, target = people('adm4', 'administrator'), people('t4', 'user')
    assert client.post('/admin/users/sites', json={'user_id': target, 'site_id': sites['SJH'], 'granted': True},
                       headers=hdr(admin)).status_code == 400
    assert client.post('/admin/users/permissions', json={'user_id': target, 'site_id': sites['SJH'],
                       'page_key': 'oru', 'enabled': True}, headers=hdr(admin)).status_code == 400
    boss = people('boss4', 'superuser', ())
    assert client.post('/admin/users/sites', json={'user_id': target, 'site_id': sites['SJH'], 'granted': True},
                       headers=hdr(boss)).status_code == 200
    from db import db
    with app.app_context():
        got = {r[0] for r in db.session.execute(text("SELECT site_id FROM user_sites WHERE user_id = :u"), {'u': target})}
    assert got == {sites['RH'], sites['SJH']}


def test_ticks_are_written_for_the_site_named(client, app, people, sites):
    admin, target = people('adm5', 'administrator'), people('t5', 'user')
    assert client.post('/admin/users/permissions', json={'user_id': target, 'page_key': 'oru', 'enabled': True},
                       headers=hdr(admin)).status_code == 200
    assert client.post('/admin/users/reports', json={'user_id': target, 'site_id': sites['RH'], 'report_id': 22,
                       'enabled': True}, headers=hdr(admin)).status_code == 200
    assert client.post('/admin/users/permissions', json={'user_id': target, 'page_key': 'nope', 'enabled': True},
                       headers=hdr(admin)).status_code == 400
    from db import db
    with app.app_context():
        page = db.session.execute(text(
            "SELECT site_id, is_enabled FROM user_page_permissions WHERE user_id = :u AND page_key = 'oru'"),
            {'u': target}).fetchone()
        report = db.session.execute(text(
            "SELECT site_id, is_enabled FROM report_access_control WHERE user_id = :u AND report_template_id = 22"),
            {'u': target}).fetchone()
        audit = db.session.execute(text(
            "SELECT COUNT(*) FROM user_audit_log WHERE target_user_id = :u"), {'u': target}).scalar()
    assert tuple(page) == (sites['RH'], True) and tuple(report) == (sites['RH'], True)
    assert audit == 2


def test_tick_all_and_untick_all(client, app, people, sites):
    from utils.roles import PAGE_KEYS
    admin, target = people('adm6', 'administrator'), people('t6', 'user')
    assert client.post('/admin/users/access-all', json={'user_id': target, 'enabled': True},
                       headers=hdr(admin)).status_code == 200
    from db import db
    with app.app_context():
        on = db.session.execute(text(
            "SELECT COUNT(*) FROM user_page_permissions WHERE user_id = :u AND is_enabled"), {'u': target}).scalar()
        reports = db.session.execute(text(
            "SELECT COUNT(*) FROM report_access_control WHERE user_id = :u AND is_enabled"), {'u': target}).scalar()
    assert on == len(PAGE_KEYS) and reports > 0
    client.post('/admin/users/access-all', json={'user_id': target, 'enabled': False}, headers=hdr(admin))
    assert client.get('/mapping', headers=hdr(target)).status_code == 403


def test_an_administrator_adds_an_account_directly(client, app, people, sites):
    admin = people('adm7', 'administrator')
    body = {'username': 'rt_new1', 'full_name': 'New One', 'password': 'temp123',
            'role': 'user', 'site_ids': [sites['RH']]}
    r = client.post('/admin/users/create', json=body, headers=hdr(admin))
    assert r.status_code == 200, r.get_json()
    from db import db
    with app.app_context():
        row = db.session.execute(text(
            "SELECT role, status, must_change_password, created_by FROM users WHERE username = 'rt_new1'")).fetchone()
        site = db.session.execute(text(
            "SELECT array_agg(site_id) FROM user_sites WHERE user_id = :u"), {'u': r.get_json()['user_id']}).scalar()
    assert tuple(row) == ('user', 'active', True, admin) and site == [sites['RH']]
    assert client.post('/admin/users/create', json=body, headers=hdr(admin)).status_code == 409


@pytest.mark.parametrize('change, message', [
    ({'role': 'cht'}, 'role'),
    ({'site_ids': []}, 'site'),
    ({'site_ids': ['SJH']}, 'site'),
    ({'password': '123'}, 'password'),
    ({'username': ''}, 'username'),
])
def test_a_direct_account_is_refused_when_wrong(client, people, sites, change, message):
    admin = people('adm8', 'administrator')
    body = {'username': 'rt_new2', 'password': 'temp123', 'role': 'user', 'site_ids': [sites['RH']], **change}
    if change.get('site_ids') == ['SJH']:
        body['site_ids'] = [sites['SJH']]
    r = client.post('/admin/users/create', json=body, headers=hdr(admin))
    assert r.status_code == 400 and message in r.get_json()['message'].lower()


def test_an_account_that_made_others_can_still_be_deleted(client, app, people, sites):
    """created_by and user_sites.granted_by point at the account that made or granted;
    deleting it keeps what it made (migration 0152 set granted_by ON DELETE SET NULL)."""
    maker, boss = people('maker', 'superuser', ()), people('boss5', 'superuser', ())
    r = client.post('/admin/users/create', json={'username': 'rt_new3', 'password': 'temp123', 'role': 'user',
                                                 'site_ids': [sites['RH']]}, headers=hdr(maker))
    made = r.get_json()['user_id']
    assert client.post('/admin/users/delete', json={'user_id': maker}, headers=hdr(boss)).status_code == 200
    from db import db
    with app.app_context():
        row = db.session.execute(text("""SELECT u.created_by, s.granted_by FROM users u
                                         JOIN user_sites s ON s.user_id = u.id WHERE u.id = :u"""), {'u': made}).fetchone()
    assert tuple(row) == (None, None)


def test_approving_a_registration_needs_a_site(client, app, people, sites):
    admin = people('adm9', 'administrator')
    fresh = people('fresh2', 'user', (), status='pending')
    assert client.post('/admin/users/approve', json={'user_id': fresh, 'role': 'user', 'site_ids': []},
                       headers=hdr(admin)).status_code == 400
    assert client.post('/admin/users/approve', json={'user_id': fresh, 'role': 'user', 'site_ids': [sites['RH']]},
                       headers=hdr(admin)).status_code == 200
    from db import db
    with app.app_context():
        assert db.session.execute(text("SELECT status FROM users WHERE id = :u"), {'u': fresh}).scalar() == 'active'


def test_a_role_change_signs_the_account_out(client, app, people):
    admin, target = people('adm10', 'administrator'), people('t10', 'user')
    from db import db
    with app.app_context():
        db.session.execute(text("INSERT INTO active_sessions (session_id, user_id, role, ip_address) VALUES ('rt-s1', :u, 'user', '127.0.0.1')"),
                           {'u': target})
        db.session.commit()
    client.post('/admin/users/role', json={'user_id': target, 'role': 'administrator'}, headers=hdr(admin))
    with app.app_context():
        assert db.session.execute(text("SELECT COUNT(*) FROM active_sessions WHERE user_id = :u"),
                                  {'u': target}).scalar() == 0


# ── nothing compares role names any more ──────────────────────────────────────

LEGACY = r"(admin|viewer2?|tec|finance|secretary)"
PATTERNS = [
    re.compile(r"\.role\s*(==|!=)\s*['\"]" + LEGACY + "['\"]"),
    re.compile(r"\.role\s+(not\s+)?in\s*[\(\[\{][^)\]\}]*['\"]" + LEGACY + "['\"]"),
    re.compile(r"_ROLES\s*=\s*[\(\[\{][^)\]\}]*['\"]" + LEGACY + "['\"]"),
    re.compile(r"role\s*=\s*['\"]" + LEGACY + "['\"]"),
]
# Templates nothing renders any more (no route names them); left as they were.
DEAD = {'dashboard.html', 'config_page.html'}


def test_no_code_compares_an_old_role_name():
    found = []
    for folder, exts in (('routes', '.py'), ('utils', '.py'), ('templates', '.html'), ('.', '.py')):
        base = os.path.join(ROOT, folder)
        names = os.listdir(base) if folder != '.' else ['app.py', 'db.py', 'hl7_listener.py']
        for name in names:
            if not name.endswith(exts) or name in DEAD or name == 'roles.py':
                continue
            path = os.path.join(base, name)
            for n, line in enumerate(open(path, encoding='utf-8', errors='replace'), 1):
                if any(p.search(line) for p in PATTERNS):
                    found.append(f'{folder}/{name}:{n}: {line.strip()}')
    assert not found, '\n'.join(found)
