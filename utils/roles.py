"""
utils/roles.py — who may do what in RAYD, in one place (meeting of 2026-11-13, item 13f).

The old roles (admin, viewer, viewer2, tec, finance, secretary) are replaced by four
(migration 0152 renames every account):

    superuser       Super user     R&D: everything, every site, every account
    cht             CHT            implementation staff: mapping, ETL, BRAYN console
    administrator   Administrator  the users and settings of the sites granted
    user            User           the pages granted, site by site

Existing admins became Administrators; viewer, viewer2, tec, finance and secretary became
Users keeping their pages. The built-in "admin" account became the Super user, so somebody
can still reach the ETL Control Room, Mapping and the BRAYN console after the change.

Code never compares role names. It asks for a capability:

    can(user, 'implementation')   ETL Control Room (report start date, demo mode, triggers),
                                  DB Manager, Mapping edits, BRAYN console, HL7 Forward,
                                  HL7 field map, floor plan, resource monitor, NLP runs
    can(user, 'settings')         hospital settings: Email Server, CRN set-up and actions,
                                  financial config, report targets and thresholds, Report
                                  Composer SQL, schedules and shared reports, ORU vocabulary
    can(user, 'users')            User Management, permission groups, Activity Log
    can(user, 'all_pages')        every licensed page and report, no per-page grants
    can(user, 'all_sites')        every site without being granted it

so a change of who may do what is a change of CAPABILITIES below, nowhere else.

Sites (table sites, RH and SJH). Each account is granted sites (user_sites), then pages and
reports per site (user_page_permissions.site_id, report_access_control.site_id), all read
from the main database whatever site the request reads. The site a request reads is picked
in the header dropdown among the sites the account holds that have a database
(utils/site_db.py, item 13a); an account holding none of them is signed out (app.py). A
Super user holds every site.

Legacy role names still found on an account (a migration that did not run, an old row) are
read as their new role, so nobody is locked out the way the HL7 branch was (db4ce865). An
unknown role has no rights at all.
"""
import logging

logger = logging.getLogger("ROLES")

SUPERUSER = 'superuser'
CHT = 'cht'
ADMINISTRATOR = 'administrator'
USER = 'user'

ROLES = (SUPERUSER, CHT, ADMINISTRATOR, USER)

ROLE_LABELS = {
    SUPERUSER: 'Super user',
    CHT: 'CHT',
    ADMINISTRATOR: 'Administrator',
    USER: 'User',
}

ROLE_HELP = {
    SUPERUSER: 'Everything, on every site, including every account.',
    CHT: 'Implementation: mapping, ETL, BRAYN console and settings, plus every page of its sites.',
    ADMINISTRATOR: 'The users and settings of its sites, plus every page of its sites.',
    USER: 'Only the pages and reports ticked for it, site by site.',
}

LEGACY_ROLES = {
    'admin': ADMINISTRATOR,
    'viewer': USER, 'viewer2': USER, 'tec': USER, 'finance': USER, 'secretary': USER,
}

CAPABILITIES = {
    'implementation': frozenset({SUPERUSER, CHT}),
    'settings':       frozenset({SUPERUSER, CHT, ADMINISTRATOR}),
    'users':          frozenset({SUPERUSER, ADMINISTRATOR}),
    'all_pages':      frozenset({SUPERUSER, CHT, ADMINISTRATOR}),
    'all_sites':      frozenset({SUPERUSER}),
}

# Which roles each role may give (and take away) in User Management.
ASSIGNABLE = {
    SUPERUSER: ROLES,
    ADMINISTRATOR: (ADMINISTRATOR, USER),
}

# The feature pages a User can be granted, per site (reports are granted by number).
# 'crn' is the CRN board, which viewers had by role before item 13f.
PAGES = [
    ('live_feed',       'Live Department View'),
    ('hl7_orders',      'HL7 Orders'),
    ('oru',             'Report Intelligence'),
    ('referring_intel', 'Referring Intel'),
    ('custom_reports',  'Report Composer'),
    ('crn',             'CRN board'),
    ('mapping',         'Modality / Procedures (view)'),
    ('report_ai',       'AI Reports'),
    ('financial',       'Revenue Intelligence'),
    ('cd_print',        'Patient CD Log'),
]
PAGE_KEYS = [k for k, _ in PAGES]


# ── roles ─────────────────────────────────────────────────────────────────────

def role_of(user):
    """The account's role in the new vocabulary, or None (signed out, unknown role)."""
    if user is None or not getattr(user, 'is_authenticated', False):
        return None
    raw = (getattr(user, 'role', None) or '').strip().lower()
    raw = LEGACY_ROLES.get(raw, raw)
    return raw if raw in ROLES else None


def can(user, capability):
    """True when the account's role holds the capability. A misspelt capability raises."""
    return role_of(user) in CAPABILITIES[capability]


def is_superuser(user):
    return role_of(user) == SUPERUSER


def role_label(role):
    role = LEGACY_ROLES.get((role or '').strip().lower(), (role or '').strip().lower())
    return ROLE_LABELS.get(role, role or '—')


def assignable_roles(actor):
    return ASSIGNABLE.get(role_of(actor), ())


# ── sites ─────────────────────────────────────────────────────────────────────

def _main_rows(sql, params=None):
    """Rows from the main database, where logins and access live whatever site the request
    reads (utils/site_db.py), on a connection of their own so a failed read never poisons
    the request's transaction."""
    from sqlalchemy import text
    from db import db
    with db.engine.connect() as conn:
        return conn.execute(text(sql), params or {}).fetchall()


def all_sites():
    """[(id, code, name)] of the active sites, the default one first."""
    try:
        return [tuple(r) for r in _main_rows(
            "SELECT id, code, name FROM sites WHERE active ORDER BY is_default DESC, id")]
    except Exception as e:
        logger.error(f"sites not read: {e}")
        return []


def default_site_id():
    from utils.site_resolver import default_site
    return default_site()


def current_site_id():
    """The site this request reads: the one picked in the header dropdown when the account
    may open it, else the default site (utils/site_db.py, item 13a)."""
    from utils.site_db import current_site_id as _current
    return _current()


def granted_site_ids(user):
    """The sites the account may open. A Super user holds every active site."""
    if role_of(user) is None:
        return set()
    if can(user, 'all_sites'):
        return {s[0] for s in all_sites()}
    return set(_sites_of(getattr(user, 'id', None)))


def has_site(user, site_id):
    return site_id is not None and site_id in granted_site_ids(user)


# ── pages and reports ─────────────────────────────────────────────────────────

def user_has_page(user, page_key):
    """True when the account may open a feature page on the current site."""
    if role_of(user) is None:
        return False
    if can(user, 'all_pages'):
        return True
    return page_key in _access(user)['pages']


def can_open_report(user, report_id):
    """True when the account may open numbered report report_id on the current site."""
    if role_of(user) is None:
        return False
    if can(user, 'all_pages'):
        return True
    try:
        return int(report_id) in _access(user)['reports']
    except (TypeError, ValueError):
        return False


def granted_report_ids(user):
    """Report numbers a User was granted on the current site (all_pages roles: None = all)."""
    if can(user, 'all_pages'):
        return None
    return set(_access(user)['reports'])


def _cache():
    """This request's cache (in its environ, which no later request shares); None outside
    a request."""
    from flask import has_request_context, request
    return request.environ.setdefault('rayd.access', {}) if has_request_context() else None


def _sites_of(uid):
    """The sites granted to an account, read once per request. Fail-closed."""
    if uid is None:
        return frozenset()
    cache = _cache()
    key = ('sites', uid)
    if cache is not None and key in cache:
        return cache[key]
    try:
        out = frozenset(r[0] for r in _main_rows(
            "SELECT site_id FROM user_sites WHERE user_id = :u", {'u': uid}))
    except Exception as e:
        logger.error(f"sites of user {uid} not read: {e}")
        out = frozenset()
    if cache is not None:
        cache[key] = out
    return out


def _access(user):
    """The account's pages and reports on the current site, read once per request and
    site. Nothing unless the site is granted to it. Fail-closed."""
    uid = getattr(user, 'id', None)
    empty = {'pages': frozenset(), 'reports': frozenset()}
    if uid is None:
        return empty
    site = current_site_id()
    cache = _cache()
    key = ('access', uid, site)
    if cache is not None and key in cache:
        return cache[key]
    out = empty
    if site in _sites_of(uid):
        try:
            out = {
                'pages': frozenset(r[0] for r in _main_rows("""
                    SELECT page_key FROM user_page_permissions
                    WHERE user_id = :u AND site_id = :s AND is_enabled""", {'u': uid, 's': site})),
                'reports': frozenset(r[0] for r in _main_rows("""
                    SELECT report_template_id FROM report_access_control
                    WHERE user_id = :u AND site_id = :s AND is_enabled""", {'u': uid, 's': site})),
            }
        except Exception as e:
            logger.error(f"access of user {uid} not read: {e}")
    if cache is not None:
        cache[key] = out
    return out


def forget_access():
    """Drop this request's cached access (after changing a grant in the same request)."""
    from flask import has_request_context, request
    if has_request_context():
        request.environ.pop('rayd.access', None)


# ── who may manage whom ───────────────────────────────────────────────────────

def can_manage(actor, target, target_sites=None, actor_sites=None):
    """True when actor may change target's account: role, sites, pages, password, status.

    A Super user manages every other account. An Administrator manages Users and
    Administrators whose sites are all among its own (an account with no site yet, a new
    registration, counts). Nobody manages their own account here, and CHT manages none.
    target_sites / actor_sites: the sites, when the caller already read them (lists)."""
    if target is None or getattr(target, 'id', None) == getattr(actor, 'id', None):
        return False
    who = role_of(actor)
    if who == SUPERUSER:
        return True
    if who != ADMINISTRATOR:
        return False
    if _role_of_row(target) not in (ADMINISTRATOR, USER):
        return False
    if target_sites is None:
        target_sites = _target_sites(target)
    if actor_sites is None:
        actor_sites = granted_site_ids(actor)
    return set(target_sites) <= set(actor_sites)


def grantable_site_ids(actor):
    """Sites actor may give or take away: a Super user every site, an Administrator its own."""
    if not can(actor, 'users'):
        return set()
    return granted_site_ids(actor)


def _role_of_row(user):
    raw = (getattr(user, 'role', None) or '').strip().lower()
    raw = LEGACY_ROLES.get(raw, raw)
    return raw if raw in ROLES else None


def _target_sites(target):
    return {r[0] for r in _main_rows("SELECT site_id FROM user_sites WHERE user_id = :u",
                                     {'u': target.id})}


# ── Flask wiring ──────────────────────────────────────────────────────────────

def init_app(app):
    """Template helpers: {% if can('implementation') %}, {{ role_label(u.role) }}."""
    from flask_login import current_user

    def site_switch():
        """The header's site dropdown (item 13a): None unless the account may open two or
        more sites that have a database."""
        from utils.site_db import current_site_id as _current, selectable_site_ids
        if not getattr(current_user, 'is_authenticated', False):
            return None
        choices = selectable_site_ids(current_user)
        if len(choices) < 2:
            return None
        return {'current': _current(),
                'choices': [{'id': sid, 'code': code, 'name': name}
                            for sid, code, name in all_sites() if sid in choices]}

    @app.context_processor
    def _roles_context():
        return {
            'can': lambda capability: can(current_user, capability),
            'role_label': role_label,
            'is_superuser': lambda: is_superuser(current_user),
            'site_switch': site_switch,
        }
