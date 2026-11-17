"""
utils/staff_status.py — staff who left (LAUMC to-do item 9, migration 0157).

The customer, 2026-11-13: the manager sets leavers inactive on the RIS and PACS herself; then,
automatically, someone inactive on the RIS and/or PACS has no KPIs shown, in any period, while
department totals still count their studies. Having no PACS account is not inactive. Nothing
is deleted.

Who left is worked out in Postgres, in the materialized view public.staff_status (one row per
RIS person, and one per PACS login no RIS person holds), rebuilt after every nightly ETL
(refresh_all). Migration 0157 explains the rule; 0158 counts a PACS account as left only on a
status a leaver is set to by hand (settings.staff_pacs_left_statuses), never a lock after wrong
passwords (operator, 2026-11-17). While the switch is on
(settings.staff_left_hidden), the rh views every page reads replace a leaver's login and name
with FORMER on studies and RIS staff rows, so each per-person list shows one "Former staff"
line and every count and total stays whole. What the views do not reach is masked in code
with left_logins_sql() (Report 32's signature turnaround reads the PACS audit copy).

Report 39 (routes/report_39.py) lists everyone, with their activity in the period, so the
manager sees who the rule takes before the switch goes on, and anyone it takes who still works.
"""
import logging
from datetime import date, datetime

from sqlalchemy import text

logger = logging.getLogger(__name__)

FORMER = 'Former staff'
SWITCH_KEY = 'staff_left_hidden'

# Report 39's columns: (header, key of a shown row).
COLUMNS = (
    ('Name', 'name'),
    ('PACS group', 'pacs_groups'),
    ('RIS role', 'ris_roles'),
    ('PACS login', 'logins'),
    ('RIS', 'ris'),
    ('PACS', 'pacs'),
    ('Linked by', 'linked_by'),
    ('Status', 'status'),
    ('Why', 'reason'),
    ('Studies signed', 'signed'),
    ('Last signed', 'last_signed'),
    ('Exams performed', 'performed'),
    ('Last performed', 'last_performed'),
)

STATUSES = (
    ('current', 'Current'),
    ('left', 'Left'),
    ('working', 'Left, but worked in the period'),
)


def login_sql(expr):
    """A login as staff_status keeps it: lower case, without the @domain (first.last@ad,
    and RIS resource IDs like first.last@ad.umcrh.com_123, are first.last)."""
    return f"LOWER(BTRIM(SPLIT_PART(BTRIM({expr}), '@', 1)))"


def left_logins_sql():
    """The logins of staff who left, while the switch is on (otherwise none), as a subquery
    for `x IN ...`."""
    return "(SELECT k.key FROM staff_left_keys k WHERE k.kind = 'login')"


def is_on(session=None):
    from db import db
    value = (session or db.session).execute(
        text("SELECT value FROM settings WHERE key = :k"), {'k': SWITCH_KEY}).scalar()
    return str(value or '').strip().lower() == 'true'


def refresh(conn):
    """Rebuild public.staff_status from std_resources_ris and std_pacs_user_groups."""
    conn.execute(text("REFRESH MATERIALIZED VIEW public.staff_status"))


def refresh_all(main_engine):
    """After the nightly ETL: the main database, then each site database (the route job
    copies both staff tables there). Returns {site_id: rows}."""
    from utils import site_db
    engines = [('main', main_engine)]
    if site_db.has_site_databases():
        main_id = site_db.default_site_id()
        engines += [(site_db.site_code(sid) or sid, site_db.engine_for(sid))
                    for sid in site_db.each_site_id() if sid != main_id]
    out = {}
    for name, engine in engines:
        with engine.begin() as conn:
            refresh(conn)
            out[name] = conn.execute(text("SELECT COUNT(*) FROM public.staff_status")).scalar()
    return out


def set_on(on):
    """The switch (Report 39, settings capability). Rebuilds the list first, so what goes on
    is what Report 39 shows now."""
    from db import db
    refresh(db.session)
    db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                       {'k': SWITCH_KEY, 'v': 'true' if on else 'false'})
    db.session.commit()


# ── Report 39 ─────────────────────────────────────────────────────────────────

# Activity in the period, from the real tables (the rh views would show a leaver as Former
# staff once the switch is on). Studies keep to this database's site and leave SR out, as the
# rh view and CLAUDE.md rule 2 do; std_pps is read through its rh view (this site's devices).
_SQL = """
WITH signed AS (
    SELECT """ + login_sql('s.rep_study_last_composed_by') + """ AS login,
           COUNT(*) AS n, MAX(s.rep_study_last_composed_ts) AS last_at
    FROM public.etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE s.study_date BETWEEN :start AND :end
      AND NULLIF(BTRIM(s.rep_study_last_composed_by), '') IS NOT NULL
      AND COALESCE(m.modality, s.study_modality, '') <> 'SR'
      AND COALESCE(s.site_id = rayd_served_site_id(), TRUE)
    GROUP BY 1
),
performed AS (
    SELECT ppr.resource_id_key, COUNT(DISTINCT p.pps_key) AS n, MAX(p.start_datetime) AS last_at
    FROM std_pps p
    JOIN std_pps_person_reference ppr ON ppr.pps_key = p.pps_key
    WHERE p.start_datetime >= :start AND p.start_datetime < CAST(:end AS date) + 1
    GROUP BY 1
)
SELECT st.staff_id, st.person_key, st.name, st.ris_roles, st.ris_active, st.logins, st.linked_by,
       st.pacs_groups, st.pacs_status, st.pacs_status_since, st.pacs_active, st.is_left, st.left_reason,
       (SELECT SUM(g.n) FROM signed g WHERE g.login = ANY (st.logins)) AS signed,
       (SELECT MAX(g.last_at) FROM signed g WHERE g.login = ANY (st.logins)) AS last_signed,
       (SELECT SUM(f.n) FROM performed f WHERE f.resource_id_key = ANY (st.resource_keys)) AS performed,
       (SELECT MAX(f.last_at) FROM performed f WHERE f.resource_id_key = ANY (st.resource_keys)) AS last_performed
FROM staff_status st
"""


def _split(value):
    return [v.strip() for v in (value or '').split(',') if v.strip()]


def _when(value):
    return value.strftime('%Y-%m-%d') if isinstance(value, (date, datetime)) else None


def _shape(r):
    signed, performed = int(r['signed'] or 0), int(r['performed'] or 0)
    ris_only = r['person_key'] is not None and not r['logins']
    if r['person_key'] is None:
        linked = 'PACS only'
    elif r['linked_by']:
        linked = r['linked_by']
    else:
        linked = 'RIS only'
    pacs = 'No account' if r['pacs_active'] is None else (r['pacs_status'] or 'ACTIVE')
    if r['pacs_status_since'] and r['pacs_active'] is False:
        pacs += f" since {_when(r['pacs_status_since'])}"
    return {
        'staff_id': r['staff_id'],
        'name': r['name'] or (r['logins'][0] if r['logins'] else ''),
        'pacs_groups': r['pacs_groups'] or '',
        'ris_roles': r['ris_roles'] or '',
        'logins': ', '.join(r['logins'] or []),
        'ris': '' if r['ris_active'] is None else ('Active' if r['ris_active'] else 'Inactive'),
        'pacs': pacs,
        'linked_by': linked,
        'status': 'Left' if r['is_left'] else 'Current',
        'reason': r['left_reason'] or '',
        'signed': signed,
        'last_signed': _when(r['last_signed']),
        'performed': performed,
        'last_performed': _when(r['last_performed']),
        'is_left': bool(r['is_left']),
        'working': bool(r['is_left']) and (signed > 0 or performed > 0),
        'pacs_only': r['person_key'] is None,
        'ris_only': ris_only,
        'group_list': _split(r['pacs_groups']),
        'role_list': _split(r['ris_roles']),
    }


def build(start, end, chosen=None):
    """(rows kept by the filters, figures). chosen: {'status'|'role'|'group': set of values};
    a filter given no values hides nothing. Sorted by PACS group, RIS role, then name, as the
    customer asked for the manager's list."""
    from db import db
    chosen = {k: set(v) for k, v in (chosen or {}).items() if v}
    rows = [_shape(r) for r in db.session.execute(
        text(_SQL), {'start': start, 'end': end}).mappings()]

    def keep(row):
        if 'status' in chosen:
            ok = (('current' in chosen['status'] and not row['is_left'])
                  or ('left' in chosen['status'] and row['is_left'])
                  or ('working' in chosen['status'] and row['working']))
            if not ok:
                return False
        if 'role' in chosen and not chosen['role'] & set(row['role_list']):
            return False
        if 'group' in chosen and not chosen['group'] & set(row['group_list']):
            return False
        return True

    kept = sorted((r for r in rows if keep(r)),
                  key=lambda r: (r['pacs_groups'] == '', r['pacs_groups'].lower(),
                                 r['ris_roles'] == '', r['ris_roles'], r['name'].lower()))
    status_copied = db.session.execute(text(
        "SELECT EXISTS (SELECT 1 FROM std_pacs_user_groups WHERE user_status IS NOT NULL)")).scalar()
    built_from = db.session.execute(text("SELECT MAX(last_update) FROM std_resources_ris")).scalar()
    left_statuses = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'staff_pacs_left_statuses'")).scalar()
    summary = {
        'listed': len(kept),
        'current': sum(not r['is_left'] for r in kept),
        'left': sum(r['is_left'] for r in kept),
        'working': sum(r['working'] for r in kept),
        'pacs_only': sum(r['pacs_only'] for r in kept),
        'ris_only': sum(r['ris_only'] for r in kept),
        'everyone_left': sum(r['is_left'] for r in rows),
        'hidden_on_reports': is_on(),
        'pacs_status_copied': bool(status_copied),
        'ris_copied_at': built_from.strftime('%Y-%m-%d %H:%M') if built_from else None,
        'pacs_left_statuses': ', '.join(_split((left_statuses or 'SUSPENDED,DISABLED,INACTIVE,DELETED').upper())),
        'roles': sorted({v for r in rows for v in r['role_list']}),
        'groups': sorted({v for r in rows for v in r['group_list']}, key=str.lower),
    }
    return kept, summary
