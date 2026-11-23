"""
utils/tat_bands.py — the hospital's July request on radiologists' turnaround and report counts
(item 14; operator 2026-11-23: "let's continue a,b,c and d"):

  a. turnaround in time bands, per reader, per modality, per patient type, for the wet read and
     for the approval, with bands the hospital sets: Report 36, tab "Time bands" (bands());
  b. reports per radiologist per day per modality: Report 32 (per_day()).

Both read the PACS audit trail copy (etl_pacs_audit_events, ETL_JOBS/etl_pacs_audit.py), the
source the operator set for signatures (2026-11-13: report sign events from PACS):
  wet read   the study's first 'wetread' event (status -> WETREAD) and its login (the resident)
  approval   the study's first 'approve' event (status -> PENDING APPROVED, or ApprovedSign
             Complete) and its login (the radiologist); at LAUMC approved is the final signature
Each study counts once per stage, for its first event, as on Report 36's reading TAT.

a. Time bands. Two clocks start the turnaround, as on the rest of Report 36:
     ris    RIS exam done (std_worklist_exam_done, the first, through the study's RIS orders)
     pacs   images reach PACS (etl_didb_studies.insert_time)
   and it ends at the wet read or the approval. A clock whose start is missing, or later than
   its end, leaves the study out of that clock only. Only studies that reached PACS while the
   audit copy holds them count (its coverage), as on the reading TAT. The page receives one row
   per study and stage, [person, modality, patient type, minutes from RIS, minutes from PACS],
   and groups and bands them itself, so a change of bands shows at once. The bands everyone
   starts with are settings tat_bands_wetread / tat_bands_approve (upper limits in hours, a JSON
   list), saved from the tab by whoever holds the settings capability; DEFAULT_BANDS otherwise
   (the targets already in RAYD: inpatient and ER 24 h, outpatient 48 h).

b. Counts per day: each study once, on the day (hospital time; the audit is the station's
   clock) of its first approval (radiologists) or first wet read (residents), per modality.
   Only days the audit copy holds.

People: the PACS login, shown by name through physician_alias_map; someone who left reads
"Former staff" while that switch is on (item 9: the audit copy has no rh view, so it is done
here, as in utils/sign_tat.py). Role from utils/pacs_roles (the PACS groups, with overrides).
A device mapped twice with different letter case counts its study once (LATERAL ... LIMIT 1).
"""
import json
from datetime import date, datetime, timedelta

from sqlalchemy import text

from db import db
from ETL_JOBS.etl_pacs_audit import coverage as audit_coverage
from utils import staff_status
from utils.pacs_roles import role_lookup_cte

STAGES = ('wetread', 'approve')
DEFAULT_BANDS = {'wetread': [3, 5, 12, 24], 'approve': [6, 24, 48]}
SETTING_KEY = {'wetread': 'tat_bands_wetread', 'approve': 'tat_bands_approve'}
MAX_LIMITS = 8
MAX_HOURS = 720                    # 30 days
CLASS_ORDER = ('ER', 'Inpatient', 'Outpatient', 'Other')
ROLE_LABEL = {'residents': 'Resident', 'radiologists': 'Radiologist'}
KINDS = ('approve', 'wetread')     # per_day's rows say 0 for an approval, 1 for a wet read

# The study's device, once (a device mapped twice in different letter case: one row)
_MAP_JOIN = """LEFT JOIN LATERAL (
        SELECT mm.* FROM aetitle_modality_map mm
        WHERE UPPER(TRIM(mm.aetitle)) = UPPER(TRIM(s.storing_ae))
        ORDER BY mm.id LIMIT 1) m ON TRUE"""
_MODALITY = "COALESCE(m.modality, s.study_modality, 'Unknown')"
_NOT_STUDIES = "COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT', 'BMD')"
_CLASS = """CASE WHEN s.patient_location = 'ER' THEN 'ER'
                WHEN s.patient_class = 'I' THEN 'Inpatient'
                WHEN s.patient_class = 'O' THEN 'Outpatient'
                ELSE 'Other' END"""


def _first(kind, alias):
    return f"""LEFT JOIN LATERAL (
        SELECT e.event_at, e.user_name FROM etl_pacs_audit_events e
        WHERE e.study_instance_uid = st.uid AND e.kind = '{kind}'
        ORDER BY e.event_at, e.log_dbid LIMIT 1) {alias} ON TRUE"""


# The studies dated start..end that reached PACS while the audit copy holds them
_STUDIES = """
studies AS MATERIALIZED (
    SELECT s.study_db_uid, BTRIM(s.study_instance_uid) AS uid, s.insert_time AS arrived_at,
           """ + _MODALITY + """ AS modality, """ + _CLASS + """ AS patient_class
    FROM etl_didb_studies s
    """ + _MAP_JOIN + """
    WHERE s.study_date BETWEEN :start AND :end
      AND """ + _NOT_STUDIES + """
      AND s.insert_time >= :audit_from AND s.insert_time < :audit_until
      AND s.study_instance_uid IS NOT NULL
      {filters}
)"""

_BANDS_SQL = """
WITH {roles},""" + _STUDIES + """,
firsts AS MATERIALIZED (
    SELECT st.study_db_uid, st.arrived_at, st.modality, st.patient_class,
           w.event_at AS wet_at, w.user_name AS wet_user,
           a.event_at AS appr_at, a.user_name AS appr_user
    FROM studies st
    """ + _first('wetread', 'w') + """
    """ + _first('approve', 'a') + """
)
SELECT f.modality, f.patient_class, f.arrived_at, f.wet_at, f.wet_user, f.appr_at, f.appr_user,
       (SELECT MIN(x.exam_done_at) FROM etl_orders o
          JOIN std_worklist_exam_done x ON x.site_worklist_key = o.order_dbid
         WHERE o.study_db_uid = f.study_db_uid) AS exam_done_at,
       wa.canonical_name AS wet_alias, aa.canonical_name AS appr_alias,
       COALESCE({wet_login} IN {left_logins}, FALSE) AS wet_left,
       COALESCE({appr_login} IN {left_logins}, FALSE) AS appr_left,
       wr.role AS wet_role, ar.role AS appr_role
FROM firsts f
LEFT JOIN physician_alias_map wa ON wa.dismissed = false AND wa.alias = f.wet_user
LEFT JOIN physician_alias_map aa ON aa.dismissed = false AND aa.alias = f.appr_user
LEFT JOIN role_lookup wr ON wr.login_id = UPPER(SPLIT_PART(f.wet_user, '@', 1))
LEFT JOIN role_lookup ar ON ar.login_id = UPPER(SPLIT_PART(f.appr_user, '@', 1))
WHERE f.wet_at IS NOT NULL OR f.appr_at IS NOT NULL
"""

_PER_DAY_SQL = """
WITH {roles},
touched AS MATERIALIZED (
    SELECT DISTINCT r.study_instance_uid FROM etl_pacs_audit_events r
    WHERE r.kind IN ('approve', 'wetread') AND r.event_at >= :day_from AND r.event_at < :day_to
),
firsts AS MATERIALIZED (
    SELECT DISTINCT ON (e.kind, e.study_instance_uid)
           e.kind, e.study_instance_uid AS uid, e.event_at, e.user_name
    FROM etl_pacs_audit_events e
    JOIN touched t ON t.study_instance_uid = e.study_instance_uid
    WHERE e.kind IN ('approve', 'wetread')
    ORDER BY e.kind, e.study_instance_uid, e.event_at, e.log_dbid
),
counted AS MATERIALIZED (
    SELECT DISTINCT ON (f.kind, f.uid) f.kind, f.uid, f.user_name, f.event_at::date AS day,
           """ + _MODALITY + """ AS modality
    FROM firsts f
    JOIN etl_didb_studies s ON BTRIM(s.study_instance_uid) = f.uid
    """ + _MAP_JOIN + """
    WHERE f.event_at >= :day_from AND f.event_at < :day_to
      AND """ + _NOT_STUDIES + """
      {filters}
    ORDER BY f.kind, f.uid, s.study_db_uid
)
SELECT c.kind, c.user_name, c.day, c.modality, COUNT(*) AS n,
       MAX(alias.canonical_name) AS alias, MAX(r.role) AS role,
       BOOL_OR(COALESCE({login} IN {left_logins}, FALSE)) AS left_
FROM counted c
LEFT JOIN physician_alias_map alias ON alias.dismissed = false AND alias.alias = c.user_name
LEFT JOIN role_lookup r ON r.login_id = UPPER(SPLIT_PART(c.user_name, '@', 1))
GROUP BY c.kind, c.user_name, c.day, c.modality
"""


# ── the bands ────────────────────────────────────────────────────────────────

def clean_limits(values):
    """Upper limits in hours: numbers above 0 and up to MAX_HOURS, going up, 1 to MAX_LIMITS of
    them. Raises ValueError with a message for the person otherwise."""
    if isinstance(values, (str, bytes)) or not hasattr(values, '__iter__'):
        raise ValueError('The limits are hours, as numbers: for example 3, 5, 12, 24.')
    try:
        limits = [float(v) for v in values]
    except (TypeError, ValueError):
        raise ValueError('The limits are hours, as numbers: for example 3, 5, 12, 24.')
    if not 1 <= len(limits) <= MAX_LIMITS:
        raise ValueError(f'Give between 1 and {MAX_LIMITS} limits.')
    if any(not (0 < v <= MAX_HOURS) for v in limits):
        raise ValueError(f'Each limit is more than 0 and at most {MAX_HOURS} hours (30 days).')
    if any(b <= a for a, b in zip(limits, limits[1:])):
        raise ValueError('The limits go up, each larger than the one before: for example 3, 5, 12, 24.')
    return [int(v) if v == int(v) else round(v, 2) for v in limits]


def load_bands(session=None):
    """{stage: [upper limits in hours]}: the settings, else DEFAULT_BANDS."""
    session = session or db.session
    out = {}
    for stage in STAGES:
        raw = session.execute(text("SELECT value FROM settings WHERE key = :k"),
                              {'k': SETTING_KEY[stage]}).scalar()
        try:
            out[stage] = clean_limits(json.loads(raw)) if raw else list(DEFAULT_BANDS[stage])
        except (ValueError, TypeError):
            out[stage] = list(DEFAULT_BANDS[stage])
    return out


def save_bands(stage, values):
    """Store one stage's limits as everyone's default; returns them cleaned."""
    if stage not in STAGES:
        raise ValueError('Unknown stage.')
    limits = clean_limits(values)
    db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                       {'k': SETTING_KEY[stage], 'v': json.dumps(limits)})
    db.session.commit()
    return limits


def band_counts(minutes, limits_h):
    """[n per band] of the measured minutes: (previous limit, limit], the first from 0, the
    last open-ended. The page does the same in JavaScript; this one is for the tests."""
    values = [v for v in minutes if v is not None]
    edges = [h * 60 for h in limits_h]
    out, low = [], None
    for upper in edges + [None]:
        out.append(sum(1 for v in values if (low is None or v > low) and (upper is None or v <= upper)))
        low = upper
    return out


# ── shared pieces ────────────────────────────────────────────────────────────

def _minutes(start, end):
    if start is None or end is None or end < start:
        return None
    return int(round((end - start).total_seconds() / 60))


class _Index:
    """Values to small numbers, in first-seen order (the page's rows carry the numbers)."""
    def __init__(self):
        self.items, self._pos = [], {}

    def __call__(self, key, item=None):
        if key not in self._pos:
            self._pos[key] = len(self.items)
            self.items.append(key if item is None else item)
        return self._pos[key]


def _person(login, alias, left, role):
    """(name, role label) of a login: Former staff while the switch hides who left."""
    from utils.sticky_notes import person_name
    if left:
        return staff_status.FORMER, None
    return person_name(alias, login) or '(no login)', ROLE_LABEL.get(role)


def _coverage():
    audit_from, audit_until = audit_coverage(db.session)
    if audit_from is None:
        return None, None, None
    return audit_from, audit_until, {'from': audit_from.strftime('%Y-%m-%d'),
                                     'until': audit_until.strftime('%Y-%m-%d %H:%M')}


# ── a. turnaround in time bands (Report 36) ─────────────────────────────────

def bands(start, end, params=None, filters=''):
    """Report 36's "Time bands" tab for studies dated start..end. params / filters: Report 36's
    left panel (utils.report_filters.sidebar_filters, aliases s and m)."""
    out = {'coverage': None, 'studies': 0, 'people': [], 'modalities': [], 'classes': list(CLASS_ORDER),
           'wetread': [], 'approve': [], 'wet_studies': 0, 'approved_studies': 0}
    audit_from, audit_until, coverage = _coverage()
    if coverage is None:
        return out
    out['coverage'] = coverage
    bind = {**(params or {}), 'start': start, 'end': end, 'audit_from': audit_from, 'audit_until': audit_until}
    sql = _BANDS_SQL.format(roles=role_lookup_cte('role_lookup'), filters=filters or '',
                            wet_login=staff_status.login_sql('f.wet_user'),
                            appr_login=staff_status.login_sql('f.appr_user'),
                            left_logins=staff_status.left_logins_sql())
    rows = db.session.execute(text(sql), bind).mappings().all()
    out['studies'] = int(db.session.execute(text(
        "WITH " + _STUDIES.format(filters=filters or '') + " SELECT COUNT(*) FROM studies"), bind).scalar() or 0)

    people, mods = _Index(), _Index()
    classes = {c: i for i, c in enumerate(CLASS_ORDER)}
    for r in rows:
        m, c = mods(r['modality']), classes.get(r['patient_class'], classes['Other'])
        for stage, at, user, alias, left, role in (
                ('wetread', r['wet_at'], r['wet_user'], r['wet_alias'], r['wet_left'], r['wet_role']),
                ('approve', r['appr_at'], r['appr_user'], r['appr_alias'], r['appr_left'], r['appr_role'])):
            if at is None:
                continue
            name, label = _person(user, alias, left, role)
            p = people(name, {'name': name, 'role': label})
            out[stage].append([p, m, c, _minutes(r['exam_done_at'], at), _minutes(r['arrived_at'], at)])
    out['people'], out['modalities'] = people.items, mods.items
    out['wet_studies'], out['approved_studies'] = len(out['wetread']), len(out['approve'])
    return out


# ── b. reports per radiologist per day per modality (Report 32) ──────────────

def per_day(start, end, params=None, filters=''):
    """Report 32's "Reports per Radiologist per Day" card: approvals and wet reads per person,
    per day, per modality, for the days start..end the audit copy holds. params / filters:
    Report 32's secondary filters (get_report_config's sec_filters on aliases s and m)."""
    out = {'coverage': None, 'from': None, 'to': None, 'days': [], 'people': [], 'modalities': [],
           'kinds': list(KINDS), 'rows': []}
    audit_from, audit_until, coverage = _coverage()
    if coverage is None:
        return out
    out['coverage'] = coverage
    first = max(_as_date(start), audit_from.date())
    last = min(_as_date(end), audit_until.date())
    if last < first:
        return out
    out['from'], out['to'] = first.isoformat(), last.isoformat()
    out['days'] = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    bind = {**(params or {}), 'day_from': datetime.combine(first, datetime.min.time()),
            'day_to': datetime.combine(last + timedelta(days=1), datetime.min.time())}
    sql = _PER_DAY_SQL.format(roles=role_lookup_cte('role_lookup'), filters=filters or '',
                              login=staff_status.login_sql('c.user_name'),
                              left_logins=staff_status.left_logins_sql())
    days = {d: i for i, d in enumerate(out['days'])}
    people, mods = _Index(), _Index()
    merged = {}
    for r in db.session.execute(text(sql), bind).mappings().all():
        name, label = _person(r['user_name'], r['alias'], r['left_'], r['role'])
        key = (KINDS.index(r['kind']), people(name, {'name': name, 'role': label}),
               days.get(r['day'].isoformat() if hasattr(r['day'], 'isoformat') else str(r['day'])),
               mods(r['modality']))
        if key[2] is None:
            continue
        merged[key] = merged.get(key, 0) + int(r['n'])       # two logins, one name: one person
    out['rows'] = [[*k, n] for k, n in sorted(merged.items())]
    out['people'], out['modalities'] = people.items, mods.items
    return out


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
