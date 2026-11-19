"""
utils/sign_tat.py — radiologist signature turnaround from the PACS audit trail (items 14A and
14B, operator 2026-11-17), shown on Report 32 (Radiologists Performance).

The signature is the radiologist's approval in the PACS audit trail: the first "approve"
event of the study (status -> PENDING APPROVED, or ApprovedSign Complete), with its login and
second. At LAUMC approved is the final signature; the PACS holds the report 15 minutes (its
grace period) before writing APPROVED, with no login, so the approval is the radiologist's
act. Proven 2026-11-17: 458 studies approved on 16 Nov in the audit, 455 in the PACS studies
view (which keeps only each study's latest approval), 13 radiologists both ways.

Four clocks end at that signature:
  exam_to_sign         RIS exam done (std_worklist_exam_done, the first)          (14A, chart 1)
  arrival_to_sign      images reach PACS (etl_didb_studies.insert_time)            (14A, chart 2)
  open_to_sign         first open by a resident or radiologist (utils/pacs_roles)  (14B)
  signer_open_to_sign  the signing radiologist's own first open                    (14B)
An open is "open" or "lock" in the audit (Report 37's first open, the same events). A clock
whose start is missing, or later than the signature, is left out of that clock only.

Only studies that arrived while the audit copy holds them count (settings
pacs_audit_copied_from/until): before that RAYD has no approvals. Reads Postgres only.

A signer who left (item 9, utils/staff_status.py) reads "Former staff" while that switch is on;
the audit copy has no rh view to do it, so it is done here.

READING TAT, the radiologists' KPI (operator, 2026-11-19), on Report 36 (reading()):
  reading_to_sign      the approving radiologist's first open AFTER the study's wet read
                       (Signed 1, the first 'wetread' event), or their first open at all when
                       the study had no wet read, up to their approval
The KPI card shows it for studies with and without a wet read and both together, in bands
(BANDS), with median and 90th percentile, per radiologist and per modality. An approved study
whose approver never opened it after the wet read (or at all) is counted aside, not measured.
Report 36's comparison tab sets it beside RIS exam done → approval and PACS arrival →
approval (COMPARE), all three ending at the same approval, overall, on the studies that have
all three, and per modality, patient class and radiologist.
"""
import statistics

from sqlalchemy import text

from db import db
from ETL_JOBS.etl_pacs_audit import coverage as audit_coverage
from utils import staff_status
from utils.pacs_roles import role_lookup_cte

MEASURES = (
    ('exam_to_sign', 'exam_done_at', 'RIS exam done → signature'),
    ('arrival_to_sign', 'arrived_at', 'Images reach PACS → signature'),
    ('open_to_sign', 'first_open_at', 'First open → signature'),
    ('signer_open_to_sign', 'signer_open_at', "Signer's own first open → signature"),
)

# The radiologists' reading clock and the comparison on Report 36 (see the module notes).
COMPARE = (
    ('exam_to_sign', 'RIS exam done → approval'),
    ('arrival_to_sign', 'PACS arrival → approval'),
    ('reading_to_sign', 'First open after the wet read → approval'),
)
BANDS = ((60, '≤ 1 h'), (180, '1–3 h'), (300, '3–5 h'), (None, '> 5 h'))   # minutes, upper bounds

_STUDIES = """
studies AS MATERIALIZED (
    SELECT s.study_db_uid, BTRIM(s.study_instance_uid) AS uid, s.insert_time AS arrived_at,
           COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
           CASE WHEN s.patient_location = 'ER' THEN 'ER'
                WHEN s.patient_class = 'I' THEN 'Inpatient'
                WHEN s.patient_class = 'O' THEN 'Outpatient'
                ELSE 'Other' END AS patient_class_bucket
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE s.study_date BETWEEN :start AND :end
      AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT', 'BMD')
      AND s.insert_time >= :audit_from AND s.insert_time < :audit_until
      AND s.study_instance_uid IS NOT NULL
      {filters}
)"""

_SQL = """
WITH {roles},
""" + _STUDIES + """,
signed AS MATERIALIZED (
    SELECT st.study_db_uid, st.uid, st.arrived_at, st.modality, st.patient_class_bucket,
           sg.event_at AS signed_at, sg.user_name AS signer,
           (SELECT MIN(w.event_at) FROM etl_pacs_audit_events w
             WHERE w.study_instance_uid = st.uid AND w.kind = 'wetread'
               AND w.event_at <= sg.event_at) AS wetread_at
    FROM studies st
    JOIN LATERAL (
        SELECT e.event_at, e.user_name FROM etl_pacs_audit_events e
        WHERE e.study_instance_uid = st.uid AND e.kind = 'approve'
        ORDER BY e.event_at, e.log_dbid
        LIMIT 1
    ) sg ON TRUE
)
SELECT g.study_db_uid, g.arrived_at, g.signed_at, g.signer, g.modality, g.patient_class_bucket,
       g.wetread_at,
       -- the approver's first open after the wet read (their first open at all with none)
       (SELECT MIN(e.event_at) FROM etl_pacs_audit_events e
         WHERE e.study_instance_uid = g.uid AND e.kind IN ('open', 'lock')
           AND e.user_name = g.signer AND e.event_at <= g.signed_at
           AND (g.wetread_at IS NULL OR e.event_at >= g.wetread_at)) AS reading_open_at,
       alias.canonical_name AS signer_alias,
       {signer_login} IN {left_logins} AS signer_left,
       (SELECT MIN(w.exam_done_at) FROM etl_orders o
          JOIN std_worklist_exam_done w ON w.site_worklist_key = o.order_dbid
         WHERE o.study_db_uid = g.study_db_uid) AS exam_done_at,
       (SELECT MIN(e.event_at) FROM etl_pacs_audit_events e
          JOIN role_lookup r ON r.login_id = UPPER(SPLIT_PART(e.user_name, '@', 1))
         WHERE e.study_instance_uid = g.uid AND e.kind IN ('open', 'lock')) AS first_open_at,
       (SELECT MIN(e.event_at) FROM etl_pacs_audit_events e
         WHERE e.study_instance_uid = g.uid AND e.kind IN ('open', 'lock')
           AND e.user_name = g.signer) AS signer_open_at
FROM signed g
LEFT JOIN physician_alias_map alias ON alias.dismissed = false AND alias.alias = g.signer
"""


def _minutes(start, end):
    if start is None or end is None or end < start:
        return None
    return (end - start).total_seconds() / 60


def _stats(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return {'n': 0, 'median_h': None, 'p90_h': None}
    p90 = values[min(len(values) - 1, int(len(values) * 0.9))]
    return {'n': len(values), 'median_h': round(statistics.median(values) / 60, 2), 'p90_h': round(p90 / 60, 2)}


def _bands(values):
    """[{'label', 'n', 'pct'}] of the measured minutes in each BANDS window."""
    values = [v for v in values if v is not None]
    out, low = [], None
    for upper, label in BANDS:           # (low, upper]: the first window starts at 0 included
        n = sum(1 for v in values if (low is None or v > low) and (upper is None or v <= upper))
        out.append({'label': label, 'n': n, 'pct': round(100.0 * n / len(values), 1) if values else None})
        low = upper
    return out


def _fetch(start, end, params, filters):
    """(coverage, studies in range and in the audit copy, one row per approved study)."""
    audit_from, audit_until = audit_coverage(db.session)
    if audit_from is None:
        return None, 0, []
    coverage = {'from': audit_from.strftime('%Y-%m-%d'), 'until': audit_until.strftime('%Y-%m-%d %H:%M')}
    bind = {**(params or {}), 'start': start, 'end': end, 'audit_from': audit_from, 'audit_until': audit_until}
    studies = int(db.session.execute(text(
        'WITH ' + _STUDIES.format(filters=filters or '') + ' SELECT COUNT(*) FROM studies'), bind).scalar() or 0)
    rows = db.session.execute(text(_SQL.format(
        roles=role_lookup_cte('role_lookup'), filters=filters or '',
        signer_login=staff_status.login_sql('g.signer'), left_logins=staff_status.left_logins_sql())),
        bind).mappings().all()
    return coverage, studies, rows


def _cases(rows):
    from utils.sticky_notes import person_name
    cases = []
    for r in rows:
        # The audit copy has no rh view: someone who left is masked here (item 9)
        case = ({'signer': staff_status.FORMER, 'login': None} if r['signer_left']
                else {'signer': person_name(r['signer_alias'], r['signer']), 'login': r['signer']})
        for key, start_col, _ in MEASURES:
            case[key] = _minutes(r[start_col], r['signed_at'])
        case['reading_to_sign'] = _minutes(r['reading_open_at'], r['signed_at'])
        case.update(modality=r['modality'], patient_class=r['patient_class_bucket'],
                    wet_read=r['wetread_at'] is not None)
        cases.append(case)
    return cases


def build(start, end, params=None, filters=''):
    """Report 32's turnaround block for studies dated start..end. params / filters: Report
    32's secondary filters (modality, patient class, site devices: get_report_config's
    sec_filters on aliases s and m, and their parameters)."""
    out = {'coverage': None, 'studies': 0, 'signed': 0,
           'measures': [{'key': k, 'label': label} for k, _, label in MEASURES],
           'overall': {}, 'by_rad': []}
    coverage, studies, rows = _fetch(start, end, params, filters)
    if coverage is None:
        return out
    out.update(coverage=coverage, studies=studies, signed=len(rows))
    if not rows:
        return out

    cases = _cases(rows)
    out['overall'] = {key: _stats([c[key] for c in cases]) for key, _, _ in MEASURES}

    by_rad = {}
    for c in cases:
        by_rad.setdefault((c['signer'], c['login']), []).append(c)
    out['by_rad'] = sorted(
        ({'name': name, 'login': login, 'signed': len(items),
          **{key: _stats([c[key] for c in items]) for key, _, _ in MEASURES}}
         for (name, login), items in by_rad.items()),
        key=lambda r: (-r['signed'], r['name'] or ''))
    return out


def _kpi(cases):
    values = [c['reading_to_sign'] for c in cases]
    return {**_stats(values), 'studies': len(cases), 'bands': _bands(values)}


def _grouped(cases, field):
    groups = {}
    for c in cases:
        groups.setdefault(c[field], []).append(c)
    return groups


def reading(start, end, params=None, filters=''):
    """Report 36: the radiologists' reading TAT KPI card and the three-clock comparison tab,
    for studies dated start..end. params / filters: Report 36's left-panel filters
    (utils.report_filters.sidebar_filters, aliases s and m)."""
    out = {'coverage': None, 'studies': 0, 'approved': 0, 'not_measured': 0,
           'bands': [label for _, label in BANDS],
           'kpi': {}, 'by_rad': [], 'by_modality': [],
           'clocks': [{'key': k, 'label': label} for k, label in COMPARE],
           'compare': {}, 'common': {}, 'compare_by_modality': [], 'compare_by_class': [],
           'compare_by_rad': []}
    coverage, studies, rows = _fetch(start, end, params, filters)
    if coverage is None:
        return out
    cases = _cases(rows)
    out.update(coverage=coverage, studies=studies, approved=len(cases),
               not_measured=sum(1 for c in cases if c['reading_to_sign'] is None))
    if not cases:
        return out

    # the KPI card
    out['kpi'] = {'all': _kpi(cases),
                  'wet': _kpi([c for c in cases if c['wet_read']]),
                  'no_wet': _kpi([c for c in cases if not c['wet_read']])}
    by_rad = _grouped(cases, 'signer')
    logins = {c['signer']: c['login'] for c in cases}
    out['by_rad'] = sorted(({'name': name, 'login': logins.get(name), **_kpi(items)}
                            for name, items in by_rad.items()),
                           key=lambda r: (-r['studies'], r['name'] or ''))
    out['by_modality'] = sorted(({'modality': mod, **_kpi(items)}
                                 for mod, items in _grouped(cases, 'modality').items()),
                                key=lambda r: (-r['studies'], r['modality']))

    # the comparison tab: the three clocks, each on the studies that have it, and together on
    # the studies that have all three
    keys = [k for k, _ in COMPARE]
    clocks = lambda items: {k: _stats([c[k] for c in items]) for k in keys}
    out['compare'] = clocks(cases)
    common = [c for c in cases if all(c[k] is not None for k in keys)]
    out['common'] = {'n': len(common), **clocks(common)}
    out['compare_by_modality'] = sorted(({'modality': mod, 'studies': len(items), **clocks(items)}
                                         for mod, items in _grouped(cases, 'modality').items()),
                                        key=lambda r: (-r['studies'], r['modality']))
    order = {'ER': 0, 'Inpatient': 1, 'Outpatient': 2, 'Other': 3}
    out['compare_by_class'] = sorted(({'patient_class': pc, 'studies': len(items), **clocks(items)}
                                      for pc, items in _grouped(cases, 'patient_class').items()),
                                     key=lambda r: order.get(r['patient_class'], 9))
    out['compare_by_rad'] = sorted(({'name': name, 'studies': len(items), **clocks(items)}
                                    for name, items in by_rad.items()),
                                   key=lambda r: (-r['studies'], r['name'] or ''))
    return out
