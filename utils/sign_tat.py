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
"""
import statistics

from sqlalchemy import text

from db import db
from ETL_JOBS.etl_pacs_audit import coverage as audit_coverage
from utils.pacs_roles import role_lookup_cte

MEASURES = (
    ('exam_to_sign', 'exam_done_at', 'RIS exam done → signature'),
    ('arrival_to_sign', 'arrived_at', 'Images reach PACS → signature'),
    ('open_to_sign', 'first_open_at', 'First open → signature'),
    ('signer_open_to_sign', 'signer_open_at', "Signer's own first open → signature"),
)

_STUDIES = """
studies AS MATERIALIZED (
    SELECT s.study_db_uid, BTRIM(s.study_instance_uid) AS uid, s.insert_time AS arrived_at
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
    SELECT st.study_db_uid, st.uid, st.arrived_at, sg.event_at AS signed_at, sg.user_name AS signer
    FROM studies st
    JOIN LATERAL (
        SELECT e.event_at, e.user_name FROM etl_pacs_audit_events e
        WHERE e.study_instance_uid = st.uid AND e.kind = 'approve'
        ORDER BY e.event_at, e.log_dbid
        LIMIT 1
    ) sg ON TRUE
)
SELECT g.study_db_uid, g.arrived_at, g.signed_at, g.signer,
       alias.canonical_name AS signer_alias,
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


def build(start, end, params=None, filters=''):
    """Report 32's turnaround block for studies dated start..end. params / filters: Report
    32's secondary filters (modality, patient class, site devices: get_report_config's
    sec_filters on aliases s and m, and their parameters)."""
    from utils.sticky_notes import person_name
    audit_from, audit_until = audit_coverage(db.session)
    out = {'coverage': None, 'studies': 0, 'signed': 0,
           'measures': [{'key': k, 'label': label} for k, _, label in MEASURES],
           'overall': {}, 'by_rad': []}
    if audit_from is None:
        return out
    out['coverage'] = {'from': audit_from.strftime('%Y-%m-%d'), 'until': audit_until.strftime('%Y-%m-%d %H:%M')}
    bind = {**(params or {}), 'start': start, 'end': end, 'audit_from': audit_from, 'audit_until': audit_until}
    out['studies'] = int(db.session.execute(text(
        'WITH ' + _STUDIES.format(filters=filters or '') + ' SELECT COUNT(*) FROM studies'), bind).scalar() or 0)
    rows = db.session.execute(text(_SQL.format(roles=role_lookup_cte('role_lookup'), filters=filters or '')),
                              bind).mappings().all()
    out['signed'] = len(rows)
    if not rows:
        return out

    cases = []
    for r in rows:
        case = {'signer': person_name(r['signer_alias'], r['signer']), 'login': r['signer']}
        for key, start_col, _ in MEASURES:
            case[key] = _minutes(r[start_col], r['signed_at'])
        cases.append(case)
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
