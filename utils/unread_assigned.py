"""
utils/unread_assigned.py — item 45 (customer, 27 Nov 2026): exams waiting to be read (current RIS
status in the exam-done family) per ASSIGNED RADIOLOGIST, Report 36's tab "Waiting to be read".

Where the assignment lives (checked on the RIS in three rounds, 27 Nov,
scripts/sql/assigned_radiologist_oracle.sql): SITE_PPS.ASSIGNED_TO, a RESOURCE_ID_KEY written as
text on the exam's performed step. It is set before the report (236 of 239 exams in Exam Done
carried it, even 70 still Scheduled) and the assigned radiologist approved 98.8 % of RH's and 92 %
of SJH's approved exams of the last 30 days: an assignment, not just the reader. The columns
whose names say "assigned" are not it: SITE_WORKLIST.ASSIGNRADCODEPERSONKEY and
ORDER_PERSON_REFERENCE type 1703 are never filled, SITE_ORDERS.ASSIGNRAD stopped in 2023 / Jan 2026.

RAYD's copies, all nightly, no new ETL: etl_orders (one row per RIS worklist row, its current
status_key), std_worklist_exam_done (worklist row -> its step pps_key and exam-done time),
std_site_pps_ext (SITE_PPS, re-read in full every night, so a changed assignment arrives the next
morning), std_resources_ris (the radiologist's name; staff who left read "Former staff" through
the views). "Unread" = a status whose stage is exam_done in worklist_status_map (100 Exam Done,
2022 Pending and the other aliases), never a hard-coded list.

One radiologist can hold several resource keys and even two person rows (Perla Audi: three keys;
Daniel Mahfoud: two persons at SJH), so exams are counted per NAME (case and spaces ignored).
An exam with no step, no assignment or a key that names nobody counts as "Not assigned".
The age of an exam = days since its last RIS exam done, on the hospital's calendar.
"""
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

from db import db

HOSPITAL_TZ = ZoneInfo('Asia/Beirut')

# (key, label): the bands shown in the table, in order (colours b1..b4 of Report 36's band bar)
AGE_BANDS = (('today', 'Done today'), ('d1_7', '1-7 days'), ('d8_30', '8-30 days'), ('over30', 'Over 30 days'))

NOT_ASSIGNED = 'Not assigned'

_SQL = """
WITH unread AS (
    SELECT o.order_dbid, NULLIF(BTRIM(o.modality), '') AS modality, ed.pps_key, ed.exam_done_at
    FROM etl_orders o
    JOIN worklist_status_map sm ON sm.status_key = o.status_key AND sm.stage = 'exam_done'
    LEFT JOIN LATERAL (
        SELECT e.pps_key, e.exam_done_at
        FROM std_worklist_exam_done e
        WHERE e.site_worklist_key = o.order_dbid
        ORDER BY e.exam_done_at DESC
        LIMIT 1
    ) ed ON TRUE
)
SELECT COALESCE(NULLIF(BTRIM(CONCAT(r.first_name, ' ', r.last_name)), ''),
                NULLIF(BTRIM(r.common_name), '')) AS name,
       u.modality,
       CAST(u.exam_done_at AS date) AS done_day,
       u.exam_done_at
FROM unread u
LEFT JOIN std_site_pps_ext x ON x.pps_key = u.pps_key
LEFT JOIN std_resources_ris r ON CAST(r.resource_id_key AS text) = BTRIM(x.assigned_to)
"""


def hospital_today():
    return datetime.now(HOSPITAL_TZ).date()


def _band(done_day, today):
    if done_day is None:
        return None
    days = (today - done_day).days
    if days <= 0:
        return 'today'
    if days <= 7:
        return 'd1_7'
    if days <= 30:
        return 'd8_30'
    return 'over30'


def build(today=None):
    """Exams waiting to be read on the served hospital, per assigned radiologist."""
    today = today or hospital_today()
    rows = db.session.execute(text(_SQL)).fetchall()
    as_of = db.session.execute(text("SELECT MAX(last_update) FROM std_site_pps_ext")).scalar()

    people = {}
    for name, modality, done_day, done_at in rows:
        clean = ' '.join(name.split()) if name else None
        key = clean.upper() if clean else None
        p = people.get(key)
        if p is None:
            p = people[key] = {'name': clean or NOT_ASSIGNED, 'assigned': clean is not None,
                               'spellings': defaultdict(int), 'n': 0, 'no_date': 0, 'oldest': None,
                               'modalities': defaultdict(int), **{k: 0 for k, _ in AGE_BANDS}}
        if clean:
            p['spellings'][clean] += 1
        p['n'] += 1
        p['modalities'][modality or '?'] += 1
        band = _band(done_day, today)
        if band:
            p[band] += 1
        else:
            p['no_date'] += 1
        if done_at is not None and (p['oldest'] is None or done_at < p['oldest']):
            p['oldest'] = done_at

    out_rows = []
    for p in people.values():
        if p['spellings']:   # the spelling used most (then the one with lower case), not "PERLA AUDI"
            p['name'] = max(p['spellings'], key=lambda s: (p['spellings'][s], s != s.upper(), s))
        n = p['n']
        out_rows.append({
            'name': p['name'], 'assigned': p['assigned'], 'n': n, 'no_date': p['no_date'],
            'oldest': p['oldest'].strftime('%d/%m/%Y') if p['oldest'] else None,
            'oldest_days': (today - p['oldest'].date()).days if p['oldest'] else None,
            'modalities': sorted(p['modalities'].items(), key=lambda kv: (-kv[1], kv[0])),
            **{k: p[k] for k, _ in AGE_BANDS},
            'bands': [{'label': label, 'n': p[k], 'pct': round(100.0 * p[k] / n, 1) if n else 0}
                      for k, label in AGE_BANDS],
        })
    out_rows.sort(key=lambda r: (not r['assigned'], -r['n'], r['name'].upper()))

    total = sum(r['n'] for r in out_rows)
    not_assigned = sum(r['n'] for r in out_rows if not r['assigned'])
    return {
        'today': today.strftime('%d/%m/%Y'),
        'as_of': as_of.strftime('%d/%m/%Y %H:%M') if as_of else None,
        'total': total,
        'assigned': total - not_assigned,
        'not_assigned': not_assigned,
        'radiologists': sum(1 for r in out_rows if r['assigned']),
        'over_7': sum(r['d8_30'] + r['over30'] for r in out_rows),
        'bands': [label for _, label in AGE_BANDS],
        'rows': out_rows,
    }
