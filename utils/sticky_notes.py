"""
utils/sticky_notes.py — the JCI urgent emergent report. The page, its CSV and the monthly
workbook all read this, so they cannot disagree. Reads Postgres only: the data comes through
the nightly ETL (ETL_JOBS/etl_sticky_notes.py), a day late, because nothing else may keep
reading the PACS server.

THE REQUEST, in the customer's words: a monthly report of all exams classified urgent
emergent in the preceding month, with the date and time the resident or radiologist first
opens the study and the date and time a sticky note is inserted, plus patient ID, accession
number and the name of the user for each action.

FIRST OPEN IS NOT FULLY AVAILABLE. PACS keeps only the latest open and overwrites it, and no
history of opens is readable by RAYD. "First open seen" is the earliest open a nightly copy
caught that has a login: the last open before that copy, so a study opened twice the same day
shows the second. It stays blank when that open came after the note, because then the true
first open is known to be gone. It also stays blank when the copy that caught it came more than
36 hours after the study was last seen unopened, or arrived: the first copy (8 November 2026)
reached back 10 days, so it caught late October opens days late, and a missed night would do
the same. Its time is LAST_ACCESS_TIME, because closing a study moves STUDY_LOCK_TIME and
leaves that one alone. Nothing exists from before the copy began.

NAMES. PACS records logins. A name comes from physician_alias_map, as in the other reports,
otherwise from the login itself (first.last@ad becomes First Last), and the login stays
beside it.

WHAT COUNTS AS URGENT. PACS and RIS have no reliable class (PACS priority is only ever R or
blank), so ER always counts and an admin ticks the other departments, here rather than per
viewer, so the people being measured cannot change what is measured. A case's department is
the RIS ordering organization, unless it is the Generic one, then the PACS location, unless
it is DEFLOC, otherwise "Department unknown", which stays visible.

The limit and the first signature are not in the written request. They came from the
operator's description of the JCI requirement, and the limit runs from images reaching PACS.

Note text is never read. PACS cannot show who received a note.
"""
import io
import json
import re
import statistics
from datetime import date, datetime, timedelta

import xlsxwriter
from sqlalchemy import text

from db import db
from utils import tat_sla
from utils.pacs_roles import role_lookup_cte
from utils.report_filters import REPORTS_WITHOUT_IMAGES_AES, study_site_sql
from utils.site_resolver import default_site

URGENT_KEY = 'sticky_notes_urgent_departments'
LIMIT_KEY = 'sticky_notes_limit_minutes'
UNKNOWN_DEPARTMENT = 'Department unknown'
TITLE = 'Urgent emergent radiology exams: first open and sticky note'
CLOCK_LABEL = 'Images reach PACS to first sticky note'
OPEN_SEEN_NOTE = ('First open seen is the earliest open a nightly copy caught. PACS keeps only the '
                  'latest open, so a study opened again the same day shows the later open. It is '
                  'blank when the study was reopened after the note, and when the copy that caught '
                  'it came more than 36 hours after the study was last seen unopened (or arrived), '
                  'which covers studies from before the copy began and nights it did not run.')
OPEN_CAUGHT_WITHIN = timedelta(hours=36)
OUTCOMES = {'within': 'Within limit', 'over': 'Over limit', 'no_note': 'No note', 'noted': 'Noted'}
OPEN_BLANK_REASONS = {'late': 'Caught over 36 h after last seen unopened',
                      'overwritten': 'Reopened after the note',
                      'none': 'No open caught'}

# The customer's fields first, in their order, then what the operator added.
COLUMNS = (
    ('Patient ID', 'patient_id'),
    ('Accession', 'accession'),
    ('Modality', 'modality'),
    ('Department', 'department'),
    ('First open seen', 'open_at'),
    ('Opened by', 'opened_by'),
    ('Opened by login', 'opened_by_login'),
    ('First open blank because', 'open_blank_reason'),
    ('Sticky note inserted', 'note_at'),
    ('Note by', 'note_by'),
    ('Note by login', 'note_author'),
    ('Note by role', 'author_role'),
    ('Images reached PACS', 'arrived_at'),
    ('Minutes images to note', 'minutes_to_note'),
    ('Minutes first open to note', 'minutes_open_to_note'),
    ('Exam done', 'exam_done_at'),
    ('First signature', 'signed_at'),
    ('Signed by', 'signed_by'),
    ('Minutes exam done to signature', 'minutes_exam_to_sign'),
    ('Outcome', 'outcome_label'),
)

_CASES_SQL = """
WITH {roles},
candidates AS MATERIALIZED (
    SELECT s.study_db_uid, s.patient_db_uid, s.accession_number, s.insert_time,
           COALESCE(NULLIF(m.modality, 'PACS'), s.study_modality, 'Unknown') AS modality,
           COALESCE(ris.name,
                    NULLIF(NULLIF(BTRIM(s.patient_location_full), ''), 'DEFLOC'),
                    NULLIF(NULLIF(BTRIM(s.patient_location), ''), 'DEF'),
                    :unknown) AS department,
           (UPPER(BTRIM(COALESCE(s.patient_location_full, s.patient_location, ''))) = 'ER'
            OR UPPER(COALESCE(s.accession_number, '')) LIKE :er_prefix
            OR COALESCE(ris.name, '') ILIKE '%emergency%') AS is_er
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    LEFT JOIN LATERAL (
        SELECT org.name
        FROM etl_orders o
        JOIN std_ordering_organizations org
          ON org.ordering_organization_key = o.ordering_organization_key
        WHERE o.study_db_uid = s.study_db_uid
          AND org.name IS NOT NULL
          AND org.name NOT ILIKE 'generic%'
        ORDER BY o.order_dbid
        LIMIT 1
    ) ris ON TRUE
    WHERE s.insert_time >= :start AND s.insert_time < :end
      AND {site}
      AND UPPER(TRIM(COALESCE(s.storing_ae, ''))) <> ALL(CAST(:no_image_aes AS text[]))
      AND COALESCE(NULLIF(m.modality, 'PACS'), s.study_modality, '') <> 'SR'
)
SELECT c.accession_number, c.insert_time AS arrived_at, c.modality, c.department, c.is_er,
       p.fallback_id AS patient_id,
       n.user_name AS note_author, n.date_created AS note_at, rl.role AS author_role,
       note_name.canonical_name AS note_alias,
       h.last_accessed_time AS open_at, h.rep_last_accessed_by AS opened_by_login,
       h.captured_at AS open_caught_at,
       GREATEST(c.insert_time, h.previous_copy_at) AS open_unwatched_since,
       open_name.canonical_name AS open_alias,
       x.exam_done_at, sig.signed_at, sig.signed_by
FROM candidates c
LEFT JOIN etl_patient_view p ON p.patient_db_uid = c.patient_db_uid
LEFT JOIN LATERAL (
    SELECT t.user_name, t.date_created
    FROM etl_sticky_notes t
    WHERE t.study_db_uid = c.study_db_uid
      AND t.delete_indication = 'N'
      AND t.data_len > 1
    ORDER BY t.date_created
    LIMIT 1
) n ON TRUE
LEFT JOIN role_lookup rl ON rl.login_id = UPPER(SPLIT_PART(n.user_name, '@', 1))
LEFT JOIN physician_alias_map note_name
       ON note_name.dismissed = false AND note_name.alias = n.user_name
LEFT JOIN LATERAL (
    SELECT a.last_accessed_time, a.rep_last_accessed_by, a.captured_at,
           (SELECT MAX(b.captured_at) FROM study_access_history b
             WHERE b.study_db_uid = a.study_db_uid AND b.captured_at < a.captured_at) AS previous_copy_at
    FROM study_access_history a
    WHERE a.study_db_uid = c.study_db_uid
      AND a.last_accessed_time IS NOT NULL
      AND a.rep_last_accessed_by IS NOT NULL
    ORDER BY a.captured_at
    LIMIT 1
) h ON TRUE
LEFT JOIN physician_alias_map open_name
       ON open_name.dismissed = false AND open_name.alias = h.rep_last_accessed_by
LEFT JOIN LATERAL (
    SELECT MIN(w.exam_done_at) AS exam_done_at
    FROM etl_orders o
    JOIN std_worklist_exam_done w ON w.site_worklist_key = o.order_dbid
    WHERE o.study_db_uid = c.study_db_uid
) x ON TRUE
LEFT JOIN LATERAL (
    SELECT r.verified1_date AS signed_at,
           COALESCE(NULLIF(BTRIM(CONCAT(res.first_name, ' ', res.last_name)), ''),
                    NULLIF(BTRIM(res.common_name), '')) AS signed_by
    FROM std_reports cur
    JOIN std_reports r ON r.report_key = cur.report_key
    LEFT JOIN std_resources_ris res ON res.resource_id_key = r.verified1_by_resource_id_key
    WHERE cur.is_max_version
      AND cur.reported_acc_number = NULLIF(BTRIM(c.accession_number), '')
      AND r.verified1_date IS NOT NULL
    ORDER BY r.verified1_date
    LIMIT 1
) sig ON TRUE
WHERE c.is_er OR c.department = ANY(CAST(:urgent AS text[]))
ORDER BY c.insert_time DESC
"""

_DEPARTMENT_OPTIONS_SQL = """
    SELECT name, SUM(n)::bigint AS n FROM (
        SELECT org.name AS name, COUNT(*) AS n
        FROM etl_orders o
        JOIN std_ordering_organizations org
          ON org.ordering_organization_key = o.ordering_organization_key
        WHERE o.scheduled_datetime >= :since
          AND org.name NOT ILIKE 'generic%'
        GROUP BY org.name
        UNION ALL
        SELECT COALESCE(NULLIF(BTRIM(s.patient_location_full), ''),
                        NULLIF(BTRIM(s.patient_location), '')) AS name, COUNT(*) AS n
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.insert_time >= :since AND {site}
        GROUP BY 1
    ) d
    WHERE name IS NOT NULL AND name NOT IN ('DEFLOC', 'DEF')
    GROUP BY name
    ORDER BY name
"""


def previous_month(today=None):
    """(first day, last day) of the month before `today`."""
    first_of_this = (today or date.today()).replace(day=1)
    last = first_of_this - timedelta(days=1)
    return last.replace(day=1), last


def load_settings():
    rows = dict(db.session.execute(
        text("SELECT key, value FROM settings WHERE key IN (:u, :l)"),
        {'u': URGENT_KEY, 'l': LIMIT_KEY}).fetchall())
    try:
        urgent = json.loads(rows.get(URGENT_KEY) or '[]')
    except ValueError:
        urgent = []
    try:
        limit = float(rows[LIMIT_KEY]) if rows.get(LIMIT_KEY) else None
    except ValueError:
        limit = None
    return urgent, limit


def _site_scope():
    """RH only (operator, 2026-11-05): the SQL condition and its parameter."""
    site = default_site()
    return (study_site_sql('site'), {'site': site}) if site is not None else ('TRUE', {})


def _fetch_cases(start, end, urgent):
    site_clause, site_params = _site_scope()
    sql = _CASES_SQL.format(roles=role_lookup_cte('role_lookup'), site=site_clause)
    params = {
        'start': start, 'end': end + timedelta(days=1),
        'unknown': UNKNOWN_DEPARTMENT,
        'er_prefix': tat_sla._ER_ACCESSION_PREFIX + '%',
        'no_image_aes': list(REPORTS_WITHOUT_IMAGES_AES),
        'urgent': list(urgent),
        **site_params,
    }
    return [dict(r) for r in db.session.execute(text(sql), params).mappings().all()]


def person_name(alias, login):
    """The curated name if there is one, otherwise what the login says: first.last@ad is
    First Last. A login with no dots may be a shared account, so it stays as it is."""
    if alias:
        return alias
    if not login:
        return None
    local = login.split('@')[0]
    parts = [p for p in re.split(r'[._]', local) if p]
    return ' '.join(p.capitalize() for p in parts) if len(parts) > 1 else local


def _minutes(earlier, later):
    """Whole-seconds difference in minutes. Floored at 0: images re-sent after a note can
    leave a later arrival time than the note, which is not a negative wait."""
    if earlier is None or later is None:
        return None
    return round(max((later - earlier).total_seconds(), 0) / 60, 1)


def _caught_late(row):
    """The copy that caught the open came too long after the study was last seen unopened
    (its previous copy, or its arrival when it had none): more than one night unwatched, so
    an earlier open may have been overwritten. The first copy reached back 10 days, and a
    night can be missed; 36 hours allows a late run, not a skipped one."""
    if row['open_at'] is None or row['open_caught_at'] is None or row['open_unwatched_since'] is None:
        return False
    return row['open_caught_at'] - row['open_unwatched_since'] > OPEN_CAUGHT_WITHIN


def _shape_case(row, limit):
    minutes = _minutes(row['arrived_at'], row['note_at'])
    if row['note_at'] is None:
        outcome = 'no_note'
    elif limit is None:
        outcome = 'noted'
    else:
        outcome = 'within' if minutes <= limit else 'over'

    late = _caught_late(row)
    # An open after the note cannot be the first: whoever wrote it had the study open earlier.
    overwritten = (not late and row['open_at'] is not None and row['note_at'] is not None
                   and row['open_at'] > row['note_at'])
    hidden = late or overwritten
    open_at = None if hidden else row['open_at']
    open_login = None if hidden else row['opened_by_login']
    return {
        **row,
        'open_at': open_at,
        'opened_by_login': open_login,
        'opened_by': person_name(row['open_alias'], open_login),
        'open_overwritten': overwritten,
        'open_caught_late': late,
        'note_by': person_name(row['note_alias'], row['note_author']),
        'minutes_to_note': minutes,
        'minutes_open_to_note': _minutes(open_at, row['note_at']),
        'minutes_exam_to_sign': _minutes(row['exam_done_at'], row['signed_at']),
        'outcome': outcome,
    }


def _median(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 1) if values else None


def _p90(values):
    values = sorted(v for v in values if v is not None)
    return values[min(len(values) - 1, int(len(values) * 0.9))] if values else None


def _block(cases):
    """The same figures for the whole page and for each row of its breakdowns."""
    noted = [c for c in cases if c['note_at'] is not None]
    return {
        'cases': len(cases),
        'noted': len(noted),
        'no_note': len(cases) - len(noted),
        'noted_pct': round(len(noted) * 100 / len(cases), 1) if cases else None,
        'median_to_note': _median([c['minutes_to_note'] for c in noted]),
        'p90_to_note': _p90([c['minutes_to_note'] for c in noted]),
        'within': sum(c['outcome'] == 'within' for c in cases),
        'over': sum(c['outcome'] == 'over' for c in cases),
    }


# Time to note, in the bars of the page's first chart. "No note" is its own bar.
BUCKETS = (('< 15 min', 15), ('15–30 min', 30), ('30–60 min', 60), ('1–2 h', 120),
           ('2–4 h', 240), ('4–8 h', 480), ('8 h or more', None))
NO_NOTE = 'No note'
WEEKLY_AFTER_DAYS = 62      # a longer period is drawn week by week, not day by day


def bucket_of(minutes):
    if minutes is None:
        return NO_NOTE
    for label, upper in BUCKETS:
        if upper is None or minutes < upper:
            return label


def _outcome_counts(cases):
    """What each stacked bar of the page's charts is made of."""
    return {
        'cases': len(cases),
        'noted': sum(c['note_at'] is not None for c in cases),
        'within': sum(c['outcome'] == 'within' for c in cases),
        'over': sum(c['outcome'] == 'over' for c in cases),
        'no_note': sum(c['note_at'] is None for c in cases),
    }


def _trend_key(arrived, weekly):
    day = arrived.date()
    return (day - timedelta(days=day.weekday())).isoformat() if weekly else day.isoformat()


def _trend(cases, start, end):
    """One bar per day (per week for a long period), including the days with no case, so
    a gap in the chart is a real gap."""
    weekly = (end - start).days + 1 > WEEKLY_AFTER_DAYS
    keys, day = [], start
    while day <= end:
        key = _trend_key(datetime.combine(day, datetime.min.time()), weekly)
        if key not in keys:
            keys.append(key)
        day += timedelta(days=1)
    groups = {k: [] for k in keys}
    for case in cases:
        groups.setdefault(case['trend_key'], []).append(case)
    return 'week' if weekly else 'day', [{'key': k, **_outcome_counts(groups[k])} for k in keys]


def _breakdown(cases, field):
    groups = {}
    for case in cases:
        groups.setdefault(case[field], []).append(case)
    rows = [{field: name, **_block(items)} for name, items in groups.items()]
    return sorted(rows, key=lambda r: (-r['cases'], str(r[field])))


FILTERED_FIELDS = ('modality', 'department', 'outcome')


def build(start, end, chosen=None):
    """(cases kept after the report's filters, newest first; the figures about them).
    `chosen` maps modality, department and outcome to the values a viewer picked; a field
    left out, or given no values, hides nothing. The monthly workbook passes none."""
    chosen = {field: set(values) for field, values in (chosen or {}).items()
              if field in FILTERED_FIELDS and values}
    urgent, limit = load_settings()
    cases = [_shape_case(r, limit) for r in _fetch_cases(start, end, urgent)]
    kept = [c for c in cases if all(c[field] in values for field, values in chosen.items())]
    weekly = (end - start).days + 1 > WEEKLY_AFTER_DAYS
    for case in kept:
        case['bucket'] = bucket_of(case['minutes_to_note'])
        case['trend_key'] = _trend_key(case['arrived_at'], weekly)
    granularity, trend = _trend(kept, start, end)
    by_bucket = [{'key': label, **_outcome_counts([c for c in kept if c['bucket'] == label])}
                 for label in [b[0] for b in BUCKETS] + [NO_NOTE]]
    kpi = _block(kept)
    open_gaps = [c['minutes_open_to_note'] for c in kept]
    sign_gaps = [c['minutes_exam_to_sign'] for c in kept]
    kpi.update({
        'open_seen': sum(c['open_at'] is not None for c in kept),
        'open_overwritten': sum(c['open_overwritten'] for c in kept),
        'open_caught_late': sum(c['open_caught_late'] for c in kept),
        'median_open_to_note': _median(open_gaps),
        'exam_to_sign_cases': sum(g is not None for g in sign_gaps),
        'median_exam_to_sign': _median(sign_gaps),
    })
    return kept, {
        'kpi': kpi,
        'by_department': _breakdown(kept, 'department'),
        'by_modality': _breakdown(kept, 'modality'),
        'by_bucket': by_bucket,
        'trend': trend,
        'trend_granularity': granularity,
        # What each dropdown can offer for the period, before any filter, so choosing one
        # department does not take the others off the list.
        'choices': {'modality': sorted({c['modality'] for c in cases}),
                    'department': sorted({c['department'] for c in cases})},
        'filters': {field: sorted(values) for field, values in chosen.items()},
        'limit_minutes': limit,
        'urgent_departments': urgent,
    }


def _when(value):
    return value.strftime('%Y-%m-%d %H:%M') if isinstance(value, datetime) else None


def _open_blank_reason(case):
    if case['open_at'] is not None:
        return None
    if case['open_caught_late']:
        return OPEN_BLANK_REASONS['late']
    if case['open_overwritten']:
        return OPEN_BLANK_REASONS['overwritten']
    return OPEN_BLANK_REASONS['none']


def case_row(case):
    """One case as the page, the CSV and the workbook show it."""
    return {
        'patient_id': case['patient_id'], 'accession': case['accession_number'],
        'arrived_at': _when(case['arrived_at']),
        'modality': case['modality'], 'department': case['department'], 'is_er': case['is_er'],
        'note_at': _when(case['note_at']), 'minutes_to_note': case['minutes_to_note'],
        'note_by': case['note_by'], 'note_author': case['note_author'], 'author_role': case['author_role'],
        'open_at': _when(case['open_at']), 'opened_by': case['opened_by'],
        'opened_by_login': case['opened_by_login'], 'open_overwritten': case['open_overwritten'],
        'open_caught_late': case['open_caught_late'], 'open_blank_reason': _open_blank_reason(case),
        'minutes_open_to_note': case['minutes_open_to_note'],
        'exam_done_at': _when(case['exam_done_at']), 'signed_at': _when(case['signed_at']),
        'signed_by': case['signed_by'], 'minutes_exam_to_sign': case['minutes_exam_to_sign'],
        'outcome': case['outcome'], 'outcome_label': OUTCOMES[case['outcome']],
        'bucket': case.get('bucket'), 'trend_key': case.get('trend_key'),
    }


def describe(start, end, summary):
    """What was counted, as (label, value), for the top of every export."""
    filters = summary['filters']
    limit = summary['limit_minutes']
    return [
        ('Period', f'{start.isoformat()} to {end.isoformat()}'),
        ('Site', 'RH only'),
        ('Modalities', ', '.join(filters.get('modality', [])) or 'all'),
        ('Departments shown', ', '.join(filters.get('department', [])) or 'all urgent departments'),
        ('Outcomes shown', ', '.join(OUTCOMES[o] for o in filters.get('outcome', []) if o in OUTCOMES) or 'all'),
        ('Urgent departments', ', '.join(['ER (always)'] + summary['urgent_departments'])),
        ('Clock', CLOCK_LABEL),
        ('Limit (minutes)', limit if limit is not None else 'not set'),
        ('First open', OPEN_SEEN_NOTE),
        ('Sticky note recipient', 'PACS cannot show who received a note.'),
    ]


def department_options():
    site_clause, site_params = _site_scope()
    params = {'since': date.today() - timedelta(days=365), **site_params}
    rows = db.session.execute(text(_DEPARTMENT_OPTIONS_SQL.format(site=site_clause)), params).mappings().all()
    return [{'name': r['name'], 'count': int(r['n'])} for r in rows if not _is_er_name(r['name'])]


def _is_er_name(name):
    return (name or '').strip().upper() == 'ER' or 'emergency' in (name or '').lower()


def workbook(start, end, cases, summary, generated_at):
    """The report as an .xlsx. Text stays text: a value typed into the RIS must not run as a
    formula, and an ID like 00123 must keep its zeros."""
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False,
                                     'strings_to_urls': False, 'strings_to_numbers': False})
    sheet = book.add_worksheet('Urgent emergent')
    bold = book.add_format({'bold': True})
    title = book.add_format({'bold': True, 'font_size': 14})
    sheet.write(0, 0, TITLE, title)
    row = 1
    for label, value in [*describe(start, end, summary), ('Generated', generated_at.strftime('%Y-%m-%d %H:%M'))]:
        sheet.write(row, 0, label, bold)
        sheet.write(row, 1, value)
        row += 1
    row += 1
    sheet.write_row(row, 0, [header for header, _ in COLUMNS], bold)
    sheet.freeze_panes(row + 1, 0)
    first_data_row = row + 1
    for offset, case in enumerate(cases):
        shown = case_row(case)
        sheet.write_row(first_data_row + offset, 0, [shown[key] for _, key in COLUMNS])
    sheet.set_column(0, len(COLUMNS) - 1, 20)
    book.close()
    return buf.getvalue()
