"""
routes/sticky_notes.py — the JCI urgent read measure, on a card next to the ER card.

JCI asks how long a resident takes from opening the images of an urgent ER or inpatient
case to writing the sticky note for the next reader. Reads Postgres only: the data comes
through the nightly ETL (ETL_JOBS/etl_sticky_notes.py), because nothing else may keep
reading the PACS server.

WHAT THE CLOCK IS. PACS keeps no history of opens that RAYD can read, so the limit runs
from the images reaching PACS (insert_time) to the first real note, and the page says so.
"Open seen" is shown beside it but is not the JCI clock: it is the last open before the
note, taken from the first nightly copy after the note, and only when that open was by the
note's author. It is missing when someone reopened the study before the copy. Its time is
LAST_ACCESS_TIME, because closing a study moves STUDY_LOCK_TIME and leaves that one alone.

WHAT COUNTS AS URGENT. ER always counts. An admin ticks the other departments, here rather
than per viewer, so the people being measured cannot change what is measured. RIS has no
reliable urgency yet (PACS priority is only ever R or blank), so "requested as urgent"
waits for the HIS work.

A case's department is the RIS ordering organization, unless it is the Generic one, then
the PACS location, unless it is DEFLOC, otherwise "Department unknown", which stays visible.

Note text is never read. PACS cannot show who received a note, so the page shows who wrote
it and who signed next.
"""
import csv
import io
import json
import logging
import statistics
from datetime import date, datetime, timedelta

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_login import current_user, login_required
from sqlalchemy import text

from db import db, user_has_page
from utils import tat_sla
from utils.pacs_roles import role_lookup_cte
from utils.report_filters import REPORTS_WITHOUT_IMAGES_AES, study_site_sql
from utils.site_resolver import default_site

logger = logging.getLogger(__name__)

sticky_notes_bp = Blueprint('sticky_notes', __name__)

PAGE_KEY = 'sticky_notes'
URGENT_KEY = 'sticky_notes_urgent_departments'
LIMIT_KEY = 'sticky_notes_limit_minutes'
UNKNOWN_DEPARTMENT = 'Department unknown'
MAX_CASES_ON_PAGE = 2000
CLOCK_LABEL = 'Images reach PACS to first sticky note'
OPEN_SEEN_NOTE = ('Open seen is the last open by the note\'s author before the note, from the '
                  'next nightly copy. It is not the first open and is missing on studies '
                  'reopened before that copy.')

_CASES_SQL = """
WITH {roles},
candidates AS MATERIALIZED (
    SELECT s.study_db_uid, s.accession_number, s.insert_time,
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
       n.user_name AS note_author, n.date_created AS note_at, rl.role AS author_role,
       CASE WHEN h.last_accessed_time <= n.date_created
             AND UPPER(h.rep_last_accessed_by) = UPPER(n.user_name)
            THEN h.last_accessed_time END AS open_at,
       x.exam_done_at, sig.signed_at, sig.signed_by
FROM candidates c
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
LEFT JOIN LATERAL (
    SELECT a.last_accessed_time, a.rep_last_accessed_by
    FROM study_access_history a
    WHERE a.study_db_uid = c.study_db_uid
      AND a.captured_at >= n.date_created
    ORDER BY a.captured_at
    LIMIT 1
) h ON TRUE
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


def _require_page():
    if current_user.role != 'admin' and not user_has_page(current_user, PAGE_KEY):
        abort(403)


def _is_er_name(name):
    return (name or '').strip().upper() == 'ER' or 'emergency' in (name or '').lower()


def _load_settings():
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


def _read_filters():
    """Dates and the modalities a viewer has unticked. Every modality starts ticked, so
    nothing is hidden until someone chooses to hide it."""
    today = date.today()
    try:
        start = date.fromisoformat(request.args.get('start') or (today - timedelta(days=30)).isoformat())
        end = date.fromisoformat(request.args.get('end') or today.isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    unticked = {m for m in (request.args.get('unticked') or '').split(',') if m}
    return start, end, unticked


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


def _minutes(earlier, later):
    """Whole-seconds difference in minutes. Floored at 0: images re-sent after a note can
    leave a later arrival time than the note, which is not a negative wait."""
    if earlier is None or later is None:
        return None
    return round(max((later - earlier).total_seconds(), 0) / 60, 1)


def _shape_case(row, limit):
    minutes = _minutes(row['arrived_at'], row['note_at'])
    if row['note_at'] is None:
        outcome = 'no_note'
    elif limit is None:
        outcome = 'noted'
    else:
        outcome = 'within' if minutes <= limit else 'over'
    return {
        **row,
        'minutes_to_note': minutes,
        'minutes_open_to_note': _minutes(row['open_at'], row['note_at']),
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


def _breakdown(cases, field):
    groups = {}
    for case in cases:
        groups.setdefault(case[field], []).append(case)
    rows = [{field: name, **_block(items)} for name, items in groups.items()]
    return sorted(rows, key=lambda r: (-r['cases'], str(r[field])))


def _summarise(cases, limit, unticked, all_modalities):
    kept = [c for c in cases if c['modality'] not in unticked]
    summary = _block(kept)
    open_gaps = [c['minutes_open_to_note'] for c in kept]
    sign_gaps = [c['minutes_exam_to_sign'] for c in kept]
    summary.update({
        'open_seen': sum(g is not None for g in open_gaps),
        'median_open_to_note': _median(open_gaps),
        'exam_to_sign_cases': sum(g is not None for g in sign_gaps),
        'median_exam_to_sign': _median(sign_gaps),
    })
    return kept, {
        'kpi': summary,
        'by_department': _breakdown(kept, 'department'),
        'by_modality': _breakdown(kept, 'modality'),
        'modalities': sorted(all_modalities),
        'unticked': sorted(unticked),
        'limit_minutes': limit,
    }


def _when(value):
    return value.strftime('%Y-%m-%d %H:%M') if isinstance(value, datetime) else None


def _case_json(case):
    return {
        'accession': case['accession_number'], 'arrived_at': _when(case['arrived_at']),
        'modality': case['modality'], 'department': case['department'], 'is_er': case['is_er'],
        'note_at': _when(case['note_at']), 'minutes_to_note': case['minutes_to_note'],
        'note_author': case['note_author'], 'author_role': case['author_role'],
        'open_at': _when(case['open_at']), 'minutes_open_to_note': case['minutes_open_to_note'],
        'exam_done_at': _when(case['exam_done_at']), 'signed_at': _when(case['signed_at']),
        'signed_by': case['signed_by'], 'minutes_exam_to_sign': case['minutes_exam_to_sign'],
        'outcome': case['outcome'],
    }


def _department_options():
    site_clause, site_params = _site_scope()
    params = {'since': date.today() - timedelta(days=365), **site_params}
    rows = db.session.execute(text(_DEPARTMENT_OPTIONS_SQL.format(site=site_clause)), params).mappings().all()
    return [{'name': r['name'], 'count': int(r['n'])} for r in rows if not _is_er_name(r['name'])]


def _load(start, end, unticked):
    urgent, limit = _load_settings()
    cases = [_shape_case(r, limit) for r in _fetch_cases(start, end, urgent)]
    kept, summary = _summarise(cases, limit, unticked, {c['modality'] for c in cases})
    summary['urgent_departments'] = urgent
    return kept, summary


@sticky_notes_bp.route('/sticky-notes')
@login_required
def page():
    _require_page()
    today = date.today()
    return render_template('sticky_notes.html',
                           default_start=(today - timedelta(days=30)).isoformat(),
                           default_end=today.isoformat(),
                           clock_label=CLOCK_LABEL, open_seen_note=OPEN_SEEN_NOTE,
                           is_admin=current_user.role == 'admin')


@sticky_notes_bp.route('/sticky-notes/data')
@login_required
def data():
    _require_page()
    start, end, unticked = _read_filters()
    from utils.audit import log_event
    log_event('sticky_notes_accessed', category='report', resource_type='sticky_notes',
              detail={'from': start.isoformat(), 'to': end.isoformat(), 'unticked': sorted(unticked)})
    try:
        kept, summary = _load(start, end, unticked)
        summary['cases'] = [_case_json(c) for c in kept[:MAX_CASES_ON_PAGE]]
        summary['cases_total'] = len(kept)
        summary['department_options'] = _department_options() if current_user.role == 'admin' else []
        return jsonify(summary)
    except Exception:
        db.session.rollback()
        logger.exception('Sticky Notes page failed')
        return jsonify({'error': 'The sticky notes data could not be loaded.'}), 500


def _csv_cell(value):
    """Keeps a spreadsheet from running a cell as a formula."""
    if value is None:
        return ''
    value = str(value)
    return "'" + value if value[0:1] in ('=', '+', '-', '@') else value


@sticky_notes_bp.route('/sticky-notes/export.csv')
@login_required
def export_csv():
    _require_page()
    start, end, unticked = _read_filters()
    kept, summary = _load(start, end, unticked)
    from utils.audit import log_event
    log_event('sticky_notes_exported', category='report', resource_type='sticky_notes',
              detail={'from': start.isoformat(), 'to': end.isoformat(), 'rows': len(kept)})

    selected = [m for m in summary['modalities'] if m not in unticked]
    limit = summary['limit_minutes']
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(['Sticky Notes: urgent reads'])
    writer.writerow(['Period', start.isoformat(), end.isoformat()])
    writer.writerow(['Modalities counted', ', '.join(selected) or 'none'])
    writer.writerow(['Modalities left out', ', '.join(sorted(unticked)) or 'none'])
    writer.writerow(['Urgent departments', ', '.join(['ER (always)'] + summary['urgent_departments'])])
    writer.writerow(['Clock', CLOCK_LABEL])
    writer.writerow(['Limit (minutes)', limit if limit is not None else 'not set'])
    writer.writerow(['Note', OPEN_SEEN_NOTE])
    writer.writerow([])
    writer.writerow(['Accession', 'Images reached PACS', 'Modality', 'Department', 'ER', 'Note written',
                     'Minutes to note', 'Note author', 'Author role', 'Open seen', 'Minutes open to note',
                     'Exam done', 'First signature', 'Signed by', 'Minutes exam done to signature', 'Outcome'])
    for case in kept:
        c = _case_json(case)
        writer.writerow([_csv_cell(v) for v in (
            c['accession'], c['arrived_at'], c['modality'], c['department'], 'yes' if c['is_er'] else 'no',
            c['note_at'], c['minutes_to_note'], c['note_author'], c['author_role'], c['open_at'],
            c['minutes_open_to_note'], c['exam_done_at'], c['signed_at'], c['signed_by'],
            c['minutes_exam_to_sign'], c['outcome'])])
    return Response(out.getvalue(), mimetype='text/csv', headers={
        'Content-Disposition': f'attachment; filename=sticky_notes_{start}_{end}.csv'})


@sticky_notes_bp.route('/sticky-notes/settings', methods=['POST'])
@login_required
def save_settings():
    """Admin only: the people being measured must not be able to change the measure."""
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Admin only'}), 403
    payload = request.get_json(force=True, silent=True) or {}
    departments = payload.get('departments', [])
    if not isinstance(departments, list) or not all(isinstance(d, str) for d in departments):
        return jsonify({'status': 'error', 'message': 'Departments must be a list of names'}), 400
    departments = sorted({d.strip() for d in departments if d.strip()})

    raw_limit = payload.get('limit_minutes')
    limit = None
    if raw_limit not in (None, ''):
        try:
            limit = float(raw_limit)
        except (TypeError, ValueError):
            return jsonify({'status': 'error', 'message': 'The limit must be a number of minutes'}), 400
        if limit <= 0:
            return jsonify({'status': 'error', 'message': 'The limit must be above 0'}), 400

    upsert = text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                  "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    db.session.execute(upsert, {'k': URGENT_KEY, 'v': json.dumps(departments)})
    if limit is None:
        db.session.execute(text("DELETE FROM settings WHERE key = :k"), {'k': LIMIT_KEY})
    else:
        db.session.execute(upsert, {'k': LIMIT_KEY, 'v': str(limit)})
    db.session.commit()

    from utils.audit import log_event
    log_event('sticky_notes_settings_changed', category='admin', resource_type='sticky_notes',
              detail={'departments': departments, 'limit_minutes': limit})
    return jsonify({'status': 'success', 'departments': departments, 'limit_minutes': limit})
