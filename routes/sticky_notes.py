"""
routes/sticky_notes.py — the JCI urgent emergent report, on a card next to the ER card.

The page, the CSV and the settings. What they show is worked out in utils/sticky_notes.py,
which explains what the customer asked for and what PACS can give. The automated monthly
report is utils/sticky_notes_monthly.py, and the page lists and downloads what it kept.
"""
import csv
import io
import json
import logging
from datetime import date

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_login import current_user, login_required
from sqlalchemy import text

from db import db, user_has_page
from utils import sticky_notes as report
from utils import sticky_notes_monthly as monthly
from utils import worklist_email

logger = logging.getLogger(__name__)

sticky_notes_bp = Blueprint('sticky_notes', __name__)

PAGE_KEY = 'sticky_notes'
MAX_CASES_ON_PAGE = 2000
XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'


def _require_page():
    if current_user.role != 'admin' and not user_has_page(current_user, PAGE_KEY):
        abort(403)


def _read_filters():
    """Dates and the modalities a viewer has unticked. The dates start on the preceding month,
    which is what the customer asked for. Every modality starts ticked, so nothing is hidden
    until someone chooses to hide it."""
    first, last = report.previous_month()
    try:
        start = date.fromisoformat(request.args.get('start') or first.isoformat())
        end = date.fromisoformat(request.args.get('end') or last.isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    unticked = {m for m in (request.args.get('unticked') or '').split(',') if m}
    return start, end, unticked


@sticky_notes_bp.route('/sticky-notes')
@login_required
def page():
    _require_page()
    first, last = report.previous_month()
    return render_template('sticky_notes.html',
                           default_start=first.isoformat(), default_end=last.isoformat(),
                           clock_label=report.CLOCK_LABEL, open_seen_note=report.OPEN_SEEN_NOTE,
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
        kept, summary = report.build(start, end, unticked)
        summary['cases'] = [report.case_row(c) for c in kept[:MAX_CASES_ON_PAGE]]
        summary['cases_total'] = len(kept)
        summary['monthly_reports'] = monthly.list_reports()
        if current_user.role == 'admin':
            summary['department_options'] = report.department_options()
            summary['monthly_recipients'] = monthly.get_recipients()
            summary['mail_ready'] = worklist_email.is_ready(worklist_email.mail_config())
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
    kept, summary = report.build(start, end, unticked)
    from utils.audit import log_event
    log_event('sticky_notes_exported', category='report', resource_type='sticky_notes',
              detail={'from': start.isoformat(), 'to': end.isoformat(), 'rows': len(kept)})

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([report.TITLE])
    for label, value in report.describe(start, end, summary):
        writer.writerow([label, value])
    writer.writerow([])
    writer.writerow([header for header, _ in report.COLUMNS])
    for case in kept:
        shown = report.case_row(case)
        writer.writerow([_csv_cell(shown[key]) for _, key in report.COLUMNS])
    return Response(out.getvalue(), mimetype='text/csv', headers={
        'Content-Disposition': f'attachment; filename=urgent_emergent_{start}_{end}.csv'})


@sticky_notes_bp.route('/sticky-notes/monthly/<month>.xlsx')
@login_required
def monthly_workbook(month):
    _require_page()
    found = monthly.read_workbook(month)
    if found is None:
        abort(404)
    from utils.audit import log_event
    log_event('sticky_notes_monthly_downloaded', category='report', resource_type='sticky_notes',
              detail={'month': month})
    filename, content = found
    return Response(content, mimetype=XLSX, headers={'Content-Disposition': f'attachment; filename={filename}'})


@sticky_notes_bp.route('/sticky-notes/monthly/generate', methods=['POST'])
@login_required
def monthly_generate():
    """Admin only. Replaces what was kept for that month, on purpose, and sends it again."""
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Admin only'}), 403
    payload = request.get_json(force=True, silent=True) or {}
    try:
        first, kpi = monthly.generate_on_request(payload.get('month'))
    except monthly.MonthError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    emailed = monthly.send(first)
    from utils.audit import log_event
    log_event('sticky_notes_monthly_generated', category='admin', resource_type='sticky_notes',
              detail={'month': payload.get('month'), 'cases': kpi['cases'], 'emailed': emailed})
    return jsonify({'status': 'success', 'cases': kpi['cases'], 'emailed': emailed})


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

    recipients = None
    if 'recipients' in payload:
        try:
            recipients = monthly.save_recipients(payload['recipients'])
        except worklist_email.ScheduleError as exc:
            db.session.rollback()
            return jsonify({'status': 'error', 'message': str(exc)}), 400

    upsert = text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                  "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    db.session.execute(upsert, {'k': report.URGENT_KEY, 'v': json.dumps(departments)})
    if limit is None:
        db.session.execute(text("DELETE FROM settings WHERE key = :k"), {'k': report.LIMIT_KEY})
    else:
        db.session.execute(upsert, {'k': report.LIMIT_KEY, 'v': str(limit)})
    db.session.commit()

    from utils.audit import log_event
    log_event('sticky_notes_settings_changed', category='admin', resource_type='sticky_notes',
              detail={'departments': departments, 'limit_minutes': limit, 'recipients': recipients})
    return jsonify({'status': 'success', 'departments': departments, 'limit_minutes': limit,
                    'recipients': recipients})
