"""
routes/report_37.py — Report 37, urgent emergent exams: first open and sticky note (the JCI
measure). What it shows is worked out in utils/sticky_notes.py, which explains what the
customer asked for and what PACS can give; the automated monthly workbook is
utils/sticky_notes_monthly.py.

A report like every other (operator, 2026-11-09: no feature built as a workaround). Its
report_template row and licence entry come from migration 0147, it registers itself below,
it opens through /viewer/37, and its filters are the shared left panel
(templates/_report_filter_panel.html). Access is per-user report access
(report_access_control), as for every report; an admin sees everything. Until 2026-11-09 it
was the standalone page /sticky-notes with a page permission of its own.

    /report/37                         the page (/viewer/37 opens it; /sticky-notes redirects)
    /report/37/data                    the figures, for the filters in the page address
    /report/37/export.csv              every case behind them
    /report/37/monthly/<YYYY-MM>.xlsx  a kept monthly workbook
    /report/37/monthly/generate        POST, admin: build a finished month again
    /report/37/settings                POST, admin: urgent departments, limit, recipients
"""
import csv
import io
import json
import logging
from datetime import date

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request
from flask_login import current_user, login_required
from sqlalchemy import text

from db import ReportAccessControl, db
from utils import sticky_notes as report
from utils import sticky_notes_monthly as monthly
from utils import worklist_email

logger = logging.getLogger(__name__)

REPORT_ID = 37
report_37_bp = Blueprint('report_37', __name__)

MAX_CASES_ON_PAGE = 2000
XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

# The panel's dropdowns: (query parameter, its "Apply filter" tick, label). Modality and its
# tick are named as on every report (utils/report_filters.sidebar_filters).
FILTERS = (
    ('modality', 'mod_enabled', 'Modality'),
    ('department', 'dept_enabled', 'Department'),
    ('outcome', 'outcome_enabled', 'Outcome'),
)


def _require_access():
    """As /viewer/<id> checks it: an admin, or a user whose access to report 37 is on."""
    if current_user.role == 'admin':
        return
    allowed = ReportAccessControl.query.filter_by(
        user_id=current_user.id, report_template_id=REPORT_ID, is_enabled=True).first()
    if not allowed:
        abort(403)


def _read_filters():
    """The period (the preceding month unless chosen, as the customer asked) and the values
    chosen in each dropdown whose "Apply filter" is ticked. Nothing chosen: nothing hidden."""
    first, last = report.previous_month()
    try:
        start = date.fromisoformat(request.args.get('start_date') or first.isoformat())
        end = date.fromisoformat(request.args.get('end_date') or last.isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    chosen = {}
    for name, toggle, _ in FILTERS:
        values = [v for v in request.args.getlist(name) if v]
        if values and request.args.get(toggle) == 'on':
            chosen[name] = set(values)
    return start, end, chosen


def _panel_filters():
    """The dropdowns as the shared panel draws them. Modality and department list what the
    period holds, which the page fills in once the figures arrive; what is chosen now is
    listed already, so it shows as chosen straight away."""
    fresh = not request.args
    panel = []
    for name, toggle, label in FILTERS:
        selected = [v for v in request.args.getlist(name) if v]
        values = (list(report.OUTCOMES.items()) if name == 'outcome'
                  else [(v, v) for v in selected])
        panel.append({'name': name, 'toggle': toggle, 'label': label, 'values': values,
                      'selected': selected, 'enabled': fresh or request.args.get(toggle) == 'on'})
    return panel


@report_37_bp.route('/report/37')
@login_required
def report_37():
    _require_access()
    start, end, _ = _read_filters()
    return render_template('report_37.html',
                           start=start.isoformat(), end=end.isoformat(), filters=_panel_filters(),
                           monthly_reports=monthly.list_reports(),
                           clock_label=report.CLOCK_LABEL, open_seen_note=report.OPEN_SEEN_NOTE,
                           is_admin=current_user.role == 'admin')


@report_37_bp.route('/sticky-notes')
@login_required
def old_address():
    """The page's address before it became Report 37 (bookmarks, the old dashboard card)."""
    return redirect(f'/viewer/{REPORT_ID}')


@report_37_bp.route('/report/37/data')
@login_required
def data():
    _require_access()
    start, end, chosen = _read_filters()
    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='report_37',
              detail={'from': start.isoformat(), 'to': end.isoformat(),
                      'filters': {k: sorted(v) for k, v in chosen.items()}})
    try:
        kept, summary = report.build(start, end, chosen)
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
        logger.exception('Report 37 failed')
        return jsonify({'error': 'The report could not be loaded.'}), 500


def _csv_cell(value):
    """Keeps a spreadsheet from running a cell as a formula."""
    if value is None:
        return ''
    value = str(value)
    return "'" + value if value[0:1] in ('=', '+', '-', '@') else value


@report_37_bp.route('/report/37/export.csv')
@login_required
def export_csv():
    _require_access()
    start, end, chosen = _read_filters()
    kept, summary = report.build(start, end, chosen)
    from utils.audit import log_event
    log_event('report_export', category='report', resource_type='report_37',
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


@report_37_bp.route('/report/37/monthly/<month>.xlsx')
@login_required
def monthly_workbook(month):
    _require_access()
    found = monthly.read_workbook(month)
    if found is None:
        abort(404)
    from utils.audit import log_event
    log_event('report_export', category='report', resource_type='report_37',
              detail={'monthly': month})
    filename, content = found
    return Response(content, mimetype=XLSX, headers={'Content-Disposition': f'attachment; filename={filename}'})


@report_37_bp.route('/report/37/monthly/generate', methods=['POST'])
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
    log_event('report_37_monthly_generated', category='admin', resource_type='report_37',
              detail={'month': payload.get('month'), 'cases': kpi['cases'], 'emailed': emailed})
    return jsonify({'status': 'success', 'cases': kpi['cases'], 'emailed': emailed})


@report_37_bp.route('/report/37/settings', methods=['POST'])
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
    log_event('report_37_settings_changed', category='admin', resource_type='report_37',
              detail={'departments': departments, 'limit_minutes': limit, 'recipients': recipients})
    return jsonify({'status': 'success', 'departments': departments, 'limit_minutes': limit,
                    'recipients': recipients})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report  # noqa: E402
register_report(REPORT_ID, report_37_bp, report_37, None)
