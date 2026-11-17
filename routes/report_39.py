"""
routes/report_39.py — Report 39, Staff status (LAUMC to-do item 9, migration 0157).

Everyone on the RIS and PACS (no RAYD accounts), grouped by PACS group and RIS role, for the
manager who sets leavers inactive on the RIS and PACS. Shows who the rule takes as having left
(utils/staff_status.py) and their signed studies and performed exams in the period, so anyone
it takes who still works stands out before the switch goes on. The switch, for whoever holds
the settings capability, makes every report read "Former staff" for them.

A report like every other (CLAUDE.md rule 8): catalogue row and licence entry from migration
0157, registered below, opened at /viewer/39, the shared left panel.

    /report/39               the page, for the filters in the address
    /report/39/export.xlsx   POST, the same rows as an Excel workbook
    /report/39/switch        POST, settings capability: {"on": true|false}
"""
import io
import logging
from datetime import date

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_login import current_user, login_required

from db import db, get_report_start_date
from utils import staff_status
from utils.roles import can, can_open_report

logger = logging.getLogger(__name__)

REPORT_ID = 39
report_39_bp = Blueprint('report_39', __name__)

XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

# The panel's dropdowns: (query parameter, its "Apply filter" tick, label).
FILTERS = (
    ('status', 'status_enabled', 'Status'),
    ('role', 'role_enabled', 'RIS role'),
    ('group', 'group_enabled', 'PACS group'),
)


def _require_access():
    if not can_open_report(current_user, REPORT_ID):
        abort(403)


def _read_filters():
    """The period (the report start date to today unless chosen) and the values chosen in
    each dropdown whose "Apply filter" is ticked."""
    values = request.values
    first = get_report_start_date() or date(date.today().year, 1, 1)
    try:
        start = date.fromisoformat(values.get('start_date') or first.isoformat())
        end = date.fromisoformat(values.get('end_date') or date.today().isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    chosen = {}
    for name, toggle, _ in FILTERS:
        picked = [v for v in values.getlist(name) if v]
        if picked and values.get(toggle) == 'on':
            chosen[name] = set(picked)
    return start, end, chosen


def _panel_filters(summary):
    fresh = not request.values
    options = {
        'status': list(staff_status.STATUSES),
        'role': [(v, v) for v in summary['roles']],
        'group': [(v, v) for v in summary['groups']],
    }
    panel = []
    for name, toggle, label in FILTERS:
        panel.append({'name': name, 'toggle': toggle, 'label': label, 'values': options[name],
                      'selected': [v for v in request.values.getlist(name) if v],
                      'enabled': fresh or request.values.get(toggle) == 'on'})
    return panel


@report_39_bp.route('/report/39')
@login_required
def report_39():
    _require_access()
    start, end, chosen = _read_filters()
    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='report_39',
              detail={'from': start.isoformat(), 'to': end.isoformat(),
                      'filters': {k: sorted(v) for k, v in chosen.items()}})
    try:
        rows, summary = staff_status.build(start, end, chosen)
    except Exception:
        db.session.rollback()
        logger.exception('Report 39 failed')
        rows, summary = None, None
    return render_template('report_39.html', start=start.isoformat(), end=end.isoformat(),
                           rows=rows, summary=summary, columns=staff_status.COLUMNS,
                           filters=_panel_filters(summary) if summary else [],
                           former=staff_status.FORMER, can_switch=can(current_user, 'settings'))


def _cell(value):
    """Keeps a spreadsheet from running a cell as a formula."""
    if isinstance(value, str) and value[:1] in ('=', '+', '-', '@'):
        return "'" + value
    return value


@report_39_bp.route('/report/39/export.xlsx', methods=['POST'])
@login_required
def export_xlsx():
    _require_access()
    start, end, chosen = _read_filters()
    rows, summary = staff_status.build(start, end, chosen)
    from openpyxl import Workbook
    from openpyxl.styles import Font
    wb = Workbook()
    ws = wb.active
    ws.title = 'Staff status'
    ws.append([f'Staff status, RIS and PACS: activity {start} to {end}'])
    ws.append([f"{summary['listed']} people, {summary['left']} left, "
               f"{summary['working']} left but worked in the period"])
    ws.append([])
    ws.append([header for header, _ in staff_status.COLUMNS])
    for cell in ws[4]:
        cell.font = Font(bold=True)
    for r in rows:
        ws.append([_cell(r[key]) for _, key in staff_status.COLUMNS])
    ws.freeze_panes = 'A5'
    ws.auto_filter.ref = f'A4:{ws.cell(row=4, column=len(staff_status.COLUMNS)).column_letter}{ws.max_row}'
    for col, width in zip('ABCDEFGHIJKLM', (28, 22, 14, 26, 9, 22, 14, 9, 26, 9, 12, 9, 12)):
        ws.column_dimensions[col].width = width
    out = io.BytesIO()
    wb.save(out)
    from utils.audit import log_event
    log_event('report_export', category='report', resource_type='report_39',
              detail={'from': start.isoformat(), 'to': end.isoformat(), 'rows': len(rows)})
    return Response(out.getvalue(), mimetype=XLSX, headers={
        'Content-Disposition': f'attachment; filename=staff_status_{date.today()}.xlsx'})


@report_39_bp.route('/report/39/switch', methods=['POST'])
@login_required
def switch():
    """Settings capability: staff who left read "Former staff" on every report, or not."""
    if not can(current_user, 'settings'):
        return jsonify({'status': 'error', 'message': 'Administrators only'}), 403
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload.get('on'), bool):
        return jsonify({'status': 'error', 'message': 'Say on: true or false'}), 400
    try:
        staff_status.set_on(payload['on'])
    except Exception:
        db.session.rollback()
        logger.exception('Report 39 switch failed')
        return jsonify({'status': 'error', 'message': 'The switch could not be changed.'}), 500
    from utils.audit import log_event
    log_event('staff_left_switch', category='admin', resource_type='report_39',
              detail={'on': payload['on']})
    return jsonify({'status': 'success', 'on': payload['on']})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report  # noqa: E402
register_report(REPORT_ID, report_39_bp, report_39, None)
