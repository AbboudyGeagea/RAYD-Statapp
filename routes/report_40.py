"""
routes/report_40.py — Report 40, Patient Schedule Lead Time (LAUMC to-do item 21, migration 0167).

How far ahead exams are booked: from when the order was created on the RIS to the appointment,
for the exams whose appointment falls in the period, with the breakdowns the RIS gives
(utils/schedule_lead_time.py holds the definitions).

A report like every other (CLAUDE.md rule 8): catalogue row and licence entry from migration
0167, registered below, opened at /viewer/40, the shared left panel.

    /report/40               the page, for the filters in the address
    /report/40/export.xlsx   POST, the exams as an Excel workbook
"""
import logging
from datetime import date

from flask import Blueprint, Response, abort, render_template, request
from flask_login import current_user, login_required

from db import db
from utils import schedule_lead_time as lead
from utils.roles import can_open_report
from utils.site_db import served_site_id, site_code

logger = logging.getLogger(__name__)

REPORT_ID = 40
report_40_bp = Blueprint('report_40', __name__)

XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'


def _require_access():
    if not can_open_report(current_user, REPORT_ID):
        abort(403)


def _read_filters():
    """The period (the last full month unless chosen) and the values chosen in each dropdown
    whose "Apply filter" is ticked."""
    values = request.values
    first, last = lead.default_period()
    try:
        start = date.fromisoformat(values.get('start_date') or first.isoformat())
        end = date.fromisoformat(values.get('end_date') or last.isoformat())
    except ValueError:
        abort(400, 'Dates must look like 2026-11-05')
    if end < start:
        start, end = end, start
    chosen = {}
    for field, name, toggle, _ in lead.FILTERS:
        picked = [v for v in values.getlist(name) if v]
        if picked and values.get(toggle) == 'on':
            chosen[field] = set(picked)
    return start, end, chosen


def _panel_filters(summary):
    fresh = not request.values
    panel = []
    for field, name, toggle, label in lead.FILTERS:
        offered = [str(v) for v in summary['options'][field]]
        panel.append({'name': name, 'toggle': toggle, 'label': label,
                      'values': [(v, v) for v in offered],
                      'selected': [v for v in request.values.getlist(name) if v],
                      'enabled': fresh or request.values.get(toggle) == 'on'})
    return panel


def _site_label():
    return site_code(served_site_id()) or 'this site'


@report_40_bp.route('/report/40')
@login_required
def report_40():
    _require_access()
    start, end, chosen = _read_filters()
    from utils.audit import log_event
    log_event('report_run', category='report', resource_type='report_40',
              detail={'from': start.isoformat(), 'to': end.isoformat(),
                      'filters': {k: sorted(v) for k, v in chosen.items()}})
    try:
        _cases, summary = lead.build(start, end, chosen)
    except Exception:
        db.session.rollback()
        logger.exception('Report 40 failed')
        summary = None
    return render_template('report_40.html', start=start.isoformat(), end=end.isoformat(),
                           summary=summary, filters=_panel_filters(summary) if summary else [],
                           dimensions=lead.DIMENSIONS, weekdays=lead.WEEKDAYS,
                           ahead_days=lead.AHEAD_DAYS, unknown=lead.UNKNOWN,
                           breakdown_rows=lead.BREAKDOWN_ROWS, site=_site_label())


@report_40_bp.route('/report/40/export.xlsx', methods=['POST'])
@login_required
def export_xlsx():
    _require_access()
    start, end, chosen = _read_filters()
    cases, summary = lead.build(start, end, chosen)
    data = lead.workbook(start, end, cases, summary, _site_label())
    from utils.audit import log_event
    log_event('report_export', category='report', resource_type='report_40',
              detail={'from': start.isoformat(), 'to': end.isoformat(), 'rows': len(cases)})
    return Response(data, mimetype=XLSX, headers={
        'Content-Disposition': f'attachment; filename=schedule_lead_time_{start}_{end}.xlsx'})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report  # noqa: E402
register_report(REPORT_ID, report_40_bp, report_40, None)
