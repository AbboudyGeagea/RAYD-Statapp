"""
utils/schedule_lead_time.py — Report 40, patient schedule lead time (LAUMC to-do item 21).

The time between when the exam was booked and the date and time it was booked for, for the
RIS exams whose appointment falls in the period, with every breakdown the RIS gives.

    booked       ORDERS.CREATED_ON_DATE (operator, 2026-11-17), etl_orders.order_created_at
                 (migration 0160)
    appointment  the exam's scheduled date and time, etl_orders.scheduled_datetime
    lead time    appointment day minus booking day, in calendar days (0 = same day); also in
                 hours for the median. An appointment earlier than its booking, by the clock (a
                 walk-in registered after the fact, even the same day), is counted aside and left
                 out of the lead time figures.
    cancelled    the exam was cancelled on the RIS (rpt.exams.cancelled)
    rescheduled  the exam got the Scheduled status more than once (RIS status history,
                 std_worklist_scheduled): booked again for another time

The breakdowns come from Report Composer's exam view (rpt.exams, migration 0136): modality,
procedure, room, patient class, referring physician, priority, age band, sex; the ordering
department from the RIS ordering organisation. Reads through the site's views (rule 0): this
database's site, without the HIS's duplicate orders or test patients.
"""
import io
import math
import statistics
from datetime import date, timedelta

from sqlalchemy import text

from db import db

TITLE = 'Patient Schedule Lead Time'
UNKNOWN = 'Not recorded'

BUCKETS = (
    ('after', 'Booked after the appointment'),
    ('0', 'Same day'),
    ('1', '1 day'),
    ('2-3', '2–3 days'),
    ('4-7', '4–7 days'),
    ('8-14', '8–14 days'),
    ('15-30', '15–30 days'),
    ('31-60', '31–60 days'),
    ('61+', 'Over 60 days'),
)
AHEAD_DAYS = 7        # "booked a week or more ahead"

# (key, label): the breakdowns, in the order the page offers them.
DIMENSIONS = (
    ('modality', 'Modality'),
    ('procedure', 'Procedure'),
    ('room', 'Room'),
    ('patient_class', 'Patient class'),
    ('department', 'Ordering department'),
    ('referring', 'Referring physician'),
    ('priority', 'Priority'),
    ('age_band', 'Age band'),
    ('sex', 'Sex'),
    ('booked_weekday', 'Weekday booked'),
    ('booked_hour', 'Hour booked'),
)
BREAKDOWN_ROWS = 60   # a breakdown longer than this shows its largest rows

# The panel's dropdowns: (field, query parameter, its "Apply filter" tick, label).
FILTERS = (
    ('modality', 'modality', 'mod_enabled', 'Modality'),
    ('patient_class', 'patient_class', 'class_enabled', 'Patient class'),
    ('department', 'department', 'dept_enabled', 'Ordering department'),
    ('room', 'room', 'room_enabled', 'Room'),
    ('priority', 'priority', 'priority_enabled', 'Priority'),
)

WEEKDAYS = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')

# The per-exam export: (header, key).
COLUMNS = (
    ('Accession', 'accession'), ('Patient ID', 'patient_id'), ('Booked', 'booked'),
    ('Appointment', 'appointment'), ('Lead time (days)', 'lead_days'),
    ('Lead time (hours)', 'lead_hours'), ('Modality', 'modality'), ('Procedure code', 'procedure_code'),
    ('Procedure', 'procedure'), ('Room', 'room'), ('Patient class', 'patient_class'),
    ('Ordering department', 'department'), ('Referring physician', 'referring'),
    ('Priority', 'priority'), ('Age band', 'age_band'), ('Sex', 'sex'), ('RIS status', 'exam_status'),
    ('Cancelled', 'cancelled_text'), ('Times scheduled', 'times_scheduled'),
)

_SQL = """
SELECT e.exam_id, e.accession, e.patient_id, e.sex, e.age_band, e.patient_class, e.modality,
       e.room, e.procedure_code, e.procedure, e.priority, e.referring, e.cancelled, e.exam_status,
       o.order_created_at AS booked_at, e.scheduled_at AS appointment_at,
       COALESCE(NULLIF(BTRIM(org.name), ''), NULLIF(BTRIM(org.code), '')) AS department,
       (SELECT COUNT(*) FROM std_worklist_scheduled w WHERE w.site_worklist_key = e.exam_id) AS times_scheduled
FROM rpt.exams e
JOIN etl_orders o ON o.order_dbid = e.exam_id
LEFT JOIN std_ordering_organizations org ON org.ordering_organization_key = o.ordering_organization_key
WHERE e.scheduled_at >= :start AND e.scheduled_at < :end
  AND o.order_created_at IS NOT NULL
"""


def bucket_of(days):
    if days < 0:
        return 'after'
    if days <= 1:
        return str(days)
    for key, upper in (('2-3', 3), ('4-7', 7), ('8-14', 14), ('15-30', 30), ('31-60', 60)):
        if days <= upper:
            return key
    return '61+'


def _value(row, key):
    v = row.get(key)
    if v is None or (isinstance(v, str) and not v.strip()):
        return UNKNOWN
    return v.strip() if isinstance(v, str) else v


def _shape(row):
    booked, appointment = row['booked_at'], row['appointment_at']
    days = (appointment.date() - booked.date()).days
    hours = round((appointment - booked).total_seconds() / 3600, 1)
    after = appointment < booked        # by the clock: a walk-in at 09:00 registered at 12:00 too
    out = {k: _value(row, k) for k, _ in DIMENSIONS if k not in ('booked_weekday', 'booked_hour')}
    out.update({
        'exam_id': row['exam_id'], 'accession': row['accession'], 'patient_id': row['patient_id'],
        'procedure_code': row['procedure_code'], 'exam_status': row['exam_status'],
        'booked_at': booked, 'appointment_at': appointment,
        'booked': booked.strftime('%Y-%m-%d %H:%M'), 'appointment': appointment.strftime('%Y-%m-%d %H:%M'),
        'lead_days': days, 'lead_hours': hours, 'after': after, 'bucket': 'after' if after else bucket_of(days),
        'cancelled': bool(row['cancelled']), 'cancelled_text': 'Yes' if row['cancelled'] else '',
        'times_scheduled': int(row['times_scheduled'] or 0),
        'rescheduled': int(row['times_scheduled'] or 0) > 1,
        'booked_weekday': WEEKDAYS[booked.weekday()], 'booked_hour': f'{booked.hour:02d}:00',
    })
    return out


def _pct(part, whole):
    return round(100.0 * part / whole, 1) if whole else None


def _p90(values):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(0.9 * (len(ordered) - 1))))]


def _block(cases):
    """The figures every table row and the headline share."""
    timed = [c for c in cases if not c['after']]
    days = [c['lead_days'] for c in timed]
    return {
        'exams': len(cases),
        'median_days': statistics.median(days) if days else None,
        'median_hours': statistics.median([c['lead_hours'] for c in timed]) if timed else None,
        'p90_days': _p90(days),
        'same_day_pct': _pct(sum(d == 0 for d in days), len(days)),
        'ahead_pct': _pct(sum(d >= AHEAD_DAYS for d in days), len(days)),
        'after': len(cases) - len(timed),
        'cancelled_pct': _pct(sum(c['cancelled'] for c in cases), len(cases)),
        'rescheduled_pct': _pct(sum(c['rescheduled'] for c in cases), len(cases)),
    }


def _breakdown(cases, key):
    groups = {}
    for c in cases:
        groups.setdefault(c[key], []).append(c)
    if key == 'booked_weekday':
        order = sorted(groups, key=WEEKDAYS.index)
    elif key == 'booked_hour':
        order = sorted(groups)
    else:
        order = sorted(groups, key=lambda v: (-len(groups[v]), str(v)))
    rows = [{'value': v, **_block(groups[v])} for v in order]
    return {'rows': rows[:BREAKDOWN_ROWS], 'total': len(rows)}


def options(cases):
    """The values each panel dropdown offers: those of the period, most exams first."""
    out = {}
    for field, *_ in FILTERS:
        counts = {}
        for c in cases:
            counts[c[field]] = counts.get(c[field], 0) + 1
        out[field] = sorted(counts, key=lambda v: (-counts[v], str(v)))
    return out


def build(start, end, chosen=None):
    """(cases, summary) for appointments from start to end (dates, both included), keeping
    only the values chosen per filter field ({field: set})."""
    rows = db.session.execute(text(_SQL), {'start': start, 'end': end + timedelta(days=1)}).mappings().all()
    every = [_shape(dict(r)) for r in rows]
    chosen = chosen or {}
    cases = [c for c in every if all(str(c[f]) in picked for f, picked in chosen.items())]
    cases.sort(key=lambda c: (c['appointment_at'], c['exam_id']))
    heat = [[0] * 24 for _ in WEEKDAYS]
    for c in cases:
        heat[c['booked_at'].weekday()][c['booked_at'].hour] += 1
    heat_max = max((max(r) for r in heat), default=0)
    peak = max(((n, d, h) for d, row in enumerate(heat) for h, n in enumerate(row)), default=(0, 0, 0))
    summary = {
        'kpi': _block(cases),
        'buckets': [{'key': k, 'label': label, 'exams': sum(c['bucket'] == k for c in cases)}
                    for k, label in BUCKETS],
        'breakdowns': {k: _breakdown(cases, k) for k, _ in DIMENSIONS},
        'heat': heat, 'heat_max': heat_max,
        # the colour step of each cell: 0 = none, 1..7 = the ramp, by the square root of its
        # share of the busiest cell, so a quiet hour still shows against an empty one
        'heat_steps': [[(max(1, min(7, math.ceil((n / heat_max) ** 0.5 * 7))) if n and heat_max else 0)
                        for n in row] for row in heat],
        'heat_rows': [sum(row) for row in heat],
        'heat_cols': [sum(heat[d][h] for d in range(len(WEEKDAYS))) for h in range(24)],
        'heat_peak': {'n': peak[0], 'weekday': WEEKDAYS[peak[1]], 'hour': peak[2]} if peak[0] else None,
        'options': options(every),
        'filters': {k: sorted(map(str, v)) for k, v in chosen.items()},
    }
    return cases, summary


def workbook(start, end, cases, summary, site_label):
    """The exams as an .xlsx. Text stays text: an ID like 00123 keeps its zeros."""
    import xlsxwriter
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False,
                                     'strings_to_urls': False, 'strings_to_numbers': False})
    sheet = book.add_worksheet('Lead time')
    bold = book.add_format({'bold': True})
    kpi = summary['kpi']
    sheet.write(0, 0, f'{TITLE}, {site_label}: appointments {start} to {end}', bold)
    sheet.write(1, 0, f"{kpi['exams']:,} exams; median {kpi['median_days']} days; "
                      f"same day {kpi['same_day_pct']} %; {AHEAD_DAYS}+ days ahead {kpi['ahead_pct']} %; "
                      f"cancelled {kpi['cancelled_pct']} %; scheduled more than once {kpi['rescheduled_pct']} %")
    sheet.write(2, 0, 'Booked = when the order was created on the RIS; lead time = appointment day minus booking day.')
    sheet.write_row(4, 0, [h for h, _ in COLUMNS], bold)
    for i, c in enumerate(cases, 5):
        sheet.write_row(i, 0, [c[k] for _, k in COLUMNS])
    sheet.freeze_panes(5, 0)
    sheet.autofilter(4, 0, 4 + len(cases), len(COLUMNS) - 1)
    sheet.set_column(0, len(COLUMNS) - 1, 16)
    book.close()
    return buf.getvalue()


def default_period(today=None):
    """The last full month."""
    today = today or date.today()
    end = today.replace(day=1) - timedelta(days=1)
    return end.replace(day=1), end
