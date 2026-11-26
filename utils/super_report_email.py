"""
utils/super_report_email.py — the Super Report sent by email on a schedule (to-do item 43).

Operator, 25 Nov 2026: "include the feature of emailing the SU like the custom reports". Built on
Report Composer's scheduled emails (utils/worklist_email.py): the same mail server (settings
smtp_*, Administration > Email Server), the same recipient rule (only smtp_allowed_domains),
the same clock (next_run, claimed before sending so a crash loses one send instead of repeating
it), the same run log. Migration 0184.

A schedule keeps the page's filters and blocks as they were when it was saved, and a period
worked out on the day it runs: yesterday, the last 7 days, last week (Monday to Sunday), last
month; each compared with the period just before it (last month with the month before). The
message: the written analysis (item 42, utils/super_report_analysis.py) and the key figures,
as HTML with a plain-text twin, and an .xlsx with one sheet per block. A PDF and the logos come
with item 44.

The body holds figures, staff and referring physicians' names, never a patient's. Times are naive
hospital time from Python, as in Report Composer: LAUMC's Postgres is UTC.
"""
import io
import json
import logging
import re
from datetime import date, datetime, timedelta
from html import escape

import xlsxwriter
from sqlalchemy import text

from db import db
from utils import super_report_blocks as sr_blocks
from utils.super_report_analysis import change, count, dur, num
from utils.worklist_email import (STALE_RUN, ScheduleError, _whole, clean_recipients, is_ready,
                                  mail_config, next_run, send_mail)

logger = logging.getLogger("SUPER_REPORT")

PERIODS = {
    'yesterday': 'Yesterday',
    'last_7_days': 'The last 7 days',
    'last_week': 'Last week (Monday to Sunday)',
    'last_month': 'Last month',
}
FILTER_KEY = re.compile(r'[a-z_]{1,40}')


# ── When and what ──────────────────────────────────────────────────────────────

def period_dates(kind, today=None):
    """(start, end, cmp_start, cmp_end) as dates for a schedule's period, on the day it runs."""
    today = today or date.today()
    if kind == 'yesterday':
        end = start = today - timedelta(days=1)
    elif kind == 'last_7_days':
        end = today - timedelta(days=1)
        start = end - timedelta(days=6)
    elif kind == 'last_week':
        end = today - timedelta(days=today.weekday() + 1)          # last Sunday
        start = end - timedelta(days=6)
    elif kind == 'last_month':
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
        cmp_end = start - timedelta(days=1)
        return start, end, cmp_end.replace(day=1), cmp_end
    else:
        raise ScheduleError('the period is yesterday, the last 7 days, last week or last month')
    days = (end - start).days + 1
    return start, end, start - timedelta(days=days), start - timedelta(days=1)


def dmy(d):
    return d.strftime('%d/%m/%Y')


def build(schedule, today=None):
    """The figures and the analysis of one schedule, in the database being read."""
    from routes.super_report import _collect_data, _generate_narrative, stored_filters
    from utils import site_db
    from utils.site_branding import brand

    start, end, cmp_start, cmp_end = period_dates(schedule['period'], today)
    blocks = sr_blocks.clean(schedule['blocks'])
    sections = sr_blocks.sections(blocks)
    filters = stored_filters(schedule['filters'])
    site = site_db.served_site_id()
    cur = _collect_data(start.isoformat(), end.isoformat(), filters, site, sections=sections)
    prev = _collect_data(cmp_start.isoformat(), cmp_end.isoformat(), filters, site, sections=sections)
    crn_on = bool((db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'crn_live_since'")).scalar() or '').strip())
    narrative = _generate_narrative(cur, prev, start.isoformat(), end.isoformat(), cmp_start.isoformat(),
                                    cmp_end.isoformat(), (cmp_end - cmp_start).days + 1, blocks=blocks,
                                    crn_enabled=crn_on)
    applied = {k: v for k, v in filters.items() if v}
    return {'start': start, 'end': end, 'cmp_start': cmp_start, 'cmp_end': cmp_end, 'blocks': blocks,
            'cur': cur, 'prev': prev, 'narrative': narrative, 'filters': applied,
            'brand': brand(site), 'name': schedule['name']}


def key_figures(data):
    """(label, now, before, change) for the message's figures."""
    c, p = data['cur'], data['prev']
    k, pk = c.get('kpis') or {}, p.get('kpis') or {}
    t, pt = c.get('tat') or {}, p.get('tat') or {}
    o, po = c.get('orders') or {}, p.get('orders') or {}
    rows = [('Studies', count(k.get('total_studies')), count(pk.get('total_studies')),
             change(k.get('total_studies'), pk.get('total_studies'))),
            ('Patients', count(k.get('total_patients')), count(pk.get('total_patients')),
             change(k.get('total_patients'), pk.get('total_patients')))]
    if 'tat' in data['blocks']:
        share = lambda tt, kk: (f"{num(tt.get('reported_count')) / num(kk.get('total_studies')) * 100:.1f} %"
                                if num(kk.get('total_studies')) else '—')
        rows += [('Reported', share(t, k), share(pt, pk), ''),
                 ('Median turnaround', dur(t.get('median_tat_min')), dur(pt.get('median_tat_min')), '')]
    if 'orders' in data['blocks'] and num(o.get('total')):
        rows.append(('RIS orders', count(o.get('total')), count(po.get('total')), change(o.get('total'), po.get('total'))))
    if 'crn' in data['blocks'] and 'critical_count' in (c.get('crn') or {}):
        rows.append(('Flagged critical', count(c['crn']['critical_count']), count((p.get('crn') or {}).get('critical_count')),
                     change(c['crn']['critical_count'], (p.get('crn') or {}).get('critical_count'))))
    return rows


# ── The message ───────────────────────────────────────────────────────────────

GREEN, BLUE, GREY = '#00664F', '#00558B', '#5f6b66'


def _filters_text(filters):
    if not filters:
        return 'None: all modalities, devices, patient classes and physicians'
    return '; '.join(f"{k.replace('_', ' ')}: {', '.join(v) if isinstance(v, list) else v}" for k, v in filters.items())


def render(data):
    """(html, plain text) of the message."""
    b = data['brand']
    period = f"{dmy(data['start'])} – {dmy(data['end'])}" if data['start'] != data['end'] else dmy(data['start'])
    compared = (f"{dmy(data['cmp_start'])} – {dmy(data['cmp_end'])}" if data['cmp_start'] != data['cmp_end']
                else dmy(data['cmp_start']))
    figures = key_figures(data)
    text_lines = [f"{data['name']}", f"{b['alt']}, radiology", f"Period: {period}, compared with {compared}",
                  f"Filters: {_filters_text(data['filters'])}", '']
    rows = ''.join(
        f'<tr><td style="padding:4px 10px;border-bottom:1px solid #e3e8e6">{escape(l)}</td>'
        f'<td style="padding:4px 10px;border-bottom:1px solid #e3e8e6;text-align:right;font-weight:700;color:{BLUE}">{escape(n)}</td>'
        f'<td style="padding:4px 10px;border-bottom:1px solid #e3e8e6;text-align:right;color:{GREY}">{escape(p)}</td>'
        f'<td style="padding:4px 10px;border-bottom:1px solid #e3e8e6;text-align:right">{escape(ch)}</td></tr>'
        for l, n, p, ch in figures)
    text_lines += ['Key figures (now / before / change)'] + [f"- {l}: {n} / {p}" + (f" ({ch})" if ch else '')
                                                            for l, n, p, ch in figures] + ['']
    parts = []
    for sec in data['narrative']:
        title, bullets = sec['title'], sec.get('bullets') or []
        if not bullets:
            continue
        text_lines += [title.upper()] + [f"- {x}" for x in bullets] + ['']
        if title == 'In three lines':
            items = ''.join(f'<li style="margin:3px 0">{escape(x)}</li>' for x in bullets)
            parts.insert(0, f'<div style="background:#f2f7f5;border-left:4px solid {GREEN};padding:10px 14px;margin:14px 0">'
                            f'<ul style="margin:0;padding-left:18px">{items}</ul></div>')
            continue
        tag = 'ol' if title == 'To act on this week' else 'ul'
        items = ''.join(f'<li style="margin:3px 0;line-height:1.45">{escape(x)}</li>' for x in bullets)
        parts.append(f'<h3 style="font-size:13px;color:{GREEN};text-transform:uppercase;letter-spacing:.06em;'
                     f'border-bottom:1px solid #d5dfdb;padding-bottom:3px;margin:18px 0 6px">{escape(title)}</h3>'
                     f'<{tag} style="margin:0;padding-left:20px">{items}</{tag}>')
    footer = (f"{b['hospital_line']} · <b><u>Internal Use only</u></b><br>"
              "Sent automatically by RAYD (Reliable Analytics For Your Department). The attachment holds staff "
              "names: do not forward it outside the hospital.")
    text_lines += [b['hospital_line'], 'Internal Use only', 'Sent automatically by RAYD. The attachment holds '
                   'staff names: do not forward it outside the hospital.']
    html = f"""<!doctype html><html><body style="margin:0;background:#ffffff">
<div style="max-width:720px;margin:0 auto;padding:18px 20px;font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#1f2a26">
<p style="margin:0;color:{GREY};font-size:12px;text-transform:uppercase;letter-spacing:.1em;font-weight:700">{escape(b['alt'])} · Radiology</p>
<h1 style="margin:4px 0 2px;font-size:22px;color:{BLUE}">{escape(data['name'])}</h1>
<p style="margin:0 0 2px;color:{GREY}">Period <b style="color:#1f2a26">{escape(period)}</b>, compared with {escape(compared)}</p>
<p style="margin:0;color:{GREY};font-size:12px">Filters: {escape(_filters_text(data['filters']))}</p>
{parts[0] if parts and parts[0].startswith('<div') else ''}
<table style="border-collapse:collapse;width:100%;font-size:13px;margin:8px 0">
<tr><th style="text-align:left;padding:4px 10px;color:{GREY};font-size:11px;text-transform:uppercase;border-bottom:2px solid {GREEN}">Key figures</th>
<th style="text-align:right;padding:4px 10px;color:{GREY};font-size:11px;border-bottom:2px solid {GREEN}">Now</th>
<th style="text-align:right;padding:4px 10px;color:{GREY};font-size:11px;border-bottom:2px solid {GREEN}">Before</th>
<th style="text-align:right;padding:4px 10px;color:{GREY};font-size:11px;border-bottom:2px solid {GREEN}">Change</th></tr>
{rows}</table>
{''.join(p for p in parts if not p.startswith('<div'))}
<p style="margin:22px 0 0;padding-top:8px;border-top:1px solid #d5dfdb;color:{GREY};font-size:11px;line-height:1.5">{footer}</p>
</div></body></html>"""
    return html, '\n'.join(text_lines)


# ── The attachment ────────────────────────────────────────────────────────────

def _pairs(cur_rows, prev_rows, key, val='cnt'):
    cur = {r.get(key): num(r.get(val)) or 0 for r in cur_rows or [] if r.get(key) is not None}
    prev = {r.get(key): num(r.get(val)) or 0 for r in prev_rows or [] if r.get(key) is not None}
    names = sorted(set(cur) | set(prev), key=lambda k: (-cur.get(k, 0), str(k)))
    return [(k, cur.get(k, 0), prev.get(k, 0)) for k in names]


def _totals(rows, key):
    out = {}
    for r in rows or []:
        out[r.get(key)] = out.get(r.get(key), 0) + (num(r.get('cnt')) or 0)
    return [{key: k, 'cnt': v} for k, v in out.items()]


def workbook(data):
    """The .xlsx: the analysis, the key figures, then one sheet per block that has a table."""
    c, p, blocks = data['cur'], data['prev'], data['blocks']
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False,
                                     'strings_to_urls': False, 'strings_to_numbers': False})
    bold = book.add_format({'bold': True})
    head = book.add_format({'bold': True, 'bottom': 1, 'font_color': GREEN})

    def sheet(name, headers, rows, note=None):
        ws = book.add_worksheet(name[:31])
        r = 0
        if note:
            ws.write(0, 0, note, bold)
            r = 2
        ws.write_row(r, 0, headers, head)
        for i, row in enumerate(rows, start=r + 1):
            ws.write_row(i, 0, list(row))
        ws.set_column(0, 0, 38)
        ws.set_column(1, max(len(headers) - 1, 1), 14)

    period = f"{dmy(data['start'])} to {dmy(data['end'])}, compared with {dmy(data['cmp_start'])} to {dmy(data['cmp_end'])}"
    sheet('Summary', ['Part', 'Text'],
          [(s['title'], b) for s in data['narrative'] for b in s.get('bullets') or []],
          note=f"{data['name']}: {data['brand']['alt']}, {period}")
    sheet('Key figures', ['Figure', 'Now', 'Before', 'Change'], key_figures(data))
    if 'volume' in blocks:
        sheet('Volume by modality', ['Modality', 'Now', 'Before'],
              _pairs(c.get('modality_series'), p.get('modality_series'), 'modality'))
        sheet('Studies per day', ['Date', 'Studies'], [(r.get('date'), num(r.get('cnt'))) for r in c.get('daily_series') or []])
    if 'referrers' in blocks:
        sheet('Referring physicians', ['Physician', 'Now', 'Before'],
              _pairs(c.get('physicians'), p.get('physicians'), 'physician'))
    if 'tat' in blocks:
        pt = {r.get('modality'): r for r in (p.get('tat') or {}).get('by_modality') or []}
        sheet('Turnaround', ['Modality', 'Median (min) now', 'Studies', 'Median (min) before'],
              [(r.get('modality'), num(r.get('median_tat_min')), num(r.get('cnt')),
                num((pt.get(r.get('modality')) or {}).get('median_tat_min')))
               for r in (c.get('tat') or {}).get('by_modality') or []])
    if 'radiologists' in blocks:
        cm, pm = (c.get('rad_matrix') or {}).get('by_modality'), (p.get('rad_matrix') or {}).get('by_modality')
        sheet('Radiologists', ['Last author', 'Reports now', 'Reports before'],
              _pairs(_totals(cm, 'radiologist'), _totals(pm, 'radiologist'), 'radiologist'))
    if 'wet_reads' in blocks and (c.get('resident_wetreads') or {}).get('residents'):
        sheet('Wet reads', ['Resident', 'First reads'],
              [(r['name'], r['total']) for r in c['resident_wetreads']['residents']])
    if 'utilization' in blocks and (c.get('utilization') or {}).get('available'):
        u = c['utilization']
        sheet('Devices', ['Device', 'Booked / opening hours (%)'],
              [(x['ae'], num(x['avg'])) for x in (u.get('busiest') or []) + (u.get('idlest') or [])])
    if 'lead_time' in blocks and (c.get('lead_time') or {}).get('by_modality'):
        sheet('Lead time by modality', ['Modality', 'Appointments', 'Median days', 'Same day %', 'Cancelled %', 'After the exam'],
              [(r.get('value'), r.get('exams'), r.get('median_days'), r.get('same_day_pct'), r.get('cancelled_pct'), r.get('after'))
               for r in c['lead_time']['by_modality']])
    if 'storage' in blocks:
        sheet('Storage', ['Modality', 'GB now', 'GB before'],
              _pairs((c.get('storage') or {}).get('by_modality'), (p.get('storage') or {}).get('by_modality'), 'modality', 'gb'))
    book.close()
    return buf.getvalue()


# ── Sending ───────────────────────────────────────────────────────────────────

def send_schedule(s, started_by=None, today=None):
    """Produce and mail one schedule, and log it. Never raises: the run says what happened."""
    start, end, _cs, _ce = period_dates(s['period'], today)
    run_id = db.session.execute(text("""
        INSERT INTO super_report_email_runs (schedule_id, schedule_name, started_by, started_at, recipients,
                                             period_from, period_to)
        VALUES (:sid, :name, :by, :at, :to, :f, :t) RETURNING id
    """), {'sid': s['id'], 'name': s['name'], 'by': started_by, 'at': datetime.now(),
           'to': list(s['recipients']), 'f': start, 't': end}).scalar()
    db.session.commit()
    error = None
    try:
        cfg = mail_config()
        to = clean_recipients(s['recipients'], cfg)        # the allowed domains may have changed
        data = build(s, today)
        db.session.commit()                                # no transaction held across the relay
        html, plain = render(data)
        when = dmy(start) if start == end else f"{dmy(start)} to {dmy(end)}"
        refused = send_mail(cfg, to, f"{s['name']} · {data['brand']['alt']} · {when}", plain,
                            [(f"super_report_{start:%Y%m%d}_{end:%Y%m%d}.xlsx", workbook(data))], html=html)
        if refused:
            error = 'Refused by the relay: ' + ', '.join(sorted(refused))
        status = 'partial' if refused else 'sent'
    except Exception as exc:
        db.session.rollback()
        logger.warning("Super Report email schedule %s failed: %s", s['id'], exc)
        status, error = 'failed', str(exc)[:500] or exc.__class__.__name__
    db.session.execute(text("""
        UPDATE super_report_email_runs SET status = :status, finished_at = :at, error = :error WHERE id = :id
    """), {'status': status, 'at': datetime.now(), 'error': error, 'id': run_id})
    db.session.commit()
    return {'id': run_id, 'status': status, 'error': error}


def run_due():
    """Send every schedule that has fallen due (per database: app.py runs it for each). A no-op
    until the mail server is set, and in demo mode."""
    cfg = mail_config()
    if not is_ready(cfg) or cfg['demo_mode'].lower() == 'true':
        db.session.rollback()
        return 0
    now = datetime.now()
    db.session.execute(text("""
        UPDATE super_report_email_runs
           SET status = 'failed', finished_at = :now, error = 'Interrupted: the app stopped during this run.'
         WHERE status = 'running' AND started_at < :stale
    """), {'now': now, 'stale': now - STALE_RUN})
    due = db.session.execute(text("""
        SELECT * FROM super_report_email_schedules WHERE enabled AND next_run_at <= :now ORDER BY next_run_at, id
    """), {'now': now}).mappings().all()
    db.session.commit()
    sent = 0
    for s in due:
        claimed = db.session.execute(text("""
            UPDATE super_report_email_schedules SET next_run_at = :next
             WHERE id = :id AND enabled AND next_run_at = :seen RETURNING id
        """), {'next': next_run(s, now), 'id': s['id'], 'seen': s['next_run_at']}).scalar()
        db.session.commit()
        if claimed:
            send_schedule(s)
            sent += 1
    return sent


# ── The page's schedules ──────────────────────────────────────────────────────

def _minutes(at):
    return at.isoformat(sep=' ', timespec='minutes') if at else None


def _run_json(r):
    return {'id': r['id'], 'status': r['status'], 'started_at': _minutes(r['started_at']),
            'finished_at': _minutes(r['finished_at']), 'recipients': r['recipients'], 'error': r['error'],
            'period_from': r['period_from'].isoformat() if r['period_from'] else None,
            'period_to': r['period_to'].isoformat() if r['period_to'] else None}


def list_schedules():
    rows = db.session.execute(text(
        "SELECT * FROM super_report_email_schedules ORDER BY lower(name), id")).mappings().all()
    last = db.session.execute(text("""
        SELECT DISTINCT ON (schedule_id) * FROM super_report_email_runs WHERE schedule_id IS NOT NULL
         ORDER BY schedule_id, started_at DESC, id DESC
    """)).mappings().all()
    last = {r['schedule_id']: _run_json(r) for r in last}
    return [{'id': s['id'], 'name': s['name'], 'period': s['period'], 'period_label': PERIODS.get(s['period']),
             'recipients': s['recipients'], 'frequency': s['frequency'], 'send_time': s['send_time'].strftime('%H:%M'),
             'weekday': s['weekday'], 'month_day': s['month_day'], 'enabled': s['enabled'],
             'next_run_at': _minutes(s['next_run_at']), 'blocks': s['blocks'], 'filters': s['filters'],
             'last_run': last.get(s['id'])} for s in rows]


def list_runs(schedule_id, limit=20):
    rows = db.session.execute(text("""
        SELECT * FROM super_report_email_runs WHERE schedule_id = :id ORDER BY started_at DESC, id DESC LIMIT :n
    """), {'id': schedule_id, 'n': limit}).mappings().all()
    return [_run_json(r) for r in rows]


def get_schedule(schedule_id):
    row = db.session.execute(text("SELECT * FROM super_report_email_schedules WHERE id = :id"),
                             {'id': schedule_id}).mappings().first()
    return dict(row) if row else None


def _clean_filters(raw):
    """The page's filters as saved: {name: [values]}, short names and values only."""
    out = {}
    for key, value in (raw or {}).items() if isinstance(raw, dict) else ():
        if not FILTER_KEY.fullmatch(str(key)):
            continue
        values = value if isinstance(value, list) else [value]
        values = [str(v)[:200] for v in values if v not in (None, '')][:200]
        if values:
            out[str(key)] = values
    return out


def save_schedule(b, me):
    """Create or update a schedule; returns its id. Saving restarts its clock."""
    name = ' '.join(str(b.get('name') or '').split())[:120]
    if not name:
        raise ScheduleError('a schedule needs a name')
    period = b.get('period')
    if period not in PERIODS:
        raise ScheduleError('the period is yesterday, the last 7 days, last week or last month')
    frequency = b.get('frequency')
    if frequency not in ('daily', 'weekly', 'monthly'):
        raise ScheduleError('the frequency is daily, weekly or monthly')
    try:
        send_time = datetime.strptime(str(b.get('send_time')), '%H:%M').time()
    except ValueError:
        raise ScheduleError('the time must be HH:MM')
    weekday = _whole(b.get('weekday'), 0, 6, 'the weekday') if frequency == 'weekly' else None
    month_day = _whole(b.get('month_day'), 1, 31, 'the day of the month') if frequency == 'monthly' else None
    recipients = clean_recipients(b.get('recipients'), mail_config())
    raw_blocks = b.get('blocks')
    if not isinstance(raw_blocks, list) or not any(str(k) in sr_blocks.KEYS for k in raw_blocks):
        raise ScheduleError('tick at least one block')
    blocks = sr_blocks.clean(raw_blocks)
    filters = _clean_filters(b.get('filters'))
    schedule_id = _whole(b.get('id'), 1, 2 ** 31 - 1, 'the schedule') if b.get('id') else None
    if schedule_id and not get_schedule(schedule_id):
        raise ScheduleError('this schedule no longer exists')
    s = {'frequency': frequency, 'send_time': send_time, 'weekday': weekday, 'month_day': month_day}
    params = {**s, 'name': name, 'period': period, 'to': recipients, 'enabled': bool(b.get('enabled', True)),
              'next': next_run(s, datetime.now()), 'me': me, 'id': schedule_id,
              'blocks': json.dumps(blocks), 'filters': json.dumps(filters)}
    if schedule_id:
        db.session.execute(text("""
            UPDATE super_report_email_schedules
               SET name = :name, period = :period, recipients = :to, frequency = :frequency,
                   send_time = :send_time, weekday = :weekday, month_day = :month_day, enabled = :enabled,
                   next_run_at = :next, blocks = CAST(:blocks AS jsonb), filters = CAST(:filters AS jsonb),
                   updated_by = :me, updated_at = NOW()
             WHERE id = :id
        """), params)
    else:
        schedule_id = db.session.execute(text("""
            INSERT INTO super_report_email_schedules (name, period, recipients, frequency, send_time, weekday,
                   month_day, enabled, next_run_at, blocks, filters, created_by, updated_by)
            VALUES (:name, :period, :to, :frequency, :send_time, :weekday, :month_day, :enabled, :next,
                    CAST(:blocks AS jsonb), CAST(:filters AS jsonb), :me, :me)
            RETURNING id
        """), params).scalar()
    db.session.commit()
    return schedule_id


def delete_schedule(schedule_id):
    db.session.execute(text("DELETE FROM super_report_email_schedules WHERE id = :id"), {'id': schedule_id})
    db.session.commit()
