"""
utils/worklist_email.py
────────────────────────────────────────────────────────────────
Scheduled email of saved Report Composer reports.

    settings smtp_*                the relay; nothing goes out until an admin sets it
    worklist_email_schedules         which worklists, to whom, when
    worklist_email_runs              what was sent, to whom, and what went wrong

run_due() is called every minute by APScheduler (app.py). It claims a schedule by moving
its next_run_at forward BEFORE sending: a crash loses one send instead of repeating it,
and a second scheduler (a one-off script calling create_app) cannot send it twice. A slot
missed while the app was down goes out once when it is back.

A worklist runs as it does on the page: its last saved version, in the rpt_ sandbox
(utils/worklist_engine), quick dates worked out on the day it runs. Nobody is there to
answer "ask when run", so those criteria use their saved values.

PATIENT DATA stays out of the message: the body holds worklist names and row counts, the
rows travel in attachments, and those go only to the domains in smtp_allowed_domains
(empty means none). Errors stay in the run log, for the admins who can read it.

Every time is naive hospital time from Python, as in the engine: LAUMC's Postgres is UTC.
"""
import calendar
import io
import json
import logging
import re
import smtplib
import ssl
from datetime import datetime, timedelta
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

import xlsxwriter
from sqlalchemy import text

from db import db
from utils import worklist_engine as engine
from utils.crypto import decrypt, encrypt
from utils.worklist_engine import WorklistError

logger = logging.getLogger("WORKLISTS")

MAX_ATTACHMENTS_BYTES = 10 * 1024 * 1024     # what a hospital relay reliably accepts
MAX_RECIPIENTS = 50
MAX_WORKLISTS = 20
SMTP_TIMEOUT = 30
STALE_RUN = timedelta(hours=1)               # longer than any run: 20 worklists at 20 s each

SECURITY = ('starttls', 'ssl', 'starttls_unverified', 'none')
XLSX_SUBTYPE = 'vnd.openxmlformats-officedocument.spreadsheetml.sheet'

_DEFAULTS = {'smtp_host': '', 'smtp_port': '587', 'smtp_security': 'starttls', 'smtp_username': '',
             'smtp_password': '', 'smtp_from': '', 'smtp_allowed_domains': '', 'demo_mode': 'false'}
_ADDRESS = re.compile(r"[A-Za-z0-9._%+'-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+")
_DOMAIN = re.compile(r'[a-z0-9-]+(\.[a-z0-9-]+)+')


class ScheduleError(ValueError):
    """A schedule or mail server setting that cannot be accepted, in words for the admin."""


# ── The relay ─────────────────────────────────────────────────────────────────

def mail_config():
    rows = db.session.execute(text("SELECT key, value FROM settings WHERE key = ANY(:keys)"),
                              {'keys': list(_DEFAULTS)}).fetchall()
    return {**_DEFAULTS, **{k: v or '' for k, v in rows}}


def allowed_domains(cfg):
    return [d.strip().lower() for d in cfg['smtp_allowed_domains'].split(',') if d.strip()]


def is_ready(cfg):
    return bool(cfg['smtp_host'] and cfg['smtp_from'] and allowed_domains(cfg))


def public_config(cfg):
    """The settings for the page: the password only as "one is saved"."""
    out = {k: cfg[k] for k in ('smtp_host', 'smtp_port', 'smtp_security', 'smtp_username',
                               'smtp_from', 'smtp_allowed_domains')}
    out['password_set'] = bool(cfg['smtp_password'])
    return out


def save_mail_config(form):
    """Save the relay. An empty host switches sending off, so it needs nothing else."""
    host = str(form.get('smtp_host') or '').strip()
    security = form.get('smtp_security') or 'starttls'
    sender = str(form.get('smtp_from') or '').strip()
    domains = [d.strip().lower() for d in str(form.get('smtp_allowed_domains') or '').split(',') if d.strip()]
    try:
        port = int(form.get('smtp_port') or 587)
    except (TypeError, ValueError):
        raise ScheduleError('the port must be a whole number')
    if not 1 <= port <= 65535:
        raise ScheduleError('the port must be between 1 and 65535')
    if security not in SECURITY:
        raise ScheduleError(f'unknown security {security!r}')
    if host and not _ADDRESS.fullmatch(sender):
        raise ScheduleError('the From address is not a valid email address')
    if host and not domains:
        raise ScheduleError('name at least one domain the attachments may be sent to')
    for d in domains:
        if not _DOMAIN.fullmatch(d):
            raise ScheduleError(f'{d!r} is not a domain, e.g. laumc.com')
    values = {'smtp_host': host, 'smtp_port': str(port), 'smtp_security': security, 'smtp_from': sender,
              'smtp_username': str(form.get('smtp_username') or '').strip(),
              'smtp_allowed_domains': ', '.join(domains)}
    password = str(form.get('smtp_password') or '')
    if form.get('clear_password'):
        values['smtp_password'] = ''
    elif password:
        values['smtp_password'] = encrypt(password)
    for key, value in values.items():
        db.session.execute(text("""
            INSERT INTO settings (key, value) VALUES (:k, :v)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """), {'k': key, 'v': value})
    db.session.commit()


def clean_recipients(raw, cfg):
    """A list of addresses, each at an allowed domain, or ScheduleError naming the first bad one."""
    items = raw if isinstance(raw, (list, tuple)) else re.split(r'[,;\s]+', str(raw or ''))
    out = []
    for item in items:
        addr = str(item).strip().lower()
        if addr and addr not in out:
            out.append(addr)
    if not out:
        raise ScheduleError('name at least one recipient')
    if len(out) > MAX_RECIPIENTS:
        raise ScheduleError(f'a schedule can have at most {MAX_RECIPIENTS} recipients')
    allowed = allowed_domains(cfg)
    if not allowed:
        raise ScheduleError('set the allowed recipient domains on the mail server first')
    for addr in out:
        if not _ADDRESS.fullmatch(addr):
            raise ScheduleError(f'{addr!r} is not an email address')
        if addr.rsplit('@', 1)[1] not in allowed:
            raise ScheduleError(f'{addr} is outside the allowed domains ({", ".join(allowed)})')
    return out


def send_mail(cfg, to, subject, body, attachments=()):
    """One message through the relay. Returns the recipients it refused; raises if it took none.
    Asked for TLS, it never falls back to plain: a relay without STARTTLS is an error."""
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = cfg['smtp_from']
    msg['To'] = ', '.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Message-ID'] = make_msgid()
    msg.set_content(body)
    for filename, data in attachments:
        msg.add_attachment(data, maintype='application', subtype=XLSX_SUBTYPE, filename=filename)

    host, port, security = cfg['smtp_host'], int(cfg['smtp_port'] or 587), cfg['smtp_security']
    context = ssl.create_default_context()
    if security == 'starttls_unverified':       # a relay on a private certificate authority
        context.check_hostname, context.verify_mode = False, ssl.CERT_NONE
    if security == 'ssl':
        connection = smtplib.SMTP_SSL(host, port, timeout=SMTP_TIMEOUT, context=context)
    else:
        connection = smtplib.SMTP(host, port, timeout=SMTP_TIMEOUT)
    with connection as smtp:
        if security in ('starttls', 'starttls_unverified'):
            smtp.starttls(context=context)
        if cfg['smtp_username']:
            smtp.login(cfg['smtp_username'], decrypt(cfg['smtp_password']))
        return smtp.send_message(msg)


# ── When ──────────────────────────────────────────────────────────────────────

def _falls_on(s, day):
    if s['frequency'] == 'weekly':
        return day.weekday() == s['weekday']
    if s['frequency'] == 'monthly':
        return day.day == min(s['month_day'], calendar.monthrange(day.year, day.month)[1])
    return True


def next_run(s, after):
    """The schedule's first slot strictly after `after`, on the hospital's wall clock."""
    day = after.date()
    while True:
        slot = datetime.combine(day, s['send_time'])
        if slot > after and _falls_on(s, day):
            return slot
        day += timedelta(days=1)


# ── The attachment ────────────────────────────────────────────────────────────

def _sum(values):
    return sum(v or 0 for v in values)


def table_for(compiled, res):
    """
    (headers, rows, total row or None) as the page shows this result (matrix() and
    renderTable() in worklists.html): labelled columns, a split spread across columns,
    totals only for measures that add up. A chart arrives as its table. compiled is None
    for hand-written SQL, whose column names are its headers.
    """
    if compiled is None or compiled['kind'] == 'list':
        return ([c['label'] for c in compiled['columns']] if compiled else res['columns']), res['rows'], None
    dims, measures, raw = compiled['dims'], compiled['measures'], res['rows']
    split = len(dims) == 2
    if split:
        names = list(dict.fromkeys(r[0] for r in raw))
        cols = sorted({r[1] for r in raw})
        cell = {(r[0], r[1]): r[2] for r in raw}
        data = [[cell.get((n, c), 0) for c in cols] for n in names]
        series = [(c, measures[0]['additive']) for c in cols]
        first = f"{dims[0]['label']} · {measures[0]['label']}"
    else:
        names = [r[0] for r in raw] if dims else ['All']
        data = [r[len(dims):] for r in raw]
        series = [(m['label'], m['additive']) for m in measures]
        first = dims[0]['label'] if dims else ''
    across = split and measures[0]['additive']      # a Total column on the right
    rows = [[n, *d, *([_sum(d)] if across else [])] for n, d in zip(names, data)]
    total = None
    if len(names) > 1:
        downs = [_sum(col) if additive else None for (_, additive), col in zip(series, zip(*data))]
        total = ['Total', *downs, *([_sum(map(_sum, data))] if across else [])]
    return [first, *[s[0] for s in series], *(['Total'] if across else [])], rows, total


def _cell(v):
    # Hand-written SQL can return anything (arrays, intervals); Excel takes these four.
    return v if v is None or isinstance(v, (str, int, float, bool)) else str(v)


def workbook(headers, rows, total=None):
    """An .xlsx. Text stays text: a value typed into the RIS must not run as a formula."""
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False,
                                     'strings_to_urls': False, 'strings_to_numbers': False,
                                     'nan_inf_to_errors': True})
    sheet = book.add_worksheet('Worklist')
    bold = book.add_format({'bold': True})
    sheet.write_row(0, 0, [_cell(h) for h in headers], bold)
    for i, row in enumerate(rows, 1):
        sheet.write_row(i, 0, [_cell(v) for v in row])
    if total:
        sheet.write_row(len(rows) + 1, 0, [_cell(v) for v in total], bold)
    sheet.freeze_panes(1, 0)
    sheet.autofit()
    book.close()
    return buf.getvalue()


# ── One send ──────────────────────────────────────────────────────────────────

def _produce(schedule_id):
    """Run each worklist of a schedule: (one log entry per worklist, attachments). One that
    fails is noted and the others still go."""
    rows = db.session.execute(text("""
        SELECT w.id, w.name, w.version, w.definition
          FROM worklist_email_schedule_items i
          JOIN worklists w ON w.id = i.worklist_id
         WHERE i.schedule_id = :id
         ORDER BY lower(w.name), w.id
    """), {'id': schedule_id}).mappings().all()
    detail, attachments, size = [], [], 0
    for w in rows:
        entry = {'worklist_id': w['id'], 'name': w['name'], 'version': w['version'], 'rows': None,
                 'attached': False, 'truncated': False, 'too_large': False, 'error': None}
        detail.append(entry)
        hand = w['definition'].get('sql') or None
        try:
            compiled = None if hand else engine.compile_definition(w['definition'])
            res = engine.run(compiled=compiled, hand_sql=hand, cap=engine.CSV_CAP)
        except WorklistError as exc:
            entry['error'] = str(exc)
            continue
        headers, table, total = table_for(compiled, res)
        entry.update(rows=len(table), truncated=res['truncated'])
        if not table:
            continue
        data = workbook(headers, table, total)
        if size + len(data) > MAX_ATTACHMENTS_BYTES:
            entry['too_large'] = True
            continue
        size += len(data)
        entry['attached'] = True
        attachments.append((engine.file_name(w['name']) + '.xlsx', data))
    return detail, attachments


def _incomplete(entry):
    return entry['error'] or entry['truncated'] or entry['too_large']


def _line(e):
    rows = f"{e['rows']:,} row{'' if e['rows'] == 1 else 's'}" if e['rows'] is not None else ''
    if e['error']:
        return 'could not be produced. An administrator can see why in Report Composer > Scheduled emails.'
    if e['too_large']:
        return f'{rows}, too large to email. Open it in RAYD: Report Composer.'
    if e['truncated']:
        return f'first {rows} only, attached. Open it in RAYD for the rest, or narrow its dates.'
    return f'{rows}, attached.' if e['rows'] else 'no rows.'


def _body(name, detail):
    lines = [f'{name} — {datetime.now():%d %b %Y, %H:%M}', '']
    lines += [f"- {e['name']}: {_line(e)}" for e in detail]
    lines += ['', 'The attachments may hold patient information. Do not forward them outside the hospital.',
              '', 'Sent automatically by RAYD.']
    return '\n'.join(lines)


def send_schedule(s, started_by=None):
    """Produce and mail one schedule, and log it. Never raises: the run says what happened."""
    run_id = db.session.execute(text("""
        INSERT INTO worklist_email_runs (schedule_id, schedule_name, started_by, started_at, recipients)
        VALUES (:sid, :name, :by, :at, :to) RETURNING id
    """), {'sid': s['id'], 'name': s['name'], 'by': started_by, 'at': datetime.now(),
           'to': list(s['recipients'])}).scalar()
    db.session.commit()
    detail, error = [], None
    try:
        cfg = mail_config()
        # Checked again here: the allowed domains may have changed since the schedule was saved.
        to = clean_recipients(s['recipients'], cfg)
        detail, attachments = _produce(s['id'])
        db.session.commit()         # no transaction held open across the relay
        refused = send_mail(cfg, to, f"{s['name']} · {datetime.now():%d %b %Y}",
                            _body(s['name'], detail), attachments)
        if refused:
            error = 'Refused by the relay: ' + ', '.join(sorted(refused))
        status = 'partial' if refused or any(_incomplete(e) for e in detail) else 'sent'
    except Exception as exc:
        db.session.rollback()
        logger.warning("worklist email schedule %s failed: %s", s['id'], exc)
        status, error = 'failed', str(exc)[:500] or exc.__class__.__name__
    db.session.execute(text("""
        UPDATE worklist_email_runs
           SET status = :status, finished_at = :at, detail = CAST(:detail AS jsonb), error = :error
         WHERE id = :id
    """), {'status': status, 'at': datetime.now(), 'detail': json.dumps(detail), 'error': error, 'id': run_id})
    db.session.commit()
    return {'id': run_id, 'status': status, 'error': error, 'detail': detail}


def run_due():
    """Send every schedule that has fallen due. A no-op until the relay is set, and in demo
    mode, when the ETL is stopped and the figures would be stale."""
    cfg = mail_config()
    if not is_ready(cfg) or cfg['demo_mode'].lower() == 'true':
        db.session.rollback()
        return 0
    now = datetime.now()
    db.session.execute(text("""
        UPDATE worklist_email_runs
           SET status = 'failed', finished_at = :now, error = 'Interrupted: the app stopped during this run.'
         WHERE status = 'running' AND started_at < :stale
    """), {'now': now, 'stale': now - STALE_RUN})
    due = db.session.execute(text("""
        SELECT * FROM worklist_email_schedules
         WHERE enabled AND next_run_at <= :now
         ORDER BY next_run_at, id
    """), {'now': now}).mappings().all()
    db.session.commit()
    sent = 0
    for s in due:
        claimed = db.session.execute(text("""
            UPDATE worklist_email_schedules SET next_run_at = :next
             WHERE id = :id AND enabled AND next_run_at = :seen
         RETURNING id
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
            'finished_at': _minutes(r['finished_at']), 'recipients': r['recipients'],
            'detail': r['detail'], 'error': r['error'], 'started_by': r['started_by_name']}


_RUN_SELECT = """
    SELECT r.*, u.username AS started_by_name
      FROM worklist_email_runs r LEFT JOIN users u ON u.id = r.started_by
"""


def list_schedules():
    schedules = db.session.execute(text(
        "SELECT * FROM worklist_email_schedules ORDER BY lower(name), id")).mappings().all()
    items = db.session.execute(text("""
        SELECT i.schedule_id, w.id, w.name
          FROM worklist_email_schedule_items i JOIN worklists w ON w.id = i.worklist_id
         ORDER BY lower(w.name), w.id
    """)).mappings().all()
    last = db.session.execute(text(f"""
        SELECT DISTINCT ON (schedule_id) * FROM ({_RUN_SELECT} WHERE r.schedule_id IS NOT NULL) x
         ORDER BY schedule_id, started_at DESC, id DESC
    """)).mappings().all()
    last = {r['schedule_id']: _run_json(r) for r in last}
    return [{'id': s['id'], 'name': s['name'], 'recipients': s['recipients'], 'frequency': s['frequency'],
             'send_time': s['send_time'].strftime('%H:%M'), 'weekday': s['weekday'],
             'month_day': s['month_day'], 'enabled': s['enabled'], 'next_run_at': _minutes(s['next_run_at']),
             'worklists': [{'id': i['id'], 'name': i['name']} for i in items if i['schedule_id'] == s['id']],
             'last_run': last.get(s['id'])} for s in schedules]


def list_runs(schedule_id, limit=20):
    rows = db.session.execute(text(f"""
        {_RUN_SELECT} WHERE r.schedule_id = :id ORDER BY r.started_at DESC, r.id DESC LIMIT :limit
    """), {'id': schedule_id, 'limit': limit}).mappings().all()
    return [_run_json(r) for r in rows]


def get_schedule(schedule_id):
    row = db.session.execute(text("SELECT * FROM worklist_email_schedules WHERE id = :id"),
                             {'id': schedule_id}).mappings().first()
    return dict(row) if row else None


def _whole(value, low, high, what):
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ScheduleError(f'{what} must be a whole number')
    if not low <= n <= high:
        raise ScheduleError(f'{what} must be from {low} to {high}')
    return n


def save_schedule(b, me):
    """Create or update a schedule for user `me`, and return its id. Saving also restarts its
    clock, so resuming a paused schedule does not send what it missed."""
    name = ' '.join(str(b.get('name') or '').split())[:120]
    if not name:
        raise ScheduleError('a schedule needs a name')
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
    try:
        ids = sorted({int(i) for i in b.get('worklist_ids') or []})
    except (TypeError, ValueError):
        raise ScheduleError('pick the reports from the list')
    if not ids:
        raise ScheduleError('pick at least one report')
    if len(ids) > MAX_WORKLISTS:
        raise ScheduleError(f'a schedule can hold at most {MAX_WORKLISTS} reports')

    schedule_id = _whole(b.get('id'), 1, 2 ** 31 - 1, 'the schedule') if b.get('id') else None
    kept = []
    if schedule_id:
        if not get_schedule(schedule_id):
            raise ScheduleError('this schedule no longer exists')
        kept = list(db.session.execute(text(
            "SELECT worklist_id FROM worklist_email_schedule_items WHERE schedule_id = :id"),
            {'id': schedule_id}).scalars())
    # Someone's personal worklist is theirs: the only way one gets in here is its owner's own
    # choice. Another admin editing the schedule may leave it in, not add one of their own finding.
    usable = set(db.session.execute(text("""
        SELECT id FROM worklists
         WHERE id = ANY(:ids) AND (visibility = 'shared' OR owner_id = :me OR id = ANY(:kept))
    """), {'ids': ids, 'me': me, 'kept': kept}).scalars())
    if usable != set(ids):
        db.session.rollback()
        raise ScheduleError('one of the reports no longer exists, or is not yours to send')

    s = {'frequency': frequency, 'send_time': send_time, 'weekday': weekday, 'month_day': month_day}
    params = {**s, 'name': name, 'to': recipients, 'enabled': bool(b.get('enabled', True)),
              'next': next_run(s, datetime.now()), 'me': me, 'id': schedule_id}
    if schedule_id:
        db.session.execute(text("""
            UPDATE worklist_email_schedules
               SET name = :name, recipients = :to, frequency = :frequency, send_time = :send_time,
                   weekday = :weekday, month_day = :month_day, enabled = :enabled, next_run_at = :next,
                   updated_by = :me
             WHERE id = :id
        """), params)
        db.session.execute(text("DELETE FROM worklist_email_schedule_items WHERE schedule_id = :id"),
                           {'id': schedule_id})
    else:
        schedule_id = db.session.execute(text("""
            INSERT INTO worklist_email_schedules (name, recipients, frequency, send_time, weekday,
                                                  month_day, enabled, next_run_at, created_by, updated_by)
            VALUES (:name, :to, :frequency, :send_time, :weekday, :month_day, :enabled, :next, :me, :me)
            RETURNING id
        """), params).scalar()
    db.session.execute(text("""
        INSERT INTO worklist_email_schedule_items (schedule_id, worklist_id)
        SELECT :id, unnest(CAST(:ids AS integer[]))
    """), {'id': schedule_id, 'ids': ids})
    db.session.commit()
    return schedule_id


def delete_schedule(schedule_id):
    db.session.execute(text("DELETE FROM worklist_email_schedules WHERE id = :id"), {'id': schedule_id})
    db.session.commit()


def schedules_using(worklist_id):
    return list(db.session.execute(text("""
        SELECT s.name FROM worklist_email_schedule_items i
          JOIN worklist_email_schedules s ON s.id = i.schedule_id
         WHERE i.worklist_id = :id ORDER BY lower(s.name)
    """), {'id': worklist_id}).scalars())
