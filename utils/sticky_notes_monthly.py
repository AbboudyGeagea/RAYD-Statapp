"""
utils/sticky_notes_monthly.py — the automated monthly JCI report.

From the 2nd of each month the preceding month is generated once and kept in
sticky_notes_monthly_reports as the workbook it was, so that JCI evidence does not change when
a department is ticked later or a study is reopened. Only an admin generating it again replaces
it. The 2nd, not the 1st, because the data is a day late: a note written on the night of the
last day reaches Postgres with the next morning's ETL.

It waits for that morning's note copy to have succeeded, up to the 5th. After that it goes
ahead, and the workbook says when the data was last copied.

Email goes through the Report Composer's relay and its rules (utils/worklist_email): nothing
is sent until an admin has set the relay, the rows travel only in the attachment and only to
the allowed domains, and the message itself holds counts. A failed send is kept and tried
again a day later. With no relay or no recipients the report is still generated and can be
downloaded from the page.

Every time is naive hospital time from Python: LAUMC's Postgres is UTC.
"""
import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from db import db
from utils import sticky_notes, worklist_email

logger = logging.getLogger(__name__)

RECIPIENTS_KEY = 'sticky_notes_monthly_recipients'
GENERATE_FROM_DAY = 2
STOP_WAITING_DAY = 5
RETRY_AFTER = timedelta(days=1)


class MonthError(ValueError):
    """A month that cannot be generated, in words for the admin."""


def _month_end(first):
    return (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


def month_label(first):
    return first.strftime('%B %Y')


def get_recipients():
    value = db.session.execute(text("SELECT value FROM settings WHERE key = :k"),
                               {'k': RECIPIENTS_KEY}).scalar()
    return [a.strip() for a in (value or '').split(',') if a.strip()]


def save_recipients(raw):
    """An empty list switches email off. Anything else must pass the relay's rules."""
    items = raw if isinstance(raw, (list, tuple)) else str(raw or '').replace(';', ',').split(',')
    items = [str(a).strip() for a in items if str(a).strip()]
    cleaned = worklist_email.clean_recipients(items, worklist_email.mail_config()) if items else []
    db.session.execute(text("""
        INSERT INTO settings (key, value) VALUES (:k, :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """), {'k': RECIPIENTS_KEY, 'v': ', '.join(cleaned)})
    return cleaned


def _exists(first):
    return db.session.execute(text("SELECT 1 FROM sticky_notes_monthly_reports WHERE report_month = :m"),
                              {'m': first}).scalar() is not None


def _notes_copied_on(day):
    return db.session.execute(text("""
        SELECT 1 FROM etl_job_log
         WHERE job_name = 'STICKY_NOTES_ETL' AND status = 'SUCCESS'
           AND start_time >= :day AND start_time < :next
         LIMIT 1
    """), {'day': day, 'next': day + timedelta(days=1)}).scalar() is not None


def generate(first, now=None):
    """Build one month and keep it, replacing any earlier copy and its email state."""
    now = now or datetime.now()
    end = _month_end(first)
    cases, summary = sticky_notes.build(first, end)
    data = sticky_notes.workbook(first, end, cases, summary, now)
    db.session.execute(text("""
        INSERT INTO sticky_notes_monthly_reports
            (report_month, generated_at, case_count, noted_count, open_seen_count, workbook)
        VALUES (:m, :at, :cases, :noted, :seen, :data)
        ON CONFLICT (report_month) DO UPDATE SET
            generated_at = EXCLUDED.generated_at, case_count = EXCLUDED.case_count,
            noted_count = EXCLUDED.noted_count, open_seen_count = EXCLUDED.open_seen_count,
            workbook = EXCLUDED.workbook, email_attempted_at = NULL, emailed_at = NULL,
            emailed_to = NULL, email_error = NULL
    """), {'m': first, 'at': now, 'cases': summary['kpi']['cases'], 'noted': summary['kpi']['noted'],
           'seen': summary['kpi']['open_seen'], 'data': data})
    db.session.commit()
    return summary['kpi']


def generate_on_request(month_text, now=None):
    """An admin asking for a month: only a finished one, or the figures would be partial."""
    now = now or datetime.now()
    try:
        first = datetime.strptime(str(month_text), '%Y-%m').date().replace(day=1)
    except ValueError:
        raise MonthError('The month must look like 2026-10.')
    if first >= now.date().replace(day=1):
        raise MonthError('Only a finished month can be generated.')
    return first, generate(first, now)


def _body(first, row):
    return '\n'.join([
        f'{sticky_notes.TITLE}: {month_label(first)}',
        '',
        f"Urgent emergent cases: {row['case_count']:,}",
        f"With a sticky note: {row['noted_count']:,}",
        f"With a first open seen: {row['open_seen_count']:,}",
        '',
        'The details are in the attachment, which holds patient information. Do not forward it '
        'outside the hospital.',
        '',
        'PACS keeps only the latest open, so the first open is the earliest one RAYD could see. '
        'The attachment explains how to read it.',
        '',
        'Sent automatically by RAYD.',
    ])


def send(first, now=None):
    """Mail one kept month if it is due. Returns True if it went out. Claims the month before
    sending, so a second scheduler cannot send it twice and a crash loses one attempt."""
    now = now or datetime.now()
    cfg = worklist_email.mail_config()
    recipients = get_recipients()
    if not recipients or not worklist_email.is_ready(cfg):
        return False
    row = db.session.execute(text("""
        UPDATE sticky_notes_monthly_reports SET email_attempted_at = :now
         WHERE report_month = :m AND emailed_at IS NULL
           AND (email_attempted_at IS NULL OR email_attempted_at <= :retry)
     RETURNING case_count, noted_count, open_seen_count, workbook
    """), {'now': now, 'm': first, 'retry': now - RETRY_AFTER}).mappings().first()
    db.session.commit()
    if row is None:
        return False

    subject = f'Urgent emergent report · {month_label(first)}'
    filename = f"urgent_emergent_{first.strftime('%Y_%m')}.xlsx"
    try:
        to = worklist_email.clean_recipients(recipients, cfg)     # the domains may have changed
        data = bytes(row['workbook'])
        if len(data) > worklist_email.MAX_ATTACHMENTS_BYTES:
            raise ValueError('The workbook is too large to email. Download it from the Sticky Notes page.')
        refused = worklist_email.send_mail(cfg, to, subject, _body(first, row), [(filename, data)])
        accepted = [a for a in to if a not in refused]
        error = ('Refused by the relay: ' + ', '.join(sorted(refused))) if refused else None
        db.session.execute(text("""
            UPDATE sticky_notes_monthly_reports
               SET emailed_at = :now, emailed_to = :to, email_error = :error
             WHERE report_month = :m
        """), {'now': now, 'to': accepted, 'error': error, 'm': first})
        db.session.commit()
        return True
    except Exception as exc:
        db.session.rollback()
        logger.warning("Sticky Notes monthly email for %s failed: %s", first, exc)
        db.session.execute(text("""
            UPDATE sticky_notes_monthly_reports SET email_error = :error WHERE report_month = :m
        """), {'error': str(exc)[:500] or exc.__class__.__name__, 'm': first})
        db.session.commit()
        return False


def run_due(now=None):
    """Called hourly (app.py). Does nothing most of the time. Demo mode stops the ETL, so its
    figures would be stale. Returns what happened, for the log."""
    now = now or datetime.now()
    if worklist_email.mail_config()['demo_mode'].lower() == 'true':
        db.session.rollback()
        return None
    today = now.date()
    if today.day < GENERATE_FROM_DAY:
        db.session.rollback()
        return None

    first, _ = sticky_notes.previous_month(today)
    done = []
    if not _exists(first):
        if today.day < STOP_WAITING_DAY and not _notes_copied_on(today):
            db.session.rollback()
            return None
        generate(first, now)
        done.append('generated')
    if send(first, now):
        done.append('emailed')
    db.session.rollback()
    return f"{month_label(first)}: {' and '.join(done)}" if done else None


def list_reports():
    rows = db.session.execute(text("""
        SELECT report_month, generated_at, case_count, noted_count, open_seen_count,
               emailed_at, emailed_to, email_error
          FROM sticky_notes_monthly_reports
         ORDER BY report_month DESC
    """)).mappings().all()
    return [{
        'month': r['report_month'].strftime('%Y-%m'), 'label': month_label(r['report_month']),
        'generated_at': r['generated_at'].strftime('%Y-%m-%d %H:%M'),
        'cases': r['case_count'], 'noted': r['noted_count'], 'open_seen': r['open_seen_count'],
        'emailed_at': r['emailed_at'].strftime('%Y-%m-%d %H:%M') if r['emailed_at'] else None,
        'emailed_to': r['emailed_to'] or [], 'email_error': r['email_error'],
    } for r in rows]


def read_workbook(month_text):
    """(file name, bytes) of a kept month, or None."""
    try:
        first = datetime.strptime(str(month_text), '%Y-%m').date().replace(day=1)
    except ValueError:
        return None
    data = db.session.execute(text("SELECT workbook FROM sticky_notes_monthly_reports WHERE report_month = :m"),
                              {'m': first}).scalar()
    return (f"urgent_emergent_{first.strftime('%Y_%m')}.xlsx", bytes(data)) if data is not None else None
