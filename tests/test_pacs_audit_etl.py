"""
tests/test_pacs_audit_etl.py — first opens copied from the PACS audit trail (to-do item 2,
migration 0155, ETL_JOBS/etl_pacs_audit.py).

AUDITING.AUDIT_LOG holds 217.8 M rows, so each night must read it one day at a time through
its EVENT_DATE index, with dates sent as text, and never the patient's name, ID or the
description. Oracle is replaced by a stand-in that answers each day's query.
"""
import re
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_pacs_audit as audit

NOW = datetime(2026, 11, 18, 5, 0, 0)
FMT = '%Y-%m-%d %H:%M:%S'


class FakeOracle:
    """Answers each query with rows_for(since, until); fail_on = the query number that fails."""
    def __init__(self, rows_for=lambda since, until: [], fail_on=None):
        self.rows_for, self.fail_on = rows_for, fail_on
        self.queries, self.pending = [], []

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params):
        self.queries.append((sql, dict(params)))
        if self.fail_on == len(self.queries):
            raise RuntimeError('ORA-00942: table or view does not exist')
        since, until = (datetime.strptime(params[k], FMT) for k in ('since', 'until'))
        self.pending = list(self.rows_for(since, until))

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


def one_open_per_day(since, until):
    """One reader's open at 08:33 of each day the query covers."""
    at = since.replace(hour=8, minute=33)
    if not (since <= at < until):
        return []
    return [(int(at.strftime('%Y%m%d')), at, 'open', 'trad@ad', 'Vue PACS Client', 'MVCLIENT',
             f'1.2.840.{at:%Y%m%d}', f'1005{at:%m%d}')]


@pytest.fixture
def engine(app):
    from db import db

    def clean():
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM etl_pacs_audit_opens'))
            conn.execute(text("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'"))
    with app.app_context():
        clean()
        yield db.engine
        clean()


def run(engine, monkeypatch, pacs, now=NOW):
    monkeypatch.setattr(audit, 'OracleConnector', SimpleNamespace(get_connection=lambda source: pacs))
    return audit.run_pacs_audit_etl(engine, 'PROD_ORACLE', now=now)


def setting(engine, key, value=None):
    with engine.begin() as conn:
        if value is not None:
            conn.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                              "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {'k': key, 'v': value})
        return conn.execute(text("SELECT value FROM settings WHERE key = :k"), {'k': key}).scalar()


def copied(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT log_dbid, event_at, kind, user_name, study_instance_uid "
                                 "FROM etl_pacs_audit_opens ORDER BY log_dbid")).fetchall()


def test_the_first_night_copies_from_the_start_day_by_day(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-11-15')
    pacs = FakeOracle(one_open_per_day)

    assert run(engine, monkeypatch, pacs) == 3

    days = [(p['since'], p['until']) for _, p in pacs.queries]
    assert days == [('2026-11-15 00:00:00', '2026-11-16 00:00:00'), ('2026-11-16 00:00:00', '2026-11-17 00:00:00'),
                    ('2026-11-17 00:00:00', '2026-11-18 00:00:00'), ('2026-11-18 00:00:00', '2026-11-18 05:00:00')]
    rows = copied(engine)
    assert [r.event_at for r in rows] == [datetime(2026, 11, d, 8, 33) for d in (15, 16, 17)]
    assert {(r.kind, r.user_name) for r in rows} == {('open', 'trad@ad')}
    assert setting(engine, audit.FROM_KEY) == '2026-11-15'
    assert setting(engine, audit.UNTIL_KEY) == '2026-11-18 05:00:00'


def test_with_no_start_set_the_copy_begins_on_1_october(engine, monkeypatch):
    pacs = FakeOracle()
    run(engine, monkeypatch, pacs)
    assert pacs.queries[0][1]['since'] == '2026-10-01 00:00:00'
    assert setting(engine, audit.FROM_KEY) == '2026-10-01'


def test_each_query_reads_one_day_through_the_date_index_and_no_patient_data(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-11-17')
    pacs = FakeOracle()
    run(engine, monkeypatch, pacs)

    sql, params = pacs.queries[0]
    assert 'INDEX(a AUDIT_EVENT_DATE_INX)' in sql
    assert "a.EVENT_DATE >= TO_DATE(:since, 'YYYY-MM-DD HH24:MI:SS')" in sql
    assert all(isinstance(v, str) for v in params.values())      # never a TIMESTAMP bind
    selected = re.search(r'SELECT(.*?)FROM', sql, re.S).group(1).upper()
    for column in ('LAST_NAME', 'FIRST_NAME', 'PATIENT_ID', 'DESCRIPTION', 'ADDITIONAL'):
        assert column not in selected


def test_the_next_night_starts_a_day_before_the_last_copy_and_keeps_rows_once(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-11-15')
    run(engine, monkeypatch, FakeOracle(one_open_per_day))
    pacs = FakeOracle(one_open_per_day)

    run(engine, monkeypatch, pacs, now=NOW + timedelta(days=1))

    assert pacs.queries[0][1]['since'] == '2026-11-17 05:00:00'
    assert [r.event_at.day for r in copied(engine)] == [15, 16, 17, 18]
    assert setting(engine, audit.FROM_KEY) == '2026-11-15'
    assert setting(engine, audit.UNTIL_KEY) == '2026-11-19 05:00:00'


def test_a_long_backfill_spreads_over_nights(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-07-01')
    pacs = FakeOracle()
    run(engine, monkeypatch, pacs)

    assert len(pacs.queries) == 62
    assert setting(engine, audit.UNTIL_KEY) == '2026-09-01 00:00:00'
    pacs = FakeOracle()
    run(engine, monkeypatch, pacs, now=NOW + timedelta(days=1))
    assert pacs.queries[0][1]['since'] == '2026-08-31 00:00:00'
    assert setting(engine, audit.FROM_KEY) == '2026-07-01'


def test_a_failed_night_keeps_the_days_it_copied(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-11-15')
    with pytest.raises(RuntimeError):
        run(engine, monkeypatch, FakeOracle(one_open_per_day, fail_on=3))

    assert [r.event_at.day for r in copied(engine)] == [15, 16]
    assert setting(engine, audit.UNTIL_KEY) == '2026-11-17 00:00:00'
    with engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM etl_job_log WHERE job_name = 'PACS_AUDIT_OPENS_ETL' "
                                   "ORDER BY id DESC LIMIT 1")).scalar()
    assert status == 'FAILED'


def test_moving_the_start_earlier_starts_the_copy_over(engine, monkeypatch):
    setting(engine, audit.SINCE_KEY, '2026-11-16')
    run(engine, monkeypatch, FakeOracle(one_open_per_day))
    setting(engine, audit.SINCE_KEY, '2026-11-14')
    pacs = FakeOracle(one_open_per_day)

    run(engine, monkeypatch, pacs, now=NOW + timedelta(hours=1))

    assert pacs.queries[0][1]['since'] == '2026-11-14 00:00:00'
    assert setting(engine, audit.FROM_KEY) == '2026-11-14'
    assert [r.event_at.day for r in copied(engine)] == [14, 15, 16, 17]


def test_coverage_is_what_the_copy_holds(engine, monkeypatch):
    from db import db
    assert audit.coverage(db.session) == (None, None)
    setting(engine, audit.SINCE_KEY, '2026-11-15')
    run(engine, monkeypatch, FakeOracle())
    assert audit.coverage(db.session) == (datetime(2026, 11, 15), NOW)
