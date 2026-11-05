"""
tests/test_sticky_notes_etl.py
────────────────────────────────────────────────────────────────
The JCI urgent read measure needs note times and lock times in Postgres, copied by the
nightly ETL (migration 0143). Nothing else may keep reading the PACS server, so the lock
fields come along with the studies pull that already runs, and the note query is one
read a night.

Oracle is replaced by stand-ins that return rows in the order of each job's SELECT.
"""
import re
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_didb_studies, etl_sticky_notes

GO_LIVE = date(2018, 1, 1)
NIGHT = datetime(2026, 11, 6, 5, 0, 0)
TODAY = NIGHT.date()


class FakeOracle:
    def __init__(self, rows=()):
        self.rows, self.sql, self.params, self.pending = list(rows), None, None, []
        self.statements = []

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params
        self.statements.append(sql)
        self.pending = list(self.rows)

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


@pytest.fixture
def engine(app):
    from db import db
    with app.app_context():
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM etl_sticky_notes'))
            conn.execute(text('DELETE FROM study_access_history'))
        yield db.engine
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM etl_sticky_notes'))
            conn.execute(text('DELETE FROM study_access_history'))


def study(engine, uid, study_date=TODAY, locked=None, lock_time=None, login=None, access=None, composed=None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_date, study_locked, study_lock_time,
                                          rep_last_accessed_by, last_accessed_time, rep_study_last_composed_ts)
            VALUES (:u, :u, :d, :l, :t, :b, :a, :c)
        """), {'u': uid, 'd': study_date, 'l': locked, 't': lock_time, 'b': login, 'a': access, 'c': composed})


def note_row(data_uid, study_uid, created=NIGHT, author='res.one@ad', deleted='N', data_len=40):
    return (data_uid, study_uid, author, created, author, created, deleted, data_len)


def run_notes(engine, monkeypatch, rows):
    pacs = FakeOracle(rows)
    monkeypatch.setattr(etl_sticky_notes, 'OracleConnector',
                        SimpleNamespace(get_connection=lambda source: pacs))
    return pacs, etl_sticky_notes.run_sticky_notes_etl(engine, 'PROD_ORACLE', GO_LIVE)


def notes(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            'SELECT data_db_uid, study_db_uid, user_name, delete_indication FROM etl_sticky_notes '
            'ORDER BY data_db_uid')).fetchall()


# ── the note copy ─────────────────────────────────────────────────────────────

def test_first_run_takes_every_note_since_go_live(engine, monkeypatch):
    study(engine, 100)
    pacs, total = run_notes(engine, monkeypatch, [note_row(1, 100), note_row(2, 100, author='res.two@ad')])

    assert total == 2
    assert [tuple(r) for r in notes(engine)] == [(1, 100, 'res.one@ad', 'N'), (2, 100, 'res.two@ad', 'N')]
    assert pacs.params == {'cutoff': '2018-01-01'}


def test_later_runs_only_read_recent_notes(engine, monkeypatch):
    study(engine, 100)
    run_notes(engine, monkeypatch, [note_row(1, 100)])

    pacs, _total = run_notes(engine, monkeypatch, [])

    assert 'lookback' in pacs.params
    assert ':lookback' in pacs.sql


def test_note_text_and_display_name_are_never_read(engine, monkeypatch):
    study(engine, 100)
    pacs, _total = run_notes(engine, monkeypatch, [note_row(1, 100)])

    columns = pacs.sql.upper().split('FROM')[0]
    assert not re.search(r'\bD\.DATA\b', columns)
    assert 'DISPLAY_NAME' not in columns


def test_a_later_deletion_in_pacs_reaches_the_row_already_copied(engine, monkeypatch):
    study(engine, 100)
    run_notes(engine, monkeypatch, [note_row(1, 100)])
    run_notes(engine, monkeypatch, [note_row(1, 100, deleted='Y', data_len=1)])

    assert notes(engine)[0].delete_indication == 'Y'


def test_notes_on_studies_rayd_does_not_hold_are_removed(engine, monkeypatch):
    # An SJH study, or a device the studies ETL leaves out.
    study(engine, 100)
    run_notes(engine, monkeypatch, [note_row(1, 100), note_row(2, 999)])

    assert [r.data_db_uid for r in notes(engine)] == [1]


def test_every_statement_sent_to_pacs_is_a_select(engine, monkeypatch):
    study(engine, 100)
    pacs, _total = run_notes(engine, monkeypatch, [note_row(1, 100)])

    assert pacs.statements
    assert all(s.lstrip().upper().startswith('SELECT') for s in pacs.statements)


# ── the access history ────────────────────────────────────────────────────────

def history(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            'SELECT study_db_uid, captured_at, study_locked FROM study_access_history '
            'ORDER BY captured_at, study_db_uid')).fetchall()


def test_each_night_keeps_a_copy_of_recent_studies(engine):
    study(engine, 100, locked='UNLOCKED', lock_time=NIGHT - timedelta(hours=8), login='res.one@ad')

    assert etl_sticky_notes.record_access_history(engine, NIGHT) == 1
    assert etl_sticky_notes.record_access_history(engine, NIGHT + timedelta(days=1)) == 1
    assert len(history(engine)) == 2


def test_running_twice_at_the_same_moment_adds_nothing(engine):
    study(engine, 100)
    etl_sticky_notes.record_access_history(engine, NIGHT)

    assert etl_sticky_notes.record_access_history(engine, NIGHT) == 0


def test_studies_older_than_the_nightly_window_are_not_copied(engine):
    study(engine, 100, study_date=TODAY - timedelta(days=30))

    assert etl_sticky_notes.record_access_history(engine, NIGHT) == 0


def test_history_of_a_study_rayd_no_longer_holds_is_removed(engine):
    # What the RH-only purge leaves behind when it removes an SJH study.
    study(engine, 100)
    study(engine, 200)
    etl_sticky_notes.record_access_history(engine, NIGHT)
    with engine.begin() as conn:
        conn.execute(text('DELETE FROM etl_didb_studies WHERE study_db_uid = 200'))

    etl_sticky_notes.record_access_history(engine, NIGHT + timedelta(days=1))

    assert {r.study_db_uid for r in history(engine)} == {100}


# ── the studies pull ──────────────────────────────────────────────────────────

def _select_columns(sql):
    """How many expressions the SELECT list has, ignoring commas inside brackets."""
    body = sql[sql.upper().index('SELECT') + 6:sql.upper().index('FROM MEDISTORE')]
    depth, count = 0, 1
    for ch in body:
        depth += ch == '('
        depth -= ch == ')'
        count += ch == ',' and depth == 0
    return count


def pacs_study_row(sql, locked, lock_time, login):
    """A row as long as the SELECT list, with the columns Postgres requires filled. A
    SELECT that left out the lock columns would misplace these values and fail the test."""
    row = [None] * _select_columns(sql)
    row[0], row[1], row[6] = 100, 100, TODAY            # study, patient, study date
    row[30], row[39], row[40] = 'false', 'false', 'false'   # the three required flags
    row[-3:] = [locked, lock_time, login]
    return tuple(row)


@pytest.mark.parametrize('join_fails', [False, True])
def test_the_studies_pull_stores_the_lock_fields(engine, monkeypatch, join_fails):
    from db import chunked_upsert

    class Pacs(FakeOracle):
        def execute(self, sql, params=None):
            if join_fails and 'didb_patients_view' in sql:
                raise RuntimeError('ORA-00942: table or view does not exist')
            self.rows = [pacs_study_row(sql, 'LOCKED', NIGHT, 'res.one@ad')]
            super().execute(sql, params)

    pacs = Pacs()
    monkeypatch.setattr(etl_didb_studies, 'OracleConnector',
                        SimpleNamespace(get_connection=lambda source: pacs))

    total, uids = etl_didb_studies.run_studies_etl(engine, 'PROD_ORACLE', 'etl_didb_studies',
                                                   chunked_upsert, GO_LIVE)

    assert (total, uids) == (1, [100])
    with engine.connect() as conn:
        row = conn.execute(text('SELECT study_locked, study_lock_time, rep_last_accessed_by '
                                'FROM etl_didb_studies WHERE study_db_uid = 100')).one()
    assert tuple(row) == ('LOCKED', NIGHT, 'res.one@ad')


def test_the_next_night_updates_the_lock_fields(engine, monkeypatch):
    from db import chunked_upsert
    study(engine, 100, locked='LOCKED', lock_time=NIGHT, login='res.one@ad')

    class Pacs(FakeOracle):
        def execute(self, sql, params=None):
            self.rows = [pacs_study_row(sql, 'UNLOCKED', NIGHT + timedelta(hours=1), 'res.one@ad')]
            super().execute(sql, params)

    pacs = Pacs()
    monkeypatch.setattr(etl_didb_studies, 'OracleConnector',
                        SimpleNamespace(get_connection=lambda source: pacs))

    etl_didb_studies.run_studies_etl(engine, 'PROD_ORACLE', 'etl_didb_studies', chunked_upsert, GO_LIVE)

    with engine.connect() as conn:
        row = conn.execute(text('SELECT study_locked, study_lock_time FROM etl_didb_studies '
                                'WHERE study_db_uid = 100')).one()
    assert tuple(row) == ('UNLOCKED', NIGHT + timedelta(hours=1))


# ── the question the copy has to answer ───────────────────────────────────────

# Given to the operator to run in Postgres: on notes not yet reported when the next nightly
# copy was taken, was there a lock before the note, and by whom?
EVIDENCE_SQL = """
WITH first_note AS (
    SELECT DISTINCT ON (n.study_db_uid) n.study_db_uid, n.user_name, n.date_created
    FROM etl_sticky_notes n
    WHERE n.delete_indication = 'N' AND n.data_len > 1
    ORDER BY n.study_db_uid, n.date_created
),
next_copy AS (
    SELECT DISTINCT ON (f.study_db_uid)
           f.study_db_uid, f.user_name, f.date_created, h.captured_at,
           h.study_lock_time, h.rep_last_accessed_by, h.last_accessed_time
    FROM first_note f
    JOIN study_access_history h ON h.study_db_uid = f.study_db_uid
                               AND h.captured_at >= f.date_created
    ORDER BY f.study_db_uid, h.captured_at
)
SELECT CASE WHEN c.study_lock_time IS NULL THEN '1 no lock'
            WHEN c.study_lock_time <= c.date_created THEN '2 lock before note'
            ELSE '3 lock after note' END AS lock_timing,
       COUNT(*) AS notes,
       COUNT(*) FILTER (WHERE UPPER(c.rep_last_accessed_by) = UPPER(c.user_name)) AS login_is_author,
       COUNT(*) FILTER (WHERE c.last_accessed_time <= c.date_created) AS access_before_note,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
             ORDER BY EXTRACT(EPOCH FROM c.date_created - c.study_lock_time) / 60))::numeric, 1)
             AS median_min_lock_to_note
FROM next_copy c
JOIN etl_didb_studies s ON s.study_db_uid = c.study_db_uid
WHERE COALESCE(s.rep_study_last_composed_ts, 'infinity'::timestamp) > c.captured_at
GROUP BY 1
ORDER BY 1
"""


def test_the_evidence_query_reads_notes_as_the_next_copy_saw_them(engine, monkeypatch):
    evening = NIGHT - timedelta(hours=9)
    author = 'res.one@ad'
    # A lock by the author 30 minutes before the note, not reported by the copy: counts.
    study(engine, 100, locked='UNLOCKED', lock_time=evening - timedelta(minutes=30),
          login=author, access=evening - timedelta(minutes=30))
    # No lock at all, not reported: counts as "no lock".
    study(engine, 101, access=evening - timedelta(minutes=5))
    # Another login's lock after the note, not reported: counts.
    study(engine, 102, locked='UNLOCKED', lock_time=evening + timedelta(hours=2), login='other@ad',
          access=evening + timedelta(hours=2))
    # Reported before the copy, so its lock was overwritten: left out.
    study(engine, 103, locked='UNLOCKED', lock_time=evening + timedelta(hours=3), login=author,
          composed=evening + timedelta(hours=2))
    run_notes(engine, monkeypatch, [note_row(i, 100 + i, created=evening) for i in range(4)])
    etl_sticky_notes.record_access_history(engine, NIGHT)

    with engine.connect() as conn:
        rows = [tuple(r) for r in conn.execute(text(EVIDENCE_SQL))]

    assert [(r[0], r[1], r[2], r[3]) for r in rows] == [
        ('1 no lock', 1, 0, 1),
        ('2 lock before note', 1, 1, 1),
        ('3 lock after note', 1, 0, 0),
    ]
    assert float(rows[1][4]) == 30.0
