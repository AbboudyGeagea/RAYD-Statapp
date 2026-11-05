"""
tests/test_ris_visits_etl.py
────────────────────────────────────────────────────────────────
The nightly RIS visits job stopped loading new visits on 2026-07-31. It remembered the
highest visit number loaded so far, compared as text, and RIS holds a few test visits
numbered from 't'. Those sort above the real numbers, which start with 0 or M, so no new
visit ever qualified. Every run still reported SUCCESS, with 0 rows. By 2026-11-05 only
12% of the last 90 days' RIS orders found their visit in std_visits.

Oracle is replaced by a stand-in that applies the job's incremental filter the way
Oracle would: its default text comparison is binary, like Python's.
"""
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_ris_visits

GO_LIVE = date(2018, 1, 1)


def ris_visit(key, number):
    """One row in the order of the job's SELECT: 24 columns, CREATED_ON_DATE is the 20th."""
    return ((key, None, None, None, number) + (None,) * 14
            + (datetime(2026, 8, 1),) + (None, None, 'N', None))


class FakeRis:
    """The RIS connection and its cursor."""

    def __init__(self, rows):
        self.rows, self.sql, self.params, self.pending = rows, None, None, []

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params):
        self.sql, self.params = sql, params
        self.pending = [r for r in self.rows if self._passes(r)]

    def _passes(self, row):
        key, number = row[0], row[4]
        if 'VISIT_KEY > :watermark' in self.sql:
            return key > self.params['watermark']
        if 'VISIT_NUMBER > :watermark' in self.sql:
            return number is not None and number > self.params['watermark']
        return True

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


@pytest.fixture
def engine(app):
    from db import db
    with app.app_context():
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM std_visits'))
        yield db.engine
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM std_visits'))


def already_loaded(engine, *visits):
    with engine.begin() as conn:
        for key, number in visits:
            conn.execute(text('INSERT INTO std_visits (visit_key, visit_number) VALUES (:k, :n)'),
                         {'k': key, 'n': number})


def run_job(engine, monkeypatch, ris_rows):
    ris = FakeRis(ris_rows)
    monkeypatch.setattr(etl_ris_visits, 'OracleConnector',
                        SimpleNamespace(get_connection=lambda source: ris))
    return ris, etl_ris_visits.run_ris_visits_etl(engine, 'RIS', GO_LIVE)


def loaded_keys(engine):
    with engine.connect() as conn:
        return {r[0] for r in conn.execute(text('SELECT visit_key FROM std_visits'))}


def test_new_visits_load_when_a_test_visit_number_sorts_highest(engine, monkeypatch):
    already_loaded(engine, (100, '0000000100'), (101, 't000000101'))

    ris, total = run_job(engine, monkeypatch, [
        ris_visit(100, '0000000100'),
        ris_visit(101, 't000000101'),
        ris_visit(102, '0000000102'),
        ris_visit(103, 'M0000000000103'),
        ris_visit(104, None),
    ])

    assert total == 3
    assert loaded_keys(engine) == {100, 101, 102, 103, 104}
    assert ris.params['watermark'] == 101


def test_visits_already_loaded_are_not_pulled_again(engine, monkeypatch):
    already_loaded(engine, (100, '0000000100'))

    _ris, total = run_job(engine, monkeypatch, [ris_visit(99, '0000000099'),
                                                ris_visit(100, '0000000100')])

    assert total == 0
    assert loaded_keys(engine) == {100}


def test_first_load_takes_everything_since_go_live(engine, monkeypatch):
    ris, total = run_job(engine, monkeypatch, [ris_visit(1, '0000000001'),
                                               ris_visit(2, 't000000002')])

    assert total == 2
    assert 'watermark' not in ris.params
    assert ris.params['cutoff'] == '2018-01-01'
