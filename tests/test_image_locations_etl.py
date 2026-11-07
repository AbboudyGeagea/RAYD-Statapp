"""
tests/test_image_locations_etl.py
────────────────────────────────────────────────────────────────
The nightly ETL died on 2026-11-06 and 2026-11-07. Phases 2, 3 and 4 checked each row's
parent against the whole parent table loaded into memory, and the raw images table grows by
about 1.2M rows a night. That load held the web worker, which also runs the ETL, past
gunicorn's --timeout 300, gunicorn killed it mid-phase, and nothing after Phase 4 ran.

They now ask Postgres only for each batch's own parent IDs (ETL_JOBS/etl_fk_guard.py), which
gives the same answer at any table size. The runner also stops leaving dead runs RUNNING for
good, and closes only its own row instead of every RUNNING one.

Oracle is replaced by stand-ins returning rows in the order of each phase's SELECT.
"""
import logging
import os
import re
import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text

from conftest import ROOT

sys.path.insert(0, os.path.join(ROOT, 'ETL_JOBS'))
import etl_didb_raw_images  # noqa: E402  (the ETL modules import each other by bare name)
import etl_fk_guard  # noqa: E402
import etl_image_locations  # noqa: E402
import etl_series  # noqa: E402

WHOLE_TABLE = re.compile(r'^\s*SELECT\s+\w+\s+FROM\s+etl_didb_(studies|serieses|raw_images)\s*$', re.I)


class FakeOracle:
    """One connection and cursor. The column dictionary query (Phase 2) gets column names;
    anything else gets the rows."""

    def __init__(self, rows, columns=()):
        self.rows, self.columns, self.pending, self.arraysize = rows, columns, [], 1

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        self.pending = [(c,) for c in self.columns] if 'ALL_TAB_COLUMNS' in sql else list(self.rows)

    def fetchall(self):
        out, self.pending = self.pending, []
        return out

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


@pytest.fixture
def engine(app):
    from db import db
    with app.app_context():
        yield db.engine
        with db.engine.begin() as conn:
            conn.execute(text('DELETE FROM etl_image_locations'))
            conn.execute(text('DELETE FROM etl_didb_raw_images'))
            conn.execute(text("DELETE FROM etl_job_log WHERE job_name IN "
                              "('4TB_SYNC', 'IMAGE_LOCATIONS_ETL', 'RAW_IMAGES_ETL', 'SERIES_ETL', 'CD_SURF_TEST')"))


@pytest.fixture
def statements(engine):
    """Every SQL statement sent to Postgres while the test runs."""
    seen = []

    def record(conn, cursor, statement, parameters, context, executemany):
        seen.append(statement)
    event.listen(engine, 'before_cursor_execute', record)
    yield seen
    event.remove(engine, 'before_cursor_execute', record)


def study(engine, uid):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid) VALUES (:s, :s)"), {'s': uid})


def series(engine, series_uid, study_uid):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid, patient_db_uid) "
                          "VALUES (:se, :st, :st)"), {'se': series_uid, 'st': study_uid})


def raw_image(engine, image, series_uid, study_uid):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_didb_raw_images (raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid,
                                             study_instance_uid, series_instance_uid)
            VALUES (:i, :st, :st, :se, 'st', 'se')"""), {'i': image, 'st': study_uid, 'se': series_uid})


def use(module, monkeypatch, oracle):
    monkeypatch.setattr(module, 'OracleConnector', SimpleNamespace(get_connection=lambda source: oracle))


def rows_in(engine, sql):
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(sql))]


def assert_no_whole_table_load(statements):
    loads = [s for s in statements if WHOLE_TABLE.match(s)]
    assert not loads, loads


# ── the guard ─────────────────────────────────────────────────────────────────

def test_the_guard_returns_only_ids_present(engine):
    study(engine, 1)
    study(engine, 2)

    found = etl_fk_guard.existing_ids(engine, 'etl_didb_studies', 'study_db_uid',
                                      [1, Decimal(2), '2', 3, None, 'R3'])

    assert found == {1, 2}
    assert etl_fk_guard.existing_ids(engine, 'etl_didb_studies', 'study_db_uid', []) == set()
    assert etl_fk_guard.existing_ids(engine, 'etl_didb_studies', 'study_db_uid', ['R3']) == set()


# ── Phase 4: image locations ──────────────────────────────────────────────────

def test_image_locations_land_orphans_are_dropped_and_nothing_loads_a_whole_table(engine, monkeypatch, statements):
    from db import chunked_upsert
    study(engine, 1)
    series(engine, 10, 1)
    raw_image(engine, 101, 10, 1)
    raw_image(engine, 102, 10, 1)
    use(etl_image_locations, monkeypatch, FakeOracle([(101, 'fs1', 10), (Decimal(102), 'fs2', 20), (999, 'fs1', 5)]))

    etl_image_locations.run_images_etl(engine, 'PROD_ORACLE', 'etl_image_locations', chunked_upsert, [1])

    assert rows_in(engine, 'SELECT raw_image_db_uid, file_system, image_size_kb FROM etl_image_locations '
                           'ORDER BY 1') == [(101, 'fs1', 10), (102, 'fs2', 20)]
    assert_no_whole_table_load(statements)


# ── Phase 3: raw images ───────────────────────────────────────────────────────

def raw_row(image, study_uid, series_uid):
    return (image, study_uid, study_uid, series_uid, 'st', 'se', 1, None)


def test_raw_images_land_orphans_are_dropped_and_nothing_loads_a_whole_table(engine, monkeypatch, statements):
    from db import chunked_upsert
    study(engine, 1)
    series(engine, 10, 1)
    use(etl_didb_raw_images, monkeypatch, FakeOracle([raw_row(101, 1, 10), raw_row(102, 1, 99)]))

    etl_didb_raw_images.run_raw_images_etl(engine, 'PROD_ORACLE', 'etl_didb_raw_images', chunked_upsert, [1])

    assert rows_in(engine, 'SELECT raw_image_db_uid FROM etl_didb_raw_images ORDER BY 1') == [(101,)]
    assert_no_whole_table_load(statements)


def test_raw_images_with_no_studies_closes_its_log_row(engine, monkeypatch):
    from db import chunked_upsert
    use(etl_didb_raw_images, monkeypatch, FakeOracle([]))

    etl_didb_raw_images.run_raw_images_etl(engine, 'PROD_ORACLE', 'etl_didb_raw_images', chunked_upsert, [])

    assert rows_in(engine, "SELECT status FROM etl_job_log WHERE job_name = 'RAW_IMAGES_ETL' "
                           "ORDER BY id DESC LIMIT 1") == [('SUCCESS',)]


# ── Phase 2: series ───────────────────────────────────────────────────────────

def series_row(series_uid, study_uid):
    return (series_uid, study_uid, study_uid, 'st', 'se', 1, 'CT', 5,
            None, None, None, None, None, None, None, None, None)


def test_series_land_orphans_and_corrupt_keys_are_dropped_and_nothing_loads_a_whole_table(
        engine, monkeypatch, statements):
    from db import chunked_upsert
    study(engine, 1)
    oracle = FakeOracle([series_row(10, 1), series_row(11, 2), series_row(12, 'R3')],
                        columns=etl_series._EXPECTED_SERIES_COLS)
    use(etl_series, monkeypatch, oracle)

    etl_series.run_series_etl(engine, 'PROD_ORACLE', 'etl_didb_serieses', chunked_upsert, [1, 2])

    assert rows_in(engine, 'SELECT series_db_uid FROM etl_didb_serieses ORDER BY 1') == [(10,)]
    assert_no_whole_table_load(statements)


# ── the runner's own bookkeeping ──────────────────────────────────────────────

def test_a_dead_run_is_closed_and_a_run_finishes_only_its_own_row(engine, monkeypatch):
    import etl_runner
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_job_log (job_name, status, start_time) VALUES
                ('4TB_SYNC', 'RUNNING', now() - interval '2 days'),
                ('IMAGE_LOCATIONS_ETL', 'RUNNING', now() - interval '2 days'),
                ('CD_SURF_TEST', 'RUNNING', now() - interval '1 hour')
        """))
    monkeypatch.setenv('RAYD_ETL_PHASES', 'none')       # run no phase: only the bookkeeping

    etl_runner._perform_migration(engine)

    got = rows_in(engine, """
        SELECT job_name, status, error_message FROM etl_job_log
         WHERE job_name IN ('4TB_SYNC', 'IMAGE_LOCATIONS_ETL', 'CD_SURF_TEST') ORDER BY id""")
    dead_sync, dead_phase, other_job, this_run = got
    assert dead_sync[1] == dead_phase[1] == 'FAILED'
    assert 'Interrupted' in dead_sync[2] and 'Interrupted' in dead_phase[2]
    assert other_job[1] == 'RUNNING'            # a job outside the ETL, still within 12 hours
    assert this_run[:2] == ('4TB_SYNC', 'SUCCESS')


def test_a_finished_run_leaves_another_running_sync_alone(engine, monkeypatch):
    import etl_runner
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO etl_job_log (job_name, status, start_time) "
                          "VALUES ('4TB_SYNC', 'RUNNING', now() - interval '1 hour')"))
    monkeypatch.setenv('RAYD_ETL_PHASES', 'none')

    etl_runner._perform_migration(engine)

    assert [r[0] for r in rows_in(engine, "SELECT status FROM etl_job_log WHERE job_name = '4TB_SYNC' "
                                          "ORDER BY id")] == ['RUNNING', 'SUCCESS']


# ── the nightly sync runs in its own process ──────────────────────────────────

def test_the_sync_starts_as_a_manual_run_in_its_own_process(monkeypatch):
    import subprocess
    import etl_runner
    calls = []
    monkeypatch.setattr(subprocess, 'run', lambda args, cwd: calls.append((args, cwd)) or SimpleNamespace(returncode=0))

    assert etl_runner.run_sync_in_child('/app') == 0
    (args, cwd), = calls
    assert args[0] == sys.executable
    assert args[1:] == [os.path.join('/app', 'app.py'), '-m']
    assert cwd == '/app'


def test_a_child_that_cannot_start_says_so_instead_of_skipping_the_night(monkeypatch):
    import subprocess
    import etl_runner

    def refuse(args, cwd):
        raise OSError('no such file')
    monkeypatch.setattr(subprocess, 'run', refuse)

    assert etl_runner.run_sync_in_child('/app') is None


def test_the_scheduler_uses_the_child_and_falls_back_to_running_in_place():
    with open(os.path.join(ROOT, 'app.py'), encoding='utf-8') as f:
        source = f.read()
    job = source[source.index('def scheduled_etl():'):source.index("id='daily_etl_sync'")]
    assert 'run_sync_in_child(app.root_path)' in job
    assert 'db.session.remove()' in job
    assert job.index('if code is None:') < job.index('execute_sync(app)')


def test_a_real_manual_run_starts_no_scheduler_and_no_listener():
    """What the child runs: app.py -m must stay a manual run."""
    with open(os.path.join(ROOT, 'app.py'), encoding='utf-8') as f:
        source = f.read()
    assert "manual_mode = len(sys.argv) > 1 and sys.argv[1] == '-m'" in source
    guarded = source[source.index('    if not manual_mode:'):]
    first_lines = guarded.splitlines()[1:3]
    assert first_lines[0].strip() == 'scheduler.start()'
    assert first_lines[1].strip().startswith('start_mllp_listener(')
