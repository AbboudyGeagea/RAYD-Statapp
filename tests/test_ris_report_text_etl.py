"""
tests/test_ris_report_text_etl.py
────────────────────────────────────────────────────────────────
Phase 9 copies each report's plain text from the RIS into hl7_oru_reports. At LAUMC the live
HL7 report stream is encrypted, so this is the only readable report text that Report
Intelligence, NLP and the critical findings log get.

On 2026-09-19 commit 91b636eb replaced its file with the Phase 19 loader, and Phase 9 went on
calling the new function with its own arguments: a TypeError every night, every sync
PARTIAL, and no report text after 2026-09-21. These run Phase 9 through the real runner, which
is where it broke. Oracle is replaced by a stand-in returning RIS REPORT rows.
"""
import os
import sys
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from conftest import ROOT

sys.path.insert(0, os.path.join(ROOT, 'ETL_JOBS'))
import etl_ris_oru_reports  # noqa: E402
import etl_runner  # noqa: E402

APPROVED = datetime(2026, 11, 6, 14, 0)


class FakeRis:
    def __init__(self, rows):
        self.rows, self.pending, self.sql, self.params = rows, [], None, None
        self.fetch_lobs = True

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        self.sql, self.params, self.pending = sql, params, list(self.rows)

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


@pytest.fixture
def engine(app):
    from db import db
    with app.app_context():
        with db.engine.begin() as conn:
            conn.execute(text("INSERT INTO db_params (name, db_role, db_type) VALUES ('ris', 'source', 'oracle') "
                              "ON CONFLICT (name) DO NOTHING"))
        yield db.engine
        with db.engine.begin() as conn:
            conn.execute(text("DELETE FROM db_params WHERE name = 'ris'"))
            conn.execute(text("DELETE FROM etl_job_log WHERE job_name IN ('4TB_SYNC', 'RIS_REPORT_TEXT_ETL')"))


def run_phase_9(engine, monkeypatch, rows):
    ris = FakeRis(rows)
    monkeypatch.setattr(etl_ris_oru_reports, 'OracleConnector', SimpleNamespace(get_connection=lambda source: ris))
    monkeypatch.setenv('RAYD_ETL_PHASES', '9')
    etl_runner._perform_migration(engine)
    return ris


def rows_in(engine, sql):
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(sql))]


def test_phase_9_runs_through_the_runner_and_the_sync_is_no_longer_partial(engine, monkeypatch):
    run_phase_9(engine, monkeypatch, [('100500001', 'FINDINGS: clear.\nIMPRESSION: Normal study.', APPROVED, 61)])

    assert rows_in(engine, "SELECT status, error_message FROM etl_job_log WHERE job_name = '4TB_SYNC' "
                           "ORDER BY id DESC LIMIT 1") == [('SUCCESS', None)]
    assert rows_in(engine, "SELECT status FROM etl_job_log WHERE job_name = 'RIS_REPORT_TEXT_ETL'") == [('SUCCESS',)]


def test_report_text_lands_with_its_impression(engine, monkeypatch):
    run_phase_9(engine, monkeypatch, [('100500001', 'FINDINGS: clear.\nIMPRESSION: Normal study.', APPROVED, 61)])

    assert rows_in(engine, "SELECT accession_number, impression_text, result_datetime, physician_id, report_source "
                           "FROM hl7_oru_reports") == [('100500001', 'Normal study.', APPROVED, '61', 'ris')]


def test_the_ris_text_replaces_what_the_encrypted_live_feed_left(engine, monkeypatch):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO hl7_oru_reports (accession_number, report_text, report_source) "
                          "VALUES ('100500001', 'unreadable', 'hl7')"))

    run_phase_9(engine, monkeypatch, [('100500001', 'IMPRESSION: Fracture.', APPROVED, 61)])

    assert rows_in(engine, "SELECT COUNT(*), MIN(report_text), MIN(report_source) FROM hl7_oru_reports") == \
        [(1, 'IMPRESSION: Fracture.', 'ris')]


def test_later_nights_catch_up_from_the_last_report_loaded(engine, monkeypatch):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO hl7_oru_reports (accession_number, report_text, report_source, result_datetime) "
                          "VALUES ('100500009', 'old', 'ris', '2026-09-21 10:00')"))

    ris = run_phase_9(engine, monkeypatch, [])

    assert ris.params['watermark'] == datetime(2026, 9, 21, 10, 0)
    assert 'lb' in ris.params


def test_phase_9_and_phase_19_have_their_own_loaders_and_job_names():
    import inspect
    import etl_ris_reports
    assert etl_ris_oru_reports.run_ris_oru_reports_etl is not etl_ris_reports.run_ris_reports_etl
    inspect.signature(etl_ris_oru_reports.run_ris_oru_reports_etl).bind('engine', 'ris', 'go_live')
    source = inspect.getsource(etl_ris_oru_reports.run_ris_oru_reports_etl)
    assert 'RIS_REPORT_TEXT_ETL' in source and '"RIS_REPORTS_ETL"' not in source


def test_reports_get_modality_site_and_patient_from_their_study(engine, monkeypatch):
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
                             VALUES ('CT_RIS', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))"""))
        conn.execute(text("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae,
                                                           study_modality, site_id)
                             VALUES (1, 1, '100500001', 'CT_RIS', 'SR', (SELECT id FROM sites WHERE is_default LIMIT 1))"""))
        conn.execute(text("INSERT INTO etl_orders (order_dbid, accession_number, patient_dbid) "
                          "VALUES (501, '100500001', '777')"))
        conn.execute(text("INSERT INTO std_patients_ris (patient_person_key) VALUES (777)"))
        conn.execute(text("INSERT INTO std_patient_ids (patient_id_list_key, patient_person_key, patient_id, is_primary) "
                          "VALUES (1, 777, 'MRN777', 'Y')"))

    run_phase_9(engine, monkeypatch, [('100500001', 'IMPRESSION: Normal.', APPROVED, 61),
                                      ('100599999', 'IMPRESSION: No study yet.', APPROVED, 61)])

    got = dict((r[0], r[1:]) for r in rows_in(engine, """
        SELECT accession_number, modality, patient_id, site_id IS NOT NULL FROM hl7_oru_reports"""))
    assert got['100500001'] == ('CT', 'MRN777', True)      # the device's modality, not the SR object's
    assert got['100599999'] == (None, None, False)          # no study loaded yet: left for a later night
    assert rows_in(engine, "SELECT status FROM etl_job_log WHERE job_name = '4TB_SYNC' "
                           "ORDER BY id DESC LIMIT 1") == [('SUCCESS',)]
