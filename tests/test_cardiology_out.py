"""
tests/test_cardiology_out.py — cardiology left out of RAYD (operator, 2026-11-17), through the
original storing AE that SJH's middleware keeps (migration 0159, ETL_JOBS/etl_didb_studies.py).

The nightly study copy reads ORIGINAL_STORING_AE and skips the cardiology devices (EPIQCVX, the
cardiac echo; AZURION, SJH's cath lab) whether they are the storing or the original AE, without
dropping the many studies that have no original AE. The gateway studies already loaded get the
column once, read from PACS by study ID ranges, and their cardiology studies are then deleted.
"""
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_didb_studies as studies


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        sql("DELETE FROM settings WHERE key LIKE 'original_ae_%'")
        yield
        db.session.rollback()
        sql("DELETE FROM settings WHERE key LIKE 'original_ae_%'")


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def scalar(statement, **params):
    from db import db
    out = db.session.execute(text(statement), params).scalar()
    db.session.commit()
    return out


def study(uid, storing, original=None):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, original_storing_ae,
                                         study_date, study_modality)
           VALUES (:u, :u, :s, :o, '2026-11-10', 'XA')""", u=uid, s=storing, o=original)


class FakeOracle:
    """Answers each query with fetchall_for(sql, params); records what was asked."""
    def __init__(self, fetchall_for=lambda q, p: []):
        self.fetchall_for = fetchall_for
        self.queries = []

    def cursor(self):
        return self

    def execute(self, query, params=None):
        self.queries.append((query, dict(params or {})))
        self._rows = list(self.fetchall_for(query, params or {}))

    def fetchall(self):
        return self._rows

    def fetchmany(self, size):
        rows, self._rows = self._rows, []
        return rows

    def close(self):
        pass


def use(monkeypatch, pacs):
    monkeypatch.setattr(studies, 'OracleConnector', SimpleNamespace(get_connection=lambda source: pacs))


def test_the_nightly_copy_reads_the_original_ae_and_skips_cardiology_with_a_null_guard(monkeypatch):
    from db import db
    pacs = FakeOracle()
    use(monkeypatch, pacs)
    stored = []

    studies.run_studies_etl(db.engine, 'PACS', 'etl_didb_studies',
                            lambda engine, table, cols, rows, key: stored.append((cols, rows)), '2021-01-01')

    query = pacs.queries[0][0]
    assert 'UPPER(TRIM(s.ORIGINAL_STORING_AE))' in query
    assert "UPPER(TRIM(s.STORING_AE)) NOT IN ('EPIQCVX', 'AZURION')" in query
    assert ("(s.ORIGINAL_STORING_AE IS NULL OR UPPER(TRIM(s.ORIGINAL_STORING_AE)) NOT IN "
            "('EPIQCVX', 'AZURION'))") in query


def test_gateway_studies_already_loaded_get_their_original_ae_once(monkeypatch):
    from db import db
    study(1001, 'SJHCSAPWFMFIR')
    study(1002, 'SJHCSAPWFMFIR')
    study(1003, 'MR750W')                                      # not a gateway: never asked
    pacs = FakeOracle(lambda q, p: [(1001, 'CM_CT_CMW_V1'), (1002, 'EPIQCVX')] if p.get('lo', 0) <= 1001 else [])
    use(monkeypatch, pacs)

    assert studies.fill_original_ae(db.engine, 'PACS', chunk=10) == 2

    got = dict(db.session.execute(text(
        "SELECT study_db_uid, original_storing_ae FROM etl_didb_studies ORDER BY 1")).all())
    db.session.commit()
    assert got == {1001: 'CM_CT_CMW_V1', 1002: 'EPIQCVX', 1003: None}
    query, params = pacs.queries[0]
    assert 'STUDY_DB_UID BETWEEN :lo AND :hi' in query and 'PATIENT' not in query.upper()
    assert 'SJHCSAPWFMFIR' in params.values() and 'MR750W' not in params.values()
    assert scalar("SELECT value FROM settings WHERE key = 'original_ae_filled'") == 'true'

    asked = len(pacs.queries)
    assert studies.fill_original_ae(db.engine, 'PACS') == 0 and len(pacs.queries) == asked


def test_a_long_history_is_filled_over_several_nights(monkeypatch):
    from db import db
    study(10, 'SJHCSAPWFMFIR')
    study(95, 'SJHCSAPWFMFIR')
    pacs = FakeOracle(lambda q, p: [(u, 'KIZUNA') for u in (10, 95) if p['lo'] <= u <= p['hi']])
    use(monkeypatch, pacs)

    studies.fill_original_ae(db.engine, 'PACS', chunk=20, max_chunks=2)       # studies 10-49
    assert scalar("SELECT original_storing_ae FROM etl_didb_studies WHERE study_db_uid = 95") is None
    assert scalar("SELECT value FROM settings WHERE key = 'original_ae_filled'") is None

    studies.fill_original_ae(db.engine, 'PACS', chunk=20, max_chunks=5)       # carries on from 50
    assert scalar("SELECT original_storing_ae FROM etl_didb_studies WHERE study_db_uid = 95") == 'KIZUNA'
    assert scalar("SELECT value FROM settings WHERE key = 'original_ae_filled'") == 'true'


def test_cardiology_studies_already_loaded_are_deleted_and_nothing_else():
    from db import db
    study(1, 'SJHCSAPWFMFIR', 'EPIQCVX')
    study(2, 'SJHCSAPWFMFIR', 'AZURION')
    study(3, 'AZURION')
    study(4, 'SJHCSAPWFMFIR', 'CM_CT_CMW_V1')
    study(5, 'AZURION7FLEXARM')                               # Rizk's hybrid OR stays
    study(6, 'MR750W')
    sql("INSERT INTO std_pps (pps_key, study_db_uid) VALUES (77, 2)")

    assert studies.purge_cardiology(db.engine) == 3

    left = db.session.execute(text("SELECT study_db_uid FROM etl_didb_studies ORDER BY 1")).scalars().all()
    db.session.commit()
    assert left == [4, 5, 6]
    assert scalar("SELECT study_db_uid FROM std_pps WHERE pps_key = 77") is None


# ── the cath lab's RIS side (item 13b, migration 0166) ────────────────────────

def _pps_row(key, ae):
    """A RIS PPS row as etl_ris_pps.py selects it (36 columns): key, start time, device AE."""
    from datetime import datetime
    row = [None] * 36
    row[0], row[9], row[13] = key, datetime(2026, 11, 10, 9), ae
    return tuple(row)


def test_the_ris_steps_copy_leaves_the_cardiology_devices_out(monkeypatch):
    from db import db
    from ETL_JOBS import etl_ris_pps
    pacs = FakeOracle(lambda q, p: [_pps_row(9101, 'AZURION'), _pps_row(9102, 'azurion '),
                                    _pps_row(9103, 'EPIQCVX'), _pps_row(9104, 'AZURION7FLEXARM'),
                                    _pps_row(9105, 'CM_CT_CMW_V1')])
    monkeypatch.setattr(etl_ris_pps, 'OracleConnector', SimpleNamespace(get_connection=lambda source: pacs))
    sql("DELETE FROM std_pps WHERE pps_key BETWEEN 9101 AND 9105")

    etl_ris_pps.run_ris_pps_etl(db.engine, 'RIS', '2021-01-01')

    kept = db.session.execute(text("SELECT pps_key FROM std_pps WHERE pps_key BETWEEN 9101 AND 9105 ORDER BY 1")).scalars().all()
    db.session.commit()
    assert kept == [9104, 9105]                      # Rizk's hybrid OR stays


def test_the_ris_device_copy_does_not_bring_the_cath_lab_back(monkeypatch):
    from db import db
    from ETL_JOBS import etl_ris_modality

    class Ris:
        def cursor(self):
            return self

        def execute(self, query, params=None):
            pass

        def fetchall(self):
            return [(1, 'AZURION', 'CATH', 'Cath lab', 'Philips Azurion 7', '5320', 'Y', 'XA'),
                    (2, 'GW_RIS_KEEP', 'X1', 'Kept', 'Room kept', '5320', 'Y', 'DX')]

        def close(self):
            pass

    monkeypatch.setattr(etl_ris_modality, 'OracleConnector', SimpleNamespace(get_connection=lambda source: Ris()))
    try:
        etl_ris_modality.run_ris_modality_etl(db.engine, 'RIS')
        got = db.session.execute(text("SELECT aetitle FROM aetitle_modality_map "
                                      "WHERE aetitle IN ('AZURION', 'GW_RIS_KEEP') ORDER BY 1")).scalars().all()
        db.session.commit()
        assert got == ['GW_RIS_KEEP']
    finally:
        sql("DELETE FROM aetitle_modality_map WHERE aetitle = 'GW_RIS_KEEP'")


def test_no_cardiology_device_is_left_in_the_mapping():
    assert scalar("SELECT COUNT(*) FROM aetitle_modality_map WHERE UPPER(TRIM(aetitle)) IN ('AZURION', 'EPIQCVX')") == 0
