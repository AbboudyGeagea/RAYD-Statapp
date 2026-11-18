"""
tests/test_gateway_device.py — a gateway study's device is the scanner behind it (item 13, device
rule; migration 0164, ETL_JOBS/etl_didb_studies.py GATEWAY_AES).

SJH's studies reach PACS through its middleware SJHCSAPWFMFIR, which PACS names as the storing
AE; the scanner is in ORIGINAL_STORING_AE. Every report joins the device mapping on storing_ae,
so RAYD stores the scanner there and the gateway in gateway_ae, but only when the mapping knows
the scanner as a device: an import agent (XVIMPORT, mapped PACS), an unmapped or empty AE keeps
the gateway, as do LAUMCWFM1AR's studies (it names itself as the original AE). The study's site
then follows the scanner. Report 25 on SJH's database: test_site_databases.py.
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


def rows(statement, **params):
    from db import db
    out = [tuple(r) for r in db.session.execute(text(statement), params).all()]
    db.session.commit()
    return out


def study(uid, storing, original=None, label='0'):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, original_storing_ae,
                                         study_date, study_modality, pacs_site_id_raw)
           VALUES (:u, :u, :s, :o, '2026-11-10', 'MR', :l)""", u=uid, s=storing, o=original, l=label)


def stored():
    return rows("SELECT study_db_uid, storing_ae, gateway_ae FROM etl_didb_studies ORDER BY 1")


SCANNERS = {'GW_MR': 'MR', 'GW_US': 'US', 'GW_CT': 'CT', 'GW_IMPORT': 'PACS'}   # GW_IMPORT: an import agent


@pytest.fixture(autouse=True)
def scanners(ctx):
    for ae, modality in SCANNERS.items():
        sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
               SELECT :a, :m, id FROM sites WHERE code = 'SJH'""", a=ae, m=modality)
    yield
    sql("DELETE FROM aetitle_modality_map WHERE aetitle = ANY(:a)", a=list(SCANNERS))


class FakeOracle:
    def __init__(self, answer):
        self.answer = answer
        self.queries = []

    def cursor(self):
        return self

    def execute(self, query, params=None):
        self.queries.append((query, dict(params or {})))
        self._rows = list(self.answer(query, params or {}))

    def fetchall(self):
        return self._rows

    def fetchmany(self, size):
        out, self._rows = self._rows, []
        return out

    def close(self):
        pass


def use(monkeypatch, pacs):
    monkeypatch.setattr(studies, 'OracleConnector', SimpleNamespace(get_connection=lambda source: pacs))


@pytest.mark.parametrize('storing, original, expected', [
    ('SJHCSAPWFMFIR', 'KIZUNA', ('KIZUNA', 'SJHCSAPWFMFIR')),
    ('LAUMCWFM2FIR', 'CM_CT_CMW_V1', ('CM_CT_CMW_V1', 'LAUMCWFM2FIR')),
    ('SJHCSAPWFMFIR', None, ('SJHCSAPWFMFIR', None)),        # no original AE: keeps the gateway
    ('SJHCSAPWFMFIR', '', ('SJHCSAPWFMFIR', None)),
    ('SJHCSAPWFMFIR', 'XVIMPORT', ('SJHCSAPWFMFIR', None)),  # not a mapped device
    ('SJHCSAPWFMFIR', 'VIMS_AE', ('SJHCSAPWFMFIR', None)),   # not mapped at all
    ('LAUMCWFM1AR', 'LAUMCWFM1AR', ('LAUMCWFM1AR', None)),     # names itself: not a gateway here
    ('MR750W', None, ('MR750W', None)),
    ('MR750W', 'KIZUNA', ('MR750W', None)),                  # only gateways are swapped
])
def test_device_of(storing, original, expected):
    assert studies.device_of(storing, original, {'KIZUNA', 'CM_CT_CMW_V1', 'MR750W'}) == expected


def test_the_mapped_devices_leave_out_gateways_and_import_agents():
    from db import db
    with db.engine.connect() as conn:
        devices = studies.mapped_devices(conn)
    assert {'GW_MR', 'GW_US', 'GW_CT'} <= devices
    assert not devices & {'GW_IMPORT', 'SJHCSAPWFMFIR', 'LAUMCWFM2FIR'}


# ── the nightly copy ──────────────────────────────────────────────────────────

def _select_columns(sql_text):
    """How many expressions the SELECT list has, ignoring commas inside brackets."""
    body = sql_text[sql_text.upper().index('SELECT') + 6:sql_text.upper().index('FROM MEDISTORE')]
    depth, count = 0, 1
    for ch in body:
        depth += ch == '('
        depth -= ch == ')'
        count += ch == ',' and depth == 0
    return count


def pacs_row(query, uid, storing, original):
    """A PACS row as long as the SELECT list: storing AE 6th, original storing AE last."""
    row = [None] * _select_columns(query)
    row[0], row[1], row[5], row[6] = uid, uid, storing, '2026-11-10'
    row[30], row[39], row[40] = 'false', 'false', 'false'
    row[-1] = original
    return tuple(row)


def test_the_nightly_copy_stores_the_scanner_and_keeps_the_gateway(monkeypatch):
    from db import db, chunked_upsert
    given = [(1, 'SJHCSAPWFMFIR', 'GW_MR'), (2, 'SJHCSAPWFMFIR', None), (3, 'LAUMCWFM1AR', 'LAUMCWFM1AR'),
             (4, 'MR750W', None), (5, 'LAUMCWFM2FIR', 'GW_US'), (6, 'SJHCSAPWFMFIR', 'GW_IMPORT'),
             (7, 'SJHCSAPWFMFIR', 'VIMS_AE')]
    pacs = FakeOracle(lambda q, p: [pacs_row(q, *g) for g in given])
    use(monkeypatch, pacs)

    for _night in range(2):                       # the lookback reads the same studies again
        total, _uids = studies.run_studies_etl(db.engine, 'PACS', 'etl_didb_studies', chunked_upsert, '2021-01-01')
        assert total == 7
        assert stored() == [(1, 'GW_MR', 'SJHCSAPWFMFIR'), (2, 'SJHCSAPWFMFIR', None),
                            (3, 'LAUMCWFM1AR', None), (4, 'MR750W', None), (5, 'GW_US', 'LAUMCWFM2FIR'),
                            (6, 'SJHCSAPWFMFIR', None),      # an import agent keeps the gateway
                            (7, 'SJHCSAPWFMFIR', None)]      # so does an unmapped AE
    assert 'GATEWAY_AE' not in pacs.queries[0][0].upper()   # RAYD's own column, never asked of PACS


def test_the_fill_gives_the_gateway_studies_it_fills_their_scanner(monkeypatch):
    from db import db
    study(1001, 'SJHCSAPWFMFIR')
    study(1002, 'SJHCSAPWFMFIR')                  # PACS has no original AE for it
    study(1003, 'MR750W')
    use(monkeypatch, FakeOracle(lambda q, p: [(1001, 'GW_CT')] if p['lo'] <= 1001 <= p['hi'] else []))

    assert studies.fill_original_ae(db.engine, 'PACS', chunk=10) == 1

    assert stored() == [(1001, 'GW_CT', 'SJHCSAPWFMFIR'), (1002, 'SJHCSAPWFMFIR', None), (1003, 'MR750W', None)]


def test_swapping_studies_already_loaded_is_idempotent_and_follows_the_mapping():
    from db import db
    study(1, 'SJHCSAPWFMFIR', 'GW_MR')
    study(2, ' sjhcsapwfmfir', 'gw_mr ')          # as PACS might spell them
    study(3, 'SJHCSAPWFMFIR')
    study(4, 'SJHCSAPWFMFIR', 'GW_LATER')         # a scanner not mapped yet
    with db.engine.begin() as conn:
        assert studies.apply_gateway_devices(conn) == 2
    with db.engine.begin() as conn:
        assert studies.apply_gateway_devices(conn) == 0
    assert stored() == [(1, 'GW_MR', 'SJHCSAPWFMFIR'), (2, 'GW_MR', 'SJHCSAPWFMFIR'),
                        (3, 'SJHCSAPWFMFIR', None), (4, 'SJHCSAPWFMFIR', None)]
    # mapped later: its studies move the next night
    sql("INSERT INTO aetitle_modality_map (aetitle, modality, site_id) SELECT 'GW_LATER', 'DX', id FROM sites WHERE code = 'SJH'")
    try:
        with db.engine.begin() as conn:
            assert studies.apply_gateway_devices(conn) == 1
        assert stored()[3] == (4, 'GW_LATER', 'SJHCSAPWFMFIR')
    finally:
        sql("DELETE FROM aetitle_modality_map WHERE aetitle = 'GW_LATER'")


# ── what follows the device ───────────────────────────────────────────────────

def test_the_site_follows_the_scanner():
    """A mapped scanner gives its site, even against the PACS label; a study left on the gateway
    (unmapped AE) is left to the label, as before (migration 0145's rule)."""
    from db import db
    sjh_label = rows("SELECT pacs_site_id FROM sites WHERE code = 'SJH'")[0][0]
    rh_label = rows("SELECT pacs_site_id FROM sites WHERE is_default")[0][0]
    study(1, 'SJHCSAPWFMFIR', 'GW_MR', label=rh_label)
    study(2, 'SJHCSAPWFMFIR', 'GW_SCAN_UNMAPPED', label=sjh_label)

    def sites():
        with db.engine.begin() as conn:
            conn.execute(text("SELECT rh_resolve_study_sites()"))
        return rows("SELECT s.study_db_uid, si.code FROM etl_didb_studies s "
                    "LEFT JOIN sites si ON si.id = s.site_id ORDER BY 1")

    assert sites() == [(1, 'RH'), (2, 'SJH')]       # the gateway: the PACS label decides
    with db.engine.begin() as conn:
        studies.apply_gateway_devices(conn)
    assert sites() == [(1, 'SJH'), (2, 'SJH')]


def test_gateway_studies_are_still_found_once_swapped():
    """Phase 2c (CARD-family studies behind a gateway) reads GATEWAY_STUDY_SQL."""
    from db import db
    study(1, 'SJHCSAPWFMFIR', 'GW_MR')
    study(2, 'SJHCSAPWFMFIR')
    study(3, 'MR750W')
    with db.engine.begin() as conn:
        studies.apply_gateway_devices(conn)
    found = rows(f"SELECT study_db_uid FROM etl_didb_studies WHERE {studies.GATEWAY_STUDY_SQL} ORDER BY 1")
    assert found == [(1,), (2,)]
