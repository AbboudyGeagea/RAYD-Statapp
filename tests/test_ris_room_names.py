"""
tests/test_ris_room_names.py
────────────────────────────────────────────────────────────────
Item 4 of the meeting of 2026-11-13: the room name from the RIS instead of the AE title,
everywhere. Decided that day:

    the RIS is the master of room name, room code, description, site and active flag,
    written over every night (ETL_JOBS/etl_ris_modality.py was fill-only, so a room name
    corrected on the RIS never reached RAYD)
    RAYD's own fields stay: modality (corrected by hand, migration 0117), capacity
    gateways and routers (modality 'PACS') have no RIS room and are checked by hand
    the label typed in RAYD (display_aetitle) is dropped, so the RIS name wins

Two safety rules of this build: a value the RIS leaves empty, or an org key with no site,
never blanks what RAYD holds; and two RIS rows for one AE keep the active one.
"""
import logging
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_ris_modality

RH_ORG, SJH_ORG = '3926', '5320'
ADMIN = {'X-Test-Role': 'admin'}


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def site(code):
    from db import db
    return db.session.execute(text("SELECT id FROM sites WHERE code = :c"), {'c': code}).scalar()


def device_row(aetitle):
    from db import db
    db.session.rollback()
    row = db.session.execute(text("""
        SELECT aetitle, modality, station_name, room_name, room_code, description, site_id,
               active, ris_modality_key, daily_capacity_minutes
        FROM aetitle_modality_map WHERE aetitle = :a"""), {'a': aetitle}).mappings().first()
    return dict(row) if row else None


def existing(aetitle, modality='CT', name='Old name', site_code='RH', active=True, **extra):
    sql("""INSERT INTO aetitle_modality_map
               (aetitle, modality, station_name, room_name, room_code, description, site_id,
                active, daily_capacity_minutes, display_aetitle)
           VALUES (:a, :m, :n, :n, 'OLD', 'old description',
                   (SELECT id FROM sites WHERE code = :s), :act, 600, :alias)""",
        a=aetitle, m=modality, n=name, s=site_code, act=active, alias=extra.get('alias'))


class FakeRis:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return self

    def execute(self, sql):
        pass

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def ris(key, ae, station, org=RH_ORG, active='Y', code='CT64', description='GE Revolution',
        modality_type='CT'):
    """One RIS MODALITY row, in the order the ETL's query selects it."""
    return (key, ae, code, description, station, org, active, modality_type)


def load(monkeypatch, *rows):
    from db import db
    monkeypatch.setattr(etl_ris_modality, 'OracleConnector',
                        SimpleNamespace(get_connection=lambda source: FakeRis(list(rows))))
    return etl_ris_modality.run_ris_modality_etl(db.engine, 'RIS')


# ── the nightly RIS load ────────────────────────────────────────────────────────

def test_a_room_corrected_on_the_ris_reaches_rayd(monkeypatch):
    existing('CTX1')
    load(monkeypatch, ris(101, 'CTX1', 'CT Room 1'))
    d = device_row('CTX1')
    assert (d['station_name'], d['room_name']) == ('CT Room 1', 'CT Room 1')
    assert (d['room_code'], d['description']) == ('CT64', 'GE Revolution')
    assert d['site_id'] == site('RH')
    assert d['ris_modality_key'] == 101


def test_rayds_own_fields_stay(monkeypatch):
    """BAY85CT-style: the RIS type says PT, RAYD corrected the modality by hand."""
    existing('CTX1', modality='CT')
    load(monkeypatch, ris(101, 'CTX1', 'CT Room 1', modality_type='PT'))
    d = device_row('CTX1')
    assert d['modality'] == 'CT'
    assert d['daily_capacity_minutes'] == 600


def test_the_site_and_active_flag_follow_the_ris(monkeypatch):
    existing('CTX1', site_code='RH', active=True)
    load(monkeypatch, ris(101, 'CTX1', 'CT Room 1', org=SJH_ORG, active='N'))
    d = device_row('CTX1')
    assert d['site_id'] == site('SJH')
    assert d['active'] is False


def test_an_empty_ris_value_never_blanks_rayd(monkeypatch):
    existing('CTX1', name='Kept name', active=False)
    load(monkeypatch, ris(101, 'CTX1', '   ', org='9999', active=None, code=None, description=''))
    d = device_row('CTX1')
    assert (d['station_name'], d['room_name']) == ('Kept name', 'Kept name')
    assert (d['room_code'], d['description']) == ('OLD', 'old description')
    assert d['site_id'] == site('RH')       # org key 9999 has no site
    assert d['active'] is False             # the RIS did not say


def test_a_new_ris_device_is_added(monkeypatch):
    load(monkeypatch, ris(102, 'mrx1 ', 'MR Room 3', active=None, modality_type='MR'))
    d = device_row('MRX1')
    assert (d['modality'], d['station_name'], d['site_id']) == ('MR', 'MR Room 3', site('RH'))
    assert d['active'] is True


def test_an_ae_spelled_differently_on_the_ris_updates_the_same_device(monkeypatch):
    existing('CTX1')
    load(monkeypatch, ris(101, ' ctx1', 'CT Room 1'))
    assert device_row('CTX1')['station_name'] == 'CT Room 1'


def test_a_gateway_is_left_alone(monkeypatch):
    """No RIS room; LAUMCWFM1AR's site was cleared on purpose (migration 0141)."""
    existing('GWX1', modality='PACS', name='Router', site_code='NONE')
    before = device_row('GWX1')
    load(monkeypatch, ris(103, 'GWX1', 'Some RIS name', code='GW'))
    assert device_row('GWX1') == before


def test_two_ris_rows_for_one_ae_keep_the_active_one(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    load(monkeypatch, ris(150, 'USX1', 'US Room 2', active='Y', modality_type='US'),
                      ris(201, 'USX1', 'Old US room', active='N', modality_type='US'))
    d = device_row('USX1')
    assert (d['station_name'], d['ris_modality_key'], d['active']) == ('US Room 2', 150, True)
    assert 'more than one RIS row: USX1' in caplog.text


def test_ris_data_errors_are_logged(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    load(monkeypatch, ris(104, 'DXX1', None, modality_type='DX'),
                      ris(105, 'DXX2', 'DR Room', org='9999', modality_type='DX'),
                      ris(106, 'DXX3', None, active='N', modality_type='DX'))
    assert 'no room name (STATION_NAME) on the RIS: DXX1' in caplog.text
    assert 'no site for its ORG_STRUCTURE_KEY: DXX2' in caplog.text
    assert 'DXX3' not in caplog.text         # inactive: not worth a warning


# ── one label everywhere: the RIS room name, never display_aetitle ─────────────

def test_the_label_is_the_ris_room_name_not_the_typed_alias():
    from db import db
    from utils.ae_display import ae_display_sql
    existing('CTX1', name='CT Room 1', alias='ALIAS')
    existing('CTX2', name=None, alias='ALIAS2')
    labels = dict(db.session.execute(text(
        f"SELECT aetitle, {ae_display_sql('m', 'm.aetitle')} FROM aetitle_modality_map m "
        "WHERE aetitle IN ('CTX1', 'CTX2')")).all())
    assert labels == {'CTX1': 'CT Room 1', 'CTX2': 'CTX2'}


def test_report_22_shows_the_ris_room_name(client):
    import json
    import re
    existing('CTX1', name='CT Room 1', alias='ALIAS')
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_instance_uid,
               storing_ae, study_date, study_modality, study_status)
           VALUES (1, 1, '1.2.3.1', 'CTX1', '2026-10-15', 'CT', 'APPROVED')""")
    html = client.get('/report/22', headers=ADMIN, query_string={
        'start_date': '2026-10-01', 'end_date': '2026-10-31'}).get_data(as_text=True)
    tree = json.loads(re.search(r'const d = (.*);', html).group(1))['tree_data'][0]
    assert tree['children'][0]['children'][0]['name'] == 'CT Room 1\n(1)'


def test_the_ae_dropdown_labels_with_the_ris_room_name(client):
    existing('CTX1', name='CT Room 1', alias='ALIAS')
    labels = client.get('/api/filter-options', headers=ADMIN).get_json()['aetitle_labels']
    assert labels['CTX1'] == 'CT Room 1'


def test_report_composer_names_the_device_by_its_ris_room():
    """Migration 0148: rpt.studies' device_name and room, the Room filter's column."""
    from db import db
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, station_name, room_name,
                                             display_aetitle, site_id)
           VALUES ('CTX1', 'CT', 'RIS Room', 'Typed room', 'ALIAS',
                   (SELECT id FROM sites WHERE is_default))""")
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_instance_uid,
               storing_ae, study_date, study_modality, study_status)
           VALUES (1, 1, '1.2.3.1', 'ctx1', '2026-10-15', 'CT', 'APPROVED')""")
    row = db.session.execute(text("SELECT device_name, room FROM rpt.studies")).one()
    assert tuple(row) == ('RIS Room', 'RIS Room')


# ── the Mapping page ────────────────────────────────────────────────────────────

def test_a_ris_device_cannot_be_renamed_in_rayd(client, monkeypatch):
    load(monkeypatch, ris(101, 'CTX1', 'CT Room 1'))
    response = client.post('/mapping/ae/update', headers=ADMIN,
                           json={'aetitle': 'CTX1', 'description': 'typed in RAYD'})
    assert response.status_code == 409
    assert device_row('CTX1')['description'] == 'GE Revolution'


def test_a_ris_devices_modality_can_still_be_corrected(client, monkeypatch):
    load(monkeypatch, ris(101, 'CTX1', 'CT Room 1', modality_type='PT'))
    response = client.post('/mapping/ae/update', headers=ADMIN,
                           json={'aetitle': 'CTX1', 'modality': 'CT'})
    assert response.status_code == 200
    assert device_row('CTX1')['modality'] == 'CT'


def test_a_device_the_ris_does_not_know_keeps_its_typed_room_name(client):
    existing('GWX1', modality='PACS', name=None, site_code='NONE')
    response = client.post('/mapping/ae/update', headers=ADMIN,
                           json={'aetitle': 'GWX1', 'room_name': 'Gateway A',
                                 'display_aetitle': 'IGNORED'})
    assert response.status_code == 200
    from db import db
    db.session.rollback()
    row = db.session.execute(text("SELECT room_name, display_aetitle FROM aetitle_modality_map "
                                  "WHERE aetitle = 'GWX1'")).one()
    assert tuple(row) == ('Gateway A', None)
