"""
tests/test_hl7_site_routing.py — item 13d (operator 2026-11-19): the HIS's orders are written to
their hospital's database once that hospital has moved (utils/hl7_site.py, hl7_listener.py).
Before the move, and for RH, everything stays on the main database, as before.
"""
import pytest
from sqlalchemy import text

from utils import hl7_site


def raw(message_id, pv1_3, unit=''):
    orc = '|'.join(['ORC', 'NW', f'{message_id}^HIS'] + [''] * 14 + [f'{unit}^^'])   # unit = ORC-17
    return (f"MSH|^~\\&|SAP_P||RAYD||20261119120000||ORM^O01|{message_id}|P|2.3\r"
            f"PID|1||P0001^^^HIS||DOE^JANE\r"
            f"PV1|1|O|{pv1_3}\r{orc}\r"
            f"OBR|1|{message_id}^HIS||J17A-01^CT SCAN, BRAIN\r")


def segments(message):
    return [s for s in message.split('\r') if s]


@pytest.fixture
def sites(app):
    from db import db
    from utils import site_db
    with app.app_context():
        if not site_db.has_site_databases():
            pytest.skip('no site database configured')
        ids = dict(db.session.execute(text("SELECT code, id FROM sites")).all())
        engines = {'RH': db.engine, 'SJH': site_db.engine_for(ids['SJH'])}

        def clean():
            for e in engines.values():
                with e.begin() as c:
                    c.execute(text("DELETE FROM hl7_orders WHERE message_id LIKE 'RT-%'"))
            with db.engine.begin() as c:
                c.execute(text("DELETE FROM settings WHERE key = 'site_route_moved_SJH'"))
            hl7_site.reset()
        clean()
        yield ids, engines
        clean()


def moved(sites_ids_engines, on=True):
    _ids, engines = sites_ids_engines
    with engines['RH'].begin() as c:
        c.execute(text("DELETE FROM settings WHERE key = 'site_route_moved_SJH'"))
        if on:
            c.execute(text("INSERT INTO settings (key, value) VALUES ('site_route_moved_SJH', '2026-11-21')"))
    hl7_site.reset()


def test_nothing_is_routed_before_the_move(app, sites):
    with app.app_context():
        assert hl7_site.his_order_site(segments(raw('RT-1', 'EM-J^^^^^^2000'))) is None


def test_after_the_move_sjhs_orders_go_to_sjh_and_rhs_stay(app, sites):
    ids, _ = sites
    moved(sites)
    with app.app_context():
        route = lambda m: hl7_site.his_order_site(segments(m))
        assert route(raw('RT-1', 'EM-J^^^^^^2000')) == ids['SJH']        # PV1-3.7
        assert route(raw('RT-2', 'WARD9^^^^^^1000')) is None              # RH: the main database
        assert route(raw('RT-3', 'EM-J')) == ids['SJH']                   # no code: its -J location
        assert route(raw('RT-4', '', unit='ENDOS-J')) == ids['SJH']       # no code: its -J unit
        assert route(raw('RT-5', 'NEWWARD')) is None                      # names no hospital: main
        assert route(raw('RT-6', 'X^^^^^^9999')) is None                  # an unknown code: main


def test_the_listener_writes_an_sjh_order_to_sjhs_database(app, sites, pipe):
    _ids, engines = sites

    def count(site, mid):
        with engines[site].connect() as c:          # closed at once: no transaction left open
            return c.execute(text("SELECT COUNT(*) FROM hl7_orders WHERE message_id = :m"), {'m': mid}).scalar()

    assert pipe.send(raw('RT-BEFORE', 'EM-J^^^^^^2000')) == 'AA'        # before the move: main
    assert (count('RH', 'RT-BEFORE'), count('SJH', 'RT-BEFORE')) == (1, 0)

    moved(sites)
    assert pipe.send(raw('RT-SJ', 'EM-J^^^^^^2000')) == 'AA'
    assert pipe.send(raw('RT-RH', 'WARD9^^^^^^1000')) == 'AA'
    assert (count('RH', 'RT-SJ'), count('SJH', 'RT-SJ')) == (0, 1)
    assert (count('RH', 'RT-RH'), count('SJH', 'RT-RH')) == (1, 0)
