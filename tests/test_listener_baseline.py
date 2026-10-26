"""
What LAUMC's HL7 listener does TODAY, pinned down before BRAYN arrives (2026-10-26).

BRAYN comes to LAUMC in shadow mode: it observes every message after the existing
path has handled it and writes only its own tables. These tests are that existing
path — the fill-only report upsert (the RIS batch stays authoritative), HIS orders,
PACS completions, ADT — so that "nothing changed" is something the suite checks,
not something anyone has to believe.
"""
import os

import hl7kit as k

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_every_migration_applies_on_a_fresh_database(pipe):
    files = sorted(f for f in os.listdir(os.path.join(ROOT, 'migrations')) if f.endswith('.sql'))
    applied = {r['name'] for r in pipe.q("SELECT name FROM schema_migrations")}
    assert [f for f in files if f not in applied] == []


# ── reports (ORU^R01) ─────────────────────────────────────────────────────────

def test_report_is_stored_as_hl7(pipe):
    assert pipe.send(k.report('ACC1', k.ts(10), text='No acute abnormality.',
                              impression='Normal study.')) == 'AA'
    r = pipe.one("SELECT accession_number, report_text, impression_text, report_source "
                 "FROM hl7_oru_reports")
    assert r['accession_number'] == 'ACC1' and r['report_source'] == 'hl7'
    assert 'No acute abnormality.' in r['report_text'] and r['impression_text'] == 'Normal study.'


def test_report_never_overwrites_the_ris_text(pipe):
    """FILL-ONLY: the RIS batch carries the authoritative, decrypted text."""
    pipe.exec("INSERT INTO hl7_oru_reports (accession_number, report_text, report_source) "
              "VALUES ('ACC2', 'RIS authoritative text', 'ris')")
    assert pipe.send(k.report('ACC2', k.ts(5), text='partial live text')) == 'AA'
    r = pipe.one("SELECT report_text, report_source FROM hl7_oru_reports "
                 "WHERE accession_number = 'ACC2'")
    assert r == {'report_text': 'RIS authoritative text', 'report_source': 'ris'}
    assert pipe.count('hl7_oru_reports') == 1


def test_report_fills_what_the_ris_left_empty(pipe):
    pipe.exec("INSERT INTO hl7_oru_reports (accession_number, report_text, report_source) "
              "VALUES ('ACC3', 'RIS text', 'ris')")
    pipe.send(k.report('ACC3', k.ts(5), text='live', impression='Live impression.'))
    assert pipe.scalar("SELECT impression_text FROM hl7_oru_reports "
                       "WHERE accession_number = 'ACC3'") == 'Live impression.'


def test_report_without_text_is_acknowledged_and_not_stored(pipe):
    raw = k.message(k.msh('PACS', None, 'ORU^R01', k.ts(5)), k.pid(), k.obr(acc='ACC4'),
                    k.obx('12', vtype='NM'))
    assert pipe.send(raw) == 'AA'
    assert pipe.count('hl7_oru_reports') == 0


# ── orders (ORM^O01) ──────────────────────────────────────────────────────────

def test_his_order_is_stored(pipe):
    assert pipe.send(k.order('PL-1', when=k.ts(30))) == 'AA'
    o = pipe.one("SELECT placer_order_number, accession_number, order_status, patient_id, "
                 "referring_physician_code FROM hl7_orders")
    assert o == {'placer_order_number': 'PL-1', 'accession_number': None, 'order_status': 'NW',
                 'patient_id': 'P0001', 'referring_physician_code': '20000191'}


def test_same_control_id_updates_the_order(pipe):
    pipe.send(k.order('PL-2', when=k.ts(30), ctrl='SAME-1'))
    pipe.send(k.status('AR', 'ACC5', k.ts(20), ctrl='SAME-1', placer='PL-2'))
    assert pipe.count('hl7_orders') == 1
    assert pipe.scalar("SELECT order_status FROM hl7_orders") == 'AR'


def test_ris_status_events_are_stored_as_orders(pipe):
    """Today a RIS status message lands in hl7_orders like any ORM (the R2I feed is
    not live yet, so LAUMC has never seen one in production)."""
    assert pipe.send(k.status('AR', 'ACC6', k.ts(20))) == 'AA'
    assert pipe.one("SELECT accession_number, order_status FROM hl7_orders") == \
        {'accession_number': 'ACC6', 'order_status': 'AR'}


# ── PACS completion (ORM with ORC-1 = SC, ORC-5 = CM) ─────────────────────────

def test_pacs_completion_closes_the_order(pipe):
    pipe.send(k.order('PL-3', acc='ACC7', when=k.ts(60)))
    assert pipe.send(k.status('CM', 'ACC7', k.ts(5), app='PACS', control='SC')) == 'AA'
    assert pipe.one("SELECT order_status, done_by FROM hl7_orders "
                    "WHERE accession_number = 'ACC7'") == {'order_status': 'CM', 'done_by': 'PACS'}
    assert pipe.count('hl7_scn_studies', "accession_number = 'ACC7'") == 1


def test_pacs_completion_without_an_order_is_still_counted(pipe):
    pipe.send(k.status('CM', 'ACC8', k.ts(5), app='PACS', control='SC'))
    assert pipe.count('hl7_scn_studies', "accession_number = 'ACC8'") == 1
    assert pipe.count('hl7_orders') == 0


# ── ADT ───────────────────────────────────────────────────────────────────────

def test_adt_is_acknowledged_and_not_stored(pipe):
    assert pipe.send(k.adt()) == 'AA'
    assert pipe.count('hl7_orders') == 0 and pipe.count('hl7_oru_reports') == 0
