"""
tests/test_hl7_parse.py
────────────────────────────────────────────────────────────────
Exercises utils/hl7_parse.py against constructed HL7 messages. Run it directly:

    python tests/test_hl7_parse.py

No database, no Flask, no container. Segments are built from Python lists and
joined with '|' rather than written as literal strings — hand-counting pipes to
land a value on ORC-19 is exactly the kind of thing that produces a test which
passes for the wrong reason.

The MSH offset is the single most dangerous detail in this file: MSH-1 IS the
field separator, so after splitting, every MSH field sits at (number - 1) while
every other segment reads naturally. Getting that wrong shifts the message type,
the control ID and the sender all by one — and the control ID is what the archive
deduplicates on, so the failure would look like random message loss rather than an
off-by-one.
"""
import os
import sys
import types
import datetime as dt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub the db module so utils.ray7 (and therefore utils.hl7_parse) imports without
# Flask or a database.
fake_db = types.ModuleType("db")
fake_db.db = types.SimpleNamespace(session=None)
sys.modules["db"] = fake_db

import utils.ray7 as ray7
import utils.hl7_parse as hp

# The status map normally comes from the database. Pin it to the seeded defaults.
_MAP = {
    ('', '', 'SC'): ('scheduled', 40),
    ('', '', 'AR'): ('arrived', 60),
    ('', '', 'IP'): ('started', 70),
    ('', '', 'CM'): ('completed', 100),
    ('', '', 'CA'): ('cancelled', -1),
}
ray7.status_map = lambda: _MAP
hp.resolve_status = ray7.resolve_status


def segment(name, fields=None):
    """Build a segment by field NUMBER, so no pipe-counting is involved."""
    fields = fields or {}
    top = max(fields) if fields else 0
    parts = [''] * (top + 1)
    parts[0] = name
    for num, val in fields.items():
        # MSH-2 lands at index 1 and the rest at number-1; other segments are natural.
        parts[num - 1 if name == 'MSH' else num] = val
    return '|'.join(parts)


MSH_ADT = segment('MSH', {2: r'^~\&', 3: 'SAP_HIS', 4: 'SAP_P', 5: 'RAYD',
                            7: '20260918093000', 9: 'ADT^A08', 10: 'MSG001',
                            11: 'P', 12: '2.4'})
MSH_RIS = segment('MSH', {2: r'^~\&', 3: 'RIS', 4: 'CARESTREAM', 5: 'RAYD',
                            7: '20260918101500', 9: 'ORM^O01', 10: 'MSG002',
                            11: 'P', 12: '2.4'})


def build(*segs):
    return '\r'.join(segs) + '\r'


passed = failed = 0


def check(label, got, want):
    global passed, failed
    ok = got == want
    passed, failed = passed + (1 if ok else 0), failed + (0 if ok else 1)
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        print(f"        got {got!r}\n        want {want!r}")


print("\nMSH envelope — the off-by-one that would look like random message loss")
m = hp.parse_message(build(MSH_ADT, segment('PID', {3: '00301796^^^HIS'})))
check("MSH-3 sending_app", m.sending_app, 'SAP_HIS')
check("MSH-4 sending_facility", m.sending_facility, 'SAP_P')
check("MSH-9 message_type", m.message_type, 'ADT^A08')
check("MSH-10 control_id", m.control_id, 'MSG001')
check("MSH-12 hl7_version", m.hl7_version, '2.4')
check("MSH-7 message_datetime", m.message_datetime, dt.datetime(2026, 9, 18, 9, 30))
check("content_hash populated", bool(m.content_hash), True)

print("\nADT — demographics, previously acknowledged and thrown away")
adt = build(MSH_ADT,
            segment('PID', {3: '00301796^^^HIS', 5: 'KHALIL^ROUAIDA^FAYSAL',
                              7: '19850312', 8: 'F'}),
            segment('PV1', {2: 'I', 3: 'EM^ROOM3^BED1^RH'}))
m = hp.parse_message(adt)
check("kind", m.kind, 'adt')
check("patient_id", m.patient_id, '00301796')
check("name reordered from XPN", m.patient_name, 'ROUAIDA FAYSAL KHALIL')
check("birth_date", m.birth_date, dt.date(1985, 3, 12))
check("sex", m.sex, 'F')
check("patient_class PV1-2", m.patient_class, 'I')
check("patient_location PV1-3.1", m.patient_location, 'EM')
check("no placeholders on a clean record", m.placeholders, [])

print("\nER quick registration — the 9999-11-11 trap")
m = hp.parse_message(build(MSH_ADT,
                           segment('PID', {3: 'QR001', 7: '99991111', 8: 'U'})))
check("placeholder birth date nulled", m.birth_date, None)
check("placeholder recorded for RAY7", len(m.placeholders), 2)
check("reason names the placeholder",
      any('99991111' in p for p in m.placeholders), True)
m = hp.parse_message(build(MSH_ADT, segment('PID', {3: 'X', 7: '20991231'})))
check("future birth date nulled", m.birth_date, None)

print("\nORM NW — a HIS order")
orm = build(MSH_RIS,
            segment('PID', {3: '00301796^^^HIS'}),
            segment('ORC', {1: 'NW', 2: 'PLACER77^HIS', 9: '20260918090000'}),
            segment('OBR', {4: 'ECABDPEL^CT ABDOMEN AND PELVIS', 24: 'CT'}),
            segment('PV1', {2: 'O', 8: '20000191^WAKIM^GERARD'}))
m = hp.parse_message(orm)
check("kind", m.kind, 'order')
check("placer order number", m.placer_order_number, 'PLACER77')
check("procedure_code", m.procedure_code, 'ECABDPEL')
check("procedure_text", m.procedure_text, 'CT ABDOMEN AND PELVIS')
check("modality", m.modality, 'CT')
check("event_time from ORC-9", m.event_time, dt.datetime(2026, 9, 18, 9, 0))
check("referring id from PV1-8 XCN", m.performed_by_id, '20000191')
check("XCN name excludes the ID", m.performed_by_name, 'GERARD WAKIM')

print("\nRIS lifecycle — the four status events")
for code, state, rank in (('SC', 'scheduled', 40), ('AR', 'arrived', 60),
                          ('IP', 'started', 70), ('CM', 'completed', 100)):
    msg = build(MSH_RIS,
                segment('PID', {3: '00301796^^^HIS'}),
                segment('ORC', {1: 'SC', 3: '249389^HIS', 5: code,
                                  9: '20260918101500', 19: 'TECH01^HADDAD^RIMA'}),
                segment('OBR', {4: 'ECABDPEL^CT ABDO', 24: 'CT64_RH'}),
                segment('PV1', {2: 'O', 3: 'RAD^CT-ROOM-2^^RH'}))
    m = hp.parse_message(msg)
    check(f"{code} -> kind=status", m.kind, 'status')
    check(f"{code} -> canonical_state", m.canonical_state, state)
    check(f"{code} -> ladder_rank", m.ladder_rank, rank)

check("accession from ORC-3", m.accession_number, '249389')
check("event_time from ORC-9", m.event_time, dt.datetime(2026, 9, 18, 10, 15))
check("performer id (ORC-19, provisional)", m.performed_by_id, 'TECH01')
check("performer name", m.performed_by_name, 'RIMA HADDAD')
check("aetitle (OBR-24, provisional)", m.aetitle, 'CT64_RH')
check("room name (PV1-3.2, provisional)", m.room_name, 'CT-ROOM-2')
check("raw ORC values kept", (m.raw_order_control, m.raw_order_status), ('SC', 'CM'))

print("\nUnmapped status code — must reach RAY7 as UNKNOWN_STATUS_CODE")
m = hp.parse_message(build(MSH_RIS,
                           segment('ORC', {1: 'SC', 3: '99^HIS', 5: 'ZZ'}),
                           segment('OBR', {4: 'X^Y'})))
check("still classified as a status message", m.kind, 'status')
check("no canonical state", m.canonical_state, None)
check("no rank — this is what the rule keys on", m.ladder_rank, None)
check("raw code preserved for the finding", m.raw_order_status, 'ZZ')

print("\nORU — identity and timing only")
oru = build(segment('MSH', {2: r'^~\&', 3: 'PACS', 5: 'RAYD',
                              7: '20260918120000', 9: 'ORU^R01', 10: 'MSG003', 12: '2.3'}),
            segment('PID', {3: '00301796^^^HIS'}),
            segment('OBR', {3: '2493890000001^PACS', 4: 'ECABDPEL^CT ABDO',
                              22: '20260918115500', 24: 'CT',
                              57: 'dany@sjh.com&Dany&Abou Chedid^^20260918115500'}))
m = hp.parse_message(oru)
check("kind", m.kind, 'result')
check("accession from OBR-3", m.accession_number, '2493890000001')
check("event_time from OBR-22", m.event_time, dt.datetime(2026, 9, 18, 11, 55))
check("signing physician matched on timestamp", m.performed_by_id, 'dany@sjh.com')
check("signer name", m.performed_by_name, 'Dany Abou Chedid')
check("role", m.performed_by_role, 'radiologist')

print("\nTimestamps must come back NAIVE, or fail-open silently kills a rule")
check("offset stripped", hp.parse_hl7_datetime('20260918101500+0300'),
      dt.datetime(2026, 9, 18, 10, 15))
check("result is naive", hp.parse_hl7_datetime('20260918101500').tzinfo, None)
check("date-only form", hp.parse_hl7_datetime('20260918'), dt.datetime(2026, 9, 18))
check("garbage yields None", hp.parse_hl7_datetime('notadate'), None)
check("empty yields None", hp.parse_hl7_datetime(''), None)

print("\nMalformed input must never raise — the message is the only copy")
for label, bad in (("empty string", ""),
                   ("no MSH", "PID|||123\r"),
                   ("MSH only", "MSH|^~\\&|\r"),
                   ("truncated mid-segment", "MSH|^~\\&|A|B|C\rORC|SC"),
                   ("binary junk", "\x00\x01\x02 not hl7 at all")):
    try:
        got = hp.parse_message(bad)
        check(f"{label} -> ParsedMessage, no exception",
              isinstance(got, ray7.ParsedMessage), True)
        check(f"{label} -> has a usable control_id", bool(got.control_id), True)
    except Exception as exc:
        check(f"{label} -> no exception", f"raised {type(exc).__name__}", "no exception")

print("\nBlank MSH-10 still yields a stable, dedupe-able identity")
no_id = build(segment('MSH', {2: r'^~\&', 3: 'RIS', 9: 'ORM^O01'}),
              segment('ORC', {1: 'NW', 2: 'P1'}))
a, b = hp.parse_message(no_id), hp.parse_message(no_id)
check("falls back to the content hash", a.control_id, a.content_hash)
check("identical redeliveries agree", a.control_id, b.control_id)
different = hp.parse_message(build(segment('MSH', {2: r'^~\&', 3: 'RIS', 9: 'ORM^O01'}),
                                   segment('ORC', {1: 'NW', 2: 'P2'})))
check("different content differs", a.control_id != different.control_id, True)

print(f"\n{'=' * 52}\n  {passed} passed, {failed} failed\n{'=' * 52}")
sys.exit(1 if failed else 0)
