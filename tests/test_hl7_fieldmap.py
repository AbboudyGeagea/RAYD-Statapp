"""
tests/test_hl7_fieldmap.py
────────────────────────────────────────────────────────────────
Exercises the configurable field-mapping overlay. Run it directly:

    python tests/test_hl7_fieldmap.py

The mapping table is stubbed, so no database is involved.

What this is really protecting is the promise made to implementation engineers:
that a mapping says OBR-24 and means OBR-24. Internally MSH is off by one, because
MSH-1 IS the field separator and so its fields land one index lower once the
segment is split. If that absorption is wrong, every MSH mapping an engineer
writes is silently off by one field — they would configure the message type and
get the security field, with nothing anywhere reporting a problem.
"""
import os
import sys
import types
import datetime as dt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

fake_db = types.ModuleType("db")
fake_db.db = types.SimpleNamespace(session=None)
sys.modules["db"] = fake_db

import utils.ray7 as ray7            # noqa: E402
import utils.hl7_parse as hp         # noqa: E402
import utils.hl7_fieldmap as fm      # noqa: E402

ray7.status_map = lambda: {('', '', 'CM'): ('completed', 100)}
hp.resolve_status = ray7.resolve_status

passed = failed = 0


def check(label, got, want):
    global passed, failed
    ok = got == want
    passed, failed = passed + (1 if ok else 0), failed + (0 if ok else 1)
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        print(f"        got {got!r}\n        want {want!r}")


def mapping(target, segment, field, component=None, priority=10,
            transform='text', kind='', app='', target_kind='parsed', repeat=None):
    return {'sending_app': app, 'message_kind': kind, 'target_kind': target_kind,
            'target_field': target, 'segment': segment, 'field_index': field,
            'component_index': component, 'repeat_index': repeat,
            'priority': priority, 'transform': transform}


def seg(name, fields):
    top = max(fields)
    parts = [''] * (top + 1)
    parts[0] = name
    for n, v in fields.items():
        parts[n - 1 if name == 'MSH' else n] = v
    return '|'.join(parts)


MSH = seg('MSH', {2: r'^~\&', 3: 'RIS', 4: 'SITE', 5: 'RAYD', 7: '20260918050000',
                  9: 'ORM^O01', 10: 'CTRL-1', 11: 'P', 12: '2.4'})
MSG = '\r'.join([
    MSH,
    seg('PID', {3: '00301796^^^HIS', 5: 'KHALIL^ROUAIDA^FAYSAL', 7: '19850312', 8: 'F'}),
    seg('ORC', {1: 'SC', 3: '249389^HIS', 5: 'CM', 9: '20260918054500',
                19: 'TECH01^HADDAD^RIMA'}),
    seg('OBR', {3: '249389^HIS', 4: 'ECABDPEL^CT ABDOMEN', 21: 'CT64_RH', 24: 'CT'}),
    seg('PV1', {2: 'O', 3: 'RAD^CT-ROOM-2^^RH'}),
]) + '\r'

SEGMENTS = hp.split_segments(MSG)
BY_NAME = fm._index_segments(SEGMENTS)


print("\nHUMAN FIELD NUMBERS — a mapping that says OBR-24 must mean OBR-24")
check("OBR-24 -> modality", fm._extract(BY_NAME, mapping('modality', 'OBR', 24)), 'CT')
check("OBR-21 -> AE title", fm._extract(BY_NAME, mapping('aetitle', 'OBR', 21)), 'CT64_RH')
check("ORC-5 -> status", fm._extract(BY_NAME, mapping('raw_order_status', 'ORC', 5)), 'CM')
check("PV1-2 -> class", fm._extract(BY_NAME, mapping('patient_class', 'PV1', 2)), 'O')

print("\nTHE MSH TRAP — MSH-1 is the separator, so its fields sit one index lower")
check("MSH-3 is the sending app, not the facility",
      fm._extract(BY_NAME, mapping('x', 'MSH', 3)), 'RIS')
check("MSH-4 is the facility", fm._extract(BY_NAME, mapping('x', 'MSH', 4)), 'SITE')
check("MSH-9 is the message type", fm._extract(BY_NAME, mapping('x', 'MSH', 9)), 'ORM^O01')
check("MSH-10 is the control ID", fm._extract(BY_NAME, mapping('x', 'MSH', 10)), 'CTRL-1')
check("MSH-12 is the version", fm._extract(BY_NAME, mapping('x', 'MSH', 12)), '2.4')

print("\nComponents are 1-based too")
check("PID-3.1", fm._extract(BY_NAME, mapping('patient_id', 'PID', 3, 1)), '00301796')
check("OBR-4.2", fm._extract(BY_NAME, mapping('procedure_text', 'OBR', 4, 2)), 'CT ABDOMEN')
check("PV1-3.2 room", fm._extract(BY_NAME, mapping('room_name', 'PV1', 3, 2)), 'CT-ROOM-2')
check("whole field when no component",
      fm._extract(BY_NAME, mapping('x', 'PID', 3)), '00301796^^^HIS')

print("\nTransforms")
check("datetime", fm._extract(BY_NAME, mapping('event_time', 'ORC', 9, transform='datetime')),
      dt.datetime(2026, 9, 18, 5, 45))
check("date", fm._extract(BY_NAME, mapping('birth_date', 'PID', 7, transform='date')),
      dt.date(1985, 3, 12))
check("name_xpn reorders to given-first",
      fm._extract(BY_NAME, mapping('patient_name', 'PID', 5, transform='name_xpn')),
      'ROUAIDA FAYSAL KHALIL')
check("name_xcn skips the leading ID",
      fm._extract(BY_NAME, mapping('performed_by_name', 'ORC', 19, transform='name_xcn')),
      'RIMA HADDAD')
check("upper", fm._extract(BY_NAME, mapping('x', 'OBR', 21, transform='upper')), 'CT64_RH')

print("\nMisses return None rather than raising")
check("segment absent", fm._extract(BY_NAME, mapping('x', 'ZZZ', 1)), None)
check("field past the end", fm._extract(BY_NAME, mapping('x', 'PID', 99)), None)
check("component past the end", fm._extract(BY_NAME, mapping('x', 'OBR', 24, 9)), None)
check("empty field", fm._extract(BY_NAME, mapping('x', 'PID', 4)), None)
check("transform rejects non-date",
      fm._extract(BY_NAME, mapping('x', 'OBR', 21, transform='datetime')), None)

print("\nScoping — '' means any")
check("blank scope applies", fm._applies(mapping('x', 'OBR', 1), 'RIS', 'status'), True)
check("matching sender applies",
      fm._applies(mapping('x', 'OBR', 1, app='RIS'), 'RIS', 'status'), True)
check("other sender does not",
      fm._applies(mapping('x', 'OBR', 1, app='PACS'), 'RIS', 'status'), False)
check("matching kind applies",
      fm._applies(mapping('x', 'OBR', 1, kind='status'), 'RIS', 'status'), True)
check("other kind does not",
      fm._applies(mapping('x', 'OBR', 1, kind='adt'), 'RIS', 'status'), False)

print("\nOVERLAY — configuration wins, and a miss leaves the built-in answer alone")
fm._cache['rows'] = [mapping('aetitle', 'ORC', 19, 1)]      # AE from ORC-19 instead
fm._cache['at'] = 9e18
m = hp.parse_message(MSG)
check("configured source overrides the built-in", m.aetitle, 'TECH01')
check("unconfigured field keeps the built-in value", m.modality, 'CT')

fm._cache['rows'] = [mapping('aetitle', 'ZZZ', 1)]          # points at nothing
m = hp.parse_message(MSG)
check("a mapping that matches nothing does NOT blank the field", m.aetitle, 'CT64_RH')

print("\nFallback chain — lowest priority first, first non-empty wins")
fm._cache['rows'] = [
    mapping('aetitle', 'OBR', 99, priority=10),   # empty
    mapping('aetitle', 'OBR', 21, priority=20),   # populated
]
m = hp.parse_message(MSG)
check("falls through an empty source to the next", m.aetitle, 'CT64_RH')

fm._cache['rows'] = [
    mapping('aetitle', 'OBR', 24, priority=10),
    mapping('aetitle', 'OBR', 21, priority=20),
]
m = hp.parse_message(MSG)
check("stops at the first source that has a value", m.aetitle, 'CT')

print("\nDirect etl_* overrides are separated from parsed ones")
fm._cache['rows'] = [
    mapping('study_modality', 'OBR', 21, target_kind='study'),
    mapping('modality', 'OBR', 24, target_kind='parsed'),
]
m = hp.parse_message(MSG)
ov = fm.direct_overrides(m, SEGMENTS)
check("study override captured", ov['study'], {'study_modality': 'CT64_RH'})
check("parsed mapping did not leak into overrides", ov['patient'], {})
check("parsed target still applied to the message", m.modality, 'CT')

print("\nTHE OVERLAY MUST NOT UNDO THE PARSER")
# The overlay runs in parse_message()'s finally block — AFTER the hardcoded parse.
# So a mapping is not only an addition, it can overwrite a value the parser
# deliberately rejected. It did: 0123 seeded parsed.birth_date <- PID-7 with the
# generic 'date' transform, parse_birth_date() dropped the 9999-11-11
# quick-registration sentinel, and the overlay put it straight back. hl7_patients
# CHECKs birth_date into 1880..2100, so the patient row was rejected and the study
# was left pointing at a patient that did not exist.
#
# Every test here passed throughout. Each half was right on its own; the defect was
# the ordering. These check the composition, which is where it actually lived.
QR = '\r'.join([
    MSH,
    seg('PID', {3: 'QR001^^^HIS', 5: 'PATIENT^UNKNOWN', 7: '99991111', 8: 'U'}),
    seg('ORC', {1: 'SC', 3: '249390^HIS', 5: 'CM'}),
    seg('OBR', {3: '249390^HIS', 4: 'ECABDPEL^CT ABDOMEN', 24: 'CT'}),
]) + '\r'

fm._cache['rows'] = [mapping('birth_date', 'PID', 7, transform='birth_date')]
check("sentinel DOB stays dropped when the overlay re-applies PID-7",
      hp.parse_message(QR).birth_date, None)

fm._cache['rows'] = [mapping('birth_date', 'PID', 7, transform='date')]
check("plain 'date' transform also refuses 9999-11-11",
      hp.parse_message(QR).birth_date, None)

REAL = '\r'.join([
    MSH,
    seg('PID', {3: 'P7^^^HIS', 5: 'KHALIL^ROUAIDA', 7: '19850312', 8: 'F'}),
    seg('ORC', {1: 'SC', 3: '249391^HIS', 5: 'CM'}),
    seg('OBR', {3: '249391^HIS', 4: 'ECABDPEL^CT ABDOMEN', 24: 'CT'}),
]) + '\r'
fm._cache['rows'] = [mapping('birth_date', 'PID', 7, transform='birth_date')]
check("a real DOB still comes through",
      hp.parse_message(REAL).birth_date, dt.date(1985, 3, 12))

check("_plausible_date passes a normal date",
      fm._plausible_date(dt.date(1985, 3, 12)), dt.date(1985, 3, 12))
check("_plausible_date rejects the year 9999", fm._plausible_date(dt.date(9999, 11, 11)), None)
check("_plausible_date rejects a pre-1880 date", fm._plausible_date(dt.date(1799, 1, 1)), None)
check("_plausible_date passes None through", fm._plausible_date(None), None)

fm._cache['rows'] = []

print("\nA broken configuration must not break ingestion")
fm._cache['rows'] = [{'bogus': 'row'}]
try:
    m = hp.parse_message(MSG)
    check("garbage mapping row -> message still parses", m.accession_number, '249389')
except Exception as e:
    check("garbage mapping row -> message still parses", f"raised {type(e).__name__}", "no raise")

fm._cache['rows'] = []
print(f"\n{'=' * 52}\n  {passed} passed, {failed} failed\n{'=' * 52}")
sys.exit(1 if failed else 0)
