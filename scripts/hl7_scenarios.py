#!/usr/bin/env python3
"""
scripts/hl7_scenarios.py
────────────────────────────────────────────────────────────────
Drive named HL7 scenarios at the MLLP listener, and reset the test database.

    python scripts/hl7_scenarios.py --list
    python scripts/hl7_scenarios.py happy_path
    python scripts/hl7_scenarios.py --all
    python scripts/hl7_scenarios.py load --studies 500
    python scripts/hl7_scenarios.py --reset --confirm

WHY THIS EXISTS AS A COMMITTED SCRIPT RATHER THAN A SCRATCH FILE
Every defect that mattered while this branch was built was found by sending real
messages, not by reasoning about the code and not by the unit tests — which all
passed throughout. OBR-24 colliding with modality, the missing state columns, the
sequence rules firing on everything, is_closed never set, hl7_orders inflating
fourfold, a RIS completion being recorded as a PACS one. Six real bugs, all found
by looking at what actually landed in the tables.

So the ability to replay a scenario deliberately, repeatedly, and identically is
not a convenience here. It is the technique that works on this codebase, and it
belongs in the repository rather than in somebody's shell history.

SEGMENTS ARE BUILT BY FIELD NUMBER, never by writing pipes by hand. Hand-counting
delimiters to land a value on ORC-19 is exactly how a fixture ends up encoding the
bug it was meant to catch — which happened once during this work, in a throwaway
check, and cost a confusing half hour.
"""
import argparse
import os
import socket
import sys
import time
from datetime import datetime, timedelta

# Windows consoles default to cp1252, which cannot encode the box-drawing
# characters used elsewhere in this codebase. Reconfigure rather than avoid them:
# the script has to run identically on the developer's Windows host and inside the
# Linux container, and silently dying on a separator line is a poor way to find
# out it does not.
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

HOST = os.environ.get('HL7_HOST', '127.0.0.1')
PORT = int(os.environ.get('HL7_PORT', '6661'))
SB, EB, CR = b'\x0b', b'\x1c', b'\x0d'

_sent = {'ok': 0, 'nak': 0, 'error': 0}


# ── message construction ──────────────────────────────────────────────────────

def seg(name, fields=None):
    """Build a segment by FIELD NUMBER. MSH's off-by-one is handled here."""
    fields = fields or {}
    top = max(fields) if fields else 1
    parts = [''] * (top + 1)
    parts[0] = name
    for num, val in fields.items():
        parts[num - 1 if name == 'MSH' else num] = str(val)
    return '|'.join(parts)


def msh(app, ctrl, mtype, ts, facility='SITE', version='2.4'):
    return seg('MSH', {2: r'^~\&', 3: app, 4: facility, 5: 'RAYD',
                       7: ts, 9: mtype, 10: ctrl, 11: 'P', 12: version})


def ts(minutes_ago):
    return (datetime.now() - timedelta(minutes=minutes_ago)).strftime('%Y%m%d%H%M%S')


def send(label, segments, quiet=False):
    """Frame in MLLP, send, and report the ACK code."""
    body = '\r'.join(segments) + '\r'
    try:
        s = socket.socket(); s.settimeout(20); s.connect((HOST, PORT))
        s.sendall(SB + body.encode('utf-8') + EB + CR)
        resp = s.recv(8192).decode('utf-8', 'replace')
        s.close()
    except Exception as exc:
        _sent['error'] += 1
        print(f"  ERROR {label}: {exc}")
        return None

    msa = next((l for l in resp.replace('\r', '\n').split('\n')
                if l.startswith('MSA')), '')
    code = msa.split('|')[1] if '|' in msa else '??'
    _sent['ok' if code == 'AA' else 'nak'] += 1
    if not quiet:
        print(f"  {code}  {label}")
    time.sleep(0.12)
    return code


# ── building blocks ───────────────────────────────────────────────────────────

def pid(pat='00301796', name='KHALIL^ROUAIDA^FAYSAL', dob='19850312', sex='F'):
    return seg('PID', {3: f'{pat}^^^HIS', 5: name, 7: dob, 8: sex})


def obr(acc, code='ECABDPEL', desc='CT ABDOMEN AND PELVIS', ae='CT64_RH', mod='CT'):
    # AE title on OBR-21, modality on OBR-24. They are DIFFERENT fields: putting
    # both on 24 is the bug this branch already shipped and fixed once.
    return seg('OBR', {3: f'{acc}^HIS', 4: f'{code}^{desc}', 21: ae, 24: mod})


def pv1(cls='O', loc='RAD^CT-ROOM-2^^RH', ref='20000191^WAKIM^GERARD'):
    return seg('PV1', {2: cls, 3: loc, 8: ref})


def lifecycle(acc, ctrl_prefix, offsets, app='RIS', tech='TECH01^HADDAD^RIMA', **kw):
    """The four RIS status events for one study, oldest first."""
    out = []
    for code, mins in offsets:
        out.append((f'{ctrl_prefix}-{code}', [
            msh(app, f'{ctrl_prefix}-{code}', 'ORM^O01', ts(mins)),
            pid(kw.get('pat', '00301796')),
            seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: code, 9: ts(mins), 19: tech}),
            obr(acc, **{k: v for k, v in kw.items() if k in
                        ('code', 'desc', 'ae', 'mod')}),
            pv1(),
        ]))
    return out


# ── scenarios ─────────────────────────────────────────────────────────────────

def sc_happy_path():
    """A complete, well-behaved study: ADT, order, four events, report."""
    acc = '900001'
    send('ADT^A08 demographics', [msh('SAP_HIS', 'HP-ADT', 'ADT^A08', ts(300)),
                                  pid(), seg('PV1', {2: 'O', 3: 'EM^ROOM3^^RH'})])
    send('ORM NW order', [msh('SAP_HIS', 'HP-ORD', 'ORM^O01', ts(290)), pid(),
                          seg('ORC', {1: 'NW', 2: 'PLACER-HP^HIS', 9: ts(290)}),
                          obr(acc), pv1()])
    for label, segs in lifecycle(acc, 'HP', [('SC', 280), ('AR', 240),
                                             ('IP', 225), ('CM', 195)]):
        send(f'lifecycle {label}', segs)
    send('ORU report', [msh('PACS', 'HP-ORU', 'ORU^R01', ts(150)), pid(),
                        seg('OBR', {3: f'{acc}^HIS', 4: 'ECABDPEL^CT ABDO',
                                    22: ts(155), 24: 'CT',
                                    57: f'rad@sjh.com&Dany&Abou Chedid^^{ts(155)}'}),
                        seg('OBX', {2: 'TX', 3: 'REPORT', 5: 'No acute abnormality.', 11: 'F'}),
                        seg('OBX', {2: 'TX', 3: 'IMPRESSION', 5: 'Normal study.', 11: 'F'})])


def sc_out_of_order():
    """
    Events delivered backwards, timestamps consistent.

    Must produce NO findings. Late delivery is routine and the projector absorbs
    it; flagging it would bury the queue in noise. This is the scenario most
    likely to regress, because the obvious implementation gets it wrong.
    """
    acc = '900002'
    events = lifecycle(acc, 'OOO', [('CM', 195), ('IP', 225), ('AR', 240), ('SC', 280)])
    for label, segs in events:
        send(f'reversed {label}', segs)


def sc_time_contradiction():
    """Completed stamped BEFORE arrived. A genuine fault, unlike out-of-order."""
    acc = '900003'
    send('arrived 10:00', [msh('RIS', 'TC-AR', 'ORM^O01', ts(200)), pid(),
                           seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'AR', 9: ts(200)}),
                           obr(acc), pv1()])
    send('completed stamped EARLIER', [msh('RIS', 'TC-CM', 'ORM^O01', ts(190)), pid(),
                                       seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'CM',
                                                   9: ts(260)}), obr(acc), pv1()])


def sc_duplicates():
    """Exact redelivery (expected) and control-ID reuse (critical)."""
    acc = '900004'
    original = [msh('RIS', 'DUP-1', 'ORM^O01', ts(180)), pid(),
                seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'AR', 9: ts(180)}),
                obr(acc), pv1()]
    send('original', original)
    send('byte-identical redelivery', original)
    send('same control ID, DIFFERENT content', [
        msh('RIS', 'DUP-1', 'ORM^O01', ts(180)), pid('00555444', 'OTHER^PATIENT'),
        seg('ORC', {1: 'SC', 3: '900099^HIS', 5: 'AR', 9: ts(180)}),
        obr('900099', 'USABD', 'US ABDOMEN', 'US1', 'US'), pv1()])


def sc_unmapped_code():
    """An ORC-5 nothing maps. Critical, quarantined, and listed in the studio."""
    acc = '900005'
    send('ORC-5 = ZZ', [msh('RIS', 'UNK-1', 'ORM^O01', ts(170)), pid(),
                        seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'ZZ', 9: ts(170)}),
                        obr(acc), pv1()])


def sc_skipped_rungs():
    """Completion with no arrival or start on record."""
    acc = '900006'
    send('completed, nothing before it', [
        msh('RIS', 'SKIP-1', 'ORM^O01', ts(160)), pid('00999888', 'TEST^PATIENT'),
        seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'CM', 9: ts(160), 19: 'TECH02^X^Y'}),
        obr(acc, 'XRCHEST', 'CHEST XRAY', 'DR_RH', 'DX'),
        seg('PV1', {2: 'O', 3: 'RAD^XR-1^^RH'})])


def sc_stalled():
    """
    Studies left at a rung long enough for the absence sweep to catch them.

    Timestamps are hours back so the seeded thresholds (4h arrived, 3h started,
    24h unreported) actually trip. Run the sweep afterwards to see the findings.
    """
    for acc, code, hours in (('900007', 'AR', 9), ('900008', 'IP', 5),
                             ('900009', 'CM', 30)):
        send(f'{acc} stuck at {code} for {hours}h', [
            msh('RIS', f'STALL-{acc}', 'ORM^O01', ts(hours * 60)),
            pid(f'P{acc}', 'STALL^PATIENT'),
            seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: code, 9: ts(hours * 60)}),
            obr(acc), pv1()])


def sc_placeholder_patient():
    """ER quick registration: birth date 9999-11-11, sex U."""
    send('ADT with placeholder DOB', [
        msh('SAP_HIS', 'QR-ADT', 'ADT^A01', ts(140)),
        seg('PID', {3: 'QR001^^^HIS', 5: 'UNKNOWN^PATIENT', 7: '99991111', 8: 'U'}),
        seg('PV1', {2: 'E', 3: 'EM^BAY2^^RH'})])


def sc_adt_correction():
    """A01 admits with one demographic set, A08 corrects it. Last write wins."""
    send('A01 initial', [msh('SAP_HIS', 'CORR-1', 'ADT^A01', ts(130)),
                         seg('PID', {3: 'C001^^^HIS', 5: 'MISPELT^NAME',
                                     7: '19700101', 8: 'M'}),
                         seg('PV1', {2: 'I', 3: 'W3^301^^RH'})])
    send('A08 correction', [msh('SAP_HIS', 'CORR-2', 'ADT^A08', ts(120)),
                            seg('PID', {3: 'C001^^^HIS', 5: 'CORRECTED^NAME',
                                        7: '19700202', 8: 'F'}),
                            seg('PV1', {2: 'I', 3: 'W3^305^^RH'})])


def sc_malformed():
    """Input that must never crash the listener and must still be archived."""
    for label, raw in (('empty segments', ['']),
                       ('no MSH', [pid()]),
                       ('MSH only', [msh('RIS', 'MAL-1', 'ORM^O01', ts(110))]),
                       ('truncated ORC', [msh('RIS', 'MAL-2', 'ORM^O01', ts(110)), 'ORC|SC']),
                       ('no identifiers', [msh('RIS', 'MAL-3', 'ORM^O01', ts(110)),
                                           seg('ORC', {1: 'SC', 5: 'AR'})]),
                       ('junk payload', [msh('RIS', 'MAL-4', 'ORM^O01', ts(110)),
                                         'ZZZ|\x01\x02 not hl7'])):
        send(f'malformed: {label}', raw)


def sc_multi_study():
    """Two studies interleaved, so per-accession state must not bleed across."""
    a, b = '900010', '900011'
    ea = lifecycle(a, 'MULTI-A', [('SC', 100), ('AR', 90), ('IP', 80), ('CM', 70)])
    eb = lifecycle(b, 'MULTI-B', [('SC', 95), ('AR', 85), ('IP', 75), ('CM', 65)],
                   pat='00777666', ae='MR1', mod='MR', code='MRBRAIN', desc='MRI BRAIN')
    for x, y in zip(ea, eb):
        send(f'A {x[0]}', x[1])
        send(f'B {y[0]}', y[1])


def sc_timezone():
    """Timestamps carrying an offset. They must be stored naive, not shifted."""
    acc = '900012'
    stamp = (datetime.now() - timedelta(minutes=60)).strftime('%Y%m%d%H%M%S') + '+0300'
    send('ORC-9 with +0300 offset', [
        msh('RIS', 'TZ-1', 'ORM^O01', ts(60)), pid(),
        seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: 'AR', 9: stamp}),
        obr(acc), pv1()])


def sc_load(studies=100):
    """Volume. Four events per study, to see where ingestion actually sits."""
    print(f"  sending {studies} studies x 4 events = {studies * 4} messages")
    start = time.time()
    for i in range(studies):
        acc = f'95{i:05d}'
        for code, mins in (('SC', 60), ('AR', 45), ('IP', 40), ('CM', 30)):
            send('', [msh('RIS', f'LOAD-{i}-{code}', 'ORM^O01', ts(mins)),
                      pid(f'LP{i:05d}', 'LOAD^PATIENT'),
                      seg('ORC', {1: 'SC', 3: f'{acc}^HIS', 5: code, 9: ts(mins),
                                  19: 'TECH01^HADDAD^RIMA'}),
                      obr(acc), pv1()], quiet=True)
        if (i + 1) % 25 == 0:
            done = (i + 1) * 4
            print(f"    {done} messages, {done / (time.time() - start):.1f}/s")
    total = time.time() - start
    print(f"  {studies * 4} messages in {total:.1f}s "
          f"({studies * 4 / total:.1f}/s, {total / (studies * 4) * 1000:.0f}ms each)")


SCENARIOS = {
    'happy_path':          sc_happy_path,
    'out_of_order':        sc_out_of_order,
    'time_contradiction':  sc_time_contradiction,
    'duplicates':          sc_duplicates,
    'unmapped_code':       sc_unmapped_code,
    'skipped_rungs':       sc_skipped_rungs,
    'stalled':             sc_stalled,
    'placeholder_patient': sc_placeholder_patient,
    'adt_correction':      sc_adt_correction,
    'malformed':           sc_malformed,
    'multi_study':         sc_multi_study,
    'timezone':            sc_timezone,
}


# ── reset ─────────────────────────────────────────────────────────────────────
#
# Everything derived from HL7 traffic. Deliberately NOT ray7_rules,
# hl7_status_map, hl7_field_mappings, hl7_field_targets or aetitle_modality_map:
# those are seeded CONFIGURATION, not data. Wiping them would silently disable
# screening and field mapping, and the install would look fine while quietly
# classifying nothing.
_RESET_TABLES = [
    'hl7_message_archive', 'ray7_findings', 'hl7_study_events', 'hl7_patients',
    'ray7_study_state', 'hl7_surrogate_keys', 'hl7_orders', 'hl7_oru_reports',
    'hl7_scn_studies', 'etl_didb_studies', 'etl_patient_view', 'etl_orders',
    'std_worklist_arrivals', 'std_worklist_exam_done', 'std_pps',
]


def reset(confirmed):
    if not confirmed:
        print("Refusing to wipe without --confirm.\n\nWould TRUNCATE:")
        for t in _RESET_TABLES:
            print(f"    {t}")
        print("\nConfiguration (ray7_rules, hl7_status_map, hl7_field_mappings,\n"
              "aetitle_modality_map) is NOT touched.")
        return 1
    try:
        import psycopg2
    except ImportError:
        # Reset talks to the database; sending scenarios only needs a socket.
        # So the send half works from a developer host and this half does not,
        # which is worth saying plainly rather than failing on an import.
        print('psycopg2 is not installed in this interpreter.')
        print('')
        print('Reset talks to the database directly, so run it inside the app')
        print('container, where the driver already lives:')
        print('')
        print('  docker compose -f docker-compose.yml -f docker-compose.dev.yml'
              ' exec rayd-app \\')
        print('    python scripts/hl7_scenarios.py --reset --confirm')
        print('')
        print('Sending scenarios needs only a socket, so that half works here.')
        return 2

    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=os.environ.get('POSTGRES_PORT', '5432'),
        user=os.environ.get('POSTGRES_USER', 'etl_user'),
        password=os.environ.get('POSTGRES_PASSWORD', 'etl_pass'),
        dbname=os.environ.get('POSTGRES_DB', 'etl_db'))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE %s RESTART IDENTITY CASCADE" % ', '.join(_RESET_TABLES))
    conn.close()
    print(f"Cleared {len(_RESET_TABLES)} tables. Configuration left intact.")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('scenario', nargs='?', help='scenario name, or "load"')
    ap.add_argument('--list', action='store_true')
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--confirm', action='store_true', help='required by --reset')
    ap.add_argument('--studies', type=int, default=100, help='for load')
    args = ap.parse_args()

    if args.list:
        print("Scenarios:")
        for name, fn in SCENARIOS.items():
            first = (fn.__doc__ or '').strip().split('\n')[0]
            print(f"  {name:22} {first}")
        print(f"  {'load':22} Volume test (--studies N)")
        return 0

    if args.reset:
        return reset(args.confirm)

    if args.all:
        for name, fn in SCENARIOS.items():
            print(f"\n── {name} ──")
            fn()
    elif args.scenario == 'load':
        sc_load(args.studies)
    elif args.scenario in SCENARIOS:
        SCENARIOS[args.scenario]()
    else:
        ap.error(f"unknown scenario {args.scenario!r} — try --list")

    print(f"\n  {_sent['ok']} accepted, {_sent['nak']} rejected, "
          f"{_sent['error']} failed to send")
    return 1 if (_sent['nak'] or _sent['error']) else 0


if __name__ == '__main__':
    sys.exit(main())
