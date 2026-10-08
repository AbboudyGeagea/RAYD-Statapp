"""
crn_send_test_report.py
────────────────────────────────────────────────────────────────
CRN dry-run workflow test. Sends one fake order (ORM^O01) and its report
(ORU^R01, containing the CRN marker) to RAYD's own HL7 listener, the same way
the RIS does, then follows the notification through detection (NLP worker) and
routing (dispatcher) and prints where to see it.

Refuses to send unless:
  - the SMS provider is the dry run ('log'): with a real provider the test would
    text a real phone
  - CRN is switched on (Admin > CRN > Settings), otherwise the marker is ignored
  - the CRN gateway is configured, otherwise the page cannot be opened
  - the test contact exists, is active and has a mobile number

The fake patient is MRN CRNTEST, name TEST^CRN, accession CRNTEST<date-time>,
ordered by the test contact. It stays in the HL7 tables and in the CRN history
(append-only by design).

Does not boot the Flask app (that would start the scheduler and bind the MLLP
port of the running app); uses a plain psycopg2 connection with the POSTGRES_*
env vars, like backfill_ordering_physician_code.py.

Usage:
    sudo docker exec rayd_service python crn_send_test_report.py          # contact TEST1
    sudo docker exec rayd_service python crn_send_test_report.py GP42     # another contact
"""
import os
import socket
import sys
import time
from datetime import datetime

import psycopg2

HL7_HOST = os.environ.get('CRN_TEST_HL7_HOST', '127.0.0.1')
HL7_PORT = int(os.environ.get('CRN_TEST_HL7_PORT', '6661'))
MLLP_START, MLLP_END = b'\x0b', b'\x1c\x0d'
DETECT_TIMEOUT_S = 300   # the NLP worker polls every 60 s
ROUTE_TIMEOUT_S = 120    # the dispatcher runs every 30 s


def _connect():
    return psycopg2.connect(
        user=os.environ.get("POSTGRES_USER", "etl_user"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        host=os.environ.get("POSTGRES_HOST", "db"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "etl_db"),
    )


def _one(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    conn.rollback()   # read-only; never hold a transaction open while waiting
    return row


def _stop(message):
    print(f"\nSTOP: {message}\nNothing was sent.")
    sys.exit(1)


def _segment(name, fields):
    """Build a segment from {index: value}, index as split('|') counts it (the
    listener's _field numbering; for MSH that is the HL7 number minus one)."""
    parts = [name] + [''] * max(fields)
    for i, v in fields.items():
        parts[i] = v
    return '|'.join(parts)


def _clean(value):
    return ''.join(ch for ch in (value or '') if ch not in '|^~\\&\r\n')


def _send(message):
    with socket.create_connection((HL7_HOST, HL7_PORT), timeout=15) as s:
        s.sendall(MLLP_START + message.encode('utf-8') + MLLP_END)
        data = b''
        while MLLP_END not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
    return data.strip(b'\x0b\x1c\r\n').decode('utf-8', 'replace')


def _wait(conn, sql, params, timeout, label):
    print(f"  waiting for {label} (up to {timeout // 60} min)", end='', flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = _one(conn, sql, params)
        if row and row[0] is not None:
            print(" done")
            return row
        print('.', end='', flush=True)
        time.sleep(5)
    print(" not yet")
    return None


def main():
    code = (sys.argv[1] if len(sys.argv) > 1 else 'TEST1').strip().upper()
    conn = _connect()

    with conn.cursor() as cur:
        cur.execute("SELECT key, value FROM settings WHERE key LIKE 'crn\\_%'")
        cfg = {k: (v or '').strip() for k, v in cur.fetchall()}
    conn.rollback()

    print("Checks")
    if cfg.get('crn_sms_provider', 'log') != 'log':
        _stop(f"an SMS provider is connected ({cfg['crn_sms_provider']}): this test would text a real phone.")
    print("  OK  SMS provider is the dry run: nothing will be texted")
    if not cfg.get('crn_live_since'):
        _stop("CRN is OFF. Switch it on first: Admin > CRN > Settings > Switch on.")
    print(f"  OK  CRN is ON since {cfg['crn_live_since'].replace('T', ' ')}")
    if not cfg.get('crn_gateway_url') or not cfg.get('crn_gateway_api_key'):
        _stop("the CRN gateway is not set: Admin > CRN > Settings, box 'CRN gateway'.")
    print(f"  OK  CRN gateway is set: {cfg['crn_gateway_url']}")
    marker = cfg.get('crn_marker') or 'CRN'
    contact = _one(conn, "SELECT full_name, phone_e164, active FROM crn_contacts WHERE doctor_code = %s", (code,))
    if not contact:
        _stop(f"no contact with doctor code {code}: add it in Admin > CRN > Contacts > Add.")
    if not contact[2]:
        _stop(f"contact {code} is not active: tick 'Active' in Admin > CRN > Contacts.")
    if not contact[1]:
        _stop(f"contact {code} has no usable mobile number: fix it in Admin > CRN > Contacts.")
    doctor = f"{_clean(code)}^^{_clean(contact[0]) or 'Test Doctor'}"
    print(f"  OK  test doctor {code} {contact[0] or ''} ({contact[1]})")

    now = datetime.now()
    ts = now.strftime('%Y%m%d%H%M%S')
    acc = f"CRNTEST{now:%y%m%d%H%M%S}"
    pid = _segment('PID', {1: '1', 3: 'CRNTEST^^^TEST', 5: 'TEST^CRN'})
    proc = 'CRNTEST^CRN workflow test'

    orm = '\r'.join([
        _segment('MSH', {1: '^~\\&', 2: 'RAYD_CRN_TEST', 3: 'RAYD', 4: 'RAYD', 5: 'RAYD', 6: ts,
                         8: 'ORM^O01', 9: f'CRNTEST-ORM-{ts}', 10: 'P', 11: '2.3'}),
        pid,
        _segment('PV1', {1: '1', 2: 'O', 3: 'CRNTEST'}),
        _segment('ORC', {1: 'NW', 2: acc, 3: acc, 5: 'SC', 7: f'^^^{ts}', 9: ts, 12: doctor}),
        _segment('OBR', {1: '1', 2: acc, 3: acc, 4: proc, 7: ts, 16: doctor, 24: 'OT'}),
    ]) + '\r'
    oru = '\r'.join([
        _segment('MSH', {1: '^~\\&', 2: 'RAYD_CRN_TEST', 3: 'RAYD', 4: 'RAYD', 5: 'RAYD', 6: ts,
                         8: 'ORU^R01', 9: f'CRNTEST-ORU-{ts}', 10: 'P', 11: '2.3'}),
        pid,
        _segment('OBR', {1: '1', 2: acc, 3: acc, 4: proc, 7: ts, 16: doctor, 22: ts, 24: 'OT', 25: 'F',
                         32: 'CRNTEST'}),
        _segment('OBX', {1: '1', 2: 'TX', 3: 'REPORT^Report', 5: 'TEST REPORT - NOT A REAL PATIENT.', 11: 'F'}),
        _segment('OBX', {1: '2', 2: 'TX', 3: 'REPORT^Report',
                         5: f'{marker} This tests the critical result notification workflow. No action is needed.',
                         11: 'F'}),
    ]) + '\r'

    print(f"\nSending to RAYD's HL7 listener ({HL7_HOST}:{HL7_PORT}), accession {acc}")
    for label, message in (('order (ORM)', orm), ('report (ORU)', oru)):
        try:
            ack = _send(message)
        except OSError as e:
            print(f"\nSTOP: could not reach the HL7 listener: {e}")
            sys.exit(1)
        if 'MSA|AA' not in ack:
            print(f"\nSTOP: the listener did not accept the {label}. Its answer: {ack!r}")
            sys.exit(1)
        print(f"  OK  {label} accepted")
        if label.startswith('order'):
            row = _wait(conn, "SELECT COALESCE(ordering_physician_code, '') FROM hl7_orders "
                              "WHERE accession_number = %s", (acc,), 30, 'the order to be stored')
            if not row:
                _stop("the order was accepted but not stored. Check: sudo docker logs rayd_service --tail 30")
            if row[0] != code:
                print(f"  !!  the order was stored with doctor code {row[0]!r}, not {code!r} "
                      f"(an HL7 field map override?). The CRN will route to {row[0]!r}.")
            time.sleep(1)   # the report must arrive after its order, as from the RIS

    print("\nFollowing the notification")
    row = _wait(conn, """SELECT n.id, n.ref_code FROM crn_notifications n
                         JOIN hl7_oru_reports r ON r.id = n.report_id WHERE r.accession_number = %s""",
                (acc,), DETECT_TIMEOUT_S, 'the NLP worker to detect the marker')
    if not row:
        print(f"\nThe report is in, but the marker was not detected yet. The NLP worker may be busy.\n"
              f"Check the CRN board in a few minutes for accession {acc}, or: sudo docker logs rayd_nlp --tail 20")
        sys.exit(1)
    nid, ref = row
    print(f"  OK  notification {ref} created")
    routed = _wait(conn, """SELECT MAX(r.page_pushed_at) FROM crn_recipients r WHERE r.notification_id = %s""",
                   (nid,), ROUTE_TIMEOUT_S, 'routing: page placed on the CRN gateway, SMS recorded')
    status = _one(conn, "SELECT status FROM crn_notifications WHERE id = %s", (nid,))[0]
    last = _one(conn, "SELECT event_type, detail->>'error' FROM crn_events WHERE notification_id = %s "
                      "ORDER BY id DESC LIMIT 1", (nid,))
    if routed:
        print(f"  OK  status: {status}")
    else:
        print(f"  !!  not routed yet. Status: {status}. Last event: {last[0] if last else '-'}"
              f"{' (' + last[1] + ')' if last and last[1] else ''}")

    print(f"\nNext: open RAYD in the browser at  /admin/crn/notification/{nid}")
    print("      and click 'Open link (dry run)' to see the page as the doctor and acknowledge it.")
    conn.close()


if __name__ == '__main__':
    main()
