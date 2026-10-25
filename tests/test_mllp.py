"""
The socket: MLLP framing, the ACK, and what a sender we do not control can throw
at port 6661. A real listener thread (hl7_listener.start_mllp_listener) runs on a
free port for the whole session.

The ACK is the contract with the sender. AA means "we hold this message, do not
resend"; AE means "we do NOT hold it, resend". Getting that backwards either loses
messages or wedges the sender's queue on one it will retry forever.
"""
import socket
import threading
import time

import pytest

import hl7kit as k

SB, EB, CR = k.SB, k.EB, k.CR


def _acks(sock, n, timeout=20):
    sock.settimeout(timeout)
    buf, out = b'', []
    while len(out) < n:
        data = sock.recv(65536)
        if not data:
            break
        buf += data
        while SB in buf and EB + CR in buf:
            start, end = buf.index(SB), buf.index(EB + CR)
            out.append(buf[start + 1:end].decode('utf-8', 'replace'))
            buf = buf[end + 2:]
    return out


def exchange(port, payload, n=1, chunks=None, pause=0.0):
    s = socket.create_connection(('127.0.0.1', port), timeout=20)
    try:
        if chunks:
            for c in chunks:
                s.sendall(c)
                time.sleep(pause)
        else:
            s.sendall(payload)
        return _acks(s, n)
    finally:
        s.close()


def msa(ack):
    line = next(l for l in ack.split('\r') if l.startswith('MSA'))
    return line.split('|')


def ack_msh(ack):
    return ack.split('\r')[0].split('|')


# ── the ACK itself ────────────────────────────────────────────────────────────

def test_ack_aa_echoes_the_control_id(pipe, mllp_port):
    raw = k.status('SC', 'A1', k.ts(5), ctrl='CTRL-777')
    [ack] = exchange(mllp_port, k.frame(raw))
    assert msa(ack)[1:3] == ['AA', 'CTRL-777']


def test_ack_swaps_the_applications(pipe, mllp_port):
    [ack] = exchange(mllp_port, k.frame(k.status('SC', 'A1', k.ts(5), app='RIS')))
    h = ack_msh(ack)
    assert (h[2], h[4], h[5], h[8]) == ('RAYD', 'RIS', 'SITE', 'ACK')


def test_ack_means_it_is_stored(pipe, mllp_port):
    [ack] = exchange(mllp_port, k.frame(k.status('SC', 'A1', k.ts(5))))
    assert msa(ack)[1] == 'AA'
    assert pipe.count('hl7_message_archive') == 1
    assert pipe.state('A1') is not None


def test_quarantined_message_is_still_aa(pipe, mllp_port):
    [ack] = exchange(mllp_port, k.frame(k.status('ZZ', 'A1', k.ts(5))))
    assert msa(ack)[1] == 'AA'
    assert pipe.archive()[0]['ray7_status'] == 'quarantined'


def test_archive_failure_is_ae(pipe, mllp_port, monkeypatch):
    from utils import hl7_ingest
    monkeypatch.setattr(hl7_ingest, 'archive', lambda msg: (None, False))
    [ack] = exchange(mllp_port, k.frame(k.status('SC', 'A1', k.ts(5))))
    assert msa(ack)[1] == 'AE'


def test_redelivery_is_aa_and_stored_once(pipe, mllp_port):
    f = k.frame(k.status('SC', 'A1', k.ts(5)))
    acks = exchange(mllp_port, f + f + f, n=3)
    assert [msa(a)[1] for a in acks] == ['AA'] * 3
    assert pipe.count('hl7_message_archive') == 1


# ── framing ───────────────────────────────────────────────────────────────────

def test_two_messages_in_one_packet(pipe, mllp_port):
    a = k.frame(k.status('SC', 'A1', k.ts(5), ctrl='M1'))
    b = k.frame(k.status('SC', 'A2', k.ts(5), ctrl='M2'))
    acks = exchange(mllp_port, a + b, n=2)
    assert [msa(x)[2] for x in acks] == ['M1', 'M2']


def test_message_split_across_many_packets(pipe, mllp_port):
    f = k.frame(k.status('SC', 'A1', k.ts(5), ctrl='SPLIT'))
    chunks = [f[i:i + 37] for i in range(0, len(f), 37)]
    [ack] = exchange(mllp_port, None, chunks=chunks, pause=0.01)
    assert msa(ack)[1:3] == ['AA', 'SPLIT']


def test_end_marker_split_between_packets(pipe, mllp_port):
    f = k.frame(k.status('SC', 'A1', k.ts(5), ctrl='EDGE'))
    [ack] = exchange(mllp_port, None, chunks=[f[:-1], f[-1:]], pause=0.05)
    assert msa(ack)[2] == 'EDGE'


def test_garbage_before_the_start_block_is_skipped(pipe, mllp_port):
    f = b'\r\n junk \x00\x01' + k.frame(k.status('SC', 'A1', k.ts(5), ctrl='G1'))
    [ack] = exchange(mllp_port, f)
    assert msa(ack)[2] == 'G1'
    assert pipe.count('hl7_message_archive') == 1


def test_newline_separated_segments(pipe, mllp_port):
    raw = k.status('SC', 'A1', k.ts(5)).replace('\r', '\n')
    [ack] = exchange(mllp_port, k.frame(raw))
    assert msa(ack)[1] == 'AA' and pipe.state('A1') is not None


def test_empty_frame(pipe, mllp_port):
    [ack] = exchange(mllp_port, SB + EB + CR)
    assert msa(ack)[1] == 'AA'
    assert pipe.count('hl7_message_archive') == 1


def test_a_dropped_connection_does_not_hurt_the_next(pipe, mllp_port):
    s = socket.create_connection(('127.0.0.1', mllp_port))
    s.sendall(SB + b'MSH|^~\\&|RIS|||||20261025||ORM^O01|HALF')
    s.close()
    [ack] = exchange(mllp_port, k.frame(k.status('SC', 'A1', k.ts(5), ctrl='AFTER')))
    assert msa(ack)[2] == 'AFTER'


def test_many_messages_on_one_connection(pipe, mllp_port):
    frames = b''.join(k.frame(k.status('SC', f'A{i}', k.ts(5), ctrl=f'SEQ{i}'))
                      for i in range(50))
    acks = exchange(mllp_port, frames, n=50)
    assert [msa(a)[2] for a in acks] == [f'SEQ{i}' for i in range(50)]
    assert pipe.count('ray7_study_state') == 50


# ── encodings and size ────────────────────────────────────────────────────────

def test_latin1_message(pipe, mllp_port):
    raw = k.adt('A08', name='HÉLÈNE^ZOÉ', pat='LAT1')
    [ack] = exchange(mllp_port, k.frame(raw, encoding='latin-1'))
    assert msa(ack)[1] == 'AA'
    assert pipe.one("SELECT patient_name FROM hl7_patients WHERE patient_id='LAT1'")[
        'patient_name'] == 'ZOÉ HÉLÈNE'


def test_utf8_arabic_name(pipe, mllp_port):
    raw = k.adt('A08', name='حداد^ريما', pat='AR1')
    [ack] = exchange(mllp_port, k.frame(raw))
    assert msa(ack)[1] == 'AA'
    assert pipe.one("SELECT patient_name FROM hl7_patients WHERE patient_id='AR1'")[
        'patient_name'] == 'ريما حداد'


def test_nul_byte_does_not_wedge_the_feed(pipe, mllp_port):
    """Postgres TEXT cannot hold \\x00. If the archive insert fails the listener
    answers AE, the sender resends the same bytes, and the queue is stuck on that
    one message for good. A NUL must be stored, not refused."""
    raw = k.report('A1', k.ts(5), text='before\x00after')
    [ack] = exchange(mllp_port, k.frame(raw))
    assert msa(ack)[1] == 'AA'
    assert pipe.count('hl7_message_archive') == 1


def test_one_megabyte_report(pipe, mllp_port):
    raw = k.report('A1', k.ts(5), text='x' * 1_000_000)
    [ack] = exchange(mllp_port, k.frame(raw))
    assert msa(ack)[1] == 'AA'
    assert pipe.count('hl7_oru_reports') == 1


# ── concurrency at the socket ─────────────────────────────────────────────────

def test_parallel_senders(pipe, mllp_port):
    results, errors = [], []

    def sender(n):
        try:
            frames = b''.join(k.frame(k.status('SC', f'P{n}-{i}', k.ts(5), ctrl=f'P{n}-{i}'))
                              for i in range(5))
            results.extend(msa(a)[1] for a in exchange(mllp_port, frames, n=5))
        except Exception as exc:          # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=sender, args=(n,)) for n in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert errors == []
    assert results.count('AA') == 60
    assert pipe.count('hl7_message_archive') == 60
