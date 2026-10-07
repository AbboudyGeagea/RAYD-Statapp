"""
backfill_ordering_physician_code.py
────────────────────────────────────────────────────────────────
One-time backfill for CRN: re-parses the ordering doctor's code (ORC-12
component 1, OBR-16 fallback, e.g. "ID2" from "ID2^^Jihad Falou") out of
hl7_orders.raw_message for orders received before hl7_listener.py kept it
(migration 0055). Uses the listener's own parse_orm_o01(), so old and new orders
are parsed exactly the same way.

Same shape as backfill_orc_start_datetime.py: does not boot the Flask app (that
would start the scheduler and bind the MLLP port of the running app), uses a plain
psycopg2 connection with the POSTGRES_* env vars, commits in batches of 500.

Safe to re-run: only touches rows where ordering_physician_code IS NULL.

Usage:
    docker compose exec rayd-app python backfill_ordering_physician_code.py
"""
import os
import sys
import psycopg2

from hl7_listener import parse_orm_o01

BATCH_SIZE = 500


def _connect():
    return psycopg2.connect(
        user=os.environ.get("POSTGRES_USER", "etl_user"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        host=os.environ.get("POSTGRES_HOST", "db"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "etl_db"),
    )


def backfill():
    conn = _connect()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        SELECT id, raw_message FROM hl7_orders
        WHERE ordering_physician_code IS NULL AND raw_message IS NOT NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[Backfill] {total:,} hl7_orders row(s) missing ordering_physician_code, checking raw_message...")

    matched = 0
    checked = 0
    batch = []

    def flush(batch):
        if not batch:
            return
        cur.executemany(
            "UPDATE hl7_orders SET ordering_physician_code = %s WHERE id = %s",
            batch,
        )
        conn.commit()

    for row_id, raw_message in rows:
        checked += 1
        parsed = parse_orm_o01(raw_message)
        if parsed and parsed.get("ordering_physician_code"):
            batch.append((parsed["ordering_physician_code"], row_id))
            matched += 1
        if len(batch) >= BATCH_SIZE:
            flush(batch)
            print(f"[Backfill] {checked:,}/{total:,} checked, {matched:,} matched so far...")
            batch = []

    flush(batch)
    cur.close()
    conn.close()

    print(f"[Backfill] Done — {matched:,}/{total:,} orders had a doctor code, now filled in.")
    return matched, total


if __name__ == "__main__":
    matched, total = backfill()
    sys.exit(0)
