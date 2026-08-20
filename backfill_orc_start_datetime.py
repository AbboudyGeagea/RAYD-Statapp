"""
backfill_orc_start_datetime.py
────────────────────────────────────────────────────────────────
One-time backfill: re-parses ORC-7.4 (Quantity/Timing, Start date/time) out of
hl7_orders.raw_message for every order received BEFORE hl7_listener.py's
orc_start_datetime parsing existed (migration 0052). The raw HL7 text has always
been stored (hl7_orders.raw_message), so this data doesn't need to wait for new
orders to arrive — it's already sitting in Postgres.

Deliberately does NOT boot the Flask app (no `from app import create_app`):
create_app() unconditionally starts the APScheduler and tries to bind the MLLP
listener on port 6661, which collides with the real app process already running
in the container. This script only needs parse_orm_o01() (pure stdlib, no
Flask/DB imports at module level) and a plain psycopg2 connection using the same
POSTGRES_* env vars config.py reads — no app context needed.

Batched commits (500 rows/batch), not one giant transaction.

Safe to re-run: only touches rows where orc_start_datetime IS NULL.

Usage:
    docker compose exec rayd-app python backfill_orc_start_datetime.py
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
        WHERE orc_start_datetime IS NULL AND raw_message IS NOT NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[Backfill] {total:,} hl7_orders row(s) missing orc_start_datetime, checking raw_message...")

    matched = 0
    checked = 0
    batch = []

    def flush(batch):
        if not batch:
            return
        cur.executemany(
            "UPDATE hl7_orders SET orc_start_datetime = %s WHERE id = %s",
            batch,
        )
        conn.commit()

    for row_id, raw_message in rows:
        checked += 1
        parsed = parse_orm_o01(raw_message)
        if parsed and parsed.get("orc_start_datetime"):
            batch.append((parsed["orc_start_datetime"], row_id))
            matched += 1
        if len(batch) >= BATCH_SIZE:
            flush(batch)
            print(f"[Backfill] {checked:,}/{total:,} checked, {matched:,} matched so far...")
            batch = []

    flush(batch)
    cur.close()
    conn.close()

    print(f"[Backfill] Done — {matched:,}/{total:,} orders had an ORC-7.4 start time, now filled in.")
    return matched, total


if __name__ == "__main__":
    matched, total = backfill()
    sys.exit(0)
