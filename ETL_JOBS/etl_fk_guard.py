"""
ETL_JOBS/etl_fk_guard.py — which of a batch's parent IDs Postgres already holds.

Phases 2, 3 and 4 drop rows whose parent row is missing, so one orphan cannot make a foreign
key reject a whole batch. They used to load the entire parent table for that check, and the raw
images table grows by about 1.2M rows a night. On LAUMC that load held the web worker, which
also runs the ETL, past gunicorn's --timeout 300, and gunicorn killed it mid-phase on 2026-11-06
and 2026-11-07 ("WORKER TIMEOUT"), so no later phase ran.

Asking only for the IDs in the batch gives the same answer at any table size, through the
parent's primary key.
"""
from sqlalchemy import text


def existing_ids(pg_engine, table, column, ids):
    """The IDs from `ids` present in table.column. table and column are fixed names from the
    calling code, never input. An ID that is not a whole number (PACS has held corrupt keys
    such as "R3") cannot be a parent, so it is left out, as the whole-table check left it out."""
    wanted = set()
    for value in ids:
        try:
            wanted.add(int(value))
        except (TypeError, ValueError):
            continue
    if not wanted:
        return set()
    with pg_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT {column} FROM {table} WHERE {column} = ANY(CAST(:ids AS bigint[]))"),
                            {"ids": sorted(wanted)})
        return {r[0] for r in rows}
