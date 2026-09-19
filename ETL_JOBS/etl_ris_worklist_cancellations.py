"""
ETL_JOBS/etl_ris_worklist_cancellations.py — RIS WORKLIST_STATUS_HISTORY (cancellation
status keys) ⋈ SITE_WORKLIST -> std_worklist_cancellations (LAUMC).

See migration 0122 for the target table, the Oracle evidence behind it, and why the
last_update proxy it replaces was not good enough.

Same table/source/pattern as its Phase 17 siblings (etl_ris_worklist_arrivals.py
status_key=60, etl_ris_worklist_exam_done.py status_key=100,
etl_ris_worklist_scheduled.py status_key=40), with ONE structural difference: those pull
a single hardcoded status, this pulls a SET of them, and which statuses belong to the set
is read from worklist_status_map (is_cancel OR stage = 'discontinued') rather than
hardcoded here. Migration 0047 makes that a standing rule -- "RAYD must map via this
table, NEVER by hardcoding raw keys -- new custom statuses appear over time" -- and it
matters more here than anywhere: a cancellation flavour added to the RIS next year starts
being collected on the next run, with no code change and no silent gap in the export.

Because the status set is dynamic, status_key is carried into the target table and forms
part of its natural key. An order can be cancelled, reinstated and cancelled again; every
transition is its own row and consumers take MAX(cancelled_at).

Feeds routes/report_27.py's cancelled-exam CSV, which falls back to the old
etl_orders.last_update proxy per row where no transition is found -- so this job being
absent, skipped or behind degrades the export's precision, never its row set.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

_STATUS_HISTORY_TABLE = "WORKLIST_STATUS_HISTORY"
_WORKLIST_TABLE       = "SITE_WORKLIST"

# Fallback only — used when worklist_status_map cannot be read at all (missing table, or
# a DB error). These are the keys migration 0047 seeded as cancellations, so a degraded
# run still collects the right statuses rather than collecting nothing. The map is always
# preferred; this exists so a lookup failure does not silently empty the export's
# precision for everyone.
_FALLBACK_CANCEL_STATUS_KEYS = [20, 30, 50, 90, 1300, 1702, 1742, 1830, 2422]

# Oracle IN-list chunking. The cancellation set is ~9 keys today and realistically will
# never approach Oracle's 1000-expression IN limit, but the set is data-driven now, so
# the guard is cheap insurance against a map someone extends carelessly.
_MAX_IN_LIST = 900

_FETCH_BATCH = 2000


def _safe_date(val):
    if val is None:
        return None
    try:
        return val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


def _load_cancel_status_keys(pg_engine):
    """Cancellation status keys, from worklist_status_map (migration 0047).

    'discontinued' is included alongside is_cancel because that is exactly the set
    ETL_JOBS/etl_orders.py _translate_order_status collapses into order_status='CA' --
    i.e. the set report_27's Cancelled bar draws and its CSV exports. Collecting a
    narrower set here would leave part of that export on the proxy timestamp for no
    stated reason.
    """
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT status_key FROM worklist_status_map "
                "WHERE is_cancel = TRUE OR stage = 'discontinued' "
                "ORDER BY status_key"
            )).fetchall()
        keys = [int(r[0]) for r in rows if r[0] is not None]
        if keys:
            return keys
        logging.warning("RIS Worklist Cancellations ETL: worklist_status_map has no "
                        "cancellation rows — falling back to the 0047 seed list")
    except Exception as e:
        logging.warning(f"RIS Worklist Cancellations ETL: could not read "
                        f"worklist_status_map ({e}) — falling back to the 0047 seed list")
    return list(_FALLBACK_CANCEL_STATUS_KEYS)


_UPSERT_SQL = text("""
    INSERT INTO std_worklist_cancellations (
        site_worklist_key, status_key, cancelled_at, last_update
    ) VALUES (
        :site_worklist_key, :status_key, :cancelled_at, :last_update
    )
    ON CONFLICT (site_worklist_key, status_key, cancelled_at) DO UPDATE SET
        last_update = EXCLUDED.last_update
""")


def run_ris_worklist_cancellations_etl(pg_engine, oracle_source):
    job_name   = "RIS_WORKLIST_CANCELLATIONS_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"RIS Worklist Cancellations ETL log error: {e}")

    cancel_keys = _load_cancel_status_keys(pg_engine)

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text(
                "SELECT MAX(cancelled_at) FROM std_worklist_cancellations"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Worklist Cancellations ETL: could not read watermark, "
                        f"falling back to full pull: {e}")
        watermark = None

    is_fresh_load = watermark is None
    # Same 10-day lookback as the sibling jobs: STATUS_TIME can land slightly behind the
    # watermark when a transition is committed late, and re-reading a window is free
    # given the upsert is idempotent on the natural key.
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        # Bind each status key individually (:k0, :k1, ...) rather than interpolating the
        # list — same reason every other Oracle query in this package binds: the values
        # come from a database table, and a bound list cannot become SQL.
        for chunk_start in range(0, len(cancel_keys), _MAX_IN_LIST):
            chunk = cancel_keys[chunk_start:chunk_start + _MAX_IN_LIST]
            key_binds = {f"k{i}": k for i, k in enumerate(chunk)}
            in_list = ", ".join(f":{name}" for name in key_binds)

            base_query = f"""
                SELECT wsh.SITE_WORKLIST_KEY, wsh.STATUS_KEY, wsh.STATUS_TIME
                FROM {_STATUS_HISTORY_TABLE} wsh
                JOIN {_WORKLIST_TABLE} sw ON sw.SITE_WORKLIST_KEY = wsh.SITE_WORKLIST_KEY
                WHERE wsh.STATUS_KEY IN ({in_list})
            """

            params = dict(key_binds)
            if is_fresh_load:
                logging.info(f"RIS Worklist Cancellations ETL starting — fresh load, "
                             f"status keys={chunk}")
                print(f"[RIS Worklist Cancellations ETL] 🚀 Starting "
                      f"({_STATUS_HISTORY_TABLE} ⋈ {_WORKLIST_TABLE}), fresh load, "
                      f"{len(chunk)} cancellation status key(s)")
                cursor.execute(base_query, params)
            else:
                logging.info(f"RIS Worklist Cancellations ETL starting — incremental, "
                             f"watermark={watermark}, lookback={lookback_date}, status keys={chunk}")
                print(f"[RIS Worklist Cancellations ETL] 🚀 Starting — incremental, "
                      f"watermark={watermark}, lookback={lookback_date}")
                params["watermark"] = watermark
                params["lb"] = lookback_date
                cursor.execute(
                    base_query + " AND (wsh.STATUS_TIME > :watermark "
                                 "OR wsh.STATUS_TIME >= TO_DATE(:lb, 'YYYY-MM-DD'))",
                    params
                )

            while True:
                batch = cursor.fetchmany(_FETCH_BATCH)
                if not batch:
                    break
                rows = []
                for (site_worklist_key, status_key, status_time) in batch:
                    cancelled_at = _safe_date(status_time)
                    if site_worklist_key is None or status_key is None or cancelled_at is None:
                        skipped += 1
                        continue
                    rows.append({
                        "site_worklist_key": site_worklist_key,
                        "status_key": int(status_key),
                        "cancelled_at": cancelled_at,
                        "last_update": datetime.now(),
                    })
                if rows:
                    with pg_engine.begin() as conn:
                        conn.execute(_UPSERT_SQL, rows)
                    total += len(rows)

        print(f"[RIS Worklist Cancellations ETL] ✅ {total:,} cancellation events upserted, "
              f"{skipped} skipped (no key/time)")
        status = "SUCCESS"
        logging.info(f"RIS Worklist Cancellations ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Worklist Cancellations ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "null_alerts=:na, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Worklist Cancellations log: {le}")

    return total
