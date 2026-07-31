"""
ETL_JOBS/etl_pacs_user_groups.py — PACS MEDILINK.SECM_USERS ⋈ SECM_USER_IN_GROUP ⋈
SECM_GROUPS -> std_pacs_user_groups (LAUMC).

See migration 0087 for the target table and full design notes. Short version: this is
the real PACS reading-permission security-group membership (e.g. "radiologists",
"radiologists@SJ", "residents") behind the "Users / Groups / Profiles" admin screen —
confirmed by the operator to be the reliable role source, unlike RIS's
resource_role_key (see ETL_JOBS/etl_ris_resources.py's docstring — that one was
over-granted to every user as a workaround for an installation-time RIS permissions
bug, and does not reflect real job function).

MEDILINK lives in the same Oracle instance as MEDISTORE (confirmed via ALL_TABLES
catalog query, 2026-07-31) — this uses the same PACS Oracle connection as
etl_didb_studies.py etc., not a separate RIS source.

BUG FIXED 2026-07-31: the original version only queried SECM_USER_IN_GROUP, which
turned out to be nearly empty on real data (1 row total). SECM_USERS.SECURITY_GROUP_ID
-- a direct FK straight to SECM_GROUPS on the user record -- is each user's actual
primary group; SECM_USER_IN_GROUP only covers rare secondary memberships. Both are
pulled now (see _QUERY).

No date/whitelist filter — this is master/reference data (who's in which group right
now), not transactional, and there's no meaningful "new since last run" concept for
group membership; a full pull every run is fine at this table's size (tens to low
hundreds of rows, not millions).
"""
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_SECM_USERS_TABLE         = "MEDILINK.SECM_USERS"
_SECM_GROUPS_TABLE        = "MEDILINK.SECM_GROUPS"
_SECM_USER_IN_GROUP_TABLE = "MEDILINK.SECM_USER_IN_GROUP"

_FETCH_BATCH = 2000


def _safe_str(val, max_len=None):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


_UPSERT_SQL = text("""
    INSERT INTO std_pacs_user_groups (
        membership_dbid, user_dbid, login_id, email, group_dbid, group_name,
        group_domain, last_update
    ) VALUES (
        :membership_dbid, :user_dbid, :login_id, :email, :group_dbid, :group_name,
        :group_domain, :last_update
    )
    ON CONFLICT (membership_dbid) DO UPDATE SET
        user_dbid    = EXCLUDED.user_dbid,    login_id    = EXCLUDED.login_id,
        email        = EXCLUDED.email,        group_dbid  = EXCLUDED.group_dbid,
        group_name   = EXCLUDED.group_name,   group_domain = EXCLUDED.group_domain,
        last_update  = EXCLUDED.last_update
""")

# Two sources, unioned: SECM_USER_IN_GROUP (bridge table -- confirmed near-empty on
# real data, 2026-07-31, only 1 row total) plus SECM_USERS.SECURITY_GROUP_ID (a direct
# FK straight to SECM_GROUPS on the user record itself) -- that direct FK is each
# user's actual primary group; the bridge table only covers rare secondary
# memberships. Negative membership_dbid (-u.DBID) for the SECURITY_GROUP_ID half so it
# can never collide with a real (positive) SECM_USER_IN_GROUP.DBID on upsert.
_QUERY = f"""
    SELECT
        uig.DBID, u.DBID, u.LOGIN_ID, u.EMAIL, g.DBID, g.GROUP_NAME, g.DOMAIN
    FROM {_SECM_USER_IN_GROUP_TABLE} uig
    JOIN {_SECM_USERS_TABLE}  u ON u.DBID = uig.USER_DBID
    JOIN {_SECM_GROUPS_TABLE} g ON g.DBID = uig.GROUP_DBID

    UNION ALL

    SELECT
        -u.DBID, u.DBID, u.LOGIN_ID, u.EMAIL, g.DBID, g.GROUP_NAME, g.DOMAIN
    FROM {_SECM_USERS_TABLE} u
    JOIN {_SECM_GROUPS_TABLE} g ON g.DBID = u.SECURITY_GROUP_ID
    WHERE u.SECURITY_GROUP_ID IS NOT NULL
"""


def run_pacs_user_groups_etl(pg_engine, oracle_source):
    job_name   = "PACS_USER_GROUPS_ETL"
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
        logging.error(f"PACS User Groups ETL log error: {e}")

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("PACS User Groups ETL starting")
        print(f"[PACS User Groups ETL] 🚀 Starting ({_SECM_USER_IN_GROUP_TABLE} ⋈ "
              f"{_SECM_USERS_TABLE} ⋈ {_SECM_GROUPS_TABLE})")

        cursor.execute(_QUERY)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (membership_dbid, user_dbid, login_id, email,
                 group_dbid, group_name, group_domain) = row

                if membership_dbid is None:
                    skipped += 1
                    continue

                params.append({
                    "membership_dbid": membership_dbid, "user_dbid": user_dbid,
                    "login_id": _safe_str(login_id), "email": _safe_str(email),
                    "group_dbid": group_dbid, "group_name": _safe_str(group_name),
                    "group_domain": _safe_str(group_domain),
                    "last_update": datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        print(f"[PACS User Groups ETL] ✅ {total:,} memberships upserted, {skipped} skipped (no key)")
        status = "SUCCESS"
        logging.info(f"PACS User Groups ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"PACS User Groups ETL error: {error_msg}")
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
                logging.error(f"Failed to update PACS User Groups log: {le}")

    return total
