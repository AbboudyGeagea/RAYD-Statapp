"""
ETL_JOBS/etl_ris_ordering_org.py — RIS ORDERING_ORGANIZATION -> std_ordering_organizations
(LAUMC).

See migration 0062. Resolves ORDERS.ordering_organization_key (already captured on
std_orders) to a real referring clinic/organization — name, address, phone/fax. Small
reference/catalog table, same style as etl_ris_modality.py: full pull, no date filter,
refresh-on-conflict (no RAYD-owned fields here to protect).

Forward-looking — not wired to anything else yet ("we might need it later"), but the
data lands now so the join is a one-line addition whenever a report wants it.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_ORDERING_ORG_TABLE = os.getenv("RAYD_RIS_ORDERING_ORG_TABLE", "ORDERING_ORGANIZATION")
_FETCH_BATCH = 2000

_UPSERT_SQL = text("""
    INSERT INTO std_ordering_organizations (
        ordering_organization_key, code, name, active, coding_scheme, alternate_code,
        street_address, other_designation, city, province, country, postal_code,
        phone_number, fax_number, edi_location_number, edi_send, source_last_updated,
        last_update
    ) VALUES (
        :ordering_organization_key, :code, :name, :active, :coding_scheme, :alternate_code,
        :street_address, :other_designation, :city, :province, :country, :postal_code,
        :phone_number, :fax_number, :edi_location_number, :edi_send, :source_last_updated,
        :last_update
    )
    ON CONFLICT (ordering_organization_key) DO UPDATE SET
        code = EXCLUDED.code, name = EXCLUDED.name, active = EXCLUDED.active,
        coding_scheme = EXCLUDED.coding_scheme, alternate_code = EXCLUDED.alternate_code,
        street_address = EXCLUDED.street_address, other_designation = EXCLUDED.other_designation,
        city = EXCLUDED.city, province = EXCLUDED.province, country = EXCLUDED.country,
        postal_code = EXCLUDED.postal_code, phone_number = EXCLUDED.phone_number,
        fax_number = EXCLUDED.fax_number, edi_location_number = EXCLUDED.edi_location_number,
        edi_send = EXCLUDED.edi_send, source_last_updated = EXCLUDED.source_last_updated,
        last_update = EXCLUDED.last_update
""")


def _safe_str(val, max_len=None):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def run_ris_ordering_org_etl(pg_engine, oracle_source):
    job_name   = "RIS_ORDERING_ORG_ETL"
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
        logging.error(f"RIS Ordering Org ETL log error: {e}")

    query = f"""
        SELECT
            ORDERING_ORGANIZATION_KEY, CODE, NAME, ACTIVE, CODING_SCHEME, ALTERNATE_CODE,
            STREET_ADDRESS, OTHER_DESIGNATION, CITY, PROVINCE, COUNTRY, POSTAL_CODE,
            PHONE_NUMBER, FAX_NUMBER, EDI_LOCATION_NUMBER, EDI_SEND, LAST_UPDATED
        FROM {_ORDERING_ORG_TABLE}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Ordering Organization ETL starting")
        print(f"[RIS Ordering Org ETL] 🚀 Starting ({_ORDERING_ORG_TABLE})")

        cursor.execute(query)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (org_key, code, name, active_flag, coding_scheme, alt_code,
                 street, other_desig, city, province, country, postal,
                 phone, fax, edi_loc, edi_send, last_updated) = row

                if org_key is None:
                    skipped += 1
                    continue

                active = None
                if active_flag is not None:
                    active = str(active_flag).strip().upper() in ('Y', 'YES', 'TRUE', '1')

                params.append({
                    "ordering_organization_key": org_key,
                    "code":               _safe_str(code),
                    "name":               _safe_str(name),
                    "active":             active,
                    "coding_scheme":      _safe_str(coding_scheme),
                    "alternate_code":     _safe_str(alt_code),
                    "street_address":     _safe_str(street),
                    "other_designation":  _safe_str(other_desig),
                    "city":               _safe_str(city),
                    "province":           _safe_str(province),
                    "country":            _safe_str(country),
                    "postal_code":        _safe_str(postal),
                    "phone_number":       _safe_str(phone),
                    "fax_number":         _safe_str(fax),
                    "edi_location_number": _safe_str(edi_loc),
                    "edi_send":           _safe_str(edi_send),
                    "source_last_updated": last_updated,
                    "last_update":         datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        status = "SUCCESS"
        print(f"[RIS Ordering Org ETL] ✅ Done — {total:,} organizations upserted, {skipped} skipped (no key)")
        logging.info(f"RIS Ordering Org ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Ordering Org ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Ordering Org log: {le}")

    return total
