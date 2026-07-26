"""
ETL_JOBS/etl_ris_resources.py — RIS RESOURCE_ID ⋈ PERSON -> std_resources_ris (LAUMC),
plus a reading/signing-physician enrichment pass on etl_didb_studies.

See migration 0063 for the target table and design notes. Unlike std_patients_ris,
THIS table carries names + contact info — the "no patient names" rule is specifically
about patient PHI; staff/physician identity is operationally required (KPI report
needs radiologist names, CRN needs referring-physician email/phone).

RESOURCE_ID.RESOURCE_ID_KEY is what REPORT/ORDERS/SITE_WORKLIST's *_RESOURCE_ID_KEY
columns actually reference (vendor-confirmed) — this loader captures that key so those
tables can resolve identity once built. RESOURCE_ID.RESOURCE_ID (the external composite
string, e.g. "kamal.tarabine@ad.umcrh.com_841630390") is the same format already sitting
in etl_didb_studies.reading_physician_id / .signing_physician_id — the enrichment step
below closes that loop on data already loaded.

site_id resolved via site_org_map (migration 0052) — RESOURCE_ID carries a real
org_structure_key, unlike VISIT.

RESOURCE_ROLE_KEY pulled RAW — no role lookup table provided yet.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_RESOURCE_ID_TABLE = os.getenv("RAYD_RIS_RESOURCE_ID_TABLE", "RESOURCE_ID")
_PERSON_TABLE       = os.getenv("RAYD_RIS_PERSON_TABLE", "PERSON")
_FETCH_BATCH = 2000

_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)


def _safe_date(val):
    if val is None:
        return None
    try:
        dt = val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
        if dt.year == 0 or not (_SAFE_DATE_MIN <= dt <= _SAFE_DATE_MAX):
            return None
        return dt
    except Exception:
        return None


def _safe_str(val, max_len=None):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _load_site_org_map(pg_engine):
    """Preload org_structure_key -> site_id (migration 0052). Never raises."""
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text("SELECT org_structure_key, site_id FROM site_org_map")).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logging.warning(f"RIS Resources ETL: could not load site_org_map ({e}) — site_id will be NULL")
        return {}


_UPSERT_SQL = text("""
    INSERT INTO std_resources_ris (
        resource_id_key, person_key, resource_role_key, resource_id, assigning_authority,
        org_structure_key, site_id, pref_rpt_delivery_method_key, pref_img_delivery_method_key,
        active, alternate_id, source_last_updated, last_name, first_name, middle_name,
        name_prefix, name_suffix, name_representation_code, common_name, mobile_phone_number,
        pager_number, primary_email_address, secondary_email_address, deleted, deleted_date,
        person_last_updated, last_update
    ) VALUES (
        :resource_id_key, :person_key, :resource_role_key, :resource_id, :assigning_authority,
        :org_structure_key, :site_id, :pref_rpt_delivery_method_key, :pref_img_delivery_method_key,
        :active, :alternate_id, :source_last_updated, :last_name, :first_name, :middle_name,
        :name_prefix, :name_suffix, :name_representation_code, :common_name, :mobile_phone_number,
        :pager_number, :primary_email_address, :secondary_email_address, :deleted, :deleted_date,
        :person_last_updated, :last_update
    )
    ON CONFLICT (resource_id_key) DO UPDATE SET
        person_key = EXCLUDED.person_key, resource_role_key = EXCLUDED.resource_role_key,
        resource_id = EXCLUDED.resource_id, assigning_authority = EXCLUDED.assigning_authority,
        org_structure_key = EXCLUDED.org_structure_key, site_id = EXCLUDED.site_id,
        pref_rpt_delivery_method_key = EXCLUDED.pref_rpt_delivery_method_key,
        pref_img_delivery_method_key = EXCLUDED.pref_img_delivery_method_key,
        active = EXCLUDED.active, alternate_id = EXCLUDED.alternate_id,
        source_last_updated = EXCLUDED.source_last_updated, last_name = EXCLUDED.last_name,
        first_name = EXCLUDED.first_name, middle_name = EXCLUDED.middle_name,
        name_prefix = EXCLUDED.name_prefix, name_suffix = EXCLUDED.name_suffix,
        name_representation_code = EXCLUDED.name_representation_code,
        common_name = EXCLUDED.common_name, mobile_phone_number = EXCLUDED.mobile_phone_number,
        pager_number = EXCLUDED.pager_number, primary_email_address = EXCLUDED.primary_email_address,
        secondary_email_address = EXCLUDED.secondary_email_address, deleted = EXCLUDED.deleted,
        deleted_date = EXCLUDED.deleted_date, person_last_updated = EXCLUDED.person_last_updated,
        last_update = EXCLUDED.last_update
""")

# Reading/signing physician resolution — matches the composite resource_id string
# already sitting in etl_didb_studies (migration 0056 widened both to TEXT for this
# exact format). IS NULL guarded: resource_id values are stable once minted, so this
# doesn't need to redo work on every run — only newly-appearing matches get resolved.
_ENRICH_READING_SQL = text("""
    UPDATE etl_didb_studies s
    SET reading_physician_resource_key = r.resource_id_key
    FROM std_resources_ris r
    WHERE r.resource_id = s.reading_physician_id
      AND s.reading_physician_resource_key IS NULL
""")

_ENRICH_SIGNING_SQL = text("""
    UPDATE etl_didb_studies s
    SET signing_physician_resource_key = r.resource_id_key
    FROM std_resources_ris r
    WHERE r.resource_id = s.signing_physician_id
      AND s.signing_physician_resource_key IS NULL
""")


def run_ris_resources_etl(pg_engine, oracle_source):
    job_name   = "RIS_RESOURCES_ETL"
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
        logging.error(f"RIS Resources ETL log error: {e}")

    site_map = _load_site_org_map(pg_engine)

    query = f"""
        SELECT
            r.RESOURCE_ID_KEY, r.PERSON_KEY, r.RESOURCE_ROLE_KEY, r.RESOURCE_ID,
            r.ASSIGNING_AUTHORITY, r.ORG_STRUCTURE_KEY, r.LAST_UPDATED,
            r.PREF_RPT_DELIVERY_METHOD_KEY, r.PREF_IMG_DELIVERY_METHOD_KEY, r.ACTIVE,
            r.ALTERNATE_ID,
            per.LAST_NAME, per.FIRST_NAME, per.MIDDLE_NAME, per.NAME_PREFIX,
            per.NAME_SUFFIX, per.NAME_REPRESENTATION_CODE, per.COMMON_NAME,
            per.MOBILE_PHONE_NUMBER, per.PAGER_NUMBER, per.PRIMARY_EMAIL_ADDRESS,
            per.SECONDARY_EMAIL_ADDRESS, per.DELETED, per.DELETED_DATE, per.LAST_UPDATED
        FROM {_RESOURCE_ID_TABLE} r
        LEFT JOIN {_PERSON_TABLE} per ON per.PERSON_KEY = r.PERSON_KEY
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Resources ETL starting")
        print(f"[RIS Resources ETL] 🚀 Starting ({_RESOURCE_ID_TABLE} ⋈ {_PERSON_TABLE})")

        cursor.execute(query)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (rid_key, person_key, role_key, resource_id, assigning_authority,
                 org_key, last_updated, pref_rpt, pref_img, active_flag, alt_id,
                 last_name, first_name, middle_name, name_prefix, name_suffix,
                 name_rep_code, common_name, mobile, pager, email1, email2,
                 deleted, deleted_date, person_last_updated) = row

                if rid_key is None:
                    skipped += 1
                    continue

                active = None
                if active_flag is not None:
                    active = str(active_flag).strip().upper() in ('Y', 'YES', 'TRUE', '1')

                org_key_str = str(org_key).strip() if org_key is not None else None
                site_id = site_map.get(org_key_str) if org_key_str else None

                params.append({
                    "resource_id_key": rid_key, "person_key": person_key,
                    "resource_role_key": role_key, "resource_id": _safe_str(resource_id),
                    "assigning_authority": _safe_str(assigning_authority),
                    "org_structure_key": org_key_str, "site_id": site_id,
                    "pref_rpt_delivery_method_key": pref_rpt,
                    "pref_img_delivery_method_key": pref_img, "active": active,
                    "alternate_id": _safe_str(alt_id), "source_last_updated": _safe_date(last_updated),
                    "last_name": _safe_str(last_name), "first_name": _safe_str(first_name),
                    "middle_name": _safe_str(middle_name), "name_prefix": _safe_str(name_prefix),
                    "name_suffix": _safe_str(name_suffix),
                    "name_representation_code": _safe_str(name_rep_code),
                    "common_name": _safe_str(common_name), "mobile_phone_number": _safe_str(mobile),
                    "pager_number": _safe_str(pager), "primary_email_address": _safe_str(email1),
                    "secondary_email_address": _safe_str(email2), "deleted": _safe_str(deleted),
                    "deleted_date": _safe_date(deleted_date),
                    "person_last_updated": _safe_date(person_last_updated),
                    "last_update": datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        print(f"[RIS Resources ETL] ✅ Resources: {total:,} upserted, {skipped} skipped (no key)")

        # Enrichment: resolve reading/signing physician on already-loaded studies.
        with pg_engine.begin() as conn:
            reading_result = conn.execute(_ENRICH_READING_SQL)
        with pg_engine.begin() as conn:
            signing_result = conn.execute(_ENRICH_SIGNING_SQL)
        print(f"[RIS Resources ETL] 🔗 reading_physician resolved on {reading_result.rowcount:,} studies, "
              f"signing_physician on {signing_result.rowcount:,}")

        status = "SUCCESS"
        logging.info(
            f"RIS Resources ETL complete: {total:,} resources, {skipped} skipped, "
            f"{reading_result.rowcount:,} reading + {signing_result.rowcount:,} signing resolved"
        )

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Resources ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Resources log: {le}")

    return total
