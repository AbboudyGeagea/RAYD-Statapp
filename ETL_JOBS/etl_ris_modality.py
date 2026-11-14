"""
ETL_JOBS/etl_ris_modality.py — RIS MODALITY -> aetitle_modality_map (LAUMC).

Per migration 0053 / docs/LAUMC_RIS_TABLES.md: the RIS device registry loads into the
EXISTING aetitle_modality_map (not a separate std_devices table), so every report,
the capacity ladder, the device grid, and the mapping tab keep working unchanged.

MODALITY ⋈ MODALITY_TYPE ON modality_type_key, resolving the type key to its DICOM-style
short code (CT/MR/US/...) at ETL time, per the vendor's own merge instruction ("resolve
MODALITY_TYPE_KEY -> MODALITY_TYPE.CODE at load, merge into one table").

Column map (vendor-confirmed):
    AE_TITLE           -> aetitle          (= PACS didb_studies.storing_ae)
    MODALITY_TYPE.CODE -> modality         (resolved via join, not the raw key)
    DESCRIPTION        -> description
    STATION_NAME       -> station_name, room_name (room_name kept for backward
                           compatibility with the pre-existing mapping-tab/CSV export,
                           which reads room_name — same value, two columns)
    CODE               -> room_code        (e.g. CT64, MAMO1 — NOT the procedure code)
    ORG_STRUCTURE_KEY  -> site_id          (via site_org_map, migration 0052)
    ACTIVE             -> active
    MODALITY_KEY       -> ris_modality_key (source back-reference)

IMPORT POLICY (operator, meeting of 2026-11-13): THE RIS IS THE MASTER of room name
(station_name and room_name), room code, description, site and active flag, written over
every night. It was fill-only (ON CONFLICT DO NOTHING) until then, so a room name corrected
on the RIS never reached RAYD. Left alone:
    - modality, capacity, opening hours, floor-plan position: RAYD's own (migration 0117
      corrected modalities by hand)
    - gateway and router rows (modality 'PACS'): no RIS room, checked by hand between the
      systems; migration 0141 cleared LAUMCWFM1AR's site on purpose
    - a value the RIS leaves empty (or a RIS org key with no site in site_org_map) never
      blanks what RAYD holds; every such device is logged as a RIS data error to fix
Never deletes.

DEMO/EXTERNAL ROWS (USDemoPhilips, RH-PACS "External Upload", SJH-CARM 2, etc.): the docs
flag these but are NOT certain the ACTIVE flag reliably marks them ("may mark
demo/deleted"). Rather than guess at a name-based exclusion list, this imports
everything and carries the real ACTIVE flag faithfully — nothing is silently dropped,
and admins can filter/deactivate in the mapping tab if ACTIVE doesn't fully cover it.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_MODALITY_TABLE      = os.getenv("RAYD_RIS_MODALITY_TABLE", "MODALITY")
_MODALITY_TYPE_TABLE = os.getenv("RAYD_RIS_MODALITY_TYPE_TABLE", "MODALITY_TYPE")

_UPSERT_SQL = text("""
    INSERT INTO aetitle_modality_map
        (aetitle, modality, description, station_name, room_code, room_name,
         active, site_id, ris_modality_key)
    VALUES
        (:aetitle, :modality, :description, :station_name, :room_code, :room_name,
         COALESCE(CAST(:active AS boolean), TRUE), :site_id, :ris_modality_key)
    ON CONFLICT (aetitle) DO UPDATE SET
        station_name     = COALESCE(EXCLUDED.station_name, aetitle_modality_map.station_name),
        room_name        = COALESCE(EXCLUDED.room_name,    aetitle_modality_map.room_name),
        room_code        = COALESCE(EXCLUDED.room_code,    aetitle_modality_map.room_code),
        description      = COALESCE(EXCLUDED.description,  aetitle_modality_map.description),
        site_id          = COALESCE(EXCLUDED.site_id,      aetitle_modality_map.site_id),
        active           = COALESCE(CAST(:active AS boolean), aetitle_modality_map.active),
        ris_modality_key = EXCLUDED.ris_modality_key
    WHERE aetitle_modality_map.modality IS DISTINCT FROM 'PACS'
""")


def _pick_one_per_ae(params):
    """One RIS row per AE title. Two rows for one AE is a RIS data error (each AE title is
    one room), and the one loaded last would silently win. Keeps the active row, then the
    newest MODALITY_KEY, and returns the AE titles that had more than one row."""
    best, dupes = {}, set()
    for p in params:
        ae = p["aetitle"]
        if ae in best:
            dupes.add(ae)
            cur = best[ae]
            rank = lambda r: (r["active"] is not False, r["ris_modality_key"] or 0)
            if rank(p) <= rank(cur):
                continue
        best[ae] = p
    return list(best.values()), sorted(dupes)


def _load_site_org_map(pg_engine):
    """Preload org_structure_key -> site_id (migration 0052, ~4 rows). Never raises:
    a missing/unreadable table just leaves site_id NULL (unassigned, fail-closed under RLS)."""
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text("SELECT org_structure_key, site_id FROM site_org_map")).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logging.warning(f"RIS Modality ETL: could not load site_org_map ({e}) — site_id will be NULL")
        return {}


def run_ris_modality_etl(pg_engine, oracle_source):
    job_name   = "RIS_MODALITY_ETL"
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
        logging.error(f"RIS Modality ETL log error: {e}")

    site_map = _load_site_org_map(pg_engine)

    query = f"""
        SELECT
            m.MODALITY_KEY, m.AE_TITLE, m.CODE, m.DESCRIPTION, m.STATION_NAME,
            m.ORG_STRUCTURE_KEY, m.ACTIVE, mt.CODE AS MODALITY_TYPE_CODE
        FROM {_MODALITY_TABLE} m
        LEFT JOIN {_MODALITY_TYPE_TABLE} mt ON mt.MODALITY_TYPE_KEY = m.MODALITY_TYPE_KEY
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Modality ETL starting")
        print("[RIS Modality ETL] 🚀 Starting (MODALITY ⋈ MODALITY_TYPE)")

        cursor.execute(query)
        rows = cursor.fetchall()

        params = []
        no_room, no_site = [], []
        for (modality_key, ae_title, code, description, station_name,
             org_key, active_flag, modality_type_code) in rows:

            # Normalize to upper-case, matching every other writer of this column
            # (mapping_controller.py's CSV upload / inline edit both do .strip().upper());
            # aetitle_modality_map.aetitle has a case-SENSITIVE UNIQUE constraint, so an
            # un-normalized mixed-case insert here would silently create a second row for
            # a device that already exists under a different casing, and admin-UI edits/
            # deletes (which look the row up by an upper-cased key) would silently 404 on
            # the mixed-case row instead of updating it.
            ae_title = str(ae_title).strip().upper() if ae_title is not None else None
            if not ae_title:
                skipped += 1
                continue

            # aetitle_modality_map.modality is NOT NULL — fall back to 'OT' (Other) for
            # any row whose MODALITY_TYPE_KEY didn't resolve, rather than dropping the row.
            modality = str(modality_type_code).strip().upper() if modality_type_code else None
            if not modality:
                modality = 'OT'

            # None = the RIS does not say: a new device starts active, a known one keeps
            # its flag (the upsert COALESCEs).
            active = None
            if active_flag is not None:
                active = str(active_flag).strip().upper() in ('Y', 'YES', 'TRUE', '1')

            org_key_str = str(org_key).strip() if org_key is not None else None
            site_id = site_map.get(org_key_str) if org_key_str else None

            station = str(station_name).strip() if station_name else None
            station = station or None   # whitespace only is empty, never a blank label

            if active is not False:
                if not station:
                    no_room.append(ae_title)
                if site_id is None:
                    no_site.append(ae_title)

            params.append({
                "aetitle":          ae_title,
                "modality":         modality,
                "description":      (str(description).strip() or None) if description else None,
                "station_name":     station,
                "room_code":        (str(code).strip() or None) if code else None,
                "room_name":        station,   # same source value, kept for the older column too
                "active":           active,
                "site_id":          site_id,
                "ris_modality_key": modality_key,
            })

        params, dupes = _pick_one_per_ae(params)

        # RIS data errors (operator: every AE title has one RIS room; fix them on the RIS).
        # RAYD keeps what it already holds for these, so they show as a warning only.
        for label, aes in (("more than one RIS row", dupes),
                           ("no room name (STATION_NAME) on the RIS", sorted(set(no_room))),
                           ("no site for its ORG_STRUCTURE_KEY", sorted(set(no_site)))):
            if aes:
                logging.warning(f"RIS Modality ETL: {len(aes)} active device(s) with {label}: "
                                f"{', '.join(aes)}")

        if params:
            with pg_engine.begin() as conn:
                conn.execute(_UPSERT_SQL, params)
            total = len(params)

        status = "SUCCESS"
        print(f"[RIS Modality ETL] ✅ Done — {total:,} devices from the RIS (room names, codes, "
              f"descriptions, sites and active flags refreshed; gateways untouched), "
              f"{skipped} skipped (no AE_TITLE)")
        logging.info(f"RIS Modality ETL complete: {total:,} devices, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Modality ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Modality log: {le}")

    return total
