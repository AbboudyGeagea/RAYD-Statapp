"""
ETL_JOBS/etl_rh_only.py — keep SJH out of RAYD (LAUMC).

Operator rule, 2026-11-05: "Do not include SJH in anything. Only RH."

About 40 route and helper files read this data, so filtering each query could only ever
be as complete as an audit of all of them. Removing the rows after each load holds for
every screen, export and dropdown, including ones written later.

This runs in Postgres, after the load, with no change to the Oracle extracts. PACS and
the RIS are never touched, so a removed study or order can be pulled again. Reports that
arrived over HL7 are the exception: RAYD holds its only copy of that message.

Which rows go:

    studies   a physical device mapped to another site, or a gateway / unmapped device
              whose PACS label names another site. Same rule as
              utils/report_filters.study_site_sql. The label alone would lose RH
              mammography, which double reading stamps with site '1'. LAUMCWFM1AR stays:
              its label '2' is not a site, and reports count it aside.
    patients  those left with no study. Phase 5 only loads patients who have one, but
              rows from before this rule would otherwise stay on Report 23's conflict list.
    orders    the RIS issuer of another site (etl_orders.order_control). Their keys go to
              etl_excluded_orders, because RIS detail has no site of its own.
    detail    status history, PPS and RIS reports of those orders, and PPS on devices
              mapped to another site.
    ORU       reports on those accessions, unless an RH order shares the accession, or a
              critical-result notification points at the report: those are clinical
              records of a message already sent, and crn_notifications requires the row.

Device rows in aetitle_modality_map stay. The rule above reads them.
"""
import logging

from sqlalchemy import text

logger = logging.getLogger("ETL_RH_ONLY")

_BATCH = 5000

_SJH_STUDIES_SQL = text("""
    SELECT s.study_db_uid
    FROM etl_didb_studies s
    LEFT JOIN LATERAL (
        SELECT m.modality, m.site_id
        FROM aetitle_modality_map m
        WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        ORDER BY m.id
        LIMIT 1
    ) m ON TRUE
    WHERE CASE WHEN m.site_id IS NULL OR m.modality = 'PACS'
               THEN s.pacs_site_id_raw IN (SELECT pacs_site_id FROM sites WHERE id <> :rh)
               ELSE m.site_id <> :rh END
    LIMIT :batch
""")

# RIS detail, by the column that links it to an order.
_DETAIL_BY_ORDER = (
    'std_worklist_arrivals', 'std_worklist_exam_done', 'std_worklist_scheduled',
    'std_worklist_cancellations', 'worklist_status_history',
)
_DETAIL_BY_PPS = ('std_pps_person_reference', 'std_site_pps_ext', 'std_pps')
_DETAIL_BY_ACCESSION = (('std_reports', 'reported_acc_number'), ('hl7_orders', 'accession_number'))


def _rh_site(conn):
    """The site to keep, or None when this install has no site split or no device map
    to judge by. With an empty map every double-read mammogram would look like SJH,
    so nothing is removed until RH has a device on it."""
    rh = conn.execute(text("SELECT id FROM sites WHERE is_default AND active")).scalar()
    if rh is None:
        return None
    mapped = conn.execute(text("""
        SELECT EXISTS (SELECT 1 FROM aetitle_modality_map
                       WHERE site_id = :rh AND COALESCE(modality, '') <> 'PACS')
    """), {"rh": rh}).scalar()
    if not mapped:
        logger.warning("RH-only purge skipped: no RH device in aetitle_modality_map yet")
        return None
    return rh


def _exists(conn, table):
    return conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": f"public.{table}"}).scalar()


def _has_column(conn, table, column):
    return conn.execute(text("""
        SELECT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = :t AND column_name = :c)
    """), {"t": table, "c": column}).scalar()


def purge_sjh_studies(engine):
    """Remove other sites' studies, a batch per transaction, and return how many went.
    Series, raw images and image locations follow by ON DELETE CASCADE. std_pps has a
    plain FK, and etl_orders points at studies without one, so both are unlinked first."""
    with engine.connect() as conn:
        rh = _rh_site(conn)
    if rh is None:
        return 0

    total = 0
    while True:
        with engine.begin() as conn:
            ids = conn.execute(_SJH_STUDIES_SQL, {"rh": rh, "batch": _BATCH}).scalars().all()
            if not ids:
                break
            patients = conn.execute(text("""
                SELECT DISTINCT patient_db_uid FROM etl_didb_studies
                WHERE study_db_uid = ANY(:ids) AND patient_db_uid IS NOT NULL
            """), {"ids": ids}).scalars().all()
            conn.execute(text("UPDATE std_pps SET study_db_uid = NULL WHERE study_db_uid = ANY(:ids)"),
                         {"ids": ids})
            conn.execute(text("""
                UPDATE etl_orders
                SET study_db_uid = NULL, study_instance_uid = NULL, has_study = FALSE
                WHERE study_db_uid = ANY(:ids)
            """), {"ids": ids})
            conn.execute(text("DELETE FROM etl_didb_studies WHERE study_db_uid = ANY(:ids)"),
                         {"ids": ids})
            conn.execute(text("""
                DELETE FROM etl_patient_view p
                WHERE p.patient_db_uid = ANY(:patients)
                  AND NOT EXISTS (SELECT 1 FROM etl_didb_studies s
                                  WHERE s.patient_db_uid = p.patient_db_uid)
            """), {"patients": patients})
        total += len(ids)
        logger.info(f"RH-only purge: {total:,} studies removed so far")
    return total


def purge_sjh_orders(engine):
    """Record other sites' orders in etl_excluded_orders, then remove them."""
    with engine.begin() as conn:
        rh = _rh_site(conn)
        if rh is None:
            return 0
        conn.execute(text("""
            INSERT INTO etl_excluded_orders (order_dbid, accession_number)
            SELECT o.order_dbid, o.accession_number
            FROM etl_orders o
            WHERE o.order_control IN (SELECT ris_issuer FROM sites WHERE id <> :rh)
            ON CONFLICT (order_dbid) DO NOTHING
        """), {"rh": rh})
        return conn.execute(text("""
            DELETE FROM etl_orders
            WHERE order_control IN (SELECT ris_issuer FROM sites WHERE id <> :rh)
        """), {"rh": rh}).rowcount


def purge_sjh_ris_detail(engine):
    """Remove RIS detail and reports that belong to orders already left out. Returns
    the rows removed per table."""
    removed = {}
    with engine.begin() as conn:
        rh = _rh_site(conn)
        if rh is None:
            return removed

        # PPS goes by its worklist rows and by its device, so collect its keys first.
        sources = [
            f"SELECT d.pps_key FROM {t} d JOIN etl_excluded_orders x ON x.order_dbid = d.site_worklist_key"
            for t in _DETAIL_BY_ORDER if _exists(conn, t) and _has_column(conn, t, 'pps_key')
        ]
        sources.append("""
            SELECT p.pps_key FROM std_pps p
            JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(p.performing_ae_title))
            WHERE COALESCE(m.modality, '') <> 'PACS' AND m.site_id <> :rh
        """)
        conn.execute(text(f"""
            CREATE TEMP TABLE _sjh_pps ON COMMIT DROP AS
            SELECT DISTINCT pps_key FROM ({' UNION ALL '.join(sources)}) k WHERE pps_key IS NOT NULL
        """), {"rh": rh})

        for t in _DETAIL_BY_ORDER:
            if _exists(conn, t):
                removed[t] = conn.execute(text(f"""
                    DELETE FROM {t} WHERE site_worklist_key IN (SELECT order_dbid FROM etl_excluded_orders)
                """)).rowcount
        for t in _DETAIL_BY_PPS:
            if _exists(conn, t):
                removed[t] = conn.execute(text(
                    f"DELETE FROM {t} WHERE pps_key IN (SELECT pps_key FROM _sjh_pps)")).rowcount

        # An accession can only go when no RH order still claims it.
        for t, col in _DETAIL_BY_ACCESSION:
            if _exists(conn, t):
                removed[t] = conn.execute(text(f"""
                    DELETE FROM {t} d
                    WHERE d.{col} IN (SELECT accession_number FROM etl_excluded_orders)
                      AND NOT EXISTS (SELECT 1 FROM etl_orders o WHERE o.accession_number = d.{col})
                """)).rowcount
        removed['hl7_oru_reports'] = conn.execute(text("""
            DELETE FROM hl7_oru_reports r
            WHERE r.accession_number IN (SELECT accession_number FROM etl_excluded_orders)
              AND NOT EXISTS (SELECT 1 FROM etl_orders o WHERE o.accession_number = r.accession_number)
              AND NOT EXISTS (SELECT 1 FROM crn_notifications c WHERE c.report_id = r.id)
        """)).rowcount
    return removed


def run_rh_only_purge(engine):
    """Everything above, in dependency order. Safe to run at any time, as often as wanted."""
    studies = purge_sjh_studies(engine)
    orders = purge_sjh_orders(engine)
    detail = purge_sjh_ris_detail(engine)
    logger.info(f"RH-only purge: {studies:,} studies, {orders:,} orders, detail {detail}")
    return {"studies": studies, "orders": orders, **detail}
