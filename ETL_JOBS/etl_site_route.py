"""
ETL_JOBS/etl_site_route.py — a site's rows into that site's own database (item 13b, option A).

LAUMC's ETL reads PACS and RIS once for both hospitals into RH's database (the main one).
This job then decides where each row belongs and stores SJH's rows in SJH's database
(POSTGRES_DB_SJH, utils/site_db.py), the same way for the one-time move of the history and
every night at the end of the ETL (operator, 2026-11-17: "let the ETL know where to store
each row"). Postgres only: it never reads PACS or RIS.

How a run goes
  1. Key sets, once, on RH's database (temp tables): the site's studies, orders, accessions,
     devices, PPS, ORU reports, patients, visits. Copying and deleting then see the same rows
     whatever order the tables go in.
  2. Copy, parents first: each table's rows go out with COPY into a staging table in the site
     database and are upserted on the table's NATURAL key (never a generated id: the site
     database gets its own writes later, so ids would collide; ORU children find their
     report by accession). Parents are upserted, never deleted and re-inserted, because a
     cascade would drop children RH no longer holds. Then each table is verified: every row
     sent must be found in the site database. All in one transaction there.
  3. Delete from RH, children first, only when every table verified, and only for the tables
     marked so (devices, catalogs and RIS patients stay: RH's site resolution and other
     orders need them). Studies go in batches; their series, raw images and image locations
     follow by cascade.

What stays on RH: rows with no site (operator, 2026-11-17: they stay flagged, so every run
reports how many remain), and the HL7 data BRAYN owns until 13d. Raw images and image
locations go every night (only the night's new images are on RH by then); the one-time move
takes those of the last 14 days only (operator, 2026-11-17: the image history is not copied;
nothing reads it but the storage rollup), after computing the site's storage history from
all of them, with the rollup's formula, into the site database.

    python -m ETL_JOBS.etl_site_route --site SJH --dry-run      counts only
    python -m ETL_JOBS.etl_site_route --site SJH --copy         copy and verify
    python -m ETL_JOBS.etl_site_route --site SJH --move         copy, verify, delete from RH
"""
import argparse
import logging
import os
import sys
import tempfile
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_ETL_SYNC_LOCK_ID = 872951065     # ETL_JOBS/etl_runner.py: never move while the ETL runs
_STUDY_BATCH = 2000
_SPOOL = 64 * 1024 * 1024


# ── what goes where ───────────────────────────────────────────────────────────

# Key sets on RH's database, in order (later ones read earlier ones). %(site)s = site id.
KEY_SETS = (
    ('_r_studies', "SELECT study_db_uid FROM etl_didb_studies WHERE site_id = %(site)s"),
    ('_r_study_uids', """SELECT DISTINCT BTRIM(s.study_instance_uid) AS uid FROM etl_didb_studies s
                         JOIN _r_studies r USING (study_db_uid) WHERE s.study_instance_uid IS NOT NULL"""),
    ('_r_orders', """SELECT order_dbid FROM etl_orders
                     WHERE order_control = (SELECT ris_issuer FROM sites WHERE id = %(site)s)"""),
    ('_r_acc', """SELECT o.accession_number AS acc FROM etl_orders o JOIN _r_orders r USING (order_dbid)
                  WHERE o.accession_number IS NOT NULL
                  UNION
                  SELECT s.accession_number FROM etl_didb_studies s JOIN _r_studies r USING (study_db_uid)
                  WHERE s.accession_number IS NOT NULL"""),
    ('_r_devices', """SELECT UPPER(TRIM(aetitle)) AS ae, COALESCE(modality, '') AS modality
                      FROM aetitle_modality_map WHERE site_id = %(site)s"""),
    ('_r_pps', """SELECT p.pps_key FROM std_pps p
                  WHERE p.study_db_uid IN (SELECT study_db_uid FROM _r_studies)
                     OR UPPER(TRIM(p.performing_ae_title)) IN
                        (SELECT ae FROM _r_devices WHERE modality <> 'PACS')"""),
    ('_r_oru', """SELECT id FROM hl7_oru_reports
                  WHERE accession_number IS NOT NULL
                    AND (accession_number IN (SELECT acc FROM _r_acc) OR site_id = %(site)s)"""),
    # the HIS's orders (item 13d, 2026-11-19): the hospital the message names (PV1-3.7 =
    # sites.hl7_building), else for SJH a location or entering unit (ORC-17) ending in -J
    ('_r_his', r"""SELECT h.id FROM hl7_orders h JOIN sites si ON si.id = %(site)s
                  WHERE BTRIM(split_part(split_part(substring(h.raw_message from 'PV1\|[^\r]*'), '|', 4), '^', 7))
                        = COALESCE(NULLIF(BTRIM(si.hl7_building), ''), '#')
                     OR (COALESCE(BTRIM(split_part(split_part(substring(h.raw_message from 'PV1\|[^\r]*'), '|', 4), '^', 7)), '') = ''
                         AND UPPER(si.code) = 'SJH'
                         AND (UPPER(COALESCE(h.patient_location, '')) LIKE '%%-J'
                              OR UPPER(BTRIM(split_part(split_part(substring(h.raw_message from 'ORC\|[^\r]*'), '|', 18), '^', 1))) LIKE '%%-J'))"""),
    ('_r_patients', """SELECT DISTINCT s.patient_db_uid FROM etl_didb_studies s
                       JOIN _r_studies r USING (study_db_uid) WHERE s.patient_db_uid IS NOT NULL"""),
    ('_r_visits', """SELECT DISTINCT o.visit_dbid::text AS visit FROM etl_orders o JOIN _r_orders r USING (order_dbid)
                     WHERE o.visit_dbid IS NOT NULL"""),
    ('_r_persons', """SELECT DISTINCT o.patient_dbid::text AS person FROM etl_orders o JOIN _r_orders r USING (order_dbid)
                      WHERE o.patient_dbid IS NOT NULL"""),
    # studies whose raw images and image locations go too: all of them every night (only
    # the night's new images are on RH by then); in the one-time move, the last days only
    ('_r_image_studies', """SELECT s.study_db_uid FROM etl_didb_studies s JOIN _r_studies r USING (study_db_uid)
                            WHERE %(images_all)s OR s.study_date >= %(images_since)s"""),
)

_DEVICE = "UPPER(TRIM(t.aetitle)) IN (SELECT ae FROM _r_devices)"
_STUDY = "t.study_db_uid IN (SELECT study_db_uid FROM _r_studies)"
_ORDER = "t.site_worklist_key IN (SELECT order_dbid FROM _r_orders)"
_PPS = "t.pps_key IN (SELECT pps_key FROM _r_pps)"
_ORU_CHILD = "t.{col} IN (SELECT id FROM _r_oru)"


@dataclass(frozen=True)
class Table:
    name: str
    key: tuple                 # natural key: the upsert target and what verification matches
    where: str                 # this site's rows on RH (alias t)
    after: str = 'delete'      # delete | cascade (goes with its study) | keep | unreferenced
    skip: tuple = ()           # never copied (generated ids)
    parent_col: str = None     # ORU child: this column holds hl7_oru_reports.id
    images: bool = False       # raw images and their locations: only when asked
    nightly: bool = True       # False: curated by people, copied once, never overwritten
    link_or_null: tuple = ()   # (column, table, key): kept only when the row it points at is there


# Copy order: parents first. Deleting goes the other way, see _DELETE_STEPS.
TABLES = (
    # devices and their schedules: the site database keeps its own; RH keeps them too,
    # because its site resolution reads every device's site
    Table('aetitle_modality_map', ('aetitle',), _DEVICE, 'keep', skip=('id',)),
    Table('device_weekly_schedule', ('aetitle', 'day_of_week'), _DEVICE, 'keep'),
    Table('device_exceptions', ('aetitle', 'exception_date'), _DEVICE, 'keep', skip=('id',)),
    Table('std_device_weekly_availability', ('aetitle', 'day_of_week'), _DEVICE, 'keep'),
    Table('std_device_weekly_windows', ('aetitle', 'day_of_week', 'from_time'), _DEVICE, 'keep'),
    Table('std_schedule_template_items', ('schedule_template_item_key',), _DEVICE, 'keep'),
    Table('std_modality_exceptions', ('modality_avail_exception_key',), _DEVICE, 'keep'),
    # catalogs read with the site's rows (RIS staff, organizations, PACS groups, schedules)
    Table('std_ordering_organizations', ('ordering_organization_key',), 'TRUE', 'keep'),
    Table('std_resources_ris', ('resource_id_key',), 'TRUE', 'keep'),
    Table('std_pacs_user_groups', ('membership_dbid',), 'TRUE', 'keep'),
    Table('std_schedule_templates', ('schedule_template_key',), 'TRUE', 'keep'),
    Table('std_schedule_template_versions', ('schedule_template_version_key',), 'TRUE', 'keep'),
    Table('std_schedule_schemes', ('schedule_scheme_key',), 'TRUE', 'keep'),
    Table('std_availability_indicators', ('availability_indicator_key',), 'TRUE', 'keep'),
    Table('std_status_ris', ('status_key',), 'TRUE', 'keep'),
    Table('std_procedure_priorities', ('priority_key',), 'TRUE', 'keep'),
    Table('std_dictations', ('dictation_key',), 'TRUE', 'keep'),
    Table('std_modality_schedule_groups', ('modality_schedule_group_key',), 'TRUE', 'keep'),
    Table('go_live_config', ('id',), 'TRUE', 'keep'),          # the site's storage rollup needs it
    Table('physician_alias_map', ('alias',), 'TRUE', 'keep', nightly=False),
    Table('pacs_user_role_override', ('login_id',), 'TRUE', 'keep', nightly=False),
    # PACS: patients, studies and what hangs off a study
    Table('etl_patient_view', ('patient_db_uid',), "t.patient_db_uid IN (SELECT patient_db_uid FROM _r_patients)",
          'unreferenced'),
    Table('etl_didb_studies', ('study_db_uid',), _STUDY),
    Table('etl_didb_serieses', ('series_db_uid',), _STUDY, 'cascade'),
    Table('etl_didb_raw_images', ('raw_image_db_uid',),
          "t.study_db_uid IN (SELECT study_db_uid FROM _r_image_studies)", 'cascade', images=True),
    Table('etl_image_locations', ('raw_image_db_uid',),
          "t.raw_image_db_uid IN (SELECT i.raw_image_db_uid FROM etl_didb_raw_images i "
          "WHERE i.study_db_uid IN (SELECT study_db_uid FROM _r_image_studies))", 'cascade', images=True),
    Table('etl_sticky_notes', ('data_db_uid',), _STUDY),
    Table('study_access_history', ('study_db_uid', 'captured_at'), _STUDY),
    Table('etl_pacs_audit_events', ('log_dbid',), "BTRIM(t.study_instance_uid) IN (SELECT uid FROM _r_study_uids)"),
    Table('cd_print_log', ('task_id',), _STUDY, skip=('id',)),
    Table('fact_exam', ('study_db_uid',), _STUDY),
    # RIS: orders and their steps, reports, visits, patients
    Table('etl_orders', ('order_dbid',), "t.order_dbid IN (SELECT order_dbid FROM _r_orders)"),
    Table('std_worklist_scheduled', ('site_worklist_key', 'scheduled_at'), _ORDER, skip=('id',)),
    Table('std_worklist_arrivals', ('site_worklist_key', 'arrived_at'), _ORDER, skip=('id',)),
    Table('std_worklist_exam_done', ('site_worklist_key', 'exam_done_at'), _ORDER, skip=('id',)),
    Table('std_worklist_cancellations', ('site_worklist_key', 'status_key', 'cancelled_at'), _ORDER, skip=('id',)),
    Table('worklist_status_history', ('site_worklist_key', 'stage'), _ORDER, skip=('id',)),
    Table('std_reports', ('report_key', 'version'), "t.reported_acc_number IN (SELECT acc FROM _r_acc)"),
    Table('std_visits', ('visit_key',), "t.visit_key::text IN (SELECT visit FROM _r_visits)", 'unreferenced'),
    Table('std_patients_ris', ('patient_person_key',), "t.patient_person_key::text IN (SELECT person FROM _r_persons)",
          'keep'),
    Table('std_patient_ids', ('patient_id_list_key',), "t.patient_person_key::text IN (SELECT person FROM _r_persons)",
          'keep'),
    # a PPS on this site's device may belong to another site's study: its link is dropped
    Table('std_pps', ('pps_key',), _PPS, link_or_null=('study_db_uid', 'etl_didb_studies', 'study_db_uid')),
    Table('std_pps_person_reference', ('pps_person_reference_key',), _PPS),
    Table('std_site_pps_ext', ('pps_key',), _PPS),
    # ORU reports and the rows built on them (children find their report by accession)
    Table('hl7_oru_reports', ('accession_number',), "t.id IN (SELECT id FROM _r_oru)", skip=('id',)),
    Table('hl7_oru_analysis', ('report_id',), _ORU_CHILD.format(col='report_id'), 'cascade',
          skip=('id',), parent_col='report_id'),
    Table('ai_nlp_cache', ('source_id',), _ORU_CHILD.format(col='source_id'), 'cascade',
          skip=('id',), parent_col='source_id'),
    Table('hl7_oru_rule_cache', ('report_id',), _ORU_CHILD.format(col='report_id'), 'cascade',
          parent_col='report_id'),
    # the HIS's orders: SJH's go to SJH's database (the listener routes new ones once moved)
    Table('hl7_orders', ('message_id',), "t.id IN (SELECT id FROM _r_his)", skip=('id',)),
)

# Deleting from RH, children first. (label, SQL); studies and the "unreferenced" ones are
# handled in code.
_DELETE_STEPS = (
    ('hl7_orders', "DELETE FROM hl7_orders WHERE id IN (SELECT id FROM _r_his)"),
    ('hl7_oru_reports', "DELETE FROM hl7_oru_reports WHERE id IN (SELECT id FROM _r_oru)"),
    ('std_site_pps_ext', "DELETE FROM std_site_pps_ext WHERE pps_key IN (SELECT pps_key FROM _r_pps)"),
    ('std_pps_person_reference', "DELETE FROM std_pps_person_reference WHERE pps_key IN (SELECT pps_key FROM _r_pps)"),
    ('std_pps', "DELETE FROM std_pps WHERE pps_key IN (SELECT pps_key FROM _r_pps)"),
    # another site's PPS may still point at one of these studies; its link goes, the PPS stays
    ('std_pps (links)', "UPDATE std_pps SET study_db_uid = NULL WHERE study_db_uid IN (SELECT study_db_uid FROM _r_studies)"),
    ('std_reports', "DELETE FROM std_reports WHERE reported_acc_number IN (SELECT acc FROM _r_acc)"),
    ('worklist_status_history', "DELETE FROM worklist_status_history WHERE site_worklist_key IN (SELECT order_dbid FROM _r_orders)"),
    ('std_worklist_cancellations', "DELETE FROM std_worklist_cancellations WHERE site_worklist_key IN (SELECT order_dbid FROM _r_orders)"),
    ('std_worklist_exam_done', "DELETE FROM std_worklist_exam_done WHERE site_worklist_key IN (SELECT order_dbid FROM _r_orders)"),
    ('std_worklist_arrivals', "DELETE FROM std_worklist_arrivals WHERE site_worklist_key IN (SELECT order_dbid FROM _r_orders)"),
    ('std_worklist_scheduled', "DELETE FROM std_worklist_scheduled WHERE site_worklist_key IN (SELECT order_dbid FROM _r_orders)"),
    ('etl_orders', "DELETE FROM etl_orders WHERE order_dbid IN (SELECT order_dbid FROM _r_orders)"),
    ('fact_exam', "DELETE FROM fact_exam WHERE study_db_uid IN (SELECT study_db_uid FROM _r_studies)"),
    ('cd_print_log', "DELETE FROM cd_print_log WHERE study_db_uid IN (SELECT study_db_uid FROM _r_studies)"),
    ('etl_pacs_audit_events', "DELETE FROM etl_pacs_audit_events WHERE BTRIM(study_instance_uid) IN (SELECT uid FROM _r_study_uids)"),
    ('study_access_history', "DELETE FROM study_access_history WHERE study_db_uid IN (SELECT study_db_uid FROM _r_studies)"),
    ('etl_sticky_notes', "DELETE FROM etl_sticky_notes WHERE study_db_uid IN (SELECT study_db_uid FROM _r_studies)"),
)
_UNREFERENCED = (
    ('etl_patient_view', """DELETE FROM etl_patient_view p
                            WHERE p.patient_db_uid IN (SELECT patient_db_uid FROM _r_patients)
                              AND NOT EXISTS (SELECT 1 FROM etl_didb_studies s WHERE s.patient_db_uid = p.patient_db_uid)"""),
    ('std_visits', """DELETE FROM std_visits v
                      WHERE v.visit_key::text IN (SELECT visit FROM _r_visits)
                        AND NOT EXISTS (SELECT 1 FROM etl_orders o WHERE o.visit_dbid::text = v.visit_key::text)"""),
)
# Rows elsewhere that point at an ORU report without a cascade: while any exist for this
# site's reports, its ORU reports are not deleted from RH.
_ORU_BLOCKERS = ('crn_notifications', 'crn_legacy_notifications')


def _q(name):
    return '"' + name.replace('"', '""') + '"'


MOVED_KEY = 'site_route_moved_{code}'     # settings on RH's database: when the one-time move succeeded


def moved_already(rh_engine, code):
    """The one-time move of this site's history has been done; until then the nightly
    route stays off (its first run would copy the whole image history)."""
    from sqlalchemy import text
    with rh_engine.connect() as c:
        return c.execute(text("SELECT 1 FROM settings WHERE key = :k"),
                         {'k': MOVED_KEY.format(code=code.upper())}).scalar() is not None


def record_moved(rh_engine, code, when):
    from sqlalchemy import text
    with rh_engine.begin() as c:
        c.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                       "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                  {'k': MOVED_KEY.format(code=code.upper()), 'v': when.strftime('%Y-%m-%d %H:%M:%S')})


# ── the run ───────────────────────────────────────────────────────────────────

def route_site(rh_engine, site_engine, site_id, *, delete=False, dry_run=False, images=False,
               images_since=None, first_time=False, lock=True):
    """Copy (and with delete=True, move) one site's rows from RH's database into the site's
    own. dry_run: counts only. images: the raw images and image locations of every study
    moved (the nightly run: only the night's new images are on RH); images_since: only those
    of studies from that date (the one-time move keeps the last days, so a late image
    recomputes a whole day). first_time: also the curated tables (alias names, role
    corrections), never overwritten afterwards, and the site's storage history computed
    from its images before they go. lock: refuse while the ETL runs (the nightly call, made
    by the ETL itself, passes False).
    Returns a report: per table what was selected, copied, missing and deleted."""
    report = {'site_id': site_id, 'tables': [], 'verified': False, 'deleted': False,
              'blocked': [], 'no_site': {}}
    src = rh_engine.raw_connection()
    dst = None if dry_run else site_engine.raw_connection()
    try:
        cur = src.cursor()
        if lock:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (_ETL_SYNC_LOCK_ID,))
            if not cur.fetchone()[0]:
                raise RuntimeError("the ETL is running; try again when it has finished")
        _key_sets(cur, {'site': site_id, 'images_all': bool(images), 'images_since': images_since})
        report['no_site'] = _no_site(cur)
        chosen = [t for t in TABLES if first_time or t.nightly]
        if dry_run:
            for t in chosen:
                cur.execute(f"SELECT COUNT(*) FROM public.{_q(t.name)} t WHERE {t.where}")
                report['tables'].append({'table': t.name, 'selected': cur.fetchone()[0]})
            src.rollback()
            return report

        dcur = dst.cursor()
        for t in chosen:
            report['tables'].append(_copy(cur, dcur, t))
        if first_time:
            report['tables'].append(_storage_history(cur, dcur, site_id))
        dst.commit()
        report['verified'] = all(r['missing'] == 0 for r in report['tables'])
        if delete and report['verified']:
            src.commit()                        # the key sets stay (temp tables live per session)
            _delete(cur, src, report)
            report['deleted'] = True
        elif delete:
            logger.error("site route: not every row was found in the site database; nothing deleted")
        src.commit()
        return report
    except Exception:
        src.rollback()
        if dst is not None:
            dst.rollback()
        raise
    finally:
        try:
            if lock:
                src.cursor().execute("SELECT pg_advisory_unlock(%s)", (_ETL_SYNC_LOCK_ID,))
                src.commit()
        except Exception:
            pass
        src.close()
        if dst is not None:
            dst.close()


def _key_sets(cur, params):
    for name, sql in KEY_SETS:
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(f"CREATE TEMP TABLE {name} AS {sql}", params)
        cur.execute(f"ANALYZE {name}")


# The site's daily storage, computed once from its images on RH before the move deletes
# them: the formula of the storage rollup (ETL_JOBS/etl_analytics_refresh.py, bytes to GB,
# SR left out, from go-live, the site's devices). RH's rollup never kept these rows (it
# counts RH's devices only); afterwards the site's own rollup adds the new days.
_STORAGE_HISTORY = """
    SELECT s.study_date, COALESCE(s.storing_ae, 'UNKNOWN'), COALESCE(s.study_modality, 'UNKNOWN'),
           COALESCE(s.procedure_code, 'UNKNOWN'),
           ROUND(CAST(COALESCE(SUM(l.image_size_kb), 0) AS numeric) / 1073741824, 4),
           COUNT(DISTINCT s.study_db_uid)
    FROM etl_didb_studies s
    JOIN etl_didb_raw_images i ON i.study_db_uid = s.study_db_uid
    JOIN etl_image_locations l ON l.raw_image_db_uid = i.raw_image_db_uid
    JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE m.site_id = %(site)s
      AND s.study_date >= (SELECT go_live_date FROM go_live_config ORDER BY id DESC LIMIT 1)
      AND s.study_modality <> 'SR'
    GROUP BY 1, 2, 3, 4
"""
_STORAGE_COLS = ('study_date', 'storing_ae', 'modality', 'procedure_code', 'total_gb', 'study_count')
_STORAGE_KEY = ('study_date', 'storing_ae', 'modality', 'procedure_code')


def _storage_history(cur, dcur, site_id):
    cols = ', '.join(_STORAGE_COLS)
    key = ', '.join(_STORAGE_KEY)
    dcur.execute("DROP TABLE IF EXISTS _stage")
    dcur.execute(f"CREATE TEMP TABLE _stage AS SELECT {cols} FROM public.summary_storage_daily WITH NO DATA")
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL, mode='w+b') as buf:
        cur.copy_expert(f"COPY ({cur.mogrify(_STORAGE_HISTORY, {'site': site_id}).decode()}) TO STDOUT", buf)
        buf.seek(0)
        dcur.copy_expert(f"COPY _stage ({cols}) FROM STDIN", buf)
    dcur.execute("SELECT COUNT(*) FROM _stage")
    selected = dcur.fetchone()[0]
    dcur.execute(f"INSERT INTO public.summary_storage_daily ({cols}) SELECT {cols} FROM _stage "
                 f"ON CONFLICT ({key}) DO UPDATE SET total_gb = EXCLUDED.total_gb, study_count = EXCLUDED.study_count")
    copied = dcur.rowcount
    match = ' AND '.join(f'x.{k} = s.{k}' for k in _STORAGE_KEY)
    dcur.execute(f"SELECT COUNT(*) FROM _stage s WHERE NOT EXISTS "
                 f"(SELECT 1 FROM public.summary_storage_daily x WHERE {match} AND x.total_gb = s.total_gb)")
    missing = dcur.fetchone()[0]
    dcur.execute("DROP TABLE _stage")
    return {'table': 'summary_storage_daily (history)', 'selected': selected, 'copied': copied,
            'missing': missing, 'deleted': 0, 'after': 'keep'}


def _no_site(cur):
    """Rows that stay on RH because they have no site (they stay flagged, operator 2026-11-17)."""
    cur.execute("SELECT COUNT(*) FROM etl_didb_studies WHERE site_id IS NULL")
    studies = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hl7_oru_reports WHERE site_id IS NULL")
    oru = cur.fetchone()[0]
    return {'studies': studies, 'oru_reports': oru}


def _columns(cur, table):
    """(column, generated?) of a table, in order."""
    cur.execute("""SELECT a.attname, a.attgenerated <> ''
                   FROM pg_attribute a
                   WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
                   ORDER BY a.attnum""", (f'public.{table}',))
    return cur.fetchall()


def _copy(cur, dcur, t):
    src_cols = {c for c, gen in _columns(cur, t.name) if not gen}
    dst_cols = [c for c, gen in _columns(dcur, t.name) if not gen]
    if not src_cols or not dst_cols:
        raise RuntimeError(f"{t.name} is missing from one of the databases")
    cols = [c for c in dst_cols if c in src_cols and c not in t.skip]
    col_list = ', '.join(_q(c) for c in cols)
    picked = ', '.join(f't.{_q(c)}' for c in cols)
    join = ''
    if t.parent_col:
        picked += ', p.accession_number'
        join = f' JOIN public.hl7_oru_reports p ON p.id = t.{_q(t.parent_col)}'
    out_sql = f"COPY (SELECT {picked} FROM public.{_q(t.name)} t{join} WHERE {t.where}) TO STDOUT"

    dcur.execute("DROP TABLE IF EXISTS _stage")
    dcur.execute(f"CREATE TEMP TABLE _stage AS SELECT {col_list} FROM public.{_q(t.name)} WITH NO DATA")
    stage_cols = col_list
    if t.parent_col:
        dcur.execute("ALTER TABLE _stage ADD COLUMN _parent_acc text")
        stage_cols += ', _parent_acc'
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL, mode='w+b') as buf:
        cur.copy_expert(out_sql, buf)
        buf.seek(0)
        dcur.copy_expert(f"COPY _stage ({stage_cols}) FROM STDIN", buf)
    dcur.execute("SELECT COUNT(*) FROM _stage")
    selected = dcur.fetchone()[0]

    def value(c):
        if c == t.parent_col:
            return "(SELECT r.id FROM public.hl7_oru_reports r WHERE r.accession_number = s._parent_acc)"
        if t.link_or_null and c == t.link_or_null[0]:
            _, table, key = t.link_or_null
            return f"(SELECT x.{_q(key)} FROM public.{_q(table)} x WHERE x.{_q(key)} = s.{_q(c)})"
        return f's.{_q(c)}'
    values = ', '.join(value(c) for c in cols)
    rest = [c for c in cols if c not in t.key]
    action = ('DO UPDATE SET ' + ', '.join(f'{_q(c)} = EXCLUDED.{_q(c)}' for c in rest)) if rest else 'DO NOTHING'
    found_parent = ("WHERE EXISTS (SELECT 1 FROM public.hl7_oru_reports r WHERE r.accession_number = s._parent_acc)"
                    if t.parent_col else '')
    dcur.execute(f"INSERT INTO public.{_q(t.name)} ({col_list}) SELECT {values} FROM _stage s {found_parent} "
                 f"ON CONFLICT ({', '.join(_q(k) for k in t.key)}) {action}")
    copied = dcur.rowcount

    match = ' AND '.join(
        f"x.{_q(k)} = (SELECT r.id FROM public.hl7_oru_reports r WHERE r.accession_number = s._parent_acc)"
        if k == t.parent_col else f"x.{_q(k)} IS NOT DISTINCT FROM s.{_q(k)}" for k in t.key)
    dcur.execute(f"SELECT COUNT(*) FROM _stage s WHERE NOT EXISTS (SELECT 1 FROM public.{_q(t.name)} x WHERE {match})")
    missing = dcur.fetchone()[0]
    dcur.execute("DROP TABLE _stage")
    return {'table': t.name, 'selected': selected, 'copied': copied, 'missing': missing,
            'deleted': 0, 'after': t.after}


def _delete(cur, conn, report):
    counts = {}
    blocked = [b for b in _ORU_BLOCKERS if _has_rows(cur, f"SELECT 1 FROM public.{b} WHERE report_id IN (SELECT id FROM _r_oru) LIMIT 1")]
    for label, sql in _DELETE_STEPS:
        if label == 'hl7_oru_reports' and blocked:
            report['blocked'].append(f"ORU reports kept on RH: rows in {', '.join(blocked)} point at them")
            continue
        cur.execute(sql)
        counts[label] = cur.rowcount
        conn.commit()
    # studies in batches; series, raw images and image locations follow by cascade
    cur.execute("DROP TABLE IF EXISTS _r_studies_n")
    cur.execute("CREATE TEMP TABLE _r_studies_n AS "
                "SELECT study_db_uid, row_number() OVER (ORDER BY study_db_uid) AS n FROM _r_studies")
    cur.execute("SELECT COUNT(*) FROM _r_studies_n")
    total, done, start = cur.fetchone()[0], 0, 0
    while start < total:
        cur.execute("DELETE FROM etl_didb_studies WHERE study_db_uid IN "
                    "(SELECT study_db_uid FROM _r_studies_n WHERE n > %s AND n <= %s)", (start, start + _STUDY_BATCH))
        done += cur.rowcount
        conn.commit()
        start += _STUDY_BATCH
    counts['etl_didb_studies'] = done
    for label, sql in _UNREFERENCED:
        cur.execute(sql)
        counts[label] = cur.rowcount
        conn.commit()
    for row in report['tables']:
        row['deleted'] = counts.get(row['table'], 0)


def _has_rows(cur, sql):
    try:
        cur.execute(sql)
        return cur.fetchone() is not None
    except Exception:
        cur.connection.rollback()
        return False


# ── command line (the one-time move) ──────────────────────────────────────────

def _engines(code):
    from sqlalchemy import create_engine, text
    user = os.environ.get('POSTGRES_USER', 'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', '')
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    port = os.environ.get('POSTGRES_PORT', '5432')
    main = os.environ.get('POSTGRES_DB', 'etl_db')
    other = (os.environ.get(f'POSTGRES_DB_{code}') or '').strip()
    if not other or other == main:
        raise SystemExit(f"POSTGRES_DB_{code} is not set (or names the main database)")
    base = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/"
    rh, site = create_engine(base + main), create_engine(base + other)
    with rh.connect() as c:
        site_id = c.execute(text("SELECT id FROM sites WHERE code = :c AND NOT is_default"), {'c': code}).scalar()
    with site.connect() as c:
        serves = c.execute(text("SELECT value FROM settings WHERE key = 'served_site'")).scalar()
    if site_id is None or str(serves or '').strip().upper() not in (str(site_id), code.upper()):
        raise SystemExit(f"{other} does not serve site {code} (settings.served_site = {serves!r})")
    return rh, site, site_id


def print_report(report, out=sys.stdout):
    out.write(f"{'table':32s} {'selected':>10s} {'copied':>10s} {'missing':>8s} {'deleted':>10s}  after\n")
    for r in report['tables']:
        out.write(f"{r['table']:32s} {r['selected']:>10,} {r.get('copied', 0):>10,} {r.get('missing', 0):>8,} "
                  f"{r.get('deleted', 0):>10,}  {r.get('after', '')}\n")
    out.write(f"verified: {report['verified']}   deleted from RH: {report['deleted']}\n")
    out.write(f"left on RH with no site (flagged): {report['no_site']}\n")
    for b in report['blocked']:
        out.write(f"blocked: {b}\n")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    p.add_argument('--site', default='SJH')
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--dry-run', action='store_true', help='count only')
    mode.add_argument('--copy', action='store_true', help='copy and verify, delete nothing')
    mode.add_argument('--move', action='store_true', help='copy, verify, then delete from RH')
    p.add_argument('--images', action='store_true', help='every image, not only the last days (not wanted at LAUMC)')
    p.add_argument('--image-days', type=int, default=14,
                   help="images of the last N days of studies go too (default 14), so a late image "
                        "recomputes a whole day of the site's storage")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    from datetime import date, timedelta
    rh, site, site_id = _engines(args.site)
    report = route_site(rh, site, site_id, delete=args.move, dry_run=args.dry_run, images=args.images,
                        images_since=date.today() - timedelta(days=args.image_days), first_time=True)
    print_report(report)
    if args.move and report['deleted']:
        from datetime import datetime
        record_moved(rh, args.site, datetime.now())
        print(f"The nightly ETL now stores {args.site}'s new rows in its database.")
    return 0 if (args.dry_run or report['verified']) else 1


if __name__ == '__main__':
    sys.exit(main())
