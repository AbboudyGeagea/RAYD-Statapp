#!/usr/bin/env python3
"""
scripts/sample_data/export_sample.py — a sample of RAYD's Postgres for a developer laptop.

READ-ONLY. Every query runs inside a READ ONLY transaction, so nothing on the server can
change. Runs on the RAYD server, inside the app container (psycopg2 and the POSTGRES_*
variables are there). Self-contained: copy it in if the server's code is older.

    docker cp scripts/sample_data/export_sample.py rayd_service:/tmp/
    docker exec rayd_service python /tmp/export_sample.py --from 2026-10-01 --to 2026-10-14
    docker cp rayd_service:/tmp/rayd_sample.tar.gz .
    docker exec rayd_service rm /tmp/rayd_sample.tar.gz /tmp/export_sample.py

What it takes (one CSV per table, plus manifest.json):
    reference tables, whole: devices, procedures, RIS catalogues, schedules, settings
        (passwords, keys, tokens and the licence left out)
    the PACS studies of the period, their series and patients, the storage summary
    the RIS orders scheduled in the period or linked to those studies, with their steps,
        status history, visits, RIS patients, PPS, RIS reports
    the ORU reports of those accessions or the period, and their NLP rows
What it never takes: logins and access, HL7 message archives and the HL7 tables that hold
patient names, BRAYN, CRN (phone numbers), CD log, audit logs, raw images.

Patient names, phone numbers, e-mail addresses and raw HL7 are never exported. With
--pseudo (the default) identifiers are also replaced: patient IDs, accession numbers and
DICOM UIDs by a hash (the same value gives the same hash in every table, so the sample
still joins; the salt is random and not kept, so it cannot be reversed), birth dates by
1 January of the year, report text by nothing. --no-pseudo keeps them, for comparing with
PACS and the RIS; then the file holds patient data and must be handled as such.
"""
import argparse
import csv
import io
import json
import os
import secrets
import shutil
import sys
import tarfile
from datetime import date, datetime, timedelta

import psycopg2

# ── what is taken ─────────────────────────────────────────────────────────────
#
# The keys of the period are worked out once, each with a plain query, and every table is
# then filtered with "= ANY(those keys)". The first version nested the selections inside
# each other with IN (...) OR ...; Postgres cannot turn an IN under an OR into a join, and
# on production (507k ORU reports) it re-ran the inner query for every row: the export
# never got past hl7_oru_reports (2026-11-15).

# (name, Postgres array type, query): later queries use earlier keys
KEYS = [
    ('studies', 'bigint[]',
     "SELECT study_db_uid FROM etl_didb_studies WHERE study_date BETWEEN %(f)s AND %(t)s"),
    ('patients', 'bigint[]',
     "SELECT DISTINCT patient_db_uid FROM etl_didb_studies "
     "WHERE study_db_uid = ANY(%(studies)s::bigint[]) AND patient_db_uid IS NOT NULL"),
    ('orders', 'bigint[]',
     "SELECT order_dbid FROM etl_orders WHERE scheduled_datetime >= %(f)s AND scheduled_datetime < %(t1)s "
     "UNION SELECT order_dbid FROM etl_orders WHERE study_db_uid = ANY(%(studies)s::bigint[])"),
    ('accessions', 'text[]',
     "SELECT accession_number FROM etl_didb_studies "
     "WHERE study_db_uid = ANY(%(studies)s::bigint[]) AND accession_number IS NOT NULL "
     "UNION SELECT accession_number FROM etl_orders "
     "WHERE order_dbid = ANY(%(orders)s::bigint[]) AND accession_number IS NOT NULL"),
    ('ris_patients', 'text[]',
     "SELECT DISTINCT patient_dbid::text FROM etl_orders "
     "WHERE order_dbid = ANY(%(orders)s::bigint[]) AND patient_dbid IS NOT NULL"),
    ('visits', 'text[]',
     "SELECT DISTINCT visit_dbid::text FROM etl_orders "
     "WHERE order_dbid = ANY(%(orders)s::bigint[]) AND visit_dbid IS NOT NULL"),
    ('pps', 'text[]',
     "SELECT pps_key::text FROM std_pps WHERE study_db_uid = ANY(%(studies)s::bigint[]) "
     "UNION SELECT pps_key::text FROM std_pps WHERE created_date >= %(f)s AND created_date < %(t1)s"),
    ('oru', 'bigint[]',
     "SELECT id FROM hl7_oru_reports WHERE accession_number = ANY(%(accessions)s::text[]) "
     "UNION SELECT id FROM hl7_oru_reports WHERE result_datetime >= %(f)s AND result_datetime < %(t1)s"),
]

# (table, WHERE or None for the whole table), in the order the import loads them
PLAN = [
    # reference
    ('site_org_map', None), ('aetitle_modality_map', None), ('device_weekly_schedule', None),
    ('device_exceptions', None), ('procedure_canonical_groups', None), ('procedure_duration_map', None),
    ('procedure_canonical_members', None), ('procedure_duplicate_candidates', None),
    ('procedure_fuzzy_candidates', None), ('procedure_modality_conflicts', None),
    ('physician_alias_map', None), ('go_live_config', None), ('pacs_user_role_override', None),
    ('std_pacs_user_groups', None), ('std_resources_ris', None), ('std_status_ris', None),
    ('std_procedure_priorities', None), ('std_ordering_organizations', None), ('std_dictations', None),
    ('std_modality_exceptions', None), ('std_modality_schedule_groups', None), ('std_schedule_schemes', None),
    ('std_schedule_templates', None), ('std_schedule_template_versions', None),
    ('std_schedule_template_items', None), ('std_availability_indicators', None),
    ('std_device_weekly_availability', None), ('std_device_weekly_windows', None),
    ('worklist_status_map', None), ('financial_config', None), ('oru_diagnosis_vocabulary', None),
    ('settings', "NOT (key ~* '(password|secret|token|licen[cs]e|oauth|smtp_|worklist_db_|api_key|served_site)')"),
    # PACS
    ('etl_patient_view', "patient_db_uid = ANY(%(patients)s::bigint[])"),
    ('etl_didb_studies', "study_db_uid = ANY(%(studies)s::bigint[])"),
    ('etl_didb_serieses', "study_db_uid = ANY(%(studies)s::bigint[])"),
    ('summary_storage_daily', "study_date BETWEEN %(f)s AND %(t)s"),
    ('fact_exam', "study_db_uid = ANY(%(studies)s::bigint[])"),
    ('etl_sticky_notes', "study_db_uid = ANY(%(studies)s::bigint[])"),
    ('study_access_history', "study_db_uid = ANY(%(studies)s::bigint[])"),
    # RIS
    ('std_patients_ris', "patient_person_key::text = ANY(%(ris_patients)s::text[])"),
    ('std_patient_ids', "patient_person_key::text = ANY(%(ris_patients)s::text[])"),
    ('std_visits', "visit_key::text = ANY(%(visits)s::text[])"),
    ('etl_orders', "order_dbid = ANY(%(orders)s::bigint[])"),
    ('std_worklist_scheduled', "site_worklist_key = ANY(%(orders)s::bigint[])"),
    ('std_worklist_arrivals', "site_worklist_key = ANY(%(orders)s::bigint[])"),
    ('std_worklist_exam_done', "site_worklist_key = ANY(%(orders)s::bigint[])"),
    ('std_worklist_cancellations', "site_worklist_key = ANY(%(orders)s::bigint[])"),
    ('worklist_status_history', "site_worklist_key = ANY(%(orders)s::bigint[])"),
    ('std_pps', "pps_key::text = ANY(%(pps)s::text[])"),
    ('std_pps_person_reference', "pps_key::text = ANY(%(pps)s::text[])"),
    ('std_reports', "reported_acc_number = ANY(%(accessions)s::text[])"),
    # ORU
    ('hl7_oru_reports', "id = ANY(%(oru)s::bigint[])"),
    ('hl7_oru_analysis', "report_id = ANY(%(oru)s::bigint[])"),
    ('ai_nlp_cache', "source_id = ANY(%(oru)s::bigint[])"),
    # ETL history (Report 37's monthly workbook waits on it)
    ('etl_job_log', "start_time >= %(f)s AND start_time < %(t1)s"),
]

# A query that runs longer than this stops the export with a clear error instead of hanging.
STATEMENT_TIMEOUT = '20min'

# Never exported, whatever the options: names, contact details, raw HL7.
ALWAYS_NULL = {'patient_name', 'raw_message', 'mobile_phone_number', 'pager_number', 'primary_email_address',
               'secondary_email_address', 'patient_phone_raw', 'patient_phone_normalized', 'other_phone_raw',
               'email_id', 'phone_number', 'email', 'phone', 'phone_raw', 'phone_e164', 'birth_time'}
# With --pseudo: hashed (identifiers), first of the year (birth dates), emptied (free text).
HASHED = {'accession_number', 'reported_acc_number', 'linked_accession_number', 'main_accession', 'patient_id',
          'study_instance_uid', 'series_instance_uid', 'pps_instance_uid', 'visit_number', 'preadmit_number',
          'patient_account_number', 'alternate_visit_id', 'fallback_id'}
HASHED_BY_TABLE = {('etl_patient_view', 'id')}
BIRTH = {'birth_date', 'date_of_birth', 'death_date'}
EMPTIED = {'report_text', 'impression_text', 'document_plain_text'}


def expression(table, column, pseudo, salt_param):
    q = '"' + column.replace('"', '""') + '"'
    if column in ALWAYS_NULL:
        return f"NULL AS {q}"
    if not pseudo:
        return q
    if column in HASHED or (table, column) in HASHED_BY_TABLE:
        return (f"CASE WHEN {q} IS NULL OR BTRIM({q}::text) = '' THEN {q}::text "
                f"ELSE 'X' || substr(md5({salt_param} || BTRIM({q}::text)), 1, 12) END AS {q}")
    if column in BIRTH:
        return f"date_trunc('year', {q})::date AS {q}"
    if column in EMPTIED:
        return f"NULL AS {q}"
    return q


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--from', dest='date_from', help='first study date (default: 14 days ago)')
    ap.add_argument('--to', dest='date_to', help='last study date (default: yesterday)')
    ap.add_argument('--out', default='/tmp/rayd_sample', help='folder, then <folder>.tar.gz')
    ap.add_argument('--no-pseudo', dest='pseudo', action='store_false',
                    help='keep patient IDs, accessions, UIDs, birth dates and report text')
    ap.add_argument('--dsn', help='libpq URL (default: the POSTGRES_* environment of the container)')
    args = ap.parse_args()

    t = date.fromisoformat(args.date_to) if args.date_to else date.today() - timedelta(days=1)
    f = date.fromisoformat(args.date_from) if args.date_from else t - timedelta(days=13)
    if f > t:
        sys.exit('--from is after --to')
    dsn = args.dsn or 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=os.environ.get('POSTGRES_USER', 'etl_user'), p=os.environ.get('POSTGRES_PASSWORD', ''),
        h=os.environ.get('POSTGRES_HOST', 'db'), port=os.environ.get('POSTGRES_PORT', '5432'),
        d=os.environ.get('POSTGRES_DB', 'etl_db'))
    params = {'f': f, 't': t, 't1': t + timedelta(days=1), 'salt': secrets.token_hex(16)}

    if os.path.exists(args.out):
        shutil.rmtree(args.out)
    os.makedirs(args.out)
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True)            # nothing on the server can change
    manifest = {'from': f.isoformat(), 'to': t.isoformat(), 'pseudo': args.pseudo,
                'exported_at': datetime.now().isoformat(timespec='seconds'), 'tables': []}
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
            cur.execute("SELECT current_database(), "
                        "(SELECT MAX(name) FROM schema_migrations WHERE name ~ '^[0-9]{4}_')")
            manifest['database'], manifest['last_migration'] = cur.fetchone()
            print(f"Keys of {f} to {t}:", flush=True)
            for name, _kind, sql in KEYS:
                cur.execute("SAVEPOINT k")
                try:
                    cur.execute(sql, params)
                    params[name] = [r[0] for r in cur.fetchall()]
                    cur.execute("RELEASE SAVEPOINT k")
                except psycopg2.errors.UndefinedTable:
                    cur.execute("ROLLBACK TO SAVEPOINT k")
                    params[name] = []
                print(f"  {name:34s} {len(params[name]):>9,}", flush=True)
            print("Tables:", flush=True)
            for table, where in PLAN:
                cur.execute("""SELECT column_name FROM information_schema.columns
                               WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position""",
                            (table,))
                cols = [r[0] for r in cur.fetchall()]
                if not cols:
                    print(f"  {table:34s} not on this server, skipped")
                    continue
                print(f"  {table:34s} ...", end='\r', flush=True)
                select = ', '.join(expression(table, c, args.pseudo, '%(salt)s') for c in cols)
                sql = f"SELECT {select} FROM public.{table}" + (f" WHERE {where}" if where else "")
                inner = cur.mogrify(sql, params).decode()
                buf = io.StringIO()
                cur.copy_expert(f"COPY ({inner}) TO STDOUT WITH (FORMAT csv, HEADER true)", buf)
                data = buf.getvalue()
                with open(os.path.join(args.out, f'{table}.csv'), 'w', encoding='utf-8', newline='') as fh:
                    fh.write(data)
                rows = sum(1 for _ in csv.reader(io.StringIO(data))) - 1 if data else 0
                manifest['tables'].append({'table': table, 'rows': rows, 'columns': cols})
                print(f"  {table:34s} {rows:>9,} rows", flush=True)
        conn.rollback()
    finally:
        conn.close()
    with open(os.path.join(args.out, 'manifest.json'), 'w', encoding='utf-8') as fh:
        json.dump(manifest, fh, indent=2)
    archive = args.out.rstrip('/') + '.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(args.out, arcname=os.path.basename(args.out.rstrip('/')))
    shutil.rmtree(args.out)
    print(f"\n{archive}  ({f} to {t}, pseudonymised: {'yes' if args.pseudo else 'NO'})")


if __name__ == '__main__':
    main()
