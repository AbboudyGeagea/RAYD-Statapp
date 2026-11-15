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

S = "SELECT study_db_uid FROM etl_didb_studies WHERE study_date BETWEEN %(f)s AND %(t)s"
P = "SELECT patient_db_uid FROM etl_didb_studies WHERE study_date BETWEEN %(f)s AND %(t)s"
O = (f"SELECT order_dbid FROM etl_orders WHERE (scheduled_datetime >= %(f)s "
     f"AND scheduled_datetime < %(t1)s) OR study_db_uid IN ({S})")
ACC = (f"SELECT accession_number FROM etl_didb_studies WHERE study_date BETWEEN %(f)s AND %(t)s "
       f"AND accession_number IS NOT NULL UNION "
       f"SELECT accession_number FROM etl_orders WHERE order_dbid IN ({O}) AND accession_number IS NOT NULL")
RISPAT = f"SELECT patient_dbid FROM etl_orders WHERE order_dbid IN ({O})"
PPS = f"SELECT pps_key FROM std_pps WHERE study_db_uid IN ({S}) OR (created_date >= %(f)s AND created_date < %(t1)s)"
ORU = (f"SELECT id FROM hl7_oru_reports WHERE accession_number IN ({ACC}) "
       f"OR (result_datetime >= %(f)s AND result_datetime < %(t1)s)")

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
    ('etl_patient_view', f"patient_db_uid IN ({P})"),
    ('etl_didb_studies', "study_date BETWEEN %(f)s AND %(t)s"),
    ('etl_didb_serieses', f"study_db_uid IN ({S})"),
    ('summary_storage_daily', "study_date BETWEEN %(f)s AND %(t)s"),
    ('fact_exam', f"study_db_uid IN ({S})"),
    ('etl_sticky_notes', f"study_db_uid IN ({S})"),
    ('study_access_history', f"study_db_uid IN ({S})"),
    # RIS
    ('std_patients_ris', f"patient_person_key::text IN ({RISPAT})"),
    ('std_patient_ids', f"patient_person_key::text IN ({RISPAT})"),
    ('std_visits', f"visit_key::text IN (SELECT visit_dbid::text FROM etl_orders WHERE order_dbid IN ({O}))"),
    ('etl_orders', f"order_dbid IN ({O})"),
    ('std_worklist_scheduled', f"site_worklist_key IN ({O})"),
    ('std_worklist_arrivals', f"site_worklist_key IN ({O})"),
    ('std_worklist_exam_done', f"site_worklist_key IN ({O})"),
    ('std_worklist_cancellations', f"site_worklist_key IN ({O})"),
    ('worklist_status_history', f"site_worklist_key IN ({O})"),
    ('std_pps', f"pps_key IN ({PPS})"),
    ('std_pps_person_reference', f"pps_key IN ({PPS})"),
    ('std_reports', f"reported_acc_number IN ({ACC})"),
    # ORU
    ('hl7_oru_reports', f"id IN ({ORU})"),
    ('hl7_oru_analysis', f"report_id IN ({ORU})"),
    ('ai_nlp_cache', f"source_id IN ({ORU})"),
    # ETL history (Report 37's monthly workbook waits on it)
    ('etl_job_log', "start_time >= %(f)s AND start_time < %(t1)s"),
]

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
            cur.execute("SELECT current_database(), "
                        "(SELECT MAX(name) FROM schema_migrations WHERE name ~ '^[0-9]{4}_')")
            manifest['database'], manifest['last_migration'] = cur.fetchone()
            for table, where in PLAN:
                cur.execute("""SELECT column_name FROM information_schema.columns
                               WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position""",
                            (table,))
                cols = [r[0] for r in cur.fetchall()]
                if not cols:
                    print(f"  {table:34s} not on this server, skipped")
                    continue
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
                print(f"  {table:34s} {rows:>9,} rows")
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
