#!/usr/bin/env python3
"""
scripts/sample_data/import_sample.py — loads a sample made by export_sample.py into the
LOCAL stack's main database (etl_db), replacing what those tables hold.

It refuses any database that is not a local stack: the PACS connection (db_params
oracle_PACS) must point at oracle-disabled.invalid, as scripts/local_stack.md sets it, and
the database must not be a site database (settings.served_site). Production never has that
host, so this cannot wipe a server.

    docker cp rayd_sample.tar.gz rayd_pytest_runner:/tmp/
    docker exec rayd_pytest_runner python /src/scripts/sample_data/import_sample.py /tmp/rayd_sample.tar.gz

(or from rayd_service once its image holds this script). The DSN comes from POSTGRES_*
or --dsn. Tables are emptied and loaded in the export's order. A table missing locally is
skipped; a column missing on either side is skipped too (the server may be at an older
migration than the laptop). settings rows are added or updated, never removed. Afterwards
the RH-only views are refreshed; the app copies the shared procedure codes to the SJH
database within 15 minutes (or at its next start).
"""
import argparse
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile

import psycopg2

LOCAL_PACS_HOST = 'oracle-disabled.invalid'


def q(name):
    return '"' + name.replace('"', '""') + '"'


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('sample', help='rayd_sample.tar.gz, or the folder it unpacks to')
    ap.add_argument('--dsn', help='libpq URL (default: POSTGRES_* environment, database etl_db)')
    args = ap.parse_args()

    dsn = args.dsn or 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=os.environ.get('POSTGRES_USER', 'etl_user'), p=os.environ.get('POSTGRES_PASSWORD', ''),
        h=os.environ.get('POSTGRES_HOST', 'localhost'), port=os.environ.get('POSTGRES_PORT', '5432'),
        d=os.environ.get('POSTGRES_DB', 'etl_db'))

    work = None
    folder = args.sample
    if os.path.isfile(args.sample):
        work = tempfile.mkdtemp(prefix='rayd_sample_')
        with tarfile.open(args.sample, 'r:gz') as tar:
            tar.extractall(work, filter='data')
        inner = [d for d in os.listdir(work) if os.path.isdir(os.path.join(work, d))]
        folder = os.path.join(work, inner[0]) if inner else work
    try:
        with open(os.path.join(folder, 'manifest.json'), encoding='utf-8') as fh:
            manifest = json.load(fh)
        load(dsn, folder, manifest)
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)


def load(dsn, folder, manifest):
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        cur.execute("SELECT host FROM db_params WHERE name = 'oracle_PACS'")
        row = cur.fetchone()
        if not row or row[0] != LOCAL_PACS_HOST:
            sys.exit(f"Refused: db_params oracle_PACS host is {row[0] if row else None!r}, not "
                     f"{LOCAL_PACS_HOST!r}. This is not a local stack (scripts/local_stack.md).")
        cur.execute("SELECT 1 FROM settings WHERE key = 'served_site'")
        if cur.fetchone():
            sys.exit("Refused: this is a site database (settings.served_site); load the main one (etl_db).")
        print(f"Sample {manifest['from']} to {manifest['to']} from {manifest.get('database')} "
              f"(migration {manifest.get('last_migration')}), pseudonymised: {manifest['pseudo']}")

        plan = []
        for t in manifest['tables']:
            table = t['table']
            # generated columns are computed here, never loaded
            cur.execute("""SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attgenerated <> '',
                                  a.attidentity = 'a'
                           FROM pg_attribute a
                           WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped""",
                        (f'public.{table}',))
            found = cur.fetchall()
            if not found:
                print(f"  {table:34s} not in this database, skipped")
                continue
            local = {name: kind for name, kind, generated, _ in found if not generated}
            always_identity = any(ident for *_, ident in found)
            common = [c for c in t['columns'] if c in local]
            plan.append((table, t['columns'], common, local, always_identity))

        data_tables = [p[0] for p in plan if p[0] != 'settings']
        if data_tables:
            cur.execute("TRUNCATE " + ', '.join(f'public.{q(t)}' for t in data_tables) + " CASCADE")
        for table, cols, common, local, always_identity in plan:
            path = os.path.join(folder, f'{table}.csv')
            with open(path, encoding='utf-8', newline='') as fh:
                data = fh.read()
            if not data.strip():
                print(f"  {table:34s}         0 rows")
                continue
            cur.execute("DROP TABLE IF EXISTS _sample")
            cur.execute("CREATE TEMP TABLE _sample (" + ', '.join(f'{q(c)} text' for c in cols) + ")")
            cur.copy_expert("COPY _sample FROM STDIN WITH (FORMAT csv, HEADER true)", io.StringIO(data))
            target = ', '.join(q(c) for c in common)
            values = ', '.join(f'{q(c)}::{local[c]}' for c in common)
            if table == 'settings':
                cur.execute(f"INSERT INTO public.settings ({target}) SELECT {values} FROM _sample "
                            f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
            else:
                overriding = ' OVERRIDING SYSTEM VALUE' if always_identity else ''
                cur.execute(f"INSERT INTO public.{q(table)} ({target}){overriding} SELECT {values} FROM _sample")
            print(f"  {table:34s} {cur.rowcount:>9,} rows"
                  + (f"   (skipped columns: {', '.join(c for c in cols if c not in local)})"
                     if len(common) < len(cols) else ''))
            if 'id' in local:
                cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (f'public.{table}',))
                seq = cur.fetchone()[0]
                if seq:
                    cur.execute(f"SELECT setval(%s, COALESCE((SELECT MAX(id) FROM public.{q(table)}), 0) + 1, false)",
                                (seq,))
        cur.execute("SELECT to_regproc('rh_refresh_views') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("SELECT rh_refresh_views()")
        conn.commit()
        print("Done. RH-only views refreshed.")
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
