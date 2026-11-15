"""
db_bootstrap.py — loads init-db/schema.sql into an empty database, as the Postgres
container's entrypoint does for the main database on first start.

Used for a site's own database (utils/site_db.py, item 13a: the SJH database is created by
the app, so no entrypoint ever loads it) and by the test suite. Migrations run afterwards
(db_migrations.run_migrations_on).
"""
import io
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(ROOT, 'init-db', 'schema.sql')

BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS hl7_oru_reports (
    id               SERIAL PRIMARY KEY,
    procedure_code   VARCHAR(100),
    procedure_name   TEXT,
    modality         VARCHAR(20),
    physician_id     VARCHAR(100),
    patient_id       VARCHAR(100),
    accession_number VARCHAR(100),
    report_text      TEXT,
    impression_text  TEXT,
    result_datetime  TIMESTAMP,
    received_at      TIMESTAMP DEFAULT NOW()
);
-- create_app() adds these on the main database after the migrations (app.py, "MIGRATE");
-- db.User reads them
ALTER TABLE users ADD COLUMN IF NOT EXISTS ui_theme VARCHAR DEFAULT 'dark';
ALTER TABLE users ADD COLUMN IF NOT EXISTS favorites TEXT DEFAULT '[]'
"""


def load_schema(dsn, schema_path=SCHEMA):
    """
    init-db/schema.sql through psycopg2, doing what psql would: skip meta-commands
    (\\restrict from pg_dump 14.19+) and feed each `COPY ... FROM stdin` block its data
    through copy_expert. Ownership statements are dropped: the objects belong to whoever
    connects. Then the tables create_app() makes before and after the migrations.
    dsn: a libpq URL (postgresql://user:pass@host:port/db).
    """
    import psycopg2
    with open(schema_path, encoding='utf-8') as f:
        lines = f.read().splitlines()

    def clean(sql):
        sql = sql.replace("set_config('search_path', '', false)",
                          "set_config('search_path', 'public', false)")
        return re.sub(r'^ALTER [^;]* OWNER TO [^;]*;$', '', sql, flags=re.M)

    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    def run(chunk):
        sql = clean('\n'.join(chunk))
        if any(l.strip() and not l.strip().startswith('--') for l in sql.splitlines()):
            cur.execute(sql)

    try:
        with conn.cursor() as cur:
            pending, i = [], 0
            while i < len(lines):
                line = lines[i]
                if line.startswith('COPY ') and line.rstrip().endswith('FROM stdin;'):
                    run(pending)
                    pending, data = [], []
                    i += 1
                    while i < len(lines) and lines[i] != '\\.':
                        data.append(lines[i])
                        i += 1
                    cur.copy_expert(line.rstrip()[:-1],
                                    io.StringIO('\n'.join(data) + '\n' if data else ''))
                elif not line.startswith('\\'):
                    pending.append(line)
                i += 1
            run(pending)
            cur.execute(BOOTSTRAP_SQL)
    finally:
        conn.close()
