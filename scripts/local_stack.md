# LAUMC on a laptop: the local stack, two databases

The local Docker stack runs the LAUMC branch as LAUMC will run: `etl_db` (RH, logins and
access) and `etl_db_sjh` (SJH's own database, item 13a), on one Postgres.

## Start

```bash
# once: .env (git-ignored) with POSTGRES_*, SECRET_KEY, MASTER_ADMIN_KEY, and
#   POSTGRES_DB_SJH=etl_db_sjh
#   RAYD_ETL_LOOKUP_FROM_PACS=false
# and nginx/certs/fullchain.pem + privkey.pem (self-signed is fine)

docker compose -p rayd-laumc -f docker-compose.yml -f docker-compose.local.yml up -d db
```

**Before the app's first start on an empty database, point the PACS connection at
nothing.** The app starts an ETL by itself when `etl_didb_studies` is empty, and migration
0004 seeds the hospital's PACS address. `.invalid` never resolves, so a laptop can never
reach the PACS (and `import_sample.py` only loads a database marked this way):

```bash
docker exec rayd_db psql -U etl_user -d etl_db -c "INSERT INTO db_params (name, db_role, db_type, host, username, password, port, sid, mode) VALUES ('oracle_PACS', 'source', 'oracle', 'oracle-disabled.invalid', 'sys', '', 1521, 'mst1', 'SYSDBA') ON CONFLICT (name) DO UPDATE SET host = 'oracle-disabled.invalid';"

docker compose -p rayd-laumc -f docker-compose.yml -f docker-compose.local.yml up -d --build

# the app created etl_db_sjh at start: mark it too
docker exec rayd_db psql -U etl_user -d etl_db_sjh -c "UPDATE db_params SET host = 'oracle-disabled.invalid' WHERE name = 'oracle_PACS';"
```

Then https://localhost (self-signed certificate). Postgres is on 127.0.0.1:5432. The
built-in `admin` account is the Super user; give it a password with:

```bash
docker exec -e NEWPW='choose-one' rayd_service python -c "import os,psycopg2;from werkzeug.security import generate_password_hash as h;c=psycopg2.connect(host=os.environ['POSTGRES_HOST'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],dbname=os.environ['POSTGRES_DB']);c.cursor().execute(\"UPDATE users SET password_hash=%s, must_change_password=FALSE WHERE username='admin'\",(h(os.environ['NEWPW'],method='pbkdf2:sha256'),));c.commit()"
```

## Tests

```bash
docker run -d --name rayd_pytest_runner --network rayd-laumc_default \
  -v "D:/Intermedic/Statistics app/RAYD-Statapp-LAUMC:/src" -w /src \
  -e POSTGRES_HOST=rayd_db -e POSTGRES_PASSWORD=<from .env> \
  --entrypoint sleep rayd-laumc-rayd-app infinity
MSYS_NO_PATHCONV=1 docker exec rayd_pytest_runner pip install -q -r /src/tests/requirements.txt
MSYS_NO_PATHCONV=1 docker exec -e TZ=Asia/Beirut rayd_pytest_runner python -m pytest tests -q -p no:cacheprovider
```

## Sample data from production

On the RAYD server (read-only; patient IDs, accessions and UIDs hashed, birth dates cut to
the year, report text dropped; never names, phones, raw HL7):

```bash
docker cp scripts/sample_data/export_sample.py rayd_service:/tmp/
docker exec rayd_service python /tmp/export_sample.py --from 2026-10-01 --to 2026-10-14
docker cp rayd_service:/tmp/rayd_sample.tar.gz .
docker exec rayd_service rm /tmp/rayd_sample.tar.gz /tmp/export_sample.py
```

`--no-pseudo` keeps the real identifiers and report text (to compare with PACS or the RIS);
that file then holds patient data. Then on the laptop:

```bash
docker cp rayd_sample.tar.gz rayd_pytest_runner:/tmp/
MSYS_NO_PATHCONV=1 docker exec rayd_pytest_runner python /src/scripts/sample_data/import_sample.py /tmp/rayd_sample.tar.gz
```

The sample is loaded into `etl_db` as production has it today: RH and SJH rows together
(13b, moving SJH's rows to `etl_db_sjh`, can then be rehearsed locally).

## How much SJH data is in RH's database

`scripts/sql/sjh_census.sql`, read-only, on the server:

```bash
docker exec -i rayd_db psql -U etl_user -d etl_db -P pager=off -f - < scripts/sql/sjh_census.sql
```
