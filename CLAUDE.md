# RAYD-Statapp — Agent Context

## Project
Flask/PostgreSQL radiology statistics platform for a medical imaging center (Intermedic, Beirut, Lebanon).
Pulls DICOM study data from a PACS Oracle DB via ETL, receives HL7 ORU radiology reports, and serves
analytics dashboards for radiologists and administrators.

## Stack
| Layer | Tech |
|-------|------|
| App server | Flask 3 + Gunicorn, Python 3.11 |
| ORM | Flask-SQLAlchemy (SQLAlchemy 2) |
| Database | PostgreSQL 15 (`rayd_db` container) |
| NLP worker | medspaCy in separate `rayd_nlp` container |
| Reverse proxy | nginx (`rayd_proxy` container) |
| ETL source | Oracle PACS via `oracledb` (cx_Oracle compatible) |
| Scheduling | APScheduler (inside main container) |

## Containers
```
rayd_proxy   nginx:stable-alpine        — TLS termination, ports 80/443
rayd_service python:3.11 (Dockerfile)   — Flask app, port 6661 internal
rayd_nlp     python:3.11 (nlp_worker/)  — medspaCy batch processor, no exposed ports
rayd_db      postgres:15                — primary DB, port 5432 (dev: exposed to host)
```

## Key Files
```
app.py                          — app factory, blueprint registration, APScheduler jobs
db.py                           — SQLAlchemy setup, ORM models, permission helpers
routes/
  report_22.py                  — main radiology stats (date/modality/physician filters)
  super_report.py               — aggregated multi-section report
  report_25.py                  — shift & device utilization
  er_dashboard.py               — ER unread studies panel + SLA tracking
  sticky_notes.py               — JCI urgent emergent report (card next to ER in Clinical KPIs; page
                                   permission `sticky_notes`), worked out in utils/sticky_notes.py,
                                   whose docstring holds the customer's request. Reads Postgres
                                   only, a day late. PACS keeps no history of opens RAYD can read,
                                   so "first open seen" is the earliest open a nightly copy caught
                                   (blank if caught over 36 h after the study was last seen
                                   unopened), and only the ETL may read the PACS server, so never
                                   poll it.
                                   ETL_JOBS/etl_sticky_notes.py copies note times nightly; note
                                   text is never read. Its two tables are read through a study, so
                                   no rh view. utils/sticky_notes_monthly.py keeps each month's
                                   workbook from the 2nd and emails it through the mail server
                                   (email_server.py). Migrations 0143, 0144, 0146
  oru_analytics.py              — HL7 ORU analytics (NLP word cloud, critical findings log)
  viewer_controller.py          — daily briefing, home dashboard
  mapping_controller.py         — AE/modality/procedure mapping config (lazy-loaded tabs)
  hl7_orders_route.py           — HL7 order analytics
  department_board.py           — Live Department board (/viewer/department), ported from HL7
                                   (2026-11-02): per device, study in progress + expected
                                   finish, who waits, what is coming, utilization. Reads
                                   brayn_study_state only (data in utils/department_board.py),
                                   on hospital time — EMPTY while BRAYN is off; no waiting /
                                   in progress until the R2I lifecycle feed is live.
                                   /viewer/live (live_feed, RIS SPS via Oracle) redirects here.
                                   Licence + page permission `live_feed`
  worklists.py                  — Report Composer (/report-composer; RAYD V2.0 editor from HL7 via
                                   Bekaa, 2026-11-02). Renamed 2026-11-04: Philips uses its earlier
                                   name, which must appear nowhere, on any branch (see
                                   tests/test_report_composer_name.py). Tables, code and API paths
                                   still say worklist. "Worklist" in a RIS sense stays as it is.
                                   REPLACED Custom Reports (custom_reports.py,
                                   report_widgets.py and 3 templates removed; same licence key +
                                   page permission `custom_reports`; /reports/custom redirects).
                                   Reads only the rpt.* views of migration 0136: rpt.exams (RIS
                                   exam from the Oracle RIS ETL, Report 35's definitions) + Bekaa's
                                   PACS views (studies, series, reports, findings, storage, crn).
                                   Catalog in utils/worklist_catalog.py (tests fail if a view
                                   column is undocumented); SQL runs only as the rpt_<db> role.
                                   Scheduled emails (admin; migration 0138): utils/worklist_email.py
                                   mails saved reports as .xlsx through settings smtp_*, only to
                                   smtp_allowed_domains. Off until the relay is set. Microsoft 365
                                   signs in with OAuth (client credentials, XOAUTH2; migration 0140,
                                   docs/LAUMC_O365_EMAIL_SETUP.md)
  email_server.py               — Administration > Email Server (/admin/email-server), admin only,
                                   no licence: the one mail server (settings smtp_*) that Report
                                   Composer's schedules and the Sticky Notes monthly report send
                                   through. Moved out of Report Composer 2026-11-07
  report_cache.py               — shared cache/dropdown helpers
hl7_listener.py                 — MLLP :6661: stores ORU (fill-only), orders, PACS completions;
                                   then hands each message to BRAYN AFTER the ACK
utils/brayn_shadow.py           — BRAYN on LAUMC, OBSERVING ONLY (migration 0133, 2026-10-26):
                                   archive after the ACK, background worker screens into
                                   BRAYN's own tables, 15-min sweep. Off until
                                   settings.brayn_enabled = 'true'. Never writes etl_*,
                                   hl7_orders or hl7_oru_reports; never holds a message
                                   ('quarantined' = "not identified"). Its transactions run
                                   on hospital time via clock() — LAUMC's Postgres is UTC.
routes/brayn_console.py         — BRAYN console (/brayn, Administration → BRAYN), ADMIN ONLY:
                                   on/off switch, To fix + Re-check, messages, study
                                   timeline, notes, settings. Writes BRAYN tables only
                                   (status map, accession field, steps, checks); devices
                                   stay in Mapping. "Not identified" = HL7's "held"
utils/brayn*.py, hl7_parse.py, hl7_fieldmap.py, hl7_dictionary.py, hl7_ingest.py
                                — the BRAYN engine, hand-ported unchanged from the HL7
                                   branch (2026-10-26); hl7_ingest is used for archive +
                                   persist_parsed only (no projector on LAUMC)
tests/                          — pytest, own DB rayd_pytest_laumc: listener baseline
                                   (what LAUMC stores today) + BRAYN shadow, incl. "LAUMC
                                   writes the same with BRAYN on or off"
ETL_JOBS/
  etl_job.py                    — Oracle → PostgreSQL ETL pipeline
  etl_analytics_refresh.py      — storage summary rollup (Phase 7 of the main ETL sync,
                                   `daily_etl_sync`, runs nightly at 05:00 — NOT the
                                   separate 05:30 `daily_analytics_snapshot` job, which
                                   runs ETL_JOBS/daily_analytics.py instead, populating
                                   analytics_snapshots for the daily briefing)
nlp_worker/worker.py            — standalone medspaCy batch loop (polls every 60s)
migrations/NNNN_*.sql           — schema migrations (canonical source of truth)
init-db/schema.sql              — initial schema applied by docker-entrypoint
install.sh                      — full production install script
```

## DB Schema

### Core ETL tables
```
etl_didb_studies
  study_db_uid BIGINT PK, patient_db_uid, study_date DATE, study_modality VARCHAR(50),
  storing_ae TEXT, accession_number TEXT, number_of_study_images INT,
  report_status TEXT, order_status TEXT, study_has_report BOOL,
  rep_final_timestamp TIMESTAMP, rep_final_signed_by TEXT,
  reading_physician_id BIGINT, reading_physician_first_name TEXT, reading_physician_last_name TEXT,
  referring_physician_first_name TEXT, referring_physician_last_name TEXT,
  rep_prelim_timestamp TIMESTAMP, patient_class TEXT, patient_location VARCHAR(3),
  study_description TEXT, study_body_part TEXT, age_at_exam NUMERIC(5,2)

etl_patient_view
  patient_db_uid BIGINT PK, patient_id TEXT, patient_name TEXT, dob DATE, sex TEXT,
  patient_class TEXT, patient_location TEXT

etl_orders
  order_dbid BIGINT PK, patient_dbid TEXT, study_db_uid BIGINT,
  proc_id TEXT, proc_text TEXT, scheduled_datetime TIMESTAMP,
  order_status TEXT, modality TEXT, has_study BOOL, order_control TEXT

etl_didb_serieses       — DICOM series detail (series_db_uid, study_db_uid, modality, body_part_examined)
etl_didb_raw_images     — DICOM image detail (raw_image_db_uid, study_db_uid, series_db_uid)
etl_image_locations     — image file sizes (raw_image_db_uid, image_size_kb, file_system)
etl_job_log             — ETL run history (job_name, status, start_time, records_processed, error_message)
```

### HL7 tables
```
hl7_oru_reports
  id SERIAL PK, accession_number TEXT, patient_id TEXT,
  procedure_code TEXT, procedure_name TEXT, modality TEXT,
  physician_id TEXT, report_text TEXT, impression_text TEXT,
  result_datetime TIMESTAMP, received_at TIMESTAMP DEFAULT now()

hl7_oru_analysis        — written by nlp-worker; never update directly from main app
  id SERIAL PK, report_id INT → hl7_oru_reports(id) UNIQUE,
  affirmed_labels TEXT[], is_critical BOOL, nlp_version TEXT, analyzed_at TIMESTAMP

hl7_orders
  id SERIAL PK, accession_number TEXT, patient_id TEXT,
  procedure_code TEXT, modality TEXT, order_date TIMESTAMP,
  completed_at TIMESTAMP, pacs_done_at TIMESTAMP,
  patient_class VARCHAR, patient_location VARCHAR
```

### Configuration
```
aetitle_modality_map    — AE title → canonical modality
  id SERIAL PK, aetitle VARCHAR NOT NULL, modality VARCHAR NOT NULL,
  daily_capacity_minutes INT DEFAULT 480

db_params               — external DB connections (Oracle PACS source, etc.)
  id SERIAL PK, name VARCHAR(100) UNIQUE, db_role VARCHAR, db_type VARCHAR,
  host VARCHAR, port INT, sid VARCHAR, username VARCHAR, password VARCHAR (encrypted),
  conn_string TEXT, mode VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP

settings                — key/value store for app config and license
  key TEXT PK, value TEXT
  Notable keys: license (JSON), demo_mode, demo_start, demo_end, demo_user,
                shift_morning_start/end, shift_afternoon_start/end, shift_night_start/end,
                oru_crit:<keyword> (custom critical NLP terms)

go_live_config          — ETL minimum date; ETL ignores studies before this date
  id SERIAL PK, go_live_date DATE

device_exceptions       — per-day capacity overrides for specific AE titles
  id, aetitle, exception_date DATE, actual_opening_minutes INT, reason VARCHAR

device_weekly_schedule  — standard weekly schedule per AE title
  aetitle VARCHAR, day_of_week INT (0=Mon–6=Sun), std_opening_minutes INT DEFAULT 720
```

### Users / Auth
```
users
  id SERIAL PK, username VARCHAR UNIQUE, email VARCHAR, password_hash VARCHAR,
  role VARCHAR (admin | viewer | viewer2), active BOOL DEFAULT true

active_sessions
  session_id VARCHAR PK, user_id INT, role VARCHAR, ip_address VARCHAR,
  login_time TIMESTAMP DEFAULT now()
```

### Procedures / AI clustering
```
procedure_canonical_groups
  id SERIAL PK, canonical_name TEXT, cluster_confidence NUMERIC,
  source TEXT (ai_suggested | manual), approved BOOL DEFAULT false

procedure_canonical_members
  group_id INT → procedure_canonical_groups(id), procedure_code TEXT

procedure_duplicate_candidates
  id SERIAL PK, code_a TEXT, code_b TEXT, status TEXT (pending | merged | dismissed)

procedure_duration_map
  procedure_code TEXT, modality TEXT, avg_duration_minutes NUMERIC

ai_nlp_cache            — TF-IDF / K-means secondary NLP (scikit-learn, main app)
  id SERIAL PK, source_id INT → hl7_oru_reports(id),
  classification VARCHAR(20), keywords JSONB, cluster_id INT,
  cluster_label TEXT, severity_score NUMERIC(3,1), processed_at TIMESTAMP
```

### Analytics
```
analytics_snapshots
  snapshot_date DATE PK, data JSONB

procedure_exceptions    — legacy exceptions table (see device_exceptions for current)
```

## Critical Conventions

0. **RH only** — SJH appears in nothing on this branch (operator, 2026-11-05), and SJH rows are
   not deleted from RAYD. Web requests read filtered views in schema `rh` through a
   transaction-local search_path (`utils/rh_scope.py`, migration 0145); the ETL, listener, NLP worker
   and CRN read the real tables. A new table holding SJH rows needs a view in `rh_refresh_views()`.
   Code that runs in a request creates tables as `public.x`. A migration that drops or retypes a
   column of a filtered table must drop its `rh` view first. To decide a study's site use
   `utils/report_filters.study_site_sql`, never the PACS label alone: double reading stamps RH
   mammography with PACS site '1'. Lists read from `aetitle_modality_map` use `rh_device_sql`.
1. **DB changes** — always via `migrations/NNNN_description.sql`. Never run DDL from psql CLI directly.
2. **SR exclusion** — every query touching `etl_didb_studies` must filter:
   `COALESCE(m.modality, s.study_modality, '') != 'SR'`
   (SR = Structured Report; auto-generated by PACS, not a real study)
3. **Modality source** — prefer `aetitle_modality_map.modality` over `study_modality`; fall back to `study_modality` if no mapping exists.
4. **Expensive CTEs** — `etl_orders` scans with `MODE() WITHIN GROUP` must use `WITH ... AS MATERIALIZED`.
5. **hl7_oru_analysis** — written only by `nlp_worker/worker.py`; route handlers read it but never write it.
6. **Password encryption** — Oracle and external DB passwords are encrypted via `utils/crypto.py` using `SECRET_KEY`.
7. **BRAYN observes, never decides** — nothing in utils/brayn_shadow.py may write LAUMC's
   own tables or run before the listener's ACK; every entry point is fail-open. Never
   truncate `brayn_rules`, `hl7_status_map` or `hl7_result_status_map` (seeded config).
   RIS-dependent checks (ORPHAN_EVENT, SKIPPED_RUNG, ORPHAN_ORDER, STALLED_*) are seeded
   OFF until the ATH real-time R2I feed is live.

## Dev Workflow

```bash
# Start stack with DB port exposed (needed for MCP postgres server)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Tail logs
docker compose logs -f rayd-app
docker compose logs -f rayd-nlp

# Run ETL sync manually
docker compose exec rayd-app python app.py -m

# Connect to DB (password from .env)
psql "postgresql://etl_user:PASSWORD@localhost:5432/etl_db"

# Apply a migration
docker exec rayd_db psql -U etl_user -d etl_db \
  -f /docker-entrypoint-initdb.d/migrations/NNNN_name.sql

# Rebuild a single service
docker compose build rayd-app && docker compose up -d rayd-app

# Check NLP worker health
docker compose logs rayd-nlp --tail 20

# Test suite (any reachable Postgres; creates and drops rayd_pytest_laumc, never etl_db)
python -m pytest tests
```

## MCP Servers (for Claude Code agents)

| Server | Purpose |
|--------|---------|
| `rayd-postgres` | Direct SQL queries against the live DB |
| `claude-flow` | Ruflo multi-agent swarm orchestration (60+ specialist agents) |

Start the dev stack first (`docker-compose.dev.yml`) so port 5432 is accessible, then Claude Code
picks up `.mcp.json` automatically when you open this directory.
