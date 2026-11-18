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
utils/roles.py                  — the four roles (Super user, CHT, Administrator, User), their
                                   capabilities, sites and per-site access (item 13f, rule 9)
utils/site_db.py                — SJH in its own database (item 13a, rule 10): POSTGRES_DB_SJH,
                                   per-request routing (db.SiteSession), site switch, user copy
db_bootstrap.py                 — loads init-db/schema.sql into an empty database (site DBs, tests)
ETL_JOBS/etl_site_route.py      — item 13b: SJH's rows from RH's database into SJH's (natural keys,
                                   verified, then deleted from RH); one-time move from the command
                                   line, then the ETL's last step every night (off until the move
                                   recorded itself). A new table holding site rows needs a line in
                                   its TABLES list
routes/site_controller.py       — POST /site/switch, the header's site dropdown
routes/
  report_22.py                  — main radiology stats (date/modality/physician filters)
  super_report.py               — aggregated multi-section report
  report_25.py                  — shift & device utilization
  report_38.py                  — Report 38, the ER dashboard (until 2026-11-14 the page /er,
                                   routes/er_dashboard.py; /er redirects): TAT, target compliance,
                                   unread ER studies; migration 0150
  report_39.py                  — Report 39, Staff status (item 9, migration 0157): everyone on the
                                   RIS and PACS by PACS group and RIS role, who left (inactive on
                                   the RIS and/or PACS, utils/staff_status.py), their activity in
                                   the period, Excel for the manager, and the switch (settings
                                   capability) that makes every report read "Former staff" for them
  report_27.py                  — Report 27, RIS orders vs PACS; its second tab (?tab=his) is HIS
                                   orders vs RIS (item 1, utils/his_ris.py): the HIS's ORMs
                                   (hl7_orders, MSH-3 in settings his_sending_apps, default SAP_P)
                                   matched to etl_orders by HIS order number; hospital from the RIS
                                   issuer, else the HIS location; reads every site's RIS orders to
                                   decide, then keeps the served site
  report_40.py                  — Report 40, Patient Schedule Lead Time (item 21, migration 0167):
                                   order creation on the RIS (etl_orders.order_created_at) to the
                                   appointment, by every rpt.exams dimension + ordering department;
                                   cancelled, scheduled more than once; utils/schedule_lead_time.py
  report_37.py                  — Report 37, JCI urgent emergent (the former Sticky Notes page,
                                   /sticky-notes redirects; report card next to ER in Clinical KPIs;
                                   per-user report access; report_template row + licence in migration
                                   0147; shared filter panel), worked out in utils/sticky_notes.py,
                                   whose docstring holds the customer's request. Reads Postgres
                                   only, a day late; only the ETL may read the PACS server, so
                                   never poll it. First open (2026-11-17) = the first image open
                                   or lock by a resident/radiologist in the PACS audit trail
                                   (AUDITING.AUDIT_LOG, Carestream), copied nightly through its
                                   EVENT_DATE index by ETL_JOBS/etl_pacs_audit.py into
                                   etl_pacs_audit_events (migrations 0155, 0156: open, lock, and the
                                   approve/wetread signatures that Report 32's turnaround reads,
                                   utils/sign_tat.py; never copies names, IDs or descriptions). Studies
                                   before the copy keep "first open seen", the earliest open a
                                   nightly snapshot caught.
                                   ETL_JOBS/etl_sticky_notes.py copies note times nightly; note
                                   text is never read. Its two tables are read through a study, so
                                   no rh view. utils/sticky_notes_monthly.py keeps each month's
                                   workbook from the 2nd and emails it through the mail server
                                   (email_server.py). Migrations 0143, 0144, 0146, 0147
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
                                   Scheduled emails (settings capability; migration 0138): utils/worklist_email.py
                                   mails saved reports as .xlsx through settings smtp_*, only to
                                   smtp_allowed_domains. Off until the relay is set. Microsoft 365
                                   signs in with OAuth (client credentials, XOAUTH2; migration 0140,
                                   docs/LAUMC_O365_EMAIL_SETUP.md)
  email_server.py               — Administration > Email Server (/admin/email-server), settings (Super user, CHT, Administrator),
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
routes/brayn_console.py         — BRAYN console (/brayn, Administration → BRAYN), SUPER USER + CHT:
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
  study_description TEXT, study_body_part TEXT, age_at_exam NUMERIC(5,2),
  gateway_ae VARCHAR(100)            -- migration 0164: for a study that came through a gateway
                                     -- (SJHCSAPWFMFIR, LAUMCWFM2FIR) with an original AE, storing_ae
                                     -- holds the SCANNER and this the gateway (etl_didb_studies.
                                     -- GATEWAY_AES, device_of); "came through a gateway" in SQL is
                                     -- GATEWAY_STUDY_SQL, never storing_ae alone
  original_storing_ae VARCHAR(100)   -- the device behind a gateway (SJH's middleware
                                     -- SJHCSAPWFMFIR keeps it; migration 0159). Cardiology devices
                                     -- (EPIQCVX, AZURION) are skipped at extract and purged
                                     -- (etl_didb_studies.CARDIOLOGY_AES, operator 2026-11-17)

etl_patient_view
  patient_db_uid BIGINT PK, patient_id TEXT, patient_name TEXT, dob DATE, sex TEXT,
  patient_class TEXT, patient_location TEXT

etl_orders
  order_dbid BIGINT PK, patient_dbid TEXT, study_db_uid BIGINT,
  proc_id TEXT, proc_text TEXT, scheduled_datetime TIMESTAMP,
  order_status TEXT, modality TEXT, has_study BOOL, order_control TEXT,
  placer_order_number TEXT (HIS order number = ORM ORC-2), order_created_at TIMESTAMP (booked;
  RIS ORDERS.CREATED_ON_DATE) -- migration 0160, one row per RIS worklist row (SPS)
  RIS steps (std_pps) link to PACS by study UID, else by this order's accession (etl_ris_pps.py)

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
  role VARCHAR NOT NULL (superuser | cht | administrator | user; CHECK, migration 0152),
  status VARCHAR (active | pending | disabled)

user_sites              — sites an account may open (RH, SJH); a Super user holds all
  user_id → users ON DELETE CASCADE, site_id → sites, PK (user_id, site_id)

user_page_permissions   — a User's feature pages, per site (utils/roles.PAGES)
  user_id, site_id (default rayd_default_site_id()), page_key, is_enabled;
  UNIQUE (user_id, site_id, page_key)

report_access_control   — a User's numbered reports, per site
  user_id, site_id (default rayd_default_site_id()), report_template_id, is_enabled;
  UNIQUE (user_id, site_id, report_template_id)  -- not (user_id, report_template_id) since 0152

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
   The same views leave out BRAYN's placeholder list (`brayn_placeholders`, migration 0151, kept on
   the BRAYN console): a test patient's rows; a placeholder referrer's name ("Referring, Generic":
   the exam still counts, the referrer reads blank); service accounts in `std_resources_ris`.
   They also replace staff who left (item 9, migration 0157, `utils/staff_status.py`): while
   `settings.staff_left_hidden` = 'true', a leaver's login and name on studies and RIS staff rows
   read 'Former staff' (every count stays). A new `etl_didb_studies` column holding a staff login
   goes in `login_cols` of `rh_refresh_views()`; a table outside the views that names staff (the
   PACS audit copy) masks in code with `staff_status.left_logins_sql()`. Who left is the
   materialized view `public.staff_status`, rebuilt after the nightly ETL: a migration that drops
   or retypes a column of `std_resources_ris` or `std_pacs_user_groups` must drop it first.
   They also leave out the HIS's duplicate orders (migration 0162): the HIS sends every order to
   the RIS twice and one copy stays Scheduled, so an order still `scheduled` once its day has
   passed (Beirut date, worked out at each query) is left out, with its worklist events.
   Code that runs in a request creates tables as `public.x`. A migration that drops or retypes a
   column of a filtered table must drop its `rh` view first. To decide a study's site use
   `utils/report_filters.study_site_sql`, never the PACS label alone: double reading stamps RH
   mammography with PACS site '1'. Lists read from `aetitle_modality_map` use `rh_device_sql`.
   **Changing (operator, 2026-11-14):** SJH gets its own database in the same web app; its
   rows are migrated out of this database, counted, then deleted here (docs/LAUMC_NEXT_SESSION.md
   item 13). Until that is built and run, this rule holds as written.
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
   **Changing (operator, 2026-11-14):** BRAYN becomes the gate that decides what reaches
   RAYD's tables (duplicates, final-only ORU, placeholders, RH/SJH routing), switched on the
   week of 2026-11-16, rolled out one message type at a time (docs/LAUMC_NEXT_SESSION.md).
   Until a message type is moved, this rule holds for it.
8. **One report UI, no standalone pages** (operator, 2026-11-09: "i really don't want the
   application to be like a workaround for every feature") — a feature that shows data is a
   numbered report: `routes/report_NN.py` calling `register_report()`, its `report_template`
   row and licence entry added by migration (see 0147), per-user report access, opened at
   `/viewer/NN`. Its left filter panel is `templates/_report_filter_panel.html` (dates,
   multi-select dropdowns each with an "Apply filter" tick named like `sidebar_filters` reads
   them, one **Generate** button; optional hidden fields, Excel button posting with the CSRF
   token, Include/Exclude per filter). `utils/report_filters.panel_filters()` builds the
   dropdowns. Reports 22, 25, 34, 35, 36, 37, 38 and the Super Report use it (2026-11-14,
   `tests/test_report_filter_panel.py`), and Report 38, the ER dashboard. The ORU tab (`/oru`)
   stays a standalone page (operator, 2026-11-14).
9. **Roles: ask for a capability, never compare a role name** (item 13f, migration 0152,
   2026-11-15). Four roles: `superuser` (Super user, everything, every site), `cht`
   (implementation: mapping, ETL, BRAYN console), `administrator` (users and settings of the
   sites granted), `user` (pages and reports ticked per site). Code calls
   `utils.roles.can(user, 'implementation' | 'settings' | 'users' | 'all_pages' | 'all_sites')`,
   `user_has_page()` and `can_open_report()`; templates `can('...')`. Who holds what is the
   CAPABILITIES table in `utils/roles.py`, nowhere else. A User's access is per site and needs
   the site granted; the site a request reads is the one picked in the header among the
   account's sites that have a database, and an account holding none of them is signed out. A migration that grants a new report writes
   `(user_id, site_id, report_template_id)` and ON CONFLICT on those three columns; nothing
   grants reports automatically any more. `tests/test_roles_access.py` fails on any
   comparison with an old role name (admin, viewer, viewer2, tec, finance, secretary).
10. **Site databases** (item 13a, 2026-11-15; `utils/site_db.py`, migration 0153). With
   `POSTGRES_DB_SJH` set, SJH has its own database on the same server, created, loaded and
   migrated at startup (every migration runs on every site database; `_startup_schema()` in
   app.py too). A request's `db.session` statements go to the picked site's database; logins
   and access (`site_db.MAIN_TABLES`) stay on the main one. In request code: open your own
   connection with `site_db.current_engine()`, never `db.engine` (always the main database);
   "this site" in SQL is `rayd_served_site_id()` and in Python `site_db.served_site_id()`,
   never `sites WHERE is_default`; an in-process cache of site data keys on
   `site_db.current_site_id()`; a streamed response uses `stream_with_context`. A new login
   or access table goes in MAIN_TABLES and its raw SQL on the main engine. Each site database
   keeps a password-less copy of `users` (`site_db.mirror_users()`), so its tables can point at
   users. The rh views (rule 0) exist in every database and keep the site it serves
   (`rayd_served_site_id()`, migration 0165), with the same rules (test patients, staff who
   left, HIS duplicates); requests on a site database read through them too, and BRAYN's
   placeholder list is a shared table. Outside a request
   everything reads the main database unless inside `site_db.use_site(site_id)`; a scheduled
   job that produces something people read runs once per database with
   `site_db.run_per_site()`. Procedure codes are shared (`site_db.SHARED_TABLES`): a route
   that writes them is decorated `@shared_write` (writes the main database, then copies to
   each site database); `sync_shared_tables()` runs every 15 minutes for the ETL's writes.

## Dev Workflow

Local stack on a laptop (two databases, the PACS pointed at `oracle-disabled.invalid`, test
runner, sample data from production): `scripts/local_stack.md`. Compose project
`rayd-laumc`, files `docker-compose.yml` + `docker-compose.local.yml`. How much SJH data RH's
database holds: `scripts/sql/sjh_census.sql` (read-only).

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

# Test suite (any reachable Postgres; creates and drops rayd_pytest_laumc and
# rayd_pytest_laumc_sjh, never etl_db)
python -m pytest tests

# SJH in its own database (item 13a): set in .env, then restart rayd-app; the app creates
# and migrates it. Leave unset until SJH's data is moved (item 13b).
POSTGRES_DB_SJH=etl_db_sjh
```

## MCP Servers (for Claude Code agents)

| Server | Purpose |
|--------|---------|
| `rayd-postgres` | Direct SQL queries against the live DB |
| `claude-flow` | Ruflo multi-agent swarm orchestration (60+ specialist agents) |

Start the dev stack first (`docker-compose.dev.yml`) so port 5432 is accessible, then Claude Code
picks up `.mcp.json` automatically when you open this directory.
