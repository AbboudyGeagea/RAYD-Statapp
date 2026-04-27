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
| AI assistant | Qwen2.5-7B via llama.cpp at `http://172.17.0.1:8081` |
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
  oru_analytics.py              — HL7 ORU analytics (NLP word cloud, critical findings log)
  viewer_controller.py          — daily briefing, home dashboard
  mapping_controller.py         — AE/modality/procedure mapping config (lazy-loaded tabs)
  bitnet_service.py             — Qwen2.5-7B AI assistant proxy (ChatML format)
  hl7_orders_route.py           — HL7 order analytics
  report_cache.py               — shared cache/dropdown helpers
ETL_JOBS/
  etl_job.py                    — Oracle → PostgreSQL ETL pipeline
  etl_analytics_refresh.py      — analytics snapshot rollup (runs nightly at 05:30)
nlp_worker/worker.py            — standalone medspaCy batch loop (polls every 60s)
migrations/NNNN_*.sql           — schema migrations (canonical source of truth)
init-db/schema.sql              — initial schema applied by docker-entrypoint
scripts/setup_qwen_prod.sh      — one-shot Qwen2.5-7B production setup on Ubuntu host
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

1. **DB changes** — always via `migrations/NNNN_description.sql`. Never run DDL from psql CLI directly.
2. **SR exclusion** — every query touching `etl_didb_studies` must filter:
   `COALESCE(m.modality, s.study_modality, '') != 'SR'`
   (SR = Structured Report; auto-generated by PACS, not a real study)
3. **Modality source** — prefer `aetitle_modality_map.modality` over `study_modality`; fall back to `study_modality` if no mapping exists.
4. **Expensive CTEs** — `etl_orders` scans with `MODE() WITHIN GROUP` must use `WITH ... AS MATERIALIZED`.
5. **hl7_oru_analysis** — written only by `nlp_worker/worker.py`; route handlers read it but never write it.
6. **Qwen prompt format** — ChatML: `<|im_start|>system\n...<|im_end|>\n<|im_start|>user\n...<|im_end|>\n<|im_start|>assistant\n`
7. **Password encryption** — Oracle and external DB passwords are encrypted via `utils/crypto.py` using `SECRET_KEY`.

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
```

## MCP Servers (for Claude Code agents)

| Server | Purpose |
|--------|---------|
| `rayd-postgres` | Direct SQL queries against the live DB |
| `claude-flow` | Ruflo multi-agent swarm orchestration (60+ specialist agents) |

Start the dev stack first (`docker-compose.dev.yml`) so port 5432 is accessible, then Claude Code
picks up `.mcp.json` automatically when you open this directory.
