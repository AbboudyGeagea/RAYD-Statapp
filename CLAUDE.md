# RAYD-Statapp (HL7 branch) — Agent Context

## Project
Flask/PostgreSQL radiology statistics platform, **HL7 distribution**. This install never
connects to any external database — no Oracle, no PACS DB, no RIS DB. All clinical data
arrives as HL7 v2 over MLLP (port 6661), is screened by RAY7, and is projected into the
`etl_*` tables that the reports read.

**This branch is a separate product, not a site variant.** Its schema, data pipeline,
installation, users and roles differ from every other RAYD branch on purpose. Never port,
cherry-pick or merge changes from LAUMC, main or the site branches into it, never restore
what was removed (ETL, Oracle, DB Manager, external DB connections, old roles), and never
push HL7 work out to them.

## Stack
| Layer | Tech |
|-------|------|
| App server | Flask 3 + Gunicorn, Python 3.11 |
| ORM | Flask-SQLAlchemy (SQLAlchemy 2) |
| Database | PostgreSQL 15 (`rayd_db` container) — the only database this app touches |
| Data source | HL7 v2 over MLLP, port 6661 (`hl7_listener.py`) |
| Screening | RAY7 (`utils/ray7.py`), inline before the ACK |
| NLP worker | medspaCy in separate `rayd_nlp` container |
| Reverse proxy | nginx (`rayd_proxy` container) |
| Scheduling | APScheduler (inside main container) |

## Containers
Names are `${RAYD_CONTAINER_PREFIX:-rayd}_*` so two clones can coexist on one host
(the local HL7 test clone uses prefix `raydtest`).
```
rayd_proxy   nginx:stable-alpine        — TLS termination, HTTPS 443
rayd_service python:3.11 (Dockerfile)   — Flask app + MLLP listener on 6661
rayd_nlp     python:3.11 (nlp_worker/)  — medspaCy batch processor, no exposed ports
rayd_db      postgres:15                — primary DB, port 5432 (dev: exposed to host)
```

## Data flow
```
MLLP :6661 ─▶ hl7_message_archive (raw, every message) ─▶ RAY7.screen()
          ─▶ persist events/state (hl7_study_events, ray7_study_state, hl7_orders,
             hl7_oru_reports, hl7_patients) ─▶ projector ─▶ etl_didb_studies,
             etl_patient_view, etl_orders ─▶ reports (report SQL is never touched)
```

## Key Files
```
app.py                    — app factory, blueprint registration, scheduler, `-m` replay CLI
db.py                     — SQLAlchemy setup, ORM models, roles + user_has_page()
hl7_listener.py           — MLLP socket listener: framing, ACK (always AA)
utils/
  hl7_parse.py            — HL7 v2 parsing into the ParsedMessage contract
  hl7_fieldmap.py         — applies operator-configured hl7_field_mappings
  hl7_dictionary.py       — HL7 v2.4 field names for the mapping explorer
  hl7_ingest.py           — archive + persist (events, study state, orders, reports)
  ray7.py                 — RAY7 screening engine (verdicts, findings)
  ray7_sweep.py           — RAY7 absence rules (stalled / unreported), runs every 15 min
  hl7_project.py          — the projector: HL7 state → etl_* tables, per accession
  hl7_replay.py           — rebuild everything downstream of the archive
  hl7_forward.py          — optional MLLP forwarding of raw messages
  crn_scan.py, crn_dispatcher.py — critical result notifications (sending is a stub)
routes/
  ray7_console.py         — RAY7 findings / quarantine console
  mapping_controller.py   — HL7 → DB field mapping, modality/procedure config
  hl7_orders.py           — HL7 order analytics
  oru_analytics.py        — ORU report intelligence (NLP, critical findings log)
  cd_log_route.py         — POST /cd-burn API (CD/DVD burn events → cd_burn_log)
  cd_log_ui.py            — CD burn log screens; Report 30 reads cd_burn_log
  report_22.py … report_36.py, super_report.py, er_dashboard.py — reports (read etl_*)
ETL_JOBS/daily_analytics.py — 05:30 snapshot for the daily briefing (Postgres only;
                              the only module left in ETL_JOBS)
nlp_worker/worker.py      — standalone medspaCy batch loop (polls every 60s)
scripts/hl7_scenarios.py  — drive named HL7 scenarios at the listener; reset test data
tests/                    — test_hl7_parse, test_hl7_fieldmap, test_ray7_rules
migrations/NNNN_*.sql     — schema migrations (canonical source of truth)
install.sh                — production install: no DB/ETL config, first user + license tier
```

## DB Schema

### HL7 pipeline (migrations 0115–0130)
```
hl7_message_archive   — every raw message received; the only copy that will ever exist
hl7_surrogate_keys    — stable BIGINT keys minted by hl7_surrogate_id() for etl_* PKs
hl7_status_map        — status codes → canonical lifecycle states (seeded config)
hl7_result_status_map — OBX-11 → signature rungs (prelim / final)
hl7_study_events      — lifecycle event log per accession
hl7_patients          — patient demographics from HL7
hl7_field_mappings, hl7_field_targets — per-site field mapping overrides
ray7_rules            — rule config (seeded config)
ray7_findings         — findings raised by RAY7
ray7_study_state      — one row per study carrying the whole lifecycle
ray7_ladder_profile   — per-site rung enable/enforce flags (seeded all off)
hl7_orders, hl7_oru_reports, hl7_scn_studies — parsed orders, reports, PACS completions
hl7_oru_analysis      — written by nlp-worker only
```

### Projected tables (filled by the projector, read by every report)
```
etl_didb_studies   — study_db_uid is a surrogate key; insert_time = COMPLETED timestamp
                     (the TAT anchor, see utils/hl7_project.py)
etl_patient_view, etl_orders
etl_didb_serieses, etl_didb_raw_images, etl_image_locations — stay EMPTY: image/series
                     counts and storage need the nightly aggregate export (not built)
```

### Other
```
aetitle_modality_map, device_weekly_schedule, device_exceptions — device config
cd_burn_log          — CD/DVD burn events (migration 0132)
settings             — key/value config and license JSON
users                — role is su | implementation | administrator | user (migration 0131)
analytics_snapshots  — daily briefing data
```
Leftover tables from the Oracle era (`db_params`, `adapter_mappings`, `go_live_config`,
`etl_job_log`) may still exist on older installs; no code on this branch writes to them.
There is no go-live date: `get_etl_cutoff_date()` returns `MIN(study_date)` from the
projected `etl_didb_studies`, and `go_live_config` is left empty.

## Roles
| Role | Who | Access |
|------|-----|--------|
| `su` | R&D | everything; only role exempt from license expiry |
| `implementation` | ATH engineers | mapping (HL7 fields, modalities, procedures), live feed, HL7 orders, custom reports |
| `administrator` | radiology manager / chief radiologist | all reports, user management |
| `user` | read-only staff | reports an administrator grants |
Old role names (`admin`, `viewer`, `viewer2`, `tec`, `finance`, `secretary`) no longer exist —
never check for them. Page access goes through `user_has_page()` in `db.py`.

## Critical Conventions

1. **DB changes** — always via `migrations/NNNN_description.sql`. Migrations apply
   automatically at app startup. Never run DDL from psql directly.
2. **No external database, ever** — do not add DB drivers, connection settings or ETL jobs.
3. **RAY7 never drops a message** — verdicts are accepted / flagged / quarantined, all
   keep the message; the listener always ACKs `AA`. Rules run inline before the ACK, so
   every rule must be an indexed O(1) lookup, under the time budget, and fail-open.
4. **Report SQL is not touched for HL7** — reports keep reading `etl_*`; HL7 data reaches
   them only through the projector.
5. **SR exclusion** — every query touching `etl_didb_studies` must filter
   `COALESCE(m.modality, s.study_modality, '') != 'SR'`.
6. **Modality source** — prefer `aetitle_modality_map.modality` over `study_modality`.
7. **hl7_oru_analysis** — written only by `nlp_worker/worker.py`.
8. **Never truncate `ray7_rules`, `hl7_status_map` or `hl7_result_status_map`** — they are
   seeded configuration; wiping them silently disables screening.

## Dev Workflow

**Always pass both `-f` flags, on every command.** The base file and the dev
override use *different* database volumes (`postgres_data` vs `postgres_dev_data`),
so dropping the flags silently switches you to a different, empty database. Define
an alias and use it for everything:

```bash
alias dc='docker compose -f docker-compose.yml -f docker-compose.dev.yml'
```

```bash
# Start the stack. nginx serves HTTPS on 443; port 80 is NOT published (another
# app owns it on this host), so use https:// explicitly — there is no redirect.
dc up -d

#   https://localhost      web UI
#   localhost:6661         HL7 MLLP listener
#   localhost:5432         Postgres, for psql and the rayd-postgres MCP server
#   localhost:8080         app direct, bypassing nginx

# Tail logs / watch HL7 traffic arrive
dc logs -f rayd-app
dc logs -f rayd-app | grep -E "HL7|MLLP|RAY7"

# Rebuild one service after a code change
dc build rayd-app && dc up -d rayd-app

# Replay the archive through parse → screen → persist → project
dc exec rayd-app python app.py -m              # everything
dc exec rayd-app python app.py -m --dry-run    # count only
#   also: --since <archive id>, --limit <n>, --no-rescreen

# Send test scenarios at the listener
python scripts/hl7_scenarios.py --list
```

**Migrations apply themselves.** `run_migrations()` runs at app startup, so
`dc up -d` after adding `migrations/NNNN_*.sql` is all that is needed — confirm with
`dc logs rayd-app | grep migrations`.

## MCP Servers (for Claude Code agents)

| Server | Purpose |
|--------|---------|
| `rayd-postgres` | Direct SQL queries against the live DB |
| `claude-flow` | Ruflo multi-agent swarm orchestration (60+ specialist agents) |

Start the dev stack first (`docker-compose.dev.yml`) so port 5432 is accessible, then Claude Code
picks up `.mcp.json` automatically when you open this directory.
