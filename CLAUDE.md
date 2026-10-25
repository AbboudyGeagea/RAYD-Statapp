# RAYD-Statapp (HL7 branch) — Agent Context

## Project
Flask/PostgreSQL radiology statistics platform, **HL7 distribution**. This install never
connects to any external database — no Oracle, no PACS DB, no RIS DB. All clinical data
arrives as HL7 v2 over MLLP (port 6661), is screened by BRAYN, and is projected into the
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
| Screening | BRAYN (`utils/brayn.py`), inline before the ACK |
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
MLLP :6661 ─▶ hl7_message_archive (raw, every message) ─▶ BRAYN ① intake (inline)
          ─held─▶ stored, nothing derived (fix config, then "Replay held messages")
          ─accepted─▶ persist events/state (hl7_study_events, brayn_study_state, hl7_orders,
             hl7_oru_reports, hl7_patients) ─▶ projector ─▶ etl_didb_studies,
             etl_patient_view, etl_orders ─▶ reports (report SQL is never touched)
          ─▶ BRAYN ② quality notes on the study  ─▶ BRAYN ③ per-sender counters
   sweep (15 min) ─▶ BRAYN ④ workflow: stalled / unreported / unsigned / orphan + ③ alerts
```

**BRAYN has four layers (migration 0135).** Only ① can keep data out of the reports,
and only for a message it cannot identify (no accession, no placer number) or
classify (status code with no mapping). ② data-quality notes (time contradiction,
skipped rung, no order, future time) are judged on the study's whole lifecycle and
close themselves. ③ transport and configuration signals (redelivery, control-ID
reuse, out-of-order delivery, duplicates, unknown AE/result status/rung) are
per-sender rates in `brayn_interface_stats` and values in `brayn_unknown_values`; an
alert only past `brayn_rules.threshold_pct`. The lifecycle state is order-independent,
so delivery order never changes a projected study.

**BRAYN was called RAY7 until migration 0137** (full rename: tables, columns,
constraints, indexes, the two engine-named rule codes, files, routes; `/ray7` redirects
to `/brayn`). Migrations 0119–0136 still say ray7 — they are history, never edit them.

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
  brayn.py                 — BRAYN ① intake gate + observations (verdict: accepted / held)
  brayn_quality.py         — BRAYN ② study quality notes, self-closing, never withhold
  brayn_health.py          — BRAYN ③ per-sender counters, unknown values, rate alerts
  brayn_sweep.py           — BRAYN ④ absence rules (stalled / unreported) + ③ alerts, 15 min
  brayn_explain.py         — the console's words: rule titles, notes as sentences, a message
                             explained field by field (used / problem / suggested accession)
  hl7_project.py          — the projector: HL7 state → etl_* tables, per accession
  hl7_replay.py           — rebuild everything downstream of the archive
  hl7_forward.py          — optional MLLP forwarding of raw messages
  crn_scan.py, crn_dispatcher.py — critical result notifications (sending is a stub)
routes/
  brayn_console.py         — BRAYN console (/brayn), one screen per operator question: Feed
                             status, To fix (inline fixes + Reprocess), Messages, Study timeline,
                             Study notes, Settings (checks editable). Fixes reuse the mapping
                             studio's endpoints; device / result-status / rule saves live here,
                             su + implementation only. Wording only in utils/brayn_explain.py
  mapping_controller.py   — HL7 → DB field mapping, modality/procedure config
  hl7_orders.py           — HL7 order analytics
  oru_analytics.py        — ORU report intelligence (NLP, critical findings log)
  cd_log_route.py         — POST /cd-burn API (CD/DVD burn events → cd_burn_log)
  cd_log_ui.py            — CD burn log screens; Report 30 reads cd_burn_log
  report_22.py … report_36.py, super_report.py, er_dashboard.py — reports (read etl_*)
  worklists.py            — Dynamic Worklists (RAYD V2.0 report editor; replaced Custom
                            Reports, same licence key + page permission `custom_reports`)
utils/worklist_catalog.py — every dataset/field/grouping/measure a worklist may name
utils/worklist_engine.py  — compiler (catalog identifiers only, values bound) + executor
                            (runs on the read-only rpt_<db> role, never the app connection)
ETL_JOBS/daily_analytics.py — 05:30 snapshot for the daily briefing (Postgres only;
                              the only module left in ETL_JOBS)
nlp_worker/worker.py      — standalone medspaCy batch loop (polls every 60s)
scripts/hl7_scenarios.py  — drive named HL7 scenarios at the listener; reset test data
tests/                    — pytest suite (~1,390 tests, see Dev Workflow): parse, every BRAYN
                            rule, lifecycle/projection, all delivery permutations, sweep,
                            replay == live, console release, MLLP socket, fuzz, concurrency,
                            time zone. Runs in its own throwaway DB `rayd_pytest`, never
                            etl_db. test_hl7_parse/test_hl7_fieldmap are the older
                            offline scripts (`python tests/<file>`), not collected.
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
brayn_rules            — rule config (seeded config); category = layer
                        (intake | quality | interface | workflow), threshold_pct for rates
brayn_findings         — held reasons (per message), study notes, interface alerts (per sender)
brayn_interface_stats  — per-sender counters in 15-min buckets (BRAYN ③)
brayn_unknown_values   — AE titles / status codes / result statuses / rungs not yet mapped
brayn_study_state      — one row per study carrying the whole lifecycle
brayn_ladder_profile   — per-site rungs: expected, and enforce_order (= "measure order")
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

### Dynamic Worklists (migration 0136)
```
rpt.exams / rpt.orders / rpt.reports — flat datasets the worklist editor queries
worklists, worklist_versions        — saved definitions; every save is kept
worklist_shifts                      — shift hours behind the "shift" field (seeded AM/PM/NIGHT)
rpt_<database> login role            — SELECT on rpt.* only, read-only, 30 s timeout,
                                       search_path rpt; password in settings
                                       (worklist_db_role / worklist_db_password)
```
The worklist runner must keep its own login: the app's connection is the table owner
(a superuser in the postgres image), so `SET ROLE` or `set_config('role', ...)` would
switch straight back — no in-session privilege drop is a sandbox. A new `rpt` view
needs no grant (default privileges cover it) but must avoid public-schema functions,
which run with the caller's (rpt role's) rights.
Leftover tables from the Oracle era (`db_params`, `adapter_mappings`, `go_live_config`,
`etl_job_log`) may still exist on older installs; no code on this branch writes to them.
There is no go-live date: `get_etl_cutoff_date()` returns `MIN(study_date)` from the
projected `etl_didb_studies`, and `go_live_config` is left empty.

## Roles
| Role | Who | Access |
|------|-----|--------|
| `su` | R&D | everything; only role exempt from license expiry |
| `implementation` | ATH engineers | mapping (HL7 fields, modalities, procedures), live feed, HL7 orders, Dynamic Worklists (incl. hand-written SQL), BRAYN console |
| `administrator` | radiology manager / chief radiologist | all reports, user management |
| `user` | read-only staff | reports an administrator grants |
Old role names (`admin`, `viewer`, `viewer2`, `tec`, `finance`, `secretary`) no longer exist —
never check for them. Page access goes through `user_has_page()` in `db.py`.

## Critical Conventions

1. **DB changes** — always via `migrations/NNNN_description.sql`. Migrations apply
   automatically at app startup. Never run DDL from psql directly.
2. **No external database, ever** — do not add DB drivers, connection settings or ETL jobs.
3. **BRAYN never drops a message** — verdicts are accepted / held (stored value
   `quarantined`), both keep the message; the listener ACKs `AA` (AE only if the archive
   insert itself failed). Only intake may hold. Everything inline before the ACK must be
   an indexed O(1) lookup, under the time budget, and fail-open.
4. **Report SQL is not touched for HL7** — reports keep reading `etl_*`; HL7 data reaches
   them only through the projector. One deliberate exception: the `rpt.*` worklist views
   read the HL7 tables directly (technician of the Started event, BRAYN notes, order →
   exam chain exist nowhere in `etl_*`). Only the worklist module reads them.
5. **SR exclusion** — every query touching `etl_didb_studies` must filter
   `COALESCE(m.modality, s.study_modality, '') != 'SR'`.
6. **Modality source** — prefer `aetitle_modality_map.modality` over `study_modality`.
7. **hl7_oru_analysis** — written only by `nlp_worker/worker.py`.
8. **Never truncate `brayn_rules`, `hl7_status_map` or `hl7_result_status_map`** — they are
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
dc logs -f rayd-app | grep -E "HL7|MLLP|BRAYN"

# Rebuild one service after a code change. The reload matters: recreating the
# container gives it a new IP and nginx keeps the old one (502 on https://localhost)
# — update.sh does the same reload in production.
dc build rayd-app && dc up -d rayd-app && dc exec nginx nginx -s reload

# Replay the archive through parse → screen → persist → project
dc exec rayd-app python app.py -m              # everything
dc exec rayd-app python app.py -m --dry-run    # count only
#   also: --since <archive id>, --limit <n>, --no-rescreen

# Send test scenarios at the listener
python scripts/hl7_scenarios.py --list

# Run the test suite (from the host, stack up; ~2 min). It creates and drops its
# own database `rayd_pytest` on the dev Postgres (localhost:5432) — etl_db is never
# touched. Deps: pip install -r tests/requirements.txt
python -m pytest tests
#   RAYD_TEST_KEEP_DB=1 keeps rayd_pytest afterwards for inspection;
#   RAYD_TEST_ADMIN_URL=postgresql://user:pass@host:port/postgres points it elsewhere
```

**One clock.** HL7 timestamps are naive hospital time, so every DB session runs on
`TZ` (default Asia/Beirut) via `db.engine_options()` — the postgres container itself
is on UTC. Anything that opens its own connection must pass `db.pg_connect_options()`.

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
