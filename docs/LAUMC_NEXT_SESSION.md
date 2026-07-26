# LAUMC — Next Session: what's done, what's left

Pick up here. Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor Qs), `LAUMC_DATA_REQUEST.md` (discovery record).

Last major update: 2026-07-27 — full RIS pipeline build (Phases 6, 9–15) + infra hardening.
Also 2026-07-26: live incident (accidental `docker compose down -v`, full data wipe) —
recovery + 4 new bugs found/fixed + new site-filter rule started. See tables below.

---

## ✅ BUILT & PUSHED (branch `LAUMC`)

### Foundation (earlier sessions)
| Item | Where |
|---|---|
| `sites` tri-vocabulary table + RH/SJH seed | `migrations/0046` |
| Status lifecycle map (STATUS_KEY → canonical stage, ~30 alias rollups) | `migrations/0047` |
| RAYD's own status-history table | `migrations/0048` |
| `site_id` columns + indexes on studies/orders/hl7/oru/aetitle/snapshots | `migrations/0049` |
| **RLS**: `rayd_app` role + fail-closed policies (owner `etl_user` bypasses) | `migrations/0050` |
| `pacs_site_id_raw` + `site_mismatch_log` (RIS-vs-PACS monitor / mammo bug) | `migrations/0051` |
| **`site_org_map`** (many-org→one-site) + **`user_sites`** grants | `migrations/0052` |
| RIS registry → real vendor schemas (declarative, `system_type_registry.py` — used for
  DDL reference only; actual tables built directly in `etl_db`, see below) | `ETL_JOBS/system_type_registry.py` |
| Site resolver (PACS / RIS-issuer / RIS-org / HL7-building → site_id) | `utils/site_resolver.py` |
| Site-scope request logic (**inert — not wired**) | `utils/site_scope.py` |
| PACS studies ETL pulls `SITE_ID` → `pacs_site_id_raw` | `ETL_JOBS/etl_didb_studies.py` |
| Portal + Scheduling modules physically removed (−3,732 lines) | many |

### Infra hardening (2026-07-25 to 27 — install/recovery reliability)
| Item | Fix |
|---|---|
| Migration runner: `%`-substitution bug (0050's RLS `format(%I,...)` DO blocks) | raw DBAPI cursor execution, `db_migrations.py` |
| Migration runner: `CONCURRENTLY` autocommit bug (`raw_connection().autocommit=True` was a silent no-op) | `execution_options(isolation_level="AUTOCOMMIT")` on a fresh connection |
| `0003` could clobber an operator-entered go-live date | rewritten to `INSERT...WHERE NOT EXISTS` |
| `0026` failed every run (`adapter_mappings` didn't exist yet) | migration now creates it |
| `db_params.password` truncated Fernet tokens (`VARCHAR(100)`) | widened to `TEXT`, `migrations/0055` |
| `reading_physician_id`/`signing_physician_id` nulled on LAUMC (composite AD-login IDs, not numeric) | widened `BIGINT→TEXT`, `migrations/0056` |
| Pre-existing ORM/DB drift: `aetitle_modality_map.room_name`/`.display_aetitle`, `procedure_duration_map.modality` declared in `db.py` but never created — CSV export routes would 500 | `migrations/0058` |
| Series ETL: hardcoded column list broke on LAUMC (`INSTITUTIONAL_DEPARTMENT_NAME`, `MANUFACTURER` missing) — was whack-a-mole per column | now queries `ALL_TAB_COLUMNS` once, builds `SELECT` from what's actually there, `etl_series.py` |
| `app.py` startup auto-trigger doubled every manual `-m` ETL run while any table was empty | gated off in manual CLI mode |
| Raw Images ETL never set `cursor.arraysize` (Oracle default ~100 rows/round-trip despite `fetchmany(2000)`) | set from `ETL_GEAR`, ~2x throughput bump given confirmed server headroom |

### RIS pipeline — Phases 6, 9–15 (2026-07-27, all new/changed this session)
| Phase | Source → Target | Notes |
|---|---|---|
| **6** Orders | `SITE_WORKLIST ⋈ ORDERS ⋈ SPS_CODE` → `etl_orders` | Switched from PACS `MDB_ORDERS` (messier data) per operator decision. `order_status` translated via `worklist_status_map` to short codes (`CA`/`CM`/raw stage) existing reports hardcode. `study_db_uid` resolved by 2-stage enrichment: primary `accession_number = sps_id` match, `linked_id`-group fallback for multi-SPS orders. `patient_dbid` = RIS `patient_person_key` (PACS identity NOT reconciled here, by design). |
| **9** RIS Reports | `REPORT.DOCUMENT_PLAIN_TEXT` → `hl7_oru_reports` | Coexists with live HL7 ORU listener via `report_source` tag + fill-only merge (RIS text wins on report/impression, listener fills gaps). |
| **10** RIS Catalog | `MODALITY⋈MODALITY_TYPE`→`aetitle_modality_map`, `SPS_CODE`→`procedure_duration_map`, `ORDERING_ORGANIZATION`→**`std_ordering_organizations`** (new) | Fill-only on the first two (never overwrite manual mapping-tab edits/RVUs); refresh-on-conflict on ordering orgs (no RAYD-owned fields there). |
| **11** RIS Visits | `VISIT` → **`std_visits`** (new) | Feeds case-mix/payer/LOS/hospital-service. `site_id` **not resolved** — VISIT carries no org/issuer column of its own. `patient_class_key`/`financial_class_key`/`hospital_service_key`/`mobility_status_key` pulled raw (lookups pending). `deleted='Y'` rows imported, not dropped. |
| **12** RIS Patients | `PATIENT⋈PERSON`→**`std_patients_ris`**, `PATIENT_ID_LIST`→**`std_patient_ids`** (both new) | **NO PATIENT NAMES** (operator instruction, PHI) — `PATIENT_ALIAS` dropped from scope entirely as a result (100% name data). `gender_key` resolved to `F/M/U/I/0/NSP/O/A` via hardcoded lookup. `language_key` still raw. Also runs **`age_at_study`** enrichment on `etl_didb_studies` (new column, `migrations/0061`) — computed from `std_patients_ris.birth_date` via the `etl_orders` bridge, kept separate from PACS's unreliable `age_at_exam`. |
| **13** RIS Resources | `RESOURCE_ID⋈PERSON`→**`std_resources_ris`** (new) | **Names + contact INCLUDED** (staff, not patient PHI — KPI needs radiologist names, CRN needs referring-physician email/phone). Vendor-confirmed: `REPORT`/`ORDERS`/`SITE_WORKLIST`'s `*_RESOURCE_ID_KEY` columns reference `RESOURCE_ID.RESOURCE_ID_KEY`, not `PERSON_KEY`. Also resolves `reading_physician_resource_key`/`signing_physician_resource_key` on `etl_didb_studies` by matching the composite `email@domain_numericid` string already sitting in `reading_physician_id`/`signing_physician_id` — fixes data already loaded, not just future rows. `resource_role_key` still raw (no role lookup yet). `site_id` resolved via `site_org_map` (RESOURCE_ID carries a real org key, unlike VISIT). |
| **14** RIS PPS | `PPS ⋈ SPS_CODE ⋈ MODALITY` → **`std_pps`** (new), plus **`std_status_ris`**, **`std_procedure_priorities`**, **`std_dictations`** (lookups), **`std_site_pps_ext`** (new) | The "treasure table". `std_status_ris` = full-fidelity RIS STATUS master (TYPE-aware: ORDER/SPS/PPS share one table; `CORE_STATUS` is the real alias→canonical pointer — more authoritative than `worklist_status_map`'s hand-curated seed, which is left untouched). **`TECHNURSE_NOTES`/`DEMONSTRATION_NOTES` never fetched** (free-text clinical notes, operator: "we're not going there yet"). Includes the empirical **`STUDY_INSTANCE_UID`↔PACS join test** (`study_db_uid` enrichment on `std_pps` — a real DICOM UID match, not inferred; run it and check the match rate). `std_site_pps_ext` = `SITE_PPS`'s structured fields (film reject/shielding/CD-burn/critical-result/consent/complications) — despite the name, NOT a site/org lookup (1:1 QA extension); 6 of 43 columns excluded (`HOLDER_NAME` + 5 free-text comment fields). |
| **15** RIS Modality Availability | `MODALITY_AVAIL_EXCEPTION`→**`std_modality_exceptions`**, `SCHEDULE_TEMPLATE_ITEM`→**`std_schedule_template_items`**, `SCHEDULE_SCHEME`→**`std_schedule_schemes`**, `AVAILABILITY_INDICATOR`→**`std_availability_indicators`** (all new) | RIS-authoritative counterparts to the existing manually-editable `device_exceptions`/`device_weekly_schedule` — **not editable from RAYD** (operator instruction; no admin route built for any of the four). `std_modality_exceptions` resolved to `aetitle` via a live `MODALITY` join, ready to use. `std_availability_indicators` fully resolves what each indicator means (03=Available, 04=Unavailable, 07=Holiday, 11=Maintenance, 2100=Closed = "unavailable" for utilization; rest are booking-rule nuances). `std_schedule_template_items` still **not attributable to a device** — `std_schedule_schemes` resolved the scheme NAME but schemes turned out to be a generic category, not device-specific; the actual device↔scheme link is still unidentified (see blocked #5). Together with Phase 14's `std_pps` (actual usage) this is the utilization pair, one link short of complete. |

### Live-incident recovery + new bugs found/fixed (2026-07-26)
Operator ran `docker compose down -v` (accidental full volume wipe) + rebuild, then hit a
cascade of previously-latent bugs while re-running ETL/reports against the fresh DB:

| Item | Fix |
|---|---|
| Stuck idle-in-transaction session on `db_params` (~3h old) blocking `ALTER TABLE db_params ADD COLUMN owner`, which then blocked every downstream `db_params` lookup — cause of the "app hanging / internal server error" report | `pg_terminate_backend()` on the leaked session (one-off; root leak cause not fully diagnosed) |
| `etl_didb_studies`'s incremental-vs-fresh-load switch keys off `MAX(study_db_uid)`, not `go_live_config` — moving the go-live date forward had no effect once the table had any rows | documented in `ETL_JOBS/etl_didb_studies.py:41`; a genuine re-cutoff needs `TRUNCATE` (resets `max_uid` to 0), not just a new `go_live_config` row |
| RIS Reports enrichment (`ETL_JOBS/etl_ris_reports.py`) referenced `pv.patient_id` — `etl_patient_view`'s real column is just `id` (see `init-db/schema.sql:425`) — crashed **the entire ETL run**, not just Phase 9 | column reference fixed |
| Image Locations ETL (`ETL_JOBS/etl_image_locations.py`) 1000-study chunks fed a `ROW_NUMBER() OVER (PARTITION BY...)` dedupe query too much data to sort at once on the full-history reload (~1,200 images/study avg) → `ORA-01652` (Oracle TEMP tablespace exhausted) on the PACS side | chunk size cut 1000→100; still open: ask whoever manages PACS Oracle to check `dba_temp_files`/`v$tempseg_usage` — this will bite other heavy queries too if TEMP is genuinely undersized |
| `age_at_study` enrichment (`ETL_JOBS/etl_ris_patients.py`) — a placeholder/sentinel `birth_date` (same "quick-registration DOB" issue the module already flags) produced a negative/absurd age that overflowed `NUMERIC(5,2)`, crashing the whole run | bounded to `0–130` years in the `WHERE` clause |
| **Systemic**: `_perform_migration` (`ETL_JOBS/etl_runner.py`) had ONE try/except around all 15 phases — any single phase's uncaught exception killed every phase after it (hit twice in one incident: Phase 9 then Phase 12) | each phase now has its own try/except + continues; `4TB_SYNC`'s final `etl_job_log` status is now `PARTIAL` (with the failed-phase list) instead of a misleading blanket `SUCCESS` when something failed |
| `report_template` had **no seeding migration** for `report_id=25` at all (only `report_id=30` does, migration `0031`) — the row only existed because someone inserted it manually at some point; a genuine full wipe leaves it missing entirely, so `get_gold_standard_data` returns nothing → "report 25 shows no data" | seeded in `migrations/0069_seed_report_25_template.sql`, `ON CONFLICT DO NOTHING` (never clobbers a live-tuned query) — **worth checking whether other reports have the same gap** (their `report_template` rows may also be manual-insert-only, unmigrated) |
| My own site-filter edit to `routes/report_25.py` (`3c544e35`) nested a triple-quoted string literal inside another triple-quoted f-string's `{}` expression — invalid on Python <3.12 (the container runs 3.11), `SyntaxError` at import time. `routes/registry.py` imports `report_25` unconditionally, so this took down **the entire app**, not just one report. My own local syntax check passed because it ran under this machine's Python 3.12/3.14, which silently tolerates the pattern — never actually validated against the real target version | fixed (`696b51a6`) by building the join fragment as a plain string variable instead of a nested literal; re-verified against a real `python:3.11-slim` container this time, not the local interpreter — **do this for every future report_25.py syntax check, local `python`/`python3` here is NOT 3.11** |
| `templates/report_25.html`'s main content block only checked `{% if run_report %}`, never whether `data` was actually non-None — `get_gold_standard_data()` already correctly returns `None` on an empty query result, but the template dereferenced `data.summary.total` unconditionally, so any legitimately-empty result 500'd (`jinja2.exceptions.UndefinedError`) instead of showing an empty state | fixed (`255fdc51`): narrowed to `{% if run_report and data %}` + added a proper "no data for this range" message. The other 5 bare `{% if run_report %}` blocks in the file don't dereference `data`, confirmed safe, left as-is |
| **The real "report 25 shows no data" root cause, found after 0069/`696b51a6`/`255fdc51` all landed and it was STILL empty**: report_25's query required `etl_didb_studies.rep_final_timestamp`/`rep_final_signed_by` IS NOT NULL to count a study as reported — but PACS's own `DIDB_STUDIES` isn't reliably synced with report completion for recent studies at LAUMC (radiologists sign in the RIS). Spot-checked directly against Oracle: a study scanned 2026-07-16 04:35 had a complete, signed report in `hl7_oru_reports` (RIS-sourced, Phase 9) dated the same day 08:56 — completely normal turnaround — yet PACS's `REP_FINAL_TIMESTAMP` was still NULL over a week later. Not a backlog, not an ETL pull bug, just checking a PACS field that never gets updated | `migrations/0070_report_25_ris_report_fallback.sql`: `LEFT JOIN hl7_oru_reports` by `accession_number`, `COALESCE` PACS's field with the RIS-sourced one for both the filter and TAT calc, preferring PACS where it does exist (172 older rows had it). **Known follow-up, not fixed**: `hl7_oru_reports.physician_id` is very likely a raw HL7 provider code, not a display name — radiologist attribution on RIS-only rows may show a code instead of a name until resolved through `std_resources_ris` (same composite-ID pattern Phase 13 already uses). **This almost certainly affects every other report computing TAT or "is reported" off `etl_didb_studies.rep_final_timestamp`/`rep_final_signed_by` directly — not audited yet, operator explicitly scoped this fix to report_25 only.** Also very likely the actual cause of punch-list #7 ("reporting backlog — RAYD not detecting reporting details") — same root cause, not a separate bug |

### New rule: reports show RH (main site) only, SJH excluded — "for now" (2026-07-26)
Operator instruction: all reports should only include site `0` from PACS / `1000` from RIS
in all data — per `migrations/0046_sites_mapping.sql` both values mean **RH**, the main site
(`is_default=TRUE`); SJH is the satellite.

- `etl_didb_studies.site_id`/`hl7_oru_reports.site_id` are **never actually populated** — the
  enrichment pass was designed (`migrations/0051`, `ETL_JOBS/etl_site_enrichment.py`
  referenced in its comment) but that file was **never built**. `utils/site_resolver.py`
  exists (full resolver for all 4 source vocabularies) but nothing calls it from any ETL job
  or the HL7 listener. Don't trust `site_id` on any row until this is actually wired up.
- Working alternative used instead: `aetitle_modality_map.site_id` — populated today by the
  already-running Phase 10 (RIS-authoritative via `ORG_STRUCTURE_KEY → site_org_map`). Since
  every study's `storing_ae` maps to a physical device at one physical site, this gives a
  reliable per-study site *right now*, and — unlike raw PACS `SITE_ID` — isn't affected by
  the known SJH-mammo-mislabeled-as-RH bug (`migrations/0051`'s own comment).
- Pattern applied in `routes/report_25.py`: `default_site()` (from `utils/site_resolver.py`,
  resolves to RH's canonical id) folded into the file's existing `_sec_filters`/
  `_sec_needs_mod_join` mechanism so every secondary query picks it up automatically; the
  main `report_template`-driven query filters via `aetitle IN (SELECT ... WHERE site_id=...)`;
  `hl7_orders`-based queries (no site marker of their own) join back to `etl_didb_studies` by
  `accession_number` first. Skips cleanly (no filter applied) if `default_site()` returns
  `None` — never zeroes out a non-LAUMC/single-site install.
- **Report 25 is the only file done.** 20 more route files touch
  `etl_didb_studies`/`hl7_orders`/`hl7_oru_reports` and need the same treatment for the rule
  to actually hold everywhere: `report_22`, `report_27`, `super_report`, `er_dashboard`,
  `oru_analytics`, `hl7_orders`, `capacity_ladder`, `cd_print_log`, `financial_dashboard`,
  `referring_intel`, `live_feed`, `report_ai`, `report_widgets`, `viewer_controller`,
  `admin_controller`, `ai_alerts`, `registry`, `auth_controller` (each needs individual
  inspection — query structures differ from report_25's). Operator explicitly deferred this
  rollout rather than doing all 20 in one pass — pick up here.
- Once `utils/site_scope.py`/RLS is actually wired app-side (READY TO BUILD NEXT #1), this
  whole per-report workaround may become unnecessary — RLS would enforce site scope at the
  DB level for every query automatically. That's the real fix; this is the interim patch.

All Phase 6/9/10/11/12/13/14/15 skip cleanly (clear log message, no PACS fallback) if no
`ris` db_params source is configured. `ETL_GEAR`/`RAYD_RIS_*_TABLE` env vars let table
names be overridden per-column-family without code changes.

**Site model refresher**: `PERSON` is a single shared demographic table for BOTH patients
(`PATIENT.PATIENT_PERSON_KEY = PERSON.PERSON_KEY`) and staff (`RESOURCE_ID.PERSON_KEY =
PERSON.PERSON_KEY`) — same person, two different "role" tables pointing into one base.

---

## ⛔ BLOCKED ON VENDOR (cannot build without these)

1. ~~PERSON / RESOURCE table~~ **✅ DONE** (Phase 13, `std_resources_ris`)
2. ~~PATIENT / PERSON table~~ **✅ DONE** (Phase 12, `std_patients_ris` — names excluded by design)
3. **Lookups still needed** (small, resolve key → label): `PATIENT_CLASS` (IP/OP/ER),
   `FINANCIAL_CLASS` (payer/TPA), `HOSPITAL_SERVICE`, `PRIORITY`, `BODY_PART` /
   `LATERALITY` / `CODING_SCHEME`, `INTERPRETATION_TYPE`, `VERSION_STATUS`,
   `JUSTIFICATION_STATUS` + `STATUS_REASON`, `MOBILITY_STATUS`, `RESOURCE_ROLE`
   (Radiologist/Technician/Referring/etc. labels for `std_resources_ris.resource_role_key`),
   `LANGUAGE` (`std_patients_ris.language_key`), ~~`AVAILABILITY_INDICATOR`~~ **✅ DONE**
   (Phase 15, `std_availability_indicators` — full semantic resolution: 03=Available,
   04=Unavailable, 07=Holiday, 11=Maintenance, 2100=Closed are "device unavailable" for
   utilization; the rest are booking-rule nuances on an open device), **`PEER_REVIEW`**
   (new — `std_pps.considered_for_review` workflow, table schema not sent).
4. **THE RIS↔PACS study join** — was accession/linked_id best-effort; Phase 14 added a
   real candidate. `std_pps.study_db_uid` is resolved by matching `STUDY_INSTANCE_UID`
   (a real DICOM UID) against `etl_didb_studies.study_instance_uid` — run Phase 14 and
   check the match-rate it prints; that's the actual answer, not a guess. If it's high,
   this supersedes the accession/linked_id approach in `etl_orders.py`. PACS-side
   grouping column also confirmed to exist (`medistore.didb_studies.WORKITEM_DB_UID`,
   paired with `IS_LINKED_STUDY='Y'`) but not yet pulled into `etl_didb_studies`.
5. **Device↔scheme assignment — the LAST blocker for device utilization.**
   `SCHEDULE_SCHEME` schema received 2026-07-27 and built (`std_schedule_schemes`,
   Phase 15) — but it turned out to be a generic template CATEGORY ("Normal"/"Emerg"/
   "OutPatient"/"InPatient"/"Scheme 1"/"Scheme 2"/"All Green", 7 rows), not
   device-specific at all — no `MODALITY_KEY` or any device reference on it. So we now
   know WHAT a scheme is, but not WHICH DEVICES use which scheme. Candidate: an
   undocumented column on `MODALITY` itself (vendor's own note says it has ~19 columns,
   only a subset confirmed so far) — worth checking there first before assuming another
   table exists. Also still need: which `SCHEDULE_TEMPLATE_VERSION_KEY` is currently
   effective (effective-date or active-flag on a version table?).
6. **PPS site resolution** — no org/issuer column on `PPS` itself; how it inherits site
   from `SITE_WORKLIST`/`ORDERS` is unconfirmed (same open question as `std_visits`).
7. **Qog1** — VASC (org 5120) counts as RH? (assumed YES; seeded that way in `0052`)
8. **Used-status list** — vendor to send which statuses are actually in use, to trim the
   `0047` seed. Cosmetic only, not blocking.
9. ~~DB access (read-only) + network path~~ **✅ DONE** — both RIS and PACS connections
   live and in daily use.

---

## 🔨 READY TO BUILD NEXT (unblocked, in order)

1. **Wire RLS app-side** (`utils/site_scope.py` → `install(app)`) — still not done:
   - add a second SQLAlchemy engine bound to `rayd_app`; route web requests to it,
     keep ETL/NLP/listener on `etl_user` (owner, bypasses RLS)
   - call `install(app)` in `create_app()` after login manager
   - **leak-test matrix**: RH-only user / SJH-only user / both-sites admin
   - ⚠️ **cache keys must include the effective scope** (`report_cache`, widgets) or a
     cached page leaks across sites *without touching the DB* — RLS cannot save you there
2. **Wire the new identity data into report UI** — the data exists in Postgres now but
   isn't joined into any report yet:
   - radiologist/technician names (`std_resources_ris`) — resolve
     `reading_physician_resource_key`/`signing_physician_resource_key` on
     `etl_didb_studies` to a display name; also `std_pps.primary_tech_person_key` (joins
     `std_resources_ris.person_key`, not `resource_id_key`) for the Technologist
     Productivity report
   - referring-org contact (`std_ordering_organizations`) — for CRN routing once that's
     scoped
   - patient MRN (`std_patient_ids`) where needed for display/reconciliation
3. **Technologist Productivity report** — now buildable. `std_pps` has
   `primary_tech_person_key` + `start_datetime`/`end_datetime` + `procedure_code` +
   `performing_ae_title` directly. The legacy `Technologist_Productivity_Report.rpt`
   (Crystal Reports, brought in this session then **accidentally deleted** — see
   Housekeeping) can't be referenced for its exact original logic anymore; rebuild from
   first principles using `std_pps`.
4. **Device utilization report** — `std_pps` (actual usage per device) is ready now;
   `std_availability_indicators`/`std_schedule_schemes`/`std_modality_exceptions`/
   `std_schedule_template_items` all built (Phase 15); blocked on ONE remaining link —
   which devices use which `SCHEDULE_SCHEME` (item #5 in blocked-on-vendor).
5. **KPI Detailed Reading report** — unblocked since Phase 13 (PERSON/RESOURCE exists) —
   TAT distribution matrix by radiologist. Not yet built.
6. **Site-enrichment pass, the rest of it** — partially done this session (studies↔orders
   via `etl_orders` enrichment, `age_at_study`, physician resolution, PPS study-UID
   test), but still open:
   - `std_visits.site_id` / `std_pps` site resolution (both deferred, see blocked #6)
   - `hl7_orders` / `hl7_oru_reports.site_id` via accession
   - **mismatch monitor** → `site_mismatch_log` (quantifies the SJH-mammo bug) — table
     exists (`0051`) but nothing populates it yet
   - Use `UPDATE…FROM` joins (NOT python row loops — 600k+ rows), matching this
     session's enrichment pattern
7. **`std_devices` / procedure-catalog refinements**:
   - `procedure_duration_map.body_part` still NULL — needs the `BODY_PART` lookup
   - `procedure_duration_map.duration_minutes` could be calibrated with `std_pps`
     actuals (`start_datetime`/`end_datetime`) instead of the 15-min guessed default —
     not done yet
   - demo/external device rows (USDemoPhilips, RH-PACS External Upload, SJH-CARM 2) are
     imported with their real `ACTIVE` flag rather than name-excluded — confirm this is
     sufficient once real data is visible in the mapping tab, or revisit
8. **Radiation dose feature** — `std_pps.radiation_dose`/`.radiation_dose_units` are
   loaded; check real fill-rate once Phase 14 has run, then decide if this replaces the
   regex-from-dictation-text approach `LAUMC_DATA_REQUEST.md`'s RDMS analysis assumed.
9. **Peer review workflow** — `std_pps.considered_for_review` flag is loaded; the full
   `PEER_REVIEW` table isn't (schema not sent, deferred per operator).
10. **Status-history population**: RIS-outbound ORM (`ORC-1=SC`) → read `STATUS_KEY` →
    map via `worklist_status_map` (or `std_status_ris`'s richer `CORE_STATUS` chain) →
    append to `worklist_status_history`. ORM only carries coarse codes (`A` = Scheduled
    AND Arrived) → DB read decides. Arrived/started have **no** DB timestamp column →
    message arrival time IS the transition.
11. **Unified exam view** (after the join answer sharpens, blocked #4) — `std_worklist` ↔
    `didb_studies`. Note: no dedicated `std_worklist` table was built this session —
    `etl_orders.py` extracts the operationally-relevant subset of `SITE_WORKLIST` directly
    into the existing `etl_orders` shape. A full 57-column `std_worklist` capture would
    be a separate, larger build if ever wanted — not started.
12. **`analytics_snapshots` per-site PK** restructure (deferred in `0049`): widen
    `(snapshot_date)` → `(snapshot_date, site_id)` when the analytics refresh is reworked.
13. **4 CD-burning/Weasis features** (explicitly requested earlier, not started) — note
    `std_site_pps_ext` now carries real CD-burn tracking data (`cd_burned`,
    `cd_burned_date`, `cd_burned_requested_by`, `image_sent_to`/`_2`/`_3`) that could
    directly inform this: auto-display incoming study on "Recently arrived - ready to
    burn"; add RF modality in Q/R; allow multiple CD burn; Weasis auto-copy to local
    drive + auto-point to DICOM folder.

---

## 💡 IDEAS / ASKS PARKED THIS SESSION (2026-07-27 →)

Running log — capture each one here as it comes up, don't lose it in chat scroll.

### KPI Detailed Reading — built, first pass, needs validation (2026-07-26)
The "revamp TAT per modality per radiologist" ask (a continuation of punch-list #2, Report 25)
turned out to have a concrete spec: `KPI Detailed reading.xlsx` in the repo root. Extracted its
structure (no
Excel needed — it's a zip of XML, parsed directly): per modality × patient-class block
(CT-IN / CT-Urg / CT-Out), three TAT stages bucketed into named time ranges (different
bucket widths for outpatient vs. in/urgent), radiologist rows per stage except the first
(aggregate, unattributed). `routes/report_25.py`'s `get_kpi_detailed_reading()` implements
this, replacing the old TAT heatmap + peer-ranking table in the Radiologists tab (both were
driven by `rad_cards`, which now shows raw `hl7_oru_reports.physician_id` codes as
"radiologist" for RIS-sourced reports — ballooned to hundreds of spurious entries, reported
as "an infinite list").

**Confirmed with operator**: patient-class blocks = modality × (Inpatient/Urgent-ER/Outpatient);
radiologist list must be fully dynamic from the DB, not hardcoded; `physician_id` needs
resolving to a real name via PERSON — done via `std_resources_ris.resource_id` (same
composite-ID format already used for `etl_didb_studies.reading/signing_physician_id`,
migration 0063); TAVI/Coro CT excluded by procedure_code pattern match — confirmed.

**Not yet confirmed, first-pass assumption, "let's test it, we can change the queries if the
data is illogical"**:
- Stage → timestamp mapping: `Ex. Done to Read` = `COALESCE(hl7_orders.done_at, .pacs_done_at)`
  → `rep_prelim_timestamp`; `Signed 1 to Approved` = `rep_prelim_timestamp` → `rep_final_timestamp`;
  `Exam done to Approved` = exam-done → `COALESCE(rep_final_timestamp, hl7_oru_reports.result_datetime)`
  (same fallback as migration 0070). Not verified against real data yet.
- `patient_class` has no CHECK constraint in schema — Inpatient/Outpatient split uses broad
  `ILIKE` pattern guesses (`IN%`/`OUT%`/`AMB%`), "Urgent" reuses the existing `2XE` accession
  prefix convention. Needs verification once real bucket counts are visible.
- Only CT is built (only modality with a defined SLA in the source spreadsheet) — extending
  to other modalities is mechanical once the CT numbers are validated as correct.
- The single aggregate `Ex. Done to Read` row is labeled `"Res."` (literal spreadsheet cell
  content) — unclear if that's meant as a real label or an artifact of merged Excel cells;
  the raw XML extraction didn't parse `<mergeCells>` ranges, so the exact intended header
  hierarchy is a best guess.

**Update, same day**: operator feedback — the CT-only scope took the spreadsheet too
literally. Reworked to query all modalities in one pass (grouped by (modality, class_bucket)
in pandas, not a hardcoded list) and render a dynamic modality `<select>` + patient-class tab
selector (client-side switching, embedded JSON) instead of statically dumping every block.
Non-CT modalities still reuse CT's bucket widths as a default — no other modality has a
confirmed SLA window yet.

**Next step**: operator reviews real rendered output against known-correct numbers for a
sample period, tells me what's wrong, queries get adjusted. Not done until validated.

### Operator punch list, 2026-07-26 — explicitly "for later," not started
1. **All reports — data integrity check against RIS and PACS.** No spec yet: presumably
   spot-check counts/sums in RAYD vs. querying RIS/PACS directly for the same period.
   Needs scoping (which reports, which fields, tolerance) before starting.
2. **Report 25 revamp — "not showing any data."** ✅ Root cause actually found and fixed
   this session, after three earlier attempted fixes (missing `report_template` row `0069`,
   a syntax-error-crashing-the-whole-app fix `696b51a6`, a template None-guard fix
   `255fdc51`) all landed but it was still empty. Real cause: the query required PACS's own
   `rep_final_timestamp`/`rep_final_signed_by`, which isn't reliably synced for recent
   LAUMC studies (radiologists sign in the RIS) — see the incident table above and
   `migrations/0070`. Verify on next deploy that report_25 actually shows data for a
   mid-July range; if still empty, it's a genuinely new/different cause at this point.
3. **Live AE status — revamp to a 2D real-time department status board.** Driven by live
   HL7 traffic (the MLLP listener already ingests ORM/ORU in real time) + one real-time
   query to RIS for scheduled patients. No existing route identified yet for "live AE
   status" — needs locating (or confirming it doesn't exist yet) before scoping the
   rebuild. Relates to `aetitle_modality_map`/device model already in place.
4. **ORU analytics page (`routes/oru_analytics.py`) — very slow load.** Operator's own
   suggestion: split the chart load across multiple ECharts partitioned by modality /
   radiologist / time window instead of one heavy render. Needs profiling first to confirm
   where the actual time goes (query vs. NLP word-cloud computation vs. render) before
   assuming chart-splitting alone fixes it.
5. **Storage calculation is wrong.** Likely `etl_analytics_refresh.py` (Phase 7 rollup) or
   the `etl_image_locations`/`image_size_kb`-derived totals — not diagnosed yet this
   session. Needs a concrete "expected vs. actual" number from the operator to start.
6. **Remove Patient CD Log** (`routes/cd_print_log.py`) — deletion request, straightforward
   once confirmed nothing else depends on it (check for cross-references before removing).
7. **Reporting backlog — RAYD not detecting reporting details.** ⚠️ Very likely the SAME
   root cause as #2 (`migrations/0070`'s finding): reports checking PACS's own
   `rep_final_timestamp`/`rep_final_signed_by` see almost nothing as "reported," not
   because there's a real backlog, but because PACS's `DIDB_STUDIES` isn't kept current
   with RIS-signed reports. Report_25 is fixed; whichever OTHER route surfaces "backlog"
   (not yet identified — operator to point at the specific view) likely needs the same
   `hl7_oru_reports` fallback applied.
8. **Modality opening hours all show 720 (Postgres column default), never actually mapped
   by ETL.** This is the SAME gap already tracked as blocked-on-vendor **#5** ("device↔scheme
   assignment") and READY TO BUILD NEXT **#4** — `std_schedule_template_items` exists
   (Phase 15) but isn't attributable to a specific device yet, so `device_weekly_schedule`
   never gets populated from real RIS data and stays at Postgres's `720` default for every
   AE. Not a new bug — confirms the existing blocker's real-world symptom. Unblocks once
   the `MODALITY`↔`SCHEDULE_SCHEME` link is found (see blocked #5).
9. **Procedure → modality mapping** — operator says they need to walk through this with me
   directly next session (not a spec I can start from written notes alone). Relates to
   `procedure_duration_map`/Phase 10's `SPS_CODE` import — wait for the guided session.
10. **Build CRN from ORU** — read referring/signing physician email + phone (now available
    via `std_resources_ris`, Phase 13) from ORU-resolved reports, stand up SMTP, build a
    token+URL scheme (secure link delivery, presumably for report/critical-result
    notification), and flag the NAT/networking requirement for IT so external delivery
    actually reaches physicians. First mentioned as "CRN routing" in READY TO BUILD NEXT
    #2 (`std_ordering_organizations` contact data) — this fleshes out the actual build:
    email/SMTP delivery mechanism, not just having the contact data available. Needs: SMTP
    relay details, token/URL security design (expiry? one-time use?), and the specific NAT
    ask to hand to IT before any code starts.

---

## ⚠️ GOTCHAS / DECISIONS TO NOT RE-LITIGATE

- **RLS is LAUMC-branch-only** (`0050`/`0051`/`0052`). Do NOT merge to main/single-site
  branches. `0046`–`0049` are merge-safe no-ops. Migration numbering may collide with
  main later.
- **Extract filter (global, RIS)**: `ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')`
  — the RIS holds other sites; this is both site scope and junk filter. Applied in
  `etl_orders.py`'s `ORDERS` join.
- **Never hardcode status 60/70/100** — always map via `worklist_status_map` (or the
  richer `std_status_ris.core_status_key` chain now available) — (Porter/Oral STR/
  General → Arrived; Contrast/Pre Exam → Started).
- **Table names can be misleading** — `SITE_WORKLIST` and `SITE_PPS` both have "SITE" in
  the name but neither is a hospital-site/org lookup (`SITE_WORKLIST` is just the RIS
  vendor's naming convention; `SITE_PPS` is a per-step QA/compliance extension of `PPS`).
  Don't assume a table resolves site/org from its name alone — verify.
- **NULL site = unassigned**, invisible under RLS by design. Org root `1` is deliberately
  unmapped in `site_org_map` (pre-scheduling orders). `resolve_ris_org` does NOT fall back
  to the default site.
- **`report.DOCUMENT_PLAIN_TEXT`** is the NLP feed — no RTF conversion needed (dropped).
  Blobs (DOCUMENT / PDF_DOCUMENT / MAP) intentionally NOT pulled.
- **Report accession is per-version** — amended versions get a NEW sequence; join the
  current study on the `is_max_version` row.
- **Report signature dates pulled RAW** — vendor: don't map PACS↔RIS statuses yet (Qr3),
  so KPI segment definitions are still open.
- **Mirth duplicate delivery is permanent** → MSH-10 idempotency is mandatory.
- **Listener contract**: whitelist-and-discard (parse ORM/ORU, ACK+ignore the rest, never NAK).
- **Placeholders ≠ NULL**: "Referring, Generic" (`4101031379966`/`...68` — seen again in
  the `RESOURCE_ID` sample, type-6/3 rows with value `SCH`), test patients, CSH service
  account → exclusion list, separate from NULL handling. Not yet actually filtered out of
  any loaded table — data is imported faithfully, exclusion is a report-time filter TODO.
- Backfill: **never** row-level the 166M image rows — aggregate at source to ~614k/study
  (`LAUMC_DATA_REQUEST.md` §B5) if the full Phase 3/4 row-level sync ever proves too slow
  or unnecessary — not evaluated this session (row-level sync was run instead).
- Site vocabularies may change at the ~2027 RIS/PACS upgrade → only `sites` / `site_org_map`
  rows change, never code.
- **NO PATIENT NAMES in Postgres, ever** (operator instruction, 2026-07-27). Staff/resource
  names ARE pulled (`std_resources_ris`) — different rule, don't conflate the two.
  Free-text clinical notes fields follow the same exclusion logic even when not literally
  a "name" column — see `std_pps` (`TECHNURSE_NOTES`/`DEMONSTRATION_NOTES`, never
  fetched) and `std_site_pps_ext` (6 columns excluded: `HOLDER_NAME` + 5 comment fields).
  Default toward excluding free text/names; ask if genuinely needed.
- **`RESOURCE_ID.RESOURCE_ID_KEY`**, not `PERSON_KEY`, is what `*_RESOURCE_ID_KEY` columns
  elsewhere (REPORT/ORDERS/SITE_WORKLIST) actually reference. But `std_pps.primary_tech_
  person_key` and `.created_by_person_key` genuinely ARE `*_PERSON_KEY` (not
  `*_RESOURCE_ID_KEY`) — join `std_resources_ris.person_key`, not `.resource_id_key`, for
  those two specifically. Check the exact column suffix before assuming which join applies.
- **Two RIS-authoritative device-availability tables are explicitly NOT editable from
  RAYD** (`std_modality_exceptions`, `std_schedule_template_items`, migration 0067) —
  don't build an admin edit route for either; that's how the "not editable" instruction
  is enforced (no DB-level lock, just no UI).
- Reference/catalog tables (Modality, Procedures, Ordering Orgs, Status, Priorities,
  Dictations, Modality Exceptions, Schedule Template Items) = full pull, no date filter.
  Transactional tables (Orders, Visits, Patients, Resources, PPS) = also currently full
  pull for Patients/Resources (no clean watermark available); Orders/Visits/PPS use
  `created_on_date`/`created_date >= go_live`.

---

## 🧹 HOUSEKEEPING

- **⚠️ `Technologist_Productivity_Report.rpt` was accidentally deleted this session
  (2026-07-27).** Operator added it to the project root for reference; while cleaning up
  a stray extraction-artifact folder from investigating the file (`rm -rf
  "Technologist_Productivity_Report/"`), the original `.rpt` file was also lost — cause
  not fully understood (a directory-scoped delete shouldn't have touched a
  differently-named file). Recovery attempted and failed: not in Windows Recycle Bin
  (Git Bash `rm` bypasses it), folder wasn't OneDrive-synced (no version history), no
  Volume Shadow Copy available. The file was never committed to git (untracked at time
  of loss), so git history can't recover it either. Only the decomposed internal OLE
  streams survive, in a scratchpad temp dir — not a usable `.rpt`. If the operator has
  another copy (original email/source, Crystal Reports Designer's recent-files list,
  network share), that's the only real recovery path. The report's *purpose* is not
  lost, though — `std_pps` (Phase 14) has everything needed to rebuild an equivalent
  Technologist Productivity report from scratch (see READY TO BUILD NEXT #3).
- **Two `.xlsx` working files were committed by an over-broad `git add -A`** in `0a843c67`:
  `KPI Detailed reading.xlsx`, `PACS_permissions_map.xlsx`. Status unconfirmed this
  session — user previously said "i am fine with the passwords, no access is given and
  noone beside me access this repo. ignore the alarm," so likely intentionally left as-is.
  If ever revisited: `git rm --cached "KPI Detailed reading.xlsx" "PACS_permissions_map.xlsx"`
  + add to `.gitignore`. (Blobs remain in history; a rewrite would be needed to purge fully.)
- `update.sh` is the correct tool for pushing fixes to the live LAUMC install (pulls +
  rebuilds + applies pending migrations via `psql`). `install.sh` is fresh-install /
  data-reset only — do not use it on the live site.
- ETL phase count is now 15. Full list: `docker compose exec rayd-app python app.py -m`
  with no `RAYD_ETL_PHASES` set runs all of them in order; use
  `RAYD_ETL_PHASES=6,9,10,11,12,13,14,15` to run just the RIS pipeline, or
  `RAYD_ETL_INTERACTIVE=1` to be prompted per-phase. Phase 14 (PPS) has internal
  ordering that matters (lookups → std_pps → enrichment → std_site_pps_ext); don't split
  it across separate `RAYD_ETL_PHASES` runs.
