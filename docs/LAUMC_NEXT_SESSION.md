# LAUMC — Next Session: what's done, what's left

Pick up here. Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor Qs), `LAUMC_DATA_REQUEST.md` (discovery record).

Last major update: 2026-07-27 — full RIS pipeline build (Phases 6, 9–13) + infra hardening.

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

### RIS pipeline — Phases 6, 9–13 (2026-07-27, all new/changed this session)
| Phase | Source → Target | Notes |
|---|---|---|
| **6** Orders | `SITE_WORKLIST ⋈ ORDERS ⋈ SPS_CODE` → `etl_orders` | Switched from PACS `MDB_ORDERS` (messier data) per operator decision. `order_status` translated via `worklist_status_map` to short codes (`CA`/`CM`/raw stage) existing reports hardcode. `study_db_uid` resolved by 2-stage enrichment: primary `accession_number = sps_id` match, `linked_id`-group fallback for multi-SPS orders. `patient_dbid` = RIS `patient_person_key` (PACS identity NOT reconciled here, by design). |
| **9** RIS Reports | `REPORT.DOCUMENT_PLAIN_TEXT` → `hl7_oru_reports` | Coexists with live HL7 ORU listener via `report_source` tag + fill-only merge (RIS text wins on report/impression, listener fills gaps). |
| **10** RIS Catalog | `MODALITY⋈MODALITY_TYPE`→`aetitle_modality_map`, `SPS_CODE`→`procedure_duration_map`, `ORDERING_ORGANIZATION`→**`std_ordering_organizations`** (new) | Fill-only on the first two (never overwrite manual mapping-tab edits/RVUs); refresh-on-conflict on ordering orgs (no RAYD-owned fields there). |
| **11** RIS Visits | `VISIT` → **`std_visits`** (new) | Feeds case-mix/payer/LOS/hospital-service. `site_id` **not resolved** — VISIT carries no org/issuer column of its own. `patient_class_key`/`financial_class_key`/`hospital_service_key`/`mobility_status_key` pulled raw (lookups pending). `deleted='Y'` rows imported, not dropped. |
| **12** RIS Patients | `PATIENT⋈PERSON`→**`std_patients_ris`**, `PATIENT_ID_LIST`→**`std_patient_ids`** (both new) | **NO PATIENT NAMES** (operator instruction, PHI) — `PATIENT_ALIAS` dropped from scope entirely as a result (100% name data). `gender_key` resolved to `F/M/U/I/0/NSP/O/A` via hardcoded lookup. `language_key` still raw. Also runs **`age_at_study`** enrichment on `etl_didb_studies` (new column, `migrations/0061`) — computed from `std_patients_ris.birth_date` via the `etl_orders` bridge, kept separate from PACS's unreliable `age_at_exam`. |
| **13** RIS Resources | `RESOURCE_ID⋈PERSON`→**`std_resources_ris`** (new) | **Names + contact INCLUDED** (staff, not patient PHI — KPI needs radiologist names, CRN needs referring-physician email/phone). Vendor-confirmed: `REPORT`/`ORDERS`/`SITE_WORKLIST`'s `*_RESOURCE_ID_KEY` columns reference `RESOURCE_ID.RESOURCE_ID_KEY`, not `PERSON_KEY`. Also resolves `reading_physician_resource_key`/`signing_physician_resource_key` on `etl_didb_studies` by matching the composite `email@domain_numericid` string already sitting in `reading_physician_id`/`signing_physician_id` — fixes data already loaded, not just future rows. `resource_role_key` still raw (no role lookup yet). `site_id` resolved via `site_org_map` (RESOURCE_ID carries a real org key, unlike VISIT). |

All Phase 6/9/10/11/12/13 skip cleanly (clear log message, no PACS fallback) if no `ris`
db_params source is configured. `ETL_GEAR`/`RAYD_RIS_*_TABLE` env vars let table names
be overridden per-column-family without code changes.

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
   `JUSTIFICATION_STATUS` + `STATUS_REASON`, `MOBILITY_STATUS`,
   **`RESOURCE_ROLE`** (new — Radiologist/Technician/Referring/etc. labels for
   `std_resources_ris.resource_role_key`), **`LANGUAGE`** (new —
   `std_patients_ris.language_key`).
4. **THE RIS↔PACS study join** — partially resolved 2026-07-27: RIS-side linking
   confirmed as `LINKED_ID`; PACS-side grouping column confirmed to exist
   (`medistore.didb_studies.WORKITEM_DB_UID`, paired with `IS_LINKED_STUDY='Y'`), but the
   *exact* resolution rule (which linked SPS's accession the merged PACS study actually
   carries) is still not nailed down. Current `etl_orders.py` enrichment uses a
   best-effort `linked_id`-group fallback (inherit a sibling SPS's resolved study) — works
   for the common case, unverified for edge cases. `WORKITEM_DB_UID` itself is not yet
   pulled into `etl_didb_studies` at all — would sharpen this if added.
5. **Qog1** — VASC (org 5120) counts as RH? (assumed YES; seeded that way in `0052`)
6. **Used-status list** — vendor to send which statuses are actually in use, to trim the
   `0047` seed. Cosmetic only, not blocking.
7. ~~DB access (read-only) + network path~~ **✅ DONE** — both RIS and PACS connections
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
     `etl_didb_studies` to a display name
   - referring-org contact (`std_ordering_organizations`) — for CRN routing once that's
     scoped
   - patient MRN (`std_patient_ids`) where needed for display/reconciliation
3. **KPI Detailed Reading report** — now unblocked (PERSON/RESOURCE exists as of Phase
   13) — TAT distribution matrix by radiologist. Not yet built.
4. **Site-enrichment pass, the rest of it** — partially done this session (studies↔orders
   via `etl_orders` enrichment, `age_at_study`, physician resolution), but still open:
   - `std_visits.site_id` (needs a join through `etl_orders.visit_dbid` — deferred, see
     Phase 11 notes)
   - `hl7_orders` / `hl7_oru_reports.site_id` via accession
   - **mismatch monitor** → `site_mismatch_log` (quantifies the SJH-mammo bug) — table
     exists (`0051`) but nothing populates it yet
   - Use `UPDATE…FROM` joins (NOT python row loops — 600k+ rows), matching this
     session's enrichment pattern
5. **`std_devices` / procedure-catalog refinements**:
   - `procedure_duration_map.body_part` still NULL — needs the `BODY_PART` lookup (#3
     above)
   - demo/external device rows (USDemoPhilips, RH-PACS External Upload, SJH-CARM 2) are
     imported with their real `ACTIVE` flag rather than name-excluded — confirm this is
     sufficient once real data is visible in the mapping tab, or revisit
6. **Status-history population**: RIS-outbound ORM (`ORC-1=SC`) → read `STATUS_KEY` →
   map via `worklist_status_map` → append to `worklist_status_history`.
   Note: ORM only carries coarse codes (`A` = Scheduled AND Arrived) → DB read decides.
   Arrived/started have **no** DB timestamp column → message arrival time IS the transition.
7. **Unified exam view** (after the join answer sharpens, #4 above) — `std_worklist` ↔
   `didb_studies`. Note: no dedicated `std_worklist` table was built this session —
   `etl_orders.py` extracts the operationally-relevant subset of `SITE_WORKLIST` directly
   into the existing `etl_orders` shape. A full 57-column `std_worklist` capture (all of
   `SITE_WORKLIST`, not just what `etl_orders` needs) would be a separate, larger build if
   ever wanted — not started.
8. **`analytics_snapshots` per-site PK** restructure (deferred in `0049`): widen
   `(snapshot_date)` → `(snapshot_date, site_id)` when the analytics refresh is reworked.
9. **4 CD-burning/Weasis features** (explicitly requested earlier, not started):
   auto-display incoming study on "Recently arrived - ready to burn"; add RF modality in
   Q/R; allow multiple CD burn; Weasis auto-copy to local drive + auto-point to DICOM folder.

---

## 💡 IDEAS / ASKS PARKED THIS SESSION (2026-07-27 →)

Running log — capture each one here as it comes up, don't lose it in chat scroll.
Nothing below is scoped or built yet unless marked done.

*(empty — populate as ideas come in)*

---

## ⚠️ GOTCHAS / DECISIONS TO NOT RE-LITIGATE

- **RLS is LAUMC-branch-only** (`0050`/`0051`/`0052`). Do NOT merge to main/single-site
  branches. `0046`–`0049` are merge-safe no-ops. Migration numbering may collide with
  main later.
- **Extract filter (global, RIS)**: `ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')`
  — the RIS holds other sites; this is both site scope and junk filter. Applied in
  `etl_orders.py`'s `ORDERS` join.
- **Never hardcode status 60/70/100** — always map via `worklist_status_map`
  (Porter/Oral STR/General → Arrived; Contrast/Pre Exam → Started).
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
- **`RESOURCE_ID.RESOURCE_ID_KEY`**, not `PERSON_KEY`, is what `*_RESOURCE_ID_KEY` columns
  elsewhere (REPORT/ORDERS/SITE_WORKLIST) actually reference.
- Reference/catalog tables (Modality, Procedures, Ordering Orgs) = full pull, no date
  filter. Transactional tables (Orders, Visits, Patients, Resources) = also currently full
  pull for Patients/Resources (no clean watermark available); Orders/Visits use
  `created_on_date >= go_live`.

---

## 🧹 HOUSEKEEPING

- **Two `.xlsx` working files were committed by an over-broad `git add -A`** in `0a843c67`:
  `KPI Detailed reading.xlsx`, `PACS_permissions_map.xlsx`. Status unconfirmed this
  session — user previously said "i am fine with the passwords, no access is given and
  noone beside me access this repo. ignore the alarm," so likely intentionally left as-is.
  If ever revisited: `git rm --cached "KPI Detailed reading.xlsx" "PACS_permissions_map.xlsx"`
  + add to `.gitignore`. (Blobs remain in history; a rewrite would be needed to purge fully.)
- `update.sh` is the correct tool for pushing fixes to the live LAUMC install (pulls +
  rebuilds + applies pending migrations via `psql`). `install.sh` is fresh-install /
  data-reset only — do not use it on the live site.
- ETL phase count is now 13. Full list: `docker compose exec rayd-app python app.py -m`
  with no `RAYD_ETL_PHASES` set runs all of them in order; use
  `RAYD_ETL_PHASES=6,9,10,11,12,13` to run just the RIS pipeline, or `RAYD_ETL_INTERACTIVE=1`
  to be prompted per-phase.
