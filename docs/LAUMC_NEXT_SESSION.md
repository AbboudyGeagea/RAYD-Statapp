# LAUMC — Next Session: what's done, what's left

Pick up here. Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor Qs), `LAUMC_DATA_REQUEST.md` (discovery record).

---

## ✅ BUILT & PUSHED (branch `LAUMC`)

| Item | Where |
|---|---|
| `sites` tri-vocabulary table + RH/SJH seed | `migrations/0046` |
| Status lifecycle map (STATUS_KEY → canonical stage, ~30 alias rollups) | `migrations/0047` |
| RAYD's own status-history table | `migrations/0048` |
| `site_id` columns + indexes on studies/orders/hl7/oru/aetitle/snapshots | `migrations/0049` |
| **RLS**: `rayd_app` role + fail-closed policies (owner `etl_user` bypasses) | `migrations/0050` |
| `pacs_site_id_raw` + `site_mismatch_log` (RIS-vs-PACS monitor / mammo bug) | `migrations/0051` |
| **`site_org_map`** (many-org→one-site) + **`user_sites`** grants | `migrations/0052` |
| **RIS registry → real vendor schemas** (6 tables) | `ETL_JOBS/system_type_registry.py` |
| Site resolver (PACS / RIS-issuer / RIS-org / HL7-building → site_id) | `utils/site_resolver.py` |
| Site-scope request logic (**inert — not wired**) | `utils/site_scope.py` |
| PACS studies ETL pulls `SITE_ID` → `pacs_site_id_raw` | `ETL_JOBS/etl_didb_studies.py` |
| **Portal + Scheduling modules physically removed** (−3,732 lines) | many |

**Registry target tables (all columns = real vendor schema):**
`std_worklist` (57c, pk site_worklist_key, wm last_update_date) ·
`std_orders` (47c, pk order_key, wm created_on_date) ·
`std_reports` (48c, **composite pk report_key+version**, wm last_modified_date) ·
`std_devices` (MODALITY+MODALITY_TYPE merged) · `std_procedure_codes` · `std_visits`

---

## ⛔ BLOCKED ON VENDOR (cannot build without these)

1. **⭐ PERSON / RESOURCE table(s)** — everything references `*_RESOURCE_ID_KEY` /
   `*_PERSON_KEY`. Needed for: radiologist **full names in the KPI report**, and
   **referring physician contact (email/phone) for CRN**. Highest priority.
2. **PATIENT / PERSON table** — resolve `PATIENT_PERSON_KEY` → identity (+phone).
   Decision made: pull RIS (`std_patients_ris`) AND keep PACS; reconcile by MRN.
   (PACS `etl_patient_view` uses a *different* key and cannot resolve RIS rows.)
3. **Lookups**: PATIENT_CLASS (IP/OP/ER — feeds KPI), FINANCIAL_CLASS (payer),
   HOSPITAL_SERVICE, PRIORITY, BODY_PART / LATERALITY / CODING_SCHEME,
   ORDERING_ORGANIZATION, INTERPRETATION_TYPE, VERSION_STATUS, JUSTIFICATION_STATUS,
   STATUS_REASON, MOBILITY_STATUS.
4. **THE RIS↔PACS study join** — *the* gate for the exam view:
   `SPS_ID = didb_studies.ACCESSION_NUMBER`, **or** `LINKED_ID = <which PACS column?>`
   Vendor said both: SPS_ID (100500*) *is* the PACS accession, AND "LINKED_ID is the key
   between RIS and PACS". Need one sentence: *join on X = Y*. Also: when several SPS link
   into ONE PACS study, does that study carry only the primary SPS_ID?
5. **Qog1** — VASC (org 5120) counts as RH? (assumed YES; seeded that way in 0052)
6. **Used-status list** — vendor to send which statuses are actually in use, to trim the
   `0047` seed ("so we do not flood the app"). Current superset is harmless meanwhile.
7. DB access (read-only) + network path — gates *any* live verification.

---

## 🔨 READY TO BUILD NEXT (unblocked, in order)

1. **Wire RLS app-side** (`utils/site_scope.py` → `install(app)`):
   - add a second SQLAlchemy engine bound to `rayd_app`; route web requests to it,
     keep ETL/NLP/listener on `etl_user` (owner, bypasses RLS)
   - call `install(app)` in `create_app()` after login manager
   - **leak-test matrix**: RH-only user / SJH-only user / both-sites admin
   - ⚠️ **cache keys must include the effective scope** (`report_cache`, widgets) or a
     cached page leaks across sites *without touching the DB* — RLS cannot save you there
2. **Site-enrichment ETL pass** (set-based, idempotent, re-runnable):
   - `std_orders.site_id` ← sites via `issuer_of_placer_order_number` (authoritative)
   - `std_worklist.site_id` ← `site_org_map` via `org_structure_key`
   - `etl_didb_studies.site_id` ← RIS order via accession; **fallback** `pacs_site_id_raw`
   - `hl7_orders` / `hl7_oru_reports.site_id` ← via accession
   - **mismatch monitor** → `site_mismatch_log` (quantifies the SJH-mammo bug)
   - Use `UPDATE…FROM` joins to `sites` (NOT python row loops — 600k+ rows)
3. **3-phase ETL runner**: parallel extract (RIS ‖ PACS) → **one idempotent enrichment
   pass** → per-site snapshots. Do NOT serialize the pulls; do NOT link during extract.
4. **`std_devices` ETL transform**: resolve `modality_type_key` → `MODALITY_TYPE.CODE`
   at load (vendor: "merge them already in one table"); exclude demo/external rows
   (USDemoPhilips, RH-PACS External Upload).
5. **Status-history population**: RIS-outbound ORM (`ORC-1=SC`) → read `STATUS_KEY` →
   map via `worklist_status_map` → append to `worklist_status_history`.
   Note: ORM only carries coarse codes (`A` = Scheduled AND Arrived) → DB read decides.
   Arrived/started have **no** DB timestamp column → message arrival time IS the transition.
6. **Unified exam view** (after the join answer) — `std_worklist` ↔ `didb_studies`.
7. **KPI Detailed Reading report** (after PERSON table) — TAT distribution matrix.
8. **`analytics_snapshots` per-site PK** restructure (deferred in 0049): widen
   `(snapshot_date)` → `(snapshot_date, site_id)` when the analytics refresh is reworked.

---

## ⚠️ GOTCHAS / DECISIONS TO NOT RE-LITIGATE

- **RLS is LAUMC-branch-only** (0050/0051/0052). Do NOT merge to main/single-site branches.
  0046–0049 are merge-safe no-ops. Migration numbering may collide with main later.
- **Extract filter (global)**: `ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')` —
  the RIS holds other sites; this is both site scope and junk filter.
- **Never hardcode status 60/70/100** — always map via `worklist_status_map`
  (Porter/Oral STR/General → Arrived; Contrast/Pre Exam → Started).
- **NULL site = unassigned**, invisible under RLS by design. Org root `1` is deliberately
  unmapped in `site_org_map` (pre-scheduling orders). `resolve_ris_org` does NOT fall back
  to the default site.
- **`report.DOCUMENT_PLAIN_TEXT`** is the NLP feed — no RTF conversion needed (dropped).
  Blobs (DOCUMENT / PDF_DOCUMENT / MAP) intentionally NOT in the registry.
- **Report accession is per-version** — amended versions get a NEW sequence; join the
  current study on the `is_max_version` row.
- **Report signature dates pulled RAW** — vendor: don't map PACS↔RIS statuses yet (Qr3),
  so KPI segment definitions are still open.
- **Mirth duplicate delivery is permanent** → MSH-10 idempotency is mandatory.
- **Listener contract**: whitelist-and-discard (parse ORM/ORU, ACK+ignore the rest, never NAK).
- **Placeholders ≠ NULL**: "Referring, Generic" (4101031379966), test patients, CSH service
  account → exclusion list, separate from NULL handling.
- Backfill: **never** row-level the 166M image rows — aggregate at source to ~614k/study.
- Site vocabularies may change at the ~2027 RIS/PACS upgrade → only `sites` / `site_org_map`
  rows change, never code.

---

## 🧹 HOUSEKEEPING

- **Two `.xlsx` working files were committed by an over-broad `git add -A`** in `0a843c67`:
  `KPI Detailed reading.xlsx`, `PACS_permissions_map.xlsx`. If unwanted:
  `git rm --cached "KPI Detailed reading.xlsx" "PACS_permissions_map.xlsx"` + add to
  `.gitignore`. (Blobs remain in history; a rewrite would be needed to purge fully.)
- `update.sh` LAUMC line + `.claude/settings.local.json` also rode along in that commit.
- **Live import/verification was NOT possible** on the Windows host (no flask/jinja2, no
  running container). Removals verified statically: no dangling imports, no template
  `url_for` to removed endpoints, no python refs to deleted modules. **Run the stack once
  next session** (`docker compose ... up -d`) to confirm boot before deploying.
