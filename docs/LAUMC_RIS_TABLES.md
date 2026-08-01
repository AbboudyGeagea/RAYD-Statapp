# LAUMC RIS — Table Roles & Mappings

Running map collected from the vendor (2026-07-07). **Do not build until the vendor says "build".**
Each RIS source table maps into a RAYD `std_*` target via the adapter registry.

**GLOBAL EXTRACT RULE:** the RIS holds data for more than LAUMC's two sites. Pull ONLY LAUMC:
filter on `ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')` (exclude all other issuer
values) wherever the column is available, and/or by the two known ORG_STRUCTURE_KEY values
(3926=RH, 5320=SJH). This is both the site filter and the noise/junk exclusion.

---

## SITE_WORKLIST  →  target `std_worklist`  (exam/SPS grain; renamed from std_orders per Qo1)
**Role:** master worklist — holds every orderable procedure, **1 row per procedure step**,
`STATUS` reflects the *current* status (mutated in place; no per-row history → RAYD builds its
own `worklist_status_history`). **PK: `SITE_WORKLIST_KEY`. Watermark: `LAST_UPDATE_DATE`.**

**Key roles (inferred types — flag any wrong):**
- **Identity/keys** (BIGINT): SITE_WORKLIST_KEY (PK), PATIENT_PERSON_KEY, VISIT_KEY, ORDER_KEY,
  ORDER_GROUP_KEY, ORDERING_GROUP_KEY, ORDERING_ORGANIZATION_KEY, REQUESTED_PROCEDURE_ID,
  PPS_KEY, DICTATION_KEY, REPORT_KEY, **LINKED_ID** (links multiple SPS → one report),
  RECALL_PPS_KEY, FOLLOWUP_PPS_KEY, LOCK_SESSION_KEY, MESSAGE_CR_KEY
- **Accession:** `SPS_ID` (TEXT, `100500*`) = the accession number the RIS mints at scheduling;
  **same value as the PACS accession number across ALL tables**, and consistent across all RIS
  tables. → the universal accession identifier.
- **RIS↔PACS link:** `LINKED_ID` = "the key between RIS and PACS" (vendor) — exact join pending Q2b.
- IGNORE (all NULL at LAUMC): `PACS_SPS_ID`, all five `*_FLAGSET`, `DICTATED_BY_TEST`.
- **Codes:** SPS_CODE_KEY, RP_CODE_KEY, PPS_CODE_KEY, FOLLOWUP_TYPE_KEY, MODALITY_TYPE (TEXT: CT/MR/US…)
- **Status:** `STATUS_KEY` (INT → worklist_status_map → canonical stage), `STATUS` (TEXT label)
- **Site:** `ORG_STRUCTURE_KEY` (TEXT → sites.ris_org_struct → canonical site_id; 3926=RH/5320=SJH)
- **Priority:** ORDER_PRIORITY (TEXT)
- **Timestamps (DATE/TIMESTAMP):** SCHEDULED_DATE, PERFORMED_DATE, APPROVED_DATE, REQUEST_DATETIME,
  SPS_CREATED_DATE, ROW_CREATED_DATE, REPORT_LAST_MODIFIED_DATE, MESSAGE_CREATED_DATE, LAST_UPDATE_DATE
  (NOTE: no arrived/started columns → those come from status events, per earlier finding)
- **People:** REQUESTED_BY_PERSON_KEY, REQUESTED_BY_RESOURCE_ID, REQUESTED_BY_RESOURCE_ID_NAME,
  JUSTIFIED_BY_PERSON_KEY, JUSTIFIED_BY_RESOURCE_ID_KEY, ASSIGNRADCODEPERSONKEY (assigned radiologist?),
  PROT_HOLD_BY_PERSON_KEY, TECHNICIAN, REFFERING_PHISICIAN, REFFERING_DOCTOR, MESSAGE_CREATED_BY,
  DICTATED_BY_TEST (see Q3)
- **Patient (denormalized):** LAST_NAME, FIRSTARABIC, LASTARABICNAME, NAME_PREFIX, GENDER_DESCRIPTION,
  DECEASED (bool?)
- **Flags:** INTO_PRIVATE_FOLDER (bool?)  [*_FLAGSET all NULL → ignored]
- **BI-RADS:** BIRAD_CATEGORY, BIRADS_BIRADS_KEY (parked feature)
- **Other:** DESCRIPTION (TEXT)

**GRAIN (golden rule, vendor-confirmed):** 1 row per SPS, each with its own SITE_WORKLIST_KEY.
A multi-procedure requisition (e.g. CT abdomen + pelvis) = multiple SPS rows; LINKED_ID groups
them for report-level counting.

**Open (SITE_WORKLIST):**
- Q2b — THE join question: to join a SITE_WORKLIST row to its PACS `didb_studies` row, do I use
  `SPS_ID = didb_studies.ACCESSION_NUMBER` (per-exam), OR `LINKED_ID = <some didb_studies column>`?
  And when several SPS link into ONE PACS study, does that study carry one accession (the primary
  SPS_ID) while LINKED_ID ties the rest? (STILL OPEN.)

---

## REPORT  →  target `std_reports` (durable report store + NLP feed)
**Role:** report content received from PACS via ORU into the RIS. **This replaces the ORU stream
as RAYD's durable, complete, re-processable NLP feed** (PACS reports are encrypted; this is plain
text). Feeds NLP, CRN content, full-text search, and the report-TAT KPIs. **PK: `REPORT_KEY`
(+ `VERSION` — see Q). Watermark: `LAST_MODIFIED_DATE`.**

**Key roles (inferred types):**
- **Keys/join:** `REPORT_KEY` (BIGINT; = SITE_WORKLIST.REPORT_KEY), `REPORTED_ACC_NUMBER` (TEXT =
  accession = SPS_ID → join to worklist/studies), BODY_KEY, CR_MESSAGE_KEY, REPORT_TEMPLATE_KEY
- **Versioning:** `VERSION` (INT), `IS_MAX_VERSION` (bool → filter to current version),
  VERSION_STATUS_KEY, FINALIZATION_STATE, INTERPRETATION_TYPE_KEY, ADDENDUM
- **Content:** `DOCUMENT_PLAIN_TEXT` (TEXT → **THE NLP feed / CRN body**), DOCUMENT_TEXT & DOCUMENT
  (formatted/RTF — ignore), PDF_DOCUMENT (blob — ignore), MAP, FORMAT
- **Report-chain timestamps (→ TAT / KPI Detailed Reading):** DRAFT_DATE, WET_READ_DATE (prelim),
  TRANSCRIPTION_DATE, VERIFIED1_DATE / VERIFIED2_DATE / VERIFIED3_DATE (signatures?), APPROVED_DATE
  (final), REVIEWED_DATE, RETURNED_DATE, REPORT_TIME, REPORT_CREATED_DATE, LAST_MODIFIED_DATE
- **People (resource id keys → radiologist identities):** REPORTED_BY, TRANSCRIBED_BY_RESOURCE_ID_KEY,
  VERIFIED1/2/3_BY_RESOURCE_ID_KEY, APPROVED_BY_RESOURCE_ID_KEY, CREATED_BY_RESOURCE_ID_KEY,
  LAST_MODIFIED_RESOURCE_ID_KEY, SIGNED_BEHALF_RESOURCE_ID_KEY, WET_READ_BY_RESOURCE_ID_KEY,
  REVIEWED_BY_RESOURCE_ID_KEY, RETURNED_BY_RESOURCE_ID_KEY, DRAFT_BY_RESOURCE_ID_KEY, REPORT_TO
- **Effort/size metrics (bonus productivity stats):** CHARACTER_COUNT, WORD_COUNT, LINE_COUNT,
  TOTAL_LINES_IN_DOCUMENT, MINUTES_OF_EDITING_FOR_SESSION
- **Flags/other:** DISTRIBUTED, NOTE_TO_RAD_FLAG, PRINT_RULES_GUI_OVERRIDE

**RESOLVED (REPORT), vendor 2026-07-07:**
- Qr1 ✓ — multiple rows per report, one per VERSION; `IS_MAX_VERSION` marks current. Pull
  IS_MAX_VERSION for the live report; keep prior versions for amendment history.
- Qr2 ✓ — `REPORTED_ACC_NUMBER` = accession (`100500*`) = SPS_ID, **but per-version**: an updated
  version gets a NEW sequence. → join on the IS_MAX_VERSION row's accession. WATCH: amended
  reports change accession sequence; use max-version for the current-study join.
- Qr3 ⏸ — DO NOT map report signature dates to PACS/RIS statuses now. **Pull all date/status/
  people columns RAW; PACS↔RIS status mapping done later** (defers KPI segment definitions).
- Qr4 ✓ — `DOCUMENT_PLAIN_TEXT` = FULL clean report with LABELED sections (INDICATION / TECHNIQUE /
  FINDINGS / IMPRESSION). Impression extraction = text after "IMPRESSION:". DOCUMENT/PDF ignored.
  Confirmed ideal for NLP + CRN body.

---

## ORDERS  →  target `std_orders` (order header)  [naming: see Qo1]
**Role:** incoming orders from HIS (SAP). **1 row per ORDER (header)** — the parent of
SITE_WORKLIST's SPS rows (one order → many SPS). Linked by `ORDER_KEY`. Carries the
authoritative site issuer and the HIS-side placer/filler numbers.
**PK: `ORDER_KEY`. Watermark: likely CREATED_ON_DATE / a last-update (confirm Qo4).**

**GRAIN NOTE — the two-table split:**
- ORDERS (this table)  = order header, 1/order, PK ORDER_KEY  → `std_orders`
- SITE_WORKLIST        = SPS/exam,   1/SPS,   PK SITE_WORKLIST_KEY → **rename target to `std_worklist`?**
  (earlier tentatively mapped SITE_WORKLIST→std_orders; now that a real ORDERS header exists,
  the exam-grain worklist should get its own target. See Qo1.)

**Key roles (inferred types):**
- **Keys/join:** `ORDER_KEY` (BIGINT, = SITE_WORKLIST.ORDER_KEY), VISIT_KEY, PATIENT_PERSON_KEY,
  ORDERING_ORGANIZATION_KEY, ORDERING_GROUP_KEY, FOLLOWUP_PPS_KEY
- **Site (authoritative):** `ISSUER_OF_PLACER_ORDER_NUMBER` (TEXT = SAP_PROD/SAP_SJH → sites.ris_issuer
  → site_id) + `ORG_STRUCTURE_KEY` (cross-check). ← the authoritative site source lives here.
- **Accession / HIS numbers:** `ACCESSION_NUMBER` + ISSUER_OF_ACCESSION_NUMBER (see Qo2 — is this the
  HIS/SAP accession, distinct from the RIS 100500* SPS_ID?), PARENT_PLACER_ORDER_NUMBER,
  PLACER_GROUP_NUMBER, PLACER_ORDER_NUMBER, FILLER_ORDER_NUMBER, ISSUER_OF_FILLER_ORDER_NUMBER
- **Status:** STATUS_KEY (INT), STATUS_REASON_KEY, JUSTIFICATION_STATUS_KEY
- **Priority:** PRIORITY_KEY, SCHEDULE_PRIORITY_KEY
- **Clinical text:** REASON_FOR_ORDER, COMMENTS, SPECIAL_INSTRUCTIONS, VISITATION_COMMENTS
- **Workflow/governance:** ORDER_DEPARTMENT, ISOLATION_STATUS (infection isolation flag),
  PROTOCOL_REQUIRED_FLAG, PROTOCOL_COMPLETED_FLAG, JUSTIFIED_ON_DATE (appropriateness),
  REVIEW_EORDER_FLAG, SECOND_OPINION_EORDER_FLAG, IMPORT_STUDY_ORDER, ORDERED_PROCEDURES (Qo3)
- **CRN-relevant (report/image delivery destination):** REQUESTING_REPORT_DELIVERY_KEY,
  REQUESTING_IMAGE_DELIVERY_KEY, REQUESTING_SEND_TO, REQUESTING_ADDRESS_KEY  ← may feed CRN routing
- **People:** SIGNED_BY_RESOURCE_ID_KEY, REQUESTED_BY_RESOURCE_ID_KEY, JUSTIFIED_BY_RESOURCE_ID_KEY,
  CREATED_BY_RESOURCE_ID_KEY, CANCELLED_BY_PERSON_KEY
- **Dates:** REQUEST_DATETIME, JUSTIFIED_ON_DATE, CREATED_ON_DATE, SIGNED_BY_DATE, RECOMMENDED_SCHEDULE_DATE
- **Flags:** WORKLIST_FLAGSET (null like the others? → ignore if so, Qo3)

**RESOLVED (ORDERS), vendor 2026-07-07:**
- Qo1 ✓ — ORDERS→`std_orders` (header); SITE_WORKLIST→`std_worklist` (exam grain).
- Qo2 ✓ — `ACCESSION_NUMBER` here = HIS/SAP accession (NOT the join). Universal PACS join stays
  `SPS_ID` (100500*). **Extract filter: ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
  only — exclude all other issuer values.**
- Qo3 ✓ — `ORDERED_PROCEDURES`, `WORKLIST_FLAGSET` all NULL → ignore.
- Qo4 ✓ — watermark `CREATED_ON_DATE`. Volume tiny: ≤500 rows/day across BOTH sites.

---

> **RETARGETED (2026-07, vendor-confirmed):** MODALITY does NOT get a separate `std_devices`
> table — it loads into RAYD's **existing `aetitle_modality_map`** (extended by migration 0053
> with station_name, room_code, active, ris_modality_key). Reason: every report / capacity
> ladder / device grid / mapping tab already reads `aetitle_modality_map`, so a new table would
> force a full-codebase rewrite for no gain. Same for SPS_CODE → **`procedure_duration_map`**
> (see below). Import policy = FILL-ONLY: insert new devices, `ON CONFLICT (aetitle) DO NOTHING`
> so manual edits + RAYD-owned `daily_capacity_minutes` survive; RIS never deletes rows.
> Column map: AE_TITLE→aetitle, MODALITY_TYPE→modality (resolved), DESCRIPTION→description,
> STATION_NAME→station_name, CODE→room_code, ORG_STRUCTURE_KEY→site_id (via site_org_map),
> ACTIVE→active, MODALITY_KEY→ris_modality_key.

## MODALITY  →  loads into existing `aetitle_modality_map`  (was std_devices — retargeted)
**Role:** device/room registry. **Maps `AE_TITLE` → room CODE + DESCRIPTION + STATION_NAME +
MODALITY_TYPE + SITE.** This is the backbone of per-device analytics and is far richer than the
manual `aetitle_modality_map` — likely REPLACES/populates it. `AE_TITLE` joins to PACS
`didb_studies.storing_ae`. Not incremental — reload occasionally.

**Named columns (vendor):** MODALITY_KEY (PK), CODE, DESCRIPTION, AE_TITLE, STATION_NAME,
MODALITY_TYPE_KEY.
**Data had ~19 columns** — extras include an active flag (Y), created date, a location note
(ER / 1st Floor / RH-PACS), **ORG_STRUCTURE_KEY (site)**, last-update date, and trailing flags.

**MODALITY_TYPE_KEY values observed:** 21501=CT, 21505=MRI, 21512=US, 21504=Mammo, 21500=CR,
22=XR/Fluoro, 21540=IR/Angio/Cath, 21507=BMD/DEXA, 21509=PET-CT, 21506=SPECT, 21503=Portable XR,
21740=External upload. (→ needs a MODALITY_TYPE lookup, Qm4.)

**⚠️ IMPORTANT — ORG_STRUCTURE_KEY has MORE than 2 values:** 3926=RH, 5320=SJH, **plus 5120
(cardiology/echo, RH), 5521 (Emergency Dept devices, RH)**. So sub-departments have their own
org keys that must ROLL UP to a site. My current `sites.ris_org_struct` (single value/site) can't
express this. → the org→site mapping must be many-org-to-one-site (a `site_org_map`, or an
ORG_STRUCTURE hierarchy table). Same likely applies to org keys on ORDERS/WORKLIST. (Qm2 — the
big one.)

**Notes:** demo/external rows present (USDemoPhilips "Demo", RH-PACS "External Upload",
SJH-CARM 2) — trailing flag may mark demo/deleted; exclude from device stats. Several AE_TITLEs
match the legacy SJH inventory (OEC9900GSP, SYMBIA, AMX1/2, LOGIQP9…).

**MERGE INSTRUCTION (vendor):** merge MODALITY + MODALITY_TYPE into ONE target `std_devices`
(the "modality mapping") — resolve MODALITY.MODALITY_TYPE_KEY → MODALITY_TYPE.CODE at ETL time
so std_devices carries the modality string (CT/MR/US…) directly, not the key.

**RESOLVED (MODALITY):**
- Qm3 ✓ — `AE_TITLE` = PACS `didb_studies.storing_ae` (the per-device RIS↔PACS join).
- Qm4 ✓ — MODALITY_TYPE lookup provided (cols: modality_type_key, CODE, Description); DICOM codes:
  21500=CR, 21501=CT, 21502=DR, 21503=DX, 21504=MG, 21505=MR, 21506=NM, 21507=OT, 21508=PR,
  21509=PT, 21510=RT, 21511=SC, 21512=US, 21513=IO, 22=RF, 21540=XA, 21740=XT(external upload).
- Qm1/Qm2 (extra columns, ORG_STRUCTURE hierarchy/rollup) — DEFERRED to the full RIS schema per
  vendor; site sub-department rollup (5120/5521→RH) noted, will resolve from the schema.

## MODALITY_TYPE  →  merged into `std_devices` (lookup only, resolve at ETL)
DICOM modality-code lookup (see values above). Not a standalone target.

## MODALITY_SCHEDULE  →  resolves `procedure_duration_map.modality`  (fill-only)
**Role:** scheduling bridge table — one row per (SCHEDULE_TEMPLATE_KEY, MODALITY_KEY) pairing
for a given SPS_CODE_KEY. This is the direct procedure↔modality link neither MODALITY nor
SPS_CODE carries on its own, and the RIS-sourced equivalent of the PACS-history auto-learn
(Phase 8 Strategies A–E) that's disabled at LAUMC (`RAYD_ETL_LOOKUP_FROM_PACS=false`).
**Not incremental — full reload each pass** (same as MODALITY).

**Named columns:** MODALITY_SCHEDULE_KEY (PK), SCHEDULE_TEMPLATE_KEY, MODALITY_KEY, PRIORITY
(always `1` in every sample row seen — no documented meaning beyond that, not used),
SPS_CODE_KEY, MODALITY_SCHEDULE_GROUP_KEY, LAST_UPDATED.

**Import (implemented, `ETL_JOBS/etl_ris_modality_schedule.py`, Phase 10 — runs after the
MODALITY and SPS_CODE steps, joining through their `ris_modality_key`/`ris_sps_code_key`
back-references):** stage `(SPS_CODE_KEY, MODALITY_KEY)` pairs, resolve MODALITY_KEY → modality
string via `aetitle_modality_map.ris_modality_key`, then per SPS_CODE_KEY take the majority
modality across all its device pairings (`MODE() WITHIN GROUP`, same idiom as Phase 8's
strategies) and `UPDATE procedure_duration_map.modality` **only where it's still NULL** —
never overwrites a manually-set or already-resolved value. A procedure schedulable on several
devices of the same modality type (e.g. two CT scanners) collapses to one modality; genuinely
mixed-modality procedures are not flagged for review yet (Phase 8's PACS-side
`procedure_modality_conflicts` table does this for the PACS strategies — could be extended here
if LAUMC needs the same visibility).

## ORG_STRUCTURE  →  drives `site_org_map` (org_structure_key → canonical site_id)
**Role:** the org hierarchy (self-referencing via PARENT_ORG_STRUCTURE_KEY). Resolves any
org_structure_key seen on ORDERS/WORKLIST/MODALITY to RH or SJH. Keep the hierarchy for display
paths (DISPLAY_PATH_NAME); derive a flat `site_org_map` for site resolution.

**Hierarchy (from data):**
```
1    LAUMC (root)
├─ 3926  RH  (LAUMCRH Radiology)      → site RH
│   └─ 5521  ER  (RH ER Department)   → site RH  (parent 3926)
├─ 5320  SJH (LAUMCSJH Radiology)     → site SJH
└─ 5120  VASC (Vascular LAB-RH)       → parent is ROOT, but described "RH" → assume RH (Qog1)
```
**Site resolution rule:** walk PARENT_ORG_STRUCTURE_KEY up until hitting 3926 (→RH) or 5320 (→SJH).
Exception: 5120 (VASC) parents to root, not RH — but its description + its devices (US CARDIO,
org 5120) are RH → map 5120→RH by business rule.

**IMPACT on migration 0046:** replace the single-value `sites.ris_org_struct` with a
`site_org_map` table (org_structure_key → site_id), seeded from this hierarchy. WORKLIST/ORDERS
site resolution then joins org_structure_key → site_org_map → site_id.

**Open (ORG_STRUCTURE) — the only one, and it's a business rule not internal detail:**
- Qog1 — Confirm **VASC (5120) counts as RH** for site stats (its devices are RH, but its parent
  is the LAUMC root, not RH). If it should be its own bucket instead, say so.

> **RETARGETED (2026-07):** SPS_CODE loads into RAYD's **existing `procedure_duration_map`**
> (extended by 0053 with active, body_part, ris_sps_code_key; procedure_name + modality already
> exist), NOT a separate `std_procedure_codes`. Same fill-only policy (`ON CONFLICT
> (procedure_code) DO NOTHING`; RAYD-owned RVUs preserved). Map: CODE→procedure_code,
> DESCRIPTION→procedure_name, DURATION→duration_minutes, ACTIVE→active,
> BODY_PART_KEY→body_part (resolved), SPS_CODE_KEY→ris_sps_code_key.

## SPS_CODE (Procedures)  →  loads into existing `procedure_duration_map`  (was std_procedure_codes)
**Role:** the procedure/exam catalog. `SPS_CODE_KEY` joins to `SITE_WORKLIST.SPS_CODE_KEY` (and
`ORDERS`). Populates RAYD's procedure catalog + the duration map (was "empty by design, filled
by RIS import"). ~coded families visible: J17=CT, J43=MRI, 93xxx/76645=US/biopsy (CODING_SCHEME).

**Column mapping (real → RAYD target):**
- `SPS_CODE_KEY` (BIGINT PK) → proc key (join to worklist/orders)
- `CODE` (TEXT, e.g. J17G-01) → proc_id/code
- `DESCRIPTION` (TEXT) → proc_text
- `DURATION` (INT minutes: 15/30/60) → default_duration → **feeds capacity/duration stats**
  (scheduled duration; measured actuals come later from status timestamps)
- `MINIMUM_STUDY_DURATION` (INT) → min duration
- `ACTIVE` (Y/N) → is_active
- `BODY_PART_KEY`, `LATERALITY_KEY`, `CODING_SCHEME_KEY`, `DOCUMENT_TOGETHER_GROUP_KEY` → lookups
  (separate tables likely coming; DOCUMENT_TOGETHER = procedures reported together)
- `CONTRA_INDICATION_WARNING_TEXT` (TEXT), `LAST_UPDATED` (watermark)

**Build note:** align registry `std_procedure_codes` columns to these real ones (SPS_CODE_KEY pk,
CODE, DESCRIPTION, DURATION, ACTIVE, body-part/laterality/coding-scheme keys).

## VISIT  →  target `std_visits`  (REAL schema — supersedes earlier provisional inference)
**Role:** patient visits/encounters. **PK: `VISIT_KEY`** (joins SITE_WORKLIST.VISIT_KEY,
ORDERS.VISIT_KEY). `VISIT_NUMBER` = **HL7 PV1.19** (links live ADT/ORM messages to this table).
Watermark: `CREATED_ON_DATE`. Feeds case-mix (IP/OP/ER), payer mix, length-of-stay, hospital service.

**Column mapping:**
- Keys: `VISIT_KEY` (PK), PATIENT_PERSON_KEY, CREATED_BY_PERSON_KEY
- Identifiers: `VISIT_NUMBER` (=PV1.19), PREADMIT_NUMBER, ALTERNATE_VISIT_ID, PATIENT_ACCOUNT_NUMBER,
  ISSUER_OF_VISIT_NUMBER, ISSUER_OF_PREADMIT_NUMBER
- **Class/financial:** `PATIENT_CLASS_KEY` (→ IP/OP/ER lookup — feeds KPI IN/Urg/Out dimension),
  `FINANCIAL_CLASS_KEY` (→ payer/TPA lookup), VISIT_PRIORITY_KEY, MOBILITY_STATUS_KEY
- **Encounter times (LOS):** ADMIT_DATE_TIME, DISCHARGE_DATE_TIME, EXPECTED_ADMIT_DATE_TIME,
  EXPECTED_DISCHARGE_DATE_TIME
- **Service:** HOSPITAL_SERVICE_KEY (ward/service lookup), VISIT_DESCRIPTION, VISIT_INDICATOR
- **Housekeeping:** IS_MASTER (primary visit flag), `DELETED` + DELETED_DATE (→ exclude deleted rows),
  CREATED_ON_DATE
- Lookups likely coming: PATIENT_CLASS, FINANCIAL_CLASS, HOSPITAL_SERVICE, MOBILITY_STATUS.

**Build note:** replace the provisional `std_visits` in the registry with these real columns.

---

## Patient details — RIS vs PACS (decision)
Question (vendor): pull patient details from RIS or keep from PACS? Same data, RIS may add phone.

**Recommendation: pull the RIS PATIENT/PERSON table AND keep PACS as-is.** Reason: every RIS
table keys patients on `PATIENT_PERSON_KEY`, but the PACS `etl_patient_view` uses the PACS key
(`patient_db_uid`) — a *different* key. So RIS-sourced features (worklist, reports, KPI, visits)
**need the RIS patient table to resolve PATIENT_PERSON_KEY → identity/name**; PACS demographics
can't do that. Plan:
- Pull RIS PATIENT/PERSON → new `std_patients_ris` (resolves PATIENT_PERSON_KEY; carries the
  extra phone/contact). Reconcile to PACS patients by MRN/patient_id.
- Keep PACS `etl_patient_view` for what's already built (no rework).
- The extra **phone** is a bonus (minimal value now that the patient portal is removed, but useful
  if patient-facing notifications ever return; also handy for CRN if patient contact is wanted).

---

## TABLES STILL NEEDED (for the vendor)

**Must-have (block the build):**
1. **PERSON / RESOURCE (staff/providers)** — ⭐ highest priority. Everything references
   `*_RESOURCE_ID_KEY` / `*_PERSON_KEY`: radiologists (REPORTED_BY, VERIFIED1/2/3_BY, APPROVED_BY,
   WET_READ_BY…), technician, requesting/referring. Needed to: show radiologist **full names** in
   the KPI, resolve LDAP identities, and — critically — get **referring physician CONTACT
   (email/phone) for the CRN** multi-channel notify. (May be two tables: PERSON + RESOURCE/staff.)
2. **PATIENT / PERSON** — resolve `PATIENT_PERSON_KEY` → patient identity (+ the extra phone).
   (See decision above.)

**Lookups (small, resolve keys → labels):**
3. PATIENT_CLASS (VISIT.PATIENT_CLASS_KEY → IP/OP/ER — feeds KPI IN/Urg/Out)
4. FINANCIAL_CLASS (VISIT.FINANCIAL_CLASS_KEY → payer/TPA)
5. HOSPITAL_SERVICE (VISIT.HOSPITAL_SERVICE_KEY → ward/service)
6. PRIORITY (ORDER_PRIORITY / PRIORITY_KEY / SCHEDULE_PRIORITY_KEY)
7. BODY_PART (SPS_CODE.BODY_PART_KEY), LATERALITY (LATERALITY_KEY), CODING_SCHEME (CODING_SCHEME_KEY)
8. ORDERING_ORGANIZATION (ORDERING_ORGANIZATION_KEY → referring clinic/org)
9. INTERPRETATION_TYPE (REPORT.INTERPRETATION_TYPE_KEY), VERSION_STATUS (REPORT.VERSION_STATUS_KEY)
10. JUSTIFICATION_STATUS + STATUS_REASON (ORDERS), MOBILITY_STATUS (VISIT.MOBILITY_STATUS_KEY)

**Already provided (no need):** STATUS_KEY lookup (→ worklist_status_map), MODALITY_TYPE.
**Parked:** BIRADS lookup (BIRADS_BIRADS_KEY) — only if BI-RADS QA is ever built.

## STILL-OPEN CROSS-SYSTEM ITEMS (needed before build)
- **The RIS↔PACS study join**: SPS_ID = didb_studies.ACCESSION_NUMBER, OR LINKED_ID = <PACS col>?
  (and how linked multi-SPS→one-study is handled). THE gate for the exam view.
- **Qog1**: VASC (org 5120) counts as RH for site stats? (assumed yes)

---
**Process note (vendor 2026-07-07):** hold internal-RIS questions for the full RIS schema;
only raise **RIS↔PACS mapping** questions (and genuine site-attribution business rules) going forward.
