# LAUMC — Data Pull List

Everything needed to pre-build the LAUMC deployment, with ready-to-run Oracle SQL.
Export results as CSV unless noted. Items marked **[BLOCKER]** gate the migrations/seed data.

Site key: `SITE_ID 0` / `SAP_PROD` = **LAUMC-RH** (main) · `SITE_ID 1` / `SAP_SJH` = **LAUMC-SJH** (satellite).

---

## A. Legacy SJH PACS — AE inventory & pollution investigation

Run against the **old SJH PACS** Oracle DB (pre-migration). Goal: know every AE's modality,
which AEs are still alive, and explain every polluted AE before device migration to the main PACS.

### A1. [BLOCKER] Full AE × modality inventory (the migration map)

```sql
SELECT original_storing_ae,
       study_modality,
       COUNT(*)              AS studies,
       MIN(study_date)       AS first_seen,
       MAX(study_date)       AS last_seen
FROM   medistore.didb_studies
GROUP  BY original_storing_ae, study_modality
ORDER  BY original_storing_ae, studies DESC;
```

`last_seen` tells you which AEs are dead — don't migrate those.

### A2. NonDicomAgent investigation

> **RESOLVED 2026-07-04**: only **141 studies** (~0.02% vs 614k) — negligible, excluded from
> further investigation. Strong contrast with CHN (~24%): AE identity effectively survives at
> LAUMC, so per-device analytics is viable here. Remaining suspect AEs (A3): investigation
> planned Monday 2026-07-06. Queries kept below for reference.

**A2a — how big, and is it still growing?**

```sql
SELECT TO_CHAR(insert_time, 'YYYY-MM') AS month, COUNT(*) AS studies
FROM   medistore.didb_studies
WHERE  original_storing_ae = 'NonDicomAgent' OR storing_ae = 'NonDicomAgent'
GROUP  BY TO_CHAR(insert_time, 'YYYY-MM')
ORDER  BY month;
```

If the monthly count stopped at some point → a job/config that ran historically.
If it's still growing → active process, must be identified before migration.

**A2b — what did the overwrite preserve?** (`storing_ae` vs `original_storing_ae` cross-tab —
if one column survived, we can recover device identity)

```sql
SELECT storing_ae, original_storing_ae, COUNT(*) AS studies
FROM   medistore.didb_studies
WHERE  original_storing_ae = 'NonDicomAgent' OR storing_ae = 'NonDicomAgent'
GROUP  BY storing_ae, original_storing_ae
ORDER  BY studies DESC;
```

**A2c — is it an import/prefetch agent?** (correlation with fetch activity)

```sql
SELECT NVL(fetch_reason, '<none>')                              AS fetch_reason,
       CASE WHEN prefetch_origin_study IS NULL THEN 'N' ELSE 'Y' END AS is_prefetch,
       COUNT(*)                                                 AS studies
FROM   medistore.didb_studies
WHERE  original_storing_ae = 'NonDicomAgent'
GROUP  BY NVL(fetch_reason, '<none>'),
          CASE WHEN prefetch_origin_study IS NULL THEN 'N' ELSE 'Y' END;
```

**A2d — 20 recent samples for manual tracing**

```sql
SELECT * FROM (
    SELECT study_db_uid, accession_number, study_date, insert_time,
           study_modality, study_description, storing_ae, original_storing_ae,
           fetch_reason, prefetch_origin_study
    FROM   medistore.didb_studies
    WHERE  original_storing_ae = 'NonDicomAgent'
    ORDER  BY insert_time DESC
) WHERE ROWNUM <= 20;
```

### A3. All other suspect AEs — one query

Same structure for the full suspect list (infrastructure nodes, workstations, test AEs):

```sql
SELECT original_storing_ae,
       study_modality,
       COUNT(*)                        AS studies,
       MIN(study_date)                 AS first_seen,
       MAX(study_date)                 AS last_seen,
       COUNT(prefetch_origin_study)    AS prefetched,
       COUNT(fetch_reason)             AS fetched
FROM   medistore.didb_studies
WHERE  original_storing_ae IN (
    'StorageSCU', 'IIP_STORE_SCU', 'VDICOM', 'VDICOM_STR_SCU', 'XVimport',
    'Tool', 'Aetitle', 'laumc', 'LAUMC', 'aws', 'adw_8', 'SVSM', 'RAPID',
    'ECHOPAC-PC', 'kizuna', 'massiFIR', 'massi1FIR', 'sjhcsapwfmFIR',
    'laumcpacsFIR', 'laumcwfm1FIR', 'laumcwfm2FIR', 'laumcwfm1AR',
    'CM_CT_CMW_V1', 'LEO22529', 'StorageSCU'
)
GROUP  BY original_storing_ae, study_modality
ORDER  BY original_storing_ae, studies DESC;
```

Plus A2d-style samples for any AE whose result is surprising.

---

## B. Main PACS (both sites)

### B1. [BLOCKER] SITE_ID distribution — validates the site contract

```sql
SELECT site_id,
       TO_CHAR(study_date, 'YYYY-MM') AS month,
       study_modality,
       COUNT(*)                       AS studies
FROM   medistore.didb_studies
GROUP  BY site_id, TO_CHAR(study_date, 'YYYY-MM'), study_modality
ORDER  BY month, site_id;
```

Also confirms daily volumes per site for sizing. Any `site_id` NOT IN (0, 1) or NULL is a red flag.

### B2. [BLOCKER] Two-site DIDB_STUDIES extract

Same format as the existing `didb_studies.csv` export (all columns), but covering **both sites** —
4–6 recent weeks. This drives the seed data, mapping tests, and the mammo quantification.

### B3. Mammo bug — PACS-side view

```sql
SELECT site_id, original_storing_ae, COUNT(*) AS studies,
       MIN(study_date) AS first_seen, MAX(study_date) AS last_seen
FROM   medistore.didb_studies
WHERE  study_modality LIKE '%MG%'
GROUP  BY site_id, original_storing_ae
ORDER  BY site_id, studies DESC;
```

Exact quantification (which SJH mammo studies claim RH) comes from crossing this with the RIS
issuer per accession — done automatically once C1/C2 are in.

### B4. AE inventory on the main PACS (same SQL as A1)

Answers whether AE identity survives on the main PACS going forward — this decides
**per-device vs per-modality capacity tables** for LAUMC, the last open architecture fork.

### B5. [BLOCKER for backfill] Aggregated image/storage backfill extract

Replaces syncing 166.9M image rows. **Note the dedupe**: `didb_image_locations` holds duplicate
rows per image (one with file path, one NULL) — sum only the best row per image:

```sql
WITH best_loc AS (
    SELECT raw_image_db_uid, image_size,
           ROW_NUMBER() OVER (PARTITION BY raw_image_db_uid
                              ORDER BY image_size DESC NULLS LAST) AS rn
    FROM   medistore.didb_image_locations
)
SELECT ri.study_db_uid,
       COUNT(*)                 AS image_count,
       SUM(bl.image_size)       AS total_size_kb
FROM   medistore.didb_raw_images ri
JOIN   best_loc bl
       ON bl.raw_image_db_uid = ri.raw_image_db_uid AND bl.rn = 1
GROUP  BY ri.study_db_uid;
```

~614k output rows. Run off-hours; if it's too heavy in one pass, chunk by `study_db_uid` ranges.
(If `didb_raw_images` isn't visible, the alternate name in this PACS is `DIDB_RAW_IMAGES_TABLE`.)

---

## C. RIS (Oracle 12)

### C1. [BLOCKER] Orders table — DDL + sample

- Full DDL of the orders table (or `DESCRIBE` output).
- ~500 recent rows covering **both** issuer values, all columns.

### C2. [BLOCKER] Site discriminator validation

```sql
SELECT issuer_of_placer_order_number, COUNT(*) AS orders,
       MIN(<order_date_col>) AS first_seen, MAX(<order_date_col>) AS last_seen
FROM   <orders_table>
GROUP  BY issuer_of_placer_order_number;
```

Expect exactly `SAP_PROD` and `SAP_SJH` — any third value or NULLs need explaining.

### C3. `site_worklist` — DDL + ~200 recent rows (accession lives in `sps_id`; need to see the
join shape to orders and confirm 1:1).

### C4. Value lists (per issuer): distinct `order_status`, `order_control`, `patient_class`
with counts. (Parked from Q8/Q9 — fine to deliver with the HL7 pull.)

### C5. The "important RIS tables" list you promised — table names + row counts + one-line purpose.

### C6. Physician master — LDAP IDs, names, role (radiologist/referring), site affiliation.

---

## D. HL7 (from PACS)

- Sample **ORU** messages: a few per modality, at least one per site — to check whether anything
  in MSH/OBR/PV1 hints at site (MSH-4 sending facility especially).
- Sample ORM messages if any will flow to RAYD.
- Full `order_status` / `order_control` vocabularies (with C4).
- **ORC-2.2 issuer — RESOLVED 2026-07-05 (negative)**: real ORM samples show NO issuer in
  ORC-2/OBR-2 and identical MSH-3 (`SAP_P`) for both sites. `SAP_PROD`/`SAP_SJH` exists only in
  the RIS DB column. Wire-level discriminators found: **PV1-3.7 building code (`1000`=RH,
  `2000`=SJH)** and `-J` suffix on locations (`EM-J`, `ER-J` in PV1-3.1/ORC-17).
  **Preferred fix**: RIS stamps site explicitly in its outbound feed to RAYD (MSH-4 =
  SAP_PROD/SAP_SJH); PV1-3.7 parse as fallback. ER detection at LAUMC is location-based
  (`EM*`/`ER*` prefix; PV1-2 stays 'O' even for ER patients).
- **RESOLVED 2026-07-05 (vendor)**:
  (a) Site vocabularies are FINAL — one pair of sites, three vocabularies, all via `sites`
      table: PACS DB `0`/`1`, RIS DB `SAP_PROD`/`SAP_SJH`, HL7/MLLP `1000`/`2000` (PV1-3.7).
      Each ingestion path resolves ONLY its own vocabulary.
  (b) OBR-1 is an internal SAP tracking number. The real RIS↔PACS accession is minted at
      SCHEDULING time = `site_worklist.sps_id`. SAP ORMs therefore carry no accession —
      HL7 lifecycle events key on placer order number; the RIS DB is the join hub
      (placer order → sps_id → PACS study).
  (c) PV1-3.7 building code is very safe — the whole integration is built on it.
  (d) ER location-prefix mapping (`EM*`/`ER*`, `-J` = SJH) confirmed correct; primary ER
      classification will come from the RIS DB anyway.
- **From full MLLP log capture (2026-07-05)**:
  - RIS webservice payload carries `<SITE_ID>` (1000/2000) AND issuer (SAP_*) explicitly —
    RIS DB has both vocabularies; adapter pulls site directly. Confirmed end-to-end.
  - `PLACER_GROUP_NUMBER` (OBR-1) is a requisition GROUP: one group can span multiple placer
    orders (e.g. CT abdomen + CT pelvis). **TRAP — workflow-dependent**: vendor confirming how
    the hospital actually treats grouped orders (one sps_id per order vs per group, one
    acquired study vs two). Join logic will be built group-aware either way.
  - **Duplicate delivery is PERMANENT** (SAP Mirth server bug, no fix available): every message
    can arrive twice with identical MSH-10. Listener idempotency on MSH-10 is a mandatory
    ingest requirement, not defensive coding.
  - Full ADT feed exists on the SAP hub (A01/A02/A08, ward/room/bed, receivers: RIS,
    CareStream, PAXERABROKER). Vendor has no control over the sender → listener contract is
    **whitelist-and-discard**: parse ORM/ORU, ACK and silently ignore all other message types;
    never NAK/error/queue on unwanted types. Bed-level ADT noted as optional future
    floor-map fuel only.
  - ER quick-registrations use placeholder DOB `9999-11-11` + gender `U` → age_at_exam must
    NULL-out future/placeholder DOBs, AND these records must be **surfaced to the sys admin**
    (data-quality panel: quick-reg records pending completion), not just silently NULLed.
  - ORC-5 codes observed so far: `E0001` (new, CONFIME_STATUS=N), `E0003` (+ORC-6=Y,
    CONFIME_STATUS=Y — likely confirmed/released). Full vocabulary still pending.
- **From real ORU sample (2026-07-05, Carestream)**:
  - Accession = **OBR-3 filler order number** (13-digit, e.g. 1005003377256) — the ONLY join
    key: ORU carries NO placer order number and NO procedure code.
  - Two signers with timestamps: principal reader (prelim) + approver (final) as LDAP email
    identities → prelim/final TAT per radiologist measurable.
  - Result status `FAP` (final/approved) in OBR-25/OBX-11 — need prelim + amended codes.
  - **Linked-study evidence live**: OBR-4 = "CT ABDOMEN" but report title = "CT ABDOMEN AND
    PELVIS" → IS_LINKED/LINK_ID case. OPEN: does each linked accession get its own ORU with
    duplicated text? ETL must pull IS_LINKED/LINK_ID; report-level stats count per LINK_ID
    group, procedure-level per accession.
  - **MSH-4 = `2`** — unknown vocabulary (not 0/1, SAP_*, or 1000/2000). OPEN: is Carestream
    MSH-4 site-stable per site (would give ORU direct site) or a fixed broker ID? Until
    confirmed, ORU site enrichment stays accession-lookup-first.
  - ZDC|0|RTF + ZDC|1|PDF carry the complete signed report (base64, hundreds of KB). OPEN
    decision: store PDF for patient portal / CD print, or skip. Listener MUST handle large
    messages and never parse ZDC as OBX.
  - Impression extraction = OBX lines after the `Impression:` marker; sections are labeled
    (Clinical information / Technique / Findings / Impression).
  - Radiation dose in narrative ("total exam DLP: NNN mGy-cm") — regex-extractable for a
    future per-device/site dose dashboard.
  - ORU is HL7 v2.3, ORM v2.3.1 — parser tolerates both.
- **Vendor rulings 2026-07-05**: ORU site = accession-lookup enrichment (confirmed, no site in
  ORU). **Patient portal COMPLETELY REMOVED from the LAUMC installation** — routes/blueprints
  physically absent, not license-disabled. ZDC/PDF/base64 ignored — plain text only for NLP.
- **Radiation Dose Management System (RDMS) — approved for analysis, "gold" feature**:
  Phase 1 = regex extraction of dictated dose (DLP mGy-cm, DAP, AGD, NM activity) from ORU
  text via the existing NLP worker into a new `radiation_dose` table + dashboard with
  per-device/procedure/site splits and a dictation-coverage meter. LAUMC-unique: per-device
  dose profiles (AE survives), cross-site cumulative patient dose, RH-vs-SJH DRL benchmarking,
  pediatric panel. Phase 2 depends on vendor: does Carestream DB expose RDSR-derived dose
  values in queryable tables? Phase 3 = DRL reference table + outlier alerts (reuse critical-
  findings pattern) + effective-dose estimates (DLP × k-factor) + cumulative view. Dose must
  attach per LINK_ID group (not per accession) or linked studies double-count.
  Vendor questions: (a) Carestream dose tables? (b) dose dictation mandated by policy —
  expected coverage? (c) technologist identity per exam in RIS?
- **Still pending**: full ORC-5 status vocabulary + which RIS outbound stream carries
  ARRIVED/STARTED/DONE events for RAYD; ORU prelim/amended status codes; MSH-4 semantics;
  ORU-per-accession-vs-per-link; integration document; RIS DB schema (drives adapter mapping +
  ER classification + accession join + IS_LINKED/LINK_ID).
- Listener must dedupe ORC-1 NW vs RQ (TPA clearance resubmission) on placer order number.
- **Status-transition events — RESOLVED 2026-07-04**: RIS emits SCHEDULED datetime, ARRIVED,
  STARTED, and EXAM DONE, plus ORU from PACS. Full measured state machine — live floor map
  (exact waiting counts + room busy state), true wait time (ARRIVED→STARTED), and measured
  exam durations (STARTED→DONE, can calibrate procedure_duration_map with actuals) are all
  viable. Still needed from samples: the exact ORM status field values/codes for each
  transition, to configure the listener field map.

---

## E. Ops facts

| # | Item | Why |
|---|------|-----|
| E1 | Simultaneous `SELECT SYSTIMESTAMP FROM dual` on RIS **and** PACS DBs | Clock skew silently corrupts turnaround stats |
| E2 | Target go-live date + how far back stats must go | Sets `go_live_config` and backfill window |
| E3 | Read-only DB accounts + network path for both DBs from the RAYD host | The usual deployment bottleneck |
| E4 | Legacy SJH PACS: connection details + schema owner (is it also `medistore`?) | Needed for section A |

---

## Delivery priority

1. **A1 + A2** — unblocks the SJH migration planning (your immediate need)
2. **B1 + B2 + C1 + C2** — unblocks RAYD migrations + seed data
3. **B4** — decides per-device vs per-modality architecture
4. **B5** — backfill extract (can run closer to go-live for freshness)
5. **C3–C6, D, E** — config + validation layer
