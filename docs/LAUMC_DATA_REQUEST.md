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
- **Confirm ORC-2.2 / OBR-2.2 content**: does the issuer of placer order number (`SAP_PROD` /
  `SAP_SJH`) appear in ORM messages? If yes, HL7 orders get site directly from the message.
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
