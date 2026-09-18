# Report 23 — "RIS and PACS study counts differ": what to expect, and how to prove it

Same format as the Report 27 diagnostic. Edit the dates at the top of each box,
run, compare. Each box runs on its own.

---

## 1. Start here: Report 23 contains no RIS data at all

Report 23's base query lives in the DB (`report_template.report_sql_query`,
`report_id = 23`), and **migration 0001 already removed the `etl_orders` join**.
The effective query after [report_23.py](../routes/report_23.py)'s runtime
patches is:

```sql
FROM etl_didb_studies s                                  -- PACS
LEFT JOIN etl_patient_view p   ON s.patient_db_uid::text = p.patient_db_uid::text
LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR','OT')
  AND m.site_id = :rh_site_id
  AND <five NOT LIKE '%SJH%' guards>
```

So the count is **PACS studies only**. A RIS number will never equal it, and
three of the four reasons below are expected rather than broken. But the site
filter is a real bug that silently deletes studies, so the number is not a
correct PACS count either.

> Note: two of the `.replace()` patches in `get_report_config()` are dead — they
> target `LEFT JOIN etl_orders o ON o.patient_dbid = p.patient_db_uid` and
> `s.patient_db_uid = p.patient_db_uid`, neither of which exists in the stored
> SQL any more (migration 0001 rewrote it). They are harmless no-ops today, but
> they will not protect anything if the template row is ever edited back.

---

## 2. The four reasons the numbers differ

### S1 — the site filter deletes studies on unmapped devices  ← **the bug**

```sql
AND m.site_id = :rh_site_id
```

`m` is a **LEFT** join. Any study whose `storing_ae` has no row in
`aetitle_modality_map` gets `m.site_id = NULL`, `NULL = 1` is NULL, and the row
is dropped. This turns the LEFT JOIN into an INNER JOIN and silently removes
real RH studies — a brand-new device, a renamed AE, anything not in the 39-row
seed from migration 0083.

It also makes the modality fallback directly above it unreachable: a row with no
`m` match fails the site filter before `COALESCE(m.modality, s.study_modality)`
could ever help. [report_23.py:82-96](../routes/report_23.py#L82-L96) already
flags this as suspected; Query 1 below measures it.

**The study's own site marker is already loaded and would be exact.**
`etl_didb_studies.pacs_site_id_raw` is populated by the studies ETL from the PACS
`SITE_ID` column (`'0'` = RH, `'1'` = SJH — see
[utils/site_resolver.py:9-12](../utils/site_resolver.py#L9-L12)). Filtering on
that instead needs no join and cannot drop unmapped devices.

> Do **not** switch to `etl_didb_studies.site_id` — migration 0049 added the
> column and migration 0051 documents an enrichment pass in
> `ETL_JOBS/etl_site_enrichment.py`, **but that file does not exist**. The column
> is NULL on every row. `pacs_site_id_raw` is the one that actually carries data.

### S2 — the device map masks the real modality, defeating the SR filter

```sql
COALESCE(m.modality, s.study_modality, '') NOT IN ('SR','OT')
```

`m.modality` is checked **first**. Migration 0083 maps 11 RH gateway / import /
workflow-manager AEs to the pseudo-modality `'PACS'`:

```
AETITLE, IIP_STORE_SCU, LAUMCPACSFIR, LAUMCWFM1AR, LAUMCWFM1FIR, LAUMCWFM2FIR,
MASSI1FIR, NONDICOMAGENT, VDICOM, VDICOM_STR_SCU, XVIMPORT
```

`s.study_modality` is derived from the series (ETL Phase 2b,
[etl_runner.py:348-373](../ETL_JOBS/etl_runner.py#L348-L373)) and is the value
that actually knows a study is SR. But `m.modality = 'PACS'` overrides it, so an
SR or PR study arriving through a gateway AE is never excluded. `etl_runner.py`
states these AEs carry "RF/CT/**SR**/MR/DX/MG/XA/US/**PR**/etc."

Report 23 also never excludes `'PACS'` itself, while report_25 does
(`NOT IN ('SR','PACS')`). So report 23 counts gateway traffic that report 25
does not — the two reports cannot agree by construction.

### S3 — the `'OT'` exclusion drops real exams

`GELUNAR11` is seeded as modality `'OT'` — bone densitometry / DEXA, a real
billable exam. `NOT IN ('SR','OT')` removes every one of its studies. Whether
that is wanted is an operator call, but right now DEXA is invisible in Report 23
while gateway SR traffic is counted (S2). That is backwards.

### S4 — RIS and PACS count different things (expected, not a bug)

Even with S1–S3 fixed the two systems will not match:

| | RIS (`SITE_WORKLIST`) | PACS (`didb_studies`) |
|---|---|---|
| Grain | 1 row per **scheduled procedure step (SPS)** | 1 row per **study** |
| Linked exams | CT abdomen + pelvis = **2 rows** | **1 study** |
| Never imaged | cancelled / DNA / still scheduled all present | absent |
| No order | absent | walk-ins, imported CDs, archive present |
| Date anchor | `SCHEDULED_DATE` | `STUDY_DATE` |

From the Report 27 run on the same window, the RIS carried 27,299 `scheduled`
and 4,877 `cancelled` rows — **~32k RIS rows that by definition have no PACS
study.** The date anchor differs too: an exam booked 31 Dec and performed 2 Jan
lands in different months on each side.

---

## 3. QUERY 1 — PACS Oracle: where the studies go

> Run on the **PACS** Oracle. Edit the two dates. One row out, read left to right.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_start,
           DATE '2026-09-18' AS d_end
      FROM dual
),
base AS (
    SELECT
        UPPER(TRIM(s.STORING_AE))      AS ae,
        TO_CHAR(s.SITE_ID)             AS pacs_site,
        s.REP_FINAL_SIGNED_BY          AS sign_f,
        s.REP_PRELIM_SIGNED_BY         AS sign_p,
        s.REP_STUDY_LAST_COMPOSED_BY   AS sign_c
    FROM medistore.didb_studies s
    CROSS JOIN params p
    WHERE s.STUDY_DATE >= p.d_start
      AND s.STUDY_DATE <  p.d_end + 1
),
flagged AS (
    SELECT b.*,
        -- AEs the studies ETL never loads (etl_didb_studies.py _EXCLUDED_AE_SQL)
        CASE WHEN b.ae NOT IN ('LAUMC','SVSM','ECHOPAC-PC','ADW_8','AETITLE',
                               'VIVIDE9-003168','VIVID_S5-050514','TERRA',
                               'VIVIDS70-003049','TERRA2','AWVASC','AWCTHD1',
                               'PHCARDIO','LOGIQV2-01')
             THEN 1 ELSE 0 END AS keep_ae,
        -- '@dn' signer exclusion, all three signer columns
        CASE WHEN (b.sign_f IS NULL OR UPPER(b.sign_f) NOT LIKE '%@DN')
              AND (b.sign_p IS NULL OR UPPER(b.sign_p) NOT LIKE '%@DN')
              AND (b.sign_c IS NULL OR UPPER(b.sign_c) NOT LIKE '%@DN')
             THEN 1 ELSE 0 END AS keep_signer,
        -- RH only ('0' = RH, '1' = SJH)
        CASE WHEN b.pacs_site = '0' THEN 1 ELSE 0 END AS is_rh,
        -- present in aetitle_modality_map (migration 0083 seed) -> survives "m.site_id = :rh"
        CASE WHEN b.ae IN ('MR750W','MR450W','AWMR450W','AWMR750W','ECHOPAC-PC','PHCARDIO',
                           'LOGIQ7-1','LOGIQ7-2','LOGIQV-000000','AWPETCT','AE_DBT','SENO2',
                           'SENOIRIS','AWSENO1','GELUNAR11','BAY85CT','AWS','AWCTHD1','RAPID',
                           'TERRA1','TERRA2','ARTIS121403','LEO22529','ADW_8','AWVASC',
                           'SYMBIANET','LAUOCT','LAUMCWFM1FIR','LAUMCWFM2FIR','LAUMCWFM1AR',
                           'LAUMCPACSFIR','IIP_STORE_SCU','VDICOM','VDICOM_STR_SCU','XVIMPORT',
                           'MASSI1FIR','AETITLE','NONDICOMAGENT','SJHCSAPWFMFIR')
             THEN 1 ELSE 0 END AS in_ae_map
    FROM base b
)
SELECT
    COUNT(*)                                              AS s0_all_pacs_studies,
    SUM(keep_ae)                                          AS s1_after_ae_exclusions,
    SUM(keep_ae * keep_signer)                            AS s2_after_dn_signers,
    SUM(keep_ae * keep_signer * is_rh)                    AS s3_rh_only,
    SUM(keep_ae * keep_signer * is_rh * in_ae_map)        AS s4_what_report_23_shows,
    SUM(keep_ae * keep_signer * is_rh * (1 - in_ae_map))  AS lost_to_unmapped_ae
FROM flagged;
```

### How to read it

| Column | Meaning |
|---|---|
| `s0_all_pacs_studies` | the number the customer is probably quoting |
| `s1`, `s2` | removed on purpose by the ETL (cardiology archive, junk signers) |
| `s3_rh_only` | SJH removed — the true RH study count |
| `s4_what_report_23_shows` | roughly what the screen shows, before SR/OT |
| **`lost_to_unmapped_ae`** | **S1 damage — real RH studies deleted by the site filter** |

`s3_rh_only` minus `s4` is the bug. If `lost_to_unmapped_ae` is non-zero, those
studies exist in PACS, passed every intentional filter, and still do not appear
in Report 23.

---

## 4. QUERY 2 — PACS Oracle: which devices are being dropped

> Run on the **PACS** Oracle, same dates. Names the AEs behind `lost_to_unmapped_ae`.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_start,
           DATE '2026-09-18' AS d_end
      FROM dual
)
SELECT
    UPPER(TRIM(s.STORING_AE))  AS storing_ae,
    TO_CHAR(s.SITE_ID)         AS pacs_site,
    COUNT(*)                   AS studies,
    CASE WHEN UPPER(TRIM(s.STORING_AE)) IN (
            'MR750W','MR450W','AWMR450W','AWMR750W','ECHOPAC-PC','PHCARDIO',
            'LOGIQ7-1','LOGIQ7-2','LOGIQV-000000','AWPETCT','AE_DBT','SENO2',
            'SENOIRIS','AWSENO1','GELUNAR11','BAY85CT','AWS','AWCTHD1','RAPID',
            'TERRA1','TERRA2','ARTIS121403','LEO22529','ADW_8','AWVASC',
            'SYMBIANET','LAUOCT','LAUMCWFM1FIR','LAUMCWFM2FIR','LAUMCWFM1AR',
            'LAUMCPACSFIR','IIP_STORE_SCU','VDICOM','VDICOM_STR_SCU','XVIMPORT',
            'MASSI1FIR','AETITLE','NONDICOMAGENT','SJHCSAPWFMFIR')
         THEN 'mapped'
         ELSE '*** NOT IN aetitle_modality_map - DROPPED ***'
    END AS report_23_status
FROM medistore.didb_studies s
CROSS JOIN params p
WHERE s.STUDY_DATE >= p.d_start
  AND s.STUDY_DATE <  p.d_end + 1
GROUP BY UPPER(TRIM(s.STORING_AE)), TO_CHAR(s.SITE_ID)
ORDER BY 3 DESC;
```

Every `*** NOT IN aetitle_modality_map ***` row with `pacs_site = '0'` is a real
RH device whose studies Report 23 is throwing away. Each one needs an
`aetitle_modality_map` row — and S1 fixed so the next new device does not repeat
this silently.

---

## 5. QUERY 3 — RIS Oracle: why the RIS number is structurally larger

> Run on the **RIS** Oracle, same window. Status key lists are from migration 0047.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_start,
           DATE '2026-09-18' AS d_end,
           DATE '2025-01-01' AS go_live
      FROM dual
)
SELECT
    COUNT(*)                                   AS sps_rows_what_ris_reports,
    COUNT(DISTINCT w.SPS_ID)                   AS distinct_accessions,
    COUNT(DISTINCT NVL(TO_CHAR(w.LINKED_ID),
                       'X' || TO_CHAR(w.SITE_WORKLIST_KEY)))
                                               AS pacs_equivalent_studies,
    COUNT(CASE WHEN w.STATUS_KEY IN (100,1724,1726,1802,1828,2022,2222,
                                     110,120,2025,130,140,150,160,1725,2024)
               THEN 1 END)                     AS reached_exam_done,
    COUNT(CASE WHEN w.STATUS_KEY IN (20,30,50,90,1300,1702,1742,1830,2422)
               THEN 1 END)                     AS cancelled_never_imaged,
    COUNT(CASE WHEN w.STATUS_KEY IN (5,10,40,1723,1825,1827,1829,
                                     60,1762,1823,1826,1831,
                                     70,1120,1822,1824,2223)
               THEN 1 END)                     AS still_in_pipeline
FROM SITE_WORKLIST w
JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
CROSS JOIN params p
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER = 'SAP_PROD'      -- RH only; 'SAP_SJH' is the satellite
  AND o.CREATED_ON_DATE  >= p.go_live
  AND w.SCHEDULED_DATE   >= p.d_start
  AND w.SCHEDULED_DATE   <  p.d_end + 1;
```

### The reconciliation

```
  sps_rows_what_ris_reports          <- the RIS number the customer quotes
- cancelled_never_imaged             <- exists in RIS, never reaches PACS
- still_in_pipeline                  <- booked/arrived, not imaged yet
= reached_exam_done                  <- the only rows that CAN have a PACS study
  then collapse linked SPS siblings  -> pacs_equivalent_studies
```

`sps_rows` minus `pacs_equivalent_studies` is the linked-exam effect: a CT
abdomen + pelvis booked together is 2 RIS rows and 1 PACS study. Compare
`reached_exam_done` (after the linked collapse) against Query 1's `s3_rh_only`.
Residual difference is walk-ins and imported studies that never had a RIS order.

Remember the anchors differ — RIS on `SCHEDULED_DATE`, PACS on `STUDY_DATE`. For
a tight comparison use a wide window, or anchor both on the same month.

---

## 6. Fixes

| | Fix | Risk |
|---|---|---|
| **S1** | Filter on `s.pacs_site_id_raw = '0'` instead of `m.site_id = :rh_site_id`. No join needed, cannot drop unmapped devices. Keep the five `NOT LIKE '%SJH%'` guards as defence in depth. | Low. Needs confirming `pacs_site_id_raw` is populated in production — **run Query 1 first**; if `pacs_site = '0'` and `'1'` both return sensible counts, it is good. |
| **S2** | Test SR/PR against `s.study_modality` directly rather than through the `COALESCE`, and add `'PACS'` to the excluded set so gateway traffic stops being counted as studies. | Medium — will **lower** the reported number. Confirm with the operator first. |
| **S3** | Decide whether DEXA (`GELUNAR11`, modality `'OT'`) is a real exam for this report. If yes, drop `'OT'` from the exclusion, or remap that AE. | Operator decision. |
| **S4** | Not a bug. Give the customer the Query 3 reconciliation. | — |
| dead patches | Remove the two no-op `.replace()` calls, or convert them to an assertion so silent drift is caught. | Low. |

**Order:** S1 first — it is the only one that deletes real data, and it is the
only one where the report is wrong rather than merely different from the RIS.
