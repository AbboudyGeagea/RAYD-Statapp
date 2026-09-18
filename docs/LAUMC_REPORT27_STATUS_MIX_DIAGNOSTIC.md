# Report 27 — Order Status Mix: what to expect, and how to prove it

Three self-contained queries. Edit the dates at the top of each box, run, compare
to the screen. No assembly needed — each box runs on its own.

---

## 0. RESOLVED — measured on LAUMC, 2026-09-18

Range 2026-01-01 .. 2026-09-18, live RIS Oracle vs the rendered chart:

| Bar | Screen | Truth | Inflation |
|---|---:|---:|---:|
| `CM` | 90,108 | 82,071 | **+8,037** |
| `scheduled` | 27,160 | 27,147 | +13 |
| `CA` | 4,854 | 4,848 | +6 |
| `arrived` | 129 | 123 | +6 |
| `in_progress` | 70 | 70 | 0 |
| **Total** | **122,321** | **114,259** | **+8,062** |

**D1 confirmed** — 99.7% of the inflation lands in `CM`, exactly as the fan-out
predicts (it can only duplicate orders that resolved to a PACS study). `CM` was
reading 9.8% high. **D2 confirmed** — 539 orders lost to the midnight bound.
**D3 was not firing** — no `*** NULL ***` row; every status key in this window is
mapped. Latent, fixed anyway.

Fixed in `fix(LAUMC): report 27 order status mix`:

| | Fix |
|---|---|
| D1 | `LEFT JOIN LATERAL … LIMIT 1`, PACS code preferred, RIS proc_id as fallback |
| D2 | `>= :start AND < (:end::date + 1)`, also applied to the multi-order query |
| D3 | unmapped statuses land in a visible `Unknown` bar instead of being dropped |
| D4 | bars relabelled to lifecycle names; RIS status breakdown in the tooltip (migration 0112) |

**After the fix the chart should read** CM→Completed/Reported 82,413 ·
Scheduled 27,299 · Cancelled 4,877 · Arrived 133 · In Progress 76 ·
**total 114,798** — i.e. the `should_be` column of Query 1. Requires the
migration 0112 backfill (`RAYD_ETL_ORDERS_FULL_REBUILD=1`) before tooltips show
real RIS status names instead of "Unknown".

---

## 1. What Report 27 SHOULD show

The bars are **not** RIS status names. `etl_orders.order_status` is a translation
([etl_orders.py:160-171](../ETL_JOBS/etl_orders.py#L160-L171)) that squeezes 42 RIS
status codes into **at most 6 labels**:

| Bar on screen | Means | RIS statuses merged into it |
|---|---|---|
| `CM` | exam done or later | Exam Done, Series, NRR, Technical Recall, To Report, Pending, Inj Done, Dictated, Prelim Typed, Wet Read, Signed 1, Signed 2, Signed 3, Approved, Ext.Rep., Reviewed |
| `CA` | cancelled / discontinued | Cancelled by RIS, Cancelled by OP, Cancelled, Rejected, Cancelled by PP, Cancelled by Patient, Not app, Cancelled Duplicate, Discontinued |
| `in_progress` | on the table | Started, Order In Progress, In Progress, Contrast, Pre Exam |
| `arrived` | patient present, waiting | Arrived, Preparation, Porter, Oral STR, General |
| `scheduled` | booked, not arrived | Scheduled, DNA, Call, Changed, Req n.a. |
| `requested` | order placed, not booked | Requested Unsigned, Requested Signed |

**So a correct chart looks like this** — six bars maximum, `CM` dominant:

```
CM          ████████████████████████████████  <- biggest by far
CA          ██████
scheduled   ████
arrived     ██
in_progress █
requested   █
            ^ these six must sum to the "Total Orders" figure
```

**Red flags, without running anything:**

- A label that is **not** one of those six → a RIS status was added after migration
  0047 was seeded, or the ETL is writing raw values.
- Bars that **don't sum to Total Orders** → orders are being dropped (defect D3 below).
- `CM` looking implausibly large relative to `CA`+`scheduled` → double-counting (D1).
- Customer says "these aren't our statuses" → that's the table above, not a bug in
  the numbers (D4).

---

## 2. QUERY 1 — RIS Oracle: the correct bar values

> Run on the **RIS** Oracle. Edit the three dates only.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_start,    -- Report 27 start_date
           DATE '2026-09-18' AS d_end,      -- Report 27 end_date
           DATE '2025-01-01' AS go_live     -- go_live_config.go_live_date
      FROM dual
),
mix AS (
    SELECT
        w.SCHEDULED_DATE,
        CASE
            WHEN w.STATUS_KEY IN (20,30,50,90,1300,1702,1742,1830,2422)
                 THEN 'CA'
            WHEN w.STATUS_KEY IN (100,1724,1726,1802,1828,2022,2222,
                                  110,120,2025,130,140,150,160,1725,2024)
                 THEN 'CM'
            WHEN w.STATUS_KEY IN (5,10)                   THEN 'requested'
            WHEN w.STATUS_KEY IN (40,1723,1825,1827,1829) THEN 'scheduled'
            WHEN w.STATUS_KEY IN (60,1762,1823,1826,1831) THEN 'arrived'
            WHEN w.STATUS_KEY IN (70,1120,1822,1824,2223) THEN 'in_progress'
        END AS bar_label
    FROM SITE_WORKLIST w
    JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
    CROSS JOIN params p
    WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
      AND o.CREATED_ON_DATE >= p.go_live
      AND w.SCHEDULED_DATE  >= p.d_start
      AND w.SCHEDULED_DATE  <  p.d_end + 1
)
SELECT
    CASE WHEN GROUPING(m.bar_label) = 1 THEN '===== TOTAL ====='
         ELSE NVL(m.bar_label, '*** NULL - HIDDEN FROM CHART ***')
    END                                                                AS bar_in_report_27,
    COUNT(CASE WHEN m.SCHEDULED_DATE <= p.d_end THEN 1 END)             AS expect_on_screen,
    COUNT(*)                                                           AS should_be,
    COUNT(*) - COUNT(CASE WHEN m.SCHEDULED_DATE <= p.d_end THEN 1 END) AS lost_last_day
FROM mix m
CROSS JOIN params p
GROUP BY ROLLUP(m.bar_label)
ORDER BY GROUPING(m.bar_label), 3 DESC;
```

### How to read Query 1

| Column | What it is |
|---|---|
| `expect_on_screen` | what Report 27 renders **today**, bugs included |
| `should_be` | what it would render if the date window were right |
| `lost_last_day` | orders silently dropped by the midnight bug (**D2**) |

Then compare against the screen:

| If the screen shows… | Diagnosis |
|---|---|
| exactly `expect_on_screen` | ETL is fine — the complaint is **D4** (label collapsing). Show them Query 2. |
| **more** than `expect_on_screen` | **D1** double-counting. The excess is the count. |
| **less** than `expect_on_screen` | ETL gap — check `etl_job_log` for `ORDERS_ETL`. |
| a `*** NULL ***` row exists | **D3** — those orders are invisible on the chart entirely. |
| `===== TOTAL =====` ≠ Total Orders tile | combination of the above. |

---

## 3. QUERY 2 — RIS Oracle: what is hidden inside each bar

> Run on the **RIS** Oracle. Same three dates. **This is the table to send the customer.**

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_start,
           DATE '2026-09-18' AS d_end,
           DATE '2025-01-01' AS go_live
      FROM dual
),
mix AS (
    SELECT
        w.STATUS_KEY,
        w.STATUS AS ris_status_name,
        CASE
            WHEN w.STATUS_KEY IN (20,30,50,90,1300,1702,1742,1830,2422)
                 THEN 'CA'
            WHEN w.STATUS_KEY IN (100,1724,1726,1802,1828,2022,2222,
                                  110,120,2025,130,140,150,160,1725,2024)
                 THEN 'CM'
            WHEN w.STATUS_KEY IN (5,10)                   THEN 'requested'
            WHEN w.STATUS_KEY IN (40,1723,1825,1827,1829) THEN 'scheduled'
            WHEN w.STATUS_KEY IN (60,1762,1823,1826,1831) THEN 'arrived'
            WHEN w.STATUS_KEY IN (70,1120,1822,1824,2223) THEN 'in_progress'
        END AS bar_label
    FROM SITE_WORKLIST w
    JOIN ORDERS o ON o.ORDER_KEY = w.ORDER_KEY
    CROSS JOIN params p
    WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
      AND o.CREATED_ON_DATE >= p.go_live
      AND w.SCHEDULED_DATE  >= p.d_start
      AND w.SCHEDULED_DATE  <  p.d_end + 1
)
SELECT
    NVL(m.bar_label, '*** NOT MAPPED - ADD TO migrations/0047 ***') AS bar_in_report_27,
    m.STATUS_KEY,
    m.ris_status_name                                               AS what_the_ris_shows,
    COUNT(*)                                                        AS orders
FROM mix m
GROUP BY NVL(m.bar_label, '*** NOT MAPPED - ADD TO migrations/0047 ***'),
         m.STATUS_KEY, m.ris_status_name
ORDER BY 1, 4 DESC;
```

Expected output shape:

```
bar_in_report_27   STATUS_KEY  what_the_ris_shows   orders
----------------   ----------  ------------------   ------
CM                 160         Approved             48,203
CM                 100         Exam Done             3,115
CM                 130         Signed 1                842
CA                 50          Cancelled             2,004
...
*** NOT MAPPED *** 2611        <new RIS status>        417   <- D3: invisible on chart
```

Any `*** NOT MAPPED ***` row is a status code the RIS added after 0047 was seeded.
Those orders exist, are counted in Total Orders, and are **missing from the chart**.

---

## 4. QUERY 3 — PACS Oracle: double-counting check (D1)

> Run on the **PACS** Oracle. Export to CSV.

```sql
SELECT
    s.ACCESSION_NUMBER,
    s.PROCEDURE_CODE          AS pacs_procedure_code,
    UPPER(TRIM(s.STORING_AE)) AS storing_ae,
    s.STUDY_DATE
FROM medistore.didb_studies s
WHERE s.STUDY_DATE >= DATE '2025-01-01'        -- go_live
  AND s.ACCESSION_NUMBER IS NOT NULL
  AND UPPER(TRIM(s.STORING_AE)) NOT IN (
      'LAUMC','SVSM',
      'ECHOPAC-PC','ADW_8','AETITLE','VIVIDE9-003168','VIVID_S5-050514','TERRA',
      'VIVIDS70-003049','TERRA2','AWVASC','AWCTHD1','PHCARDIO','LOGIQV2-01')
  AND (s.REP_FINAL_SIGNED_BY        IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY)        NOT LIKE '%@DN')
  AND (s.REP_PRELIM_SIGNED_BY       IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY)       NOT LIKE '%@DN')
  AND (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN');
```

Companion export, **RIS** Oracle, same dates:

```sql
SELECT
    w.SPS_ID                              AS accession_number,
    NVL(sc.CODE, TO_CHAR(w.SPS_CODE_KEY)) AS ris_proc_id,
    w.STATUS_KEY,
    w.STATUS                              AS ris_status_name,
    w.SCHEDULED_DATE
FROM SITE_WORKLIST w
JOIN ORDERS o         ON o.ORDER_KEY     = w.ORDER_KEY
LEFT JOIN SPS_CODE sc ON sc.SPS_CODE_KEY = w.SPS_CODE_KEY
WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
  AND o.CREATED_ON_DATE >= DATE '2025-01-01'
  AND w.SCHEDULED_DATE  >= DATE '2026-01-01'
  AND w.SCHEDULED_DATE  <  DATE '2026-09-18' + 1;
```

Join both CSVs on accession, count rows where
`ris_proc_id <> pacs_procedure_code` (both non-null). **Every one of those orders
can be counted twice by Report 27**, and almost all of them sit in the `CM` bar.

---

## 5. The four defects

### D1 — the `OR` join duplicates rows

```sql
LEFT JOIN procedure_duration_map m
    ON m.procedure_code::TEXT = s.procedure_code::TEXT
    OR m.procedure_code::TEXT = o.proc_id::TEXT
```
[report_27.py:38-40](../routes/report_27.py#L38-L40)

`procedure_code` is UNIQUE, so each side matches one row — but `OR` lets **both**
match when the PACS code differs from the RIS code, and `value_counts()` counts
rows. The order is counted twice.

**It skews one bar specifically.** Fan-out needs `s.procedure_code` to be non-NULL,
which only happens once the order resolved to a PACS study — i.e. exam-done or
later — i.e. `CM`. So `CM` inflates while `scheduled` and `CA` do not. "Too many
completed" is the expected symptom.

**Fix:** join once on `s.procedure_code`, fall back to `o.proc_id` — never allow two matches.

### D2 — end date truncated to midnight

```sql
WHERE o.scheduled_datetime BETWEEN :start AND :end
```
[report_27.py:41](../routes/report_27.py#L41)

`:end` is the string `'YYYY-MM-DD'` → casts to `00:00:00`. Everything scheduled
*during* the last day is excluded. Same class as the Report 25 anchor bug.

**Fix:** `>= :start AND < (:end::date + 1)`

### D3 — unmapped statuses vanish silently

`_translate_order_status()` returns `None` for any `STATUS_KEY` not in
`worklist_status_map`, and `value_counts()` drops NaN by default. The bars stop
summing to Total Orders. Migration 0047 says these must be *"surfaced, not
silently dropped"* — the chart does the opposite. `ae_mix` and `sex` both got
`.fillna('Unknown')` ([report_27.py:63-66](../routes/report_27.py#L63-L66));
`order_status` was missed.

**Fix:** `.fillna('Unknown')` on `order_status`, plus add the new keys to a 0115 migration.

### D4 — 42 RIS statuses collapsed into 6 labels

The table in section 1. The card text promises *"current RIS status (e.g.
Scheduled, Completed, Cancelled, In Progress)"*
([report_27.html:287](../templates/report_27.html#L287)) while the axis reads
`CM` / `CA`. If the customer is comparing against their RIS worklist, one `CM`
bar equalling the sum of Approved + Exam Done + Signed + Dictated reads as wrong
even when the arithmetic is right.

**Fix:** needs your decision — several report files hardcode `'CM'`/`'CA'`
comparisons ([etl_orders.py:25-33](../ETL_JOBS/etl_orders.py#L25-L33)), so
un-collapsing is not a local change. Cheapest option is relabelling `CM` →
`Completed` and `CA` → `Cancelled` in the chart only, plus a tooltip listing what
each contains.

---

## 6. Reusable recipe for the next report

Same three steps work for any "the numbers are wrong" complaint on a
RIS-sourced chart:

1. **Trace the displayed value back to its source column.** Find the
   `value_counts()` / `groupby` in `routes/report_NN.py`, then find which ETL job
   writes that column and whether it *translates* on the way in.
2. **Rebuild the number in the source Oracle**, inlining any translation the ETL
   applies (status maps, AE exclusions, signer filters, go-live cutoffs). Return
   the buggy window and the correct window side by side.
3. **Three-way compare:** screen vs `expect_on_screen` vs `should_be`.
   Screen high = duplication from a join; screen low = ETL gap; screen equal =
   the complaint is about labelling, not arithmetic.

Recurring things to check first, since they have already bitten more than once:
`BETWEEN :start AND :end` on a timestamp column, `value_counts()` without
`fillna`, and any `LEFT JOIN ... ON a OR b`.
