# LAUMC — deploy & verification checklist

Everything committed to `origin/LAUMC` on 2026-09-18, in the order it must be done.
Nothing here has been run against the live system yet.

Commits: `578ec776` · `f0a12cb0` · `0ce6f6b9` · `d3944461` · `51c59d4c`

---

## 1. Deploy

```bash
dc build rayd-app && dc up -d rayd-app
dc logs rayd-app | grep migrations          # expect 0112_etl_orders_status_key applied
```

Migration 0112 adds `etl_orders.status_key` and applies itself at startup.

### Then two jobs that do NOT run automatically

```bash
# (a) Report 27 status tooltips — orders ETL is incremental on LAST_UPDATE_DATE,
#     so existing rows keep status_key NULL until a full rebuild
RAYD_ETL_ORDERS_FULL_REBUILD=1   <orders etl invocation>

# (b) RVU import — Phase 10, runs after run_ris_procedures_etl
<normal ETL cycle, or Phase 10 specifically>
```

Until (a) runs, Report 27's bars are still correct but every tooltip reads
"Unknown". Until (b) runs, Report 25's two RVU columns stay identical.

---

## 2. Expected numbers

Range used for every figure below: **2026-01-01 .. 2026-09-18**.

### Report 27 — Order Status Mix

| Bar | Expect |
|---|---:|
| Completed / Reported | 82,413 |
| Scheduled | 27,299 |
| Cancelled | 4,877 |
| Arrived | 133 |
| In Progress | 76 |
| **Total** | **114,798** |

Was showing 122,321 (CM alone was 90,108 against a true 82,071). If the total is
114,798, D1 and D2 are both fixed.

After the backfill, hovering a bar must list real RIS statuses — Approved 82,013,
Signed 1 223, Exam Done 102, Pending 58, Signed 2 16, Dictated 1. Any bar still
showing a single "Unknown" entry means the backfill did not run.

### Report 23 — study counts

| | Expect |
|---|---:|
| Total Analyzed Studies | **~41,101** (was 6,877) |
| Reports Without Images | **1,024** |

Note the old 6,877 *included* the 1,024 report-only rows, so real imaging studies
went from ~5,853 to ~41,101. If the total comes back near 6,877, the site filter
change did not take effect.

### Report 25 / RVU import

```bash
dc logs rayd-app | grep "RIS Procedure RVU ETL"
# expect: ~587 procedures priced (586 clinical, 60 technical)
```

```sql
SELECT COUNT(*)                                                   AS procedures,
       COUNT(*) FILTER (WHERE clinical_rvu <> technical_rvu)      AS actually_split,
       COUNT(*) FILTER (WHERE clinical_rvu = 1.0
                          AND technical_rvu = 1.0)                AS still_default,
       MAX(clinical_rvu)                                          AS max_clinical,
       MAX(technical_rvu)                                         AS max_technical
FROM procedure_duration_map;
```

Expect `max_clinical` ≈ 42.09 and `max_technical` ≈ 2.59. If `actually_split` is
still 0 the import did not land.

Also confirm the technical column was **not** zeroed — only 60/597 procedures have
a RIS technical value, and the other 537 must keep whatever they had. If Report
25's Device/Technical RVU column collapses toward zero, stop and revert.

---

## 3. Report 31 — suspected already broken, unrelated to today's work

[report_31.py:120](../routes/report_31.py#L120) selects `rvu` from the
`report_id = 25` template, but that template emits `clinical_rvu`/`technical_rvu`
(every migration 0069→0110 writes `pm.clinical_rvu, pm.technical_rvu`; none emit
`rvu`). The query should therefore raise `column "rvu" does not exist`,
[lines 127-130](../routes/report_31.py#L127-L130) swallow it, and the report
renders as "no data" with the error only in the log.

```sql
SELECT report_sql_query LIKE '%clinical_rvu%' AS has_split,
       report_sql_query LIKE '%as rvu,%'      AS has_legacy_rvu
FROM report_template WHERE report_id = 25;
```

```bash
dc logs rayd-app | grep "Report 31 base query failed"
```

`has_split = true` + `has_legacy_rvu = false` + any log hit = confirmed broken.
Fix not written yet — deliberately, since the right fix depends on this answer.

---

## 4. Things that will change and are not bugs

**Report 25 Match Success % and Avg Duration will shift.** Both were computed over
the rows the D1 fan-out duplicated. They are getting more accurate, but the
customer will see the numbers move.

**Report 32 radiologist rankings may reorder.** If RVUs are currently at the 1.0
default, every "RVU" figure in the app is effectively a study count and RVU/hr is
studies/hr. Once real values land they become complexity-weighted, so "highest
RVU/hr" can name a different radiologist. Warn whoever reads that report.

**Report 32's RVU KPI is now two figures, not one.** It used to print
clinical + technical summed, which double-counts; `routes/report_31.py:156-163`
documents that convention. Nobody noticed because the halves were identical.

**Mapping-tab RVU edits are no longer durable.** The RIS now overwrites
`clinical_rvu`/`technical_rvu` on every run for any procedure it prices (operator
decision). The mapping tab still presents those fields as editable — a UI note is
outstanding.

**The revenue/financial tab is deliberately out of scope**, but its `$/RVU` rate
was almost certainly calibrated against the placeholder 1.0 values. Revenue figures
will move when real RVUs land.

---

## 5. Still open

| # | Item | Needs |
|---|---|---|
| 1 | Report 23 **S2** — 11 gateway AEs mapped to modality `'PACS'` override the series-derived `study_modality`, so SR/PR traffic counts as studies | operator decision; **lowers** the number |
| 2 | Report 23 **S3** — `'OT'` exclusion drops real DEXA (`GELUNAR`) | operator decision; changes the number |
| 3 | ~21 unmapped AEs need `aetitle_modality_map` rows — they now count correctly but show modality NULL, so they are missing from modality charts and the Modality filter | device types confirmed (list in the todo memory) |
| 4 | Four near-miss seed names: `GELUNAR11`/`GELUNAR`, `SYMBIANET`/`SYMBIA`, `AWCTHD1`/`CTHD`, `SENO2`/`SENO1` | same migration as #3 |
| 5 | PACS `SITE_ID = '2'` (`LAUMCWFM1AR`, 1,024 studies) is not in the `sites` table | is it a real third site? |
| 6 | `SENO1` appears on both sites (1,415 SJH / 102 RH) — one AE title cannot carry a single `site_id` | same treatment as the SJH gateway AEs |
| 7 | Report 31 fix | section 3 answer first |
| 8 | Four SQL sites still sum clinical + technical (`financial_dashboard.py:68,:136`, `report_widgets.py:711,:719`) | **not a bug** — they feed revenue, where professional + technical is the correct global fee |

---

## 6. If something looks wrong

The two diagnostic docs carry the Oracle queries that produced every expected
number above, so a mismatch can be re-measured rather than guessed:

- [LAUMC_REPORT27_STATUS_MIX_DIAGNOSTIC.md](LAUMC_REPORT27_STATUS_MIX_DIAGNOSTIC.md)
- [LAUMC_REPORT23_RIS_VS_PACS_DIAGNOSTIC.md](LAUMC_REPORT23_RIS_VS_PACS_DIAGNOSTIC.md)

Section 6 of the Report 27 doc has the reusable 3-step recipe for the next one of
these. The repeat offenders so far: `BETWEEN :start AND :end` on a timestamp
column, `value_counts()` without `fillna`, `LEFT JOIN ... ON a OR b`, and a
`LEFT JOIN`ed column used in a `WHERE` equality (which silently makes it an INNER
JOIN — that was the 83% Report 23 bug).
