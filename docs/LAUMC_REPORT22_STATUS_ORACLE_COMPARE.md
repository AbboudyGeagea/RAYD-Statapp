# Report 22 — Study Status bars vs Oracle: how to prove what's wrong

Operator reported the Study Status chart on Report 22 showing wrong numbers
(a `signed` value appearing split across bars with counts like 1 and 2).

Three self-contained Oracle queries. **Edit the two dates in the `params` block
at the top of each box**, run, compare to the screen. Each box runs on its own —
no assembly needed. Run them against the PACS Oracle (`medistore`), which is the
only side reachable during this investigation.

Set the RAYD date range to the **same two dates** before comparing.

---

## Why the Oracle query is this long

Report 22's chart does not count "all studies in PACS for the period". It counts
what survived the ETL plus the report's own WHERE clause. Comparing a bare
`SELECT COUNT(*) FROM didb_studies` against the screen proves nothing — it will
always be higher. Each box below reproduces the same funnel:

| Stage | Where it lives in RAYD |
|---|---|
| Date range | `report_22.get_where_params()` |
| 14 excluded AE titles | `ETL_JOBS/etl_didb_studies.py` → `_EXCLUDED_AE_SQL` |
| `@dn` signer exclusion | `ETL_JOBS/etl_didb_studies.py` → `_EXCLUDED_SIGNER_SQL` |
| CARD-family purge on the 2 SJH gateway AEs | `ETL_JOBS/etl_runner.py` Phase 2c |
| `study_modality` = most common series modality | `ETL_JOBS/etl_runner.py` Phase 2b |
| Drop `SR` and `OT` | `report_22.py` base_data `WHERE` |

**Two things Oracle cannot reproduce**, both noted again where they matter:

1. **Modality preference.** RAYD uses
   `COALESCE(aetitle_modality_map.modality, study_modality)` — the AE→modality
   map is a Postgres-only table. These queries can only use the series-derived
   modality, so a study whose AE is *mapped* to `SR`/`OT` but whose series say
   otherwise is dropped by RAYD and kept here.
2. **The LAUMC site filter** (`aetitle_modality_map.site_id`, RH only). Also
   Postgres-only. **These queries therefore cover RH + SJH.** If the RAYD screen
   is RH-only, expect Oracle to be higher by roughly the SJH volume. To compare
   like for like, either read the screen with the site filter off, or subtract
   the SJH AEs.

---

## 1. Is the status value itself fragmented?

**This is the decisive query for the reported symptom.** Brackets make trailing
whitespace visible; `chars` makes it unambiguous.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_from,
           DATE '2026-09-19' AS d_to
    FROM dual
),
series_mod AS (
    SELECT se.STUDY_DB_UID,
           STATS_MODE(se.MODALITY) AS study_modality
    FROM   medistore.didb_serieses se
    WHERE  se.MODALITY IS NOT NULL
      AND  TRIM(se.MODALITY) IS NOT NULL
    GROUP  BY se.STUDY_DB_UID
),
rayd_rows AS (
    SELECT s.STUDY_STATUS,
           UPPER(TRIM(s.STORING_AE))      AS storing_ae,
           UPPER(TRIM(sm.study_modality)) AS study_modality
    FROM   medistore.didb_studies s
    CROSS  JOIN params pr
    LEFT   JOIN series_mod sm ON sm.STUDY_DB_UID = s.STUDY_DB_UID
    WHERE  s.STUDY_DATE >= pr.d_from
      AND  s.STUDY_DATE <  pr.d_to + 1
      AND  UPPER(TRIM(s.STORING_AE)) NOT IN (
               'LAUMC','SVSM','ECHOPAC-PC','ADW_8','AETITLE','VIVIDE9-003168',
               'VIVID_S5-050514','TERRA','VIVIDS70-003049','TERRA2','AWVASC',
               'AWCTHD1','PHCARDIO','LOGIQV2-01')
      AND  (s.REP_FINAL_SIGNED_BY        IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY)        NOT LIKE '%@DN')
      AND  (s.REP_PRELIM_SIGNED_BY       IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY)       NOT LIKE '%@DN')
      AND  (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')
)
SELECT '[' || STUDY_STATUS || ']'  AS status_as_stored,
       LENGTH(STUDY_STATUS)        AS chars,
       UPPER(TRIM(STUDY_STATUS))   AS normalised,
       COUNT(*)                    AS studies
FROM   rayd_rows
WHERE  NVL(study_modality,'~') NOT IN ('SR','OT')
  AND  NOT (storing_ae IN ('SJHCSAPWFMFIR','LAUMCWFM2FIR')
            AND study_modality IN ('CARD','SJH_CARD','CARDUS','SJHCARD'))
GROUP  BY STUDY_STATUS
ORDER  BY UPPER(TRIM(STUDY_STATUS)), STUDY_STATUS;
```

### How to read it

**If two or more rows share the same `normalised` value** — e.g.

```
status_as_stored   chars  normalised  studies
[signed]               6  SIGNED         8214
[signed  ]             8  SIGNED            2
[SIGNED]               6  SIGNED            1
```

then the bug is confirmed and it is entirely on the RAYD side. The chart groups
the **raw** string ([report_22.py:147](../routes/report_22.py#L147)
`COALESCE(study_status,'N/A')`), so each spelling becomes its own bar — which is
exactly the "signed 1, 2" the operator saw. Meanwhile:

- the sidebar dropdown lists `DISTINCT TRIM(study_status)`
  ([report_cache.py:166](../routes/report_cache.py#L166)) — collapses padding, keeps case;
- the sidebar filter matches `UPPER(TRIM(study_status))`
  ([report_22.py:52](../routes/report_22.py#L52)) — collapses both;
- the click-through drilldown matches `UPPER(study_status)` with **no TRIM**
  ([report_22.py:520](../routes/report_22.py#L520)) — so clicking a padded bar
  returns a row list that does not add up to the bar.

Four consumers, three different normalisations. The fix is to normalise all of
them and, ideally, to `UPPER(TRIM(...))` `STUDY_STATUS` in the ETL the way
`STORING_AE` already is at
[etl_didb_studies.py:93](../ETL_JOBS/etl_didb_studies.py#L93).

**If every `normalised` value appears exactly once**, the status strings are
clean and the problem is volume, not labelling — go to box 2 and 3.

---

## 2. What the chart SHOULD show

Same scope, collapsed the way every other consumer collapses it. These numbers
are the target: after the fix, the bars should read exactly this.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_from,
           DATE '2026-09-19' AS d_to
    FROM dual
),
series_mod AS (
    SELECT se.STUDY_DB_UID,
           STATS_MODE(se.MODALITY) AS study_modality
    FROM   medistore.didb_serieses se
    WHERE  se.MODALITY IS NOT NULL
      AND  TRIM(se.MODALITY) IS NOT NULL
    GROUP  BY se.STUDY_DB_UID
)
SELECT NVL(UPPER(TRIM(s.STUDY_STATUS)), 'N/A') AS status,
       COUNT(*)                                AS studies
FROM   medistore.didb_studies s
CROSS  JOIN params pr
LEFT   JOIN series_mod sm ON sm.STUDY_DB_UID = s.STUDY_DB_UID
WHERE  s.STUDY_DATE >= pr.d_from
  AND  s.STUDY_DATE <  pr.d_to + 1
  AND  UPPER(TRIM(s.STORING_AE)) NOT IN (
           'LAUMC','SVSM','ECHOPAC-PC','ADW_8','AETITLE','VIVIDE9-003168',
           'VIVID_S5-050514','TERRA','VIVIDS70-003049','TERRA2','AWVASC',
           'AWCTHD1','PHCARDIO','LOGIQV2-01')
  AND  (s.REP_FINAL_SIGNED_BY        IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY)        NOT LIKE '%@DN')
  AND  (s.REP_PRELIM_SIGNED_BY       IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY)       NOT LIKE '%@DN')
  AND  (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')
  AND  NVL(UPPER(TRIM(sm.study_modality)),'~') NOT IN ('SR','OT')
  AND  NOT (UPPER(TRIM(s.STORING_AE)) IN ('SJHCSAPWFMFIR','LAUMCWFM2FIR')
            AND UPPER(TRIM(sm.study_modality)) IN ('CARD','SJH_CARD','CARDUS','SJHCARD'))
GROUP  BY NVL(UPPER(TRIM(s.STUDY_STATUS)), 'N/A')
ORDER  BY 2 DESC;
```

Write the screen's bars next to this and total both columns:

| Status | Screen | Oracle | Delta |
|---|---:|---:|---:|
| | | | |
| **Total** | | | |

**Screen total HIGHER than Oracle** → row fan-out. Report 22 counts with
`COUNT(*)` on a base query that LEFT JOINs `aetitle_modality_map` and
`procedure_duration_map` on `UPPER(TRIM(...))`, while both tables' UNIQUE
constraint is on the **raw** column — so two case-variant rows both match one
study and double it. Migration 0116 repaired this for `aetitle_modality_map`
(it names Report 22: CT99 read 30 where PACS had 15); **no equivalent fix exists
for `procedure_duration_map`**, whose three writers disagree on casing
(`etl_runner.py` Phase 8 and `etl_ris_procedures.py` insert as-is,
`mapping_controller.py`'s CSV upload uppercases). Same class of bug already
measured on Report 27: `CM` read 9.8% high.

**Screen total LOWER than Oracle** → either the site filter (Oracle here is
RH + SJH — see the caveat at the top) or ETL lag. Box 3 separates them.

---

## 3. Where do the studies go? (one-pass funnel)

Run this when box 2 shows a total mismatch. Each column is the previous one plus
one more RAYD filter, so the drop between two adjacent columns names the cause.

```sql
WITH params AS (
    SELECT DATE '2026-01-01' AS d_from,
           DATE '2026-09-19' AS d_to
    FROM dual
),
series_mod AS (
    SELECT se.STUDY_DB_UID,
           STATS_MODE(se.MODALITY) AS study_modality
    FROM   medistore.didb_serieses se
    WHERE  se.MODALITY IS NOT NULL
      AND  TRIM(se.MODALITY) IS NOT NULL
    GROUP  BY se.STUDY_DB_UID
),
flagged AS (
    SELECT CASE WHEN UPPER(TRIM(s.STORING_AE)) NOT IN (
                    'LAUMC','SVSM','ECHOPAC-PC','ADW_8','AETITLE','VIVIDE9-003168',
                    'VIVID_S5-050514','TERRA','VIVIDS70-003049','TERRA2','AWVASC',
                    'AWCTHD1','PHCARDIO','LOGIQV2-01')
                THEN 1 ELSE 0 END AS ae_ok,
           CASE WHEN (s.REP_FINAL_SIGNED_BY        IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY)        NOT LIKE '%@DN')
                 AND (s.REP_PRELIM_SIGNED_BY       IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY)       NOT LIKE '%@DN')
                 AND (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')
                THEN 1 ELSE 0 END AS signer_ok,
           CASE WHEN NOT (UPPER(TRIM(s.STORING_AE)) IN ('SJHCSAPWFMFIR','LAUMCWFM2FIR')
                          AND UPPER(TRIM(sm.study_modality)) IN ('CARD','SJH_CARD','CARDUS','SJHCARD'))
                THEN 1 ELSE 0 END AS card_ok,
           CASE WHEN NVL(UPPER(TRIM(sm.study_modality)),'~') NOT IN ('SR','OT')
                THEN 1 ELSE 0 END AS mod_ok
    FROM   medistore.didb_studies s
    CROSS  JOIN params pr
    LEFT   JOIN series_mod sm ON sm.STUDY_DB_UID = s.STUDY_DB_UID
    WHERE  s.STUDY_DATE >= pr.d_from
      AND  s.STUDY_DATE <  pr.d_to + 1
)
SELECT COUNT(*)                                                                AS s1_all_in_period,
       COUNT(CASE WHEN ae_ok=1                                        THEN 1 END) AS s2_after_ae_excl,
       COUNT(CASE WHEN ae_ok=1 AND signer_ok=1                        THEN 1 END) AS s3_after_dn_excl,
       COUNT(CASE WHEN ae_ok=1 AND signer_ok=1 AND card_ok=1          THEN 1 END) AS s4_after_card_purge,
       COUNT(CASE WHEN ae_ok=1 AND signer_ok=1 AND card_ok=1 AND mod_ok=1 THEN 1 END) AS s5_rayd_should_show
FROM   flagged;
```

`s5_rayd_should_show` must equal the total of box 2. Compare it to the Report 22
total on screen:

| Observation | Cause | Next step |
|---|---|---|
| Screen > `s5` | Join fan-out (`COUNT(*)` × duplicate map rows) | Dedupe `procedure_duration_map` the way 0116 did `aetitle_modality_map`; switch Report 22 to `COUNT(DISTINCT study_db_uid)` |
| Screen < `s5`, gap ≈ SJH volume | Site filter — expected, not a bug | Re-read the screen with the site filter off |
| Screen < `s5`, gap is recent dates only | ETL lag / incremental watermark | Check the last `daily_etl_sync` run |
| Screen ≈ `s5` but bars are split | Status string fragmentation | Box 1 — normalise `study_status` |

---

## Appendix — status values exactly as PACS stores them

Useful if box 1 shows padding: confirms whether it originates in Oracle (a
`CHAR` column pads to its declared width) or is introduced later.

```sql
SELECT column_name, data_type, data_length, char_used
FROM   all_tab_columns
WHERE  owner = 'MEDISTORE'
  AND  table_name = 'DIDB_STUDIES'
  AND  column_name = 'STUDY_STATUS';
```

`data_type = CHAR` means PACS pads every value to `data_length` and the ETL
loads the padding verbatim — the ETL-side `TRIM` is then the real fix, not just
the report-side one.
