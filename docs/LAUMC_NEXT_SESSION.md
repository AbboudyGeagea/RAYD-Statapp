# LAUMC — To do

Updated 2026-11-17. This file holds only what is still open. What was built is in the commit
messages, and the July version of this file (history, punch list round 2) is in git history.
Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor questions), `LAUMC_DATA_REQUEST.md` (discovery record).

---

## Deadline and sessions

- **Deadline for this list: 2026-11-30.** Item 2 (first open) has its own deadline, 2026-11-20.
- **Plan to 30 November (set 2026-11-17):**

  | When | Server (operator) | Laptop (build) |
  |---|---|---|
  | Tue 17 – Wed 18 | item 2 timing query, PACS account, grant; item 29 check; **deploy** (0154, 0155, CRN, item 6, item 2) | 13b: the SJH move script, rehearsed on the local sample |
  | Thu 19 – Fri 20 | 05:00 copies the audit from 1 Oct; regenerate Oct/Nov in Report 37; spot-check (**item 2 due Fri**) | 13b: RH's ETL stops loading SJH, SJH's own nightly ETL |
  | Mon 23 – Wed 25 | 14A/14B validation against Oracle (Wed) | 14A and 14B in Report 32 |
  | Thu 26 | deploy 14A/14B and 13b's code | — |
  | Sat 28 – Sun 29 | **13b on production**: copy SJH, compare counts, delete from RH, SJH's first ETL | on call |
  | through Nov | CRN go-live on site: Email Server, CRN gateway + firewall, fallback contact, wording, test, switch on | — |
- **Monday 2026-11-16, 10:00: run the two room-name queries (item 4)** and compare the RIS
  room names with the labels RAYD shows. Then a working session on the PACS audit table
  (item 2). Sessions start at 9:30.

  **Item 2, finding the audit table that records opens.** First, with any login:
  `scripts/sql/pacs_first_open_finder.sql` (open an old study, then it searches every table's
  rows of the last 20 minutes for that accession and login). Then, if needed (PACS Oracle, a DBA login: RAYD's
  account cannot see `AUDITING` or `AUDIT_OFFLINE`, checked 2026-11-05; run by a person in
  SQL Developer, once). A–C read the data dictionary only, no load:
  ```sql
  -- A. the audit tables and their size
  SELECT owner, table_name, num_rows, last_analyzed FROM dba_tables
  WHERE owner IN ('AUDITING', 'AUDIT_OFFLINE') ORDER BY num_rows DESC NULLS LAST;

  -- B. their columns
  SELECT owner, table_name, column_name, data_type FROM dba_tab_columns
  WHERE owner IN ('AUDITING', 'AUDIT_OFFLINE') ORDER BY owner, table_name, column_id;

  -- C. time columns that lead an index (the only ones D reads)
  SELECT ic.table_owner, ic.table_name, ic.column_name, ic.index_name
  FROM dba_ind_columns ic
  JOIN dba_tab_columns c ON c.owner = ic.table_owner AND c.table_name = ic.table_name
                        AND c.column_name = ic.column_name
  WHERE ic.table_owner IN ('AUDITING', 'AUDIT_OFFLINE') AND ic.column_position = 1
    AND (c.data_type = 'DATE' OR c.data_type LIKE 'TIMESTAMP%');
  ```
  D, the live test: someone opens a known study in the PACS viewer; note the exact time, the
  accession and the login. Within 5 minutes, count each audit table's rows of the last 10
  minutes, through indexed time columns only:
  ```sql
  SET SERVEROUTPUT ON
  DECLARE n NUMBER;
  BEGIN
    FOR c IN (SELECT DISTINCT ic.table_owner o, ic.table_name t, ic.column_name col
              FROM dba_ind_columns ic
              JOIN dba_tab_columns tc ON tc.owner = ic.table_owner AND tc.table_name = ic.table_name
                                     AND tc.column_name = ic.column_name
              WHERE ic.table_owner IN ('AUDITING', 'AUDIT_OFFLINE') AND ic.column_position = 1
                AND (tc.data_type = 'DATE' OR tc.data_type LIKE 'TIMESTAMP%')) LOOP
      BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM "' || c.o || '"."' || c.t || '" WHERE "' || c.col
                          || '" > SYSDATE - 10/1440' INTO n;
        IF n > 0 THEN DBMS_OUTPUT.PUT_LINE(c.o || '.' || c.t || '.' || c.col || ': ' || n); END IF;
      EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE(c.o || '.' || c.t || ': ' || SQLERRM);
      END;
    END LOOP;
  END;
  /
  ```
  E. For each table D names: `SELECT * FROM <owner>.<table> WHERE <column> > SYSDATE - 10/1440
  ORDER BY <column>;` and look for the open: accession or study UID, login, time.

  RIS Oracle (the query `ETL_JOBS/etl_ris_modality.py` runs; `ORDER BY` added for reading):
  ```sql
  SELECT m.MODALITY_KEY, m.AE_TITLE, m.CODE, m.DESCRIPTION, m.STATION_NAME,
         m.ORG_STRUCTURE_KEY, m.ACTIVE, mt.CODE AS MODALITY_TYPE_CODE
  FROM MODALITY m
  LEFT JOIN MODALITY_TYPE mt ON mt.MODALITY_TYPE_KEY = m.MODALITY_TYPE_KEY
  ORDER BY m.AE_TITLE;
  ```
  `STATION_NAME` → room name, `CODE` → room code, `ORG_STRUCTURE_KEY` → site,
  `MODALITY_TYPE.CODE` → modality.

  RAYD Postgres (what is stored, and the label shown on screen):
  ```sql
  SELECT aetitle, room_code, station_name, room_name, display_aetitle, modality, site_id, active,
         COALESCE(NULLIF(BTRIM(display_aetitle),''), NULLIF(BTRIM(room_name),''),
                  NULLIF(BTRIM(station_name),''), aetitle) AS label_on_screen
  FROM aetitle_modality_map ORDER BY aetitle;
  ```
- **Monday too, item 19: find the test patients.** Names stay in Oracle; only the patient IDs
  are typed on the BRAYN console (Settings → Placeholder records). Review the list by hand:
  `%TEST%` also catches real surnames.
  ```sql
  -- RIS Oracle: test patients and their patient IDs
  SELECT pil.PATIENT_ID, pil.IS_PRIMARY, per.LAST_NAME, per.FIRST_NAME
  FROM PATIENT p
  JOIN PERSON per ON per.PERSON_KEY = p.PATIENT_PERSON_KEY
  JOIN PATIENT_ID_LIST pil ON pil.PATIENT_PERSON_KEY = p.PATIENT_PERSON_KEY
  WHERE UPPER(per.LAST_NAME) LIKE '%TEST%' OR UPPER(per.FIRST_NAME) LIKE '%TEST%'
     OR UPPER(per.LAST_NAME) LIKE '%PHANTOM%' OR UPPER(per.LAST_NAME) IN ('QA', 'DUMMY')
  ORDER BY per.LAST_NAME, pil.PATIENT_ID;

  -- PACS Oracle: the same on PACS (QA images sent with no RIS order have only this ID)
  SELECT p.ID, p.LAST_NAME, p.FIRST_NAME, p.NUMBER_OF_PATIENT_STUDIES
  FROM MEDISTORE.DIDB_PATIENTS p
  WHERE UPPER(p.LAST_NAME) LIKE '%TEST%' OR UPPER(p.FIRST_NAME) LIKE '%TEST%'
     OR UPPER(p.LAST_NAME) LIKE '%PHANTOM%' OR UPPER(p.LAST_NAME) IN ('QA', 'DUMMY')
  ORDER BY p.NUMBER_OF_PATIENT_STUDIES DESC;
  ```
  After deploy, the console's "Reaches" column shows how many studies each record touches.
  If "Referring, Generic" reaches 0 studies, PACS spells it differently. Find the spelling on
  RAYD Postgres and correct the name on the console:
  ```sql
  SELECT referring_physician_last_name, referring_physician_first_name, COUNT(*)
  FROM etl_didb_studies
  WHERE referring_physician_last_name ILIKE '%generic%' OR referring_physician_first_name ILIKE '%generic%'
     OR referring_physician_last_name ILIKE '%referring%'
  GROUP BY 1, 2 ORDER BY 3 DESC;
  ```
- **Monday too, item 27 (read-only):** the one table outside HL7 with a patient name column
  (the HL7 tables may hold names, operator 2026-11-15).
  ```sql
  SELECT COUNT(*) FILTER (WHERE NULLIF(BTRIM(patient_name), '') IS NOT NULL) AS with_name, COUNT(*) AS total
  FROM cd_print_log;
  ```
  Optional, for the laptop (item 28): a pseudonymised sample, read-only on the server (the
  first version hung on `hl7_oru_reports`; fixed 2026-11-15, copy the script in again).
  ```bash
  docker cp scripts/sample_data/export_sample.py rayd_service:/tmp/
  docker exec rayd_service python /tmp/export_sample.py --from 2026-10-01 --to 2026-10-14
  docker cp rayd_service:/tmp/rayd_sample.tar.gz .
  docker exec rayd_service rm /tmp/rayd_sample.tar.gz /tmp/export_sample.py
  ```
- **Tuesday 2026-11-17, from 9:30:** double-check session on Report 27 Order Status Mix,
  mainly the scheduled status (item 7); a session on the PACS user columns in
  `MEDILINK.SECM_USERS` (item 9); and finding the RIS column that holds ORC-2 (item 1).

## Estimates (2026-11-14)

Working days. A day is one working day of building, with the operator available for
questions; validation days need the operator (and Oracle queries he runs). Waiting on others
(Mirth forwarding, the chief radiologist, the vendor) is not counted. Oracle backfills run
only at weekends, which can add calendar time.

| # | Item | Difficulty | Analysis | Build + tests | Validation | Total | Waits on |
|---|---|---|---|---|---|---|---|
| 1 | HIS vs RIS orders (Report 27) | Medium | 0.5–1 | 2–3 | 0.5–1 | 3–5 | Mirth ORM forwarding; RIS column for ORC-2 |
| 2 | First open from PACS audit (due 2026-11-20) — audit found and proven, built, **deployed** 2026-11-17 | High | 1–2 | 2–3 | 0.5–1 | 3.5–6 | the 05:00 copy of 18 Nov; regenerate October |
| 3 | Report 22 AE filter + stress test of all filters — **done** | Medium | 0.5 | 3–4 | 0.5 | 4–5 | — |
| 4 | RIS room names everywhere — **built** | Medium | 0.5 | 3–4 | 0.5–1 | 4–5.5 | RIS data fixed after the Monday queries |
| 5 | ORU severity on a standard | Medium | 1–2 | 2–3 | 1–2 | 4–7 | chief radiologist |
| 6 | Report 27 hourly load, scheduled vs started — **done** ("started" = the RIS performed step's start) | Low | 0.25 | 0.5–1 | 0.25 | 1–1.5 | — |
| 7 | Report 27 status mix check | Low–Medium | 0.5 | 0–2 | 0.5 | 1–3 | Tuesday session |
| 8 | Super Report residents by wet read (PACS) | Medium–High | 1–1.5 | 2–2.5 | 0.5–1 | 3.5–5 | PACS timestamp check |
| 9 | Staff who left, hidden from KPIs — **built** 2026-11-17 (migration 0157, Report 39; ships off) | Medium | 1 | 2.5–3 | 0.5 | 4–4.5 | deploy; Report 39 check on production; switch on |
| 10 | Modality "PACS" samples | Low | 0.25 | 0–1 | 0.25 | 0.5–1.5 | operator’s review |
| 11 | Report start date setting — **done** | Low | 0.1 | 1 | 0.25 | 1–1.5 | — |
| 12 | Chart explanations (~150 notes, 16 pages) — **done** | Low–Medium | 0.5 | 2.5–3.5 | 0.5 | 3.5–4.5 | — |
| 13 | SJH database, all parts (13f roles and access **built**; 13a **built** but BRAYN routing by site) | High | 3–4 | 14–18 | 2–3 | 19–25 | BRAYN on; site-ID walkthrough |
| 14 | Radiologist TAT and report counts on PACS | High | 2 | 4–5 | 1–2 | 7–9 | shares item 8’s PACS copy |
| 14A | Radiologist TAT, two charts: RIS exam done → signature, PACS arrival → signature (added 2026-11-17) — **built**, ETL query checked on Oracle | Low–Medium | 0.25 | 1–1.5 | 0.5 | 1.75–2.25 | deploy; one night; spot-check |
| 14B | Reporting time: first open → signature (added 2026-11-17) — **built** with 14A | Low | 0.1 | 0.5 | 0.25 | 0.75–1 | as 14A |

Item 13 splits into: second database, per-request switch, migrations and SJH ETL (6–8);
migrate, count and delete SJH rows (2–3); site dropdown (1); RH vs SJH per-modality views
(2–3); BRAYN routing by site (1–2); new roles and permissions (3–4); mail server per site and
licence (1). Built by 2026-11-15: roles and permissions, second database, switch, migrations,
site dropdown, per-modality views, mail server per site. Left: BRAYN routing (13d, the
operator's walkthrough), SJH's ETL and the data move (13b, on production).

Items 1–14 without item 5 add up to about 54–76 days (35–51 without item 13). Items 8 and 14
share about one day. Doing item 3 together with the shared filter panel saves 1–2 days.

| # | Earlier and added items | Difficulty | Total | Waits on |
|---|---|---|---|---|
| 15 | Urgent emergent in HL7 | Low | 2 | end user and HIS answer |
| 16 | Shared filter panel, 6 reports | Medium | 4–5.5 | — |
| 17 | R2I and BRAYN switch-on checks | Medium | 2 | feed live |
| 18 | ORU final-only via BRAYN | Low–Medium | 2 | meaning of `FAP` or a prelim sample |
| 19 | Placeholders via BRAYN — **done** (test patient IDs on Monday) | Low | 1.5–2 | — |
| 20 | CRN go-live — **this month** (operator, 2026-11-15); email-to-SMS sender and hospital name **built** | Medium | 2–3 left | Email Server, CRN gateway, fallback contact, wording |
| 21 | Patient schedule lead time (very low) | Low–Medium | 2.5–3 | which moment counts as "booked" |
| 22 | Daily Briefing snapshot RH only (found 2026-11-14) — **done** | Low | 1 | — (counts checked on the server) |
| 23 | Yesterday tab Orders ↔ PACS reconciliation (found 2026-11-14) — **done** | Low | 0.5–1 | — |
| 24 | Deploy — **done** 2026-11-17 (`update.sh LAUMC`: 0154, 0155, CRN email to SMS, item 6, item 2) | Low | 0.5 | after-deploy: test patients on BRAYN (item 19) |
| 25 | ER dashboard onto the one report UI (Report 38) — **done** | Low | 1–1.5 | — |
| 26 | SJH census of RH's Postgres — **done** (run 2026-11-15) | Low | 0.25 | — |
| 27 | Patient names outside the HL7 tables (`cd_print_log`) — **done** (0 rows) | Low | 0.25 | — |
| 28 | Local LAUMC stack and sample data from production — **done** (October 1–14 loaded) | Low | — | — |
| 29 | Cardiology purge (ETL Phase 2c) not running on production — **done** (1,036 purged on 17 Nov, count 0) | Low | 0.25 | — |

Item 25 is the ER dashboard only (operator, 2026-11-14): the ORU tab stays a standalone page,
out of scope.

## Server access (2026-11-14)

Whether an item needs LAUMC's RAYD server (its Postgres) to be built and finished. "Local" means
built and tested here on a local Postgres. Oracle queries and decisions from other people
are listed under "Waits on" above. Those waits hold whatever the class is.

| Class | Items | Can start now |
|---|---|---|
| **Local, no server** | 3 Report 22 AE filter + stress test; 6 hourly load; 11 report start date; 12 chart explanations; shared filter panel; placeholders via BRAYN | yes |
| **Local, no server** (blocked) | 1 HIS vs RIS (Mirth ORMs, RIS column for ORC-2); 2 first open (audit table, Monday); 5 ORU severity (chief radiologist); 8 wet read (PACS Oracle check first); ORU final-only (`FAP`); urgent emergent in HL7 (a question, no code); patient schedule lead time | no |
| **Local build, server to validate** | 4 RIS room names (Monday queries, one on RAYD Postgres); 14 radiologist TAT (compared with Oracle on production) | build yes |
| **Needs the server** | 7 status mix check (RAYD vs Oracle at one cut-off); 9 staff who left (export of names, PACS user columns); 10 modality "PACS" samples; 13 SJH database (the build is local, the move, counts and delete run on production); R2I and BRAYN switch-on checks; CRN go-live; 27 patient-name check | no |

Order for work without the server: 3, then 4 (build), 6, 11, 12, shared filter panel.

## Priorities from the meeting of 2026-11-13 (highest first)

Operator's answers of 2026-11-13 and 2026-11-14 are folded in below.

**BRAYN becomes the gate (agreed 2026-11-14).** It switches on the week of 2026-11-16 and
decides what reaches RAYD's tables: duplicate first-open messages, final-only ORU, placeholder
records, and routing each HL7 message to the RH or SJH database. Roll out per message type,
starting with the new first-open messages, then orders, then ORU. CLAUDE.md rule 7 changes
with it.

**Standing rule from this meeting:** all reporting data (wet read, prelim, approved, final:
who and when) comes from PACS. Each figure is proven by a query on PACS Oracle first, then
copied to Postgres by the nightly ETL and shown with the same logic. Residents only sign wet
read; radiologists approve or finalise (check both).

1. **HIS orders vs RIS completed — a new tab in Report 27.** Count the orders arriving by HL7
   (ORM only), compare them with orders that reached exam done on the RIS, and highlight
   orders done on the RIS with no HIS order. RAYD receives only ORU today; the operator will
   forward the ORMs from Mirth, SJH included. Match on ORC-2 (HIS order number, which RIS
   orders carry); which RIS column holds it is still to be found. One HL7 message = one exam.
   Count each order number once, from its first new-order message; HIS cancellations shown
   separately; change messages are not new orders. Every order should have a HIS order
   (inpatient, outpatient, walk-in, ER), since HIS is the billing source, and billing always
   comes first (until the RIS/PACS upgrade), so a RIS exam with no HIS order is highlighted at
   once, with no grace time. "Completed" = exam done for now. The tab starts on the day the
   order feed starts.
2. **First open of a study (Report 37, JCI) — deadline 2026-11-20 for a final solution.** A
   Mirth channel (Mirth 3.3.1 on the PACS server, the operator configures it; upgrade to 4.4.1
   in 3-5 months) reads the PACS audit table on insert or update and sends RAYD a message.
   No triggers on the vendor DB. Polling interval (1 or 30 minutes) to be agreed with the
   manager. Messages pass through BRAYN. The audit table and channel are worked out together
   with the operator (session Monday 2026-11-16). The operator says the audit keeps every
   activity since the beginning, but is not convinced it records the open, so other ways are
   checked too. If the audit table keeps every open with its own time, a once-a-night read
   gives exact first opens with the least load. Which studies count as urgent emergent waits
   on the JCI answer (end user, Monday): PACS alone recognises about half of October's 1,430
   cases (734 by location ER; 696 only through the RIS order's "Emergency room B1"). If the
   audit query works, the message to RAYD is HL7 to the listener (port 6661) with accession
   number, study UID, login and open time.
   **Found 2026-11-17:** the audit is `AUDITING.AUDIT_LOG`, 217,776,000 rows, analysed
   nightly (03:09), so written every day; `AUDIT_OFFLINE.AUDIT_LOG` exists too (not
   analysed, probably the archived part). Readable as SYS (the operator's login), not by
   RAYD's account. Other log-like tables: `MEDISTORE.DIDB_STUDY_EVENTS` (8.0 M) and
   `_PREVIOUS` (11.7 M), the 14 event types, which have no open; `MEDILINK.SECM_SESSIONS`
   (21,291, sign-ins).
   **Proven 2026-11-17** (live open of accession 1005003240136 by `trad` at 08:33:04):
   opening a study in the Vue PACS workstation writes, the same second, "Instances Used /
   Modify" with tags LOCKED + the login, and "Instances Used / Access" "Access to N
   instances" (images displayed); Vue Motion writes the Access row too. `EVENT_DATE` is
   hospital time. History is complete: the same study's 2025 reading shows arrival 00:50,
   first open roy.ibrahim 09:54:27, wet read (tag WETREAD) 09:59:26, approved and signed by
   julie.ferzli 11:39:00, so items 8 and 14 can use this audit too. Indexed: LOG_DBID (PK),
   EVENT_DATE, STUDY_INSTANCE_UID, UPPER(accession), USER_NAME, CATEGORY+SUB_CATEGORY,
   ACTION. About 150,000 rows a day; the open + lock rows about 6,000.
   **Built 2026-11-17** (not deployed): migration 0155 `etl_pacs_audit_opens`; nightly step
   `ETL_JOBS/etl_pacs_audit.py` after the sticky notes (one day per query through
   `AUDIT_EVENT_DATE_INX`, dates as text, never names/IDs/description; first night backfills
   from `settings.pacs_audit_since`, default 2026-10-01, at most 62 days a night); Report 37
   takes the first open or lock by a resident or radiologist (`utils/pacs_roles`) for every
   study the copy covers, the nightly snapshot for earlier ones. Tests:
   `tests/test_pacs_audit_etl.py`, `tests/test_report_37_audit_open.py`.
   **Deployed 2026-11-17** (`update.sh LAUMC`, migration 0155 applied). No grant needed:
   RAYD's PACS connection (`db_params.oracle_PACS`) signs in as **SYS**, which reads the
   audit. (SYS for an application is a risk to raise with the operator later: a read-only
   account with SELECT on the tables the ETL reads would be safer.) One night's read, the
   ETL's exact query for 16 Nov: 6,948 rows on 2,357 studies. **Next:** after the 05:00 run
   of 18 Nov (backfill from 1 Oct), check `etl_job_log` job `PACS_AUDIT_OPENS_ETL` (status,
   rows, duration), the settings `pacs_audit_copied_from/until` and the rows per kind; then
   regenerate October from Report 37's panel and spot-check a few studies on Oracle.
3. **Report 22: filtering on one or more AE titles shows 0.** Then stress-test every filter on
   every report so this bug does not come back.
   **AE part fixed 2026-11-14 (code + tests, no server needed).** Could not be reproduced
   with a mapped RH device; the code had three ways to show 0. (a) The shared AE dropdown
   (Reports 22, 25, 34, 36) listed every storing AE, but those reports keep only devices
   mapped to RH, so an unmapped or other-site AE was offered and always gave 0. It now offers
   only those devices, once each, in the mapping's spelling. (b) Reports 25 and 34 compared
   `UPPER(TRIM(storing_ae))` with the raw pick, and their other sections and Report 36 compared
   the raw `storing_ae`, so lower case or a trailing space gave 0. (c) Report 22 cleaned the
   pick with Python `strip()`, which disagrees with SQL `TRIM` on tabs. All now go through
   `utils/report_filters.picked_sql`. Tests: `tests/test_report_22_filters.py` (sums over
   every value offered) and `tests/test_ae_filter_reports.py`.
   **Other filters done 2026-11-14:** patient class, location and modality on Reports 25 and
   34 (main and secondary queries), the shared sidebar filter (Report 36, Report 25's
   sections), Report 35's modality, and the single-value filters of Reports 23, 31 and 32
   (class, modality, sex; they had UPPER without TRIM) all match whatever the case or
   padding. The modality dropdown no longer lists the other site's modalities (rule 0), and
   the Super Report's AE list only offers AEs it can count. Report 37 needed nothing: its
   dropdowns list the period's own values. Tests: `tests/test_report_filters_all.py`.
   Optional, on production: which AE titles were offered before and gave 0 (read-only):
   ```sql
   SELECT TRIM(s.storing_ae) AS ae, COUNT(*) AS studies, m.aetitle AS mapped_as, m.modality, m.site_id
   FROM rh.etl_didb_studies s
   LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
   WHERE TRIM(COALESCE(s.storing_ae, '')) <> ''
     AND (m.site_id IS DISTINCT FROM (SELECT id FROM sites WHERE is_default)
          OR TRIM(s.storing_ae) <> UPPER(TRIM(s.storing_ae)) OR s.storing_ae ~ '[\t\r\n]')
   GROUP BY 1, 3, 4, 5 ORDER BY 2 DESC;
   ```
4. **Room name from the RIS instead of AE title everywhere**, sidebar filters included. Mostly
   done in the reports, not everywhere; RIS descriptions may be empty. Every AE title should
   have a RIS room, and no room has several AE titles; any exception is a data error to fix on
   the RIS. Queries to validate: see Monday 2026-11-16 above. Found:
   `ETL_JOBS/etl_ris_modality.py` only adds new devices (`ON CONFLICT DO NOTHING`), so a room
   name corrected on the RIS never reaches RAYD. Decided: the RIS is the master for room name,
   room code, description, site and active flag, overwritten every night; RAYD-only fields
   (capacity, opening hours) untouched. The label typed in RAYD (floor plan,
   `display_aetitle`) is dropped, so the RIS name wins. Everywhere = every chart, table,
   sidebar filter and export, the Department board and Report Composer. Gateway AE titles
   (modality "PACS", no RIS room): validated by hand between the systems.
   **Slice 1 built 2026-11-14 (code + tests, not deployed):** `etl_ris_modality.py` now writes
   over room name, code, description, site, active flag from the RIS every night. Left
   alone: modality (0117's hand fixes), capacity, gateways/routers (modality `PACS`, so 0141
   holds). An empty RIS value or an org key with no site never blanks RAYD, and two RIS rows
   for one AE keep the active one. All three are logged as RIS data errors.
   `utils/ae_display` is the one label (RIS room name, else AE title, `display_aetitle`
   ignored). Report 22, the floor plan, the Department board, the daily briefing, the
   Super Report and the AE dropdown labels all use it. The Mapping page lost "Display As";
   a RIS device's description shows read-only (the API answers 409). Tests:
   `tests/test_ris_room_names.py`. Deploy after Monday's comparison, so RIS data errors are
   fixed first.
   **Slice 2 built 2026-11-14:** Report Composer (migration 0148: `device_name` and `room`
   are the RIS room name). Screens that showed the bare AE title now show the room name:
   Report 27 (AE mix chart; a Room column added to the exports beside AE Title), Report 29
   (storage by device, chart and table), Report 32 (radiologists by device), Report 34
   (utilization matrix, TAT chart), the AI report (device chips, chart title, summary), AI
   alerts, the capacity ladder. Checked and left: Reports 23, 31, 33, 35, 36, ER dashboard,
   financial, ORU analytics, referring intel (AE titles in joins only); the BRAYN console
   (admin tool, AE titles identify messages). **Left:** the AE sidebar filters show
   "Room (AE)", and stay keyed on the AE; Monday's comparison of RIS and RAYD names.
5. **ORU tab severity — on hold until the chief radiologist answers.** It is RAYD's own
   rule-based score (`nlp_worker/clustering.py`, `classify_report`), averaged per modality in
   `routes/oru_analytics.py`, with no published medical reference. It must follow a recognised
   standard.
6. **Report 27 hourly load:** one chart, blue = when orders were scheduled, red = when they
   were started (RIS status Started; exams, not reports). Each hour shows the average per day
   over the range. Busiest day counted on started, ranked Monday to Friday; Saturday (half
   day) and Sunday (ER only) shown separately.
   **Built 2026-11-14** (`tests/test_report_27_hourly.py`), not deployed. Check on the server
   that `worklist_status_history` holds `in_progress` rows, or the red line stays at 0. The
   "Busiest Day & Time" grid still counts scheduled orders.
   **Checked 2026-11-15 (census): `worklist_status_history` is empty on production** (0 rows:
   it is filled from HL7 orders, and none arrive). So the red line would be 0.
   **Fixed 2026-11-16** (operator: yes): "started" is the RIS performed step's start
   (`std_pps.start_datetime`), reached from any of the order's worklist rows (scheduled,
   arrivals, exam done), else the status history. On the local sample (production's 1–14
   October): started exams peak at 10:00–12:00 (about 18 an hour); busiest weekday Tuesday
   (203 a day). `tests/test_report_27_hourly.py`.
7. **Report 27 Order Status Mix:** double-check session on both ends (Oracle and RAYD) on
   Tuesday 2026-11-17, mainly the scheduled status. Compare at the same cut-off time (RAYD
   copies nightly, Oracle is live). Last tied out 2026-09-18, see
   `docs/LAUMC_REPORT27_STATUS_MIX_DIAGNOSTIC.md`.
8. **Super Report, residents: count reports by wet read**, rebuilt from PACS (see the rule
   above), tested and validated before deploy. Report events only (status, who signed, when);
   the report text on the ORU tab stays from the RIS. PACS `DIDB_STUDIES_VIEW` has
   `REP_WET_READ_SIGNED_BY/_TIMESTAMP`; the nightly pull reads `didb_studies`, which lacks
   them. Look deeper at the PACS report timestamps first (in July the final time was blank on
   recent studies the RIS showed signed; the operator suspects data migrated from the old
   PACS): pull example studies with blank sign times per AE title to see where they come
   from. A resident is anyone in the PACS "residents" security group.
9. **Staff who left.** Export the names of everyone in RIS and PACS (no RAYD accounts),
   grouped by PACS group and RIS resource role, for the manager, who sets leavers inactive on
   the RIS and PACS herself. Then, automatically: inactive on RIS and/or PACS → the person is
   flagged inactive and their KPIs (technician, radiologist, etc.) are no longer shown, in
   every period; department totals still include their studies. Having no PACS account does
   not count as inactive. Nothing is deleted from RAYD. Session on the PACS user columns
   Tuesday 2026-11-17.
   **Found 2026-11-17 (server):** PACS `SECM_USERS.STATUS` is ACTIVE on every staff account
   (only patient-portal accounts are SUSPENDED/TIMEOUT); `IS_EXPIRED` is the password and
   `LAST_LOGIN` is unused (directory sign-in), so PACS has no "left" signal yet. RIS `active` is
   kept (radiologists 52 inactive rows of 102, technologists 50 of 136), but per row: 32 of the
   52 inactive radiologist rows belong to someone with an active row, and two RIS person records
   can share one login. The RIS resource ID holds the PACS login
   (`first.last@ad.umcrh.com_123`): on the laptop sample it links 112/136 technologists, 86/102
   radiologists, 32/32 residents (e-mail alone: 105, 62, 32 on the server).
   **Built 2026-11-17 (migration 0157, not deployed):** `public.staff_status` (materialized,
   rebuilt after every nightly ETL): one row per RIS person (any role but DOC) and per PACS
   login no RIS person holds. Left = no active RIS row and no active person shares the login,
   or PACS accounts none of which is ACTIVE; no PACS account does not count. The nightly PACS
   copy now brings `STATUS` and `IN_STATE_SINCE`. While `settings.staff_left_hidden` = 'true'
   the rh views read "Former staff" for a leaver's login and name on studies and RIS staff rows,
   so every report (and Report Composer) shows one "Former staff" line instead of the people and
   every total keeps their studies; Report 32's turnaround card does the same in code. Report 37's
   case list keeps who opened and who wrote the note (a JCI record, tables outside the views).
   **Report 39, Staff status:** everyone by PACS group and RIS role, RIS and PACS status, how
   they are linked, studies signed and exams performed in the period, Excel for the manager, and
   the switch (Super user, CHT, Administrator). **Ships OFF.** Laptop sample: 480 listed, 42
   left (all "inactive on the RIS"), none of them signed a study in 1–14 Oct; one technologist
   inactive on the RIS (Tanios Jalkh) performed 99 exams up to 14 Oct, the sample's last day
   (the staff list is mid-November's, so he may have left since).
   **After the deploy:** open Report 39 for the **last 7 days**, filter Status = "Left, but
   worked in the period". Anyone there worked this week while inactive on the RIS or PACS: the
   flag is wrong for them (fix it on the RIS or PACS) or they left this week. Work weeks ago
   only means they left since. Then switch on in Report 39, or on the server:
   `sudo docker exec rayd_db psql -U etl_user -d etl_db -c "UPDATE settings SET value = 'true' WHERE key = 'staff_left_hidden';"`
   Not covered yet: SJH's own database has no rh views (as for placeholders), so leavers keep
   their names there once SJH moves (28 Nov). Referring doctors are not touched.
10. **Modality "PACS": share sample studies** to decide whether to remove it. These are the
    gateway and router AE titles seeded in `migrations/0083`.
11. **New global setting "report start date" = 2026-01-01**, editable on the ETL page, used as
    the starting value of every report's date filter (Report Composer and the Super Report
    included). Users can still pick earlier dates; it only saves them changing the date each
    time. The ER dashboard (last 30 days) and Report 37 (previous month) keep their own
    defaults.
    **Built 2026-11-14:** `settings.report_start_date` (migration 0149 seeds 2026-01-01),
    a card on the ETL Control Room to change it, `db.get_report_start_date()` read by every
    numbered report, the shared sidebar filter, the Super Report (it used to open on the last
    30 days) and Report Composer's "Fixed dates". Tests: `tests/test_report_start_date.py`.
    The ORU tab keeps its rolling last 30 days, as the ER dashboard does: say if it should
    follow too.
12. **Chart explanations (info icon on each chart) from the other branches**, written for
    LAUMC's own queries. Newest copy: Bekaa `static/js/chart-explanations.js` (2026-10-14).
    **Started 2026-11-14:** `static/js/chart-explanations.js` (the browser's own dialog, no
    CDN) and Report 22, all 15 charts. `tests/test_chart_explanations.py` fails when a page
    names a note with no text or does not load the script.
    **Done 2026-11-14:** every page now has its notes (about 150): Reports 22, 23, 25, 27, 29,
    31–36, the Super Report, the home page (Daily Briefing, Yesterday tab), the ER dashboard
    and the ORU tab. Report 31 and Report 34's shared panels reuse Report 25's notes (same
    query). Not deployed. Writing them turned up two things to fix, not fixed yet:
    - **Super Report → Daily Briefing card** (`ETL_JOBS/daily_analytics.py`, nightly) keeps
      structured reports and times TAT on the sparse PACS final signature. **Fixed
      2026-11-14, item 22.** (First written here as also reading every site: wrong, the
      scheduler runs it inside `rh_scope()` and `summary_storage_daily` is built from RH
      devices only; only a run by hand skipped the views.)
    - **Home → Yesterday → Orders ↔ PACS Reconciliation** groups on `etl_orders.proc_id`
      (the procedure code, not the accession) and counts groups as "Orders Placed", so Orders
      Placed always equals Expected Studies. **Fixed 2026-11-14, item 23.**
13. **Separate database for SJH, same web app** (confirmed; RBAC rejected, full database
    separation is safer). Replaces the RH-only rule (CLAUDE.md rule 0) once built.
    a. A dropdown on every report switches between the two databases (selected users only).
    b. Direct comparison only in the Super Report and the Live Department board, per modality
       (RH CT vs SJH CT, etc.).
    c. SJH from 2019-01-01, by migration: the ETL go-live is 2019-01-01 and the studies ETL
       still loads SJH (no site filter at the source), so every SJH row is already in the
       LAUMC database. Copy them to the SJH database, compare counts table by table, then
       delete them from the LAUMC database (approved by the operator 2026-11-14: "all my
       rules can be broken with customer needs"). After that the LAUMC ETL stops loading SJH
       at the source, and the SJH database gets its own nightly ETL; measure ETL run times to
       choose when it runs. Work that loads only Postgres runs any time; anything that loads
       Oracle runs only at weekends. CARD is always dropped, in the SJH database too.
    d. All HL7 messages carry the site ID (R2I includes it in every message); the operator
       will explain where, so BRAYN routes each message to the right database.
    e. AE mappings separate per database in RAYD; procedure codes shared.
    f. One set of RAYD logins; a user signs in once and switches site if allowed. The old
       roles (viewer, viewer2, tec, admin) are replaced by: Super user (R&D, everything),
       CHT (Cure Health Tech implementation staff: mapping, ETL, BRAYN console),
       Administrator (users and settings of the sites granted), User (pages granted). Each
       user is granted sites, then pages per site. Existing admins become Administrators,
       viewer/viewer2/tec become Users keeping their pages.
    g. One licence covering both sites. Two mail servers, one per site, each with its own
       domain.

    **13f built 2026-11-15** (roles and access, no server needed; migration 0152,
    `utils/roles.py`, `tests/test_roles_access.py`), not deployed:
    - The four roles replace the old ones. Code asks `can(user, capability)`, never a role
      name (CLAUDE.md rule 9); who holds what is one table in `utils/roles.py`:

      | Capability | Super user | CHT | Administrator | User |
      |---|---|---|---|---|
      | Every page and report of its sites | yes | yes | yes | only what is ticked |
      | Implementation: ETL Control Room (report start date, demo mode, triggers), DB Manager, Mapping edits, BRAYN console, HL7 Forward and field map, Floor Plan, resource monitor, NLP runs | yes | yes | no | no |
      | Settings: Email Server, CRN set-up and actions, financial config, report targets (Reports 35, 37), Report Composer SQL, schedules and shared reports, ORU keywords and vocabulary, AI storage capacity | yes | yes | yes | no |
      | Users: User Management, permission groups, Activity Log | every account | no | Users and Administrators of its own sites | no |
      | Sites | all | granted | granted | granted |
    - Sites, then pages and reports per site: `user_sites`, and a `site_id` on
      `user_page_permissions` and `report_access_control`. Until the SJH database exists the
      site served is RH: an account without RH cannot sign in. Grants for SJH can be
      ticked already; they take effect with item 13a.
    - Migration 0152: admin → Administrator; viewer, viewer2, tec, finance, secretary → User
      with their pages written out as they were in effect (role defaults, plus Mapping and
      the CRN board that viewer/viewer2 had by role, AI Reports for viewer) and every report
      the Viewer Dashboard would have granted them (tec: none, as before). Every account gets
      RH. The built-in **admin** account becomes the Super user, so the ETL Control Room,
      Mapping and the BRAYN console stay reachable after deploy.
    - The Viewer Dashboard no longer grants every report to everyone on each visit: a new
      report is granted by its migration or ticked by an administrator.
    - User Management: role, sites, and pages and reports per site in the drawer (Tick all /
      Untick all); "Add user" creates an active account with a temporary password; approving a
      registration needs a role and a site. An Administrator cannot touch CHT or Super user
      accounts, nor give those roles, nor grant a site it does not hold. Nobody changes their
      own account there. Every change is audited and signs the account out.
    - Dropped: the tec landing on HL7 Orders (every User lands on the Viewer Dashboard), the
      hidden Help for tec. Small widenings, each tied to a page now: live-feed revert for
      whoever opens the live page.
    **After deploy:** sign in as `admin` (Super user), set CHT accounts for CHT staff (they
    are Administrators after the migration), check one former viewer and one former tec.

    **13a, first part built 2026-11-15** (no server needed; `utils/site_db.py`, migration
    0153, `tests/test_site_databases.py`), not deployed. Off until `POSTGRES_DB_SJH` is set:
    - SJH's own database on the same Postgres server (`POSTGRES_DB_SJH=etl_db_sjh` in .env,
      passed by docker-compose). At startup the app creates it, loads the schema, runs every
      migration and the startup columns, records that it serves SJH, and copies the user
      list into it (no passwords). Checked by starting the real app on two scratch databases.
    - Per request: the header shows a site dropdown to accounts holding RH and SJH; every
      page and report then reads the picked site's database. Logins, sites, page and report
      grants, sessions and the audit log always stay on RH's database. An account with SJH
      only lands on SJH.
    - Reports keep "this site's devices" with `rayd_served_site_id()` (each database knows
      the site it serves) instead of the default site, so the same reports work on both;
      Report 23's '%SJH%' name guards apply on RH's database only.
    - Cached figures and dropdown values are kept per site. Report Composer runs as each
      database's own reporting role.
    - Found and fixed on the way: Reports 22, 23 and 31's CSV exports streamed outside their
      request, so they skipped the RH-only views (SJH rows could reach an RH export).
    - Per site by construction (each database has its own): AE mapping, settings (the Email
      Server page edits the picked site's mail server), Report Composer's saved reports,
      BRAYN tables, CRN set-up.
    **13a, second part built 2026-11-15** (`tests/test_site_databases.py`), not deployed:
    - Scheduled work per site (`site_db.run_per_site`): Report Composer's scheduled emails
      and Report 37's monthly workbook run on each site's database, through that site's
      mail server and recipients; the 05:30 Daily Briefing snapshot and the query-log purge
      too. A failure on one site does not stop the other.
    - Procedure codes shared (e.): the six procedure tables live on RH's database. Every
      write goes there (the Mapping page's procedure edits, whatever site is picked; the
      ETL), and each site database keeps an identical copy: copied right after a Mapping
      change, checked every 15 minutes for the ETL's changes (a checksum, copied only when
      it differs). AE mappings stay per site.
    - Side by side per modality (b.): the Super Report shows "Sites side by side, per
      modality" (studies, patients, median TAT, GB) and the Live Department board a
      "Sites" panel (busy devices, waiting, done today, utilization), each site read from
      its own database with the same definitions. Only for accounts holding both sites.
    **Left in 13a:**
    - BRAYN routing each HL7 message to its site's database (d.): needs the operator's
      walkthrough of where the site ID sits in each message. Until then HL7 data (BRAYN,
      the Live board, CRN) stays on RH's database, so SJH's board is empty.
    - On SJH the ETL Control Room shows SJH's (empty) job history, but its triggers still run
      RH's ETL: SJH's own ETL comes with 13b.

    **13b design, draft for the operator's review (2026-11-17).** One Python job reads RH's
    database and writes SJH's (two connections on the same server; Postgres has no
    cross-database query): for each table, in an order the foreign keys allow,
    `COPY (SELECT … WHERE <SJH rule>) TO STDOUT` into SJH's database (through a staging
    table, `ON CONFLICT DO NOTHING`, so a rerun is harmless), then counts both sides. A dry
    run only counts (the census). Deleting from RH is a separate step, refused unless every
    count matched, children before parents, in batches, then `VACUUM ANALYZE`.

    | Data | SJH rule | In SJH's DB | In RH's DB after |
    |---|---|---|---|
    | studies, series | `site_id` = SJH (CARD never: Phase 2c) | copied | deleted |
    | raw images, image locations | through the study | **not copied**: no page reads them, only the storage rollup | deleted with the study |
    | daily storage summary | SJH devices | copied | deleted |
    | sticky notes, access history, audit opens, CD log, exam facts | through the study | copied | deleted |
    | patients | with an SJH study | copied | deleted unless also seen at RH (7,085 stay) |
    | RIS orders, scheduled/arrivals/exam done/cancellations, status history | issuer SAP_SJH | copied | deleted |
    | RIS reports | accession of an SJH order | copied | deleted |
    | PPS (+ person reference, site extension) | SJH device or SJH study | copied | deleted |
    | ORU reports (+ NLP, rule cache) | accession of an SJH order or study | copied | deleted (stops if a CRN row points at one) |
    | RIS visits, RIS patients | SJH site / SJH orders | copied | deleted |
    | devices and their schedules, exceptions | `site_id` = SJH | copied | kept (the site resolution needs them) |
    | RIS staff, reference catalogs | all | copied | kept |
    | studies and ORU with no site (9,221 / 20,706) | — | — | **stay on RH (proposed)** |
    | HL7 orders, BRAYN tables, HL7 study events | — | — | stay on RH until 13d |
    | daily briefing snapshots | — | rebuilt per site | kept |

    **After the move (decision): (A, proposed)** RH's nightly ETL keeps its one PACS/RIS
    pull for both sites, then the same job, Postgres only, moves the night's new SJH rows to
    SJH's database: no extra load on PACS, nothing new to schedule. **(B)** Two ETLs filtered
    at the source, SJH's reading Oracle again (the earlier plan): twice the PACS reads, and a
    site filter in every Oracle query, which some cannot have (a study's site is resolved
    from its device after the load).
    **To decide:** (1) no-site rows stay on RH; (2) A or B; (3) SJH's raw images not copied.
    Then: build and rehearse on the local sample, deploy Thu 26, production move Sat 28.
    **Decided 2026-11-17 (operator):** (1) yes, and they stay flagged (site still empty, every
    run reports how many remain); (2) **A**: the ETL decides where each row belongs and
    stores SJH's rows in SJH's database; (3) yes: locally the databases are filled from the
    sample, on production by the ETL.
    **Engineering notes (2026-11-17):**
    - Rows are matched on their natural keys, never on generated ids: every RIS table is
      upserted on one (`site_worklist_key`+time, `report_key`+`version`, `visit_key`,
      `patient_person_key`, `resource_id_key`, ORU `accession_number`, ...). Surrogate ids
      are not copied (SJH's database gets its own writes after 13d, so ids would collide);
      ORU children (NLP analysis, AI cache, rule cache) find their report by accession.
    - Parents are upserted, never deleted and re-inserted in SJH's database: a cascade
      would drop children that RH no longer holds.
    - The studies ETL re-reads the last 10 days every night (`study_db_uid > max` or the
      lookback), so recent SJH studies come back to RH nightly: the nightly step updates
      them in SJH's database and removes them from RH again (about 1,000 studies a night).
      Images load on a watermark, so only new images come.
    - The nightly step runs at the very end of the ETL, so the series, RIS rows and ORU of
      later phases go with their study.
    - RH's storage rollup (Phase 7) already leaves SJH's devices out, so there is no SJH
      storage summary to copy: the one-time move computes SJH's history once from SJH's
      images (same formula), before deleting them; afterwards SJH's own rollup builds the
      new days from the images the nightly step moves. The rollup (and any other "this
      site" code in the ETL) must use the site its database serves (rule 10).
    - Key sets are taken once at the start (SJH studies, orders, accessions, PPS keys), so
      copying and deleting see the same rows whatever order the tables go in.

    **13b part 1 built 2026-11-17** (not deployed; `tests/test_site_route.py`, 12 tests):
    - `ETL_JOBS/etl_site_route.py`: the table list above as code (rule, natural key, what
      happens on RH), `route_site()` (count / copy and verify / delete only if every table
      verified; studies deleted 2,000 at a time, series and images by cascade; ORU kept on
      RH while a CRN row points at one; a PPS on an SJH device for an RH study keeps no
      link). Command line for the one-time move, refused while the ETL runs:
      `python -m ETL_JOBS.etl_site_route --site SJH --dry-run | --copy | --move`.
    - Nightly: last step of the ETL (`_route_sites` in `etl_runner.py`), with the night's
      images, logged as job `SITE_ROUTE_SJH` (rows moved, no-site counts). Off until
      `POSTGRES_DB_SJH` is set and the one-time `--move` has recorded itself
      (`settings.site_route_moved_SJH`), so it can never copy the image history.
    - **Rehearsed on the local sample** (1–14 Oct): dry run, copy (every table verified),
      move. After: RH's database 2,445 studies (2,393 RH + 52 no site), 0 SJH; SJH's 1,870
      studies, 2,800 orders, 1,813 ORU reports, 1,218 patients (15 seen at both sites kept
      on RH). Report 22 on RH still 2,363, Report 38 still 346; Report 22 on SJH 1,870; the
      Super Report side by side RH 2,363 | SJH 1,870.
    **13b part 2 built 2026-11-17** (storage; `tests/test_site_route.py`, 14 tests): the
    one-time move computes SJH's daily storage from all of SJH's images on RH (the
    rollup's formula, SJH's devices, from go-live) into SJH's `summary_storage_daily`,
    before deleting them, and copies the images of the last 14 days of studies only
    (`--image-days`), so a late image recomputes a complete day. `go_live_config` is copied.
    The storage rollup now counts the site its database serves (`rayd_served_site_id()`),
    and the ETL runs SJH's right after the nightly route, on SJH's database. (The local
    sample has no images: covered by the tests only.)
    **Production runbook (Sat 28), on the server:**
    1. In `/opt/rayd/.env` set `POSTGRES_DB_SJH=etl_db_sjh`, then
       `cd /opt/rayd && sudo docker compose up -d rayd-app`: the app creates, loads and
       migrates SJH's database (`sudo docker logs rayd_service 2>&1 | grep "site SJH"` shows
       "site SJH: database etl_db_sjh ready").
    2. `sudo docker exec rayd_service python -m ETL_JOBS.etl_site_route --site SJH --dry-run`
       and compare with the census (182,207 studies on 2026-11-15, plus what came since).
    3. `... --copy`: every table verified; check SJH's reports with an account holding both
       sites (Report 22, the Super Report side by side).
    4. `... --move`: about 182,000 studies in 92 batches, series and images by cascade
       (allow 1–2 hours); it records itself, and the nightly ETL takes over.
    5. `sudo docker exec rayd_db psql -U etl_user -d etl_db -c "VACUUM ANALYZE;"`, then
       check RH's reports unchanged (Report 22, 38, Super Report) against Friday's figures.
    Never while the ETL runs (the job refuses); not before step 3 is checked.
14. **Radiologists TAT and radiologists' number of reports: revamp and validate thoroughly;
    this is the selling point.** Source is PACS Oracle (rule above). Show both start clocks,
    RIS exam done and PACS insert time (`DIDB_STUDIES.INSERT_TIME`), because they disagree.
    Keep the per-patient-class targets as documented (migration 0121: inpatient 24 h, ER 24 h,
    outpatient 48 h) and check their numbers. Report 32 becomes the single place for
    radiologist TAT and report counts; the duplicates in Reports 25 and 36 are removed.
    Validate on random dates against Oracle. Breakdowns the hospital asked for in July:
    1. Exam done → wet read / Signed 1, per resident, per modality, per patient class
       (including ER), bucketed into adjustable threshold windows (e.g. 0-3 h, 3-5 h;
       configurable, not hardcoded).
    2. The same breakdown for the approved/final stage.
    3. Report count per radiologist, per day, per modality.

    Report 32 is Radiologists Performance; Resident vs Radiologist TAT is in Report 36.

    **14A and 14B (operator, 2026-11-17), in Report 32:**
    - **A. Two TAT charts:** (1) RIS exam done → signature; (2) PACS arrival
      (`insert_time`) → signature. Both start clocks are in Postgres: exam done exists for
      97% of RH studies (2,363 of 2,435, production's 1–14 October), arrival for all.
    - **B. Reporting time:** first open → signature. Shown twice: from the first reader's
      open (the whole read, resident included) and from the signing radiologist's own first
      open (the radiologist's part). Possible extra: time actually spent in the study, the
      sum of the audit's LOCKED → UNLOCKED sessions.
    - **Signature from PACS** (operator rule): the audit trail has every approve/sign event
      with its login and second ("ApprovedSign Complete", report status tag `(07a1,2a)`:
      WETREAD, PENDING APPROVED, APPROVED), better than `DIDB_STUDIES_VIEW`, which keeps
      only the latest value per study. Extend item 2's nightly copy with these events
      (login taken from the tags when the row has none), never the description itself.
    - **Proof query run 2026-11-17** (Vue PACS Client, Monday 16 Nov, both sites): status →
      PENDING APPROVED 469 rows / 458 studies / 13 logins, never without a login;
      ApprovedSign Started 386 / 376; ApprovedSign Complete 342 / 332 (13 logins) plus
      "ApprovedSign Completed" 46 / 46 (one login, whose studies then show PROCESSING
      APPROVED and APPROVED with no login); WETREAD 421 / 400 / 12 logins, 273 rows without
      a login (the login is in the tags `(07a1,42)`/`(07a1,d6)`); UNREAD 43; PRELIM and
      PreliminarySign under 15. No FINAL status: at LAUMC approved is the final signature.
      Approving saves the report pending for 900 s (grace period) before APPROVED.
      **Signature = the radiologist's approve: the first status → PENDING APPROVED (or
      ApprovedSign Complete/Completed) with its login**, proposed; it covers 80 more
      studies than the ApprovedSign rows. **Tied out 2026-11-17:** `DIDB_STUDIES_VIEW`
      shows 455 studies approved on 16 Nov by 13 radiologists, against 458 studies and 13
      logins in the audit (the view keeps only each study's latest approval).
    - **Built 2026-11-17** (not deployed; signature = the approve click, operator "let's do
      next"): migration 0156 renames the copy `etl_pacs_audit_events` and allows `approve`
      and `wetread`; the nightly audit step copies them (login from the tags when the row
      has none; the description is read only inside Oracle) and, being a new version,
      re-reads from 1 October once; `utils/sign_tat.py` (four clocks, median and p90 overall
      and per signing radiologist, only studies the copy covers); Report 32's new card
      "Signature Turnaround (PACS audit trail)": tiles, chart 1 RIS exam done → signature,
      chart 2 images reach PACS → signature (14A), chart 3 first open → signature from the
      first reader's open and from the signer's own (14B). Tests: `tests/test_sign_tat.py`,
      `tests/test_pacs_audit_etl.py`. Report 37 reads opens and locks only.
      **Checked on Oracle 2026-11-17** (the ETL's query for 16 Nov, per kind): approve 857
      rows / 459 studies / 13 logins (= 469 PENDING APPROVED + 342 + 46 ApprovedSign rows);
      wetread 421 / 400 / 16 logins, none without a login (the 273 rows with no user get it
      from the tags); open 5,483 + lock 1,465 = 6,948, the timing count. Open's 131 logins
      include clinicians on Vue Motion and the HIS account; the reports keep readers only.
      **Next:** deploy (with item 2's check on Wed 18); after one night, spot-check a few
      radiologists' medians on Report 32 against the audit.
    - **Plan:** Wed 18 – Thu 19, the Oracle proof query (which sign events exist, how many);
      after item 2 is live, build Mon 23 – Tue 24, validate against Oracle on random dates
      Wed 25, deploy Thu 26.

## Earlier items, still open

- **Urgent emergent in HL7.** Check with the end user and HIS whether urgent emergent can be
  sent in the HL7 messages.
- **Shared filter panel (item 16).** Move Reports 22, 25, 34, 35, 36 and the Super Report onto
  `templates/_report_filter_panel.html`, one at a time, each tested (CLAUDE.md rule 8).
  **Done 2026-11-14**, not deployed (`tests/test_report_filter_panel.py`). Fixed on the way:
  unticked filters came back ticked; the Excel/CSV downloads of Reports 22, 25, 34 and 35
  could not work in production (a GET to a POST-only route, or a POST without the CSRF
  token); Report 34 took one value per filter. The Super Report's dropdowns now take several
  values each and have a tick; its Protocol Name and Body Part (Series) filters were dropped,
  because the query never applied them. Check on the server after deploy: each download, and
  a saved Super Report preset loading.
- **R2I real-time feed and BRAYN — both switched on the week of 2026-11-16.** When live: the
  Department board gets "waiting" and "in progress", BRAYN's RIS-dependent checks
  (ORPHAN_EVENT, SKIPPED_RUNG, ORPHAN_ORDER, STALLED_*, seeded off) can be turned on, and HL7
  orders arrive on time. Check each of these once the feed runs.
- **Only final ORU reports should count — to be filtered by BRAYN.** Keep open; no code change
  for now. `FAP` (OBR-25 and OBX-11) is the vendor's result status, and its meaning, or a
  preliminary sample to compare with, is still missing. BRAYN only observes today (CLAUDE.md
  rule 7), so this needs BRAYN switched on and a decision on how reports read its result.
- **Placeholder records — to be filtered by BRAYN.** "Referring, Generic"
  (`4101031379966`/`...68`), test patients and the CSH service account are loaded faithfully
  and not yet excluded from any report. Same BRAYN note as above.
  **Done 2026-11-15**, not deployed (migration 0151, `tests/test_placeholders.py`). BRAYN keeps
  the list (`brayn_placeholders`, console Settings → Placeholder records, admin only), seeded
  with both "Referring, Generic" codes and the CSH account (9271030205716). BRAYN never sees
  what the Oracle ETL loads, so the list is applied in the `rh` views every page reads, the way
  RH-only works; nothing is deleted:
  - a test patient (listed by patient ID, RIS or PACS, never by name) is left out of every
    page: studies, RIS orders, steps, status history, RIS reports and ORU reports;
  - a placeholder referrer's exams stay in every count, with the referrer blank, so it leads
    no referrer list (matched by "Last, First" on PACS studies, by code on HL7). CRN texts the
    fallback contact straight away and records why;
  - a service account is left out of `std_resources_ris`, so of every staff list and the
    Report Composer.
  BRAYN names a listed patient, referrer or account on any HL7 message (`PLACEHOLDER_DATA`).
  Keeping such messages out of RAYD's tables comes with the BRAYN gate, per message type.
  Test patients are not listed yet: their IDs come from Monday's queries (see the Monday
  section). Cost: the views add roughly 0.15 µs per study scanned, and blanking the referrer
  about 0.5 µs (about +45 ms and +150 ms on a cold query over a year of studies, measured on
  a test database).

Numbered 15–21 in the estimates above, in this order: urgent emergent, shared filter panel,
R2I and BRAYN checks, ORU final-only, placeholders, CRN go-live, patient schedule lead time.

## Added 2026-11-14

22. **Daily Briefing snapshot, RH only.** See item 12's note: `ETL_JOBS/daily_analytics.py`
    keeps SR and times TAT on the PACS final signature.
    **Done 2026-11-14**, not deployed (`tests/test_daily_briefing.py`): structured reports,
    other objects and bone densitometry left out of every figure (modality from the device
    first); TAT from the PACS arrival to `COALESCE(rep_study_last_composed_ts,
    rep_final_timestamp)`; a run by hand (`python ETL_JOBS/daily_analytics.py`) now goes
    through the `rh` views too (`run_rh_only`). The site was already RH in the scheduled run.
    Left as it is: a gateway AE mapped to RH would carry both sites' images into
    `summary_storage_daily`; check on the server whether any gateway is mapped to a site.
    After deploy the next 05:30 snapshot shows the new figures; old snapshots age out in
    30 days.
23. **Yesterday tab, Orders ↔ PACS reconciliation.** See item 12's note: groups on
    `proc_id`; redo with Report 27's linked-group method, RH only, then give it its icon.
    **Done 2026-11-14**, not deployed (`tests/test_yesterday_reconciliation.py`): yesterday's
    RIS exams that reached Exam Done or later (status CM), grouped by `etl_orders.linked_id`
    (a step with none is its own group) = expected studies, against the PACS studies of the
    day. Orders still Scheduled or cancelled no longer count. The orders were already RH only
    (the `rh.etl_orders` view). "Orders Placed" is now "RIS Exams Done"; the panel has its
    info icon.
24. **Deploy the 2026-11-14 and 2026-11-15 work** (items 3, 4, 6, 11, 12, 16, 19, 22, 23,
    25, 13f, 13a; migrations 0148–0153) after item 4's Monday room-name check.
    Leave `POSTGRES_DB_SJH` unset until SJH's data is moved (13b). Then add Monday's
    test patients on the BRAYN console and check each placeholder record's "Reaches" count,
    and do item 13f's after-deploy steps (CHT accounts, one former viewer and tec).
    **2026-11-15: production is already at migration 0153** (the sample's manifest), so the
    code looks deployed; to confirm with the operator, then do the after-deploy steps above.
    **Still to deploy (2026-11-16):** migration 0154 (CRN hospital name), CRN email to SMS,
    item 6's "started" fix. A `git pull` and rebuild brings them all.
25. **ER dashboard onto the one report UI** (CLAUDE.md rule 8). The ORU tab was taken out of
    scope (operator, 2026-11-14) and stays the standalone page `/oru`.
    **Done 2026-11-14**, not deployed: the ER dashboard is Report 38 (`routes/report_38.py`,
    `templates/report_38.html`, migration 0150: report row, licence entry unless the licence
    switched `er_dashboard` off, access for every role but tec as the page had it). Opens at
    `/viewer/38`, `/er` redirects; the home page card is a report card shown to whoever has
    access. The shared left panel: dates (last 30 days by default), the reporting target in
    hours, and a modality filter (new). Tests: `tests/test_report_38.py`. After deploy: run
    migration 0150, then check the home card and a technologist account (no access).

## Added 2026-11-15

26. **How much SJH data RH's Postgres holds.** `scripts/sql/sjh_census.sql`, read-only:
    SJH studies (by site, per year), their series and patients, RIS orders by SJH's issuer
    and the steps, status history and reports hanging off them, PPS, ORU reports and their
    NLP rows, devices per site, and every table with a `site_id` column. SJH rows are
    recognised the way the RH-only views hide them, so the counts are what 13b will move.
    Checked on a migrated LAUMC database.
    **Run on production 2026-11-15.** RH's Postgres holds about a third of everything for SJH:

    | Data | SJH | Total | Note |
    |---|---|---|---|
    | PACS studies | 182,207 | 561,342 | 2021-04-13 to today (not 2019: SJH starts April 2021); 9,221 studies have no site |
    | series | 975,840 | 2,614,739 | images not counted (heavy) |
    | patients | 50,781 SJH only; 7,085 at both sites | 157,630 | the 7,085 are needed in both databases |
    | RIS orders | 294,907 | 917,496 | by SJH's issuer; `etl_orders.site_id` is never filled |
    | RIS steps | scheduled 272,114, arrivals 223,554, exam done 202,699, cancellations 17,826 | | |
    | RIS reports | 335,642 | 1,062,880 | `std_reports.site_id` never filled |
    | PPS | 198,961 | 610,525 | |
    | ORU reports, NLP rows | 160,228 each | 507,344 | its own `site_id` says 155,479; 20,706 have no site |
    | devices | 12 (1 gateway) | 90 | 73 RH, 5 no site |
    | RIS staff | 169 | 1,575 | 216 RH, 1,190 no site |

    Per year: 5,523 (2021), 21,510, 32,445, 39,664, 42,689, 40,376 (2026 so far). The PACS
    label says 191,705 SJH studies, 9,498 more than the resolved site: the double-read RH
    mammography stamped '1' (rule 0).
    **For 13b, to decide with the operator:** where the 9,221 studies and 20,706 ORU reports
    with no site go (today the RH views count them as RH); patients seen at both sites are
    copied, not moved; orders, RIS reports and visits are told apart by the RIS issuer, since
    their `site_id` columns are empty. Also found: `worklist_status_history` is empty on
    production (item 6, fixed).
**Users per site (operator, 2026-11-15)** — already possible since 13f/13a, checked on the
local stack with real accounts: (A) an RH user = role User, site RH: reads RH only, no site
dropdown, SJH refused; (B) an SJH user = User, site SJH: reads SJH's database only (signs in
once SJH has its database, after 13b); (C) a chief radiologist for both sites = Administrator
(or User) with RH and SJH: the header's site dropdown and the side-by-side views. Created in
Administration > User Management > Add user: role, then the site boxes; a User then gets its
pages and reports ticked per site in its drawer.

27. **Patient names in Postgres.** Four tables have a `patient_name` column (`hl7_orders`,
    `hl7_scn_studies`, `hl7_patients`, `cd_print_log`), and `hl7_orders.raw_message` and
    BRAYN's `hl7_message_archive` keep whole HL7 messages. **Operator, 2026-11-15: the HL7
    tables are the only ones allowed to hold names.** `cd_print_log` counted 2026-11-16: no
    name. **Done.** The sample export (item 28) still never takes names, HL7 tables included.
28. **Local LAUMC stack and sample data.** **Done 2026-11-15** (`scripts/local_stack.md`):
    the laptop's Docker runs the LAUMC branch with two databases (`etl_db`, `etl_db_sjh`),
    the PACS connection pointed at `oracle-disabled.invalid` so its start-up ETL can never
    reach the hospital PACS. `scripts/sample_data/export_sample.py` takes a date window
    from production read-only (by default patient IDs, accessions and UIDs hashed, birth
    dates cut to the year, report text dropped; never names, phones or raw HL7);
    `import_sample.py` loads it into a local stack only. With a sample, 13b's move can be
    rehearsed on the laptop first. Found on the way: the site lookup recursed ~1000 levels
    on every reload once a site database existed (fixed, with tests).
    **Loaded 2026-11-15:** production's 1–14 October, pseudonymised (4,492 studies, 6,508
    orders, 8,052 RIS reports, 4,416 ORU reports, 22,835 series, reference tables). Locally:
    2,393 RH studies, 1,870 SJH, 52 with no site; Report 22 shows 2,363 RH studies for the
    two weeks, Report 38 346 ER studies, SJH nothing until 13b moves its rows. The first
    export hung on `hl7_oru_reports` (fixed). The sample showed production's settings: BRAYN
    is on (`brayn_enabled`), CRN's hospital name reads **"Mazloum Hospital"** (item 20), and
    production is at migration 0153 (item 24).
29. **Cardiology purge not running on production.** ETL Phase 2c deletes the CARD-family
    studies (`CARD`, `SJH_CARD`, `CARDUS`, `SJHCARD`) of the two shared gateways
    (`SJHCSAPWFMFIR`, `LAUMCWFM2FIR`) every night. The October 1–14 sample still held 177
    of them, all `SJH_CARD`, SJH, last loaded 11–24 October, and the local ETL purged them
    at once. So on production 2c has not run since at least 24 October.
    **Cause, 2026-11-16:** production's `.env` has
    `RAYD_ETL_PHASES=1,2,2b,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19` (left from a phased
    backfill): 2c is not in it ("Phase 2c ... not in RAYD_ETL_PHASES, skipped" at 05:00), nor
    18 (RIS device schedules, read by the capacity ladder, the AI report and the Live board:
    schedule changes on the RIS have stopped reaching RAYD) nor 20 (`fact_exam`, read by no
    page). **Fix:** add 2c and 18, recreate the app container (commands below); the next
    05:00 run purges the CARD studies, as migration 0111 and 2c always did.
    **Counted on production 2026-11-16:** 1,036 CARD gateway studies, study dates
    2026-07-28 to 2026-11-13, so 2c has been off since about late July and new ones still
    arrive every day. The `.env` change was made the same day; recreating the container
    starts no ETL (that happens only when a table is empty), so 2c first runs at 05:00 on
    2026-11-17. **Done:** counted 0 on 2026-11-17 after that run, and the deploy the same
    day kept `2c` and `18` in the phase list. They were SJH rows
    (RH reports never showed them), but they inflate SJH's census and 13b must not move them
    (CARD is always dropped, 13c). **On the server:** first count what 2c will delete
    (read-only, through psql in the database container), then change the line and recreate
    the container (`docker restart` does not re-read `.env`):
    ```bash
    sudo docker exec rayd_db psql -U etl_user -d etl_db -c "SELECT COUNT(*), MIN(study_date), MAX(study_date) FROM etl_didb_studies WHERE UPPER(TRIM(storing_ae)) IN ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR') AND UPPER(TRIM(study_modality)) IN ('CARD', 'SJH_CARD', 'CARDUS', 'SJHCARD');"
    ```
    ```bash
    sudo sed -i 's/^RAYD_ETL_PHASES=.*/RAYD_ETL_PHASES=1,2,2b,2c,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19/' /opt/rayd/.env
    grep RAYD_ETL_PHASES /opt/rayd/.env
    cd /opt/rayd && sudo docker compose up -d rayd-app
    ```
    Next morning: `sudo docker logs rayd_service 2>&1 | grep -E "Phase (2c|18)" | tail -5`.

## CRN go-live — this month (operator, 2026-11-15)

CRN is off (`crn_live_since` empty, provider "log", nothing sent). **Built 2026-11-15 and 2026-11-16**
(`tests/test_crn_sms.py`), not deployed:
- **LAUMC sends by email to SMS** (operator, 2026-11-16): one email per mobile to the
  hospital's email-to-SMS gateway, through the site's Email Server (Administration > Email
  Server, Microsoft 365 included). Admin > CRN > Settings, provider "Email to SMS": the
  gateway address, with the mobile as {number} (70123456), {intl} (96170123456) or {e164}
  (+96170123456), and an optional subject (for a fixed address that reads the number
  there). The gateway's domain must be among the Email Server's allowed domains; saving
  checks all of it, and the readiness panel shows where a sample mobile would be emailed.
- **e-tec SMS** (e-tecsms.com), ported from the Mazloum branch where CHT runs it, stays
  available but LAUMC does not use it: provider, username, password (stored encrypted),
  sender name; one POST per batch of up to 50 numbers. Another provider plugs in at
  `utils/crn_sms.send_sms`.
- **The hospital name** on the page a doctor opens was "Mazloum Hospital" (migration 0132's
  seed, ported from Mazloum). Migration 0154 sets each database's own site name: "LAUMC -
  Rizk Hospital", and "LAUMC - Saint John" in SJH's database. A typed name stays.

**To go live (in order):**
1. Deploy (git pull, rebuild; migration 0154 runs).
2. The email-to-SMS gateway is LAUMC's own, already in use by another application (operator,
   2026-11-16: no question to ask IT about it). Set up the Email Server (Microsoft 365:
   `docs/LAUMC_O365_EMAIL_SETUP.md`) with the gateway's domain in its allowed domains, then
   provider "Email to SMS" and the gateway address on Admin > CRN > Settings.
3. The CRN gateway (the page a doctor opens from the SMS): its address
   (`crn_public_base_url`), `crn_gateway_url` and API key, and IT's firewall rules
   (`docs/LAUMC_CRN_FIREWALL_REQUEST.md`).
4. The fallback contact (who is texted when the referring doctor cannot be reached) and
   the doctors' mobile numbers (Admin > CRN > Contacts; the RIS sync fills them nightly).
5. SMS wording signed off (only `{ref}` and `{link}`; never patient data).
6. A test SMS to a real phone (Settings > Test), then switch CRN on (Settings > Live).
The readiness panel on Admin > CRN > Settings shows each of these as ok / missing.

## Low priority


## Very low priority — later, as a goodwill gesture (added 2026-11-14)

- **Patient schedule lead time.** The time between when the patient booked the exam and the
  date and time the exam was booked for, with every breakdown the data allows: modality,
  procedure, room, patient class, ordering department, referring physician, priority, age
  band and sex, weekday and hour of booking, same-day vs advance bookings, and how many were
  later cancelled or rescheduled. The RIS orders pull already copies the exam's
  `SCHEDULED_DATE`, but not when the booking was made (`ORDERS.CREATED_ON_DATE` is only used
  as a filter). Decide with the operator which moment counts as "booked": the order's
  creation, or the time the step first got a Scheduled status in the RIS status history
  (Phase 17 already reads that history for arrivals).

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
  account → exclusion list, separate from NULL handling: BRAYN's `brayn_placeholders`,
  applied by the `rh` views (migration 0151, item 19).
- Backfill: **never** row-level the 166M image rows — aggregate at source to ~614k/study
  (`LAUMC_DATA_REQUEST.md` §B5) if the full Phase 3/4 row-level sync ever proves too slow
  or unnecessary.
- Site vocabularies may change at the ~2027 RIS/PACS upgrade → only `sites` / `site_org_map`
  rows change, never code.
- **NO PATIENT NAMES in Postgres, ever** (operator instruction, 2026-07-27), **except the
  HL7 tables** (`hl7_*`, BRAYN's message archive: operator, 2026-11-15). Staff/resource
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
- `update.sh` is the tool for pushing fixes to the live LAUMC install
  (`cd /opt/rayd && sudo bash update.sh LAUMC`). `install.sh` is fresh-install / data-reset
  only — do not use it on the live site.
