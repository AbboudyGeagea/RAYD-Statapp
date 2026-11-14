# LAUMC — To do

Updated 2026-11-13. This file holds only what is still open. What was built is in the commit
messages, and the July version of this file (history, punch list round 2) is in git history.
Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor questions), `LAUMC_DATA_REQUEST.md` (discovery record).

---

## Deadline and sessions

- **Deadline for this list: 2026-11-30.** Item 2 (first open) has its own deadline, 2026-11-20.
- **Monday 2026-11-16, 10:00: run the two room-name queries (item 4)** and compare the RIS
  room names with the labels RAYD shows. Then a working session on the PACS audit table
  (item 2). Sessions start at 9:30.

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
| 2 | First open from PACS audit (due 2026-11-20) | High | 1–2 | 2–3 | 0.5–1 | 3.5–6 | audit table logs opens; JCI definition |
| 3 | Report 22 AE filter + stress test of all filters — **done** | Medium | 0.5 | 3–4 | 0.5 | 4–5 | — |
| 4 | RIS room names everywhere — **built** | Medium | 0.5 | 3–4 | 0.5–1 | 4–5.5 | RIS data fixed after the Monday queries |
| 5 | ORU severity on a standard | Medium | 1–2 | 2–3 | 1–2 | 4–7 | chief radiologist |
| 6 | Report 27 hourly load, scheduled vs started — **built** | Low | 0.25 | 0.5–1 | 0.25 | 1–1.5 | — |
| 7 | Report 27 status mix check | Low–Medium | 0.5 | 0–2 | 0.5 | 1–3 | Tuesday session |
| 8 | Super Report residents by wet read (PACS) | Medium–High | 1–1.5 | 2–2.5 | 0.5–1 | 3.5–5 | PACS timestamp check |
| 9 | Staff who left, hidden from KPIs | Medium | 1 | 2.5–3 | 0.5 | 4–4.5 | Tuesday session; manager’s list |
| 10 | Modality "PACS" samples | Low | 0.25 | 0–1 | 0.25 | 0.5–1.5 | operator’s review |
| 11 | Report start date setting — **done** | Low | 0.1 | 1 | 0.25 | 1–1.5 | — |
| 12 | Chart explanations (~150 notes, 16 pages) — **done** | Low–Medium | 0.5 | 2.5–3.5 | 0.5 | 3.5–4.5 | — |
| 13 | SJH database, all parts | High | 3–4 | 14–18 | 2–3 | 19–25 | BRAYN on; site-ID walkthrough |
| 14 | Radiologist TAT and report counts on PACS | High | 2 | 4–5 | 1–2 | 7–9 | shares item 8’s PACS copy |

Item 13 splits into: second database, per-request switch, migrations and SJH ETL (6–8);
migrate, count and delete SJH rows (2–3); site dropdown (1); RH vs SJH per-modality views
(2–3); BRAYN routing by site (1–2); new roles and permissions (3–4); mail server per site and
licence (1).

Items 1–14 without item 5 add up to about 54–76 days (35–51 without item 13). Items 8 and 14
share about one day. Doing item 3 together with the shared filter panel saves 1–2 days.

| # | Earlier and added items | Difficulty | Total | Waits on |
|---|---|---|---|---|
| 15 | Urgent emergent in HL7 | Low | 2 | end user and HIS answer |
| 16 | Shared filter panel, 6 reports | Medium | 4–5.5 | — |
| 17 | R2I and BRAYN switch-on checks | Medium | 2 | feed live |
| 18 | ORU final-only via BRAYN | Low–Medium | 2 | meaning of `FAP` or a prelim sample |
| 19 | Placeholders via BRAYN | Low | 1.5–2 | — |
| 20 | CRN go-live (low) | Medium | 3.5–4.5 | provider, signed-off wording |
| 21 | Patient schedule lead time (very low) | Low–Medium | 2.5–3 | which moment counts as "booked" |
| 22 | Daily Briefing snapshot RH only (found 2026-11-14) — **done** | Low | 1 | — (counts checked on the server) |
| 23 | Yesterday tab Orders ↔ PACS reconciliation (found 2026-11-14) — **done** | Low | 0.5–1 | — |
| 24 | Deploy the 2026-11-14 work (migrations 0148, 0149) | Low | 0.5 | item 4's Monday check |
| 25 | ER dashboard onto the one report UI (Report 38) — **done** | Low | 1–1.5 | — |

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
| **Needs the server** | 7 status mix check (RAYD vs Oracle at one cut-off); 9 staff who left (export of names, PACS user columns); 10 modality "PACS" samples; 13 SJH database (the build is local, the move, counts and delete run on production); R2I and BRAYN switch-on checks; CRN go-live | no |

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
24. **Deploy the 2026-11-14 work** (items 3, 4, 6, 11, 12; migrations 0148 and 0149) after
    item 4's Monday room-name check.
25. **ER dashboard onto the one report UI** (CLAUDE.md rule 8). The ORU tab was taken out of
    scope (operator, 2026-11-14) and stays the standalone page `/oru`.
    **Done 2026-11-14**, not deployed: the ER dashboard is Report 38 (`routes/report_38.py`,
    `templates/report_38.html`, migration 0150: report row, licence entry unless the licence
    switched `er_dashboard` off, access for every role but tec as the page had it). Opens at
    `/viewer/38`, `/er` redirects; the home page card is a report card shown to whoever has
    access. The shared left panel: dates (last 30 days by default), the reporting target in
    hours, and a modality filter (new). Tests: `tests/test_report_38.py`. After deploy: run
    migration 0150, then check the home card and a technologist account (no access).

## Low priority

- **CRN go-live.** CRN is off (`crn_live_since` empty, provider "log"). It needs a real
  provider and signed-off message wording before it is switched on.

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
  account → exclusion list, separate from NULL handling (open item 7 above).
- Backfill: **never** row-level the 166M image rows — aggregate at source to ~614k/study
  (`LAUMC_DATA_REQUEST.md` §B5) if the full Phase 3/4 row-level sync ever proves too slow
  or unnecessary.
- Site vocabularies may change at the ~2027 RIS/PACS upgrade → only `sites` / `site_org_map`
  rows change, never code.
- **NO PATIENT NAMES in Postgres, ever** (operator instruction, 2026-07-27). Staff/resource
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
