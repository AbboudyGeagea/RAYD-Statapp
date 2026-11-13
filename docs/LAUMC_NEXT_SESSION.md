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
  (item 2).

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
- **Tuesday 2026-11-17:** double-check session on Report 27 Order Status Mix, mainly the
  scheduled status (item 7), and a session on the PACS user columns in `MEDILINK.SECM_USERS`
  (item 9).

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
   (modality "PACS", no RIS room): the operator picks manually what to show.
5. **ORU tab severity — on hold until the chief radiologist answers.** It is RAYD's own
   rule-based score (`nlp_worker/clustering.py`, `classify_report`), averaged per modality in
   `routes/oru_analytics.py`, with no published medical reference. It must follow a recognised
   standard.
6. **Report 27 hourly load:** one chart, blue = when orders were scheduled, red = when they
   were started (RIS status Started; exams, not reports). Each hour shows the average per day
   over the range. Busiest day counted on started, by weekday. Working pattern: Monday to
   Friday full days, Saturday half day, Sunday ER only.
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
   PACS). A resident is anyone in the PACS "residents" security group.
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
12. **Chart explanations (info icon on each chart) from the other branches**, written for
    LAUMC's own queries. Newest copy: Bekaa `static/js/chart-explanations.js` (2026-10-14).
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
       choose when it runs. Done over a weekend.
    d. All HL7 messages carry the site ID (R2I includes it in every message); the operator
       will explain where, so BRAYN routes each message to the right database.
    e. AE mappings separate per database in RAYD; procedure codes shared.
    f. One set of RAYD logins; a user signs in once and switches site if allowed. The old
       roles (viewer, viewer2, tec, admin) are replaced by a new set of roles, with
       permissions per site and per page.
    g. One licence covering both sites. Two mail servers, each with its own domain.
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
- **Shared filter panel.** Move Reports 22, 25, 34, 35, 36 and the Super Report onto
  `templates/_report_filter_panel.html`, one at a time, each tested (CLAUDE.md rule 8). Only
  Report 37 uses it so far.
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

## Low priority

- **CRN go-live.** CRN is off (`crn_live_since` empty, provider "log"). It needs a real
  provider and signed-off message wording before it is switched on.

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
