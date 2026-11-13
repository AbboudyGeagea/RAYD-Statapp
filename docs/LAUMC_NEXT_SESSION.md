# LAUMC — To do

Updated 2026-11-13. This file holds only what is still open. What was built is in the commit
messages, and the July version of this file (history, punch list round 2) is in git history.
Companion docs: `LAUMC_SCOPE.md` (features), `LAUMC_RIS_TABLES.md` (RIS map),
`LAUMC_OPEN_QUESTIONS.md` (vendor questions), `LAUMC_DATA_REQUEST.md` (discovery record).

---

## Open

1. **First open of a study (Report 37, JCI).** Plan: a Mirth channel on the PACS server itself,
   on the audit table, sends RAYD a message when a study is first opened (on create or update).
   Feasibility is to be studied separately. The study should state the load on PACS: the
   once-a-minute watcher was stopped on 2026-11-05 over load, and only the nightly ETL reads
   PACS today. Until this exists, Report 37's "first open seen" is the earliest open a nightly
   copy caught (blank when caught over 36 h late).
2. **Urgent emergent in HL7.** Check with the end user and HIS whether urgent emergent can be
   sent in the HL7 messages.
3. **Shared filter panel.** Move Reports 22, 25, 34, 35, 36 and the Super Report onto
   `templates/_report_filter_panel.html`, one at a time, each tested (CLAUDE.md rule 8). Only
   Report 37 uses it so far.
4. **R2I real-time feed — to be activated the week of 2026-11-16.** When it is live: the
   Department board gets "waiting" and "in progress", BRAYN's RIS-dependent checks
   (ORPHAN_EVENT, SKIPPED_RUNG, ORPHAN_ORDER, STALLED_*, seeded off) can be turned on, and HL7
   orders arrive on time. Check each of these once the feed runs.
5. **Report 32 (Radiologists Performance) — revamp, very wrong numbers are showing.** Resident
   vs Radiologist TAT is in Report 36; confirm which screens show the wrong numbers before
   starting. Breakdowns the hospital asked for (July):
   1. Exam done → Signed 1 (preliminary), per resident, per modality, per patient class
      (including ER), bucketed into adjustable threshold windows (e.g. 0-3 h, 3-5 h;
      configurable, not hardcoded).
   2. The same breakdown for the Final/Approved stage.
   3. Report count per radiologist, per day, per modality.

   Sign times come from `std_reports` (RIS REPORT, Phase 19): `verified1_date` for Signed 1.
6. **Only final ORU reports should count — to be filtered by BRAYN.** Keep open; no code change
   for now. `FAP` (OBR-25 and OBX-11) is the vendor's result status, and its meaning, or a
   preliminary sample to compare with, is still missing. BRAYN only observes today (CLAUDE.md
   rule 7), so this needs BRAYN switched on and a decision on how reports read its result.
7. **Placeholder records — to be filtered by BRAYN.** "Referring, Generic"
   (`4101031379966`/`...68`), test patients and the CSH service account are loaded faithfully
   and not yet excluded from any report. Same BRAYN note as item 6.

## Low priority

8. **CRN go-live.** CRN is off (`crn_live_since` empty, provider "log"). It needs a real
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
