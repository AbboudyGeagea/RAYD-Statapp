# LAUMC — Feature Scope

The committed feature set for the LAUMC deployment. Grows as decisions are made.
Detail and data findings live in `LAUMC_DATA_REQUEST.md`; open questions in
`LAUMC_OPEN_QUESTIONS.md`.

## In scope — build

1. **Multi-site row-level security** — two hospitals (RH main, SJH satellite), one DB.
   `sites` tri-vocabulary map, `site_id` on all per-site tables, RLS enforcement, app-role split.
2. **Site picker + compare mode** — Combined | RH | SJH | Compare, enforced by scope∩grants.
3. **Per-device analytics & capacity** — AE identity survives at LAUMC (NonDicomAgent ~0.02%),
   so device-level (not just modality-level) utilization and capacity.
4. **Live board + 2D floor map** — rooms by busyness, waiting patients as anonymous personas.
   Powered by RIS status events → RAYD-built `worklist_status_history`.
5. **True wait-time & exam-duration KPIs** — measured from status lifecycle (arrived→started→done).
6. **KPI Detailed Reading report** *(added 2026-07-07)* — radiologist turnaround-time distribution
   matrix. Per patient class (IN/Urg/Out), lifecycle segments (Ex.Done→Read, Ex.Done→Signed 1,
   Signed 1→Approved, Ex.Done→Approved), bucketed into time ranges, per radiologist (full names),
   monthly + total, per-site. Replaces a manual Excel. Maps directly to `worklist_status_history`
   (0048) + `worklist_status_map` (0047): each segment = difference between status timestamps;
   patient class from the visit table; radiologist = report signer (LDAP full names).
7. **RIS-sourced reporting (architectural direction)** *(2026-07-07)* — RIS data is cleaner and a
   superset of demand (sees no-shows/cancellations PACS never records; both deployed 2018-12-18 so
   equal history depth). Approach: build ONE unified exam view joining RIS `SITE_WORKLIST` ↔ PACS
   `didb_studies` on `PACS_SPS_ID` (clean 1:1); reports pick RIS columns for funnel/TAT/lifecycle,
   PACS columns for device/image/storage. Migrate reports to the view incrementally — reuse, not
   rewrite. NOT a wholesale switch (per-device + storage stay PACS).
8. **Flexible custom reports / self-service dashboards** *(added 2026-07-07)* — extend the existing
   widget composer (`custom_reports.py` + `report_widgets.py` + `saved_reports`) so users build,
   arrange, save and share their own multi-widget dashboards. See "Custom reports" note below.
9. **RIS RTF → plain-text report ingestion as the durable NLP feed** *(added 2026-07-07)* — PACS
   stores reports encrypted, so today NLP is fed only by the (lossy, forward-only) ORU stream.
   The RIS holds every report (back to 2018) as RTF. Pull RTF → convert to plain text (`striprtf`,
   pure-Python, ~1-5 ms/report) → durable, complete, RE-PROCESSABLE report store. Wins: kills the
   encryption blocker, enables NLP over FULL history (not just since-listener-start), re-runnable
   when the NLP model improves, smaller storage, clean text for CRN. RIS = source of truth for
   report content (watermark `REPORT_LAST_MODIFIED_DATE` catches amendments); reconcile by
   accession. Verify on a real sample: full report (findings+impression) present + Arabic renders.
10. **Closed-loop Critical Result Notification (CRN)** *(added 2026-07-07, demo-requested)* —
   detect critical result (RAYD's existing NLP critical-keyword engine + native ORU flag if present)
   → notify referring physician via their preferred channel (email / SMS / WhatsApp, per-referring
   `referring_contacts` lookup, optional time-of-day routing) → one-click tokenized acknowledgment
   link (channel-agnostic URL → token landing page records who+when) → escalation if unacknowledged
   → **write the ack back to the RIS as an HL7 message** over existing MLLP (`hl7_forward.py`),
   ideally recorded as a native worklist status; RIS MSA ACK closes the loop. New pieces: SMTP +
   SMS/WhatsApp adapters (external providers; WhatsApp needs Business API + template approval),
   `crn_notifications` table, public ack endpoint. Caveats: "Referring, Generic" placeholder orders
   need an exception queue; keep PHI out of message bodies (minimal + secure link); CRN *timing*
   uses a fast trigger (real-time ORU or frequent RIS poll) while content comes from the RIS store.
   Accreditation-grade (Joint Commission closed-loop critical results / ACR actionable findings).

## Out of scope — removed from LAUMC

- **Patient portal** — COMPLETELY REMOVED (physically absent, not license-disabled). ZDC/PDF
  report payloads ignored; plain text only for NLP.
- **Scheduling module** *(removed 2026-07-07)* — remove completely from LAUMC. It is already
  license-gated (`scheduling` flag; routes in `admin_bp`, sidebar hidden when off), so the
  mechanism is: LAUMC license has `scheduling=false` → routes not reachable, sidebar hidden.
  If physical code removal (not just gating) is required on the LAUMC branch, that is a follow-up.

## Optional / leverage only (not planned)

- **RDMS / radiation dose** — data already in the ORU pipeline; activatable on demand. Benchmarked
  vs Medsquare RDM. Do not plan/scope unless requested.
- **BI-RADS mammography QA** — `BIRAD_CATEGORY` in the worklist; accreditation-relevant; parked.
- **Bed-level ADT floor view** — ADT feed carries ward/room/bed; future extension.

## Custom reports — flexibility direction (item 8)

Current: `custom_reports.py` composes reports from a fixed `WIDGET_CATALOGUE` via `run_widget`,
saved through `saved_reports`. LAUMC wants users to build their own dashboards. Extension path
(to detail later): user-defined dashboard = ordered layout of widgets; per-widget config
(metric, filters, chart type); save + share per user/role; all widgets site-scoped through RLS
so a dashboard obeys the viewer's site grants automatically. Site scope must be part of every
widget's cache key. Full design pending; recorded here as committed direction.
