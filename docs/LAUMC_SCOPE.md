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
