# Graph Report - /home/stats/StatsApp  (2026-04-14)

## Corpus Check
- 78 files · ~71,043 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 594 nodes · 774 edges · 61 communities detected
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 82 edges (avg confidence: 0.5)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_App Factory & Config|App Factory & Config]]
- [[_COMMUNITY_Admin Controller|Admin Controller]]
- [[_COMMUNITY_AI  BitNet Service|AI / BitNet Service]]
- [[_COMMUNITY_Database Models (copy)|Database Models (copy)]]
- [[_COMMUNITY_HL7 Listener & Parser|HL7 Listener & Parser]]
- [[_COMMUNITY_Mapping Controller|Mapping Controller]]
- [[_COMMUNITY_ORU Analytics|ORU Analytics]]
- [[_COMMUNITY_Patient Portal|Patient Portal]]
- [[_COMMUNITY_Capacity Ladder|Capacity Ladder]]
- [[_COMMUNITY_Super Report Builder|Super Report Builder]]
- [[_COMMUNITY_Database Service|Database Service]]
- [[_COMMUNITY_NLP Processor|NLP Processor]]
- [[_COMMUNITY_Report AI & Intelligence|Report AI & Intelligence]]
- [[_COMMUNITY_Adapter Mapper|Adapter Mapper]]
- [[_COMMUNITY_Live Feed|Live Feed]]
- [[_COMMUNITY_DB Provisioner|DB Provisioner]]
- [[_COMMUNITY_Portal Admin|Portal Admin]]
- [[_COMMUNITY_Auto Mapper|Auto Mapper]]
- [[_COMMUNITY_Oracle Connector & ETL Orders|Oracle Connector & ETL Orders]]
- [[_COMMUNITY_Report Cache|Report Cache]]
- [[_COMMUNITY_HL7 Orders|HL7 Orders]]
- [[_COMMUNITY_Report Registry|Report Registry]]
- [[_COMMUNITY_ETL Discontinues|ETL Discontinues]]
- [[_COMMUNITY_HL7 Orders Route|HL7 Orders Route]]
- [[_COMMUNITY_System Type Registry|System Type Registry]]
- [[_COMMUNITY_ETL Phase 9 Clustering|ETL Phase 9 Clustering]]
- [[_COMMUNITY_Crypto Utilities|Crypto Utilities]]
- [[_COMMUNITY_Viewer Controller|Viewer Controller]]
- [[_COMMUNITY_ETL Runner|ETL Runner]]
- [[_COMMUNITY_Schema Discovery|Schema Discovery]]
- [[_COMMUNITY_Auth Decorators|Auth Decorators]]
- [[_COMMUNITY_Report 25 (Shift)|Report 25 (Shift)]]
- [[_COMMUNITY_Report 23 (Conflicts)|Report 23 (Conflicts)]]
- [[_COMMUNITY_Report 22|Report 22]]
- [[_COMMUNITY_API Controller|API Controller]]
- [[_COMMUNITY_Config|Config]]
- [[_COMMUNITY_Report 25 Production|Report 25 Production]]
- [[_COMMUNITY_Liveview|Liveview]]
- [[_COMMUNITY_Report 27|Report 27]]
- [[_COMMUNITY_User Preferences|User Preferences]]
- [[_COMMUNITY_Report 29|Report 29]]
- [[_COMMUNITY_ER Dashboard|ER Dashboard]]
- [[_COMMUNITY_HL7 Portal Integration|HL7 Portal Integration]]
- [[_COMMUNITY_Patient Conflicts|Patient Conflicts]]
- [[_COMMUNITY_Oracle Service|Oracle Service]]
- [[_COMMUNITY_ETL Patients View|ETL Patients View]]
- [[_COMMUNITY_ETL Debug|ETL Debug]]
- [[_COMMUNITY_Admin Logout|Admin Logout]]
- [[_COMMUNITY_ETL Gear Route|ETL Gear Route]]
- [[_COMMUNITY_Docs Page|Docs Page]]
- [[_COMMUNITY_ETL Image Locations|ETL Image Locations]]
- [[_COMMUNITY_ETL DIDB Studies|ETL DIDB Studies]]
- [[_COMMUNITY_ETL Raw Images|ETL Raw Images]]
- [[_COMMUNITY_ETL Series|ETL Series]]
- [[_COMMUNITY_ETL DIDB Raw Images|ETL DIDB Raw Images]]
- [[_COMMUNITY_DB Service Rationale|DB Service Rationale]]
- [[_COMMUNITY_Extensions|Extensions]]
- [[_COMMUNITY_Routes Init|Routes Init]]
- [[_COMMUNITY_ETL Jobs Init|ETL Jobs Init]]
- [[_COMMUNITY_ETL Settings|ETL Settings]]
- [[_COMMUNITY_Oracle Test|Oracle Test]]

## God Nodes (most connected - your core abstractions)
1. `DBParams` - 21 edges
2. `User` - 13 edges
3. `ReportAccessControl` - 12 edges
4. `ReportTemplate` - 10 edges
5. `_admin_only()` - 10 edges
6. `AiFeedback` - 8 edges
7. `AiCorrection` - 8 edges
8. `parse_oru_r01()` - 8 edges
9. `parse_orm_o01()` - 8 edges
10. `_require_portal_access()` - 8 edges

## Surprising Connections (you probably didn't know these)
- `Read license JSON from settings table. Falls back to full access.` --uses--> `User`  [INFERRED]
  /home/stats/StatsApp/routes/registry.py → /home/stats/StatsApp/db.py
- `Runtime license checks called from routes.     Returns (ok: bool, message: str).` --uses--> `User`  [INFERRED]
  /home/stats/StatsApp/routes/registry.py → /home/stats/StatsApp/db.py
- `Check if a specific report ID is licensed.` --uses--> `User`  [INFERRED]
  /home/stats/StatsApp/routes/registry.py → /home/stats/StatsApp/db.py
- `Return max studies per report, or 0 for unlimited.` --uses--> `User`  [INFERRED]
  /home/stats/StatsApp/routes/registry.py → /home/stats/StatsApp/db.py
- `Register a stub route that renders the 'not licensed' page instead of a 404.` --uses--> `User`  [INFERRED]
  /home/stats/StatsApp/routes/registry.py → /home/stats/StatsApp/db.py

## Communities

### Community 0 - "App Factory & Config"
Cohesion: 0.04
Nodes (58): Config, create_app(), get_db_uri_from_db(), Uses raw psycopg2 to fetch the final connection string from db_params.     Runs, create_app(), is_db_empty(), Returns True if ANY of the critical ETL tables has zero rows.     This covers a, Runs the full ETL in a background thread with app context.     Called once on st (+50 more)

### Community 1 - "Admin Controller"
Cohesion: 0.07
Nodes (18): _get_demo_settings(), login(), register(), check_license_limit(), check_report_licensed(), get_study_limit(), _load_license(), Read license JSON from settings table. Falls back to full access. (+10 more)

### Community 2 - "AI / BitNet Service"
Cohesion: 0.1
Nodes (23): admin_feedback_list(), alerts(), _build_context(), chat(), _contains_hallucination(), _ensure_ai_tables(), _fetch_base_context(), _get_corrections() (+15 more)

### Community 3 - "Database Models (copy)"
Cohesion: 0.11
Nodes (27): AETitleModalityMap, DBParams, ETLJobLog, get_etl_cutoff_date(), get_go_live_date(), get_pg_engine(), get_report_data(), GoLiveDate (+19 more)

### Community 4 - "HL7 Listener & Parser"
Cohesion: 0.14
Nodes (23): _build_ack(), _clean_obx_text(), _component(), _field(), _format_name(), _handle_client(), _parse_hl7_datetime(), parse_orm_o01() (+15 more)

### Community 5 - "Mapping Controller"
Cohesion: 0.09
Nodes (10): confirm_pair(), get_or_create(), move_member(), Apply a modality to all procedure codes in a canonical group., Mark a candidate pair as confirmed and add both codes to a canonical group., Mark a candidate pair as rejected (different procedures)., Move a code to a different cluster, or delete from all clusters (unclustered)., reject_pair() (+2 more)

### Community 6 - "ORU Analytics"
Cohesion: 0.14
Nodes (16): _best_text(), _count_diagnoses(), _is_normal(), _matched_diagnoses(), oru_data(), oru_section_gaps(), oru_sections(), _parse_sections() (+8 more)

### Community 7 - "Patient Portal"
Cohesion: 0.14
Nodes (17): _generate_password(), _get_config(), _parse_pid_from_hl7(), portal_login(), portal_redirect(), portal_viewer_proxy(), process_orm_for_portal(), routes/portal_bp.py ------------------- Patient Portal — login, masked redirect, (+9 more)

### Community 8 - "Capacity Ladder"
Cohesion: 0.17
Nodes (17): detail(), _find_gaps(), _get_all_procedures(), _get_default_start_hour(), _get_opening_minutes(), _get_scheduled(), overview(), routes/capacity_ladder.py -------------------------- Capacity Ladder — shows dai (+9 more)

### Community 9 - "Super Report Builder"
Cohesion: 0.16
Nodes (15): _build_where(), _collect_data(), delete_saved_report(), _fmt(), _fp(), _generate_narrative(), list_saved_reports(), _pct() (+7 more)

### Community 10 - "Database Service"
Cohesion: 0.16
Nodes (10): DatabaseService, Error, get_db_cursor(), MockPsycopg2, Executes a non-query command (INSERT, UPDATE, DELETE).          Args:, # NOTE: This part will fail with an ImportError or connection error, A service class responsible for securely managing connections and executing, Initializes the service with database connection parameters.          Args: (+2 more)

### Community 11 - "NLP Processor"
Cohesion: 0.22
Nodes (13): build_idf(), classify_report(), cluster_reports(), extract_keywords(), process_reports(), nlp_processor.py ───────────────────────────────────────────────────────────────, Unicode-aware tokenizer — preserves French accented chars (é, è, ç, œ …)., Returns (classification, severity_score).     classification: 'normal' | 'border (+5 more)

### Community 12 - "Report AI & Intelligence"
Cohesion: 0.29
Nodes (11): _detect_anomalies(), _generate_explanation(), _get_physician_intelligence(), _get_storage_intelligence(), _get_utilization_intelligence(), _get_volume_intelligence(), _linear_forecast(), _pct_change() (+3 more)

### Community 13 - "Adapter Mapper"
Cohesion: 0.26
Nodes (13): adapter_mapper_page(), _admin_only(), auto_map_endpoint(), confirm_mapping(), delete_mapping(), discover_schema(), _ensure_table(), get_mapping() (+5 more)

### Community 14 - "Live Feed"
Cohesion: 0.18
Nodes (9): add_procedure(), live_events(), live_status(), live_version(), _make_tile(), routes/live_feed.py ────────────────────────────────────────────────────────────, Returns the timestamp of the latest HL7 order received today.     Used only when, Server-Sent Events endpoint.     Keeps a persistent psycopg2 connection listenin (+1 more)

### Community 15 - "DB Provisioner"
Cohesion: 0.23
Nodes (11): ensure_database(), _ensure_registry_table(), _get_pg_url(), get_provisioned_databases(), get_target_engine(), Database Provisioner — creates per-system-type PostgreSQL databases.  All databa, Get a SQLAlchemy engine for a system type's database., Build PostgreSQL connection URL from environment. (+3 more)

### Community 16 - "Portal Admin"
Cohesion: 0.33
Nodes (9): portal_config(), portal_users(), routes/portal_admin.py ---------------------- Admin management for the patient p, Abort 403 for demo users (always) and non-permitted non-admins., _require_portal_access(), resend_whatsapp(), reset_password(), test_whatsapp() (+1 more)

### Community 17 - "Auto Mapper"
Cohesion: 0.27
Nodes (9): auto_map(), _build_alias_index(), _detect_transform(), Auto-Mapper — strict column matching for Adapter Mapper.  Matching rules (in ord, Infer transform from Oracle data type + column name., Build a reverse index: {alias_lower: (std_table, target_col)} for fast lookup., Score how well a source table matches a standard table. Higher = better., Strict auto-mapper. Takes a schema dump dict and system type.     Returns mappin (+1 more)

### Community 18 - "Oracle Connector & ETL Orders"
Cohesion: 0.33
Nodes (9): OracleConnector, _clean_row(), Sanitize a date/datetime value coming from Oracle.     Returns None for anything, Strip and truncate strings, return None for empty., Sanitize a single Oracle row before upsert.     Columns by index (matches col_na, # IMPORTANT: Do NOT filter on SCHEDULED_DATETIME in Oracle at all., run_orders_etl(), _safe_date() (+1 more)

### Community 19 - "Report Cache"
Cohesion: 0.28
Nodes (8): cache_get(), cache_invalidate(), cache_put(), _make_key(), routes/report_cache.py ────────────────────── Simple in-memory TTL cache for rep, Return cached result tuple or None if missing/expired., Store result. Evicts oldest entry if over _MAX_SIZE., Remove entries for a report_id, or all if None. Returns count removed.

### Community 20 - "HL7 Orders"
Cohesion: 0.31
Nodes (8): _fetch_filter_options(), _fetch_orders(), hl7_orders_count(), hl7_orders_data(), hl7_orders_page(), Fetch hl7_orders with optional filters. Returns list of dicts., Get distinct modalities and statuses for filter dropdowns., Lightweight endpoint — today's order count only, used by sidebar badge.

### Community 21 - "Report Registry"
Cohesion: 0.22
Nodes (8): get_all_reports(), get_report(), get_report_ids(), Register a report module. Called at import time by each report file., Return dict of all registered reports: {id: {bp, view, export}}., Return sorted list of all registered report IDs., Return a single report's registration, or None., register_report()

### Community 22 - "ETL Discontinues"
Cohesion: 0.28
Nodes (8): daily_etl_job(), get_oracle_connection_string(), Reads the Oracle connection string from the source_db_params table.     Example:, Executes the ETL logic for a given system after checking go-live date., Main scheduled ETL job that runs daily at 5:00 AM.     Dynamically connects to O, Starts the ETL scheduler. Should be called once during app startup., run_etl_for_system(), start_etl_scheduler()

### Community 23 - "HL7 Orders Route"
Cohesion: 0.36
Nodes (7): _fetch_filter_options(), _fetch_orders(), hl7_orders_data(), hl7_orders_page(), Fetch hl7_orders with optional filters. Returns list of dicts., Get distinct modalities and statuses for filter dropdowns., JSON endpoint for live auto-refresh.

### Community 24 - "System Type Registry"
Cohesion: 0.25
Nodes (7): generate_ddl(), get_all_types(), get_system_type(), System Type Registry — standardized target schemas per system type. Each system, Return a system type definition or None., Return list of {key, label, db_suffix, table_count}., Generate CREATE TABLE DDL for all tables in a system type.     Returns a list of

### Community 25 - "ETL Phase 9 Clustering"
Cohesion: 0.32
Nodes (7): _accumulate(), Phase 9: Ensemble clustering of procedure codes/descriptions.  Pipeline --------, Run the 3 algorithms on a subsample distance/similarity matrix.     Yields (algo, For each non-noise cluster in labels, add 1 to co_count for every pair., Called from etl_runner._perform_migration after Phase 8., _run_algorithms(), run_phase9_clustering()

### Community 26 - "Crypto Utilities"
Cohesion: 0.32
Nodes (7): decrypt(), encrypt(), _get_fernet(), utils/crypto.py — Symmetric encryption for secrets stored in the database. Uses, Derive a Fernet key from the app's SECRET_KEY., Encrypt a string. Returns a base64-encoded ciphertext string., Decrypt a Fernet token back to plaintext. Returns '' on failure.

### Community 27 - "Viewer Controller"
Cohesion: 0.33
Nodes (4): index(), viewer_dashboard(), viewer_export_report(), viewer_report()

### Community 28 - "ETL Runner"
Cohesion: 0.43
Nodes (6): _detect_canonical_groups(), execute_sync(), _perform_migration(), Auto-populate aetitle_modality_map and procedure_duration_map from ETL data., Detect duplicate procedure names using BOTH code similarity AND description, _sync_lookup_tables()

### Community 29 - "Schema Discovery"
Cohesion: 0.38
Nodes (6): _connect_oracle(), _get_oracle_params(), Schema Discovery — connects to a foreign Oracle DB (via db_params), extracts all, Discover the schema of a foreign Oracle DB and save to JSON.     Returns: dict w, Read connection params from db_params table., run_discovery()

### Community 30 - "Auth Decorators"
Cohesion: 0.29
Nodes (6): admin_required(), auth_required(), Checks if user is logged in and has admin role., Checks if user is logged in and has viewer role., Checks if user is logged in. Redirects to login if not., viewer_required()

### Community 31 - "Report 25 (Shift)"
Cohesion: 0.6
Nodes (4): export_report_25(), get_gold_standard_data(), _load_shift_config(), report_25()

### Community 32 - "Report 23 (Conflicts)"
Cohesion: 0.47
Nodes (3): export_report_23(), get_report_config(), report_23()

### Community 33 - "Report 22"
Cohesion: 0.53
Nodes (5): export_report_22(), get_where_params(), Return studies for a given status as JSON (for click-through on the status chart, report_22(), status_drilldown_22()

### Community 34 - "API Controller"
Cohesion: 0.33
Nodes (5): auth_required(), get_reports_data(), Decorator to check if the user is logged in., API endpoint to fetch structured report data based on user selections.     Expec, # NOTE: We allow the query to proceed, but warn the user that

### Community 35 - "Config"
Cohesion: 0.4
Nodes (4): _bool(), Config, config.py ───────────────────────────────────────────────────────────────── RAYD, Read an env var as boolean. Accepts true/1/yes (case-insensitive).

### Community 36 - "Report 25 Production"
Cohesion: 0.5
Nodes (3): Utility to handle date range selection with defaults., report_25(), resolve_dates()

### Community 37 - "Liveview"
Cohesion: 0.4
Nodes (2): liveview_rooms(), Return list of rooms for the room selector.

### Community 38 - "Report 27"
Cohesion: 0.6
Nodes (3): export_report_27(), get_report_data(), report_27()

### Community 39 - "User Preferences"
Cohesion: 0.4
Nodes (1): routes/preferences.py ───────────────────── User preference endpoints:   POST /u

### Community 40 - "Report 29"
Cohesion: 0.83
Nodes (3): export_report_29(), get_report_data(), report_29()

### Community 41 - "ER Dashboard"
Cohesion: 0.67
Nodes (0): 

### Community 42 - "HL7 Portal Integration"
Cohesion: 0.67
Nodes (2): _handle_orm_portal_hook(), Call this after every successful ORM INSERT into hl7_orders.     Wraps in try/ex

### Community 43 - "Patient Conflicts"
Cohesion: 0.67
Nodes (0): 

### Community 44 - "Oracle Service"
Cohesion: 0.67
Nodes (2): get_oracle_connection(), Read credentials from Postgres (SourceDBParams) and return an oracledb connectio

### Community 45 - "ETL Patients View"
Cohesion: 0.67
Nodes (2): Optimized Patient ETL: Standardized on BIGINT for UIDs.     Updates gender and a, run_patients_etl()

### Community 46 - "ETL Debug"
Cohesion: 0.67
Nodes (1): etl_debug.py  —  run this directly on the server to find the crash:   cd /home/s

### Community 47 - "Admin Logout"
Cohesion: 1.0
Nodes (0): 

### Community 48 - "ETL Gear Route"
Cohesion: 1.0
Nodes (0): 

### Community 49 - "Docs Page"
Cohesion: 1.0
Nodes (0): 

### Community 50 - "ETL Image Locations"
Cohesion: 1.0
Nodes (0): 

### Community 51 - "ETL DIDB Studies"
Cohesion: 1.0
Nodes (0): 

### Community 52 - "ETL Raw Images"
Cohesion: 1.0
Nodes (0): 

### Community 53 - "ETL Series"
Cohesion: 1.0
Nodes (0): 

### Community 54 - "ETL DIDB Raw Images"
Cohesion: 1.0
Nodes (0): 

### Community 55 - "DB Service Rationale"
Cohesion: 1.0
Nodes (1): Context manager to establish a database connection and yield a cursor.         I

### Community 56 - "Extensions"
Cohesion: 1.0
Nodes (0): 

### Community 57 - "Routes Init"
Cohesion: 1.0
Nodes (0): 

### Community 58 - "ETL Jobs Init"
Cohesion: 1.0
Nodes (0): 

### Community 59 - "ETL Settings"
Cohesion: 1.0
Nodes (0): 

### Community 60 - "Oracle Test"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **147 isolated node(s):** `Config`, `config.py ───────────────────────────────────────────────────────────────── RAYD`, `Read an env var as boolean. Accepts true/1/yes (case-insensitive).`, `active_sessions`, `report_derivative` (+142 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Admin Logout`** (2 nodes): `logout()`, `admin.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Gear Route`** (2 nodes): `save_etl_gear()`, `etl_gear_route.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Docs Page`** (2 nodes): `docs_page()`, `docs.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Image Locations`** (2 nodes): `run_images_etl()`, `etl_image_locations.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL DIDB Studies`** (2 nodes): `run_studies_etl()`, `etl_didb_studies.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Raw Images`** (2 nodes): `run_raw_images_etl()`, `etl_raw_images.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Series`** (2 nodes): `run_series_etl()`, `etl_series.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL DIDB Raw Images`** (2 nodes): `run_raw_images_etl()`, `etl_didb_raw_images.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `DB Service Rationale`** (1 nodes): `Context manager to establish a database connection and yield a cursor.         I`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Extensions`** (1 nodes): `extensions.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Routes Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Jobs Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `ETL Settings`** (1 nodes): `etl_settings.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Oracle Test`** (1 nodes): `test_oracle.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `User` connect `App Factory & Config` to `Admin Controller`, `Database Models (copy)`, `User Preferences`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Why does `DBParams` connect `Database Models (copy)` to `App Factory & Config`?**
  _High betweenness centrality (0.027) - this node is a cross-community bridge._
- **Are the 20 inferred relationships involving `DBParams` (e.g. with `OracleConnector` and `DBParams`) actually correct?**
  _`DBParams` has 20 INFERRED edges - model-reasoned connections that need verification._
- **Are the 11 inferred relationships involving `User` (e.g. with `Config` and `Uses raw psycopg2 to fetch the final connection string from db_params.     Runs`) actually correct?**
  _`User` has 11 INFERRED edges - model-reasoned connections that need verification._
- **Are the 11 inferred relationships involving `ReportAccessControl` (e.g. with `Returns True if ANY of the critical ETL tables has zero rows.     This covers a` and `Runs the full ETL in a background thread with app context.     Called once on st`) actually correct?**
  _`ReportAccessControl` has 11 INFERRED edges - model-reasoned connections that need verification._
- **Are the 9 inferred relationships involving `ReportTemplate` (e.g. with `Returns True if ANY of the critical ETL tables has zero rows.     This covers a` and `Runs the full ETL in a background thread with app context.     Called once on st`) actually correct?**
  _`ReportTemplate` has 9 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Config`, `config.py ───────────────────────────────────────────────────────────────── RAYD`, `Read an env var as boolean. Accepts true/1/yes (case-insensitive).` to the rest of the system?**
  _147 weakly-connected nodes found - possible documentation gaps or missing edges._