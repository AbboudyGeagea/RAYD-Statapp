"""
System Type Registry — standardized target schemas per system type.
Each system type (PACS, RIS, LIS, HIS) defines:
  - A database suffix (rayd_pacs, rayd_ris, ...)
  - A set of standardized tables with known column definitions
  - Known aliases per column (for strict auto-mapping)

Columns use PostgreSQL types. Only exact + alias matches are auto-mapped;
everything else requires human review.
"""

SYSTEM_TYPES = {

    # ── PACS ────────────────────────────────────────────────────────────
    "PACS": {
        "db_name_suffix": "pacs",
        "label": "Picture Archiving & Communication System",
        "tables": {
            "std_studies": {
                "description": "Radiology studies / exams",
                "pk": "study_db_uid",
                "columns": {
                    "study_db_uid":            {"pg_type": "BIGINT NOT NULL",        "aliases": ["stu_db_uid", "study_dbid", "studydbuid"]},
                    "patient_db_uid":          {"pg_type": "BIGINT NOT NULL",        "aliases": ["pat_db_uid", "patient_dbid", "patientdbuid"]},
                    "study_instance_uid":      {"pg_type": "TEXT",                   "aliases": ["stu_instance_uid", "study_uid", "dicom_study_uid"]},
                    "accession_number":        {"pg_type": "TEXT",                   "aliases": ["accession_no", "acc_number", "accession"]},
                    "study_id":                {"pg_type": "TEXT",                   "aliases": ["stu_id"]},
                    "storing_ae":              {"pg_type": "TEXT",                   "aliases": ["ae_title", "aetitle", "source_ae", "station_ae"]},
                    "study_date":              {"pg_type": "DATE",                   "aliases": ["stu_dt", "exam_date", "study_dt", "studydate"]},
                    "study_description":       {"pg_type": "TEXT",                   "aliases": ["stu_description", "exam_description", "study_desc"]},
                    "study_body_part":         {"pg_type": "TEXT",                   "aliases": ["body_part", "body_part_examined", "bodypart"]},
                    "study_age":               {"pg_type": "TEXT",                   "aliases": ["patient_age", "pat_age"]},
                    "age_at_exam":             {"pg_type": "NUMERIC(5,2)",           "aliases": []},
                    "number_of_study_series":  {"pg_type": "INTEGER",               "aliases": ["num_series", "series_count"]},
                    "number_of_study_images":  {"pg_type": "INTEGER",               "aliases": ["num_images", "image_count", "total_images"]},
                    "study_status":            {"pg_type": "TEXT",                   "aliases": ["stu_status", "exam_status"]},
                    "patient_class":           {"pg_type": "TEXT",                   "aliases": ["pat_class", "patient_type"]},
                    "patient_location":        {"pg_type": "VARCHAR(3)",             "aliases": ["pat_location"]},
                    "procedure_code":          {"pg_type": "TEXT",                   "aliases": ["proc_code", "proc_id", "procedure_id"]},
                    "modality":                {"pg_type": "VARCHAR(16)",            "aliases": ["study_modality"]},
                    "referring_physician_first_name": {"pg_type": "TEXT",            "aliases": ["ref_phys_first", "ref_physician_given_name"]},
                    "referring_physician_last_name":  {"pg_type": "TEXT",            "aliases": ["ref_phys_last", "ref_physician_family_name"]},
                    "reading_physician_first_name":  {"pg_type": "TEXT",            "aliases": ["read_phys_first", "reading_physician_given_name"]},
                    "reading_physician_last_name":   {"pg_type": "TEXT",            "aliases": ["read_phys_last", "reading_physician_family_name"]},
                    "reading_physician_id":          {"pg_type": "BIGINT",          "aliases": []},
                    "signing_physician_first_name":  {"pg_type": "TEXT",            "aliases": ["sign_phys_first", "signing_physician_given_name"]},
                    "signing_physician_last_name":   {"pg_type": "TEXT",            "aliases": ["sign_phys_last", "signing_physician_family_name"]},
                    "signing_physician_id":          {"pg_type": "BIGINT",          "aliases": []},
                    "report_status":           {"pg_type": "TEXT",                   "aliases": ["rep_status"]},
                    "order_status":            {"pg_type": "TEXT",                   "aliases": ["ord_status"]},
                    "study_has_report":        {"pg_type": "BOOLEAN DEFAULT FALSE",  "aliases": ["has_report"]},
                    "rep_prelim_timestamp":    {"pg_type": "TIMESTAMP",              "aliases": ["prelim_timestamp", "prelim_report_time"]},
                    "rep_prelim_signed_by":    {"pg_type": "TEXT",                   "aliases": ["prelim_signed_by"]},
                    "rep_final_signed_by":     {"pg_type": "TEXT",                   "aliases": ["final_signed_by", "rep_signed_by"]},
                    "rep_final_timestamp":     {"pg_type": "TIMESTAMP",              "aliases": ["final_timestamp", "final_report_time"]},
                    "rep_transcribed_by":      {"pg_type": "TEXT",                   "aliases": ["transcribed_by"]},
                    "rep_transcribed_timestamp": {"pg_type": "TIMESTAMP",            "aliases": ["transcribed_timestamp"]},
                    "rep_addendum_by":         {"pg_type": "TEXT",                   "aliases": ["addendum_by"]},
                    "rep_addendum_timestamp":  {"pg_type": "TIMESTAMP",              "aliases": ["addendum_timestamp"]},
                    "rep_has_addendum":        {"pg_type": "BOOLEAN DEFAULT FALSE",  "aliases": ["has_addendum"]},
                    "is_linked_study":         {"pg_type": "BOOLEAN DEFAULT FALSE",  "aliases": ["linked_study"]},
                    "insert_time":             {"pg_type": "TIMESTAMP",              "aliases": ["created_at", "create_time", "stu_insert_dt"]},
                    "last_update":             {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
            "std_series": {
                "description": "DICOM series within studies",
                "pk": "series_db_uid",
                "columns": {
                    "series_db_uid":            {"pg_type": "BIGINT NOT NULL",       "aliases": ["ser_db_uid", "series_dbid"]},
                    "study_db_uid":             {"pg_type": "BIGINT NOT NULL",       "aliases": ["stu_db_uid"]},
                    "patient_db_uid":           {"pg_type": "BIGINT",               "aliases": ["pat_db_uid"]},
                    "study_instance_uid":       {"pg_type": "TEXT",                  "aliases": []},
                    "series_instance_uid":      {"pg_type": "TEXT",                  "aliases": ["ser_instance_uid"]},
                    "series_number":            {"pg_type": "INTEGER",               "aliases": ["ser_number"]},
                    "modality":                 {"pg_type": "TEXT",                   "aliases": []},
                    "number_of_series_images":  {"pg_type": "INTEGER",               "aliases": ["num_images", "image_count"]},
                    "body_part_examined":       {"pg_type": "TEXT",                   "aliases": ["body_part"]},
                    "protocol_name":            {"pg_type": "TEXT",                   "aliases": []},
                    "series_description":       {"pg_type": "TEXT",                   "aliases": ["ser_description"]},
                    "institution_name":         {"pg_type": "TEXT",                   "aliases": []},
                    "station_name":             {"pg_type": "TEXT",                   "aliases": []},
                    "manufacturer":             {"pg_type": "TEXT",                   "aliases": []},
                    "last_update":              {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
            "std_images": {
                "description": "Raw DICOM image references",
                "pk": "raw_image_db_uid",
                "columns": {
                    "raw_image_db_uid":    {"pg_type": "BIGINT NOT NULL",        "aliases": ["image_db_uid", "img_db_uid"]},
                    "patient_db_uid":      {"pg_type": "BIGINT NOT NULL",        "aliases": ["pat_db_uid"]},
                    "study_db_uid":        {"pg_type": "BIGINT NOT NULL",        "aliases": ["stu_db_uid"]},
                    "series_db_uid":       {"pg_type": "BIGINT NOT NULL",        "aliases": ["ser_db_uid"]},
                    "study_instance_uid":  {"pg_type": "TEXT",                   "aliases": []},
                    "series_instance_uid": {"pg_type": "TEXT",                   "aliases": []},
                    "image_number":        {"pg_type": "INTEGER",               "aliases": ["img_number"]},
                    "file_system":         {"pg_type": "TEXT",                   "aliases": ["file_path", "storage_path"]},
                    "image_size_kb":       {"pg_type": "INTEGER",               "aliases": ["image_size", "file_size_kb"]},
                    "last_update":         {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
            "std_patients": {
                "description": "Patient demographics",
                "pk": "patient_db_uid",
                "columns": {
                    "patient_db_uid":            {"pg_type": "BIGINT NOT NULL",  "aliases": ["pat_db_uid", "patient_dbid", "patientdbuid"]},
                    "patient_id":                {"pg_type": "TEXT",             "aliases": ["id", "mrn", "patient_mrn", "external_id"]},
                    "birth_date":                {"pg_type": "DATE",             "aliases": ["dob", "date_of_birth"]},
                    "sex":                       {"pg_type": "VARCHAR(1)",       "aliases": ["gender", "patient_sex"]},
                    "number_of_patient_studies":  {"pg_type": "INTEGER",        "aliases": ["num_studies", "study_count"]},
                    "number_of_patient_series":   {"pg_type": "INTEGER",        "aliases": ["num_series"]},
                    "number_of_patient_images":   {"pg_type": "INTEGER",        "aliases": ["num_images"]},
                    "age_group":                 {"pg_type": "TEXT",             "aliases": []},
                    "last_update":               {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
        },
    },

    # ── RIS ─────────────────────────────────────────────────────────────
    # LAUMC RIS (CSHRIS schema, Oracle 12). Real schemas confirmed by vendor 2026-07-07
    # (docs/LAUMC_RIS_TABLES.md). Target column names = lowercase source column names so
    # the auto-mapper resolves 1:1 without aliases.
    #
    # GLOBAL EXTRACT RULE: the RIS holds more than LAUMC's two sites. Pull ONLY rows for
    # LAUMC: ORDERS filtered by ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH');
    # org-keyed tables resolve site via site_org_map (3926/5521/5120=RH, 5320=SJH).
    # Columns confirmed all-NULL at LAUMC are omitted entirely (PACS_SPS_ID, *_FLAGSET,
    # DICTATED_BY_TEST, ORDERED_PROCEDURES). Report blobs (DOCUMENT, DOCUMENT_TEXT, MAP,
    # PDF_DOCUMENT) are omitted — plain text only, per vendor ruling.
    "RIS": {
        "db_name_suffix": "ris",
        "label": "Radiology Information System",
        "tables": {
            "std_worklist": {
                # Source: CSHRIS.SITE_WORKLIST — 1 row per SPS (exam). Current status only
                # (mutated in place); RAYD builds its own worklist_status_history.
                # sps_id ('100500…') = the accession minted at scheduling = PACS accession.
                "description": "RIS worklist — one row per scheduled procedure step (exam)",
                "pk": "site_worklist_key",
                "incremental_key": "last_update_date",   # rows mutate for years → upsert
                "columns": {
                    "site_worklist_key":     {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "patient_person_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "visit_key":             {"pg_type": "BIGINT",  "aliases": []},
                    "order_key":             {"pg_type": "BIGINT",  "aliases": []},
                    "requested_procedure_id": {"pg_type": "TEXT",   "aliases": []},
                    "sps_id":                {"pg_type": "TEXT",    "aliases": []},   # accession (100500…) = PACS accession
                    "pps_key":               {"pg_type": "BIGINT",  "aliases": []},
                    "dictation_key":         {"pg_type": "BIGINT",  "aliases": []},
                    "report_key":            {"pg_type": "BIGINT",  "aliases": []},   # → std_reports
                    "org_structure_key":     {"pg_type": "TEXT",    "aliases": []},   # → site_org_map → site
                    "order_priority":        {"pg_type": "TEXT",    "aliases": []},
                    "scheduled_date":        {"pg_type": "TIMESTAMP", "aliases": []},
                    "sps_code_key":          {"pg_type": "BIGINT",  "aliases": []},   # → std_procedure_codes
                    "last_update_date":      {"pg_type": "TIMESTAMP", "aliases": []},
                    "status_key":            {"pg_type": "INTEGER", "aliases": []},   # → worklist_status_map
                    "rp_code_key":           {"pg_type": "BIGINT",  "aliases": []},
                    "ordering_organization_key": {"pg_type": "BIGINT", "aliases": []},
                    "status":                {"pg_type": "TEXT",    "aliases": []},   # RIS label (traceability)
                    "deceased":              {"pg_type": "TEXT",    "aliases": []},
                    "pps_code_key":          {"pg_type": "BIGINT",  "aliases": []},
                    "requested_by_person_key": {"pg_type": "BIGINT", "aliases": []},
                    "requested_by_resource_id": {"pg_type": "TEXT", "aliases": []},
                    "request_datetime":      {"pg_type": "TIMESTAMP", "aliases": []},
                    "justified_by_person_key": {"pg_type": "BIGINT", "aliases": []},
                    "modality_type":         {"pg_type": "TEXT",    "aliases": []},   # CT/MR/US…
                    "recall_pps_key":        {"pg_type": "BIGINT",  "aliases": []},
                    "followup_pps_key":      {"pg_type": "BIGINT",  "aliases": []},
                    "linked_id":             {"pg_type": "BIGINT",  "aliases": []},   # multi-SPS → one report/study
                    "justified_by_resource_id_key": {"pg_type": "BIGINT", "aliases": []},
                    "last_name":             {"pg_type": "TEXT",    "aliases": []},   # denormalized patient
                    "message_created_by":    {"pg_type": "TEXT",    "aliases": []},
                    "description":           {"pg_type": "TEXT",    "aliases": []},   # procedure text
                    "message_cr_key":        {"pg_type": "BIGINT",  "aliases": []},
                    "order_group_key":       {"pg_type": "BIGINT",  "aliases": []},
                    "message_created_date":  {"pg_type": "TIMESTAMP", "aliases": []},
                    "report_last_modified_date": {"pg_type": "TIMESTAMP", "aliases": []},
                    "row_created_date":      {"pg_type": "TIMESTAMP", "aliases": []},
                    "performed_date":        {"pg_type": "TIMESTAMP", "aliases": []},  # exam done
                    "gender_description":    {"pg_type": "TEXT",    "aliases": []},
                    "name_prefix":           {"pg_type": "TEXT",    "aliases": []},
                    "sps_created_date":      {"pg_type": "TIMESTAMP", "aliases": []},  # accession minted
                    "followup_type_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "assignradcodepersonkey": {"pg_type": "BIGINT", "aliases": []},   # assigned radiologist
                    "into_private_folder":   {"pg_type": "TEXT",    "aliases": []},
                    "birad_category":        {"pg_type": "TEXT",    "aliases": []},   # parked BI-RADS
                    "ordering_group_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "birads_birads_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "lock_session_key":      {"pg_type": "BIGINT",  "aliases": []},
                    "approved_date":         {"pg_type": "TIMESTAMP", "aliases": []},  # report approved
                    "firstarabic":           {"pg_type": "TEXT",    "aliases": []},
                    "lastarabicname":        {"pg_type": "TEXT",    "aliases": []},
                    "reffering_phisician":   {"pg_type": "TEXT",    "aliases": []},   # [sic] source spelling
                    "reffering_doctor":      {"pg_type": "TEXT",    "aliases": []},   # [sic]
                    "requested_by_resource_id_name": {"pg_type": "TEXT", "aliases": []},
                    "prot_hold_by_person_key": {"pg_type": "BIGINT", "aliases": []},
                    "technician":            {"pg_type": "TEXT",    "aliases": []},
                    "site_id":               {"pg_type": "INTEGER", "aliases": []},   # RAYD-resolved (enrichment)
                },
            },
            "std_orders": {
                # Source: CSHRIS.ORDERS — 1 row per HIS order (header; parent of std_worklist
                # rows via order_key). Carries the AUTHORITATIVE site issuer.
                # EXTRACT FILTER: issuer_of_placer_order_number IN ('SAP_PROD','SAP_SJH').
                "description": "RIS order headers from HIS (1 per order; parent of worklist SPS)",
                "pk": "order_key",
                "incremental_key": "created_on_date",   # ≤500 rows/day across both sites
                "columns": {
                    "order_key":             {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "visit_key":             {"pg_type": "BIGINT",  "aliases": []},
                    "patient_person_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "accession_number":      {"pg_type": "TEXT",    "aliases": []},   # HIS/SAP accession (NOT the PACS join)
                    "issuer_of_accession_number": {"pg_type": "TEXT", "aliases": []},
                    "reason_for_order":      {"pg_type": "TEXT",    "aliases": []},
                    "request_datetime":      {"pg_type": "TIMESTAMP", "aliases": []},
                    "parent_placer_order_number": {"pg_type": "TEXT", "aliases": []},
                    "placer_group_number":   {"pg_type": "TEXT",    "aliases": []},
                    "placer_order_number":   {"pg_type": "TEXT",    "aliases": []},   # HL7 lifecycle key
                    "issuer_of_placer_order_number": {"pg_type": "TEXT", "aliases": []},  # SAP_PROD/SAP_SJH → site (authoritative)
                    "filler_order_number":   {"pg_type": "TEXT",    "aliases": []},
                    "issuer_of_filler_order_number": {"pg_type": "TEXT", "aliases": []},
                    "comments":              {"pg_type": "TEXT",    "aliases": []},
                    "priority_key":          {"pg_type": "BIGINT",  "aliases": []},
                    "ordering_organization_key": {"pg_type": "BIGINT", "aliases": []},
                    "status_key":            {"pg_type": "INTEGER", "aliases": []},
                    "isolation_status":      {"pg_type": "TEXT",    "aliases": []},
                    "order_department":      {"pg_type": "TEXT",    "aliases": []},
                    "special_instructions":  {"pg_type": "TEXT",    "aliases": []},
                    "protocol_required_flag": {"pg_type": "TEXT",   "aliases": []},
                    "protocol_completed_flag": {"pg_type": "TEXT",  "aliases": []},
                    "org_structure_key":     {"pg_type": "TEXT",    "aliases": []},   # cross-check vs issuer
                    "visitation_comments":   {"pg_type": "TEXT",    "aliases": []},
                    "schedule_priority_key": {"pg_type": "BIGINT",  "aliases": []},
                    "justification_status_key": {"pg_type": "BIGINT", "aliases": []},
                    "justified_on_date":     {"pg_type": "TIMESTAMP", "aliases": []},
                    "created_on_date":       {"pg_type": "TIMESTAMP", "aliases": []},
                    "signed_by_date":        {"pg_type": "TIMESTAMP", "aliases": []},
                    "requesting_address_key": {"pg_type": "BIGINT", "aliases": []},
                    "requesting_report_delivery_key": {"pg_type": "BIGINT", "aliases": []},  # CRN routing
                    "requesting_image_delivery_key":  {"pg_type": "BIGINT", "aliases": []},
                    "requesting_send_to":    {"pg_type": "TEXT",    "aliases": []},   # CRN routing
                    "signed_by_resource_id_key":    {"pg_type": "BIGINT", "aliases": []},
                    "requested_by_resource_id_key": {"pg_type": "BIGINT", "aliases": []},
                    "justified_by_resource_id_key": {"pg_type": "BIGINT", "aliases": []},
                    "recommended_schedule_date": {"pg_type": "TIMESTAMP", "aliases": []},
                    "followup_pps_key":      {"pg_type": "BIGINT",  "aliases": []},
                    "status_reason_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "created_by_resource_id_key": {"pg_type": "BIGINT", "aliases": []},
                    "cancelled_by_person_key": {"pg_type": "BIGINT", "aliases": []},
                    "ordering_group_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "followup_type_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "review_eorder_flag":    {"pg_type": "TEXT",    "aliases": []},
                    "second_opinion_eorder_flag": {"pg_type": "TEXT", "aliases": []},
                    "import_study_order":    {"pg_type": "TEXT",    "aliases": []},
                    "site_id":               {"pg_type": "INTEGER", "aliases": []},   # RAYD-resolved (from issuer)
                },
            },
            "std_reports": {
                # Source: CSHRIS.REPORT — 1 row per report VERSION (report_key repeats across
                # versions; is_max_version marks current). Accession is PER-VERSION (amended
                # versions get a new sequence) → current-study join uses the max-version row.
                # DOCUMENT_PLAIN_TEXT = THE durable NLP feed / CRN body (full labeled sections:
                # INDICATION/TECHNIQUE/FINDINGS/IMPRESSION). Blobs omitted per ruling.
                # NOTE Qr3: signature dates pulled RAW; PACS↔RIS status mapping deferred.
                "description": "RIS report versions — plain-text content + full signature chain",
                "pk": "report_key, version",
                "incremental_key": "last_modified_date",
                "columns": {
                    "report_key":            {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "version":               {"pg_type": "INTEGER NOT NULL", "aliases": []},
                    "is_max_version":        {"pg_type": "TEXT",    "aliases": []},   # current-version flag
                    "reported_acc_number":   {"pg_type": "TEXT",    "aliases": []},   # accession (per-version!)
                    "version_status_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "finalization_state":    {"pg_type": "TEXT",    "aliases": []},
                    "interpretation_type_key": {"pg_type": "BIGINT", "aliases": []},
                    "addendum":              {"pg_type": "TEXT",    "aliases": []},
                    "report_template_key":   {"pg_type": "BIGINT",  "aliases": []},
                    "format":                {"pg_type": "TEXT",    "aliases": []},
                    "body_key":              {"pg_type": "BIGINT",  "aliases": []},
                    "cr_message_key":        {"pg_type": "BIGINT",  "aliases": []},
                    "distributed":           {"pg_type": "TEXT",    "aliases": []},
                    "note_to_rad_flag":      {"pg_type": "TEXT",    "aliases": []},
                    "print_rules_gui_override": {"pg_type": "TEXT", "aliases": []},
                    # --- content ---
                    "document_plain_text":   {"pg_type": "TEXT",    "aliases": []},   # THE NLP feed
                    # --- report chain timestamps (raw; mapping deferred per Qr3) ---
                    "report_created_date":   {"pg_type": "TIMESTAMP", "aliases": []},
                    "draft_date":            {"pg_type": "TIMESTAMP", "aliases": []},
                    "wet_read_date":         {"pg_type": "TIMESTAMP", "aliases": []},
                    "transcription_date":    {"pg_type": "TIMESTAMP", "aliases": []},
                    "verified1_date":        {"pg_type": "TIMESTAMP", "aliases": []},
                    "verified2_date":        {"pg_type": "TIMESTAMP", "aliases": []},
                    "verified3_date":        {"pg_type": "TIMESTAMP", "aliases": []},
                    "approved_date":         {"pg_type": "TIMESTAMP", "aliases": []},
                    "reviewed_date":         {"pg_type": "TIMESTAMP", "aliases": []},
                    "returned_date":         {"pg_type": "TIMESTAMP", "aliases": []},
                    "report_time":           {"pg_type": "TIMESTAMP", "aliases": []},
                    "last_modified_date":    {"pg_type": "TIMESTAMP", "aliases": []},
                    # --- people (resource keys → PERSON/RESOURCE table, next session) ---
                    "reported_by":           {"pg_type": "TEXT",    "aliases": []},
                    "report_to":             {"pg_type": "TEXT",    "aliases": []},
                    "transcribed_by_resource_id_key": {"pg_type": "BIGINT", "aliases": []},
                    "verified1_by_resource_id_key":   {"pg_type": "BIGINT", "aliases": []},
                    "verified2_by_resource_id_key":   {"pg_type": "BIGINT", "aliases": []},
                    "verified3_by_resource_id_key":   {"pg_type": "BIGINT", "aliases": []},
                    "approved_by_resource_id_key":    {"pg_type": "BIGINT", "aliases": []},
                    "created_by_resource_id_key":     {"pg_type": "BIGINT", "aliases": []},
                    "last_modified_resource_id_key":  {"pg_type": "BIGINT", "aliases": []},
                    "signed_behalf_resource_id_key":  {"pg_type": "BIGINT", "aliases": []},
                    "wet_read_by_resource_id_key":    {"pg_type": "BIGINT", "aliases": []},
                    "reviewed_by_resource_id_key":    {"pg_type": "BIGINT", "aliases": []},
                    "returned_by_resource_id_key":    {"pg_type": "BIGINT", "aliases": []},
                    "draft_by_resource_id_key":       {"pg_type": "BIGINT", "aliases": []},
                    # --- effort metrics (per-radiologist productivity) ---
                    "character_count":       {"pg_type": "INTEGER", "aliases": []},
                    "word_count":            {"pg_type": "INTEGER", "aliases": []},
                    "line_count":            {"pg_type": "INTEGER", "aliases": []},
                    "total_lines_in_document": {"pg_type": "INTEGER", "aliases": []},
                    "minutes_of_editing_for_session": {"pg_type": "NUMERIC", "aliases": []},
                    "site_id":               {"pg_type": "INTEGER", "aliases": []},   # RAYD-resolved (via accession)
                },
            },
            "std_devices": {
                # Source: CSHRIS.MODALITY merged with MODALITY_TYPE (vendor: "merge them
                # already in one table"). ae_title = PACS didb_studies.storing_ae (the
                # per-device RIS↔PACS join). One-time load; reload occasionally.
                # modality = MODALITY_TYPE.CODE resolved at ETL time via modality_type_key.
                "description": "Device/room registry (MODALITY + MODALITY_TYPE merged)",
                "pk": "modality_key",
                "columns": {
                    "modality_key":          {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "code":                  {"pg_type": "TEXT",    "aliases": []},   # room code (CT64, MAMO1…)
                    "description":           {"pg_type": "TEXT",    "aliases": []},   # room display name
                    "ae_title":              {"pg_type": "TEXT",    "aliases": []},   # = didb_studies.storing_ae
                    "station_name":          {"pg_type": "TEXT",    "aliases": []},
                    "modality_type_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "modality":              {"pg_type": "TEXT",    "aliases": []},   # resolved CT/MR/US… (ETL transform)
                    "org_structure_key":     {"pg_type": "TEXT",    "aliases": []},   # → site_org_map → site
                    "active":                {"pg_type": "TEXT",    "aliases": []},
                    "site_id":               {"pg_type": "INTEGER", "aliases": []},   # RAYD-resolved
                },
            },
            "std_procedure_codes": {
                # Source: CSHRIS.SPS_CODE — the procedure catalog. sps_code_key joins
                # std_worklist.sps_code_key. duration = scheduled minutes (capacity math);
                # measured actuals come later from status timestamps.
                "description": "Procedure catalog (SPS codes) with scheduled durations",
                "pk": "sps_code_key",
                "incremental_key": "last_updated",
                "columns": {
                    "sps_code_key":          {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "code":                  {"pg_type": "TEXT",    "aliases": []},   # J17G-01C…
                    "description":           {"pg_type": "TEXT",    "aliases": []},
                    "duration":              {"pg_type": "INTEGER", "aliases": []},   # scheduled minutes
                    "minimum_study_duration": {"pg_type": "INTEGER", "aliases": []},
                    "active":                {"pg_type": "TEXT",    "aliases": []},
                    "body_part_key":         {"pg_type": "BIGINT",  "aliases": []},
                    "laterality_key":        {"pg_type": "BIGINT",  "aliases": []},
                    "coding_scheme_key":     {"pg_type": "BIGINT",  "aliases": []},
                    "document_together_group_key": {"pg_type": "BIGINT", "aliases": []},
                    "contra_indication_warning_text": {"pg_type": "TEXT", "aliases": []},
                    "last_updated":          {"pg_type": "TIMESTAMP", "aliases": []},
                },
            },
            "std_visits": {
                # Source: CSHRIS.VISIT (real schema, supersedes earlier inference).
                # visit_number = HL7 PV1.19 (links live ADT/ORM to visits). Exclude
                # DELETED='Y' rows from stats. Feeds case-mix/payer/LOS.
                "description": "Patient visits/encounters (RIS)",
                "pk": "visit_key",
                "incremental_key": "created_on_date",
                "columns": {
                    "visit_key":             {"pg_type": "BIGINT NOT NULL", "aliases": []},
                    "patient_person_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "patient_class_key":     {"pg_type": "BIGINT",  "aliases": []},   # → IP/OP/ER lookup (pending)
                    "preadmit_number":       {"pg_type": "TEXT",    "aliases": []},
                    "visit_number":          {"pg_type": "TEXT",    "aliases": []},   # = HL7 PV1.19
                    "financial_class_key":   {"pg_type": "BIGINT",  "aliases": []},   # → payer/TPA lookup (pending)
                    "admit_date_time":       {"pg_type": "TIMESTAMP", "aliases": []},
                    "discharge_date_time":   {"pg_type": "TIMESTAMP", "aliases": []},
                    "expected_admit_date_time":    {"pg_type": "TIMESTAMP", "aliases": []},
                    "expected_discharge_date_time": {"pg_type": "TIMESTAMP", "aliases": []},
                    "visit_description":     {"pg_type": "TEXT",    "aliases": []},
                    "visit_priority_key":    {"pg_type": "BIGINT",  "aliases": []},
                    "hospital_service_key":  {"pg_type": "BIGINT",  "aliases": []},
                    "visit_indicator":       {"pg_type": "TEXT",    "aliases": []},
                    "issuer_of_visit_number": {"pg_type": "TEXT",   "aliases": []},
                    "issuer_of_preadmit_number": {"pg_type": "TEXT", "aliases": []},
                    "alternate_visit_id":    {"pg_type": "TEXT",    "aliases": []},
                    "mobility_status_key":   {"pg_type": "BIGINT",  "aliases": []},
                    "created_by_person_key": {"pg_type": "BIGINT",  "aliases": []},
                    "created_on_date":       {"pg_type": "TIMESTAMP", "aliases": []},
                    "patient_account_number": {"pg_type": "TEXT",   "aliases": []},
                    "is_master":             {"pg_type": "TEXT",    "aliases": []},
                    "deleted":               {"pg_type": "TEXT",    "aliases": []},   # exclude 'Y' from stats
                    "deleted_date":          {"pg_type": "TIMESTAMP", "aliases": []},
                    "site_id":               {"pg_type": "INTEGER", "aliases": []},   # RAYD-resolved
                },
            },
        },
    },
    # ── LIS ─────────────────────────────────────────────────────────────
    "LIS": {
        "db_name_suffix": "lis",
        "label": "Laboratory Information System",
        "tables": {
            "std_results": {
                "description": "Lab test results",
                "pk": "result_id",
                "columns": {
                    "result_id":         {"pg_type": "BIGINT NOT NULL",          "aliases": ["result_dbid", "test_result_id"]},
                    "patient_id":        {"pg_type": "TEXT",                     "aliases": ["patient_dbid", "mrn"]},
                    "order_id":          {"pg_type": "TEXT",                     "aliases": ["order_dbid", "lab_order_id"]},
                    "test_code":         {"pg_type": "TEXT",                     "aliases": ["analyte_code", "loinc_code"]},
                    "test_name":         {"pg_type": "TEXT",                     "aliases": ["analyte_name", "test_description"]},
                    "result_value":      {"pg_type": "TEXT",                     "aliases": ["value", "result_text"]},
                    "result_unit":       {"pg_type": "TEXT",                     "aliases": ["unit", "units"]},
                    "reference_range":   {"pg_type": "TEXT",                     "aliases": ["normal_range", "ref_range"]},
                    "abnormal_flag":     {"pg_type": "TEXT",                     "aliases": ["flag", "result_flag"]},
                    "result_datetime":   {"pg_type": "TIMESTAMP",               "aliases": ["result_dt", "observation_datetime"]},
                    "collected_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["collection_dt", "specimen_datetime"]},
                    "result_status":     {"pg_type": "TEXT",                     "aliases": ["status", "obs_status"]},
                    "last_update":       {"pg_type": "TIMESTAMP DEFAULT NOW()",  "aliases": []},
                },
            },
            "std_specimens": {
                "description": "Lab specimens / samples",
                "pk": "specimen_id",
                "columns": {
                    "specimen_id":       {"pg_type": "BIGINT NOT NULL",          "aliases": ["specimen_dbid", "sample_id"]},
                    "patient_id":        {"pg_type": "TEXT",                     "aliases": ["patient_dbid", "mrn"]},
                    "specimen_type":     {"pg_type": "TEXT",                     "aliases": ["sample_type"]},
                    "collected_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["collection_dt"]},
                    "received_datetime": {"pg_type": "TIMESTAMP",               "aliases": ["received_dt"]},
                    "department":        {"pg_type": "TEXT",                     "aliases": ["lab_dept", "lab_section"]},
                    "last_update":       {"pg_type": "TIMESTAMP DEFAULT NOW()",  "aliases": []},
                },
            },
        },
    },

    # ── HIS ─────────────────────────────────────────────────────────────
    "HIS": {
        "db_name_suffix": "his",
        "label": "Hospital Information System",
        "tables": {
            "std_visits": {
                "description": "Patient visits / encounters",
                "pk": "visit_id",
                "columns": {
                    "visit_id":           {"pg_type": "BIGINT NOT NULL",         "aliases": ["visit_dbid", "encounter_id", "visit_db_uid"]},
                    "patient_id":         {"pg_type": "TEXT",                    "aliases": ["patient_dbid", "mrn"]},
                    "admission_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["admission_date", "admit_dt", "admit_date"]},
                    "discharge_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["discharge_date", "disch_dt"]},
                    "patient_class":      {"pg_type": "TEXT",                    "aliases": ["pat_class", "visit_type"]},
                    "department":         {"pg_type": "TEXT",                    "aliases": ["dept", "ward"]},
                    "location":           {"pg_type": "TEXT",                    "aliases": ["bed", "room"]},
                    "attending_physician": {"pg_type": "TEXT",                   "aliases": ["attending_dr"]},
                    "referring_physician": {"pg_type": "TEXT",                   "aliases": ["ref_physician"]},
                    "visit_number":       {"pg_type": "TEXT",                    "aliases": ["encounter_number"]},
                    "last_update":        {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
            "std_admissions": {
                "description": "Inpatient admissions detail",
                "pk": "admission_id",
                "columns": {
                    "admission_id":       {"pg_type": "BIGINT NOT NULL",         "aliases": ["admission_dbid"]},
                    "visit_id":           {"pg_type": "BIGINT",                  "aliases": ["visit_dbid", "encounter_id"]},
                    "patient_id":         {"pg_type": "TEXT",                    "aliases": ["patient_dbid", "mrn"]},
                    "admission_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["admit_dt"]},
                    "discharge_datetime": {"pg_type": "TIMESTAMP",              "aliases": ["disch_dt"]},
                    "diagnosis_code":     {"pg_type": "TEXT",                    "aliases": ["icd_code", "dx_code"]},
                    "diagnosis_text":     {"pg_type": "TEXT",                    "aliases": ["dx_text"]},
                    "department":         {"pg_type": "TEXT",                    "aliases": ["dept", "ward"]},
                    "last_update":        {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
        },
    },
}


def get_system_type(name):
    """Return a system type definition or None."""
    return SYSTEM_TYPES.get(name.upper())


def get_all_types():
    """Return list of {key, label, db_suffix, table_count}."""
    return [
        {
            "key": k,
            "label": v["label"],
            "db_suffix": v["db_name_suffix"],
            "table_count": len(v["tables"]),
        }
        for k, v in SYSTEM_TYPES.items()
    ]


def generate_ddl(system_type_key):
    """
    Generate CREATE TABLE DDL for all tables in a system type.
    Returns a list of SQL strings.
    """
    st = SYSTEM_TYPES.get(system_type_key.upper())
    if not st:
        raise ValueError(f"Unknown system type: {system_type_key}")

    ddl = []
    for tbl_name, tbl_def in st["tables"].items():
        pk = tbl_def["pk"]
        col_lines = []
        for col_name, col_def in tbl_def["columns"].items():
            col_lines.append(f"    {col_name} {col_def['pg_type']}")
        col_lines.append(f"    PRIMARY KEY ({pk})")
        sql = f"CREATE TABLE IF NOT EXISTS {tbl_name} (\n" + ",\n".join(col_lines) + "\n);"
        ddl.append(sql)
    return ddl
