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
    "RIS": {
        "db_name_suffix": "ris",
        "label": "Radiology Information System",
        "tables": {
            "std_orders": {
                "description": "Radiology orders / scheduled procedures",
                "pk": "order_dbid",
                "columns": {
                    "order_dbid":          {"pg_type": "BIGINT NOT NULL",         "aliases": ["order_id", "ord_dbid", "order_db_uid"]},
                    "patient_dbid":        {"pg_type": "TEXT",                    "aliases": ["patient_id", "pat_dbid", "patient_db_uid"]},
                    "study_db_uid":        {"pg_type": "BIGINT",                 "aliases": ["stu_db_uid"]},
                    "visit_dbid":          {"pg_type": "TEXT",                    "aliases": ["visit_id", "encounter_id"]},
                    "study_instance_uid":  {"pg_type": "TEXT",                    "aliases": []},
                    "proc_id":             {"pg_type": "TEXT",                    "aliases": ["procedure_code", "proc_code"]},
                    "proc_text":           {"pg_type": "TEXT",                    "aliases": ["procedure_text", "procedure_name", "proc_description"]},
                    "scheduled_datetime":  {"pg_type": "TIMESTAMP",              "aliases": ["scheduled_dt", "schedule_date", "exam_datetime"]},
                    "order_status":        {"pg_type": "TEXT",                    "aliases": ["ord_status", "status"]},
                    "modality":            {"pg_type": "TEXT",                    "aliases": []},
                    "has_study":           {"pg_type": "BOOLEAN DEFAULT FALSE",   "aliases": []},
                    "order_control":       {"pg_type": "TEXT",                    "aliases": ["ord_control"]},
                    "accession_number":    {"pg_type": "TEXT",                    "aliases": ["accession_no", "acc_number"]},
                    "referring_physician": {"pg_type": "TEXT",                    "aliases": ["ref_physician", "ordering_physician"]},
                    "priority":            {"pg_type": "TEXT",                    "aliases": ["order_priority", "urgency"]},
                    "last_update":         {"pg_type": "TIMESTAMP DEFAULT NOW()", "aliases": []},
                },
            },
            "std_procedure_codes": {
                "description": "Procedure catalog / exam codes",
                "pk": "proc_id",
                "columns": {
                    "proc_id":            {"pg_type": "TEXT NOT NULL",            "aliases": ["procedure_code", "proc_code", "code"]},
                    "proc_text":          {"pg_type": "TEXT",                     "aliases": ["procedure_text", "procedure_name", "description"]},
                    "modality":           {"pg_type": "TEXT",                     "aliases": []},
                    "body_part":          {"pg_type": "TEXT",                     "aliases": ["body_region"]},
                    "department":         {"pg_type": "TEXT",                     "aliases": ["dept"]},
                    "default_duration":   {"pg_type": "INTEGER",                 "aliases": ["duration_minutes", "expected_minutes"]},
                    "is_active":          {"pg_type": "BOOLEAN DEFAULT TRUE",     "aliases": ["active"]},
                    "last_update":        {"pg_type": "TIMESTAMP DEFAULT NOW()",  "aliases": []},
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
