"""
utils/radiologist_sql.py
------------------------
Shared SQL fragments for every radiologist report count (Report 36's tables,
Report 25's addendum rate and coverage). Each fragment assumes etl_didb_studies
is aliased `s`; NOT_SR also needs MOD_JOIN (alias `m`) in the same query.

Rule: a report is counted on the day it was FINALIZED, not the study day.
  Study 01/01, final 03/01 -> not counted on 01/01; counted once on 03/01.
Every radiologist table must use these same fragments so their counts agree.
"""

MOD_JOIN = ("LEFT JOIN aetitle_modality_map m "
            "ON UPPER(TRIM(s.original_storing_ae)) = UPPER(TRIM(m.aetitle))")

_RAD_BASE = ("COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
             "s.signing_physician_last_name)),''),s.rep_final_signed_by,'Unknown')")

# Collapses migrated name variants to one canonical radiologist name
ALIAS_JOIN = ("LEFT JOIN physician_alias_map pam "
              "ON pam.dismissed = false "
              "AND pam.alias = COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
              "s.signing_physician_last_name)),''),s.rep_final_signed_by)")

# Radiologist display name (needs ALIAS_JOIN)
RAD_NAME = "COALESCE(pam.canonical_name, " + _RAD_BASE + ")"

# Only finalized reports with a known radiologist
RAD_OK = (f"AND {RAD_NAME} NOT IN ('','Unknown')"
          f" AND s.rep_final_timestamp IS NOT NULL")

# Same as DATE(s.rep_final_timestamp) BETWEEN :start AND :end, but index-friendly
FINAL_IN_RANGE = ("s.rep_final_timestamp >= CAST(:start AS date) "
                  "AND s.rep_final_timestamp < CAST(:end AS date) + 1")

# SR = PACS-generated structured report, not a real study (needs MOD_JOIN)
NOT_SR = "COALESCE(m.modality, s.study_modality, '') != 'SR'"
