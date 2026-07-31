"""
utils/report_filters.py — shared left-sidebar filter builder for report route modules.

Extracted from routes/report_25.py 2026-07-31 when a second report module
(report_36.py) started needing the exact same filter set — every chart across
both reports must follow the report's left-sidebar filters (date range,
patient class, modality, AE title, patient location, RH-only site scope), and
duplicating this per-file risked the same silent drift that caused the
insert_time/site-scope bugs fixed earlier that session.
"""
from datetime import date
from db import get_etl_cutoff_date
from utils.site_resolver import default_site


def sidebar_filters(form_data):
    """
    Left-sidebar filter set (date range, patient class, modality, AE title,
    patient location) plus the RH-only site scope. Mirrors the report_template
    wrapper query's own where_clauses/_sec_filters construction (that one stays
    inline in get_gold_standard_data since it filters unaliased report_template
    columns, not s./m.) -- this is for any other per-section query built against
    etl_didb_studies (aliased s, LEFT JOIN aetitle_modality_map aliased m).

    Returns (params, clause, start, end) -- clause is a SQL fragment to append
    after a WHERE, referencing s.* and m.* (callers must LEFT JOIN
    aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))).
    """
    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    params = {"start": start, "end": end}
    clause = ""

    if form_data.get("class_enabled") == "on" and form_data.getlist("patient_class"):
        params["classes"] = tuple(form_data.getlist("patient_class"))
        clause += " AND s.patient_class IN :classes"

    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        params["modalities"] = tuple(form_data.getlist("modality"))
        clause += " AND UPPER(TRIM(m.modality)) IN :modalities"

    if form_data.get("ae_enabled") == "on" and form_data.getlist("aetitle"):
        params["aetitles"] = tuple(form_data.getlist("aetitle"))
        clause += " AND s.storing_ae IN :aetitles"

    if form_data.get("loc_enabled") == "on" and form_data.getlist("patient_location"):
        params["locations"] = tuple(form_data.getlist("patient_location"))
        clause += " AND s.patient_location IN :locations"

    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        clause += " AND m.site_id = :rh_site_id"

    return params, clause, start, end
