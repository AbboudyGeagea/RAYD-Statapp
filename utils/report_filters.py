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

# AE titles that carry reports with no images attached (operator, 2026-09-18).
# Real PACS rows and worth knowing about, but not imaging studies — mixing them
# into the study total inflates every volume chart and every modality breakdown,
# while dropping them loses a number the operator wants to see. A report using this
# list leaves them out of its totals and charts and shows their count on its own.
#
# LAUMCWFM1AR also carries PACS SITE_ID '2', which is not a real site (operator,
# 2026-11-05) and not in the `sites` table. It is excluded by name rather than by
# site, so the rule keeps holding if the AE is ever re-stamped with a known site.
REPORTS_WITHOUT_IMAGES_AES = ('LAUMCWFM1AR',)


def study_site_sql(param):
    """
    SQL condition: this study belongs to the site bound to :param. For queries that
    must keep studies on unmapped devices, which `m.site_id = :param` alone would drop.
    Needs etl_didb_studies as s and aetitle_modality_map as m (LEFT JOIN is fine).

        physical device    the device's site. The PACS label cannot decide this:
                           double reading stamps RH mammography (SENO1) with site '1',
                           167 of 183 studies in October 2026, all on RH RIS orders.
        gateway (modality  the study's PACS site. A gateway carries traffic for both
        'PACS'), unmapped  sites, so its mapped site says nothing about one study, and
        or no site         an unmapped device has no mapped site to read.
    """
    pacs_label = f"s.pacs_site_id_raw = (SELECT pacs_site_id FROM sites WHERE id = :{param})"
    return (f"(CASE WHEN m.site_id IS NULL OR m.modality = 'PACS' THEN {pacs_label} "
            f"ELSE m.site_id = :{param} END)")


def rh_device_sql(alias='m'):
    """
    SQL condition for screens that list devices from aetitle_modality_map itself, where
    removing studies changes nothing: leaves out devices mapped to another site (RH only,
    operator 2026-11-05). A device with no site stays, as does every device on an install
    with no default site.
    """
    return f"COALESCE({alias}.site_id = (SELECT id FROM sites WHERE is_default), TRUE)"


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
