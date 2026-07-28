"""
routes/report_32.py
--------------------
Report 32 — Radiologists Performance.

Standalone single-topic report, split out of Report 25's "Radiologists" tab
(routes/report_25.py, tab id="tab-clinical"). Covers:

  1. Per-radiologist TAT / RVU performance cards with peer percentile ranking
     — ported from get_gold_standard_data()'s `rad_cards` block (report_25.py,
     roughly lines 434-487).
  2. Reports per radiologist × modality / AE title / procedure / month
     cross-tabs — ported from the `rad_volume_matrix` block (report_25.py,
     roughly lines 675-748).
  3. Addendum rate per radiologist — ported from report_25.py's
     `addendum_data` block (roughly lines 643-673), which the old template
     placed in an orphaned "Workflow" tab (tab id="tab-workflow") that had NO
     nav button anywhere — i.e. it was never actually reachable in the old
     report. It gets a real, reachable UI here for the first time.
  4. Reporting Cadence Analysis — per-radiologist signing-time heatmap
     (arrival/departure/break patterns), derived from
     etl_didb_studies.rep_final_timestamp. Originally part of report_25's
     compute_bg_data() (roughly lines 852-937), it was first carried over
     into Report 35 ("Technician TAT") only because it shared that
     function's tech-monitoring compute pass — but it's radiologist
     behaviour, not technician, so it has been moved here and now runs its
     own self-contained query (get_radiologist_performance_data(), reusing
     this report's own sec_filters/sec_needs_mod_join), with no dependency
     on Report 35.

Explicitly NOT ported (out of scope for this report):
  - KPI Detailed Reading (hospital TAT-bucket spreadsheet format,
    get_kpi_detailed_reading() in report_25.py) — that is Report 33.
  - Technician TAT by AE Station, unread study aging, and studies-per-shift
    — those remain under Report 25 (or other split reports) since they are
    not radiologist-TAT/RVU/addendum topics.

Data source: the SAME SQL template Report 25 uses
(`SELECT report_sql_query FROM report_template WHERE report_id = 25`),
wrapped in a CTE and trimmed to only the columns this report needs — the
same pattern report_23.py's get_report_config() uses for report_id=23.

Register in registry.py:
    import routes.report_32
"""
import csv
import io
import logging
import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from utils.site_resolver import default_site

logger = logging.getLogger("report_32")

report_32_bp = Blueprint("report_32", __name__)


def get_report_config(form_data):
    """
    Fetch Report 25's gold-standard SQL template and build the date range /
    filter params against it, mirroring report_23.py's get_report_config().
    """
    base_sql = db.session.execute(
        text("SELECT report_sql_query FROM report_template WHERE report_id = 25")
    ).scalar()

    go_live = get_etl_cutoff_date()
    default_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    default_end = date.today().strftime("%Y-%m-%d")
    start = form_data.get("start_date") or default_start
    end = form_data.get("end_date") or default_end

    params = {"start": start, "end": end}
    extra = []

    if form_data.get("f_mod_active") == "on" and form_data.get("f_mod"):
        extra.append("UPPER(modality) = UPPER(:mod)")
        params["mod"] = form_data.get("f_mod")

    if form_data.get("f_class_active") == "on" and form_data.get("f_class"):
        extra.append("UPPER(patient_class) = UPPER(:p_class)")
        params["p_class"] = form_data.get("f_class")

    extra_where = (" AND " + " AND ".join(extra)) if extra else ""

    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH (satellite) excluded. Resolved via storing_ae ->
    # aetitle_modality_map.site_id (RIS-authoritative), same as
    # get_gold_standard_data() in routes/report_25.py. default_site() resolves
    # to None (filter skipped) on a non-LAUMC/single-site install.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id
        extra_where += (
            " AND aetitle IN (SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE site_id = :rh_site_id)"
        )

    # Secondary-query filter fragment for raw SQL against etl_didb_studies
    # (prefixed "s."), used by the volume-matrix and addendum-rate queries
    # below, which don't run against the base_data CTE.
    sec_filters = ""
    if "mod" in params:
        sec_filters += " AND UPPER(TRIM(m.modality)) = UPPER(:mod)"
    if "p_class" in params:
        sec_filters += " AND UPPER(s.patient_class) = UPPER(:p_class)"
    if rh_site_id is not None:
        sec_filters += " AND m.site_id = :rh_site_id"
    sec_needs_mod_join = ("mod" in params) or (rh_site_id is not None)

    return base_sql, start, end, params, extra_where, sec_filters, sec_needs_mod_join


def get_radiologist_performance_data(form_data):
    """
    Main data-fetch for Report 32. Returns a 3-tuple:
    (data_dict | None, start_date_str, end_date_str).
    """
    cached = cache_get(32, form_data)
    if cached is not None:
        return cached

    base_sql, start, end, params, extra_where, sec_filters, sec_needs_mod_join = get_report_config(form_data)
    if not base_sql:
        return None, start, end

    # SR exclusion (mandatory convention — see CLAUDE.md): every query
    # touching etl_didb_studies (directly, or via this CTE) must filter SR.
    # OT is excluded alongside it, matching Report 25's own convention.
    sql_exec = text(f"""
        WITH base_data AS ({base_sql})
        SELECT reading_radiologist, modality, patient_location,
               total_tat_min, proc_duration, clinical_rvu, technical_rvu
        FROM base_data
        WHERE study_date BETWEEN :start AND :end
          AND COALESCE(modality, '') NOT IN ('SR', 'OT')
          {extra_where}
    """)
    df = pd.DataFrame(db.session.execute(sql_exec, params).mappings().all())

    if df.empty:
        logger.warning("Report 32 query returned 0 rows (start=%s end=%s)", start, end)
        return None, start, end

    # Defensive numeric cleaning (mirrors report_25.py's get_gold_standard_data)
    for col in ['total_tat_min', 'proc_duration', 'clinical_rvu', 'technical_rvu']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(1.0 if col.endswith('_rvu') else 0)
        else:
            df[col] = 1.0 if col.endswith('_rvu') else 0.0

    # Apply physician alias mapping so migrated name variants collapse to canonical
    try:
        from utils.physician_aliases import get_alias_dict as _get_aliases
        _alias_dict = _get_aliases()
        if _alias_dict and 'reading_radiologist' in df.columns:
            df['reading_radiologist'] = df['reading_radiologist'].map(
                lambda x: _alias_dict.get(x, x) if x else x
            )
    except Exception:
        pass

    # ── Radiologist performance cards ─────────────────────────────────────
    # NOTE: the base template (report_template.report_id=25) already
    # restricts to rows with a non-null TAT AND a non-null radiologist (see
    # migration 0075) — i.e. only signed/reported studies reach this
    # DataFrame at all. report_25.py's rad_cards builder additionally checks
    # rep_final_timestamp / study_has_report as a defensive filter, but
    # neither column is selected here (kept to "only the columns we need"
    # per the CTE), so that extra check is a no-op we've omitted.
    rad_cards = []
    if 'reading_radiologist' in df.columns:
        _df_rads = df[df['reading_radiologist'].notna() & (df['reading_radiologist'] != '')]
        loc_col = 'patient_location' if 'patient_location' in df.columns else 'modality'
        for rad, r_df in _df_rads.groupby('reading_radiologist'):
            drill = []
            for loc, l_df in r_df.groupby(loc_col):
                mods = [
                    {"m": m, "avg": round(m_df['total_tat_min'].mean(), 1),
                     "count": len(m_df), "rvu": round(m_df['clinical_rvu'].sum(), 1)}
                    for m, m_df in l_df.groupby('modality')
                ]
                drill.append({"loc": loc, "mods": mods, "loc_rvu": round(l_df['clinical_rvu'].sum(), 1)})

            # Only count studies with a mapped duration — unmapped studies (0 min)
            # would contribute RVU without time, inflating the rate
            r_df_mapped = r_df[r_df['proc_duration'] > 0]
            total_scan_hours = r_df_mapped['proc_duration'].sum() / 60
            rvu_per_hour = round(r_df_mapped['clinical_rvu'].sum() / total_scan_hours, 2) if total_scan_hours > 0 else 0.0

            r_df_valid = r_df[r_df['total_tat_min'] > 0]
            rad_cards.append({
                "name": rad,
                "count": int(len(r_df)),
                "overall": round(r_df_valid['total_tat_min'].mean(), 1) if len(r_df_valid) > 0 else 0.0,
                "tat_median": round(float(r_df[r_df['total_tat_min'] > 0]['total_tat_min'].median()), 1) if (r_df['total_tat_min'] > 0).any() else 0.0,
                "total_rvu": round(r_df['clinical_rvu'].sum(), 1),
                "total_technical_rvu": round(r_df['technical_rvu'].sum(), 1),
                "rvu_per_hour": rvu_per_hour,
                "drilldown": drill,
            })

        # Add percentile rank among peers (lower TAT = better = lower percentile)
        peer_tats = sorted([r['overall'] for r in rad_cards if r['overall'] > 0])
        n = len(peer_tats)
        for r in rad_cards:
            if r['overall'] > 0 and n > 0:
                rank = sum(1 for t in peer_tats if t <= r['overall'])
                r['tat_percentile'] = round(rank / n * 100)
            else:
                r['tat_percentile'] = None
        rad_cards.sort(key=lambda r: -r['count'])

    # ── Department summary (for the KPI row) ──────────────────────────────
    tat_vals = df[df['total_tat_min'] > 0]['total_tat_min'] if 'total_tat_min' in df.columns else pd.Series([], dtype=float)
    summary = {
        "total_studies": int(len(df)),
        "total_radiologists": int(df['reading_radiologist'].nunique()) if 'reading_radiologist' in df.columns else 0,
        "dept_avg_tat": round(float(tat_vals.mean()), 1) if len(tat_vals) > 0 else 0.0,
        "dept_median_tat": round(float(tat_vals.median()), 1) if len(tat_vals) > 0 else 0.0,
        "total_clinical_rvu": round(float(df['clinical_rvu'].sum()), 1) if 'clinical_rvu' in df.columns else 0.0,
        "total_technical_rvu": round(float(df['technical_rvu'].sum()), 1) if 'technical_rvu' in df.columns else 0.0,
    }

    # ── Reports per radiologist × modality / AE title / procedure / month ─
    # Ported verbatim from report_25.py's get_gold_standard_data() (roughly
    # lines 675-748). Runs against etl_didb_studies directly (not the
    # base_data CTE above) because it needs signing_physician_first/last_name
    # + physician_alias_map resolution, which the report_id=25 template
    # doesn't expose.
    rad_volume_matrix = {"by_modality": [], "by_aetitle": [], "by_procedure": [], "by_month": []}
    try:
        _RAD_BASE = ("COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                     "s.signing_physician_last_name)),''),s.rep_final_signed_by,'Unknown')")
        _PAM = ("LEFT JOIN physician_alias_map pam "
                "ON pam.dismissed = false "
                "AND pam.alias = COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                "s.signing_physician_last_name)),''),s.rep_final_signed_by)")
        _RAD = "COALESCE(pam.canonical_name, " + _RAD_BASE + ")"
        _RAD_OK = (f"AND {_RAD} NOT IN ('','Unknown')"
                   f" AND s.rep_final_timestamp IS NOT NULL")
        _MJ = "LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))"

        rad_volume_matrix["by_modality"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD} AS radiologist,
                   COALESCE(UPPER(m.modality), 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {_MJ} {_PAM}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {sec_filters} {_RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """), params).mappings().fetchall()]

        rad_volume_matrix["by_aetitle"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD} AS radiologist,
                   COALESCE(s.storing_ae, 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if sec_needs_mod_join else ""}
            {_PAM}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(s.study_modality, '') != 'SR'
              {sec_filters} {_RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """), params).mappings().fetchall()]

        rad_volume_matrix["by_procedure"] = [dict(r) for r in db.session.execute(text(f"""
            WITH top_procs AS (
                SELECT s.procedure_code
                FROM etl_didb_studies s
                {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if sec_needs_mod_join else ""}
                WHERE s.study_date BETWEEN :start AND :end
                  AND s.rep_final_timestamp IS NOT NULL
                  AND s.procedure_code IS NOT NULL AND s.procedure_code != ''
                  {sec_filters}
                GROUP BY 1 ORDER BY COUNT(DISTINCT s.study_db_uid) DESC LIMIT 60
            )
            SELECT {_RAD} AS radiologist,
                   s.procedure_code AS proc,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if sec_needs_mod_join else ""}
            {_PAM}
            JOIN top_procs tp ON tp.procedure_code = s.procedure_code
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(s.study_modality, '') != 'SR'
              {sec_filters} {_RAD_OK}
            GROUP BY 1, 2 ORDER BY 2, 3 DESC
        """), params).mappings().fetchall()]

        rad_volume_matrix["by_month"] = [dict(r) for r in db.session.execute(text(f"""
            SELECT {_RAD} AS radiologist,
                   TO_CHAR(s.study_date, 'YYYY-MM') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {_MJ} {_PAM}
            WHERE s.study_date BETWEEN :start AND :end
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
              {sec_filters} {_RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 2
        """), params).mappings().fetchall()]
    except Exception:
        logger.exception("Failed to build radiologist x modality/AE/procedure volume matrix")
        db.session.rollback()

    # ── Addendum rate by radiologist ───────────────────────────────────────
    # Ported verbatim from report_25.py's get_gold_standard_data() (roughly
    # lines 643-673). The old template placed this inside tab-workflow, which
    # had no nav button and was therefore never reachable — first working UI
    # for it lives here.
    addendum_data = {'overall_pct': 0.0, 'total_addenda': 0, 'by_rad': []}
    try:
        add_rows = db.session.execute(text(f"""
            SELECT
                COALESCE(pam.canonical_name, s.rep_final_signed_by, 'Unknown') AS radiologist,
                COUNT(*) AS total,
                SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END) AS addendum_count,
                ROUND(SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS addendum_pct
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if sec_needs_mod_join else ""}
            LEFT JOIN physician_alias_map pam ON pam.dismissed = false AND pam.alias = s.rep_final_signed_by
            WHERE s.study_date BETWEEN :start AND :end
              AND s.rep_final_signed_by IS NOT NULL
              AND s.rep_final_timestamp IS NOT NULL
              {sec_filters}
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY addendum_pct DESC
        """), params).fetchall()
        by_rad = [
            {'rad': r[0], 'total': int(r[1]), 'addendum_count': int(r[2]), 'pct': float(r[3] or 0)}
            for r in add_rows
        ]
        total_studies_add = sum(r['total'] for r in by_rad)
        total_addenda = sum(r['addendum_count'] for r in by_rad)
        overall_pct = round(total_addenda / total_studies_add * 100, 1) if total_studies_add > 0 else 0.0
        addendum_data = {'overall_pct': overall_pct, 'total_addenda': total_addenda, 'by_rad': by_rad}
    except Exception:
        logger.exception("Failed to load addendum rate data")
        db.session.rollback()

    # ── Reporting Cadence Analysis (radiologist shift patterns) ────────────
    # Moved here from Report 35 ("Technician TAT", routes/report_35.py), which
    # had inherited it from report_25.compute_bg_data() (roughly lines 852-937)
    # — it's radiologist signing-time behaviour, not technician activity, so
    # it belongs in this report instead. Runs its own self-contained query
    # directly against etl_didb_studies (same pattern as the volume-matrix and
    # addendum-rate sections above, not the base_data CTE), reusing this
    # function's own sec_filters/sec_needs_mod_join from get_report_config()
    # so it stays in sync with this report's Modality / Patient Class filters
    # — no dependency on Report 35 or its cache slot.
    shift_patterns = {}
    try:
        _BREAK_MIN = 20
        _SP_RAD_BASE = ("COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                        "s.signing_physician_last_name)),''),s.rep_final_signed_by,'Unknown')")
        _SP_PAM = ("LEFT JOIN physician_alias_map pam "
                   "ON pam.dismissed = false "
                   "AND pam.alias = COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                   "s.signing_physician_last_name)),''),s.rep_final_signed_by)")
        _SP_RAD = "COALESCE(pam.canonical_name, " + _SP_RAD_BASE + ")"
        # SR/OT exclusion (mandatory convention — see CLAUDE.md); fall back to
        # study_modality when the aetitle_modality_map join isn't in play,
        # same conditional pattern the volume-matrix queries above use.
        _sp_sr_ot_filter = (
            "AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')"
            if sec_needs_mod_join else
            "AND COALESCE(s.study_modality, '') NOT IN ('SR', 'OT')"
        )

        ts_rows = db.session.execute(text(f"""
            SELECT
                {_SP_RAD} AS radiologist,
                s.rep_final_timestamp,
                s.accession_number
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if sec_needs_mod_join else ""}
            {_SP_PAM}
            WHERE s.rep_final_timestamp IS NOT NULL
              AND s.rep_final_timestamp::date BETWEEN :start AND :end
              {_sp_sr_ot_filter}
              {sec_filters}
            ORDER BY 1, 2
        """), params).fetchall()

        if ts_rows:
            ts_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts', 'accession_number'])
            ts_df['ts']        = pd.to_datetime(ts_df['ts'])
            ts_df['work_date'] = ts_df['ts'].dt.date
            ts_df['hour']      = ts_df['ts'].dt.hour
            ts_df['dow']       = ts_df['ts'].dt.dayofweek

            def _h_to_hhmm(h):
                hh = int(h); mm = int(round((h - hh) * 60))
                return f"{hh:02d}:{mm:02d}"

            for rad, rdf in ts_df.groupby('radiologist'):
                if rad.strip() in ('Unknown', ''):
                    continue
                rdf = rdf.sort_values('ts')
                hm = rdf.groupby(['dow', 'hour']).size().reset_index(name='cnt')
                heatmap = [[int(r['hour']), int(r['dow']), int(r['cnt'])] for _, r in hm.iterrows()]
                hm_max  = int(hm['cnt'].max()) if not hm.empty else 1
                arrivals, departures, break_cnts, break_durs = [], [], [], []
                daily_log = []
                for work_date, ddf in rdf.groupby('work_date'):
                    times = ddf['ts'].sort_values().tolist()
                    first, last = times[0], times[-1]
                    arr_h = first.hour + first.minute / 60
                    dep_h = last.hour  + last.minute  / 60
                    arrivals.append(arr_h)
                    departures.append(dep_h)
                    breaks = []
                    for i in range(1, len(times)):
                        gap = (times[i] - times[i - 1]).total_seconds() / 60
                        if gap >= _BREAK_MIN:
                            dur = round(gap)
                            icon = '☕' if dur <= 35 else ('🚬' if dur <= 70 else '🏃')
                            kind = 'Coffee' if dur <= 35 else ('Long break' if dur <= 70 else 'Disappeared')
                            breaks.append({'start': times[i-1].strftime('%H:%M'),
                                           'end':   times[i].strftime('%H:%M'),
                                           'duration': dur, 'icon': icon, 'kind': kind})
                    break_cnts.append(len(breaks))
                    break_durs.extend([b['duration'] for b in breaks])
                    daily_log.append({
                        'date': str(work_date), 'dow': first.strftime('%a'),
                        'arrival': first.strftime('%H:%M'), 'departure': last.strftime('%H:%M'),
                        'studies': len(times),
                        'span_h': round(dep_h - arr_h, 1),
                        'breaks': breaks,
                    })
                wd = len(daily_log)
                shift_patterns[rad] = {
                    'avg_arrival':    _h_to_hhmm(sum(arrivals)   / len(arrivals))   if arrivals   else '—',
                    'avg_departure':  _h_to_hhmm(sum(departures) / len(departures)) if departures else '—',
                    'avg_breaks_day': round(sum(break_cnts) / wd, 1)                if wd         else 0,
                    'avg_break_dur':  int(round(sum(break_durs) / len(break_durs))) if break_durs else 0,
                    'working_days':   wd,
                    'heatmap':        heatmap,
                    'hm_max':         hm_max,
                    'daily_log':      daily_log[-60:],
                }
    except Exception:
        logger.exception("Failed to compute shift patterns")
        db.session.rollback()

    result = ({
        "summary": summary,
        "rad_cards": rad_cards,
        "rad_volume_matrix": rad_volume_matrix,
        "addendum_data": addendum_data,
        "shift_patterns": shift_patterns,
    }, start, end)
    cache_put(32, form_data, result)
    return result


@report_32_bp.route("/report/32", methods=["GET", "POST"])
@login_required
def report_32():
    run_report = 'start_date' in request.values

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end = date.today().strftime("%Y-%m-%d")

    filters = {
        "f_mod_active": request.values.get("f_mod_active") == "on",
        "f_class_active": request.values.get("f_class_active") == "on",
        "f_mod": request.values.get("f_mod"),
        "f_class": request.values.get("f_class"),
    }

    data = None
    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_32',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date')})
        data, display_start, display_end = get_radiologist_performance_data(request.values)

    return render_template(
        "report_32.html",
        data=data,
        run_report=run_report,
        display_start=display_start,
        display_end=display_end,
        filters=filters,
    )


@report_32_bp.route("/report/32/export", methods=["POST"])
@login_required
def export_report_32():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403

    data, start, end = get_radiologist_performance_data(request.form)
    if not data:
        return "No data", 400

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Radiologist', 'Study Count', 'Avg TAT (min)', 'Median TAT (min)',
                      'Clinical RVU', 'Technical RVU', 'RVU/hr', 'TAT Percentile',
                      'Addendum Count', 'Addendum %'])
    addendum_by_rad = {r['rad']: r for r in data.get('addendum_data', {}).get('by_rad', [])}
    for r in data.get('rad_cards', []):
        add = addendum_by_rad.get(r['name'], {})
        writer.writerow([
            r['name'], r['count'], r['overall'], r['tat_median'],
            r['total_rvu'], r['total_technical_rvu'], r['rvu_per_hour'],
            r.get('tat_percentile', ''),
            add.get('addendum_count', ''), add.get('pct', ''),
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=radiologist_performance_{start}_to_{end}.csv"},
    )


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(32, report_32_bp, report_32, export_report_32)
