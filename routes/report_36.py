"""
routes/report_36.py
-------------------
Report 36 — Radiology Reporting Analytics.

Split out of Report 25's "Radiologists" tab (CHN). Same panels, same numbers:
statistical insights, TAT heatmap (radiologist x modality), peer ranking,
reports per radiologist (by modality / AE / month / procedure), and reporting
cadence (signing density + daily log).

Every count here follows one rule (utils/radiologist_sql.py): a report is
counted on the day it was FINALIZED, not the study day. Unlike Report 25, this
page does not depend on Report 25's study-date dataset, so a period with
signed reports always shows them.

PACS data only. LAUMC/HL7's Report 36 panels that need RIS tables (resident vs
radiologist TAT, patient wait time) are not ported — CHN has no RIS access.

Everything is computed in one request (no background fetch): there are no
other tabs competing for a fast first paint.
"""
import logging
from datetime import date

import pandas as pd
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text

from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from routes.insights_engine import run_rad_insights
from utils.radiologist_sql import MOD_JOIN, ALIAS_JOIN, RAD_NAME, RAD_OK, FINAL_IN_RANGE, NOT_SR

logger = logging.getLogger("report_36")

report_36_bp = Blueprint("report_36", __name__)


def _filters(form_data):
    """Date range + sidebar filters → (start, end, params, sql fragment on `s`/`m`)."""
    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    params = {"start": start, "end": end}

    sec = (" AND UPPER(TRIM(s.original_storing_ae)) NOT IN "
           "(SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE exclude_from_stats = TRUE)")
    if form_data.get("class_enabled") == "on" and form_data.getlist("patient_class"):
        params["classes"] = tuple(form_data.getlist("patient_class"))
        sec += " AND s.patient_class IN :classes"
    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        params["modalities"] = tuple(form_data.getlist("modality"))
        sec += " AND UPPER(TRIM(m.modality)) IN :modalities"
    if form_data.get("ae_enabled") == "on" and form_data.getlist("aetitle"):
        params["aetitles"] = tuple(form_data.getlist("aetitle"))
        sec += " AND s.original_storing_ae IN :aetitles"
    if form_data.get("loc_enabled") == "on" and form_data.getlist("patient_location"):
        params["locations"] = tuple(form_data.getlist("patient_location"))
        sec += " AND s.patient_location IN :locations"
    return start, end, params, sec


def _rad_cards(params, sec):
    """Per-radiologist TAT / volume / RVU cards (peer ranking + heatmap)."""
    rad_cards = []
    rows = db.session.execute(text(f"""
        SELECT
            {RAD_NAME} AS reading_radiologist,
            COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'Unknown') AS modality,
            COALESCE(s.patient_location, '') AS patient_location,
            GREATEST(EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date::timestamp)) / 60, 0) AS total_tat_min,
            COALESCE(pm.duration_minutes, 0) AS proc_duration,
            COALESCE(pm.clinical_rvu, 1.0) AS clinical_rvu
        FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
        LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
        WHERE {FINAL_IN_RANGE}
          AND {NOT_SR}
          AND COALESCE(m.exclude_from_stats, FALSE) = FALSE
          {sec} {RAD_OK}
    """), params).mappings().all()
    rc_df = pd.DataFrame(rows)
    if rc_df.empty:
        return rad_cards

    for col in ['total_tat_min', 'proc_duration', 'clinical_rvu']:
        rc_df[col] = pd.to_numeric(rc_df[col], errors='coerce').fillna(0)
    for rad, r_df in rc_df.groupby('reading_radiologist'):
        drill = []
        for loc, l_df in r_df.groupby('patient_location'):
            mods = [{"m": m, "avg": round(m_df['total_tat_min'].mean(), 1), "count": len(m_df),
                     "rvu": round(m_df['clinical_rvu'].sum(), 1)} for m, m_df in l_df.groupby('modality')]
            drill.append({"loc": loc or 'Unknown', "mods": mods, "loc_rvu": round(l_df['clinical_rvu'].sum(), 1)})
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
            "tat_median": round(float(r_df_valid['total_tat_min'].median()), 1) if len(r_df_valid) > 0 else 0.0,
            "total_rvu": round(r_df['clinical_rvu'].sum(), 1),
            "rvu_per_hour": rvu_per_hour,
            "drilldown": drill,
        })

    peer_tats = sorted(r['overall'] for r in rad_cards if r['overall'] > 0)
    n = len(peer_tats)
    for r in rad_cards:
        if r['overall'] > 0 and n > 0:
            rank = sum(1 for t in peer_tats if t <= r['overall'])
            r['tat_percentile'] = round(rank / n * 100)
        else:
            r['tat_percentile'] = None
    return rad_cards


def _volume_matrix(params, sec):
    """Reports per radiologist × modality / AE title / month / top procedures."""
    def q(sql):
        return [dict(r) for r in db.session.execute(text(sql), params).mappings().fetchall()]

    return {
        "by_modality": q(f"""
            SELECT {RAD_NAME} AS radiologist,
                   COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
            WHERE {FINAL_IN_RANGE}
              AND {NOT_SR}
              AND COALESCE(m.exclude_from_stats, FALSE) = FALSE
              {sec} {RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """),
        "by_aetitle": q(f"""
            SELECT {RAD_NAME} AS radiologist,
                   COALESCE(s.original_storing_ae, 'Unknown') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
            WHERE {FINAL_IN_RANGE}
              AND {NOT_SR}
              {sec} {RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """),
        "by_procedure": q(f"""
            WITH top_procs AS (
                SELECT s.procedure_code
                FROM etl_didb_studies s {MOD_JOIN}
                WHERE {FINAL_IN_RANGE}
                  AND {NOT_SR}
                  AND s.procedure_code IS NOT NULL AND s.procedure_code != ''
                  {sec}
                GROUP BY 1 ORDER BY COUNT(DISTINCT s.study_db_uid) DESC LIMIT 60
            )
            SELECT {RAD_NAME} AS radiologist,
                   s.procedure_code AS proc,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
            JOIN top_procs tp ON tp.procedure_code = s.procedure_code
            WHERE {FINAL_IN_RANGE}
              AND {NOT_SR}
              {sec} {RAD_OK}
            GROUP BY 1, 2 ORDER BY 2, 3 DESC
        """),
        "by_month": q(f"""
            SELECT {RAD_NAME} AS radiologist,
                   TO_CHAR(DATE(s.rep_final_timestamp), 'YYYY-MM') AS dim,
                   COUNT(DISTINCT s.study_db_uid) AS cnt
            FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
            WHERE {FINAL_IN_RANGE}
              AND {NOT_SR}
              AND COALESCE(m.exclude_from_stats, FALSE) = FALSE
              {sec} {RAD_OK}
            GROUP BY 1, 2 ORDER BY 1, 2
        """),
    }


def _shift_patterns(params, sec):
    """Reporting cadence per radiologist from signing timestamps.

    Returns (shift_patterns dict, ts_rows) — ts_rows also feeds the insights engine.
    """
    _BREAK_MIN = 20
    shift_patterns = {}
    ts_rows = db.session.execute(text(f"""
        SELECT {RAD_NAME} AS radiologist,
               s.rep_final_timestamp,
               s.accession_number
        FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
        WHERE {FINAL_IN_RANGE}
          AND {NOT_SR}
          {sec} {RAD_OK}
        ORDER BY 1, 2
    """), params).fetchall()
    if not ts_rows:
        return shift_patterns, ts_rows

    ts_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts', 'accession_number'])
    ts_df['ts']        = pd.to_datetime(ts_df['ts'])
    ts_df['work_date'] = ts_df['ts'].dt.date
    ts_df['hour']      = ts_df['ts'].dt.hour
    ts_df['dow']       = ts_df['ts'].dt.dayofweek

    def _h_to_hhmm(h):
        hh = int(h); mm = int(round((h - hh) * 60))
        return f"{hh:02d}:{mm:02d}"

    for rad, rdf in ts_df.groupby('radiologist'):
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
            break_durs.extend(b['duration'] for b in breaks)
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
    return shift_patterns, ts_rows


def get_radiology_data(form_data):
    """All Report 36 data. Returns (data | None, start, end); None when no finalized reports."""
    cached = cache_get(36, form_data)
    if cached is not None:
        return cached

    start, end, params, sec = _filters(form_data)

    # Each panel fails on its own so one bad query never blanks the page
    rad_cards = []
    try:
        rad_cards = _rad_cards(params, sec)
    except Exception:
        logger.exception("Failed to build radiologist performance cards")
        db.session.rollback()

    rad_volume_matrix = {"by_modality": [], "by_aetitle": [], "by_procedure": [], "by_month": []}
    try:
        rad_volume_matrix = _volume_matrix(params, sec)
    except Exception:
        logger.exception("Failed to build radiologist × modality/AE/procedure volume matrix")
        db.session.rollback()

    shift_patterns, ts_rows = {}, []
    try:
        shift_patterns, ts_rows = _shift_patterns(params, sec)
    except Exception:
        logger.exception("Failed to compute reporting cadence")
        db.session.rollback()

    rad_insights = []
    try:
        signing_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts', 'accession_number']) if ts_rows else None
        rad_insights = run_rad_insights(rad_cards, signing_df)
    except Exception:
        logger.exception("Failed to run radiologist insight signals")

    if not rad_cards and not any(rad_volume_matrix.values()):
        result = (None, start, end)
    else:
        result = ({
            "summary": {
                "total_reports": sum(r['count'] for r in rad_cards),
                "radiologists":  len(rad_cards),
            },
            "rad_cards":         rad_cards,
            "rad_volume_matrix": rad_volume_matrix,
            "shift_patterns":    shift_patterns,
            "rad_insights":      rad_insights,
        }, start, end)
    cache_put(36, form_data, result)
    return result


@report_36_bp.route("/report/36", methods=["GET", "POST"])
@login_required
def report_36():
    run_report = 'start_date' in request.values
    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")
    data = None
    if run_report:
        from utils.audit import log_event
        log_event('report_run', category='report', resource_type='report_36',
                  detail={'from': request.values.get('start_date'), 'to': request.values.get('end_date')})
        data, display_start, display_end = get_radiology_data(request.values)
    return render_template("report_36.html", data=data, run_report=run_report,
                           display_start=display_start, display_end=display_end)


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(36, report_36_bp, report_36)
