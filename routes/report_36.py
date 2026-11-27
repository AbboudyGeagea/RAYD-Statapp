"""
routes/report_36.py
-------------------
Report 36 — Radiology Reporting Analytics.

Split out of Report 25's "Radiologists" tab (CHN). Same panels, same numbers:
statistical insights, TAT heatmap (radiologist x modality), peer ranking,
reports per radiologist (by modality / AE / month / procedure), and reporting
cadence (signing density + daily log). Plus the current unread backlog grouped
by the PACS "assign to radiologist" value (DIDB_STUDIES.ASSIGN_TO_DOCTOR).

TAT runs from the FIRST OPEN by a radiologist (PACS audit trail) to the final
report (utils/tat_first_open.py); a report that cannot be timed counts in the
volumes and is reported as not measured, never given a guessed start.

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
from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required
from sqlalchemy import text

from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from routes.insights_engine import run_rad_insights
from utils.radiologist_sql import MOD_JOIN, ALIAS_JOIN, RAD_NAME, RAD_OK, FINAL_IN_RANGE, NOT_SR
from utils.tat_first_open import (READERS_CTE, FIRST_OPEN_JOIN, IN_AUDIT, TAT_MIN,
                                  audit_params, audit_window)

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


def _tat_avg(series):
    """Mean of the timed studies, None when none were timed."""
    return round(float(series.mean()), 1) if series.notna().any() else None


def _rad_cards(params, sec):
    """Per-radiologist TAT / volume / RVU cards (peer ranking + heatmap).

    TAT = first open by a reader -> final report (utils/tat_first_open.py). A study that
    cannot be timed still counts in the volume and RVU, not in any TAT figure.
    Returns (cards, tat_summary).
    """
    rad_cards = []
    tat_summary = {"reports": 0, "timed": 0, "not_opened": 0, "outside_audit": 0,
                   "audit": audit_window(db.session)}
    rows = db.session.execute(text(f"""
        WITH {READERS_CTE}
        SELECT
            {RAD_NAME} AS reading_radiologist,
            COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'Unknown') AS modality,
            COALESCE(s.patient_location, '') AS patient_location,
            {IN_AUDIT} AS in_audit,
            {TAT_MIN} AS total_tat_min,
            COALESCE(pm.duration_minutes, 0) AS proc_duration,
            COALESCE(pm.clinical_rvu, 1.0) AS clinical_rvu
        FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
        LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
        {FIRST_OPEN_JOIN}
        WHERE {FINAL_IN_RANGE}
          AND {NOT_SR}
          AND COALESCE(m.exclude_from_stats, FALSE) = FALSE
          {sec} {RAD_OK}
    """), {**params, **audit_params(db.session)}).mappings().all()
    rc_df = pd.DataFrame(rows)
    if rc_df.empty:
        return rad_cards, tat_summary

    rc_df['total_tat_min'] = pd.to_numeric(rc_df['total_tat_min'], errors='coerce')   # NaN = not measured
    for col in ['proc_duration', 'clinical_rvu']:
        rc_df[col] = pd.to_numeric(rc_df[col], errors='coerce').fillna(0)
    timed = rc_df['total_tat_min'].notna()
    tat_summary.update(reports=int(len(rc_df)), timed=int(timed.sum()),
                       not_opened=int((rc_df['in_audit'] & ~timed).sum()),
                       outside_audit=int((~rc_df['in_audit']).sum()))

    for rad, r_df in rc_df.groupby('reading_radiologist'):
        drill = []
        for loc, l_df in r_df.groupby('patient_location'):
            mods = [{"m": m, "avg": _tat_avg(m_df['total_tat_min']), "count": len(m_df),
                     "timed": int(m_df['total_tat_min'].notna().sum()),
                     "rvu": round(m_df['clinical_rvu'].sum(), 1)} for m, m_df in l_df.groupby('modality')]
            drill.append({"loc": loc or 'Unknown', "mods": mods, "loc_rvu": round(l_df['clinical_rvu'].sum(), 1)})
        # Only count studies with a mapped duration — unmapped studies (0 min)
        # would contribute RVU without time, inflating the rate
        r_df_mapped = r_df[r_df['proc_duration'] > 0]
        total_scan_hours = r_df_mapped['proc_duration'].sum() / 60
        rvu_per_hour = round(r_df_mapped['clinical_rvu'].sum() / total_scan_hours, 2) if total_scan_hours > 0 else 0.0
        r_tat = r_df['total_tat_min'].dropna()
        rad_cards.append({
            "name": rad,
            "count": int(len(r_df)),
            "timed": int(len(r_tat)),
            "overall": _tat_avg(r_tat),
            "tat_median": round(float(r_tat.median()), 1) if len(r_tat) else None,
            "total_rvu": round(r_df['clinical_rvu'].sum(), 1),
            "rvu_per_hour": rvu_per_hour,
            "drilldown": drill,
        })

    peer_tats = sorted(r['overall'] for r in rad_cards if r['overall'] is not None)
    n = len(peer_tats)
    for r in rad_cards:
        if r['overall'] is not None and n > 0:
            rank = sum(1 for t in peer_tats if t <= r['overall'])
            r['tat_percentile'] = round(rank / n * 100)
        else:
            r['tat_percentile'] = None
    return rad_cards, tat_summary


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
        # Procedures are shown by their description from the order table (the most
        # common etl_orders.proc_text for the code); codes with no order text keep
        # the code. Codes sharing one description count as one row.
        "by_procedure": q(f"""
            WITH proc_desc AS MATERIALIZED (
                SELECT UPPER(TRIM(proc_id)) AS code,
                       MODE() WITHIN GROUP (ORDER BY TRIM(proc_text)) AS descr
                FROM etl_orders
                WHERE NULLIF(TRIM(proc_id), '') IS NOT NULL
                  AND NULLIF(TRIM(proc_text), '') IS NOT NULL
                GROUP BY 1
            ),
            reported AS (
                SELECT {RAD_NAME} AS radiologist,
                       COALESCE(pd.descr, TRIM(s.procedure_code)) AS proc,
                       s.study_db_uid
                FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
                LEFT JOIN proc_desc pd ON pd.code = UPPER(TRIM(s.procedure_code))
                WHERE {FINAL_IN_RANGE}
                  AND {NOT_SR}
                  AND NULLIF(TRIM(s.procedure_code), '') IS NOT NULL
                  {sec} {RAD_OK}
            ),
            top_procs AS (
                SELECT proc FROM reported
                GROUP BY 1 ORDER BY COUNT(DISTINCT study_db_uid) DESC LIMIT 60
            )
            SELECT r.radiologist, r.proc, COUNT(DISTINCT r.study_db_uid) AS cnt
            FROM reported r
            JOIN top_procs USING (proc)
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


def _unread_by_assignee(params, sec):
    """Unread studies of the selected period, grouped by the PACS "assign to radiologist" value.

    The period is the study date (unread studies have no final date), so a 2025 filter
    never shows a radiologist who joined in 2026. Status and assignee are as of the last
    studies sync. Unread = PACS study status unread, same as the home page and Report 25.
    Assignee logins (DBUSTROS@DN) become display names through physician_alias_map, else
    the name on reports that login signed, else the login.

    Caveat: the studies ETL re-reads only the last 10 days of study dates
    (ETL_JOBS/etl_didb_studies.py), so a study read later than that keeps "unread" here.
    """
    rows = db.session.execute(text(f"""
        WITH login_names AS (
            SELECT UPPER(TRIM(s.rep_final_signed_by)) AS login,
                   MODE() WITHIN GROUP (ORDER BY {RAD_NAME}) AS rad_name
            FROM etl_didb_studies s {ALIAS_JOIN}
            WHERE s.rep_final_signed_by IS NOT NULL
              AND s.rep_final_timestamp >= CURRENT_DATE - 365
              {RAD_OK}
            GROUP BY 1
        ),
        unread AS (
            SELECT s.assign_to_doctor AS raw_assignee,
                   NULLIF(UPPER(TRIM(SPLIT_PART(s.assign_to_doctor, '@', 1))), '') AS login,
                   COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'Unknown') AS modality,
                   s.study_date
            FROM etl_didb_studies s {MOD_JOIN}
            WHERE s.study_status ILIKE '%unread%'
              AND s.study_date BETWEEN CAST(:start AS date) AND CAST(:end AS date)
              AND {NOT_SR}
              {sec}
        )
        SELECT COALESCE(pa.canonical_name, ln.rad_name, u.login) AS assignee,
               u.modality,
               COUNT(*)          AS cnt,
               MIN(u.study_date) AS oldest
        FROM unread u
        LEFT JOIN physician_alias_map pa ON pa.dismissed = false AND pa.alias = u.raw_assignee
        LEFT JOIN login_names ln ON ln.login = u.login
        GROUP BY 1, 2
    """), params).mappings().all()

    groups = {}
    for r in rows:
        g = groups.setdefault(r['assignee'], {"assignee": r['assignee'], "total": 0, "mods": {}, "oldest": None})
        g["total"] += int(r['cnt'])
        g["mods"][r['modality']] = int(r['cnt'])
        if g["oldest"] is None or r['oldest'] < g["oldest"]:
            g["oldest"] = r['oldest']
    today = date.today()
    for g in groups.values():
        g["mods"] = sorted(g["mods"].items(), key=lambda kv: -kv[1])
        g["waiting_days"] = (today - g["oldest"]).days if g["oldest"] else None
        g["oldest"] = str(g["oldest"]) if g["oldest"] else None

    not_assigned = groups.pop(None, None)
    assigned = sorted(groups.values(), key=lambda g: (-g["total"], g["assignee"]))

    as_of = db.session.execute(text(
        "SELECT MAX(end_time) FROM etl_job_log WHERE job_name = 'STUDIES_ETL' AND status = 'SUCCESS'"
    )).scalar()
    return {
        "start":        params["start"],
        "end":          params["end"],
        "assigned":     assigned,
        "not_assigned": not_assigned,
        "total":        sum(g["total"] for g in assigned) + (not_assigned["total"] if not_assigned else 0),
        "assigned_total": sum(g["total"] for g in assigned),
        "as_of":        as_of.strftime("%Y-%m-%d %H:%M") if as_of else None,
    }


def get_radiology_data(form_data):
    """All Report 36 data. Returns (data | None, start, end); None when no finalized reports."""
    cached = cache_get(36, form_data)
    if cached is not None:
        return cached

    start, end, params, sec = _filters(form_data)

    # Each panel fails on its own so one bad query never blanks the page
    rad_cards, tat_summary = [], None
    try:
        rad_cards, tat_summary = _rad_cards(params, sec)
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

    unread_assign = None
    try:
        unread_assign = _unread_by_assignee(params, sec)
    except Exception:
        logger.exception("Failed to build unread backlog by assigned radiologist")
        db.session.rollback()

    if not rad_cards and not any(rad_volume_matrix.values()) and not (unread_assign and unread_assign["total"]):
        result = (None, start, end)
    else:
        result = ({
            "summary": {
                "total_reports": sum(r['count'] for r in rad_cards),
                "radiologists":  len(rad_cards),
            },
            "rad_cards":         rad_cards,
            "tat_summary":       tat_summary,
            "rad_volume_matrix": rad_volume_matrix,
            "shift_patterns":    shift_patterns,
            "rad_insights":      rad_insights,
            "unread_assign":     unread_assign,
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


@report_36_bp.route("/report/36/cadence-cell")
@login_required
def cadence_cell():
    """Accessions behind one Reporting Cadence box: the reports one radiologist
    finalized on one weekday (0 = Mon) and hour, with the page's period and filters.
    Same rows as _shift_patterns, so the list length equals the box's count."""
    rad = (request.args.get("rad") or "").strip()
    try:
        dow, hour = int(request.args["dow"]), int(request.args["hour"])
    except (KeyError, ValueError):
        return jsonify({"error": "dow and hour are required"}), 400
    if not rad or not (0 <= dow <= 6 and 0 <= hour <= 23):
        return jsonify({"error": "invalid radiologist, day or hour"}), 400

    _, _, params, sec = _filters(request.args)
    rows = db.session.execute(text(f"""
        SELECT s.accession_number,
               s.rep_final_timestamp,
               COALESCE(UPPER(m.modality), UPPER(s.study_modality), 'Unknown') AS modality
        FROM etl_didb_studies s {MOD_JOIN} {ALIAS_JOIN}
        WHERE {FINAL_IN_RANGE}
          AND {NOT_SR}
          AND {RAD_NAME} = :rad
          AND EXTRACT(ISODOW FROM s.rep_final_timestamp) = :isodow
          AND EXTRACT(HOUR FROM s.rep_final_timestamp) = :hour
          {sec} {RAD_OK}
        ORDER BY s.rep_final_timestamp
    """), {**params, "rad": rad, "isodow": dow + 1, "hour": hour}).fetchall()
    return jsonify({"rows": [{
        "accession": r.accession_number or "—",
        "signed":    r.rep_final_timestamp.strftime("%Y-%m-%d %H:%M"),
        "modality":  r.modality,
    } for r in rows]})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(36, report_36_bp, report_36)
