import json
import logging
import numpy as np
import pandas as pd
from datetime import date, timedelta
from flask import Blueprint, render_template, request, abort, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, get_go_live_date, user_has_page
from routes.report_cache import cache_get, cache_put

logger = logging.getLogger("REPORT_AI")
report_ai_bp = Blueprint("report_ai", __name__)

# Report ID used as the cache namespace for all AI-intelligence sections.
# Must not collide with numeric report IDs used by other routes.
_AI_CACHE_REPORT_ID = 9900

# This page is trend/forecast/anomaly analysis, not a live operational view --
# nothing here needs to be fresher than an hour. The shared report_cache's
# default TTL (5 min) meant almost every open recomputed all four sections
# from scratch (each a full-history scan + numpy/pandas regression), since 5
# minutes rarely elapses between opens. A 1-hour TTL cuts that down to ~once
# per hour per distinct date range instead of ~once per open.
_AI_CACHE_TTL = 3600


def _ai_cache_get(section: str, start: str, end: str):
    """Look up a cached AI-intelligence section result."""
    return cache_get(_AI_CACHE_REPORT_ID, {"section": section, "start": start, "end": end}, ttl=_AI_CACHE_TTL)


def _ai_cache_put(section: str, start: str, end: str, data) -> None:
    """Store an AI-intelligence section result in the shared cache."""
    cache_put(_AI_CACHE_REPORT_ID, {"section": section, "start": start, "end": end}, data, ttl=_AI_CACHE_TTL)

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _linear_forecast(dates, values, forecast_days=90):
    """Simple linear regression forecast. Returns (forecast_dates, forecast_values, r2)."""
    if len(values) < 5:
        return [], [], 0, 0
    x = np.arange(len(values))
    coeffs = np.polyfit(x, values, 1)
    slope, intercept = coeffs
    ss_res = np.sum((np.array(values) - np.polyval(coeffs, x)) ** 2)
    ss_tot = np.sum((np.array(values) - np.mean(values)) ** 2)
    r2 = round(max(0, 1 - (ss_res / ss_tot)) if ss_tot > 0 else 0, 3)

    last_date = pd.to_datetime(dates[-1])
    future_x = np.arange(len(values), len(values) + forecast_days)
    future_dates = [(last_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, forecast_days + 1)]
    future_vals = [max(0, round(np.polyval(coeffs, xi), 1)) for xi in future_x]
    return future_dates, future_vals, r2, round(slope, 2)


def _detect_anomalies(values, threshold=2.0, dates=None):
    """Returns list of booleans — True = anomaly.
    If dates are provided, uses weekday-aware detection (compares each day
    against its own weekday's mean/std) to avoid flagging normal Mon-vs-Sun
    differences as anomalies."""
    if len(values) < 4:
        return [False] * len(values)
    arr = np.array(values, dtype=float)

    if dates is not None and len(dates) == len(values):
        dows = np.array([pd.to_datetime(d).weekday() for d in dates])
        flags = []
        for i, v in enumerate(arr):
            same_dow = arr[dows == dows[i]]
            if len(same_dow) < 3:
                # Not enough same-weekday samples — fall back to global
                mean, std = arr.mean(), arr.std()
            else:
                mean, std = same_dow.mean(), same_dow.std()
            flags.append(bool(std > 0 and abs(v - mean) > threshold * std))
        return flags

    mean, std = arr.mean(), arr.std()
    if std == 0:
        return [False] * len(values)
    return [bool(abs(v - mean) > threshold * std) for v in values]



def _generate_explanation(section, data):
    """Generate plain-language explanation for each section."""
    explanations = {}

    if section == "storage":
        gb = data.get("current_gb", 0)
        daily_growth = data.get("daily_growth_gb", 0)
        days_to_full = data.get("days_to_full")
        trend = "increasing" if daily_growth > 0 else "stable"
        exp = f"Storage is currently at {gb:.1f} GB and is {trend} at {abs(daily_growth):.2f} GB/day. "
        if days_to_full and days_to_full < 365:
            exp += f"At this rate, capacity will be reached in approximately {days_to_full} days. Immediate planning is recommended."
        elif days_to_full:
            exp += f"At current growth, capacity is projected to last {days_to_full} days — no immediate concern."
        else:
            exp += "Insufficient data to project capacity exhaustion."
        explanations["storage"] = exp

    elif section == "volume":
        avg = data.get("avg_daily", 0)
        trend_slope = data.get("slope", 0)
        exp = f"Average daily study volume is {avg:.0f} studies. "
        if trend_slope > 0:
            exp += f"Volume is trending upward (+{trend_slope} studies/day)."
        elif trend_slope < 0:
            exp += f"Volume is trending downward ({trend_slope} studies/day)."
        else:
            exp += "Volume is stable."
        explanations["volume"] = exp

    elif section == "utilization":
        anomaly_count = data.get("anomaly_count", 0)
        high_stress = data.get("high_stress", [])
        low_util = data.get("low_util", [])
        exp = f"{anomaly_count} utilization anomalies detected across all AE titles. "
        if high_stress:
            exp += f"High stress (>85%): {', '.join(high_stress[:3])}. "
        if low_util:
            exp += f"Under-utilized (<30%): {', '.join(low_util[:3])}. "
        if not anomaly_count:
            exp += "All equipment is operating within normal utilization ranges."
        explanations["utilization"] = exp

    elif section == "physician":
        churning = data.get("churning", [])
        growing = data.get("growing", [])
        exp = ""
        if churning:
            exp += f"{len(churning)} physician(s) showing declining referral trend: {', '.join(churning[:3])}. "
        if growing:
            exp += f"{len(growing)} physician(s) showing growing referral activity: {', '.join(growing[:3])}. "
        if not churning and not growing:
            exp += "Referral patterns are stable across all physicians."
        explanations["physician"] = exp

    return explanations


# ─────────────────────────────────────────────
#  DATA FUNCTIONS
# ─────────────────────────────────────────────

def _get_storage_intelligence(start, end):
    cached = _ai_cache_get("storage", start, end)
    if cached is not None:
        return cached

    rows = db.session.execute(text("""
        SELECT study_date, SUM(total_gb) AS total_gb
        FROM summary_storage_daily
        WHERE study_date BETWEEN :s AND :e
        GROUP BY study_date
        ORDER BY study_date
    """), {"s": start, "e": end}).fetchall()

    if not rows or len(rows) < 3:
        return None

    dates  = [str(r[0]) for r in rows]
    values = [float(r[1]) for r in rows]

    result = _linear_forecast(dates, values, 90)
    f_dates, f_vals, r2, slope = result

    # total_gb is GB of images ADDED on that study_date (ETL_JOBS/etl_analytics_
    # refresh.py groups by study_date/storing_ae/modality/procedure_code -- it's
    # a daily delta, not a running total). current_gb must be the all-time
    # cumulative SUM across every row ever recorded, not values[-1] -- that was
    # only the last day's delta within the selected date range, which
    # understated actual archive usage by orders of magnitude and made
    # days_to_full meaningless. Deliberately unbounded by start/end: "how full
    # is the archive right now" shouldn't change just because the trend
    # chart's date range was narrowed.
    current_gb = float(db.session.execute(text(
        "SELECT COALESCE(SUM(total_gb), 0) FROM summary_storage_daily"
    )).scalar() or 0.0)

    daily_growth  = slope
    days_to_full  = None

    # User-configurable storage capacity (from settings table, default 10 TB)
    cap_row = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'storage_capacity_gb'"
    )).fetchone()
    capacity_gb = float(cap_row[0]) if cap_row else 10240.0

    remaining = capacity_gb - current_gb
    if daily_growth > 0:
        days_to_full = int(remaining / daily_growth)

    data = {
        "current_gb": current_gb,
        "daily_growth_gb": round(daily_growth, 3),
        "days_to_full": days_to_full,
        "capacity_gb": capacity_gb,
        "r2": r2,
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    f_vals
        }
    }
    data["explanation"] = _generate_explanation("storage", data)["storage"]
    _ai_cache_put("storage", start, end, data)
    return data


def _get_volume_intelligence(start, end):
    cached = _ai_cache_get("volume", start, end)
    if cached is not None:
        return cached

    rows = db.session.execute(text("""
        SELECT study_date, COUNT(*) as cnt
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
          AND COALESCE(study_modality, '') != 'SR'
        GROUP BY study_date
        ORDER BY study_date
    """), {"s": start, "e": end}).fetchall()

    if not rows or len(rows) < 5:
        return None

    dates  = [str(r[0]) for r in rows]
    values = [int(r[1]) for r in rows]

    f_dates, f_vals, r2, slope = _linear_forecast(dates, values, 90)
    anomalies = _detect_anomalies(values, dates=dates)

    avg_daily = round(np.mean(values), 1)
    cur_total = sum(values)

    # Modality breakdown — prefer procedure's true modality, fall back to AE map
    mod_rows = db.session.execute(text("""
        SELECT
            UPPER(TRIM(COALESCE(pm.modality, m.modality, s.study_modality, 'UNMAPPED'))) AS modality,
            COUNT(*) as cnt
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :s AND :e
          AND COALESCE(m.modality, s.study_modality, '') != 'SR'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """), {"s": start, "e": end}).fetchall()

    data = {
        "avg_daily": avg_daily,
        "total": cur_total,
        "slope": slope,
        "r2": r2,
        "modality_split": [{"name": r.modality or "UNMAPPED", "value": int(r.cnt)} for r in mod_rows],
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    f_vals,
            "anomaly_flags":    anomalies
        }
    }
    data["explanation"] = _generate_explanation("volume", data)["volume"]
    _ai_cache_put("volume", start, end, data)
    return data


def _get_utilization_intelligence(start, end):
    cached = _ai_cache_get("utilization", start, end)
    if cached is not None:
        return cached

    # Pull estimated load per AE per day from procedure_duration_map.
    # COALESCE(pm.duration_minutes, 15) mirrors the sitewide default applied
    # everywhere else a duration has to be estimated for a procedure code with
    # no procedure_duration_map row (report_25.get_gold_standard_data,
    # capacity_ladder.py, viewer_controller.py, ETL_JOBS/etl_ris_procedures.py's
    # _DEFAULT_DURATION). Previously this summed pm.duration_minutes directly, so
    # any unmapped procedure code contributed 0 minutes instead of the 15-min
    # estimate — with a sparsely-populated procedure_duration_map that silently
    # collapsed load_mins to ~0 for most AEs, which is why utilization always
    # came back null/empty. Also applies the SR exclusion + storing_ae guard
    # every other etl_didb_studies query in this file already uses.
    # SJHCSAPWFMFIR excluded (operator instruction): a PACS-side workflow-manager
    # forwarding node (aetitle_modality_map.modality='PACS', same non-imaging
    # infra category as the other *FIR AEs), not a real imaging device --
    # doesn't belong in device utilization.
    rows = db.session.execute(text("""
        SELECT
            s.storing_ae,
            s.study_date,
            COALESCE(SUM(COALESCE(pm.duration_minutes, 15)), 0) as load_mins
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :s AND :e
          AND s.storing_ae IS NOT NULL
          AND UPPER(TRIM(s.storing_ae)) != 'SJHCSAPWFMFIR'
          AND COALESCE(m.modality, s.study_modality, '') != 'SR'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {"s": start, "e": end}).fetchall()

    # Modality mix per AE — uses procedure's true modality for accuracy
    ae_modality_mix = {}
    mix_rows = db.session.execute(text("""
        SELECT
            s.storing_ae,
            UPPER(TRIM(COALESCE(pm.modality, am.modality, s.study_modality))) AS modality,
            COUNT(*) AS cnt
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN aetitle_modality_map am ON UPPER(TRIM(am.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :s AND :e
          AND s.storing_ae IS NOT NULL
          AND UPPER(TRIM(s.storing_ae)) != 'SJHCSAPWFMFIR'
          AND COALESCE(am.modality, s.study_modality, '') != 'SR'
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """), {"s": start, "e": end}).fetchall()
    for r in mix_rows:
        ae_modality_mix.setdefault(r[0], []).append({"modality": r[1] or "UNMAPPED", "count": int(r[2])})

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['ae', 'study_date', 'load_mins'])
    df['load_mins']  = pd.to_numeric(df['load_mins'], errors='coerce').fillna(0)
    df['study_date'] = pd.to_datetime(df['study_date'])
    df['dow']        = df['study_date'].dt.dayofweek
    df['ae_upper']   = df['ae'].astype(str).str.upper().str.strip()
    df['date_str']   = df['study_date'].dt.strftime('%Y-%m-%d')

    # Denominator priority: std_device_weekly_availability (RIS-authoritative, LAUMC —
    # real weekly Available-minutes resolved from the RIS's own schedule, see
    # ETL_JOBS/etl_ris_modality_availability.py) first, then the RAYD-manual
    # daily_capacity_minutes / device_weekly_schedule chain this always used before, then
    # a flat 480-minute default. Previously this only iterated device_weekly_schedule, so
    # any AE with no row there (every LAUMC device — Phase 8, the only writer of that
    # table, is disabled here via RAYD_ETL_LOOKUP_FROM_PACS=false) got day_cap=0 and thus
    # util_pct=0 for every single day — driving from aetitle_modality_map (the master
    # device registry, every AE regardless of source) x 7 weekdays fixes that.
    sched = db.session.execute(text("""
        SELECT
            UPPER(TRIM(m.aetitle)) AS ae,
            d.day_of_week,
            COALESCE(dwa.available_minutes, m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS std_opening_minutes
        FROM aetitle_modality_map m
        CROSS JOIN generate_series(0, 6) AS d(day_of_week)
        LEFT JOIN std_device_weekly_availability dwa
            ON UPPER(TRIM(dwa.aetitle)) = UPPER(TRIM(m.aetitle)) AND dwa.day_of_week = d.day_of_week
        LEFT JOIN device_weekly_schedule ws
            ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle)) AND ws.day_of_week = d.day_of_week
    """)).mappings().all()
    schedule_lookup = {(s['ae'], int(s['day_of_week'])): s['std_opening_minutes'] for s in sched}

    # Actual (measured) per-device-per-day minutes from std_pps (RIS Performed
    # Procedure Step — start/end timestamps), where available. Mirrors the
    # "actuals over estimate" precedence report_25.get_gold_standard_data() uses
    # for its own utilization matrix (~lines 362-389): std_pps is the real
    # measured source, procedure_duration_map above is only the estimate used
    # when a given AE/day has no PPS coverage (non-RIS sites, or dates before
    # the PPS feed started). Wrapped defensively since std_pps only exists on
    # RIS-integrated sites (migration 0065) — this file previously had no PPS
    # integration at all and relied solely on the static estimate.
    actual_lookup = {}
    try:
        pps_rows = db.session.execute(text("""
            SELECT
                UPPER(TRIM(pps.performing_ae_title)) AS ae,
                pps.start_datetime::date AS pps_date,
                SUM(EXTRACT(EPOCH FROM (pps.end_datetime - pps.start_datetime)) / 60) AS mins
            FROM std_pps pps
            JOIN etl_didb_studies s ON s.study_db_uid = pps.study_db_uid
            LEFT JOIN aetitle_modality_map am ON UPPER(TRIM(am.aetitle)) = UPPER(TRIM(s.storing_ae))
            WHERE pps.start_datetime::date BETWEEN :s AND :e
              AND pps.end_datetime IS NOT NULL
              AND pps.end_datetime > pps.start_datetime
              AND pps.performing_ae_title IS NOT NULL
              AND COALESCE(am.modality, s.study_modality, '') != 'SR'
            GROUP BY 1, 2
        """), {"s": start, "e": end}).fetchall()
        actual_lookup = {(r[0], str(r[1])): float(r[2]) for r in pps_rows}
    except Exception:
        logger.exception("[report_ai] std_pps actuals unavailable for utilization — using estimate only")
        db.session.rollback()

    # Vectorized cap/utilization computation. This used to be a per-row
    # iterrows() loop (re-run once per AE, plus a re-filter of the whole
    # DataFrame per AE via df[df['ae']==ae]) — doesn't scale on wide date
    # ranges: a year of data across dozens of AEs means tens of thousands of
    # row-by-row Python iterations. Merge + groupby scales to large row counts.
    sched_df = pd.DataFrame(
        [{"ae_upper": k[0], "dow": k[1], "day_cap": v} for k, v in schedule_lookup.items()]
    )
    if not sched_df.empty:
        df = df.merge(sched_df, on=["ae_upper", "dow"], how="left")
    else:
        df["day_cap"] = 0
    df["day_cap"] = df["day_cap"].fillna(0)

    actual_df = pd.DataFrame(
        [{"ae_upper": k[0], "date_str": k[1], "actual_mins": v} for k, v in actual_lookup.items()]
    )
    if not actual_df.empty:
        df = df.merge(actual_df, on=["ae_upper", "date_str"], how="left")
    else:
        df["actual_mins"] = np.nan

    df["effective_mins"] = df["actual_mins"].where(df["actual_mins"].notna(), df["load_mins"])
    df["util_pct"] = np.where(
        df["day_cap"] > 0,
        (df["effective_mins"] / df["day_cap"] * 100).round(1),
        0.0
    )
    # Theoretical utilization: what utilization WOULD be if every exam took
    # exactly its SPS-scheduled duration (procedure_duration_map.duration_minutes
    # -- populated from the RIS's own SPS_CODE table via Phase 10 catalog import
    # on LAUMC, see ris_sps_code_key; a flat 15-min default elsewhere per the
    # load_mins comment above). Always computed from load_mins, unlike util_pct
    # which prefers real std_pps actuals when available -- this is the
    # "100% schedule adherence" baseline to compare actual performance against.
    # On AE/days with no PPS coverage, effective_mins == load_mins, so util_pct
    # and theoretical_pct are identical there (honestly: no measured gap exists
    # without real PPS data, not a fabricated "on schedule" result).
    df["theoretical_pct"] = np.where(
        df["day_cap"] > 0,
        (df["load_mins"] / df["day_cap"] * 100).round(1),
        0.0
    )
    df = df.sort_values(["ae", "study_date"])

    ae_results   = []
    all_anomalies = 0
    high_stress  = []
    low_util     = []

    for ae, ae_df in df.groupby('ae', sort=False):
        daily_utils = ae_df['util_pct'].tolist()
        daily_theoretical = ae_df['theoretical_pct'].tolist()
        daily_dates = ae_df['date_str'].tolist()

        avg_util        = round(np.mean(daily_utils), 1) if daily_utils else 0
        avg_theoretical = round(np.mean(daily_theoretical), 1) if daily_theoretical else 0
        # Positive gap = actual running hotter than the SPS schedule assumes
        # (exams overrunning their scheduled duration -- real demand on this
        # device exceeds what the schedule accounts for). Negative gap = exams
        # finishing faster than scheduled (either genuine slack capacity, or
        # the SPS duration for this device's procedure mix is padded).
        gap_pct    = round(avg_util - avg_theoretical, 1)
        anomalies  = _detect_anomalies(daily_utils, dates=daily_dates)
        anom_count = sum(anomalies)
        all_anomalies += anom_count

        if avg_util > 85:
            high_stress.append(ae)
        elif 0 < avg_util < 30:
            low_util.append(ae)

        # Forecast
        f_dates, f_vals, r2, slope = _linear_forecast(daily_dates, daily_utils, 30) if len(daily_utils) >= 5 else ([], [], 0, 0)

        ae_results.append({
            "ae": ae,
            "avg_util": avg_util,
            "avg_theoretical_util": avg_theoretical,
            "gap_pct": gap_pct,
            "anomaly_count": anom_count,
            "slope": slope,
            "modality_mix": ae_modality_mix.get(ae, []),
            "chart": {
                "dates":          daily_dates,
                "theoretical_utils": daily_theoretical,
                "utils":          daily_utils,
                "anomaly_flags":  anomalies,
                "forecast_dates": f_dates,
                "forecast_vals":  f_vals
            }
        })

    ae_results.sort(key=lambda x: x['avg_util'], reverse=True)

    data = {
        "anomaly_count": all_anomalies,
        "high_stress":   high_stress,
        "low_util":      low_util,
        "ae_list":       ae_results
    }
    data["explanation"] = _generate_explanation("utilization", data)["utilization"]
    _ai_cache_put("utilization", start, end, data)
    return data


def _get_physician_intelligence(start, end):
    cached = _ai_cache_get("physician", start, end)
    if cached is not None:
        return cached

    rows = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(TRIM(CONCAT_WS(' ',
                referring_physician_first_name,
                referring_physician_last_name)), ''), 'Unknown') as physician,
            TO_CHAR(study_date, 'YYYY-MM') as month,
            COUNT(*) as cnt
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
          AND referring_physician_first_name IS NOT NULL
          AND COALESCE(study_modality, '') != 'SR'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {"s": start, "e": end}).fetchall()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['physician', 'month', 'cnt'])

    # Per-physician modality referral pattern
    phys_mod_rows = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(TRIM(CONCAT_WS(' ',
                referring_physician_first_name,
                referring_physician_last_name)), ''), 'Unknown') as physician,
            UPPER(TRIM(COALESCE(pm.modality, am.modality, s.study_modality))) AS modality,
            COUNT(*) as cnt
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN aetitle_modality_map am ON UPPER(TRIM(am.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :s AND :e
          AND s.referring_physician_first_name IS NOT NULL
          AND COALESCE(am.modality, s.study_modality, '') != 'SR'
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """), {"s": start, "e": end}).fetchall()
    phys_modality_map = {}
    for r in phys_mod_rows:
        phys_modality_map.setdefault(r[0], []).append({"modality": r[1] or "UNMAPPED", "count": int(r[2])})

    physician_data = []
    churning = []
    growing  = []

    for physician, p_df in df.groupby('physician'):
        if physician == 'Unknown':
            continue
        months = p_df['month'].tolist()
        counts = p_df['cnt'].tolist()
        total  = sum(counts)

        # Forecast
        if len(counts) >= 3:
            f_dates, f_vals, r2, slope = _linear_forecast(months, counts, 3)
        else:
            f_dates, f_vals, r2, slope = [], [], 0, 0

        if slope < -0.5 and len(counts) >= 2:
            churning.append(physician)
        elif slope > 0.5 and len(counts) >= 2:
            growing.append(physician)

        physician_data.append({
            "name":         physician,
            "total":        total,
            "slope":        slope,
            "modality_mix": phys_modality_map.get(physician, []),
            "chart": {
                "months":         months,
                "counts":         counts,
                "forecast_months": f_dates,
                "forecast_vals":   f_vals
            }
        })

    # Sort by total descending, take top 20
    physician_data.sort(key=lambda x: x['total'], reverse=True)
    physician_data = physician_data[:20]

    data = {
        "physicians": physician_data,
        "churning":   churning[:5],
        "growing":    growing[:5]
    }
    data["explanation"] = _generate_explanation("physician", data)["physician"]
    _ai_cache_put("physician", start, end, data)
    return data


# ─────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────

# Maps the tab name used in the URL/DOM ('util', not 'utilization' -- that
# mismatch is pre-existing, the data dict has always used 'utilization') to
# (data dict key, fetch function, partial template). Single source of truth
# for both the main route's eager tab and the on-demand panel endpoint below,
# so the two can never drift out of sync on which function/template a tab maps to.
_SECTION_CONFIG = {
    'storage':   ('storage',     _get_storage_intelligence,     '_ai_panel_storage.html'),
    'volume':    ('volume',      _get_volume_intelligence,      '_ai_panel_volume.html'),
    'util':      ('utilization', _get_utilization_intelligence, '_ai_panel_util.html'),
    'physician': ('physician',   _get_physician_intelligence,   '_ai_panel_physician.html'),
}


def _fetch_section(tab_name, start, end):
    """Compute one section's data, tolerating failure the same way the old
    all-sections _safe() wrapper did (log, roll back, return None -> renders
    the partial's "no data" branch instead of a 500)."""
    _, fn, _ = _SECTION_CONFIG[tab_name]
    try:
        return fn(start, end)
    except Exception as exc:
        logger.error(f"[report_ai] {fn.__name__} failed: {exc}", exc_info=True)
        try:
            db.session.rollback()
        except Exception:
            pass
        return None


def _resolve_date_range(values):
    go_live = get_go_live_date() or date(2025, 1, 1)
    today   = date.today()
    start = values.get("start_date", go_live.strftime('%Y-%m-%d'))
    end   = values.get("end_date",   today.strftime('%Y-%m-%d'))
    return start, end


@report_ai_bp.route("/report/ai", methods=["GET", "POST"])
@login_required
def report_ai():
    if current_user.role not in ('admin', 'viewer') and not user_has_page(current_user, 'report_ai'):
        abort(403)
    start, end = _resolve_date_range(request.values)
    active_tab = request.values.get("tab", "storage")
    if active_tab not in _SECTION_CONFIG:
        active_tab = "storage"
    active_data_key = _SECTION_CONFIG[active_tab][0]

    # Only the tab actually being shown is computed on page load -- the other
    # three are fetched on demand by report_ai_panel() below, only if/when the
    # user clicks over to them. All 4 used to be computed unconditionally on
    # every load (each with its own DB round-trips + numpy/pandas regression)
    # even though a given visit typically only looks at one or two tabs.
    active_data = _fetch_section(active_tab, start, end)
    data = {active_data_key: active_data}

    # Load current storage capacity setting for the form
    cap_row = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'storage_capacity_gb'"
    )).fetchone()
    storage_capacity_gb = float(cap_row[0]) if cap_row else 10240.0

    ui_theme = getattr(current_user, "ui_theme", None) or "dark"

    return render_template(
        "report_ai.html",
        data=data,
        run_report=True,
        display_start=start,
        display_end=end,
        active_tab=active_tab,
        active_data_key=active_data_key,
        storage_capacity_gb=storage_capacity_gb,
        ui_theme=ui_theme,
    )


@report_ai_bp.route("/report/ai/panel/<section>", methods=["GET"])
@login_required
def report_ai_panel(section):
    """On-demand fetch for one AI-intelligence tab, triggered by switchTab()
    in report_ai.html the first time a user opens that tab. Returns the same
    partial markup the main route would have inlined had that tab been the
    active one, plus the raw section data (for chart init on the client)."""
    if current_user.role not in ('admin', 'viewer') and not user_has_page(current_user, 'report_ai'):
        abort(403)
    if section not in _SECTION_CONFIG:
        abort(404)
    data_key, _, template = _SECTION_CONFIG[section]

    start, end = _resolve_date_range(request.args)
    section_data = _fetch_section(section, start, end)
    html = render_template(template, data={data_key: section_data})
    return jsonify({"html": html, "data": section_data})


@report_ai_bp.route("/report/ai/storage-capacity", methods=["POST"])
@login_required
def save_storage_capacity():
    if current_user.role != 'admin':
        return jsonify({"status": "error", "message": "Admin only"}), 403
    try:
        val = float(request.get_json(force=True).get("capacity_gb", 0))
        if val <= 0:
            return jsonify({"status": "error", "message": "Must be > 0"}), 400
        exists = db.session.execute(text(
            "SELECT 1 FROM settings WHERE key = 'storage_capacity_gb'"
        )).fetchone()
        if exists:
            db.session.execute(text(
                "UPDATE settings SET value = :v WHERE key = 'storage_capacity_gb'"
            ), {"v": str(val)})
        else:
            db.session.execute(text(
                "INSERT INTO settings (key, value) VALUES ('storage_capacity_gb', :v)"
            ), {"v": str(val)})
        db.session.commit()
        return jsonify({"status": "success", "capacity_gb": val})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
