"""
routes/report_ai.py — Report AI (/report/ai): storage, volume, device utilization, referrers.

Since 2026-11-18 (operator: "the linear regression is not AI to be relied on") the forecasts come
from utils/forecasting.py: Prophet with the holiday calendar (ai_calendar, migration 0169) plus a
sequential correction, trained every night and stored in ai_forecast. This page shows the history
of the dates picked and, when they reach the latest data, the stored forecast with its 80 % band.
Referrers get a test instead of a forecast (forecasting.referrer_changes). The calendar is edited
here (settings capability) and the Retrain button reruns the forecasts of the site picked.
"""
import logging
import math
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from flask import Blueprint, render_template, request, abort, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, get_report_start_date, user_has_page
from routes.report_cache import cache_get, cache_put, cache_invalidate
from utils import forecasting
from utils.roles import can

logger = logging.getLogger("REPORT_AI")
report_ai_bp = Blueprint("report_ai", __name__)

# Report ID used as the cache namespace for all AI-intelligence sections.
# Must not collide with numeric report IDs used by other routes.
_AI_CACHE_REPORT_ID = 9900

# Trend and forecast analysis, not a live view: the forecasts change once a night (or on
# Retrain, which clears this cache), so an hour is plenty.
_AI_CACHE_TTL = 3600


def _ai_cache_key(section, start, end):
    """The cache keys on the site too (report_cache), and on the forecasting runs done in this
    process: a finished run (nightly or Retrain) makes the next page fresh by itself."""
    return {"section": section, "start": start, "end": end, "runs": forecasting.generation()}


def _ai_cache_get(section: str, start: str, end: str):
    """Look up a cached AI-intelligence section result."""
    return cache_get(_AI_CACHE_REPORT_ID, _ai_cache_key(section, start, end), ttl=_AI_CACHE_TTL)


def _ai_cache_put(section: str, start: str, end: str, data) -> None:
    """Store an AI-intelligence section result in the shared cache."""
    cache_put(_AI_CACHE_REPORT_ID, _ai_cache_key(section, start, end), data, ttl=_AI_CACHE_TTL)


def _clear_ai_cache():
    cache_invalidate(_AI_CACHE_REPORT_ID)


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
#
# Every study query here joins the device map LATERAL ... LIMIT 1 (one row per study): the map's
# UNIQUE is on the raw aetitle, so 'ct99' and 'CT99' can both be rows, and the plain case-blind
# join counted a study once per row (found 2026-11-20). The training's queries match
# (utils/forecasting.VOLUME_SQL ...).

def _calendar_names(start, end):
    """{'YYYY-MM-DD': (name, kind)} for the calendar days between start and end (events too)."""
    rows = db.session.execute(text("""
        SELECT start_date, end_date, name, kind FROM public.ai_calendar
        WHERE end_date >= :s AND start_date <= :e ORDER BY start_date, id
    """), {"s": start, "e": end}).fetchall()
    out = {}
    for r in rows:
        d = max(r[0], pd.Timestamp(start).date())
        while d <= min(r[1], pd.Timestamp(end).date()):
            out.setdefault(d.strftime('%Y-%m-%d'), (r[2], r[3]))
            d += timedelta(days=1)
    return out


def _detect_anomalies(values, threshold=2.0, dates=None, skip=None):
    """Returns list of booleans — True = anomaly.
    If dates are provided, uses weekday-aware detection (compares each day
    against its own weekday's mean/std) to avoid flagging normal Mon-vs-Sun
    differences as anomalies. Days in skip (holidays, closures: the calendar
    explains them) are never anomalies and stay out of the weekday statistics.

    Each day is measured against the OTHER days (leave-one-out, review of 2026-11-20): with five
    to seven same-weekday samples, counting the day in its own mean and spread pulled the mean
    towards it and shrank the spread, and real anomalies went unflagged."""
    if len(values) < 4:
        return [False] * len(values)
    arr = np.array(values, dtype=float)
    idx = np.arange(len(arr))
    skip = skip or set()

    def flag(v, others):
        if others.sum() < 2:
            return False
        mean, std = arr[others].mean(), arr[others].std()
        if std == 0:                 # the other days all alike: any difference stands out
            return bool(v != mean)
        return bool(abs(v - mean) > threshold * std)

    if dates is not None and len(dates) == len(values):
        dows = np.array([pd.to_datetime(d).weekday() for d in dates])
        keep = np.array([str(d) not in skip for d in dates])
        flags = []
        for i, v in enumerate(arr):
            if not keep[i]:
                flags.append(False)
                continue
            same_dow = (dows == dows[i]) & keep & (idx != i)
            # fewer than two other same-weekday days (three with this one, the old rule): every
            # other kept day instead
            flags.append(flag(v, same_dow if same_dow.sum() >= 2 else keep & (idx != i)))
        return flags

    return [flag(v, idx != i) for i, v in enumerate(arr)]


def _forecast_run(key):
    row = db.session.execute(text("""
        SELECT model, trained_from, trained_to, run_at, info FROM public.ai_forecast_run
        WHERE series_key = :k
    """), {"k": key}).mappings().first()
    return dict(row) if row else None


def _forecast_rows(key):
    return db.session.execute(text("""
        SELECT ds, yhat, yhat_lower, yhat_upper FROM public.ai_forecast
        WHERE series_key = :k ORDER BY ds
    """), {"k": key}).fetchall()


def _accuracy(key, days=30):
    """The next-day error of the last 30 checked days: what the model said the night before
    against what came (ai_forecast_check). Days with nothing (closures at 0) are left out."""
    rows = db.session.execute(text("""
        SELECT forecast, actual FROM public.ai_forecast_check
        WHERE series_key = :k ORDER BY ds DESC LIMIT :n
    """), {"k": key, "n": days}).fetchall()
    pairs = [(float(f), float(a)) for f, a in rows if a and a > 0]
    if not pairs:
        return None
    mape = sum(abs(f - a) / a for f, a in pairs) / len(pairs) * 100
    return {"mape": round(mape, 1), "days": len(pairs)}


def _model_meta(key, run):
    if not run:
        return None
    return {"model": run["model"], "trained_to": str(run["trained_to"]),
            "run_at": run["run_at"].strftime('%Y-%m-%d %H:%M') if run["run_at"] else None,
            "accuracy": _accuracy(key), "fallback_reason": (run["info"] or {}).get("fallback_reason")}


def _forecast_shown(run, end):
    """A forecast continues the history only when the dates picked reach the latest data."""
    return bool(run) and str(end) >= str(run["trained_to"])


def _pct(x):
    return f"{x * 100:+.0f}%"


DAY_NAMES = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')


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
    values = [round(float(r[1]), 3) for r in rows]

    # total_gb is GB of images ADDED on that study_date (a daily delta, not a running total):
    # how full the archive is now is the sum of every row ever recorded, whatever dates are picked.
    # Site: summary_storage_daily has no site column, and needs none. Each site has its own
    # database (CLAUDE.md rule 10) and the rollup writes only the devices of the site its database
    # serves (ETL_JOBS/etl_analytics_refresh._served_site, rayd_served_site_id()), so the sum here
    # is this site's archive. The Super Report reads the same table the same way.
    current_gb = float(db.session.execute(text(
        "SELECT COALESCE(SUM(total_gb), 0) FROM summary_storage_daily"
    )).scalar() or 0.0)

    # User-configurable storage capacity (from settings table, default 10 TB)
    cap_row = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'storage_capacity_gb'"
    )).fetchone()
    capacity_gb = float(cap_row[0]) if cap_row else 10240.0

    run = _forecast_run("storage")
    fc = _forecast_rows("storage") if run else []
    f_dates, f_vals, f_lo, f_hi = [], [], [], []
    daily_gb = days_to_full = soonest = latest = None
    if fc:
        yhat = np.array([float(r[1]) for r in fc])
        lo = np.array([float(r[2] if r[2] is not None else r[1]) for r in fc])
        hi = np.array([float(r[3] if r[3] is not None else r[1]) for r in fc])
        daily_gb = round(float(yhat[:90].mean()), 2)
        remaining = capacity_gb - current_gb

        def days_until(daily):
            if remaining <= 0:
                return 0
            cum = np.cumsum(daily)
            hit = np.argmax(cum >= remaining) if (cum >= remaining).any() else None
            if hit is not None:
                return int(hit) + 1
            rate = float(daily[-90:].mean())
            return int(len(daily) + (remaining - cum[-1]) / rate) if rate > 0 else None
        days_to_full, soonest, latest = days_until(yhat), days_until(hi), days_until(lo)
        if _forecast_shown(run, end):
            f_dates = [str(r[0]) for r in fc[:90]]
            f_vals = [round(float(v), 2) for v in yhat[:90]]
            f_lo = [round(float(v), 2) for v in lo[:90]]
            f_hi = [round(float(v), 2) for v in hi[:90]]

    cal = _calendar_names(start, f_dates[-1] if f_dates else end)
    info = (run or {}).get("info") or {}
    data = {
        "current_gb": current_gb,
        "daily_growth_gb": daily_gb,
        "days_to_full": days_to_full,
        "days_to_full_soonest": soonest,
        "days_to_full_latest": latest,
        "full_on": (forecasting.beirut_today() + timedelta(days=days_to_full)).strftime('%d %b %Y')
                   if days_to_full else None,
        "capacity_gb": capacity_gb,
        "growth_per_year": info.get("growth_per_year"),
        "spike_allowance": info.get("spike_allowance"),
        "meta": _model_meta("storage", run),
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    f_vals,
            "forecast_lower":   f_lo,
            "forecast_upper":   f_hi,
            "holidays": {d: n for d, (n, k) in cal.items() if k in ('closure', 'half_day')},
        }
    }
    data["explanation"] = _explain_storage(data)
    _ai_cache_put("storage", start, end, data)
    return data


def _explain_storage(d):
    gb, cap = d["current_gb"], d["capacity_gb"]
    exp = f"The archive holds {gb:,.0f} GB of {cap:,.0f} GB ({gb / cap * 100:.0f}% used). "
    if d["daily_growth_gb"] is None:
        return exp + "No forecast yet: it is trained every night after the ETL, or with Retrain now."
    exp += f"The model expects about {d['daily_growth_gb']:.1f} GB a day over the next 90 days, less on weekends and holidays. "
    if d["days_to_full"] is not None:
        rng = ""
        if d["days_to_full_soonest"] and d["days_to_full_latest"] and d["days_to_full_soonest"] != d["days_to_full_latest"]:
            rng = f" (between {d['days_to_full_soonest']:,} and {d['days_to_full_latest']:,} days)"
        exp += f"At that pace the archive is full in about {d['days_to_full']:,} days{rng}, around {d['full_on']}. "
        if d["days_to_full"] < 180:
            exp += "Plan more storage now. "
    if d.get("spike_allowance"):
        exp += (f"Large one-off loads (a few studies of hundreds of GB) are kept out of the daily pattern "
                f"and added back as an average of {d['spike_allowance']:.1f} GB a day.")
    return exp


def _get_volume_intelligence(start, end):
    cached = _ai_cache_get("volume", start, end)
    if cached is not None:
        return cached

    # modality from the device first (CLAUDE.md rule 3), as the forecast counts it.
    # Site: etl_didb_studies here is the request's rh view (utils/rh_scope.py, migration 0165),
    # which keeps the site its database serves; the nightly training reads the same views
    # (forecasting.run_for_site), so history and forecast count the same studies.
    rows = db.session.execute(text("""
        SELECT s.study_date, COUNT(*) AS cnt
        FROM etl_didb_studies s
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) m ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
        GROUP BY s.study_date
        ORDER BY s.study_date
    """), {"s": start, "e": end}).fetchall()

    if not rows or len(rows) < 5:
        return None

    dates  = [str(r[0]) for r in rows]
    values = [int(r[1]) for r in rows]

    run = _forecast_run("volume")
    fc = _forecast_rows("volume") if _forecast_shown(run, end) else []
    f_dates = [str(r[0]) for r in fc]
    cal = _calendar_names(start, f_dates[-1] if f_dates else end)
    hol = {d for d, (n, k) in cal.items() if k in ('closure', 'half_day')}
    anomalies = _detect_anomalies(values, dates=dates, skip=hol)

    avg_daily = round(np.mean(values), 1)
    cur_total = sum(values)
    normal = [(pd.Timestamp(d).dayofweek, v) for d, v in zip(dates, values) if d not in hol]
    by_dow = {k: [v for dw, v in normal if dw == k] for k in range(7)}
    weekday_avg = round(float(np.mean([v for dw, v in normal if dw <= 4])), 1) if any(dw <= 4 for dw, _ in normal) else None
    sat_avg = round(float(np.mean(by_dow[5])), 1) if by_dow[5] else None
    sun_avg = round(float(np.mean(by_dow[6])), 1) if by_dow[6] else None

    # Modality breakdown — prefer procedure's true modality, fall back to AE map
    mod_rows = db.session.execute(text("""
        SELECT
            UPPER(TRIM(COALESCE(pm.modality, m.modality, s.study_modality, 'UNMAPPED'))) AS modality,
            COUNT(*) as cnt
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) m ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """), {"s": start, "e": end}).fetchall()

    info = (run or {}).get("info") or {}
    data = {
        "avg_daily": avg_daily,
        "total": cur_total,
        "weekday_avg": weekday_avg,
        "saturday_avg": sat_avg,
        "sunday_avg": sun_avg,
        "totals": info.get("totals") if fc else None,
        "growth_per_year": info.get("growth_per_year"),
        "weekday_ratio": info.get("weekday"),
        "holiday_effect": info.get("holiday_effect") or {},
        "correction_now": info.get("correction_now"),
        "upcoming": info.get("upcoming") or [],
        "meta": _model_meta("volume", run),
        "modality_split": [{"name": r.modality or "UNMAPPED", "value": int(r.cnt)} for r in mod_rows],
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    [round(float(r[1]), 1) for r in fc],
            "forecast_lower":   [round(float(r[2]), 1) for r in fc],
            "forecast_upper":   [round(float(r[3]), 1) for r in fc],
            "anomaly_flags":    anomalies,
            "holidays":         {d: n for d, (n, k) in cal.items() if k in ('closure', 'half_day')},
        }
    }
    data["explanation"] = _explain_volume(data)
    _ai_cache_put("volume", start, end, data)
    return data


def _explain_volume(d):
    exp = f"In the dates picked: {d['avg_daily']:.0f} studies a day on average"
    if d["weekday_avg"]:
        exp += f" ({d['weekday_avg']:.0f} Monday to Friday"
        if d["saturday_avg"]:
            exp += f", {d['saturday_avg']:.0f} on Saturdays"
        if d["sunday_avg"]:
            exp += f", {d['sunday_avg']:.0f} on Sundays"
        exp += ", holidays left out)"
    exp += ". "
    if not d["meta"]:
        return exp + "No forecast yet: it is trained every night after the ETL, or with Retrain now."
    w = d.get("weekday_ratio")
    if w and len(w) == 7:
        exp += (f"The model has learnt that a Saturday runs at {w[5] * 100:.0f}% of a weekday and a Sunday at "
                f"{w[6] * 100:.0f}%. ")
    eff = d["holiday_effect"].get("closure_wd")
    if eff:
        exp += f"A weekday holiday or Eid runs at about {eff * 100:.0f}% of a normal day; Ramadan itself changes nothing in LAUMC's data. "
    if d["growth_per_year"] is not None:
        exp += f"Underlying growth: {_pct(d['growth_per_year'])} a year. "
    if d["correction_now"] is not None and abs(d["correction_now"]) >= 0.03:
        exp += (f"The last days ran {'above' if d['correction_now'] > 0 else 'below'} what the calendar explains, "
                f"so the next days are pulled {abs(d['correction_now']) * 100:.0f}% "
                f"{'up' if d['correction_now'] > 0 else 'down'} (the sequential correction). ")
    t = d.get("totals")
    if t:
        exp += f"Next {t['days']} days: about {t['expected']:,.0f} studies (likely between {t['low']:,.0f} and {t['high']:,.0f}). "
    else:
        exp += "The forecast shows when the end date reaches the latest data. "
    if d["upcoming"]:
        nxt = d["upcoming"][0]
        exp += (f"Next holiday: {pd.Timestamp(nxt['date']).strftime('%a %d %b')}, {nxt['name']}, "
                f"about {nxt['expected']:.0f} studies expected.")
    return exp


# A device's average utilization above the first is "stressed", below the second (and above 0)
# "low": settings keys, these values when absent or not a number (review of 2026-11-20; they were
# literals). Report AI's own keys: sr_* are the Super Report's.
_UTIL_DEFAULTS = {"ai_util_high_warn_pct": 85.0, "ai_util_low_warn_pct": 30.0}


def _util_thresholds():
    rows = db.session.execute(text("SELECT key, value FROM settings WHERE key = ANY(:k)"),
                              {"k": list(_UTIL_DEFAULTS)}).fetchall()
    out = dict(_UTIL_DEFAULTS)
    for key, value in rows:
        try:
            out[key] = float(value)
        except (TypeError, ValueError):
            logger.warning("settings %s = %r is not a number; using %s", key, value, out[key])
    return out["ai_util_high_warn_pct"], out["ai_util_low_warn_pct"]


def _get_utilization_intelligence(start, end):
    cached = _ai_cache_get("utilization", start, end)
    if cached is not None:
        return cached
    high_pct, low_pct = _util_thresholds()

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
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) m ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND s.storing_ae IS NOT NULL
          AND UPPER(TRIM(s.storing_ae)) != 'SJHCSAPWFMFIR'
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
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
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) am ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND s.storing_ae IS NOT NULL
          AND UPPER(TRIM(s.storing_ae)) != 'SJHCSAPWFMFIR'
          AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
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
    #
    # day_of_week in both tables is RAYD's convention, 0 = Monday .. 6 = Sunday (migration 0108;
    # the RIS's 0 = Sunday is converted at load, ETL_JOBS/etl_ris_modality_availability.py), the
    # same as pandas' dayofweek and Python's weekday() used below.
    #
    # One row per device and weekday, whatever the case of the AE titles (review of 2026-11-20):
    # aetitle_modality_map's UNIQUE is on the raw aetitle, so 'ct99' and 'CT99' can both be rows,
    # and the case-blind joins gave two rows for one device and day; the dict below kept the last,
    # in no particular order. DISTINCT ON keeps one, by a stated priority: a row with the RIS's
    # opening minutes first, then the lowest map id.
    sched = db.session.execute(text("""
        SELECT DISTINCT ON (UPPER(TRIM(m.aetitle)), d.day_of_week)
            UPPER(TRIM(m.aetitle)) AS ae,
            d.day_of_week,
            COALESCE(dwa.available_minutes, m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS std_opening_minutes
        FROM aetitle_modality_map m
        CROSS JOIN generate_series(0, 6) AS d(day_of_week)
        LEFT JOIN std_device_weekly_availability dwa
            ON UPPER(TRIM(dwa.aetitle)) = UPPER(TRIM(m.aetitle)) AND dwa.day_of_week = d.day_of_week
        LEFT JOIN device_weekly_schedule ws
            ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle)) AND ws.day_of_week = d.day_of_week
        ORDER BY UPPER(TRIM(m.aetitle)), d.day_of_week, (dwa.available_minutes IS NULL), m.id,
                 dwa.aetitle, ws.aetitle
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
            LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) am ON TRUE
            WHERE pps.start_datetime::date BETWEEN :s AND :e
              AND pps.end_datetime IS NOT NULL
              AND pps.end_datetime > pps.start_datetime
              AND pps.performing_ae_title IS NOT NULL
              AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
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
    # what the page calls each device: its RIS room name, else the AE (utils.ae_display)
    from utils.ae_display import ae_label_map, ae_label
    labels = ae_label_map(db.session)

    # the stored 30-day forecasts of each device's minutes (utils/forecasting.py), turned into
    # utilization with the device's capacity of that weekday; none where the RIS gives none
    # The training writes the AE upper case (utils/forecasting DEVICE_EST_SQL: UPPER(TRIM(...))),
    # and the lookups below use UPPER(TRIM(ae)); both sides are put upper case here too, so a
    # device's forecast is found whatever wrote the key (review of 2026-11-20).
    fc_by_ae = {}
    runs = {"device:" + r[0][len('device:'):].upper(): r for r in db.session.execute(text("""
        SELECT series_key, model, trained_to, run_at, info FROM public.ai_forecast_run
        WHERE series_key LIKE 'device:%'
    """)).fetchall()}
    for k, ds, yhat, lo, hi in db.session.execute(text("""
        SELECT series_key, ds, yhat, yhat_lower, yhat_upper FROM public.ai_forecast
        WHERE series_key LIKE 'device:%' ORDER BY series_key, ds
    """)).fetchall():
        fc_by_ae.setdefault(k[len('device:'):].upper(), []).append((ds, float(yhat), float(lo), float(hi)))
    cal = _calendar_names(start, end)
    hol = {d for d, (n, k) in cal.items() if k in ('closure', 'half_day')}

    for ae, ae_df in df.groupby('ae', sort=False):
        label = ae_label(labels, ae)
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
        anomalies  = _detect_anomalies(daily_utils, dates=daily_dates, skip=hol)
        anom_count = sum(anomalies)
        all_anomalies += anom_count

        if avg_util > high_pct:
            high_stress.append(label)
        elif 0 < avg_util < low_pct:
            low_util.append(label)

        ae_up = str(ae).upper().strip()
        caps = [float(schedule_lookup.get((ae_up, k)) or 0) for k in range(7)]
        no_capacity = not any(c > 0 for c in caps)
        run = runs.get(f"device:{ae_up}")
        f_dates, f_vals, f_lo, f_hi, f_mins = [], [], [], [], []
        if run and str(end) >= str(run[2]):
            for ds, yhat, lo, hi in fc_by_ae.get(ae_up, []):
                cap = caps[ds.weekday()]
                f_dates.append(str(ds))
                f_mins.append(round(yhat))
                f_vals.append(round(yhat / cap * 100, 1) if cap > 0 else None)
                f_lo.append(round(lo / cap * 100, 1) if cap > 0 else None)
                f_hi.append(round(hi / cap * 100, 1) if cap > 0 else None)
        shown = [v for v in f_vals if v is not None]
        meta = None
        if run:
            meta = {"model": run[1], "trained_to": str(run[2]),
                    "run_at": run[3].strftime('%Y-%m-%d %H:%M') if run[3] else None,
                    "accuracy": _accuracy(f"device:{ae_up}")}

        ae_results.append({
            "ae": ae,
            "label": label,
            "avg_util": avg_util,
            "avg_theoretical_util": avg_theoretical,
            "gap_pct": gap_pct,
            "anomaly_count": anom_count,
            "forecast_avg_util": round(float(np.mean(shown)), 1) if shown else None,
            "forecast_minutes_per_day": round(float(np.mean(f_mins))) if f_mins else None,
            "no_capacity": no_capacity,
            "meta": meta,
            "modality_mix": ae_modality_mix.get(ae, []),
            "chart": {
                "dates":          daily_dates,
                "theoretical_utils": daily_theoretical,
                "utils":          daily_utils,
                "anomaly_flags":  anomalies,
                "forecast_dates": f_dates,
                "forecast_vals":  f_vals,
                "forecast_lower": f_lo,
                "forecast_upper": f_hi,
                "holidays":       {d: n for d, (n, k) in cal.items() if k in ('closure', 'half_day')},
            }
        })

    ae_results.sort(key=lambda x: x['avg_util'], reverse=True)

    data = {
        "anomaly_count": all_anomalies,
        "high_pct":      high_pct,
        "low_pct":       low_pct,
        "high_stress":   high_stress,
        "low_util":      low_util,
        "no_capacity":   [a["label"] for a in ae_results if a["no_capacity"]],
        "ae_list":       ae_results
    }
    data["explanation"] = _explain_utilization(data)
    _ai_cache_put("utilization", start, end, data)
    return data


def _explain_utilization(d):
    exp = (f"{d['anomaly_count']} unusual days across the devices (a day far from that device's normal "
           f"for the same weekday; holidays are not counted). ")
    hi, lo = d.get("high_pct", 85.0), d.get("low_pct", 30.0)
    if d["high_stress"]:
        exp += f"Running above {hi:g}% on average: {', '.join(d['high_stress'][:3])}. "
    if d["low_util"]:
        exp += f"Below {lo:g}%: {', '.join(d['low_util'][:3])}. "
    if d["no_capacity"]:
        exp += (f"{len(d['no_capacity'])} device(s) have no opening hours on the RIS "
                f"({', '.join(d['no_capacity'][:4])}), so their utilization cannot be worked out; "
                f"their forecast is shown in minutes. ")
    if any(a["meta"] for a in d["ae_list"]):
        exp += ("Each device's next 30 days are forecast from its own weekly pattern and the holiday "
                "calendar, then divided by its opening minutes of that weekday.")
    else:
        exp += "No forecast yet: it is trained every night after the ETL, or with Retrain now."
    return exp


def _get_physician_intelligence(start, end):
    cached = _ai_cache_get("physician", start, end)
    if cached is not None:
        return cached

    rows = db.session.execute(text(f"""
        SELECT {_PHYSICIAN} AS physician, TO_CHAR(s.study_date, 'YYYY-MM') AS month, COUNT(*) AS cnt
        FROM etl_didb_studies s
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) am ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND s.referring_physician_first_name IS NOT NULL
          AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {"s": start, "e": end}).fetchall()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['physician', 'month', 'cnt'])

    # Per-physician modality referral pattern
    phys_mod_rows = db.session.execute(text(f"""
        SELECT
            {_PHYSICIAN} AS physician,
            UPPER(TRIM(COALESCE(pm.modality, am.modality, s.study_modality))) AS modality,
            COUNT(*) as cnt
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) am ON TRUE
        WHERE s.study_date BETWEEN :s AND :e
          AND s.referring_physician_first_name IS NOT NULL
          AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """), {"s": start, "e": end}).fetchall()
    phys_modality_map = {}
    for r in phys_mod_rows:
        phys_modality_map.setdefault(r[0], []).append({"modality": r[1] or "UNMAPPED", "count": int(r[2])})

    # Each referrer's real numbers: the 13 weeks to the end date against the same 13 weeks a year
    # earlier (utils/forecasting.referrer_trends). Not a straight line through monthly counts,
    # which read the current, unfinished month as a drop for everyone.
    r0, r1, l0, l1 = forecasting.referrer_windows(end)
    counts = db.session.execute(text(f"""
        SELECT {_PHYSICIAN} AS physician,
               COUNT(*) FILTER (WHERE s.study_date BETWEEN :r0 AND :r1) AS recent,
               COUNT(*) FILTER (WHERE s.study_date BETWEEN :l0 AND :l1) AS last_year
        FROM etl_didb_studies s
        LEFT JOIN LATERAL (SELECT m0.modality FROM aetitle_modality_map m0 WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) ORDER BY m0.id LIMIT 1) am ON TRUE
        WHERE (s.study_date BETWEEN :r0 AND :r1 OR s.study_date BETWEEN :l0 AND :l1)
          AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
        GROUP BY 1
    """), {"r0": r0, "r1": r1, "l0": l0, "l1": l1}).fetchall()
    # The hospital's figures count EVERY study, a named referrer or not ('Unknown' included): it is
    # the hospital as a whole. The referrers' trends leave 'Unknown' out (it is nobody). Said in
    # _explain_physician (review of 2026-11-20).
    hosp_recent, hosp_last = sum(r[1] for r in counts), sum(r[2] for r in counts)
    trend = forecasting.referrer_trends({r[0]: r[1] for r in counts if r[0] != 'Unknown'},
                                        {r[0]: r[2] for r in counts if r[0] != 'Unknown'})
    keys = ("status", "grade", "recent", "last_year", "change_pct", "diff", "certain")
    churning = sorted((n for n, t in trend.items() if t["listed"] and t["status"] in ("stopped", "declining")),
                      key=lambda n: trend[n]["diff"])
    growing = sorted((n for n, t in trend.items() if t["listed"] and t["status"] in ("new", "growing")),
                     key=lambda n: -trend[n]["diff"])

    physician_data = []
    for physician, p_df in df.groupby('physician'):
        if physician == 'Unknown':
            continue
        t = trend.get(physician)
        by_month = dict(zip(p_df['month'], p_df['cnt']))
        months = p_df['month'].tolist()
        prev = [by_month.get(f"{int(m[:4]) - 1}{m[4:]}") for m in months]
        physician_data.append({
            "name":         physician,
            "total":        int(p_df['cnt'].sum()),
            "trend":        {k: t[k] for k in keys} if t else None,
            "modality_mix": phys_modality_map.get(physician, []),
            "chart": {
                "months":    months,
                "counts":    [int(c) for c in p_df['cnt'].tolist()],
                "last_year": [int(v) if v is not None else None for v in prev],
            }
        })

    # Sort by total descending, take top 20
    physician_data.sort(key=lambda x: x['total'], reverse=True)
    physician_data = physician_data[:20]

    data = {
        "physicians": physician_data,
        "churning":   [{"name": n, **{k: trend[n][k] for k in keys}} for n in churning[:10]],
        "growing":    [{"name": n, **{k: trend[n][k] for k in keys}} for n in growing[:10]],
        "counts":     {s: sum(1 for t in trend.values() if t["status"] == s)
                       for s in ("stopped", "declining", "steady", "growing", "new")},
        "hospital":   {"recent": hosp_recent, "last_year": hosp_last,
                       "change_pct": round((hosp_recent / hosp_last - 1) * 100) if hosp_last else None},
        "window":     {"recent_from": str(r0), "to": str(r1), "last_from": str(l0), "last_to": str(l1)},
    }
    data["explanation"] = _explain_physician(data)
    _ai_cache_put("physician", start, end, data)
    return data


_PHYSICIAN = """COALESCE(NULLIF(TRIM(CONCAT_WS(' ', s.referring_physician_first_name,
                s.referring_physician_last_name)), ''), 'Unknown')"""


def _explain_physician(d):
    w, h, c = d["window"], d["hospital"], d["counts"]
    fmt = lambda x: pd.Timestamp(x).strftime('%d %b %Y')
    exp = (f"Each referrer's studies in the last 13 weeks ({fmt(w['recent_from'])} to {fmt(w['to'])}) against "
           f"the same 13 weeks a year earlier, so the season and holidays line up. ")
    if h["change_pct"] is not None:
        exp += (f"The hospital as a whole (every study, with or without a named referrer): {h['recent']:,} studies "
                f"against {h['last_year']:,} ({h['change_pct']:+d}%), so a referrer near {h['change_pct']:+d}% is "
                f"moving with the hospital. ")
    exp += (f"{c['declining']} referrers are down 10% or more, {c['stopped']} stopped, {c['growing']} are up 10% "
            f"or more, {c['new']} are new, {c['steady']} are within 10%. ")
    if d["churning"]:
        exp += "Largest losses: " + ", ".join(
            f"{x['name']} ({x['diff']:+d} studies)" for x in d["churning"][:3]) + ". "
    if d["growing"]:
        exp += "Largest gains: " + ", ".join(
            f"{x['name']} ({x['diff']:+d} studies)" for x in d["growing"][:3]) + "."
    return exp


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
    """(data, failed) for one section. A failure is logged and rolled back, and said as such on
    the page ("could not compute", the partials' section_error branch, an ERROR badge): it used to
    come back as None, which read exactly like "no data" (review of 2026-11-20)."""
    _, fn, _ = _SECTION_CONFIG[tab_name]
    try:
        return fn(start, end), False
    except Exception as exc:
        logger.error(f"[report_ai] {fn.__name__} failed: {exc}", exc_info=True)
        try:
            db.session.rollback()
        except Exception:
            pass
        return None, True


def _resolve_date_range(values):
    """start, end as 'YYYY-MM-DD'. Empty or unreadable dates take the defaults (the report start
    date, today) instead of reaching a query: ?start_date=&end_date= used to give '' (review of
    2026-11-20). Dates the wrong way round are swapped."""
    go_live = get_report_start_date() or date(2025, 1, 1)

    def day(raw, default):
        try:
            return datetime.strptime(str(raw or '').strip(), '%Y-%m-%d').date()
        except ValueError:
            return default
    start = day(values.get("start_date"), go_live)
    end = day(values.get("end_date"), date.today())
    if end < start:
        start, end = end, start
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


@report_ai_bp.route("/report/ai", methods=["GET", "POST"])
@login_required
def report_ai():
    if not user_has_page(current_user, 'report_ai'):
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
    active_data, section_error = _fetch_section(active_tab, start, end)
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
        section_error=section_error,
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
    if not user_has_page(current_user, 'report_ai'):
        abort(403)
    if section not in _SECTION_CONFIG:
        abort(404)
    data_key, _, template = _SECTION_CONFIG[section]

    start, end = _resolve_date_range(request.args)
    section_data, failed = _fetch_section(section, start, end)
    # the partials use only data.<key> (and their own macros): checked 2026-11-20
    html = render_template(template, data={data_key: section_data}, section_error=failed)
    return jsonify({"html": html, "data": section_data, "error": failed})


@report_ai_bp.route("/report/ai/storage-capacity", methods=["POST"])
@login_required
def save_storage_capacity():
    if not can(current_user, 'settings'):
        return jsonify({"status": "error", "message": "Admin only"}), 403
    # A body that is not JSON, or a value that is not a number, is the sender's mistake: 400, not
    # 500 (review of 2026-11-20). "1,024" reads as 1024.
    body = request.get_json(silent=True) or {}
    try:
        val = float(str(body.get("capacity_gb", "")).replace(",", "").strip())
    except ValueError:
        return jsonify({"status": "error", "message": "The capacity must be a number of GB"}), 400
    if not math.isfinite(val) or val <= 0:
        return jsonify({"status": "error", "message": "Must be > 0"}), 400
    try:
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
        _clear_ai_cache()
        return jsonify({"status": "success", "capacity_gb": val})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


# ─────────────────────────────────────────────
#  CALENDAR AND RETRAIN (settings capability)
# ─────────────────────────────────────────────

def _calendar_json(row):
    return {"id": row[0], "start_date": str(row[1]), "end_date": str(row[2]), "name": row[3],
            "kind": row[4], "kind_label": forecasting.KIND_LABELS.get(row[4], row[4]), "source": row[5]}


@report_ai_bp.route("/report/ai/calendar", methods=["GET"])
@login_required
def calendar_list():
    """The holiday calendar the forecasts use: one list for both hospitals (national holidays)."""
    if not user_has_page(current_user, 'report_ai'):
        abort(403)
    rows = db.session.execute(text("""
        SELECT id, start_date, end_date, name, kind, source FROM public.ai_calendar
        ORDER BY start_date, id
    """)).fetchall()
    return jsonify({"rows": [_calendar_json(r) for r in rows], "kinds": forecasting.KIND_LABELS,
                    "can_edit": can(current_user, 'settings')})


def _parse_day(raw):
    return datetime.strptime(str(raw or '').strip(), '%Y-%m-%d').date()


def _shared_write(view):
    from utils.site_db import shared_write
    return shared_write(view)


@report_ai_bp.route("/report/ai/calendar", methods=["POST"])
@login_required
def calendar_add():
    if not can(current_user, 'settings'):
        return jsonify({"status": "error", "message": "Only administrators can change the calendar"}), 403
    body = request.get_json(silent=True) or {}
    try:
        start = _parse_day(body.get("start_date"))
        end = _parse_day(body.get("end_date") or body.get("start_date"))
    except ValueError:
        return jsonify({"status": "error", "message": "Dates as YYYY-MM-DD"}), 400
    name = str(body.get("name") or "").strip()
    kind = str(body.get("kind") or "").strip()
    if end < start or (end - start).days > 366:
        return jsonify({"status": "error", "message": "The end date must be on or after the start, within a year"}), 400
    if not name or len(name) > 120:
        return jsonify({"status": "error", "message": "A name of 1 to 120 characters"}), 400
    if kind not in forecasting.KINDS:
        return jsonify({"status": "error", "message": "Unknown kind"}), 400
    return _save_calendar(start, end, name, kind)


@_shared_write
def _save_calendar(start, end, name, kind):
    row = db.session.execute(text("""
        INSERT INTO public.ai_calendar (start_date, end_date, name, kind, source, created_by)
        VALUES (:s, :e, :n, :k, 'user', :u)
        ON CONFLICT (start_date, name) DO UPDATE
            SET end_date = EXCLUDED.end_date, kind = EXCLUDED.kind, source = 'user',
                created_by = EXCLUDED.created_by, created_at = now()
        RETURNING id, start_date, end_date, name, kind, source
    """), {"s": start, "e": end, "n": name, "k": kind, "u": current_user.username}).fetchone()
    db.session.commit()
    _clear_ai_cache()
    return jsonify({"status": "success", "row": _calendar_json(row)})


@report_ai_bp.route("/report/ai/calendar/<int:cid>/delete", methods=["POST"])
@login_required
def calendar_delete(cid):
    if not can(current_user, 'settings'):
        return jsonify({"status": "error", "message": "Only administrators can change the calendar"}), 403
    return _delete_calendar(cid)


@_shared_write
def _delete_calendar(cid):
    gone = db.session.execute(text("DELETE FROM public.ai_calendar WHERE id = :i"), {"i": cid}).rowcount
    db.session.commit()
    _clear_ai_cache()
    if not gone:
        return jsonify({"status": "error", "message": "Not found"}), 404
    return jsonify({"status": "success"})


@report_ai_bp.route("/report/ai/retrain", methods=["POST"])
@login_required
def retrain():
    """Retrain the forecasts of the site picked, in the background (about a minute)."""
    if not can(current_user, 'settings'):
        return jsonify({"status": "error", "message": "Only administrators can retrain"}), 403
    from utils import site_db
    started = forecasting.start_background(current_app._get_current_object(), site_db.current_site_id())
    _clear_ai_cache()
    return jsonify({"status": "started" if started else "running"})


@report_ai_bp.route("/report/ai/status", methods=["GET"])
@login_required
def forecast_status():
    if not user_has_page(current_user, 'report_ai'):
        abort(403)
    rows = db.session.execute(text("""
        SELECT series_key, model, trained_to, run_at FROM public.ai_forecast_run ORDER BY series_key
    """)).fetchall()
    # No cache clearing here: a finished run changes forecasting.generation(), part of the cache
    # key. Clearing on every call made while idle emptied the cache for good as soon as anything
    # polled this endpoint (review of 2026-11-20).
    return jsonify({"running": forecasting.is_running(),
                    "series": [{"key": r[0], "model": r[1], "trained_to": str(r[2]),
                                "run_at": r[3].strftime('%Y-%m-%d %H:%M') if r[3] else None} for r in rows]})
