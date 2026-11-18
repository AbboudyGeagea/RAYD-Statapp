"""
utils/forecasting.py — Report AI's forecasts (operator, 2026-11-18; migration 0169).

The operator: "the linear regression is not AI to be relied on, specially on trends that change
weekly (weekdays vs weekends) or holiday periods (Christmas and Ramadan)". Measured on LAUMC's
production data (2019-2026, scripts/sql/ai_trend_export.sql):

    Saturdays run at 35 % of a weekday and Sundays at 24 %, the same every year since 2019;
    official holidays and both Eids close like a Sunday (about 0.3); Dec 24 and Dec 31 are half
    days; Ashura, the Prophet's Birthday and the Islamic New Year change nothing; Ramadan itself
    changes nothing (only its Eid); growth about +8 % a year; shocks (COVID, the 2021 crisis, the
    war of autumn 2024) must stay out of training.

The model, chosen on a backtest of 14 cut-offs x 90 days (straight line 67 % daily error, Prophet
15 %, neural networks and gradient boosting on the last 28 days worse at 90 days):

    1. Prophet, multiplicative: weekly pattern, yearly pattern, a bending trend, and the calendar
       of ai_calendar (closures split by weekday, half days). Event periods are left out.
    2. A sequential correction (the operator, 2026-11-18: "every period should be able to discover
       the period that comes right after"): an autoregression on Prophet's own errors of the last
       14 days. When volume has been running above or below what the calendar explains, the next
       days are pulled the same way; the pull fades over the following days.

run() trains every series of one database (volume, each active device's minutes, storage GB) and
stores the forecast in ai_forecast; before replacing it, every forecast day that has now happened
goes to ai_forecast_check with what came, which is the next-day error the page shows. It runs every
night after the ETL (app.py) and from the page's Retrain button (settings capability), in the
background. Without Prophet (not installed, too little history, a failed fit) a series falls back
to the average of its last four same weekdays times the holiday factors seen in its history.
"""
import logging
import math
import threading
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("FORECASTING")

MODEL_PROPHET = "Prophet + sequential correction"
MODEL_FALLBACK = "Weekday average + holidays"
TRAIN_FROM = date(2022, 1, 1)        # after COVID and the 2021 crisis; settings ai_train_from overrides
HORIZON = {"volume": 90, "device": 30, "storage": 730}
MIN_DAYS = 120                       # fewer days of history: the fallback
AR_LAGS = (1, 2, 3, 4, 5, 6, 7, 14)
HOLIDAY_TYPES = ("closure_wd", "closure_sat", "closure_sun", "half_day")
KINDS = ("closure", "half_day", "no_effect", "event")
KIND_LABELS = {"closure": "Closed", "half_day": "Half day", "no_effect": "No effect",
               "event": "Unusual period (left out of training)"}
# what the fallback assumes for a holiday type never seen in the history it has
DEFAULT_FACTORS = {"closure_wd": 0.3, "closure_sat": 0.8, "closure_sun": 0.8, "half_day": 0.65}
DEVICE_ACTIVE_DAYS = 30              # a device with no study in the last 30 days is not forecast
# Prophet's band, which the page calls the likely range (8 days in 10). Prophet's own 80 % band
# held only 70 % of the days in the backtest on production (2025-26), its 90 % band held 80 %.
INTERVAL_WIDTH = 0.9

_lock = threading.Lock()


def beirut_today():
    import pytz
    return datetime.now(pytz.timezone("Asia/Beirut")).date()


# ── the calendar ──────────────────────────────────────────────────────────────

def load_calendar(conn):
    rows = conn.execute(text("""
        SELECT start_date, end_date, name, kind FROM public.ai_calendar ORDER BY start_date, id
    """)).fetchall()
    return [dict(start_date=r[0], end_date=r[1], name=r[2], kind=r[3]) for r in rows]


class CalendarDays:
    """The calendar as days: holiday type per day (for Prophet), its name, and the event days."""

    def __init__(self, rows):
        self.kind, self.name, self.events = {}, {}, set()
        for r in rows:
            day, end = pd.Timestamp(r["start_date"]), pd.Timestamp(r["end_date"])
            while day <= end:
                if r["kind"] == "event":
                    self.events.add(day)
                elif r["kind"] in ("closure", "half_day"):
                    # one type per day: a closure wins over a half day
                    if r["kind"] == "half_day":
                        t = "half_day"
                    else:
                        t = ("closure_wd", "closure_wd", "closure_wd", "closure_wd", "closure_wd",
                             "closure_sat", "closure_sun")[day.dayofweek]
                    if self.kind.get(day) not in ("closure_wd", "closure_sat", "closure_sun"):
                        self.kind[day] = t
                    self.name.setdefault(day, r["name"])
                elif r["kind"] == "no_effect":
                    self.name.setdefault(day, r["name"])
                day += pd.Timedelta(days=1)

    def is_holiday(self, day):
        return pd.Timestamp(day) in self.kind

    def prophet_frame(self, start, end):
        days = sorted(d for d in self.kind if pd.Timestamp(start) <= d <= pd.Timestamp(end))
        if not days:
            return None
        return pd.DataFrame({"ds": days, "holiday": [self.kind[d] for d in days]})


# ── one series ────────────────────────────────────────────────────────────────

def _prophet_class():
    try:
        logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
        logging.getLogger("prophet").setLevel(logging.WARNING)
        from prophet import Prophet
        return Prophet
    except Exception:
        return None


def _training_copy(y, cal, robust):
    """y with event days (and, robust, one-off spikes) set to NaN; and the spikes' daily excess."""
    train = y.astype(float).copy()
    train[train.index.isin(list(cal.events))] = np.nan
    allowance = 0.0
    if robust:
        med = train.rolling(56, center=True, min_periods=14).median()
        cap = med * 4 + 1
        spikes = train > cap
        excess = (train - cap).where(spikes, 0.0).fillna(0.0)
        allowance = float(excess[-365:].sum() / 365.0)
        train[spikes] = np.nan
    return train, allowance


def _ar_correction(resid, horizon, lags=AR_LAGS, alpha=1.0):
    """Forecast of Prophet's log error for the next days from its last errors. The errors are
    centred first, so the pull fades to nothing instead of settling on a permanent offset (that
    offset is only noise: Prophet is already fit to the level). Returns (array, coefficients)."""
    resid = np.asarray(resid, dtype=float)
    mean = np.nanmean(resid) if np.isfinite(resid).any() else 0.0
    dev = resid - mean
    p = max(lags)
    X, T = [], []
    for j in range(p, len(dev)):
        row = [dev[j - k] for k in lags]
        if np.isnan(dev[j]) or np.isnan(row).any():
            continue
        X.append(row)
        T.append(dev[j])
    if len(T) < 60:
        return np.zeros(horizon), None
    X, T = np.array(X), np.array(T)
    w = np.linalg.solve(X.T @ X + alpha * np.eye(len(lags)), X.T @ T)
    hist = list(np.nan_to_num(dev[-p:], nan=0.0))
    out = []
    for _ in range(horizon):
        r = float(sum(w[i] * hist[-k] for i, k in enumerate(lags)))
        out.append(r)
        hist.append(r)
    return np.clip(np.array(out), -0.7, 0.7), [round(float(x), 4) for x in w]


def _upcoming(cal, fc, limit=8, days=90):
    out = []
    for _, r in fc.head(days).iterrows():
        d = pd.Timestamp(r.ds)
        if d in cal.kind:
            out.append({"date": d.strftime("%Y-%m-%d"), "name": cal.name.get(d, ""),
                        "kind": "half_day" if cal.kind[d] == "half_day" else "closure",
                        "expected": round(float(r.yhat), 1)})
    return out[:limit]


def _totals(fc, days=90):
    head = fc.head(days)
    return {"days": int(len(head)), "expected": round(float(head.yhat.sum()), 1),
            "low": round(float(head.yhat_lower.sum()), 1), "high": round(float(head.yhat_upper.sum()), 1)}


def _fallback(y, train, cal, horizon, allowance, reason):
    """Average of the last four same weekdays (holidays and events left out) times the holiday
    factor seen in the history; the band is the spread of the last eight weeks around it."""
    clean = train.copy()
    clean[clean.index.isin(list(cal.kind))] = np.nan
    recent = clean[-28:]
    prof = recent.groupby(recent.index.dayofweek).mean()
    overall = float(np.nanmean(recent)) if np.isfinite(recent).any() else 0.0
    factors = dict(DEFAULT_FACTORS)
    for t in HOLIDAY_TYPES:
        ratios = []
        for d in (d for d, k in cal.kind.items() if k == t and d in y.index and not np.isnan(train.get(d, np.nan))):
            near = [clean.get(d + pd.Timedelta(weeks=k)) for k in (-2, -1, 1, 2)]
            near = [v for v in near if v is not None and not np.isnan(v)]
            if near and np.median(near) > 0:
                ratios.append(train[d] / np.median(near))
        if len(ratios) >= 2:
            factors[t] = float(np.median(ratios))
    last8 = clean[-56:].dropna()
    spread = 0.2
    if len(last8) >= 14:
        base = last8.index.dayofweek.map(lambda k: prof.get(k, overall))
        dev = np.abs(last8.values / np.maximum(np.asarray(base, dtype=float), 1) - 1)
        spread = float(np.clip(np.quantile(dev, 0.8), 0.05, 0.6))
    ds = pd.date_range(y.index[-1] + pd.Timedelta(days=1), periods=horizon)
    yhat = np.array([(prof.get(d.dayofweek, overall) if not np.isnan(prof.get(d.dayofweek, np.nan)) else overall)
                     * factors.get(cal.kind.get(d), 1.0) for d in ds], dtype=float)
    yhat = np.nan_to_num(yhat) + allowance
    fc = pd.DataFrame({"ds": ds, "yhat": yhat, "yhat_lower": np.maximum(0, yhat * (1 - spread)),
                       "yhat_upper": yhat * (1 + spread)})
    wk = prof.reindex(range(7)).fillna(0).values
    wmean = wk[:5].mean() if wk[:5].mean() > 0 else 1
    info = {"model": MODEL_FALLBACK, "fallback_reason": reason,
            "weekday": [round(float(v / wmean), 3) for v in wk],
            "holiday_effect": {k: round(v, 3) for k, v in factors.items()},
            "growth_per_year": None, "correction_now": None}
    return fc, info


def fit_forecast(y, cal, horizon, yearly=True, robust=False, sequential=True):
    """Forecast a daily series. y: pd.Series on a complete daily DatetimeIndex (0 on days with
    nothing). Returns (DataFrame ds, yhat, yhat_lower, yhat_upper; info for the page)."""
    y = y.astype(float).sort_index()
    train, allowance = _training_copy(y, cal, robust)
    base_info = {"trained_from": y.index[0].strftime("%Y-%m-%d"), "trained_to": y.index[-1].strftime("%Y-%m-%d"),
                 "days": int(train.notna().sum()), "spike_allowance": round(allowance, 3) if robust else None}
    Prophet = _prophet_class()
    if Prophet is None:
        fc, info = _fallback(y, train, cal, horizon, allowance, "Prophet is not installed")
    elif train.notna().sum() < MIN_DAYS:
        fc, info = _fallback(y, train, cal, horizon, allowance, f"less than {MIN_DAYS} days of history")
    else:
        try:
            fc, info = _fit_prophet(Prophet, y, train, cal, horizon, yearly, allowance, sequential)
        except Exception as exc:
            logger.exception("Prophet failed; weekday average used")
            fc, info = _fallback(y, train, cal, horizon, allowance, f"Prophet failed: {exc}")
    for col in ("yhat", "yhat_lower", "yhat_upper"):
        fc[col] = np.maximum(0.0, fc[col].astype(float))
    info.update(base_info)
    info["totals"] = _totals(fc)
    info["upcoming"] = _upcoming(cal, fc)
    return fc.reset_index(drop=True), info


def _fit_prophet(Prophet, y, train, cal, horizon, yearly, allowance, sequential):
    span = (y.index[-1] - y.index[0]).days
    future_end = y.index[-1] + pd.Timedelta(days=horizon)
    m = Prophet(growth="linear", weekly_seasonality=True,
                yearly_seasonality=6 if (yearly and span >= 730) else False,
                daily_seasonality=False, seasonality_mode="multiplicative",
                holidays=cal.prophet_frame(y.index[0], future_end),
                changepoint_prior_scale=0.05, holidays_prior_scale=10.0,
                interval_width=INTERVAL_WIDTH, uncertainty_samples=300)
    m.fit(pd.DataFrame({"ds": train.index, "y": train.values}))
    all_ds = pd.DataFrame({"ds": pd.date_range(y.index[0], future_end)})
    p = m.predict(all_ds)
    n = len(y)
    hist, fut = p.iloc[:n], p.iloc[n:].copy()

    corr = np.zeros(horizon)
    coefs = None
    if sequential:
        resid = np.log((train.values + 1.0) / (np.maximum(hist.yhat.values, 0) + 1.0))
        corr, coefs = _ar_correction(resid, horizon)
    factor = np.exp(corr)
    fc = pd.DataFrame({"ds": fut.ds.values,
                       "yhat": fut.yhat.values * factor + allowance,
                       "yhat_lower": fut.yhat_lower.values * factor + allowance,
                       "yhat_upper": fut.yhat_upper.values * factor + allowance})

    # what the page explains: weekday shape, holiday effects, growth, the current pull
    w = 1 + p["weekly"]
    wk = w.groupby(p.ds.dt.dayofweek).mean().reindex(range(7)).values
    # a device idle that weekday can come out a hair below zero (the multiplicative term)
    weekday = [round(max(0.0, float(v / wk[:5].mean())), 3) for v in wk]
    effects = {}
    mt = p["multiplicative_terms"]
    for t in HOLIDAY_TYPES:
        if t in p.columns:
            on = p[t] != 0
            if on.any():
                f = (1 + mt[on]) / (1 + mt[on] - p.loc[on, t])
                effects[t] = round(float(np.median(f)), 3)
    trend = hist.trend.values
    growth = None
    if n > 365 and trend[-366] > 0:
        growth = round(float(trend[-1] / trend[-366] - 1), 4)
    info = {"model": MODEL_PROPHET if sequential else "Prophet", "weekday": weekday,
            "holiday_effect": effects, "growth_per_year": growth,
            "correction_now": round(float(math.exp(corr[0]) - 1), 4) if len(corr) else None,
            "ar_coefficients": coefs}
    return fc, info


# ── the series of one database ────────────────────────────────────────────────

def _daily(rows, start, end):
    s = pd.Series({pd.Timestamp(r[0]): float(r[1] or 0) for r in rows}, dtype=float)
    return s.reindex(pd.date_range(start, end), fill_value=0.0)


def train_from(conn):
    raw = conn.execute(text("SELECT value FROM settings WHERE key = 'ai_train_from'")).scalar()
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date() if raw else TRAIN_FROM
    except ValueError:
        return TRAIN_FROM


VOLUME_SQL = """
    SELECT s.study_date, COUNT(*)
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE s.study_date >= :f AND s.study_date < :today
      AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
    GROUP BY 1
"""

# the device minutes the Utilization tab shows: measured (RIS performed steps) where the RIS has
# them, else estimated from the procedure durations (15 minutes when a code has none)
DEVICE_EST_SQL = """
    SELECT UPPER(TRIM(s.storing_ae)) AS ae, s.study_date, SUM(COALESCE(pm.duration_minutes, 15))
    FROM etl_didb_studies s
    LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
    LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE s.study_date >= :f AND s.study_date < :today
      AND s.storing_ae IS NOT NULL
      AND UPPER(TRIM(s.storing_ae)) != 'SJHCSAPWFMFIR'
      AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
    GROUP BY 1, 2
"""
DEVICE_ACT_SQL = """
    SELECT UPPER(TRIM(pps.performing_ae_title)) AS ae, pps.start_datetime::date,
           SUM(EXTRACT(EPOCH FROM (pps.end_datetime - pps.start_datetime)) / 60)
    FROM std_pps pps
    JOIN etl_didb_studies s ON s.study_db_uid = pps.study_db_uid
    LEFT JOIN aetitle_modality_map am ON UPPER(TRIM(am.aetitle)) = UPPER(TRIM(s.storing_ae))
    WHERE pps.start_datetime >= :f AND pps.start_datetime < :today
      AND pps.end_datetime > pps.start_datetime
      AND pps.performing_ae_title IS NOT NULL
      AND COALESCE(am.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
    GROUP BY 1, 2
"""
STORAGE_SQL = """
    SELECT study_date, SUM(total_gb) FROM public.summary_storage_daily
    WHERE study_date >= :f AND study_date < :today GROUP BY 1
"""


def load_series(conn, today):
    """{series_key: (daily series, horizon, fit options)} for one database."""
    f = train_from(conn)
    end = today - timedelta(days=1)
    p = {"f": f, "today": today}
    out = {}

    rows = conn.execute(text(VOLUME_SQL), p).fetchall()
    if rows:
        start = max(f, min(r[0] for r in rows))
        out["volume"] = (_daily(rows, start, end), HORIZON["volume"], {})

    est = conn.execute(text(DEVICE_EST_SQL), p).fetchall()
    try:
        with conn.begin_nested():
            act = conn.execute(text(DEVICE_ACT_SQL), p).fetchall()
    except Exception:
        logger.warning("std_pps not readable: device minutes are the estimate only")
        act = []
    measured = {(r[0], pd.Timestamp(r[1])): float(r[2]) for r in act}
    by_ae = {}
    for ae, day, mins in est:
        by_ae.setdefault(ae, []).append((day, measured.get((ae, pd.Timestamp(day)), float(mins or 0))))
    recent = pd.Timestamp(today - timedelta(days=DEVICE_ACTIVE_DAYS))
    for ae, pts in by_ae.items():
        first = min(d for d, _ in pts)
        if max(pd.Timestamp(d) for d, _ in pts) < recent or (end - first).days < MIN_DAYS:
            continue
        out[f"device:{ae}"] = (_daily(pts, max(f, first), end), HORIZON["device"], {"yearly": False})

    rows = conn.execute(text(STORAGE_SQL), p).fetchall()
    if rows:
        start = max(f, min(r[0] for r in rows))
        out["storage"] = (_daily(rows, start, end), HORIZON["storage"], {"robust": True})
    return out


def _check_previous(conn, key, y, made_on):
    """Before a forecast is replaced: each of its days that has happened, with what came."""
    rows = conn.execute(text("""
        SELECT ds, yhat FROM public.ai_forecast WHERE series_key = :k AND ds <= :last
    """), {"k": key, "last": y.index[-1].date()}).fetchall()
    batch = [{"k": key, "ds": r[0], "f": float(r[1]), "a": float(y.get(pd.Timestamp(r[0]), 0.0)),
              "m": made_on} for r in rows if pd.Timestamp(r[0]) in y.index]
    if batch:
        conn.execute(text("""
            INSERT INTO public.ai_forecast_check (series_key, ds, forecast, actual, made_on)
            VALUES (:k, :ds, :f, :a, :m) ON CONFLICT (series_key, ds) DO NOTHING
        """), batch)
    return len(batch)


def run(engine, today=None):
    """Train and store every series of the database behind engine. On the main database the
    caller runs it inside rh_scope() (RH only, like the reports). Returns a summary."""
    today = today or beirut_today()
    with engine.connect() as conn:
        cal = CalendarDays(load_calendar(conn))
        series = load_series(conn, today)
    results = {}
    for key, (y, horizon, opts) in series.items():
        try:
            results[key] = (y, fit_forecast(y, cal, horizon, **opts))
        except Exception:
            logger.exception(f"forecast failed for {key}")
    with engine.begin() as conn:
        prev = {r[0]: r[1] for r in conn.execute(text(
            "SELECT series_key, trained_to FROM public.ai_forecast_run")).fetchall()}
        checked = 0
        for key, (y, (fc, info)) in results.items():
            if key in prev and prev[key] is not None:
                checked += _check_previous(conn, key, y, prev[key])
            conn.execute(text("DELETE FROM public.ai_forecast WHERE series_key = :k"), {"k": key})
            conn.execute(text("""
                INSERT INTO public.ai_forecast (series_key, ds, yhat, yhat_lower, yhat_upper)
                VALUES (:k, :ds, :y, :lo, :hi)
            """), [{"k": key, "ds": r.ds.date(), "y": round(float(r.yhat), 3),
                    "lo": round(float(r.yhat_lower), 3), "hi": round(float(r.yhat_upper), 3)}
                   for r in fc.itertuples()])
            conn.execute(text("""
                INSERT INTO public.ai_forecast_run (series_key, model, trained_from, trained_to, run_at, info)
                VALUES (:k, :m, :f, :t, now(), CAST(:i AS jsonb))
                ON CONFLICT (series_key) DO UPDATE SET model = EXCLUDED.model,
                    trained_from = EXCLUDED.trained_from, trained_to = EXCLUDED.trained_to,
                    run_at = EXCLUDED.run_at, info = EXCLUDED.info
            """), {"k": key, "m": info["model"], "f": y.index[0].date(), "t": y.index[-1].date(),
                   "i": _json(info)})
        # devices no longer active keep no stale forecast
        gone = [k for k in prev if k.startswith("device:") and k not in results]
        for k in gone:
            conn.execute(text("DELETE FROM public.ai_forecast WHERE series_key = :k"), {"k": k})
            conn.execute(text("DELETE FROM public.ai_forecast_run WHERE series_key = :k"), {"k": k})
    summary = {"series": len(results), "failed": len(series) - len(results), "checked_days": checked,
               "models": sorted({info["model"] for _y, (_fc, info) in results.values()})}
    logger.info(f"[AI forecasts] {summary}")
    return summary


def _json(obj):
    import json
    return json.dumps(obj, default=lambda o: None if isinstance(o, float) and not math.isfinite(o) else str(o))


# ── running it ────────────────────────────────────────────────────────────────

def run_for_site(site_id, today=None):
    """run() on one site's database: the main one through the RH views (rh_scope)."""
    from db import db
    from utils import site_db
    from utils.rh_scope import rh_scope
    if site_id is None or site_id == site_db.default_site_id():
        with rh_scope():
            return run(db.engine, today)
    return run(site_db.engine_for(site_id), today)


def run_all_sites():
    """The nightly job: every database, one at a time, never next to a Retrain."""
    from utils import site_db
    with _lock:
        return site_db.run_per_site("AI forecasts", run_for_site)


def is_running():
    return _lock.locked()


def start_background(app, site_id):
    """The page's Retrain button: one site, in a thread. False when a run is already going."""
    if not _lock.acquire(blocking=False):
        return False

    def work():
        try:
            with app.app_context():
                run_for_site(site_id)
        except Exception:
            logger.exception("AI forecast retrain failed")
        finally:
            _lock.release()

    threading.Thread(target=work, name="ai-forecast-retrain", daemon=True).start()
    return True


# ── referrers: their real numbers against the same weeks a year earlier ───────
#
# Operator, 2026-11-19: the first version (each referrer's share of the hospital, flagged only from
# 25 %) showed "all normal with a difference up to 20 %": a referrer growing with the hospital read
# as normal, and real 10-25 % moves were hidden. Now the counts themselves, a year apart (the same
# weeks, so season and holidays line up), every change from 10 % shown with its strength.

RECENT_WEEKS = 13
GRADES = ((0.40, "sharp"), (0.20, "clear"), (0.10, "slight"))


def referrer_windows(end):
    """(recent start, end, last year's start, last year's end): the 13 weeks to end, and the same
    13 weeks 52 weeks earlier (same weekdays)."""
    end = pd.Timestamp(end).date()
    recent_start = end - timedelta(weeks=RECENT_WEEKS) + timedelta(days=1)
    return recent_start, end, recent_start - timedelta(weeks=52), end - timedelta(weeks=52)


def referrer_trends(recent, last_year, min_studies=40, list_change=0.20, p_max=0.05):
    """Each referrer's studies in the last 13 weeks against the same weeks a year earlier.
      stopped    sent 20 or more then, 10 % of that or less now
      new        none then, 10 or more now
      declining / growing   10 % or more down / up, graded slight (10-20), clear (20-40), sharp (40+)
      steady     within 10 %
    certain = enough studies (40 in both periods together) and too large a change to be chance
    (binomial test, p < 5 %); an uncertain change keeps its arrow and is marked, not hidden.
    listed = what the page's two lists show: stopped and new, and clear or sharp changes that are
    certain. recent, last_year: {name: studies}. Returns {name: {...}}."""
    from scipy.stats import binomtest
    out = {}
    for name in set(recent) | set(last_year):
        r, b = int(recent.get(name, 0)), int(last_year.get(name, 0))
        if r + b == 0:
            continue
        if b == 0:
            out[name] = {"recent": r, "last_year": 0, "change_pct": None, "diff": r, "grade": None, "p": None,
                         "status": "new" if r >= 10 else "few", "certain": r >= 10, "listed": r >= 10}
            continue
        change = r / b - 1
        p = float(binomtest(r, r + b, 0.5).pvalue)
        certain = (r + b) >= min_studies and p < p_max
        if b >= 20 and r <= 0.1 * b:
            status = "stopped"
        elif change <= -0.10:
            status = "declining"
        elif change >= 0.10:
            status = "growing"
        else:
            status = "steady"
        grade = next((g for t, g in GRADES if abs(change) >= t), None) if status in ("declining", "growing") else None
        listed = status == "stopped" or (status in ("declining", "growing") and abs(change) >= list_change and certain)
        out[name] = {"recent": r, "last_year": b, "change_pct": round(change * 100), "diff": r - b,
                     "grade": grade, "p": p, "status": status, "certain": certain, "listed": listed}
    return out
