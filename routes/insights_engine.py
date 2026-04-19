"""
routes/insights_engine.py
─────────────────────────
Pure Python / pandas statistical signal detection for radiology workflow.
No external AI calls. No network. Data stays local.

Each public function returns a list of signal dicts:
    { "signal", "severity" (info|warning|critical), "entity", "message", "anchor" }

Entry points:
    run_tech_insights(completed_df)             → list[dict]
    run_rad_insights(rad_cards, signing_ts_df)  → list[dict]
    run_dept_insights(current_data, previous_data) → list[dict]
"""
from __future__ import annotations
from typing import Optional
import pandas as pd


# ─────────────────────────────────────────────────────────────────
#  PRIMITIVES
# ─────────────────────────────────────────────────────────────────

def _cv(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) < 3 or s.mean() == 0:
        return None
    return round(float(s.std() / s.mean()), 2)


def _skew_ratio(avg: float, median: float) -> Optional[float]:
    return round(avg / median, 2) if median else None


def _p90(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    return round(float(s.quantile(0.90)), 1) if len(s) >= 5 else None


def _sig(signal: str, severity: str, entity: str, message: str, anchor: str = None) -> dict:
    return {"signal": signal, "severity": severity, "entity": entity,
            "message": message, "anchor": anchor}


# ─────────────────────────────────────────────────────────────────
#  TECHNICIAN SIGNALS
# ─────────────────────────────────────────────────────────────────

def _batch_approval(timestamps: pd.Series, name: str) -> list[dict]:
    """≥3 exams completed within 4 min = possible rubber-stamping."""
    out, ts = [], timestamps.dropna().sort_values().reset_index(drop=True)
    i = 0
    while i < len(ts):
        cluster = ts[(ts >= ts[i]) & (ts <= ts[i] + pd.Timedelta(minutes=4))]
        if len(cluster) >= 3:
            out.append(_sig("batch_approval", "warning", name,
                            f"{len(cluster)} exams done in 4 min "
                            f"({ts[i].strftime('%Y-%m-%d %H:%M')}) — bulk completion?",
                            "tech-flags"))
            i += len(cluster)
        else:
            i += 1
    return out


def _end_of_shift_rush(timestamps: pd.Series, name: str, shift_end_hour: int = 15) -> list[dict]:
    """Completion density 3× higher in last 30 min of shift vs rest of day."""
    out, ts = [], timestamps.dropna()
    if len(ts) < 4:
        return out
    for day, grp in ts.groupby(ts.dt.normalize()):
        day_ts   = pd.Series(grp.values).sort_values()
        end_ts   = pd.Timestamp(day) + pd.Timedelta(hours=shift_end_hour)
        rush_ts  = end_ts - pd.Timedelta(minutes=30)
        before   = day_ts[day_ts < rush_ts]
        during   = day_ts[(day_ts >= rush_ts) & (day_ts <= end_ts)]
        if len(before) < 2 or len(during) < 2:
            continue
        before_rate = len(before) / max((rush_ts - pd.Timestamp(day)).total_seconds() / 60, 1)
        if (len(during) / 30) >= before_rate * 3:
            out.append(_sig("end_of_shift_rush", "warning", name,
                            f"{day.strftime('%Y-%m-%d')}: {len(during)} completions in last "
                            f"30 min vs {len(before)} rest of day — end-of-shift rush.",
                            "tech-trend"))
    return out


def _idle_gaps(timestamps: pd.Series, name: str,
               threshold_hours: float = 2.5,
               work_start: int = 7, work_end: int = 18) -> list[dict]:
    """Work-hour gap > threshold with no completions — possible unlogged downtime."""
    out, ts = [], timestamps.dropna().sort_values()
    if len(ts) < 2:
        return out
    for day, grp in ts.groupby(ts.dt.normalize()):
        day_ts = grp.sort_values().reset_index(drop=True)
        in_shift = day_ts[(day_ts >= pd.Timestamp(day) + pd.Timedelta(hours=work_start)) &
                          (day_ts <= pd.Timestamp(day) + pd.Timedelta(hours=work_end))]
        if len(in_shift) < 2:
            continue
        for i in range(1, len(in_shift)):
            gap_h = (in_shift.iloc[i] - in_shift.iloc[i - 1]).total_seconds() / 3600
            if gap_h >= threshold_hours:
                out.append(_sig("idle_gap", "info", name,
                                f"{day.strftime('%Y-%m-%d')}: {gap_h:.1f}h gap "
                                f"({in_shift.iloc[i-1].strftime('%H:%M')}–"
                                f"{in_shift.iloc[i].strftime('%H:%M')}) no completions.",
                                "tech-trend"))
    return out


def _tech_cv(tats: pd.Series, name: str) -> list[dict]:
    cv = _cv(tats)
    if cv is None:
        return []
    if cv >= 1.2:
        return [_sig("high_cv", "warning", name,
                     f"TAT CV={cv:.2f} — very inconsistent turnaround.", "tech-by-tech")]
    if cv >= 0.8:
        return [_sig("high_cv", "info", name,
                     f"TAT CV={cv:.2f} — moderate TAT inconsistency.", "tech-by-tech")]
    return []


# ─────────────────────────────────────────────────────────────────
#  RADIOLOGIST SIGNALS
# ─────────────────────────────────────────────────────────────────

def _rad_skew(avg: float, median: float, name: str) -> list[dict]:
    ratio = _skew_ratio(avg, median)
    if ratio is None:
        return []
    if ratio >= 2.5:
        return [_sig("tat_skew", "critical", name,
                     f"Avg {avg:.0f}m vs median {median:.0f}m ({ratio:.1f}×) — "
                     "outlier cases inflating average.", "rad-performance")]
    if ratio >= 1.8:
        return [_sig("tat_skew", "warning", name,
                     f"Avg {avg:.0f}m vs median {median:.0f}m ({ratio:.1f}×) — "
                     "a few slow cases pulling up the average.", "rad-performance")]
    return []


def _batch_signing(timestamps: pd.Series, name: str,
                   window_minutes: int = 3, min_count: int = 5) -> list[dict]:
    """≥5 reports signed within 3 min = possible batch signing."""
    out, ts = [], timestamps.dropna().sort_values().reset_index(drop=True)
    i = 0
    while i < len(ts):
        cluster = ts[(ts >= ts[i]) & (ts <= ts[i] + pd.Timedelta(minutes=window_minutes))]
        if len(cluster) >= min_count:
            out.append(_sig("batch_signing", "warning", name,
                            f"{len(cluster)} reports signed in {window_minutes} min "
                            f"({ts[i].strftime('%Y-%m-%d %H:%M')}) — batch signing?",
                            "rad-performance"))
            i += len(cluster)
        else:
            i += 1
    return out


def _shift_drift(timestamps: pd.Series, tat_values: pd.Series, name: str) -> list[dict]:
    if len(timestamps) < 10:
        return []
    df = pd.DataFrame({'ts': timestamps.values, 'tat': tat_values.values}).dropna()
    df['hour'] = pd.to_datetime(df['ts']).dt.hour
    early, late = df[df['hour'].between(7, 11)]['tat'], df[df['hour'].between(14, 18)]['tat']
    if len(early) < 3 or len(late) < 3 or not early.mean():
        return []
    drift = round((late.mean() - early.mean()) / early.mean() * 100, 1)
    if drift >= 40:
        return [_sig("shift_drift", "warning", name,
                     f"Late-shift TAT {drift:.0f}% higher than early-shift "
                     f"({late.mean():.0f}m vs {early.mean():.0f}m) — fatigue/backlog.",
                     "rad-performance")]
    return []


def _case_mix_adjusted(name: str, avg_tat: float,
                        benchmarks: dict, mod_counts: dict) -> list[dict]:
    total = sum(mod_counts.values())
    if total == 0:
        return []
    bench = sum((mod_counts.get(m, 0) / total) * t for m, t in benchmarks.items())
    if bench == 0:
        return []
    ratio = round(avg_tat / bench, 2)
    if ratio >= 1.5:
        return [_sig("case_mix_adj", "warning", name,
                     f"Case-mix adjusted TAT {ratio:.2f}× benchmark "
                     f"({avg_tat:.0f}m vs expected {bench:.0f}m).",
                     "rad-performance")]
    return []


# ─────────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINTS
# ─────────────────────────────────────────────────────────────────

def run_tech_insights(completed_df: pd.DataFrame) -> list[dict]:
    """
    completed_df columns: done_by, done_at, tat_min, modality, scheduled_datetime
    """
    signals = []
    if completed_df is None or completed_df.empty:
        return signals
    cdf = completed_df.copy()
    cdf['done_at'] = pd.to_datetime(cdf['done_at'], errors='coerce')
    cdf['tat_min'] = pd.to_numeric(cdf.get('tat_min', pd.Series(dtype=float)), errors='coerce')

    for tech, grp in cdf[cdf['done_by'].notna()].groupby('done_by'):
        tats = grp['tat_min'].dropna()
        if len(tats) < 2:
            continue
        signals += _tech_cv(tats, tech)
        signals += _batch_approval(grp['done_at'], tech)
        signals += _end_of_shift_rush(grp['done_at'], tech)
        signals += _idle_gaps(grp['done_at'], tech)

    # Deduplicate: one warning per signal type per entity
    seen, deduped = set(), []
    for s in signals:
        key = (s['signal'], s['entity'])
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    return deduped


def run_rad_insights(rad_cards: list[dict], signing_ts_df,
                     modality_benchmarks: dict = None) -> list[dict]:
    """
    rad_cards       — list of rad perf dicts from report_25 (name, overall, tat_median, drilldown)
    signing_ts_df   — DataFrame[radiologist, ts] from shift_patterns query
    """
    signals = []
    if not rad_cards:
        return signals

    rad_ts: dict[str, pd.Series] = {}
    if signing_ts_df is not None and not signing_ts_df.empty:
        sdf = signing_ts_df.copy()
        sdf.columns = ['radiologist', 'ts']
        sdf['ts'] = pd.to_datetime(sdf['ts'], errors='coerce')
        for rad, grp in sdf.groupby('radiologist'):
            rad_ts[str(rad)] = grp['ts'].dropna().sort_values()

    for card in rad_cards:
        name   = card.get("name", "Unknown")
        avg    = card.get("overall", 0) or 0
        median = card.get("tat_median", 0) or 0
        signals += _rad_skew(avg, median, name)

        ts = rad_ts.get(name)
        if ts is not None and len(ts) > 0:
            signals += _batch_signing(ts, name)

        if modality_benchmarks and card.get("drilldown"):
            mod_counts = {}
            for loc in card.get("drilldown", []):
                for m in loc.get("mods", []):
                    mod_counts[m["m"]] = mod_counts.get(m["m"], 0) + m.get("count", 0)
            signals += _case_mix_adjusted(name, avg, modality_benchmarks, mod_counts)

    # Deduplicate per signal type + entity
    seen, deduped = set(), []
    for s in signals:
        key = (s['signal'], s['entity'])
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    return deduped


def run_dept_insights(current: dict, previous: dict) -> list[dict]:
    """Department-level signals for the super report."""
    signals = []
    ck = current.get("kpis", {})
    pk = previous.get("kpis", {})
    ct = current.get("tat", {})
    cs = current.get("storage", {})
    ps = previous.get("storage", {})

    # Volume change
    cur_vol = float(ck.get("total_studies") or 0)
    prv_vol = float(pk.get("total_studies") or 0)
    if prv_vol > 0 and cur_vol > 0:
        chg = (cur_vol - prv_vol) / prv_vol * 100
        if chg <= -25:
            signals.append(_sig("volume_drop", "critical", "Department",
                                f"Volume dropped {abs(chg):.0f}% vs prior period "
                                f"({int(cur_vol):,} vs {int(prv_vol):,}).",
                                "insights-volume"))
        elif chg >= 30:
            signals.append(_sig("volume_surge", "info", "Department",
                                f"Volume up {chg:.0f}% vs prior period — check staffing.",
                                "insights-volume"))

    # TAT
    median_tat = float(ct.get("median_tat_min") or 0)
    if median_tat > 1440:
        signals.append(_sig("tat_over_24h", "critical", "Department",
                            f"Median TAT {median_tat/60:.1f}h — exceeds 24h target.",
                            "insights-tat"))
    elif median_tat > 480:
        signals.append(_sig("tat_over_8h", "warning", "Department",
                            f"Median TAT {median_tat/60:.1f}h — above 8h inpatient guideline.",
                            "insights-tat"))

    for row in ct.get("by_modality", []):
        m_tat = float(row.get("median_tat_min") or 0)
        if m_tat > 1440:
            signals.append(_sig("modality_tat_critical", "critical",
                                row.get("modality", "Unknown"),
                                f"Median TAT {m_tat/60:.1f}h — exceeds 24h target "
                                f"({_fn(row.get('cnt', 0))} studies).",
                                "insights-tat"))

    # Reporting coverage
    reported = int(ct.get("reported_count") or 0)
    total_st = int(ck.get("total_studies") or 1)
    cov = round(reported / total_st * 100, 1) if total_st else 0
    if cov < 60:
        signals.append(_sig("low_coverage", "critical", "Department",
                            f"Reporting coverage {cov}% — critical backlog "
                            f"({total_st - reported:,} unsigned).",
                            "insights-tat"))
    elif cov < 80:
        signals.append(_sig("low_coverage", "warning", "Department",
                            f"Reporting coverage {cov}% — below 80% target "
                            f"({total_st - reported:,} unsigned).",
                            "insights-tat"))

    # Storage growth
    cur_gb = float(cs.get("total_gb") or 0)
    prv_gb = float(ps.get("total_gb") or 0)
    if prv_gb > 0:
        gb_chg = (cur_gb - prv_gb) / prv_gb * 100
        if gb_chg >= 40:
            signals.append(_sig("storage_surge", "warning", "Department",
                                f"Storage +{gb_chg:.0f}% vs prior period ({cur_gb:.1f} GB).",
                                "insights-volume"))

    return signals


def _fn(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)
