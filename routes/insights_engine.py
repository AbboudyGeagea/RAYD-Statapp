"""
routes/insights_engine.py
─────────────────────────
Pure Python / pandas statistical signal detection for radiology workflow.

No external AI calls. No network. Data stays local.

Each public function returns a list of signal dicts:
    {
        "signal":   str,           # machine-readable key
        "severity": "info"|"warning"|"critical",
        "entity":   str,           # technician name, radiologist name, modality, etc.
        "message":  str,           # plain-English explanation for end users
        "anchor":   str | None,    # optional super-report section anchor
    }

Entry points:
    run_tech_insights(completed_df, never_done_df)  → list[dict]
    run_rad_insights(rad_cards, signing_ts_df)      → list[dict]
    run_dept_insights(current_data, previous_data)  → list[dict]  (super report)
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────
#  PRIMITIVE HELPERS
# ─────────────────────────────────────────────────────────────────

def _cv(series: pd.Series) -> Optional[float]:
    """Coefficient of variation = std / mean.  Returns None if mean == 0."""
    s = series.dropna()
    if len(s) < 3:
        return None
    m = s.mean()
    if m == 0:
        return None
    return round(float(s.std() / m), 3)


def _skew_ratio(avg: float, median: float) -> Optional[float]:
    """avg / median.  Returns None if median is 0."""
    if not median:
        return None
    return round(avg / median, 3)


def _p90(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) < 5:
        return None
    return round(float(s.quantile(0.90)), 1)


def _signal(signal: str, severity: str, entity: str, message: str, anchor: str = None) -> dict:
    return {"signal": signal, "severity": severity, "entity": entity,
            "message": message, "anchor": anchor}


# ─────────────────────────────────────────────────────────────────
#  TECHNICIAN SIGNALS
# ─────────────────────────────────────────────────────────────────

def _batch_approval(timestamps: pd.Series, tech_name: str,
                    window_minutes: int = 5, min_count: int = 4) -> list[dict]:
    """Detect ≥ min_count completions within a rolling window_minutes window.
    Suggests rubber-stamping — exams marked done in bulk without being performed."""
    signals = []
    ts = timestamps.dropna().sort_values().reset_index(drop=True)
    if len(ts) < min_count:
        return signals

    i = 0
    while i < len(ts):
        window_end = ts[i] + pd.Timedelta(minutes=window_minutes)
        cluster = ts[(ts >= ts[i]) & (ts <= window_end)]
        if len(cluster) >= min_count:
            signals.append(_signal(
                "batch_approval", "warning", tech_name,
                f"{len(cluster)} exams marked done within {window_minutes} min "
                f"starting {ts[i].strftime('%Y-%m-%d %H:%M')} — "
                "possible bulk completion without actual performance.",
                anchor="tech-flags"
            ))
            i += len(cluster)
        else:
            i += 1
    return signals


def _end_of_shift_rush(timestamps: pd.Series, tech_name: str,
                        shift_end_hour: int = 15,
                        rush_window_minutes: int = 30) -> list[dict]:
    """Detect a disproportionate spike in completions in the last `rush_window_minutes`
    before shift end.  Suggests dumping unfinished work at end of shift."""
    signals = []
    ts = timestamps.dropna()
    if len(ts) < 6:
        return signals

    # Only look on days where there are completions throughout the shift
    for day, grp in ts.dt.normalize().to_frame(name='d').assign(ts=ts.values).groupby('d'):
        day_ts = pd.Series(grp['ts'].values)
        shift_end = pd.Timestamp(day) + pd.Timedelta(hours=shift_end_hour)
        rush_start = shift_end - pd.Timedelta(minutes=rush_window_minutes)
        before = day_ts[(day_ts >= pd.Timestamp(day)) & (day_ts < rush_start)]
        during = day_ts[(day_ts >= rush_start) & (day_ts <= shift_end)]
        if len(before) < 3 or len(during) < 2:
            continue
        # Rush if density during last window is ≥ 3× the rest of the shift
        before_rate = len(before) / max((rush_start - pd.Timestamp(day)).total_seconds() / 60, 1)
        during_rate = len(during) / rush_window_minutes
        if during_rate >= before_rate * 3:
            signals.append(_signal(
                "end_of_shift_rush", "warning", tech_name,
                f"On {day.strftime('%Y-%m-%d')}: {len(during)} completions in the "
                f"last {rush_window_minutes} min of shift vs {len(before)} in the "
                f"rest of the day — end-of-shift workload dump detected.",
                anchor="tech-trend"
            ))
    return signals


def _idle_gaps(timestamps: pd.Series, tech_name: str,
               threshold_hours: float = 2.0,
               work_start_hour: int = 7,
               work_end_hour: int = 18) -> list[dict]:
    """Detect work-hour gaps > threshold_hours with no exam completions."""
    signals = []
    ts = timestamps.dropna().sort_values()
    if len(ts) < 3:
        return signals

    for day, grp in ts.groupby(ts.dt.normalize()):
        day_ts = grp.sort_values().reset_index(drop=True)
        shift_start = pd.Timestamp(day) + pd.Timedelta(hours=work_start_hour)
        shift_end   = pd.Timestamp(day) + pd.Timedelta(hours=work_end_hour)
        day_ts_in   = day_ts[(day_ts >= shift_start) & (day_ts <= shift_end)]
        if len(day_ts_in) < 2:
            continue
        for i in range(1, len(day_ts_in)):
            gap_h = (day_ts_in.iloc[i] - day_ts_in.iloc[i - 1]).total_seconds() / 3600
            if gap_h >= threshold_hours:
                start_str = day_ts_in.iloc[i - 1].strftime('%H:%M')
                end_str   = day_ts_in.iloc[i].strftime('%H:%M')
                signals.append(_signal(
                    "idle_gap", "info", tech_name,
                    f"On {day.strftime('%Y-%m-%d')}: {gap_h:.1f}-hour gap with no "
                    f"completions during work hours ({start_str}–{end_str}).",
                    anchor="tech-trend"
                ))
    return signals


def _tech_cv(tats: pd.Series, tech_name: str) -> list[dict]:
    """High CV (> 0.8) means highly inconsistent exam turnaround."""
    cv = _cv(tats)
    if cv is None:
        return []
    if cv >= 1.2:
        return [_signal("high_cv", "warning", tech_name,
                        f"TAT coefficient of variation is {cv:.2f} — very inconsistent "
                        "turnaround. Some exams take far longer than others.",
                        anchor="tech-by-tech")]
    if cv >= 0.8:
        return [_signal("high_cv", "info", tech_name,
                        f"TAT coefficient of variation is {cv:.2f} — moderate inconsistency "
                        "in turnaround time across exams.",
                        anchor="tech-by-tech")]
    return []


# ─────────────────────────────────────────────────────────────────
#  RADIOLOGIST SIGNALS
# ─────────────────────────────────────────────────────────────────

def _rad_skew(avg: float, median: float, rad_name: str) -> list[dict]:
    """avg >> median implies outlier long-TAT cases (complex studies or breaks)."""
    ratio = _skew_ratio(avg, median)
    if ratio is None:
        return []
    if ratio >= 2.5:
        return [_signal("tat_skew", "critical", rad_name,
                        f"Avg TAT is {ratio:.1f}× the median — a few very long studies "
                        "are masking the true throughput. Check for break gaps or "
                        "stuck complex cases.",
                        anchor="rad-performance")]
    if ratio >= 1.8:
        return [_signal("tat_skew", "warning", rad_name,
                        f"Avg TAT is {ratio:.1f}× the median — some long outlier cases "
                        "are pulling up the average.",
                        anchor="rad-performance")]
    return []


def _rad_p90(tat_series: pd.Series, rad_name: str) -> list[dict]:
    """P90 / median > 4 means the tail is extremely heavy."""
    p90 = _p90(tat_series)
    if p90 is None:
        return []
    median = float(tat_series.median())
    if not median:
        return []
    ratio = round(p90 / median, 1)
    if ratio >= 5:
        return [_signal("p90_outlier", "warning", rad_name,
                        f"P90 TAT is {ratio:.1f}× the median ({p90:.0f} min vs "
                        f"{median:.0f} min) — the slowest 10% of studies are extreme "
                        "outliers. These may be complex cases or delays in signing.",
                        anchor="rad-performance")]
    return []


def _batch_signing(timestamps: pd.Series, rad_name: str,
                   window_minutes: int = 3, min_count: int = 5) -> list[dict]:
    """Detect many reports signed within a very short window — suggests signing
    without careful reading (rubber-stamping)."""
    signals = []
    ts = timestamps.dropna().sort_values().reset_index(drop=True)
    if len(ts) < min_count:
        return signals

    i = 0
    while i < len(ts):
        window_end = ts[i] + pd.Timedelta(minutes=window_minutes)
        cluster = ts[(ts >= ts[i]) & (ts <= window_end)]
        if len(cluster) >= min_count:
            signals.append(_signal(
                "batch_signing", "warning", rad_name,
                f"{len(cluster)} reports signed within {window_minutes} min "
                f"starting {ts[i].strftime('%Y-%m-%d %H:%M')} — "
                "possible batch signing without individual review.",
                anchor="rad-performance"
            ))
            i += len(cluster)
        else:
            i += 1
    return signals


def _shift_drift(timestamps: pd.Series, tat_values: pd.Series, rad_name: str) -> list[dict]:
    """TAT increasing as shift progresses (fatigue/backlog effect).
    Compares average TAT in early-shift hours vs late-shift hours."""
    if len(timestamps) < 10:
        return []
    df = pd.DataFrame({'ts': timestamps.values, 'tat': tat_values.values}).dropna()
    df['hour'] = pd.to_datetime(df['ts']).dt.hour
    early = df[df['hour'].between(7, 11)]['tat']
    late  = df[df['hour'].between(14, 18)]['tat']
    if len(early) < 3 or len(late) < 3:
        return []
    early_avg = float(early.mean())
    late_avg  = float(late.mean())
    if early_avg == 0:
        return []
    drift_pct = round((late_avg - early_avg) / early_avg * 100, 1)
    if drift_pct >= 40:
        return [_signal("shift_drift", "warning", rad_name,
                        f"Late-shift TAT ({late_avg:.0f} min) is {drift_pct}% higher than "
                        f"early-shift ({early_avg:.0f} min) — possible fatigue or backlog "
                        "building up through the day.",
                        anchor="rad-performance")]
    return []


def _case_mix_adjusted(rad_name: str, avg_tat: float, modality_benchmarks: dict,
                        modality_counts: dict) -> list[dict]:
    """Compare radiologist avg TAT against a weighted case-mix benchmark.
    modality_benchmarks = {modality: expected_tat_minutes}
    modality_counts     = {modality: count}
    """
    if not modality_benchmarks or not modality_counts:
        return []
    total = sum(modality_counts.values())
    if total == 0:
        return []
    weighted_bench = sum(
        (modality_counts.get(m, 0) / total) * t
        for m, t in modality_benchmarks.items()
    )
    if weighted_bench == 0:
        return []
    ratio = round(avg_tat / weighted_bench, 2)
    if ratio >= 1.5:
        return [_signal("case_mix_adj", "warning", rad_name,
                        f"Case-mix adjusted TAT is {ratio:.2f}× the benchmark "
                        f"({avg_tat:.0f} min vs expected {weighted_bench:.0f} min). "
                        "Performance is below expected for this workload composition.",
                        anchor="rad-performance")]
    return []


# ─────────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINTS
# ─────────────────────────────────────────────────────────────────

def run_tech_insights(completed_df: pd.DataFrame) -> list[dict]:
    """
    Accepts the `completed` DataFrame from report_25.py technician section.
    Required columns: done_by, done_at, tat_min, modality, scheduled_datetime
    Returns a flat list of signal dicts.
    """
    signals = []
    if completed_df is None or completed_df.empty:
        return signals

    cdf = completed_df.copy()
    cdf['done_at'] = pd.to_datetime(cdf['done_at'], errors='coerce')
    cdf['tat_min'] = pd.to_numeric(cdf['tat_min'], errors='coerce')

    for tech, grp in cdf[cdf['done_by'].notna()].groupby('done_by'):
        tats = grp['tat_min'].dropna()
        if len(tats) < 2:
            continue
        signals += _tech_cv(tats, tech)
        signals += _batch_approval(grp['done_at'], tech)
        signals += _end_of_shift_rush(grp['done_at'], tech)
        signals += _idle_gaps(grp['done_at'], tech)

    return signals


def run_rad_insights(rad_cards: list[dict], signing_ts_df: pd.DataFrame,
                     modality_benchmarks: dict = None) -> list[dict]:
    """
    rad_cards         — list of rad performance dicts from report_25.py
    signing_ts_df     — DataFrame with columns [radiologist, ts] from shift_patterns query
    modality_benchmarks — {modality: expected_tat_min}, optional
    Returns a flat list of signal dicts.
    """
    signals = []
    if not rad_cards:
        return signals

    # Build per-radiologist signing timestamps
    rad_ts: dict[str, pd.Series] = {}
    if signing_ts_df is not None and not signing_ts_df.empty:
        sdf = signing_ts_df.copy()
        sdf.columns = ['radiologist', 'ts']
        sdf['ts'] = pd.to_datetime(sdf['ts'], errors='coerce')
        for rad, grp in sdf.groupby('radiologist'):
            rad_ts[str(rad)] = grp['ts'].dropna().sort_values()

    for card in rad_cards:
        name    = card.get("name", "Unknown")
        avg     = card.get("overall", 0) or 0
        median  = card.get("tat_median", 0) or 0
        signals += _rad_skew(avg, median, name)

        ts = rad_ts.get(name)
        if ts is not None and len(ts) > 0:
            signals += _batch_signing(ts, name)
            # For shift drift we need tat_values aligned to timestamps —
            # use signing_ts_df if we can reconstruct, otherwise skip
            # (drilldown TAT per hour not in rad_cards)

        # P90 from drilldown — aggregate all modality TATs if available
        # (rad_cards don't carry full TAT series, so P90 skipped here;
        #  run_rad_insights_full() below handles the full-series case)

        if modality_benchmarks and card.get("drilldown"):
            mod_counts = {}
            for loc in card.get("drilldown", []):
                for m in loc.get("mods", []):
                    mod_counts[m["m"]] = mod_counts.get(m["m"], 0) + m.get("count", 0)
            signals += _case_mix_adjusted(name, avg, modality_benchmarks, mod_counts)

    return signals


def run_dept_insights(current: dict, previous: dict) -> list[dict]:
    """
    Generates department-level insights for the super report.
    current / previous are the dicts returned by super_report._collect_data().
    Returns a list of signal dicts.
    """
    signals = []

    ck = current.get("kpis", {})
    pk = previous.get("kpis", {})
    ct = current.get("tat",   {})

    # ── Volume anomaly ────────────────────────────────────────────
    cur_vol = float(ck.get("total_studies") or 0)
    prv_vol = float(pk.get("total_studies") or 0)
    if prv_vol > 0 and cur_vol > 0:
        chg = (cur_vol - prv_vol) / prv_vol * 100
        if chg <= -25:
            signals.append(_signal(
                "volume_drop", "critical", "Department",
                f"Study volume dropped {abs(chg):.1f}% vs prior period "
                f"({int(cur_vol):,} vs {int(prv_vol):,}) — investigate scheduling or "
                "equipment downtime.",
                anchor="insights-volume"
            ))
        elif chg >= 30:
            signals.append(_signal(
                "volume_surge", "info", "Department",
                f"Study volume increased {chg:.1f}% vs prior period — ensure staffing "
                "levels and reporting capacity scale accordingly.",
                anchor="insights-volume"
            ))

    # ── TAT benchmark check ───────────────────────────────────────
    median_tat = float(ct.get("median_tat_min") or 0)
    if median_tat > 0:
        if median_tat > 1440:
            signals.append(_signal(
                "tat_over_24h", "critical", "Department",
                f"Median TAT is {median_tat/60:.1f} hours — exceeds the 24-hour reporting "
                "target. Immediate review required.",
                anchor="insights-tat"
            ))
        elif median_tat > 480:
            signals.append(_signal(
                "tat_over_8h", "warning", "Department",
                f"Median TAT is {median_tat/60:.1f} hours — above the 8-hour inpatient "
                "guideline.",
                anchor="insights-tat"
            ))

    # ── TAT by modality — outlier modalities ─────────────────────
    for row in ct.get("by_modality", []):
        m_tat = float(row.get("median_tat_min") or 0)
        if m_tat > 1440:
            signals.append(_signal(
                "modality_tat_critical", "critical", row.get("modality", "Unknown"),
                f"Median TAT of {m_tat/60:.1f} hours exceeds 24-hour target "
                f"for {row.get('modality')} ({_fmt_n(row.get('cnt', 0))} studies).",
                anchor="insights-tat"
            ))

    # ── Reporting coverage ────────────────────────────────────────
    reported = int(ct.get("reported_count") or 0)
    total_st = int(ck.get("total_studies") or 1)
    cov = round(reported / total_st * 100, 1) if total_st else 0
    if cov < 60:
        signals.append(_signal(
            "low_coverage", "critical", "Department",
            f"Reporting coverage is {cov}% — fewer than 60% of studies have a signed "
            "report. Critical backlog detected.",
            anchor="insights-tat"
        ))
    elif cov < 80:
        signals.append(_signal(
            "low_coverage", "warning", "Department",
            f"Reporting coverage is {cov}% — below the 80% target. "
            f"{total_st - reported:,} studies awaiting final report.",
            anchor="insights-tat"
        ))

    # ── Storage growth ────────────────────────────────────────────
    cur_gb = float(current.get("storage", {}).get("total_gb") or 0)
    prv_gb = float(previous.get("storage", {}).get("total_gb") or 0)
    if prv_gb > 0:
        gb_chg = (cur_gb - prv_gb) / prv_gb * 100
        if gb_chg >= 40:
            signals.append(_signal(
                "storage_surge", "warning", "Department",
                f"Storage grew {gb_chg:.1f}% vs prior period ({cur_gb:.1f} GB now). "
                "Consider archiving or capacity expansion.",
                anchor="insights-volume"
            ))

    return signals


def _fmt_n(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)
