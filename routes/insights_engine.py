"""
routes/insights_engine.py
─────────────────────────
Pure Python / pandas statistical signal detection. No external calls.

Signal dict shape:
    {
        "signal":   str,
        "severity": "info"|"warning"|"critical",
        "entity":   str,
        "message":  str,
        "anchor":   str | None,
        "evidence": list[dict] | None   ← traceable records (accession, user, time)
    }

Entry points:
    run_tech_insights(completed_df)              → list[dict]
    run_rad_insights(rad_cards, signing_ts_df)   → list[dict]
    run_dept_insights(current_data, prev_data)   → list[dict]
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


def _sig(signal: str, severity: str, entity: str, message: str,
         anchor: str = None, evidence: list = None) -> dict:
    return {"signal": signal, "severity": severity, "entity": entity,
            "message": message, "anchor": anchor, "evidence": evidence or []}


# ─────────────────────────────────────────────────────────────────
#  TECHNICIAN SIGNALS
# ─────────────────────────────────────────────────────────────────

def _batch_approval(grp: pd.DataFrame, name: str) -> list[dict]:
    """≥3 exams completed within 4 min — possible rubber-stamping.
    grp must have: done_at, accession_number (optional), modality (optional)
    """
    out = []
    ts_col = grp['done_at'].dropna().sort_values()
    ts = ts_col.reset_index()   # keeps original index for row lookup
    ts.columns = ['orig_idx', 'done_at']

    i = 0
    while i < len(ts):
        t0  = ts.iloc[i]['done_at']
        mask = (ts['done_at'] >= t0) & (ts['done_at'] <= t0 + pd.Timedelta(minutes=4))
        cluster = ts[mask]
        if len(cluster) >= 3:
            rows = grp.loc[cluster['orig_idx']]
            evidence = []
            for _, r in rows.iterrows():
                evidence.append({
                    "accession": str(r.get('accession_number', '') or ''),
                    "user":      str(name),
                    "time":      r['done_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(r['done_at']) else '',
                    "modality":  str(r.get('modality', '') or ''),
                })
            out.append(_sig(
                "batch_approval", "warning", name,
                f"{len(cluster)} exams done in 4 min ({t0.strftime('%Y-%m-%d %H:%M')}) — bulk completion?",
                "tech-flags", evidence
            ))
            i += len(cluster)
        else:
            i += 1
    return out


def _end_of_shift_rush(grp: pd.DataFrame, name: str, shift_end_hour: int = 15) -> list[dict]:
    """Completion density 3× higher in last 30 min of shift vs rest of day."""
    out = []
    ts = grp['done_at'].dropna()
    if len(ts) < 4:
        return out
    for day, day_grp in grp.dropna(subset=['done_at']).groupby(grp['done_at'].dt.normalize()):
        day_ts   = day_grp['done_at'].sort_values()
        end_ts   = pd.Timestamp(day) + pd.Timedelta(hours=shift_end_hour)
        rush_ts  = end_ts - pd.Timedelta(minutes=30)
        before   = day_ts[day_ts < rush_ts]
        during_mask = (day_ts >= rush_ts) & (day_ts <= end_ts)
        during   = day_ts[during_mask]
        if len(before) < 2 or len(during) < 2:
            continue
        before_rate = len(before) / max((rush_ts - pd.Timestamp(day)).total_seconds() / 60, 1)
        if (len(during) / 30) >= before_rate * 3:
            rows = day_grp[during_mask]
            evidence = [{
                "accession": str(r.get('accession_number', '') or ''),
                "user":      str(name),
                "time":      r['done_at'].strftime('%H:%M') if pd.notna(r['done_at']) else '',
                "modality":  str(r.get('modality', '') or ''),
            } for _, r in rows.iterrows()]
            out.append(_sig(
                "end_of_shift_rush", "warning", name,
                f"{day.strftime('%Y-%m-%d')}: {len(during)} completions in last 30 min "
                f"vs {len(before)} rest of day — end-of-shift rush.",
                "tech-trend", evidence
            ))
    return out


def _idle_gaps(grp: pd.DataFrame, name: str,
               threshold_hours: float = 2.5,
               work_start: int = 7, work_end: int = 18) -> list[dict]:
    out = []
    ts = grp['done_at'].dropna().sort_values()
    if len(ts) < 2:
        return out
    for day, day_ts_raw in ts.groupby(ts.dt.normalize()):
        day_ts = day_ts_raw.sort_values().reset_index(drop=True)
        in_shift = day_ts[(day_ts >= pd.Timestamp(day) + pd.Timedelta(hours=work_start)) &
                          (day_ts <= pd.Timestamp(day) + pd.Timedelta(hours=work_end))]
        if len(in_shift) < 2:
            continue
        for i in range(1, len(in_shift)):
            gap_h = (in_shift.iloc[i] - in_shift.iloc[i - 1]).total_seconds() / 3600
            if gap_h >= threshold_hours:
                out.append(_sig(
                    "idle_gap", "info", name,
                    f"{day.strftime('%Y-%m-%d')}: {gap_h:.1f}h gap "
                    f"({in_shift.iloc[i-1].strftime('%H:%M')}–{in_shift.iloc[i].strftime('%H:%M')}).",
                    "tech-trend"
                ))
    return out


def _tech_cv(tats: pd.Series, name: str) -> list[dict]:
    cv = _cv(tats)
    if cv is None:
        return []
    if cv >= 1.2:
        return [_sig("high_cv", "warning", name, f"TAT CV={cv:.2f} — very inconsistent turnaround.", "tech-by-tech")]
    if cv >= 0.8:
        return [_sig("high_cv", "info",    name, f"TAT CV={cv:.2f} — moderate TAT inconsistency.",   "tech-by-tech")]
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
                     f"Avg {avg:.0f}m vs median {median:.0f}m ({ratio:.1f}×) — outlier cases inflating average.",
                     "rad-performance")]
    if ratio >= 1.8:
        return [_sig("tat_skew", "warning", name,
                     f"Avg {avg:.0f}m vs median {median:.0f}m ({ratio:.1f}×) — a few slow cases pulling average up.",
                     "rad-performance")]
    return []


def _batch_signing(rad_grp: pd.DataFrame, name: str,
                   window_minutes: int = 3, min_count: int = 5) -> list[dict]:
    """≥5 reports signed within 3 min. rad_grp columns: ts, accession_number (optional)"""
    out = []
    ts_col = rad_grp['ts'].dropna().sort_values()
    ts = ts_col.reset_index()
    ts.columns = ['orig_idx', 'ts']

    i = 0
    while i < len(ts):
        t0   = ts.iloc[i]['ts']
        mask = (ts['ts'] >= t0) & (ts['ts'] <= t0 + pd.Timedelta(minutes=window_minutes))
        cluster = ts[mask]
        if len(cluster) >= min_count:
            rows = rad_grp.loc[cluster['orig_idx']]
            evidence = []
            for _, r in rows.iterrows():
                evidence.append({
                    "accession": str(r.get('accession_number', '') or ''),
                    "user":      str(name),
                    "time":      r['ts'].strftime('%Y-%m-%d %H:%M') if pd.notna(r['ts']) else '',
                    "modality":  "",
                })
            out.append(_sig(
                "batch_signing", "warning", name,
                f"{len(cluster)} reports signed in {window_minutes} min ({t0.strftime('%Y-%m-%d %H:%M')}) — batch signing?",
                "rad-performance", evidence
            ))
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


# ─────────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINTS
# ─────────────────────────────────────────────────────────────────

def run_tech_insights(completed_df: pd.DataFrame) -> list[dict]:
    """
    completed_df expected columns: done_by, done_at, tat_min, modality,
                                   scheduled_datetime, accession_number
    """
    signals = []
    if completed_df is None or completed_df.empty:
        return signals

    cdf = completed_df.copy()
    cdf['done_at'] = pd.to_datetime(cdf['done_at'], errors='coerce')
    if 'tat_min' in cdf.columns:
        cdf['tat_min'] = pd.to_numeric(cdf['tat_min'], errors='coerce')

    for tech, grp in cdf[cdf['done_by'].notna()].groupby('done_by'):
        grp = grp.copy()
        tats = grp['tat_min'].dropna() if 'tat_min' in grp.columns else pd.Series(dtype=float)
        if len(tats) >= 2:
            signals += _tech_cv(tats, tech)
        signals += _batch_approval(grp, tech)
        signals += _end_of_shift_rush(grp, tech)
        signals += _idle_gaps(grp, tech)

    # Deduplicate: one signal type per entity
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
    rad_cards       — list of rad perf dicts (name, overall, tat_median, drilldown)
    signing_ts_df   — DataFrame[radiologist, ts, accession_number]
    """
    signals = []
    if not rad_cards:
        return signals

    rad_grps: dict[str, pd.DataFrame] = {}
    if signing_ts_df is not None and not signing_ts_df.empty:
        sdf = signing_ts_df.copy()
        if sdf.shape[1] == 3:
            sdf.columns = ['radiologist', 'ts', 'accession_number']
        else:
            sdf.columns = ['radiologist', 'ts']
            sdf['accession_number'] = ''
        sdf['ts'] = pd.to_datetime(sdf['ts'], errors='coerce')
        for rad, grp in sdf.groupby('radiologist'):
            rad_grps[str(rad)] = grp.dropna(subset=['ts']).sort_values('ts')

    for card in rad_cards:
        name   = card.get("name", "Unknown")
        avg    = card.get("overall", 0) or 0
        median = card.get("tat_median", 0) or 0
        signals += _rad_skew(avg, median, name)

        grp = rad_grps.get(name)
        if grp is not None and len(grp) > 0:
            signals += _batch_signing(grp, name)

    seen, deduped = set(), []
    for s in signals:
        key = (s['signal'], s['entity'])
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    return deduped


def run_dept_insights(current: dict, previous: dict) -> list[dict]:
    signals = []
    ck = current.get("kpis", {})
    pk = previous.get("kpis", {})
    ct = current.get("tat", {})
    cs = current.get("storage", {})
    ps = previous.get("storage", {})

    cur_vol = float(ck.get("total_studies") or 0)
    prv_vol = float(pk.get("total_studies") or 0)
    if prv_vol > 0 and cur_vol > 0:
        chg = (cur_vol - prv_vol) / prv_vol * 100
        if chg <= -25:
            signals.append(_sig("volume_drop", "critical", "Department",
                                f"Volume dropped {abs(chg):.0f}% vs prior period ({int(cur_vol):,} vs {int(prv_vol):,}).",
                                "insights-volume"))
        elif chg >= 30:
            signals.append(_sig("volume_surge", "info", "Department",
                                f"Volume up {chg:.0f}% vs prior period — check staffing.", "insights-volume"))

    median_tat = float(ct.get("median_tat_min") or 0)
    if median_tat > 1440:
        signals.append(_sig("tat_over_24h", "critical", "Department",
                            f"Median TAT {median_tat/60:.1f}h — exceeds 24h target.", "insights-tat"))
    elif median_tat > 480:
        signals.append(_sig("tat_over_8h", "warning", "Department",
                            f"Median TAT {median_tat/60:.1f}h — above 8h inpatient guideline.", "insights-tat"))

    for row in ct.get("by_modality", []):
        m_tat = float(row.get("median_tat_min") or 0)
        if m_tat > 1440:
            signals.append(_sig("modality_tat_critical", "critical",
                                row.get("modality", "Unknown"),
                                f"{row.get('modality')} median TAT {m_tat/60:.1f}h — exceeds 24h target.",
                                "insights-tat"))

    reported = int(ct.get("reported_count") or 0)
    total_st = int(ck.get("total_studies") or 1)
    cov = round(reported / total_st * 100, 1) if total_st else 0
    if cov < 60:
        signals.append(_sig("low_coverage", "critical", "Department",
                            f"Reporting coverage {cov}% — critical backlog ({total_st - reported:,} unsigned).",
                            "insights-tat"))
    elif cov < 80:
        signals.append(_sig("low_coverage", "warning", "Department",
                            f"Reporting coverage {cov}% — below 80% target ({total_st - reported:,} unsigned).",
                            "insights-tat"))

    cur_gb = float(cs.get("total_gb") or 0)
    prv_gb = float(ps.get("total_gb") or 0)
    if prv_gb > 0:
        gb_chg = (cur_gb - prv_gb) / prv_gb * 100
        if gb_chg >= 40:
            signals.append(_sig("storage_surge", "warning", "Department",
                                f"Storage +{gb_chg:.0f}% vs prior period ({cur_gb:.1f} GB).",
                                "insights-volume"))

    return signals
