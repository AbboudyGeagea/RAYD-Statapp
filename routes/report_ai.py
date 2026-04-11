import json
import logging
import numpy as np
import pandas as pd
from datetime import date, timedelta
from flask import Blueprint, render_template, request, abort, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, get_go_live_date, user_has_page

logger = logging.getLogger("REPORT_AI")
report_ai_bp = Blueprint("report_ai", __name__)

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


def _pct_change(current, previous):
    if not previous or previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


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
        prev_pct = data.get("vs_prev_pct")
        exp = f"Average daily study volume is {avg:.0f} studies. "
        if trend_slope > 0:
            exp += f"Volume is trending upward (+{trend_slope} studies/day). "
        elif trend_slope < 0:
            exp += f"Volume is trending downward ({trend_slope} studies/day). "
        else:
            exp += "Volume is stable. "
        if prev_pct is not None:
            direction = "up" if prev_pct >= 0 else "down"
            exp += f"Compared to the same period last year, volume is {direction} {abs(prev_pct)}%."
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
    rows = db.session.execute(text("""
        SELECT study_date, total_gb
        FROM summary_storage_daily
        WHERE study_date BETWEEN :s AND :e
        ORDER BY study_date
    """), {"s": start, "e": end}).fetchall()

    if not rows or len(rows) < 3:
        return None

    dates  = [str(r[0]) for r in rows]
    values = [float(r[1]) for r in rows]

    result = _linear_forecast(dates, values, 90)
    f_dates, f_vals, r2, slope = result

    current_gb    = values[-1]
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

    # Compare with same period last year
    one_year_ago_start = (pd.to_datetime(start) - timedelta(days=365)).strftime('%Y-%m-%d')
    one_year_ago_end   = (pd.to_datetime(end)   - timedelta(days=365)).strftime('%Y-%m-%d')
    prev_row = db.session.execute(text("""
        SELECT AVG(total_gb) FROM summary_storage_daily
        WHERE study_date BETWEEN :s AND :e
    """), {"s": one_year_ago_start, "e": one_year_ago_end}).fetchone()
    prev_avg  = float(prev_row[0]) if prev_row and prev_row[0] else None
    vs_prev   = _pct_change(current_gb, prev_avg)

    data = {
        "current_gb": current_gb,
        "daily_growth_gb": round(daily_growth, 3),
        "days_to_full": days_to_full,
        "capacity_gb": capacity_gb,
        "r2": r2,
        "vs_prev_pct": vs_prev,
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    f_vals
        }
    }
    data["explanation"] = _generate_explanation("storage", data)["storage"]
    return data


def _get_volume_intelligence(start, end):
    rows = db.session.execute(text("""
        SELECT study_date, COUNT(*) as cnt
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
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

    # Same period last year
    one_year_ago_start = (pd.to_datetime(start) - timedelta(days=365)).strftime('%Y-%m-%d')
    one_year_ago_end   = (pd.to_datetime(end)   - timedelta(days=365)).strftime('%Y-%m-%d')
    prev_rows = db.session.execute(text("""
        SELECT COUNT(*) FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
    """), {"s": one_year_ago_start, "e": one_year_ago_end}).fetchone()
    prev_total = int(prev_rows[0]) if prev_rows else None
    cur_total  = sum(values)
    vs_prev    = _pct_change(cur_total, prev_total)

    # Modality breakdown for context
    mod_rows = db.session.execute(text("""
        SELECT m.modality, COUNT(*) as cnt
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
        WHERE s.study_date BETWEEN :s AND :e
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """), {"s": start, "e": end}).fetchall()

    data = {
        "avg_daily": avg_daily,
        "total": cur_total,
        "slope": slope,
        "r2": r2,
        "vs_prev_pct": vs_prev,
        "vs_prev_total": prev_total,
        "modality_split": [{"name": r[0] or "UNMAPPED", "value": int(r[1])} for r in mod_rows],
        "chart": {
            "historical_dates": dates,
            "historical_vals":  values,
            "forecast_dates":   f_dates,
            "forecast_vals":    f_vals,
            "anomaly_flags":    anomalies
        }
    }
    data["explanation"] = _generate_explanation("volume", data)["volume"]
    return data


def _get_utilization_intelligence(start, end):
    # Pull utilization per AE per day using proc_duration
    rows = db.session.execute(text("""
        SELECT
            s.storing_ae,
            s.study_date,
            COALESCE(SUM(m.duration_minutes), 0) as load_mins
        FROM etl_didb_studies s
        LEFT JOIN procedure_duration_map m ON m.procedure_code = s.procedure_code
        WHERE s.study_date BETWEEN :s AND :e
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {"s": start, "e": end}).fetchall()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['ae', 'study_date', 'load_mins'])

    sched = db.session.execute(text("""
        SELECT
            UPPER(TRIM(ws.aetitle)) AS ae,
            ws.day_of_week,
            COALESCE(m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS std_opening_minutes
        FROM device_weekly_schedule ws
        LEFT JOIN aetitle_modality_map m
            ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle))
    """)).mappings().all()
    schedule_lookup = {(s['ae'], int(s['day_of_week'])): s['std_opening_minutes'] for s in sched}

    ae_results   = []
    all_anomalies = 0
    high_stress  = []
    low_util     = []

    for ae in df['ae'].unique():
        ae_df    = df[df['ae'] == ae].copy()
        ae_upper = str(ae).upper().strip()
        ae_df['study_date'] = pd.to_datetime(ae_df['study_date'])
        ae_df['dow'] = ae_df['study_date'].dt.dayofweek

        daily_utils = []
        daily_dates = []
        for _, row in ae_df.iterrows():
            dow      = int(row['dow'])
            day_cap  = schedule_lookup.get((ae_upper, dow), 0)
            util_pct = round((row['load_mins'] / day_cap * 100), 1) if day_cap > 0 else 0
            daily_utils.append(util_pct)
            daily_dates.append(str(row['study_date'].date()))

        avg_util   = round(np.mean(daily_utils), 1) if daily_utils else 0
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
            "anomaly_count": anom_count,
            "slope": slope,
            "chart": {
                "dates":          daily_dates,
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
    return data


def _get_physician_intelligence(start, end):
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
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {"s": start, "e": end}).fetchall()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['physician', 'month', 'cnt'])

    # Same period last year for comparison
    one_year_ago_start = (pd.to_datetime(start) - timedelta(days=365)).strftime('%Y-%m-%d')
    one_year_ago_end   = (pd.to_datetime(end)   - timedelta(days=365)).strftime('%Y-%m-%d')
    prev_rows = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(TRIM(CONCAT_WS(' ',
                referring_physician_first_name,
                referring_physician_last_name)), ''), 'Unknown') as physician,
            COUNT(*) as cnt
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
          AND referring_physician_first_name IS NOT NULL
        GROUP BY 1
    """), {"s": one_year_ago_start, "e": one_year_ago_end}).fetchall()
    prev_lookup = {r[0]: int(r[1]) for r in prev_rows}

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

        prev_total = prev_lookup.get(physician)
        vs_prev    = _pct_change(total, prev_total)

        if slope < -0.5 and len(counts) >= 2:
            churning.append(physician)
        elif slope > 0.5 and len(counts) >= 2:
            growing.append(physician)

        physician_data.append({
            "name":      physician,
            "total":     total,
            "slope":     slope,
            "vs_prev":   vs_prev,
            "prev_total": prev_total,
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
    return data


# ─────────────────────────────────────────────
#  ROUTE
# ─────────────────────────────────────────────

@report_ai_bp.route("/report/ai", methods=["GET", "POST"])
@login_required
def report_ai():
    if not user_has_page(current_user, 'report_ai'):
        abort(403)
    go_live = get_go_live_date() or date(2025, 1, 1)
    today   = date.today()

    start = request.values.get("start_date", go_live.strftime('%Y-%m-%d'))
    end   = request.values.get("end_date",   today.strftime('%Y-%m-%d'))
    active_tab = request.values.get("tab", "storage")

    def _safe(fn, *args):
        try:
            return fn(*args)
        except Exception as exc:
            logger.error(f"[report_ai] {fn.__name__} failed: {exc}", exc_info=True)
            try:
                db.session.rollback()
            except Exception:
                pass
            return None

    storage     = _safe(_get_storage_intelligence,     start, end)
    volume      = _safe(_get_volume_intelligence,      start, end)
    utilization = _safe(_get_utilization_intelligence, start, end)
    physician   = _safe(_get_physician_intelligence,   start, end)

    data = {
        "storage":     storage,
        "volume":      volume,
        "utilization": utilization,
        "physician":   physician
    }

    # Load current storage capacity setting for the form
    cap_row = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'storage_capacity_gb'"
    )).fetchone()
    storage_capacity_gb = float(cap_row[0]) if cap_row else 10240.0

    return render_template(
        "report_ai.html",
        data=data,
        run_report=True,
        display_start=start,
        display_end=end,
        active_tab=active_tab,
        storage_capacity_gb=storage_capacity_gb
    )


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
