import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_27_bp = Blueprint("report_27", __name__)

def calculate_age(birth_date):
    if birth_date is None or pd.isna(birth_date):
        return np.nan
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def get_report_data(start, end):
    sql = text("""
        SELECT
            o.order_dbid,
            o.order_status,
            o.proc_id,
            o.proc_text,
            o.scheduled_datetime,
            o.has_study,
            s.study_date,
            s.storing_ae,
            s.procedure_code,
            p.birth_date,
            p.sex,
            m.duration_minutes
        FROM etl_orders o
        LEFT JOIN etl_didb_studies s 
            ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
        LEFT JOIN etl_patient_view p 
            ON p.patient_db_uid::TEXT = o.patient_dbid::TEXT
        LEFT JOIN procedure_duration_map m 
            ON m.procedure_code::TEXT = s.procedure_code::TEXT 
            OR m.procedure_code::TEXT = o.proc_id::TEXT
        WHERE o.scheduled_datetime BETWEEN :start AND :end
    """)
    res = db.session.execute(sql, {"start": start, "end": end}).fetchall()
    df = pd.DataFrame(res)
    
    if not df.empty:
        # 1. Standardize types and clean data
        df['scheduled_datetime'] = pd.to_datetime(df['scheduled_datetime'])
        df['study_date'] = pd.to_datetime(df['study_date'])
        df['match'] = df['proc_id'].astype(str).str.strip() == df['procedure_code'].astype(str).str.strip()
        
        # 2. Handle Age Grouping Safely
        df['age'] = df['birth_date'].apply(calculate_age)
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        
        bins = [-1, 8, 18, 64, 150]
        labels = ['0-8', '9-18', '19-64', '65+']
        
        # Use observed=False in groupby later, and fillna for categories
        df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels)
        df['age_group'] = df['age_group'].cat.add_categories('Unknown').fillna('Unknown')
        
    return df

@report_27_bp.route("/report/27", methods=["GET", "POST"])
@login_required
def report_27():
    today = date.today()
    go_live = get_go_live_date() or date(2025, 1, 1)
    
    # Primary Range
    start_a = request.form.get("start_date", go_live.strftime('%Y-%m-%d'))
    end_a = request.form.get("end_date", today.strftime('%Y-%m-%d'))
    # Comparison Range
    start_b = request.form.get("comp_start_date", (go_live - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_b = request.form.get("comp_end_date", go_live.strftime('%Y-%m-%d'))

    run_report = request.method == "POST"
    data = {}

    if run_report:
        df_a = get_report_data(start_a, end_a)
        df_b = get_report_data(start_b, end_b)

        if not df_a.empty:
            # Duration IQR filter (exclude NULL, 0-min entries, and statistical outliers)
            df_a['duration_minutes'] = pd.to_numeric(df_a['duration_minutes'], errors='coerce')
            dur_raw = df_a[df_a['duration_minutes'] > 0]['duration_minutes']
            if len(dur_raw) > 0:
                q1, q3 = dur_raw.quantile(0.25), dur_raw.quantile(0.75)
                iqr = q3 - q1
                dur_clean = dur_raw[dur_raw.between(q1 - 1.5 * iqr, q3 + 1.5 * iqr)]
                avg_duration = round(dur_clean.mean(), 1)
                duration_outliers_removed = int(len(dur_raw) - len(dur_clean))
            else:
                avg_duration = 0.0
                duration_outliers_removed = 0

            # Audit Metrics
            data['audit'] = {
                "total": len(df_a),
                "orphans": int(len(df_a[df_a['has_study'] == False])),
                "matches": int(df_a['match'].sum()),
                "mismatches": int(len(df_a) - df_a['match'].sum()),
                "avg_duration": avg_duration,
                "duration_outliers_removed": duration_outliers_removed,
                "hourly": df_a['scheduled_datetime'].dt.hour.value_counts().sort_index().to_dict(),
                "status_mix": df_a['order_status'].value_counts().to_dict(),
                "ae_mix": df_a['storing_ae'].fillna('Unknown').value_counts().to_dict()
            }
            
            # Comparison Metrics (observed=False handles categorical gaps)
            vol_a = len(df_a)
            vol_b = len(df_b) if not df_b.empty else 0
            delta = vol_a - vol_b
            delta_pct = round(delta / vol_b * 100, 1) if vol_b > 0 else 0.0
            # Flag as significant if change exceeds 15% — meaningful operational shift
            delta_significant = abs(delta_pct) >= 15

            data['growth'] = {
                "vol_a": vol_a,
                "vol_b": vol_b,
                "delta_pct": delta_pct,
                "delta_significant": delta_significant,
                "demo": df_a.groupby(['age_group', 'sex'], observed=False).size().unstack(fill_value=0).to_dict('index')
            }

    return render_template("report_27.html", data=data, run_report=run_report,
                           display_start=start_a, display_end=end_a,
                           comp_start=start_b, comp_end=end_b)

@report_27_bp.route("/report/27/export", methods=["POST"])
@login_required
def export_report_27():
    # Helper to re-fetch data for export based on the hidden start_date field
    start = request.form.get("start_date")
    end = request.form.get("end_date")
    df = get_report_data(start, end)
    return Response(df.to_csv(index=False), mimetype="text/csv", 
                    headers={"Content-disposition": f"attachment; filename=Audit_Export_{start}.csv"})
