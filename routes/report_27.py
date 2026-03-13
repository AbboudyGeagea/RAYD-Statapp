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
            COALESCE(m.duration_minutes, 0) as duration_minutes
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
            # Orphan drill-down table (top 50)
            orphan_df = df_a[df_a['has_study'] == False][['proc_id','proc_text','scheduled_datetime','order_status']].head(50)
            orphan_list = []
            for _, row in orphan_df.iterrows():
                orphan_list.append({
                    "proc": str(row['proc_id']) if row['proc_id'] else 'N/A',
                    "desc": str(row['proc_text'])[:40] if row['proc_text'] else 'N/A',
                    "date": row['scheduled_datetime'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['scheduled_datetime']) else 'N/A',
                    "status": str(row['order_status']) if row['order_status'] else 'N/A'
                })

            # Weekly match rate trend
            df_a['week'] = df_a['scheduled_datetime'].dt.to_period('W').astype(str)
            weekly = df_a.groupby('week').apply(
                lambda x: round(x['match'].sum() / len(x) * 100, 1) if len(x) > 0 else 0
            ).reset_index()
            weekly.columns = ['week', 'match_rate']

            # No-show / cancellation rate
            cancelled_statuses = ['Cancelled', 'CANCELLED', 'No Show', 'NO SHOW', 'Canceled']
            cancelled_count = int(df_a[df_a['order_status'].isin(cancelled_statuses)].shape[0])
            cancellation_rate = round(cancelled_count / len(df_a) * 100, 1) if len(df_a) > 0 else 0

            # Daily cancellation trend
            df_a['day'] = df_a['scheduled_datetime'].dt.date.astype(str)
            daily_cancel = df_a[df_a['order_status'].isin(cancelled_statuses)].groupby('day').size().reset_index()
            daily_total = df_a.groupby('day').size().reset_index()
            daily_total.columns = ['day', 'total']
            if not daily_cancel.empty:
                daily_cancel.columns = ['day', 'cancelled']
                daily_merged = daily_total.merge(daily_cancel, on='day', how='left').fillna(0)
                daily_merged['rate'] = (daily_merged['cancelled'] / daily_merged['total'] * 100).round(1)
                cancel_trend = {"labels": daily_merged['day'].tolist(), "values": daily_merged['rate'].tolist()}
            else:
                cancel_trend = {"labels": [], "values": []}

            # Audit Metrics
            data['audit'] = {
                "total": len(df_a),
                "orphans": int(len(df_a[df_a['has_study'] == False])),
                "orphan_list": orphan_list,
                "matches": int(df_a['match'].sum()),
                "mismatches": int(len(df_a) - df_a['match'].sum()),
                "avg_duration": round(df_a['duration_minutes'].mean(), 1),
                "hourly": df_a['scheduled_datetime'].dt.hour.value_counts().sort_index().to_dict(),
                "status_mix": df_a['order_status'].value_counts().to_dict(),
                "ae_mix": df_a['storing_ae'].fillna('Unknown').value_counts().to_dict(),
                "weekly_match": {"labels": weekly['week'].tolist(), "values": weekly['match_rate'].tolist()},
                "cancellation_rate": cancellation_rate,
                "cancel_trend": cancel_trend,
                "cancelled_count": cancelled_count
            }
            
            # Comparison Metrics (observed=False handles categorical gaps)
            data['growth'] = {
                "vol_a": len(df_a),
                "vol_b": len(df_b) if not df_b.empty else 0,
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
