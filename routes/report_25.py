import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, Response, redirect, url_for
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_25_bp = Blueprint("report_25", __name__)

def get_dynamic_report_data(inputs):
    """Fetches SQL from report_template and executes it with date parameters."""
    go_live = get_go_live_date()
    start = inputs.get('start_date') or str(go_live)
    end = inputs.get('end_date') or str(date.today())

    # Fetch SQL from the metadata table
    template_query = text("""
        SELECT report_sql_query 
        FROM report_template 
        WHERE report_id = 25 
        LIMIT 1
    """)
    
    with db.engine.connect() as conn:
        result = conn.execute(template_query).fetchone()
        if not result:
            return pd.DataFrame(), start, end
        
        raw_sql = result[0]
        df = pd.read_sql(text(raw_sql), conn, params={"start": start, "end": end})
    
    return df, start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    if request.method == "POST" and request.form.get("action") == "reset":
        return redirect(url_for('viewer.viewer_report', report_id=25))

    inputs = request.form if request.method == "POST" else request.args
    df, start, end = get_dynamic_report_data(inputs)
    
    chart_json = {}
    stats = {"avg_perf": 0, "avg_tech": 0, "avg_throughput": 0, "on_time_rate": 0, "outlier_count": 0}

    if not df.empty:
        # Standardize Datetimes
        date_cols = ['study_date', 'scheduled_datetime', 'archive_time', 'order_entry_time']
        for col in [c for c in date_cols if c in df.columns]:
            df[col] = pd.to_datetime(df[col])

        # TAT Calculations
        if 'study_date' in df.columns and 'scheduled_datetime' in df.columns:
            df['perf_min'] = (df['study_date'] - df['scheduled_datetime']).dt.total_seconds() / 60
        else:
            df['perf_min'] = 0

        if 'archive_time' in df.columns and 'study_date' in df.columns:
            df['tech_min'] = (df['archive_time'] - df['study_date']).dt.total_seconds() / 60
        
        if 'study_date' in df.columns and 'order_entry_time' in df.columns:
            df['total_tat_hrs'] = (df['study_date'] - df['order_entry_time']).dt.total_seconds() / 3600

        # Outlier Detection (>15 mins is non-normal for Rad workflow)
        outlier_threshold = 15
        outliers = df[df['perf_min'] > outlier_threshold]

        stats = {
            "avg_perf": round(df['perf_min'].mean(), 1) if 'perf_min' in df.columns else 0,
            "avg_tech": round(df['tech_min'].mean(), 1) if 'tech_min' in df.columns else 0,
            "avg_throughput": round(df['total_tat_hrs'].mean(), 1) if 'total_tat_hrs' in df.columns else 0,
            "on_time_rate": round((len(df[df['perf_min'] <= outlier_threshold]) / len(df)) * 100, 1),
            "total_count": len(df),
            "outlier_count": len(outliers)
        }

        # Modality Chart
        if 'storing_ae' in df.columns:
            modality_avg = df.groupby('storing_ae')['perf_min'].mean().sort_values(ascending=False).head(10)
            chart_json['modality_bar'] = {
                "labels": modality_avg.index.tolist(),
                "datasets": [{
                    "label": "Avg Lateness (Min)",
                    "data": modality_avg.tolist(),
                    "backgroundColor": "#38ada9"
                }]
            }

    results = df.to_dict(orient='records')
    return render_template("report_25.html", results=results, stats=stats, chart_json=chart_json, start_date=start, end_date=end, run_report=not df.empty)

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    df, _, _ = get_dynamic_report_data(request.form)
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename=TAT_25_{date.today()}.csv"})
