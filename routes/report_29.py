import pandas as pd
from io import BytesIO
from datetime import date
from flask import Blueprint, render_template, request, Response, make_response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date # Using the utility we already have

report_29_bp = Blueprint("report_29", __name__)

def get_report_data(form_data):
    # 1. Date Handling (Dynamic Go-Live)
    go_live_result = get_etl_cutoff_date()
    default_start = str(go_live_result) if go_live_result else "2025-01-01"
    default_end = date.today().strftime('%Y-%m-%d')

    start = form_data.get("start_date") or default_start
    end = form_data.get("end_date") or default_end

    # 2. Get the SQL
    template_sql = db.session.execute(
        text("SELECT report_sql_query FROM report_template WHERE report_id = 29")
    ).scalar()

    if not template_sql:
        return pd.DataFrame(), start, end

    res = db.session.execute(text(template_sql), {"start": start, "end": end}).mappings().all()
    df = pd.DataFrame(res)

    # --- THE FIX: Force numeric types ---
    if not df.empty:
        # Convert total_gb and study_count to floats/ints
        df['total_gb'] = pd.to_numeric(df['total_gb'], errors='coerce').fillna(0)
        df['study_count'] = pd.to_numeric(df['study_count'], errors='coerce').fillna(0)
        
        # Calculate Density safely (prevents division by zero)
        df['avg_mb_per_study'] = (df['total_gb'] * 1024 / df['study_count'].replace(0, 1)).round(2)

    return df, start, end
@report_29_bp.route("/report/29", methods=["GET", "POST"])
@login_required
def report_29():
    run_report = False
    stats = {'total_tb': 0}
    modality_bar_json = {}
    proc_bar_json = {}
    table_data = []
    alerts = []
    
    # Initial load dates
    _, display_start, display_end = get_report_data({})

    if request.method == "POST":
        run_report = True
        df, display_start, display_end = get_report_data(request.form)

        if not df.empty:
            stats['total_tb'] = round(df['total_gb'].sum() / 1024, 2)
            
            # 1. Modality Storage (Summing by Modality)
            mod_df = df.groupby('modality')['total_gb'].sum().sort_values(ascending=False).reset_index()
            modality_bar_json = {
                "labels": mod_df['modality'].fillna('Unknown').tolist(),
                "datasets": [{"label": "GB", "data": mod_df['total_gb'].round(2).tolist(), "backgroundColor": "#38ada9"}]
            }

            # 2. Top 12 Procedures by Volume
            # Grouping by procedure in case the raw data has multiple dates
            proc_df = df.groupby('procedure_code')['total_gb'].sum().sort_values(ascending=False).head(12).reset_index()
            proc_bar_json = {
                "labels": proc_df['procedure_code'].tolist(),
                "datasets": [{"label": "Total GB", "data": proc_df['total_gb'].round(2).tolist(), "backgroundColor": "#1e293b"}]
            }

            # 3. Intelligence Alerts
            # Check for high average storage per study
            if 'avg_mb_per_study' in df.columns:
                high_hogs = df[df['avg_mb_per_study'] > 500].sort_values('avg_mb_per_study', ascending=False)
                for _, row in high_hogs.head(5).iterrows():
                    alerts.append({
                        "type": "critical", 
                        "msg": f"Storage Hog: {row['procedure_code']} ({round(row['avg_mb_per_study'], 1)} MB/avg)"
                    })
            
            table_data = df.to_dict(orient='records')

    return render_template(
        "report_29.html",
        report_name="Infrastructure & Storage Audit",
        run_report=run_report,
        display_start=display_start,
        display_end=display_end,
        stats=stats,
        modality_bar_json=modality_bar_json,
        proc_bar_json=proc_bar_json,
        table_data=table_data,
        alerts=alerts
    )

@report_29_bp.route("/report/29/export", methods=["POST"])
@login_required
def export_report_29():
    """
    Export function that matches the columns in the summary table exactly.
    """
    df, start, end = get_report_data(request.form)
    
    if df.empty:
        return "No data to export", 400

    csv_data = df.to_csv(index=False)
    
    filename = f"Storage_Audit_{start}_to_{end}.csv"
    
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={filename}"}
    )
