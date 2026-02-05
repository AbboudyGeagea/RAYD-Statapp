import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, Response, redirect, url_for
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_27_bp = Blueprint("report_27", __name__)

def calculate_clinical_stage(birth_date):
    """Data Science approach: Classifying by life stage instead of raw numbers."""
    if not birth_date:
        return 'Unknown'
    try:
        # birth_date comes from Postgres as a date object usually
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        if age < 2: return 'Infant'
        if age < 13: return 'Pediatric'
        if age < 18: return 'Adolescent'
        if age < 65: return 'Adult'
        return 'Geriatric'
    except Exception:
        return 'Unknown'

def get_report_data(inputs):
    go_live = get_go_live_date()
    # Handle the empty string syntax error for Postgres
    start = inputs.get('start_date') if inputs.get('start_date') else str(go_live)
    end = inputs.get('end_date') if inputs.get('end_date') else str(date.today())

    query = text("""
        SELECT
            o.order_dbid,
            o.order_status,
            o.proc_id,
            o.scheduled_datetime,
            s.study_date,
            s.storing_ae,
            p.birth_date,
            p.sex
        FROM etl_orders o
        LEFT JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
        LEFT JOIN etl_patient_view p ON p.patient_db_uid = o.patient_dbid
        WHERE o.scheduled_datetime::date >= :start 
          AND o.scheduled_datetime::date <= :end
        ORDER BY o.scheduled_datetime DESC
    """)
    
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start": start, "end": end})
    
    return df, start, end

@report_27_bp.route("/report/27", methods=["GET", "POST"])
@login_required
def report_27():
    if request.method == "POST" and request.form.get("action") == "reset":
        return redirect(url_for('report_27.report_27'))

    inputs = request.form if request.method == "POST" else request.args
    df, start, end = get_report_data(inputs)

    chart_json = {}
    static_stats = {"total": 0, "fulfillment_rate": 0, "top_ae": "N/A"}

    if not df.empty:
        # Feature Engineering: Apply Clinical Stage
        df['clinical_stage'] = df['birth_date'].apply(calculate_clinical_stage)
        
        # Calculate Stats
        total = len(df)
        fulfilled = df['study_date'].notnull().sum()
        
        static_stats = {
            "total": total,
            "fulfilled": int(fulfilled),
            "fulfillment_rate": round((fulfilled / total * 100), 2) if total > 0 else 0,
            "top_ae": df['storing_ae'].mode()[0] if not df['storing_ae'].dropna().empty else "N/A"
        }

        # Visualization 1: Order Status Distribution
        status_counts = df['order_status'].value_counts()
        chart_json['status_pie'] = {
            "labels": status_counts.index.tolist(),
            "datasets": [{"data": status_counts.tolist(), "backgroundColor": ["#38ada9", "#f3a683", "#e55039"]}]
        }

        # Visualization 2: Fulfillment by Clinical Stage
        # (Percent of orders that resulted in a study, per age group)
        stage_group = df.groupby('clinical_stage').apply(
            lambda x: (x['study_date'].notnull().sum() / len(x)) * 100
        ).round(1)
        
        chart_json['fulfillment_by_stage'] = {
            "labels": stage_group.index.tolist(),
            "datasets": [{"label": "Fulfillment %", "data": stage_group.tolist(), "backgroundColor": "#60a3bc"}]
        }

    results = df.to_dict(orient='records')

    return render_template(
        "report_27.html",
        results=results,
        start_date=start,
        end_date=end,
        static_stats=static_stats,
        chart_json=chart_json,
        run_report=True if not df.empty else False
    )

@report_27_bp.route("/report/27/export", methods=["POST"])
@login_required
def export_report_27():
    df, _, _ = get_report_data(request.form)
    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename=Order_Fulfillment_{date.today()}.csv"}
    )
