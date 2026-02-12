import json
import io
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, send_file
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date
import pandas as pd

report_23_bp = Blueprint('report_23', __name__)

def resolve_dates(form):
    today = date.today()
    # Fetch go-live from DB, fallback to 30 days ago if DB is empty
    go_live = get_go_live_date() or (today - timedelta(days=30))
    
    # If user hasn't picked a date yet (first load), use go_live
    start = form.get("start_date")
    end = form.get("end_date")
    
    start_dt = datetime.strptime(start, "%Y-%m-%d").date() if start else go_live
    end_dt = datetime.strptime(end, "%Y-%m-%d").date() if end else today
    
    return start_dt, end_dt

@report_23_bp.route('/report/23', methods=['GET', 'POST'])
@login_required
def report_23():
    run_report = request.method == 'POST'
    start_date, end_date = resolve_dates(request.form)
    
    metrics = {"conflicted_ids": 0, "total_count": 0}
    chart_json = {}

    if run_report:
        # 1. Conflicted ID Count
        conf_q = text("SELECT COUNT(*) FROM etl_patient_view WHERE fallback_id LIKE '%$$$%'")
        metrics["conflicted_ids"] = db.session.execute(conf_q).scalar() or 0

        # 2. Main Population Query
        query = text("""
            SELECT s.patient_db_uid::text as patient_id, 
                   p.sex, 
                   p.age_group, 
                   p.fallback_id
            FROM etl_didb_studies s
            JOIN etl_patient_view p ON s.patient_db_uid = p.patient_db_uid
            WHERE s.study_date BETWEEN :s AND :e
        """)
        
        res = db.session.execute(query, {"s": start_date, "e": end_date}).fetchall()
        df = pd.DataFrame(res)

        if not df.empty:
            metrics["total_count"] = len(df)
            
            # Gender Distribution
            gender_data = df['sex'].value_counts().to_dict()
            # Age Group Distribution
            age_data = df['age_group'].value_counts().to_dict()
            
            # Top repetitive patients (High Utilizers)
            top_q = text("""
                SELECT fallback_id, COUNT(*) as number_of_patient_studies
                FROM etl_didb_studies s
                JOIN etl_patient_view p ON s.patient_db_uid = p.patient_db_uid
                WHERE s.study_date BETWEEN :s AND :e
                GROUP BY fallback_id
                ORDER BY number_of_patient_studies DESC
                LIMIT 10
            """)
            top_df = pd.DataFrame(db.session.execute(top_q, {"s": start_date, "e": end_date}).fetchall())

            chart_json = {
                "gender": {"labels": list(gender_data.keys()), "values": list(gender_data.values())},
                "age": {"labels": list(age_data.keys()), "values": list(age_data.values())},
                "repetitive": {
                    "labels": top_df['fallback_id'].tolist() if not top_df.empty else [],
                    "values": top_df['number_of_patient_studies'].tolist() if not top_df.empty else []
                }
            }

    return render_template(
        "report_23.html",
        report_name="Patient Population Intelligence",
        display_start=start_date.strftime('%Y-%m-%d'),
        display_end=end_date.strftime('%Y-%m-%d'),
        metrics=metrics,
        chart_json=chart_json,
        run_report=run_report
    )

@report_23_bp.route('/report/23/export', methods=['POST'])
@login_required
def export_report_23():
    start_date, end_date = resolve_dates(request.form)
    query = text("SELECT * FROM etl_didb_studies WHERE study_date BETWEEN :s AND :e")
    df = pd.read_sql_query(query, db.engine, params={"s": start_date, "e": end_date})
    
    output = io.BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    return send_file(
        output,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"population_report_{start_date}.csv"
    )
