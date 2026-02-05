import json
from datetime import date
from flask import Blueprint, render_template, request, send_file
from sqlalchemy import text
from db import db
import pandas as pd
import io

report_23_bp = Blueprint('report_23', __name__)

@report_23_bp.route('/report/23', methods=['GET', 'POST'])
def report_23():
    # 1. Defaults & Initialization
    go_live = "2000-01-01"
    rows = []
    top_patients_rows = []
    total_count = 0
    chart_json = json.dumps({"labels": [], "datasets": []}) 
    
    start_date = request.form.get("start_date") or go_live
    end_date = request.form.get("end_date") or str(date.today())
    run_report = request.method == 'POST'

    if run_report:
        try:
            # A. Main Data Log (Recent 20)
            main_q = text("""
                SELECT study_date, patient_db_uid as fallback_id, modality, study_description 
                FROM etl_didb_studies 
                WHERE study_date BETWEEN :start AND :end
                ORDER BY study_date DESC LIMIT 20
            """)
            result_main = db.session.execute(main_q, {"start": start_date, "end": end_date}).fetchall()
            
            for r in result_main:
                item = dict(r._mapping)
                # Convert Date objects to strings for the HTML table
                item['study_date'] = str(item['study_date'])
                rows.append(item)

            # B. Top 10 Patients Query
            top_q = text("""
                SELECT patient_db_uid as fallback_id, 
                       COUNT(*) as number_of_patient_studies, 
                       MIN(study_date) as first_study, 
                       MAX(study_date) as last_study
                FROM etl_didb_studies
                WHERE study_date BETWEEN :start AND :end
                GROUP BY patient_db_uid
                ORDER BY number_of_patient_studies DESC 
                LIMIT 10
            """)
            result_top = db.session.execute(top_q, {"start": start_date, "end": end_date}).fetchall()
            
            chart_labels = []
            chart_values = []

            for row in result_top:
                d = dict(row._mapping)
                # Ensure all values are JSON-serializable
                d['first_study'] = str(d['first_study']) if d['first_study'] else ""
                d['last_study'] = str(d['last_study']) if d['last_study'] else ""
                d['fallback_id'] = str(d['fallback_id'])
                d['number_of_patient_studies'] = int(d['number_of_patient_studies'])
                
                top_patients_rows.append(d)
                chart_labels.append(d['fallback_id'])
                chart_values.append(d['number_of_patient_studies'])

            # C. Serialize Chart Data
            chart_data = {
                "labels": chart_labels,
                "datasets": [{
                    "label": "Studies per Patient",
                    "data": chart_values,
                    "backgroundColor": "#38ada9",
                    "borderRadius": 6,
                    "borderWidth": 0
                }]
            }
            chart_json = json.dumps(chart_data)

            # D. Total Count for the Metrics Card
            count_q = text("SELECT COUNT(*) FROM etl_didb_studies WHERE study_date BETWEEN :s AND :e")
            total_count = db.session.execute(count_q, {"s": start_date, "e": end_date}).scalar() or 0

        except Exception as e:
            print(f"REPORT_23 DATABASE ERROR: {e}")

    return render_template(
        "report_23.html", 
        report_name="Patient Overview",
        display_start=start_date, 
        display_end=end_date, 
        rows=rows,
        top_patients_rows=top_patients_rows, 
        total_count=total_count, 
        run_report=run_report,
        chart_json=chart_json, 
        go_live=go_live
    )

@report_23_bp.route('/report/23/export', methods=['POST'])
def export_report_23():
    start_date = request.form.get("start_date") or "2000-01-01"
    end_date = request.form.get("end_date") or str(date.today())
    query = text("SELECT * FROM etl_didb_studies WHERE study_date BETWEEN :s AND :e")
    df = pd.read_sql_query(query, db.engine, params={"s": start_date, "e": end_date})
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Export')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"Patient_Overview_{date.today()}.xlsx")
