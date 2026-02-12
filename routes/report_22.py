import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_22_bp = Blueprint("report_22", __name__)

def get_report_data(start, end):
    # Using your EXACT Base SQL structure
    sql = text("""
        SELECT
            s.study_db_uid,
            s.procedure_code,
            s.study_date,
            s.storing_ae,        
            m.modality,          
            s.study_status,
            s.patient_db_uid,
            p.sex,
            p.age_group,
            s.last_update,
            s.patient_class,
            s.referring_physician_last_name,  
            s.signing_physician_last_name
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
        LEFT JOIN etl_patient_view p ON p.patient_db_uid::TEXT = s.patient_db_uid::TEXT
        WHERE s.study_date BETWEEN :start AND :end
    """)
    res = db.session.execute(sql, {"start": start, "end": end}).fetchall()
    return pd.DataFrame(res)

@report_22_bp.route("/report/22", methods=["GET", "POST"])
@login_required
def report_22():
    today = date.today()
    go_live = get_go_live_date() or date(2025, 1, 1)
    
    start_a = request.form.get("start_date", go_live.strftime('%Y-%m-%d'))
    end_a = request.form.get("end_date", today.strftime('%Y-%m-%d'))
    start_b = request.form.get("comp_start_date", (go_live - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_b = request.form.get("comp_end_date", go_live.strftime('%Y-%m-%d'))

    run_report = request.method == "POST"
    data = {}

    if run_report:
        df_a = get_report_data(start_a, end_a)
        df_b = get_report_data(start_b, end_b)

        if not df_a.empty:
            # 1. Modality Comparison (The "Shift")
            mod_a = df_a['modality'].fillna('Unknown').value_counts().to_dict()
            mod_b = df_b['modality'].fillna('Unknown').value_counts().to_dict() if not df_b.empty else {}
            
            # 2. Referring Physician Comparison
            ref_a = df_a['referring_physician_last_name'].fillna('None').value_counts().head(10).to_dict()
            ref_b = df_b['referring_physician_last_name'].fillna('None').value_counts().to_dict() if not df_b.empty else {}

            # 3. Demographics (Using DB age_group)
            demo = df_a.groupby(['age_group', 'sex'], observed=False).size().unstack(fill_value=0).to_dict('index')

            data = {
                "summary": {
                    "total_a": len(df_a),
                    "total_b": len(df_b),
                    "patients": int(df_a['patient_db_uid'].nunique()),
                    "top_ae": df_a['storing_ae'].value_counts().head(5).to_dict()
                },
                "modality_comp": {"current": mod_a, "prev": mod_b},
                "physician_comp": {"current": ref_a, "prev": ref_b},
                "demo": demo
            }

    return render_template("report_22.html", data=data, run_report=run_report,
                           display_start=start_a, display_end=end_a,
                           comp_start=start_b, comp_end=end_b)
@report_22_bp.route("/report/22/export", methods=["POST"])
@login_required
def export_report_22():
    report = db.session.get(ReportTemplate, 22)
    base_sql = report.report_sql_query.strip().rstrip(";")
    start_date, end_date = resolve_dates(request.form)
    sql, params = build_sql(base_sql, start_date, end_date, request.form)
    result = db.session.execute(text(sql), params)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename=census_export.csv"})
