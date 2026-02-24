import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_22_bp = Blueprint("report_22", __name__)

def get_report_data(start, end):
    # Updated SQL to include concatenated names
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
            -- Concatenating name parts for consistent referring physician display
            TRIM(CONCAT_WS(' ', 
                NULLIF(s.referring_physician_first_name, ''), 
                NULLIF(s.referring_physician_mid_name, ''), 
                NULLIF(s.referring_physician_last_name, '')
            )) as referring_physician_full,
            s.signing_physician_last_name
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
        LEFT JOIN etl_patient_view p ON p.patient_db_uid::TEXT = s.patient_db_uid::TEXT
        WHERE s.study_date BETWEEN :start AND :end
    """)
    res = db.session.execute(sql, {"start": start, "end": end})
    return pd.DataFrame(res.fetchall(), columns=res.keys())

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
            # 1. Modality Comparison
            mod_a = df_a['modality'].fillna('Unknown').value_counts().to_dict()
            mod_b = df_b['modality'].fillna('Unknown').value_counts().to_dict() if not df_b.empty else {}
            
            # 2. Referring Physician Comparison (Using Full Name)
            ref_a_counts = df_a['referring_physician_full'].replace('', 'Unknown').value_counts()
            ref_a = ref_a_counts.head(10).to_dict()
            ref_b = df_b['referring_physician_full'].replace('', 'Unknown').value_counts().to_dict() if not df_b.empty else {}

            # Feature: Best Physician for current selection
            best_physician = ref_a_counts.idxmax() if not ref_a_counts.empty else "N/A"
            current_month_name = datetime.strptime(start_a, '%Y-%m-%d').strftime('%B %Y')

            # Feature: Referring Physicians per Procedure Code (Top 10 Physicians)
            # This creates a data structure for a stacked or grouped bar chart
            top_physicians = ref_a_counts.head(10).index
            proc_per_ref = df_a[df_a['referring_physician_full'].isin(top_physicians)]
            proc_chart_data = proc_per_ref.groupby(['referring_physician_full', 'procedure_code']).size().unstack(fill_value=0).to_dict('index')

            # 3. Demographics
            demo = df_a.groupby(['age_group', 'sex'], observed=False).size().unstack(fill_value=0).to_dict('index')

            data = {
                "summary": {
                    "total_a": len(df_a),
                    "total_b": len(df_b),
                    "patients": int(df_a['patient_db_uid'].nunique()),
                    "top_ae": df_a['storing_ae'].value_counts().head(5).to_dict(),
                    "best_physician": best_physician,
                    "report_month": current_month_name
                },
                "modality_comp": {"current": mod_a, "prev": mod_b},
                "physician_comp": {"current": ref_a, "prev": ref_b},
                "physician_proc_chart": proc_chart_data,
                "demo": demo
            }

    return render_template("report_22.html", data=data, run_report=run_report,
                           display_start=start_a, display_end=end_a,
                           comp_start=start_b, comp_end=end_b)

@report_22_bp.route("/report/22/export", methods=["POST"])
@login_required
def export_report_22():
    # Feature: Global export giving the raw data shown in the page
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    
    df = get_report_data(start_date, end_date)
    
    csv_data = df.to_csv(index=False)
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename=report_22_raw_data_{start_date}.csv"}
    )
