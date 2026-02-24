import json
from datetime import date, datetime
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db
import pandas as pd

report_23_bp = Blueprint('report_23', __name__)

def get_report_config(form):
    # Fetch base SQL from template
    base_sql_query = text("SELECT report_sql_query FROM report_template WHERE report_id = 23")
    base_sql = db.session.execute(base_sql_query).scalar()
    
    # Apply type casting to the base_sql to prevent the "text = bigint" error
    # We force both sides of the join to text to ensure compatibility
    if base_sql:
        base_sql = base_sql.replace(
            "o.patient_dbid = p.patient_db_uid", 
            "o.patient_dbid::text = p.patient_db_uid::text"
        ).replace(
            "s.patient_db_uid = p.patient_db_uid",
            "s.patient_db_uid::text = p.patient_db_uid::text"
        )

    # Fetch Go-Live for default start date
    gl_query = text("SELECT go_live_date FROM go_live_config LIMIT 1")
    db_gl = db.session.execute(gl_query).scalar()
    
    default_start = db_gl.strftime('%Y-%m-%d') if db_gl else "2024-01-01"
    default_end = date.today().strftime('%Y-%m-%d')
    
    start = form.get("start_date") if form.get("start_date") else default_start
    end = form.get("end_date") if form.get("end_date") else default_end
    
    return base_sql, start, end

@report_23_bp.route('/report/23', methods=['GET', 'POST'])
@login_required
def report_23():
    run_report = request.method == 'POST'
    base_sql, start_date, end_date = get_report_config(request.form)
    
    metrics = {"total_count": 0, "conflicted_ids": 0}
    chart_json = {}

    if run_report and base_sql:
        # Wrap the corrected base query as a CTE
        cte_base = f"WITH base_data AS ({base_sql})"
        
        # Aggregated Metrics & Charts
        # 1. Modality & Patient Class (Using safe casting for AVG age)
        agg_sql = text(f"""
            {cte_base}
            SELECT 
                COALESCE(modality, 'Unknown') as modality,
                COALESCE(patient_class, 'Other') as patient_class,
                AVG(EXTRACT(YEAR FROM age(study_date, birth_date)))::numeric(10,1) as avg_age,
                COUNT(*) as study_count
            FROM base_data
            WHERE study_date BETWEEN :s AND :e
            GROUP BY 1, 2
        """)
        
        # 2. Demographics
        demo_sql = text(f"""
            {cte_base}
            SELECT sex, age_group, COUNT(*) as cnt
            FROM base_data
            WHERE study_date BETWEEN :s AND :e
            GROUP BY sex, age_group
        """)

        # 3. High Utilizers
        rep_sql = text(f"""
            {cte_base}
            SELECT fallback_id, COUNT(*) as cnt
            FROM base_data
            WHERE study_date BETWEEN :s AND :e
            GROUP BY fallback_id
            ORDER BY cnt DESC LIMIT 10
        """)

        try:
            df_agg = pd.DataFrame(db.session.execute(agg_sql, {"s": start_date, "e": end_date}).fetchall())
            df_demo = pd.DataFrame(db.session.execute(demo_sql, {"s": start_date, "e": end_date}).fetchall())
            df_rep = pd.DataFrame(db.session.execute(rep_sql, {"s": start_date, "e": end_date}).fetchall())

            if not df_agg.empty:
                metrics["total_count"] = int(df_agg['study_count'].sum())
                
                # ECharts Data Prep
                mod_avg = df_agg.groupby('modality')['avg_age'].mean().sort_values().to_dict()
                class_vol = df_agg.groupby('patient_class')['study_count'].sum().sort_values(ascending=False).to_dict()
                
                chart_json = {
                    "class": {"labels": list(class_vol.keys()), "values": list(class_vol.values())},
                    "mod_age": {"labels": list(mod_avg.keys()), "values": list(mod_avg.values())},
                    "gender": {"labels": df_demo.groupby('sex')['cnt'].sum().index.tolist() if not df_demo.empty else [], 
                               "values": df_demo.groupby('sex')['cnt'].sum().values.tolist() if not df_demo.empty else []},
                    "age_dist": {"labels": df_demo.groupby('age_group')['cnt'].sum().index.tolist() if not df_demo.empty else [], 
                                 "values": df_demo.groupby('age_group')['cnt'].sum().values.tolist() if not df_demo.empty else []},
                    "repetitive": {"labels": df_rep['fallback_id'].tolist() if not df_rep.empty else [], 
                                   "values": df_rep['cnt'].tolist() if not df_rep.empty else []}
                }
        except Exception as e:
            print(f"Database Error: {e}")

    return render_template(
        "report_23.html",
        display_start=start_date,
        display_end=end_date,
        metrics=metrics,
        chart_json=chart_json,
        run_report=run_report
    )
@report_23_bp.route('/report/23/export', methods=['POST'])
@login_required
def export_report_23():
    start_date, end_date = resolve_dates(request.form)
    query = text("""
        SELECT s.*, p.sex, s.age_at_exam, p.age_group, p.fallback_id, m.modality
        FROM etl_didb_studies s 
        JOIN etl_patient_view p ON s.patient_db_uid = p.patient_db_uid
        LEFT JOIN aetitle_modality_map m ON s.src_aet = m.aetitle
        WHERE s.study_date BETWEEN :s AND :e
    """)
    df = pd.read_sql_query(query, db.engine, params={"s": start_date, "e": end_date})
    output = io.BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)
    return send_file(output, mimetype="text/csv", as_attachment=True, download_name=f"population_data.csv")

