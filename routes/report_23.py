import json
import io
import csv
import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db

report_23_bp = Blueprint('report_23', __name__)

def get_report_config(form):
    base_sql_query = text("SELECT report_sql_query FROM report_template WHERE report_id = 23")
    base_sql = db.session.execute(base_sql_query).scalar()

    if base_sql:
        base_sql = base_sql.replace(
            "s.patient_db_uid = p.patient_db_uid", "s.patient_db_uid::text = p.patient_db_uid::text"
        ).replace(
            "o.patient_dbid = p.patient_db_uid", "o.patient_dbid::text = p.patient_db_uid::text"
        )

    gl_query = text("SELECT go_live_date FROM go_live_config LIMIT 1")
    db_gl = db.session.execute(gl_query).scalar()

    default_start = db_gl.strftime('%Y-%m-%d') if db_gl else "2024-01-01"
    default_end = date.today().strftime('%Y-%m-%d')

    start = form.get("start_date", default_start)
    end = form.get("end_date", default_end)

    return base_sql, start, end


@report_23_bp.route('/report/23', methods=['GET', 'POST'])
@login_required
def report_23():
    run_report = request.method == 'POST'
    base_sql, start_date, end_date = get_report_config(request.form)

    metrics = {"total_count": 0}
    chart_json = {}

    if run_report and base_sql:
        cte_base = f"WITH base_data AS ({base_sql})"

        agg_sql = text(f"""
            {cte_base}
            SELECT modality, patient_class, age_at_exam, study_date
            FROM base_data
            WHERE study_date BETWEEN :s AND :e
        """)

        demo_sql = text(f"""
            {cte_base}
            SELECT
                CASE
                    WHEN age_at_exam <= 0.083 THEN '[0-1 month]'
                    WHEN age_at_exam <= 1     THEN '[1 month - 1 year]'
                    WHEN age_at_exam <= 12    THEN '[1-12 years]'
                    WHEN age_at_exam <= 18    THEN '[13-18]'
                    WHEN age_at_exam <= 35    THEN '[19-35]'
                    WHEN age_at_exam <= 64    THEN '[36-64]'
                    ELSE '[65+]'
                END as age_bucket,
                COALESCE(proc_id, 'Unknown Proc') as description,
                sex,
                COUNT(*) as cnt
            FROM base_data
            WHERE study_date BETWEEN :s AND :e
            GROUP BY 1, 2, 3
        """)

        try:
            df_agg  = pd.DataFrame(db.session.execute(agg_sql,  {"s": start_date, "e": end_date}).fetchall())
            df_demo = pd.DataFrame(db.session.execute(demo_sql, {"s": start_date, "e": end_date}).fetchall())

            if not df_agg.empty:
                metrics["total_count"] = len(df_agg)
                df_agg['age_at_exam'] = pd.to_numeric(df_agg['age_at_exam'], errors='coerce').fillna(0)

                mod_avg   = df_agg.groupby('modality')['age_at_exam'].mean().sort_values().to_dict()
                class_vol = df_agg['patient_class'].value_counts().to_dict()

                age_order = ['[0-1 month]', '[1 month - 1 year]', '[1-12 years]',
                             '[13-18]', '[19-35]', '[36-64]', '[65+]']

                if not df_demo.empty:
                    top_5 = df_demo.groupby('description')['cnt'].sum().nlargest(5).index.tolist()
                    df_demo['display_desc'] = df_demo['description'].apply(
                        lambda x: x if x in top_5 else 'Others'
                    )
                    pivot = df_demo.pivot_table(
                        index='age_bucket', columns='display_desc',
                        values='cnt', aggfunc='sum'
                    ).fillna(0).reindex(age_order).fillna(0)

                    chart_json = {
                        "class":   {"labels": list(class_vol.keys()), "values": list(class_vol.values())},
                        "mod_age": {"labels": list(mod_avg.keys()),   "values": [round(v, 1) for v in mod_avg.values()]},
                        "gender":  {
                            "labels": df_demo.groupby('sex')['cnt'].sum().index.tolist(),
                            "values": df_demo.groupby('sex')['cnt'].sum().values.tolist()
                        },
                        "age_desc_stack": {
                            "labels": age_order,
                            "series": [
                                {"name": col, "type": "bar", "stack": "total", "data": pivot[col].tolist()}
                                for col in pivot.columns
                            ]
                        }
                    }
        except Exception as e:
            print(f"Error executing report: {e}")

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
    base_sql, start_date, end_date = get_report_config(request.form)

    if not base_sql:
        return Response("No query configured.", status=500)

    sql = text(f"""
        WITH base_data AS ({base_sql})
        SELECT
            study_date,
            modality,
            patient_class,
            sex,
            age_at_exam,
            COALESCE(proc_id, 'Unknown') as procedure_code,
            CASE
                WHEN age_at_exam <= 0.083 THEN '[0-1 month]'
                WHEN age_at_exam <= 1     THEN '[1 month - 1 year]'
                WHEN age_at_exam <= 12    THEN '[1-12 years]'
                WHEN age_at_exam <= 18    THEN '[13-18]'
                WHEN age_at_exam <= 35    THEN '[19-35]'
                WHEN age_at_exam <= 64    THEN '[36-64]'
                ELSE '[65+]'
            END as age_group
        FROM base_data
        WHERE study_date BETWEEN :s AND :e
        ORDER BY study_date DESC
    """)

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Study Date', 'Modality', 'Patient Class', 'Sex',
                         'Age at Exam', 'Procedure Code', 'Age Group'])
        yield output.getvalue()
        output.seek(0); output.truncate(0)

        with db.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                sql, {"s": start_date, "e": end_date}
            )
            for row in result:
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0); output.truncate(0)

    filename = f"population_{start_date}_to_{end_date}.csv"
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
