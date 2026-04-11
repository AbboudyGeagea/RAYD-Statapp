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
    end   = form.get("end_date",   default_end)

    # Build extra WHERE clauses
    extra = []
    params = {"s": start, "e": end}

    if form.get("f_mod_active") == "on" and form.get("f_mod"):
        extra.append("UPPER(modality) = UPPER(:mod)")
        params["mod"] = form.get("f_mod")

    if form.get("f_class_active") == "on" and form.get("f_class"):
        extra.append("UPPER(patient_class) = UPPER(:p_class)")
        params["p_class"] = form.get("f_class")

    if form.get("f_sex_active") == "on" and form.get("f_sex"):
        extra.append("UPPER(sex) = UPPER(:sex)")
        params["sex"] = form.get("f_sex")

    if form.get("f_age_min"):
        extra.append("age_at_exam >= :age_min")
        params["age_min"] = float(form.get("f_age_min"))

    if form.get("f_age_max"):
        extra.append("age_at_exam <= :age_max")
        params["age_max"] = float(form.get("f_age_max"))

    extra_where = (" AND " + " AND ".join(extra)) if extra else ""

    return base_sql, start, end, params, extra_where


@report_23_bp.route('/report/23', methods=['GET', 'POST'])
@login_required
def report_23():
    run_report = request.method == 'POST'
    base_sql, start_date, end_date, params, extra_where = get_report_config(request.form)

    mod_list   = db.session.execute(text("SELECT DISTINCT modality FROM aetitle_modality_map ORDER BY 1")).fetchall()
    class_list = db.session.execute(text("SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL ORDER BY 1")).fetchall()
    sex_list   = db.session.execute(text("SELECT DISTINCT sex FROM etl_patient_view WHERE sex IS NOT NULL ORDER BY 1")).fetchall()

    filters = {
        "f_mod_active":   request.form.get("f_mod_active") == "on",
        "f_class_active": request.form.get("f_class_active") == "on",
        "f_sex_active":   request.form.get("f_sex_active") == "on",
        "mod":     request.form.get("f_mod"),
        "p_class": request.form.get("f_class"),
        "sex":     request.form.get("f_sex"),
        "age_min": request.form.get("f_age_min"),
        "age_max": request.form.get("f_age_max"),
    }

    metrics    = {"total_count": 0}
    chart_json = {}

    if run_report and base_sql:
        cte_base = f"WITH base_data AS ({base_sql})"

        agg_sql = text(f"""
            {cte_base}
            SELECT modality, patient_class, age_at_exam, study_date, patient_db_uid
            FROM base_data
            WHERE study_date BETWEEN :s AND :e{extra_where}
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
            WHERE study_date BETWEEN :s AND :e{extra_where}
            GROUP BY 1, 2, 3
        """)

        cube_sql = text(f"""
            {cte_base}
            SELECT
                modality AS mod,
                CASE
                    WHEN age_at_exam <= 0.083 THEN '[0-1 month]'
                    WHEN age_at_exam <= 1     THEN '[1 month - 1 year]'
                    WHEN age_at_exam <= 12    THEN '[1-12 years]'
                    WHEN age_at_exam <= 18    THEN '[13-18]'
                    WHEN age_at_exam <= 35    THEN '[19-35]'
                    WHEN age_at_exam <= 64    THEN '[36-64]'
                    ELSE '[65+]'
                END AS age,
                COALESCE(sex, 'U') AS sex,
                COUNT(*) AS cnt
            FROM base_data
            WHERE study_date BETWEEN :s AND :e{extra_where}
            GROUP BY 1, 2, 3
        """)

        try:
            df_agg  = pd.DataFrame(db.session.execute(agg_sql,  params).fetchall())
            df_demo = pd.DataFrame(db.session.execute(demo_sql, params).fetchall())
            df_cube = pd.DataFrame(db.session.execute(cube_sql, params).fetchall())

            if not df_agg.empty:
                metrics["total_count"] = len(df_agg)
                df_agg['age_at_exam'] = pd.to_numeric(df_agg['age_at_exam'], errors='coerce')
                age_total = df_agg['age_at_exam'].notna().sum()
                df_agg = df_agg[df_agg['age_at_exam'].isna() | df_agg['age_at_exam'].between(0, 110)]
                age_outliers_removed = int(age_total - df_agg['age_at_exam'].notna().sum())
                df_agg['age_at_exam'] = df_agg['age_at_exam'].fillna(0)
                metrics["age_outliers_removed"] = age_outliers_removed

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

                    # ── Monthly volume trend ──────────────────────────
                    df_agg['month'] = pd.to_datetime(df_agg['study_date']).dt.to_period('M').astype(str)
                    monthly = df_agg.groupby('month').size().reset_index(name='cnt').sort_values('month')
                    monthly_trend = {
                        "labels": monthly['month'].tolist(),
                        "values": monthly['cnt'].tolist(),
                    }

                    # ── Patient return rate (30 / 60 / 90 days) ───────
                    repeat_rates = {"30d": 0, "60d": 0, "90d": 0}
                    if 'patient_db_uid' in df_agg.columns:
                        df_pt = df_agg[['patient_db_uid', 'study_date']].dropna().copy()
                        df_pt['study_date'] = pd.to_datetime(df_pt['study_date'])
                        first = df_pt.groupby('patient_db_uid')['study_date'].min().rename('first_date')
                        df_pt = df_pt.join(first, on='patient_db_uid')
                        df_pt['days'] = (df_pt['study_date'] - df_pt['first_date']).dt.days
                        period_end = pd.to_datetime(params['e'])
                        if not df_pt.empty:
                            for window, key in [(30, '30d'), (60, '60d'), (90, '90d')]:
                                # Only count patients whose first visit had enough
                                # runway (N days before period end) to possibly return
                                eligible = first[first <= period_end - pd.Timedelta(days=window)]
                                eligible_pts = eligible.index.nunique()
                                if eligible_pts > 0:
                                    returned = df_pt[
                                        (df_pt['patient_db_uid'].isin(eligible.index)) &
                                        (df_pt['days'] > 0) & (df_pt['days'] <= window)
                                    ]['patient_db_uid'].nunique()
                                    repeat_rates[key] = round(returned / eligible_pts * 100, 1)

                    # ── Age × Gender × Modality cube ──────────────────
                    cube_data = []
                    if not df_cube.empty:
                        top5_mods = df_agg['modality'].value_counts().nlargest(5).index.tolist()
                        cube_data = (
                            df_cube[df_cube['mod'].isin(top5_mods)]
                            .rename(columns={"cnt": "cnt"})
                            .to_dict('records')
                        )

                    # ── Modality × Patient Class heatmap ─────────────────────
                    mod_class_heatmap = {}
                    if 'patient_class' in df_agg.columns and 'modality' in df_agg.columns:
                        hm = df_agg.dropna(subset=['modality', 'patient_class']).groupby(['modality', 'patient_class']).size().reset_index(name='cnt')
                        mods_hm = sorted(hm['modality'].unique().tolist())
                        classes_hm = sorted(hm['patient_class'].unique().tolist())
                        hm_data = [
                            [classes_hm.index(r['patient_class']), mods_hm.index(r['modality']), int(r['cnt'])]
                            for _, r in hm.iterrows()
                        ]
                        mod_class_heatmap = {
                            'mods': mods_hm, 'classes': classes_hm,
                            'data': hm_data, 'max': int(hm['cnt'].max()) if not hm.empty else 1
                        }

                    # ── Age distribution per modality ─────────────────────────
                    age_mod = {}
                    if 'modality' in df_agg.columns:
                        _age_order = ['[0-1 month]', '[1 month - 1 year]', '[1-12 years]', '[13-18]', '[19-35]', '[36-64]', '[65+]']
                        df_agg['_am_bucket'] = pd.cut(
                            df_agg['age_at_exam'],
                            bins=[-0.001, 0.083, 1, 12.999, 18, 35, 64, 999],
                            labels=_age_order
                        )
                        am_piv = df_agg.dropna(subset=['modality']).groupby(['modality', '_am_bucket']).size().unstack(fill_value=0)
                        mods_am = sorted(am_piv.index.tolist())
                        age_mod = {
                            'mods': mods_am, 'ages': _age_order,
                            'series': [
                                {'name': age, 'data': [int(am_piv.loc[m, age]) if age in am_piv.columns else 0 for m in mods_am]}
                                for age in _age_order
                            ]
                        }

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
                        },
                        "monthly_trend": monthly_trend,
                        "repeat_rates":  repeat_rates,
                        "cube_data":     cube_data,
                        "mod_class_heatmap": mod_class_heatmap,
                        "age_mod": age_mod,
                    }
        except Exception as e:
            print(f"Error executing report: {e}")

    return render_template(
        "report_23.html",
        display_start=start_date,
        display_end=end_date,
        metrics=metrics,
        chart_json=chart_json,
        run_report=run_report,
        filters=filters,
        mod_list=mod_list,
        class_list=class_list,
        sex_list=sex_list,
    )


@report_23_bp.route('/report/23/export', methods=['POST'])
@login_required
def export_report_23():
    base_sql, start_date, end_date, params, extra_where = get_report_config(request.form)

    if not base_sql:
        return Response("No query configured.", status=500)

    sql = text(f"""
        WITH base_data AS ({base_sql})
        SELECT
            study_date, modality, patient_class, sex, age_at_exam,
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
        WHERE study_date BETWEEN :s AND :e{extra_where}
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
                sql, params
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


_CONFLICT_SQL = """
    SELECT
        patient_db_uid,
        id              AS patient_id,
        fallback_id,
        birth_date::text,
        sex,
        number_of_patient_studies AS studies,
        last_update::text
    FROM etl_patient_view
    WHERE fallback_id LIKE '%$$$%'
    ORDER BY last_update DESC NULLS LAST
"""


@report_23_bp.route('/patients/conflicts')
@login_required
def conflict_count():
    try:
        count = db.session.execute(text(
            "SELECT COUNT(*) FROM etl_patient_view WHERE fallback_id LIKE '%$$$%'"
        )).scalar() or 0

        from flask import jsonify
        return jsonify({'count': int(count), 'error': None})
    except Exception as e:
        db.session.rollback()
        from flask import jsonify
        return jsonify({'count': 0, 'error': str(e)})


@report_23_bp.route('/patients/conflicts/export')
@login_required
def conflict_export():
    rows = db.session.execute(text(_CONFLICT_SQL)).mappings().fetchall()

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(['patient_db_uid', 'patient_id', 'fallback_id',
                         'birth_date', 'sex', 'studies', 'last_update'])
        buf.seek(0); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for r in rows:
            writer.writerow([r['patient_db_uid'], r['patient_id'], r['fallback_id'],
                             r['birth_date'], r['sex'], r['studies'], r['last_update']])
            buf.seek(0); yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    return Response(
        generate(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename="conflict_patients.csv"'}
    )
