import io
import csv
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_22_bp = Blueprint("report_22", __name__)

def get_where_params(form):
    start_date = form.get("start_date")
    end_date = form.get("end_date")
    where = "WHERE study_date BETWEEN :start AND :end"
    params = {"start": start_date, "end": end_date}
    
    if form.get("f_class_active") == "on" and form.get("f_class"):
        where += " AND patient_class = :p_class"
        params["p_class"] = form.get("f_class")
    
    if form.get("f_sex_active") == "on" and form.get("f_sex"):
        where += " AND sex = :sex"
        params["sex"] = form.get("f_sex")
        
    if form.get("f_status_active") == "on" and form.get("f_status"):
        where += " AND study_status = :status"
        params["status"] = form.get("f_status")
        
    if form.get("f_mod_active") == "on" and form.get("f_mod"):
        where += " AND modality = :mod"
        params["mod"] = form.get("f_mod")
        
    if form.get("f_ae_active") == "on" and form.get("f_ae"):
        where += " AND storing_ae = :ae"
        params["ae"] = form.get("f_ae")
        
    return where, params

@report_22_bp.route("/report/22", methods=["GET", "POST"])
@login_required
def report_22():
    go_live = get_go_live_date() or date(2025, 1, 1)
    today = date.today()
    start_date = request.form.get("start_date", go_live.strftime('%Y-%m-%d'))
    end_date = request.form.get("end_date", today.strftime('%Y-%m-%d'))

    status_list = db.session.execute(text("SELECT DISTINCT study_status FROM etl_didb_studies WHERE study_status IS NOT NULL")).fetchall()
    mod_list = db.session.execute(text("SELECT DISTINCT modality FROM aetitle_modality_map")).fetchall()
    ae_list = db.session.execute(text("SELECT DISTINCT storing_ae FROM etl_didb_studies WHERE storing_ae IS NOT NULL")).fetchall()

    filters = {
        "f_class_active": request.form.get("f_class_active") == "on",
        "f_sex_active": request.form.get("f_sex_active") == "on",
        "f_status_active": request.form.get("f_status_active") == "on",
        "f_mod_active": request.form.get("f_mod_active") == "on",
        "f_ae_active": request.form.get("f_ae_active") == "on",
        "p_class": request.form.get("f_class"),
        "sex": request.form.get("f_sex"),
        "status": request.form.get("f_status"),
        "mod": request.form.get("f_mod"),
        "ae": request.form.get("f_ae")
    }

    run_report = request.method == "POST"
    data = {}

    if run_report:
        where, params = get_where_params(request.form)

        base_sql = """
            SELECT
                s.study_db_uid, s.procedure_code, s.study_date, s.storing_ae, s.study_description,
                m.modality, s.study_status, s.patient_db_uid, p.sex, p.age_group,
                s.patient_class,
                s.age_at_exam,
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ', s.referring_physician_first_name, s.referring_physician_last_name)), ''), 'Unknown') as physician,
                s.patient_location, p.fallback_id as patient_id
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
            LEFT JOIN etl_patient_view p ON p.patient_db_uid::TEXT = s.patient_db_uid::TEXT
        """
        
        cte = f"WITH base_data AS ({base_sql})"

        # 1. Base Stats
        res_status = db.session.execute(text(f"{cte} SELECT COALESCE(study_status, 'N/A'), COUNT(*) FROM base_data {where} GROUP BY 1"), params).fetchall()
        
        # 2. Top Physicians (Study Count)
        res_phys = db.session.execute(text(f"{cte} SELECT physician, COUNT(*) FROM base_data {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"), params).fetchall()

        # 2b. Top Physicians (UNIQUE Patient Count)
        res_phys_unique = db.session.execute(text(f"{cte} SELECT physician, COUNT(DISTINCT patient_id) FROM base_data {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"), params).fetchall()
        
        # 2c. NEW: PHYSICIAN CHURN/TREND LOGIC
        # This looks at the 60-day window to compare Last Month vs This Month
        res_phys_trend = db.session.execute(text(f"""
            {cte},
            monthly_agg AS (
                SELECT 
                    physician,
                    DATE_TRUNC('month', study_date) as month,
                    COUNT(*) as vol
                FROM base_data
                WHERE study_date >= CURRENT_DATE - INTERVAL '60 days'
                GROUP BY 1, 2
            ),
            comparison AS (
                SELECT 
                    physician,
                    vol as current_vol,
                    LAG(vol) OVER (PARTITION BY physician ORDER BY month) as prev_vol
                FROM monthly_agg
            )
            SELECT 
                physician, 
                current_vol, 
                prev_vol,
                ROUND(((current_vol - prev_vol)::numeric / NULLIF(prev_vol, 0)) * 100, 1) as pct_change
            FROM comparison
            WHERE prev_vol IS NOT NULL AND current_vol < prev_vol
            ORDER BY pct_change ASC LIMIT 10
        """)).fetchall()

        # 2d. Physician → modality breakdown (top 10 physicians, their modality split)
        res_phys_mod = db.session.execute(text(f"""
            {cte}
            SELECT physician, COALESCE(modality, 'UNMAPPED') as mod, COUNT(*) as cnt
            FROM base_data {where}
            AND physician != 'Unknown'
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """), params).fetchall()

        # 2e. Average age per physician (top 10 by volume, exclude Unknown, outliers removed)
        res_phys_age = db.session.execute(text(f"""
            {cte}
            SELECT physician, ROUND(AVG(age_at_exam)::numeric, 1) as avg_age, COUNT(*) as cnt
            FROM base_data {where}
            AND physician != 'Unknown'
            AND age_at_exam BETWEEN 0 AND 110
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY cnt DESC LIMIT 15
        """), params).fetchall()

        # Consolidated: both age outlier counts in a single query using FILTER
        age_phys_counts = db.session.execute(text(f"""
            {cte}
            SELECT
                COUNT(*) FILTER (WHERE physician != 'Unknown' AND age_at_exam IS NOT NULL)   AS total,
                COUNT(*) FILTER (WHERE physician != 'Unknown' AND age_at_exam BETWEEN 0 AND 110) AS clean
            FROM base_data {where}
        """), params).fetchone()
        age_phys_outliers = int(age_phys_counts[0] or 0) - int(age_phys_counts[1] or 0)

        # 2f. Average age per procedure code (top 20 by volume, outliers removed)
        res_proc_age = db.session.execute(text(f"""
            {cte}
            SELECT procedure_code, ROUND(AVG(age_at_exam)::numeric, 1) as avg_age, COUNT(*) as cnt
            FROM base_data {where}
            AND procedure_code IS NOT NULL
            AND age_at_exam BETWEEN 0 AND 110
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY cnt DESC LIMIT 20
        """), params).fetchall()

        # Consolidated: proc age outlier counts in a single query
        age_proc_counts = db.session.execute(text(f"""
            {cte}
            SELECT
                COUNT(*) FILTER (WHERE procedure_code IS NOT NULL AND age_at_exam IS NOT NULL)          AS total,
                COUNT(*) FILTER (WHERE procedure_code IS NOT NULL AND age_at_exam BETWEEN 0 AND 110)    AS clean
            FROM base_data {where}
        """), params).fetchone()
        age_proc_outliers = int(age_proc_counts[0] or 0) - int(age_proc_counts[1] or 0)

        # 2g. Study status breakdown per top physician
        res_phys_status = db.session.execute(text(f"""
            {cte}
            SELECT physician, COALESCE(study_status, 'Unknown') as status, COUNT(*) as cnt
            FROM base_data {where}
            AND physician != 'Unknown'
            GROUP BY 1, 2
        """), params).fetchall()

        # 3. Demographics
        res_demo = db.session.execute(text(f"{cte} SELECT COALESCE(age_group, 'Unknown'), COALESCE(sex, 'U'), COUNT(*) FROM base_data {where} GROUP BY 1, 2"), params).fetchall()
        
        gender_counts = {"M": 0, "F": 0, "U": 0}
        age_map = {}
        for age, sex, count in res_demo:
            if sex in gender_counts: gender_counts[sex] += count
            age_map[age] = age_map.get(age, 0) + count

        # 4. Tree Flow Logic
        res_flow = db.session.execute(text(f"{cte} SELECT COALESCE(modality, 'UNMAPPED'), COALESCE(storing_ae, 'Unknown AE'), COALESCE(study_description, 'No Description'), COUNT(*) FROM base_data {where} GROUP BY 1, 2, 3"), params).fetchall()

        total_vol = 0
        mod_map = {}
        for mod, ae, desc, count in res_flow:
            total_vol += count
            if mod not in mod_map: mod_map[mod] = {"count": 0, "aes": {}}
            if ae not in mod_map[mod]["aes"]: mod_map[mod]["aes"][ae] = {"count": 0, "procs": {}}
            mod_map[mod]["aes"][ae]["procs"][desc] = mod_map[mod]["aes"][ae]["procs"].get(desc, 0) + count
            mod_map[mod]["aes"][ae]["count"] += count
            mod_map[mod]["count"] += count

        final_tree = {"name": f"TOTAL\n{total_vol}", "children": []}
        for m_name, m_data in mod_map.items():
            m_node = {"name": f"{m_name}\n{m_data['count']}", "children": []}
            for ae_name, ae_data in m_data["aes"].items():
                ae_node = {"name": f"{ae_name}\n({ae_data['count']})", "children": []}
                top_procs = sorted(ae_data["procs"].items(), key=lambda x: x[1], reverse=True)[:5]
                for p_name, p_count in top_procs:
                    ae_node["children"].append({"name": f"{p_name}: {p_count}"})
                m_node["children"].append(ae_node)
            final_tree["children"].append(m_node)

        # Build physician → modality stacked bar data
        phys_mod_map = {}
        all_mods = set()
        for phys, mod, cnt in res_phys_mod:
            phys_mod_map.setdefault(phys, {})[mod] = cnt
            all_mods.add(mod)
        # Keep top 10 physicians by total volume
        top10_phys = sorted(phys_mod_map, key=lambda p: sum(phys_mod_map[p].values()), reverse=True)[:10]
        all_mods = sorted(all_mods)

        phys_status_map = {}
        all_statuses_set = set()
        for phys, status, cnt in res_phys_status:
            phys_status_map.setdefault(phys, {})[status] = cnt
            all_statuses_set.add(status)
        top10_phys_st = sorted(phys_status_map, key=lambda p: sum(phys_status_map[p].values()), reverse=True)[:10]
        all_statuses_list = sorted(all_statuses_set)

        data = {
            "stat_c": {r[0]: r[1] for r in res_status},
            "phys_c": {r[0]: r[1] for r in res_phys},
            "phys_unique": {r[0]: r[1] for r in res_phys_unique},
            "phys_churn": [{"name": r[0], "cur": r[1], "prev": r[2], "change": r[3]} for r in res_phys_trend],
            "tree_data": [final_tree],
            "gender_data": [{"name": k, "value": v} for k, v in gender_counts.items() if v > 0],
            "age_labels": sorted(age_map.keys()),
            "age_values": [age_map[a] for a in sorted(age_map.keys())],
            "phys_mod": {
                "physicians": top10_phys,
                "modalities": all_mods,
                "series": [
                    {"name": mod, "data": [phys_mod_map.get(p, {}).get(mod, 0) for p in top10_phys]}
                    for mod in all_mods
                ]
            },
            "phys_age": [{"name": r[0], "avg_age": float(r[1]) if r[1] else 0} for r in res_phys_age],
            "proc_age": [{"code": r[0], "avg_age": float(r[1]) if r[1] else 0, "cnt": r[2]} for r in res_proc_age],
            "age_phys_outliers": age_phys_outliers,
            "age_proc_outliers": age_proc_outliers,
            "phys_status": {
                "physicians": top10_phys_st,
                "statuses": all_statuses_list,
                "series": [
                    {"name": s, "data": [phys_status_map.get(p, {}).get(s, 0) for p in top10_phys_st]}
                    for s in all_statuses_list
                ]
            },
        }

    return render_template("report_22.html", data=data, filters=filters, run_report=run_report, display_start=start_date, display_end=end_date, status_list=status_list, mod_list=mod_list, ae_list=ae_list)



@report_22_bp.route("/report/22/export", methods=["POST"])
@login_required
def export_report_22():
    where, params = get_where_params(request.form)
    sql = text(f"""
        WITH base_data AS (
            SELECT s.study_date, s.patient_class, m.modality, p.sex, s.study_status, s.patient_location,
            TRIM(CONCAT_WS(' ', s.referring_physician_first_name, s.referring_physician_last_name)) as physician,
            p.fallback_id as patient_id
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
            LEFT JOIN etl_patient_view p ON p.patient_db_uid::TEXT = s.patient_db_uid::TEXT
        )
        SELECT study_date, COALESCE(patient_class, 'N/A'), COALESCE(modality, 'N/A'), COALESCE(sex, 'U'), 
               COALESCE(study_status, 'N/A'), COALESCE(patient_location, 'N/A'), COALESCE(physician, 'Unknown'),
               patient_id
        FROM base_data {where} ORDER BY study_date DESC
    """)
    
    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Date', 'Class', 'Modality', 'Sex', 'Status', 'Location', 'Physician', 'PatientID'])
        yield output.getvalue()
        output.seek(0); output.truncate(0)
        with db.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(sql, params)
            for row in result:
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0); output.truncate(0)
    
    return Response(generate(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=raw_data.csv"})
