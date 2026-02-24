import json
import io
import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response, send_file
from flask_login import login_required
import json
import io
import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response, send_file
from flask_login import login_required
from sqlalchemy import text
from db import db

report_25_bp = Blueprint("report_25", __name__)

def format_duration(minutes):
    """Converts raw minutes into a human-readable hospital format."""
    if pd.isna(minutes) or minutes <= 0: return "0m"
    total_mins = int(float(minutes))
    days = total_mins // 1440
    hours = (total_mins % 1440) // 60
    mins = total_mins % 60
    parts = []
    if days > 0: parts.append(f"{days}d")
    if hours > 0: parts.append(f"{hours}h")
    if mins > 0 or not parts: parts.append(f"{mins}m")
    return " ".join(parts)

def get_gold_standard_data(form_data):
    # 1. Setup Dates
    start = form_data.get("start_date") or (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")

    # 2. Fetch Base SQL from Template
    template_res = db.session.execute(
        text("SELECT report_sql_query FROM report_template WHERE report_id = 25")
    ).fetchone()
    
    if not template_res: return None, start, end
    
    # 3. Execute Query
    query = db.session.execute(text(template_res[0]), {"start": start, "end": end}).mappings().all()
    df = pd.DataFrame(query)
    if df.empty: return None, start, end

    days_count = (datetime.strptime(end, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days + 1

    # --- MACHINE UTILIZATION ---
    usage_data = []
    for ae, ae_df in df.groupby('aetitle'):
        total_work_mins = ae_df['proc_duration'].sum()
        # Default to 24h capacity if not specified in DB
        shift_hours = ae_df['capacity_hours'].iloc[0] if 'capacity_hours' in ae_df.columns else 24
        total_capacity_mins = shift_hours * 60 * days_count
        util_pct = round((total_work_mins / total_capacity_mins) * 100, 1) if total_capacity_mins > 0 else 0
        
        worst_case = ae_df.loc[ae_df['total_tat_min'].idxmax()]
        usage_data.append({
            "aetitle": ae,
            "modality": ae_df['modality'].iloc[0] or "N/A",
            "exam_count": len(ae_df),
            "total_work_time": format_duration(total_work_mins),
            "util_pct": util_pct,
            "util_color": "rose-500" if util_pct > 85 else "emerald-400" if util_pct > 40 else "sky-400",
            "worst_case_id": worst_case.get('patient_db_uid', 'N/A'),
            "worst_proc": worst_case.get('procedure_code', 'N/A'),
            "worst_tat": int(worst_case['total_tat_min'])
        })

    # --- RADIOLOGIST PERFORMANCE ---
    rad_cards = []
    rad_subset = df.dropna(subset=['reading_radiologist', 'total_tat_min'])
    for rad_name, r_df in rad_subset.groupby('reading_radiologist'):
        mods = []
        for m_name, m_df in r_df.groupby('modality'):
            procs = []
            for p_code, p_df in m_df.groupby('procedure_code'):
                procs.append({"code": p_code, "tat": f"{p_df['total_tat_min'].mean():.1f}m"})
            mods.append({
                "name": m_name,
                "avg_tat": round(m_df['total_tat_min'].mean(), 1),
                "count": len(m_df),
                "procedures": procs
            })
        rad_cards.append({
            "name": rad_name,
            "overall_avg": round(r_df['total_tat_min'].mean(), 1),
            "modalities": mods
        })

    # --- PATIENT CLASS TAT (New Chart) ---
    class_tat = {"labels": [], "values": []}
    if 'patient_class' in df.columns:
        class_group = df.groupby('patient_class')['total_tat_min'].mean().sort_values(ascending=False)
        class_tat = {
            "labels": class_group.index.tolist(),
            "values": [round(v, 1) for v in class_group.values.tolist()]
        }

    # --- SUMMARY ---
    summary = {
        "total_exams": len(df),
        "active_time": format_duration(df['proc_duration'].sum()),
        "avg_util": f"{pd.DataFrame(usage_data)['util_pct'].mean():.1f}%" if usage_data else "0%"
    }

    return {
        "summary": summary,
        "rad_cards": rad_cards,
        "usage_table": usage_data,
        "class_tat": class_tat,
        "raw_df": df
    }, start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    data, start, end = get_gold_standard_data(request.form)
    return render_template("report_25.html", data=data, display_start=start, display_end=end, run_report=bool(data))

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    data, _, _ = get_gold_standard_data(request.form)
    etype = request.form.get("type", "raw")
    if not data: return "No data", 400

    if etype == "usage":
        df_out = pd.DataFrame(data['usage_table']).drop(columns=['util_color'])
    elif etype == "rads":
        rows = []
        for r in data['rad_cards']:
            for m in r['modalities']:
                rows.append({"Radiologist": r['name'], "Modality": m['name'], "Avg_TAT": m['avg_tat'], "Exams": m['count']})
        df_out = pd.DataFrame(rows)
    else:
        df_out = data['raw_df']

    output = io.BytesIO()
    df_out.to_csv(output, index=False)
    output.seek(0)
    return send_file(output, mimetype="text/csv", as_attachment=True, download_name=f"EFFICIENCY_{etype}_{date.today()}.csv")


