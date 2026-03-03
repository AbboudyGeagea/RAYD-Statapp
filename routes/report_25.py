import json
import pandas as pd
import io
from datetime import date
from flask import Blueprint, render_template, request, send_file, url_for
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date

report_25_bp = Blueprint("report_25", __name__)

def get_gold_standard_data(form_data):
    # 1. Handle Dates and Filters
    go_live = get_etl_cutoff_date() 
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    
    use_class = form_data.get("class_enabled") == "on"
    selected_classes = form_data.getlist("patient_class") if use_class else []
    use_loc = form_data.get("loc_enabled") == "on"
    selected_locs = form_data.getlist("patient_location") if use_loc else []

    params = {"start": start, "end": end}
    where_clauses = ["study_date BETWEEN :start AND :end"]
    
    if use_class and selected_classes:
        where_clauses.append("patient_class IN :classes")
        params["classes"] = tuple(selected_classes)
    if use_loc and selected_locs:
        where_clauses.append("patient_location IN :locs")
        params["locs"] = tuple(selected_locs)

    # 2. Fetch SQL Template
    template_res = db.session.execute(text("SELECT report_sql_query FROM report_template WHERE report_id = 25")).fetchone()
    if not template_res: 
        return None, start, end
    
    # 3. Execute Query
    sql_exec = f"SELECT * FROM ({template_res[0]}) as sub WHERE {' AND '.join(where_clauses)}"
    df = pd.DataFrame(db.session.execute(text(sql_exec), params).mappings().all())
    
    if df.empty: 
        return None, start, end

    # 4. Defensive Data Cleaning (Prevents KeyErrors)
    df['total_tat_min'] = pd.to_numeric(df.get('total_tat_min', 0), errors='coerce').fillna(0)
    df['proc_duration'] = pd.to_numeric(df.get('proc_duration', 0), errors='coerce').fillna(0)
    
    if 'study_date' in df.columns:
        df['study_date_dt'] = pd.to_datetime(df['study_date'], errors='coerce')
    else:
        df['study_date_dt'] = pd.to_datetime(date.today())

    # --- Metrics Generation ---
    # Chart 1: TAT per Class
    class_tat = df.groupby('patient_class')['total_tat_min'].mean().round(1).to_dict() if 'patient_class' in df.columns else {}

    # Chart 2 & 6: Heatmap & Global Util
    matrix_rows = []
    if 'aetitle' in df.columns:
        weekday_counts = pd.date_range(start, end).dayofweek.value_counts().to_dict()
        sched_q = db.session.execute(text("SELECT UPPER(TRIM(aetitle)) as ae, day_of_week, std_opening_minutes FROM device_weekly_schedule")).mappings().all()
        schedule_lookup = {(s['ae'], int(s['day_of_week'])): s['std_opening_minutes'] for s in sched_q}
        for ae in sorted(df['aetitle'].unique()):
            days_util = []
            ae_df = df[df['aetitle'] == ae]
            for i in range(7):
                day_load = ae_df[ae_df['study_date_dt'].dt.weekday == i]['proc_duration'].sum()
                total_cap = schedule_lookup.get((ae, i), 0) * weekday_counts.get(i, 0)
                util = round((day_load / total_cap) * 100, 1) if total_cap > 0 else 0
                days_util.append({"pct": util})
            matrix_rows.append({"ae": ae, "days": days_util, "avg": round(sum(d['pct'] for d in days_util)/7, 1) if days_util else 0})

    # Chart 4: Rad Performance
    rad_cards = []
    if 'reading_radiologist' in df.columns:
        for rad, r_df in df.groupby('reading_radiologist'):
            drill = []
            loc_col = 'patient_location' if 'patient_location' in df.columns else 'modality'
            for loc, l_df in r_df.groupby(loc_col):
                mods = [{"m": m, "avg": round(m_df['total_tat_min'].mean(), 1), "count": len(m_df)} for m, m_df in l_df.groupby('modality')]
                drill.append({"loc": loc, "mods": mods})
            rad_cards.append({"name": rad, "overall": round(r_df['total_tat_min'].mean(), 1), "drilldown": drill})

    # Charts 5, 6, 7
    modality_split = [{"name": k, "value": int(v)} for k, v in df['modality'].value_counts().items()] if 'modality' in df.columns else []
    
    hourly_patterns = {i: 0 for i in range(24)}
    if 'scheduled_datetime' in df.columns:
        df['hour'] = pd.to_datetime(df['scheduled_datetime'], errors='coerce').dt.hour.fillna(0).astype(int)
        hourly_patterns.update(df['hour'].value_counts().sort_index().to_dict())

    correlation = df[['proc_duration', 'total_tat_min']].values.tolist() if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else []

    er_val = "0m"
    if 'patient_class' in df.columns:
        er_df = df[df['patient_class'].str.contains('ER|Emergency', case=False, na=False)]
        if not er_df.empty: er_val = f"{er_df['total_tat_min'].mean():.1f}m"

    return {
        "summary": {"total": len(df), "global_util": f"{sum(r['avg'] for r in matrix_rows)/len(matrix_rows) if matrix_rows else 0:.1f}%", "er_wait": er_val},
        "matrix": matrix_rows, "class_tat": class_tat, "rad_cards": rad_cards,
        "modality_split": modality_split, "hourly_patterns": hourly_patterns, "correlation": correlation,
        "raw_df": df
    }, start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    classes = [r[0] for r in db.session.execute(text("SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL")).all()]
    locations = [r[0] for r in db.session.execute(text("SELECT DISTINCT patient_location FROM etl_didb_studies WHERE patient_location IS NOT NULL")).all()]
    
    # Infrastructure Tree
    tree_raw = db.session.execute(text("SELECT modality, aetitle FROM aetitle_modality_map")).all()
    tree_dict = {}
    for mod, ae in tree_raw:
        if mod not in tree_dict: tree_dict[mod] = []
        tree_dict[mod].append({"name": ae})
    tree_json = json.dumps({"name": "FLEET", "children": [{"name": k, "children": v} for k, v in tree_dict.items()]})

    data, start, end = get_gold_standard_data(request.form)
    
    # Handle Tab Persistence
    active_tab = request.form.get("active_tab", "ops")

    # Journey Audit
    journey_json = None
    pid = request.form.get("fallback_id")
    if pid:
        q = text("""SELECT procedure_code, scheduled_datetime, insert_time, report_time, proc_duration 
                    FROM etl_didb_studies s JOIN etl_patient_view p ON s.fallback_id = p.fallback_id 
                    WHERE p.fallback_id = :pid ORDER BY s.insert_time ASC""")
        res = db.session.execute(q, {"pid": pid}).mappings().all()
        if res:
            nodes = []
            for r in res:
                t_ent = (r['insert_time'] - pd.Timedelta(minutes=r['proc_duration'])) if r['insert_time'] and r['proc_duration'] else None
                nodes.append({"name": r['procedure_code'], "children": [
                    {"name": f"Sched: {r['scheduled_datetime'].strftime('%H:%M') if r['scheduled_datetime'] else 'N/A'}"},
                    {"name": f"True Entry: {t_ent.strftime('%H:%M') if t_ent else 'N/A'}"},
                    {"name": f"Draft: {round((r['report_time'] - r['insert_time']).total_seconds()/3600, 1) if r['report_time'] and r['insert_time'] else 'N/A'}h"}
                ]})
            journey_json = json.dumps({"name": f"ID: {pid}", "children": nodes})

    # Strip DataFrame for JSON serialization
    template_data = {k: v for k, v in data.items() if k != 'raw_df'} if data else None

    return render_template("report_25.html", data=template_data, display_start=start, display_end=end, 
                           classes=classes, locations=locations, tree_json=tree_json, 
                           journey_json=journey_json, run_report=bool(data), active_tab=active_tab)

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    data, _, _ = get_gold_standard_data(request.form)
    if not data: return "Error", 400
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['raw_df'].drop(columns=['study_date_dt', 'hour'], errors='ignore').to_excel(writer, index=False, sheet_name='RawData')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"RAYD_PRO_Export_{date.today()}.xlsx")
