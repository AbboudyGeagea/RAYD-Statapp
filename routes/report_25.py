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
    print("\n--- [DIAGNOSTIC START: REPORT 25] ---")
    
    go_live = get_etl_cutoff_date() 
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    
    params = {"start": start, "end": end}
    where_clauses = ["study_date BETWEEN :start AND :end"]
    
    if form_data.get("class_enabled") == "on" and form_data.getlist("patient_class"):
        where_clauses.append("patient_class IN :classes")
        params["classes"] = tuple(form_data.getlist("patient_class"))

    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        where_clauses.append("modality IN :modalities")
        params["modalities"] = tuple(form_data.getlist("modality"))

    if form_data.get("ae_enabled") == "on" and form_data.getlist("aetitle"):
        where_clauses.append("aetitle IN :aetitles")
        params["aetitles"] = tuple(form_data.getlist("aetitle"))

    if form_data.get("loc_enabled") == "on" and form_data.getlist("patient_location"):
        where_clauses.append("patient_location IN :locations")
        params["locations"] = tuple(form_data.getlist("patient_location"))
        
    # 2. Fetch SQL Template
    template_res = db.session.execute(text("SELECT report_sql_query FROM report_template WHERE report_id = 25")).fetchone()
    if not template_res: 
        return None, start, end
    
    # 3. Execute Query
    sql_exec = f"SELECT * FROM ({template_res[0]}) as sub WHERE {' AND '.join(where_clauses)}"
    df = pd.DataFrame(db.session.execute(text(sql_exec), params).mappings().all())
    
    if df.empty: 
        print("!! WARNING: Query returned 0 rows.")
        return None, start, end

    print(f"STEP 1: Columns found: {list(df.columns)}")

    # 4. Defensive Data Cleaning
    # If the SQL returns 'rvu_value' instead of 'rvu', let's map it automatically
    if 'rvu_value' in df.columns and 'rvu' not in df.columns:
        df['rvu'] = df['rvu_value']

    for col in ['total_tat_min', 'proc_duration', 'rvu']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            print(f"STEP 2/3: Column '{col}' sum: {df[col].sum()}")
        else:
            print(f"!! ALERT: Column '{col}' MISSING. Defaulting to 0.0")
            df[col] = 0.0
    
    df['study_date_dt'] = pd.to_datetime(df['study_date'], errors='coerce') if 'study_date' in df.columns else pd.to_datetime(date.today())

    # --- Metrics Generation ---
    matrix_rows = []
    high_stress = 0
    under_utilized = 0
    total_active_mins = df['proc_duration'].sum()

    if 'aetitle' in df.columns:
        date_range = pd.date_range(start, end)
        weekday_counts = date_range.dayofweek.value_counts().to_dict()
        
        sched_q = db.session.execute(text("SELECT UPPER(TRIM(aetitle)) as ae, day_of_week, std_opening_minutes FROM device_weekly_schedule")).mappings().all()
        schedule_lookup = {(s['ae'], int(s['day_of_week'])): s['std_opening_minutes'] for s in sched_q}
        
        for ae in sorted(df['aetitle'].unique()):
            ae_upper = str(ae).upper().strip()
            ae_df = df[df['aetitle'] == ae]
            ae_total_load = 0
            ae_total_cap = 0
            days_util = []

            for i in range(7):
                day_load = ae_df[ae_df['study_date_dt'].dt.weekday == i]['proc_duration'].sum()
                opening_mins = schedule_lookup.get((ae_upper, i), 0)
                occ = weekday_counts.get(i, 0)
                total_cap = opening_mins * occ
                
                util = round((day_load / total_cap) * 100, 1) if total_cap > 0 else 0
                days_util.append({"pct": util, "mins": int(day_load)})
                ae_total_load += day_load
                ae_total_cap += total_cap

            ae_avg = round((ae_total_load / ae_total_cap * 100), 1) if ae_total_cap > 0 else 0
            if ae_avg > 85: high_stress += 1
            elif 0 < ae_avg < 30: under_utilized += 1

            matrix_rows.append({
                "ae": ae, "days": days_util, "avg": ae_avg,
                "total_rvu": round(ae_df['rvu'].sum(), 1)
            })

    # Rad Performance
    rad_cards = []
    if 'reading_radiologist' in df.columns:
        for rad, r_df in df.groupby('reading_radiologist'):
            drill = []
            loc_col = 'patient_location' if 'patient_location' in df.columns else 'modality'
            for loc, l_df in r_df.groupby(loc_col):
                mods = [{"m": m, "avg": round(m_df['total_tat_min'].mean(), 1), "count": len(m_df), "rvu": round(m_df['rvu'].sum(), 1)} for m, m_df in l_df.groupby('modality')]
                drill.append({"loc": loc, "mods": mods, "loc_rvu": round(l_df['rvu'].sum(), 1)})
            
            rad_cards.append({
                "name": rad, "overall": round(r_df['total_tat_min'].mean(), 1), 
                "total_rvu": round(r_df['rvu'].sum(), 1), "drilldown": drill
            })

    print(f"STEP 4: Final Summary RVU: {df['rvu'].sum()}")
    print("--- [DIAGNOSTIC END] ---\n")

    return {
        "summary": {
            "total": len(df), "global_util": f"{(sum(r['avg'] for r in matrix_rows)/len(matrix_rows) if matrix_rows else 0):.1f}%", 
            "er_wait": f"{df[df['patient_class'].str.contains('ER|Emergency', case=False, na=False)]['total_tat_min'].mean():.1f}m" if 'patient_class' in df.columns else "0m",
            "high_stress_count": high_stress, "low_util_count": under_utilized,
            "work_hours": round(total_active_mins / 60, 1), "total_rvu": round(df['rvu'].sum(), 1)
        },
        "matrix": matrix_rows, 
        "class_tat": df.groupby('patient_class')['total_tat_min'].mean().round(1).to_dict() if 'patient_class' in df.columns else {}, 
        "rad_cards": rad_cards,
        "modality_split": [{"name": k, "value": int(v)} for k, v in df['modality'].value_counts().items()] if 'modality' in df.columns else [], 
        "hourly_patterns": {i: int(v) for i, v in pd.to_datetime(df['scheduled_datetime']).dt.hour.value_counts().sort_index().items()} if 'scheduled_datetime' in df.columns else {}, 
        "correlation": df[['proc_duration', 'total_tat_min']].values.tolist() if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else [],
        "raw_df": df
    }, start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    classes   = [r[0] for r in db.session.execute(text("SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL ORDER BY 1")).all()]
    locations = [r[0] for r in db.session.execute(text("SELECT DISTINCT patient_location FROM etl_didb_studies WHERE patient_location IS NOT NULL ORDER BY 1")).all()]
    modalities= [r[0] for r in db.session.execute(text("SELECT DISTINCT modality FROM aetitle_modality_map ORDER BY 1")).all()]
    aetitles  = [r[0] for r in db.session.execute(text("SELECT DISTINCT aetitle FROM aetitle_modality_map ORDER BY 1")).all()]
    
    tree_raw = db.session.execute(text("SELECT modality, aetitle FROM aetitle_modality_map")).all()
    tree_dict = {}
    for mod, ae in tree_raw:
        if mod not in tree_dict: tree_dict[mod] = []
        tree_dict[mod].append({"name": ae})
    tree_json = json.dumps({"name": "FLEET", "children": [{"name": k, "children": v} for k, v in tree_dict.items()]})

    data, start, end = get_gold_standard_data(request.form)
    active_tab = request.form.get("active_tab", "ops")

    journey_json = None
    pid = request.form.get("fallback_id")
    if pid:
        res = db.session.execute(text("SELECT procedure_code, scheduled_datetime, insert_time, report_time, proc_duration FROM etl_didb_studies s JOIN etl_patient_view p ON s.fallback_id = p.fallback_id WHERE p.fallback_id = :pid ORDER BY s.insert_time ASC"), {"pid": pid}).mappings().all()
        if res:
            nodes = []
            for r in res:
                t_ent = (r['insert_time'] - pd.Timedelta(minutes=r['proc_duration'])) if r['insert_time'] and r['proc_duration'] else None
                nodes.append({"name": r['procedure_code'], "children": [{"name": f"Sched: {r['scheduled_datetime'].strftime('%H:%M') if r['scheduled_datetime'] else 'N/A'}"}, {"name": f"True Entry: {t_ent.strftime('%H:%M') if t_ent else 'N/A'}"}]})
            journey_json = json.dumps({"name": f"ID: {pid}", "children": nodes})

    template_data = {k: v for k, v in data.items() if k != 'raw_df'} if data else None
    return render_template("report_25.html", data=template_data, display_start=start, display_end=end, classes=classes, locations=locations, modalities=modalities, aetitles=aetitles, tree_json=tree_json, journey_json=journey_json, run_report=bool(data), active_tab=active_tab)

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    data, _, _ = get_gold_standard_data(request.form)
    if not data: return "Error", 400
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['raw_df'].drop(columns=['study_date_dt'], errors='ignore').to_excel(writer, index=False, sheet_name='RawData')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"RAYD_PRO_Export_{date.today()}.xlsx")
