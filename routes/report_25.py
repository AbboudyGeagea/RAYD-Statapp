import json
import pandas as pd
import io
from datetime import date
from flask import Blueprint, render_template, request, send_file, url_for
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put

report_25_bp = Blueprint("report_25", __name__)

def _load_shift_config():
    defaults = {'morning_start': 7, 'morning_end': 15,
                'afternoon_start': 15, 'afternoon_end': 23,
                'night_start': 23, 'night_end': 7}
    try:
        rows = db.session.execute(text(
            "SELECT key, value FROM settings WHERE key LIKE 'shift_%'"
        )).fetchall()
        for key, val in rows:
            k = key.replace('shift_', '')
            if k in defaults:
                defaults[k] = int(val)
    except Exception:
        pass
    return defaults

def get_gold_standard_data(form_data):
    # Cache hit check — skip full DB scan for identical re-runs within 5 min
    cached = cache_get(25, form_data)
    if cached is not None:
        return cached

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

    # TAT percentiles for the whole dataset
    tat_vals_all = df[df['total_tat_min'] > 0]['total_tat_min'] if 'total_tat_min' in df.columns else pd.Series([], dtype=float)
    tat_median = round(float(tat_vals_all.median()), 1) if len(tat_vals_all) > 0 else 0.0
    tat_p25    = round(float(tat_vals_all.quantile(0.25)), 1) if len(tat_vals_all) > 0 else 0.0
    tat_p75    = round(float(tat_vals_all.quantile(0.75)), 1) if len(tat_vals_all) > 0 else 0.0

    # Rad Performance
    rad_cards = []
    if 'reading_radiologist' in df.columns:
        for rad, r_df in df.groupby('reading_radiologist'):
            drill = []
            loc_col = 'patient_location' if 'patient_location' in df.columns else 'modality'
            for loc, l_df in r_df.groupby(loc_col):
                mods = [{"m": m, "avg": round(m_df['total_tat_min'].mean(), 1), "count": len(m_df), "rvu": round(m_df['rvu'].sum(), 1)} for m, m_df in l_df.groupby('modality')]
                drill.append({"loc": loc, "mods": mods, "loc_rvu": round(l_df['rvu'].sum(), 1)})

            total_scan_hours = r_df['proc_duration'].sum() / 60
            rvu_per_hour = round(r_df['rvu'].sum() / total_scan_hours, 2) if total_scan_hours > 0 else 0.0

            rad_cards.append({
                "name": rad,
                "overall": round(r_df['total_tat_min'].mean(), 1),
                "tat_median": round(float(r_df[r_df['total_tat_min'] > 0]['total_tat_min'].median()), 1) if (r_df['total_tat_min'] > 0).any() else 0.0,
                "total_rvu": round(r_df['rvu'].sum(), 1),
                "rvu_per_hour": rvu_per_hour,
                "drilldown": drill
            })

        # Add percentile rank among peers (lower TAT = better = lower percentile)
        peer_tats = sorted([r['overall'] for r in rad_cards if r['overall'] > 0])
        n = len(peer_tats)
        for r in rad_cards:
            if r['overall'] > 0 and n > 0:
                rank = sum(1 for t in peer_tats if t <= r['overall'])
                r['tat_percentile'] = round(rank / n * 100)
            else:
                r['tat_percentile'] = None

    print(f"STEP 4: Final Summary RVU: {df['rvu'].sum()}")
    print("--- [DIAGNOSTIC END] ---\n")

    # ── New analytics ─────────────────────────────────────────────────
    tat_hist, ae_tat, rvu_tat, outlier_studies, global_mean_tat = [], [], [], [], 0.0

    if 'total_tat_min' in df.columns:
        tat_vals = df[df['total_tat_min'] > 0]['total_tat_min']
        global_mean_tat = round(float(tat_vals.mean()), 1) if len(tat_vals) > 0 else 0.0
        bins   = [0, 30, 60, 90, 120, 180, 240, 360]
        labels = ['0-30m', '31-60m', '61-90m', '91-120m', '121-180m', '181-240m', '241-360m', '360m+']
        for i, label in enumerate(labels):
            lo = bins[i]
            hi = bins[i + 1] if i < len(bins) - 1 else float('inf')
            tat_hist.append({'label': label, 'count': int(((tat_vals > lo) & (tat_vals <= hi)).sum())})

        if 'aetitle' in df.columns:
            ae_g = df[df['total_tat_min'] > 0].groupby('aetitle')['total_tat_min'].agg(['mean', 'count']).reset_index()
            ae_g = ae_g[ae_g['count'] >= 5].sort_values('mean')
            ae_tat = [{'ae': r['aetitle'], 'avg_tat': round(float(r['mean']), 1), 'cnt': int(r['count'])} for _, r in ae_g.iterrows()]

        threshold = global_mean_tat * 2
        out_cols = [c for c in ['aetitle', 'modality', 'reading_radiologist', 'patient_class', 'procedure_code', 'study_date', 'total_tat_min'] if c in df.columns]
        out_df = df[df['total_tat_min'] > threshold][out_cols].sort_values('total_tat_min', ascending=False).head(50)
        for row in out_df.to_dict('records'):
            if 'study_date' in row and hasattr(row['study_date'], 'strftime'):
                row['study_date'] = str(row['study_date'])[:10]
            if 'total_tat_min' in row:
                row['total_tat_min'] = round(float(row['total_tat_min']), 1)
            outlier_studies.append(row)

    # IQR-based outlier filter for scatter plots
    def _iqr_filter(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        return (series >= q1 - 1.5 * iqr) & (series <= q3 + 1.5 * iqr)

    scatter_outliers_removed = 0
    if 'proc_duration' in df.columns and 'total_tat_min' in df.columns:
        raw = df[(df['proc_duration'] > 0) & (df['total_tat_min'] > 0)]
        mask = _iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])
        scatter_outliers_removed = int((~mask).sum())

    rvu_outliers_removed = 0
    if 'rvu' in df.columns and 'total_tat_min' in df.columns:
        tmp_raw = df[(df['rvu'] > 0) & (df['total_tat_min'] > 0)]
        mask_rvu = _iqr_filter(tmp_raw['rvu']) & _iqr_filter(tmp_raw['total_tat_min'])
        rvu_outliers_removed = int((~mask_rvu).sum())
        tmp = tmp_raw[mask_rvu][['rvu', 'total_tat_min']]
        rvu_tat = [[round(float(r[0]), 2), round(float(r[1]), 1)] for r in tmp.values.tolist()]

    # TAT by modality (from existing df)
    modality_tat = []
    try:
        if 'modality' in df.columns and 'total_tat_min' in df.columns:
            mod_g = df[df['total_tat_min'] > 0].groupby('modality')['total_tat_min'].agg(
                ['mean', 'median', 'count']
            ).reset_index()
            mod_g = mod_g[mod_g['count'] >= 5].sort_values('mean')
            modality_tat = [
                {'mod': r['modality'], 'avg': round(float(r['mean']), 1),
                 'median': round(float(r['median']), 1), 'cnt': int(r['count'])}
                for _, r in mod_g.iterrows()
            ]
    except Exception:
        pass

    # Unread study aging buckets
    unread_aging = []
    try:
        aging_rows = db.session.execute(text("""
            SELECT
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - study_date::timestamp))/3600 <= 24 THEN '0-24h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - study_date::timestamp))/3600 <= 48 THEN '24-48h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - study_date::timestamp))/3600 <= 72 THEN '48-72h'
                    ELSE '72h+'
                END AS bucket,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_status ILIKE '%unread%'
              AND s.study_date BETWEEN :start AND :end
            GROUP BY 1, 2
            ORDER BY
                CASE bucket WHEN '0-24h' THEN 1 WHEN '24-48h' THEN 2 WHEN '48-72h' THEN 3 ELSE 4 END
        """), {"start": start, "end": end}).fetchall()
        for bucket, modality, cnt in aging_rows:
            unread_aging.append({'bucket': bucket, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        pass

    # Studies per shift
    shift_breakdown = []
    try:
        sc = _load_shift_config()
        shift_rows = db.session.execute(text("""
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM s.scheduled_datetime) >= :ms AND EXTRACT(HOUR FROM s.scheduled_datetime) < :me THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM s.scheduled_datetime) >= :as AND EXTRACT(HOUR FROM s.scheduled_datetime) < :ae THEN 'Afternoon'
                    ELSE 'Night'
                END AS shift,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_date BETWEEN :start AND :end
              AND s.scheduled_datetime IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
        """), {
            "start": start, "end": end,
            "ms": sc['morning_start'],   "me": sc['morning_end'],
            "as": sc['afternoon_start'], "ae": sc['afternoon_end'],
        }).fetchall()
        for shift, modality, cnt in shift_rows:
            shift_breakdown.append({'shift': shift, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        pass

    # Addendum rate by radiologist
    addendum_data = {'overall_pct': 0.0, 'by_rad': []}
    try:
        add_rows = db.session.execute(text("""
            SELECT
                COALESCE(rep_final_signed_by, 'Unknown') AS radiologist,
                COUNT(*) AS total,
                SUM(CASE WHEN rep_has_addendum THEN 1 ELSE 0 END) AS addendum_count,
                ROUND(SUM(CASE WHEN rep_has_addendum THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS addendum_pct
            FROM etl_didb_studies
            WHERE study_date BETWEEN :start AND :end
              AND rep_final_signed_by IS NOT NULL
              AND rep_final_timestamp IS NOT NULL
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY addendum_pct DESC
        """), {"start": start, "end": end}).fetchall()
        by_rad = [
            {'rad': r[0], 'total': int(r[1]), 'addendum_count': int(r[2]), 'pct': float(r[3] or 0)}
            for r in add_rows
        ]
        total_studies = sum(r['total'] for r in by_rad)
        total_addenda = sum(r['addendum_count'] for r in by_rad)
        overall_pct = round(total_addenda / total_studies * 100, 1) if total_studies > 0 else 0.0
        addendum_data = {'overall_pct': overall_pct, 'total_addenda': total_addenda, 'by_rad': by_rad}
    except Exception:
        pass

    result = ({
        "summary": {
            "total": len(df), "global_util": f"{(sum(r['avg'] for r in matrix_rows)/len(matrix_rows) if matrix_rows else 0):.1f}%",
            "er_wait": f"{df[df['patient_class'].str.contains('ER|Emergency', case=False, na=False)]['total_tat_min'].mean():.1f}m" if 'patient_class' in df.columns else "0m",
            "high_stress_count": high_stress, "low_util_count": under_utilized,
            "work_hours": round(total_active_mins / 60, 1), "total_rvu": round(df['rvu'].sum(), 1),
            "tat_median": tat_median, "tat_p25": tat_p25, "tat_p75": tat_p75,
        },
        "matrix": matrix_rows, 
        "class_tat": df.groupby('patient_class')['total_tat_min'].mean().round(1).to_dict() if 'patient_class' in df.columns else {}, 
        "rad_cards": rad_cards,
        "modality_split": [{"name": k, "value": int(v)} for k, v in df['modality'].value_counts().items()] if 'modality' in df.columns else [], 
        "hourly_patterns": {i: int(v) for i, v in pd.to_datetime(df['scheduled_datetime']).dt.hour.value_counts().sort_index().items()} if 'scheduled_datetime' in df.columns else {}, 
        "correlation": (lambda: (
            lambda raw: raw[_iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])][['proc_duration','total_tat_min']].values.tolist()
        )(df[(df['proc_duration']>0)&(df['total_tat_min']>0)]) if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else [])(),
        "scatter_outliers_removed": scatter_outliers_removed,
        "rvu_outliers_removed": rvu_outliers_removed,
        "raw_df": df,
        "tat_hist": tat_hist,
        "ae_tat": ae_tat,
        "rvu_tat": rvu_tat,
        "outlier_studies": outlier_studies,
        "global_mean_tat": global_mean_tat,
        "modality_tat":    modality_tat,
        "unread_aging":    unread_aging,
        "shift_breakdown": shift_breakdown,
        "addendum_data":   addendum_data,
    }, start, end)
    cache_put(25, form_data, result)
    return result

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

    shift_config = _load_shift_config()
    run_report = request.method == "POST"
    active_tab = request.form.get("active_tab", "ops")

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    data          = None
    journey_json  = None
    template_data = None

    if run_report:
        data, display_start, display_end = get_gold_standard_data(request.form)

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

    return render_template("report_25.html", data=template_data, display_start=display_start, display_end=display_end, classes=classes, locations=locations, modalities=modalities, aetitles=aetitles, tree_json=tree_json, journey_json=journey_json, run_report=run_report, active_tab=active_tab, shift_config=shift_config)

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

@report_25_bp.route("/report/25/save-shifts", methods=["POST"])
@login_required
def save_shifts_25():
    from flask import redirect
    keys = ['morning_start', 'morning_end', 'afternoon_start', 'afternoon_end', 'night_start', 'night_end']
    for k in keys:
        val = request.form.get(k)
        if val is not None:
            existing = db.session.execute(
                text("SELECT id FROM settings WHERE key = :k"), {"k": f"shift_{k}"}
            ).fetchone()
            if existing:
                db.session.execute(
                    text("UPDATE settings SET value = :v WHERE key = :k"),
                    {"k": f"shift_{k}", "v": val}
                )
            else:
                db.session.execute(
                    text("INSERT INTO settings (key, value) VALUES (:k, :v)"),
                    {"k": f"shift_{k}", "v": val}
                )
    db.session.commit()
    return redirect(url_for('report_25.report_25'))
