import json
import pandas as pd
import io
from datetime import date
from flask import Blueprint, render_template, request, send_file, url_for
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from routes.insights_engine import run_tech_insights, run_rad_insights

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

    # Build secondary filter fragments for raw SQL queries against etl_didb_studies (prefix "s.")
    _sec_filters = ""
    if "classes" in params:
        _sec_filters += " AND s.patient_class IN :classes"
    if "modalities" in params:
        _sec_filters += " AND UPPER(TRIM(m.modality)) IN :modalities"
    if "aetitles" in params:
        _sec_filters += " AND s.storing_ae IN :aetitles"
    if "locations" in params:
        _sec_filters += " AND s.patient_location IN :locations"
    # Whether secondary queries need the modality JOIN
    _sec_needs_mod_join = "modalities" in params

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
    total_active_mins = df.loc[df['proc_duration'] > 0, 'proc_duration'].sum()

    if 'aetitle' in df.columns:
        date_range = pd.date_range(start, end)
        weekday_counts = date_range.dayofweek.value_counts().to_dict()
        
        sched_q = db.session.execute(text("""
            SELECT
                UPPER(TRIM(ws.aetitle)) AS ae,
                ws.day_of_week,
                COALESCE(m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS std_opening_minutes
            FROM device_weekly_schedule ws
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle))
        """)).mappings().all()
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
                "total_rvu": round(ae_df['rvu'].sum(), 1),
                "total_cap": ae_total_cap,
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

            # Only count studies with a mapped duration — unmapped studies (0 min)
            # would contribute RVU without time, inflating the rate
            r_df_mapped = r_df[r_df['proc_duration'] > 0]
            total_scan_hours = r_df_mapped['proc_duration'].sum() / 60
            rvu_per_hour = round(r_df_mapped['rvu'].sum() / total_scan_hours, 2) if total_scan_hours > 0 else 0.0

            r_df_valid = r_df[r_df['total_tat_min'] > 0]
            rad_cards.append({
                "name": rad,
                "overall": round(r_df_valid['total_tat_min'].mean(), 1) if len(r_df_valid) > 0 else 0.0,
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

    # ── Signing pattern per radiologist ───────────────────────────────
    shift_patterns = {}
    ts_rows = []
    try:
        _BREAK_MIN = 20
        ts_rows = db.session.execute(text(f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(CONCAT(
                        COALESCE(s.signing_physician_first_name, ''), ' ',
                        COALESCE(s.signing_physician_last_name,  '')
                    )), ''),
                    s.rep_final_signed_by,
                    'Unknown'
                ) AS radiologist,
                s.rep_final_timestamp
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
            WHERE s.rep_final_timestamp IS NOT NULL
              AND s.rep_final_timestamp::date BETWEEN :start AND :end
              {_sec_filters}
            ORDER BY 1, 2
        """), params).fetchall()

        if ts_rows:
            ts_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts'])
            ts_df['ts']        = pd.to_datetime(ts_df['ts'])
            ts_df['work_date'] = ts_df['ts'].dt.date
            ts_df['hour']      = ts_df['ts'].dt.hour
            ts_df['dow']       = ts_df['ts'].dt.dayofweek  # 0=Mon

            def _h_to_hhmm(h):
                hh = int(h); mm = int(round((h - hh) * 60))
                return f"{hh:02d}:{mm:02d}"

            for rad, rdf in ts_df.groupby('radiologist'):
                if rad.strip() in ('Unknown', ''):
                    continue
                rdf = rdf.sort_values('ts')

                hm = rdf.groupby(['dow', 'hour']).size().reset_index(name='cnt')
                heatmap = [[int(r['hour']), int(r['dow']), int(r['cnt'])] for _, r in hm.iterrows()]
                hm_max  = int(hm['cnt'].max()) if not hm.empty else 1

                arrivals, departures, break_cnts, break_durs = [], [], [], []
                daily_log = []

                for work_date, ddf in rdf.groupby('work_date'):
                    times = ddf['ts'].sort_values().tolist()
                    first, last = times[0], times[-1]
                    arr_h = first.hour + first.minute / 60
                    dep_h = last.hour  + last.minute  / 60
                    arrivals.append(arr_h)
                    departures.append(dep_h)

                    breaks = []
                    for i in range(1, len(times)):
                        gap = (times[i] - times[i - 1]).total_seconds() / 60
                        if gap >= _BREAK_MIN:
                            dur = round(gap)
                            icon = '☕' if dur <= 35 else ('🚬' if dur <= 70 else '🏃')
                            kind = 'Coffee' if dur <= 35 else ('Long break' if dur <= 70 else 'Disappeared')
                            breaks.append({'start': times[i-1].strftime('%H:%M'),
                                           'end':   times[i].strftime('%H:%M'),
                                           'duration': dur, 'icon': icon, 'kind': kind})
                    break_cnts.append(len(breaks))
                    break_durs.extend([b['duration'] for b in breaks])
                    daily_log.append({
                        'date': str(work_date), 'dow': first.strftime('%a'),
                        'arrival': first.strftime('%H:%M'), 'departure': last.strftime('%H:%M'),
                        'studies': len(times),
                        'span_h': round(dep_h - arr_h, 1),
                        'breaks': breaks,
                    })

                wd = len(daily_log)
                shift_patterns[rad] = {
                    'avg_arrival':    _h_to_hhmm(sum(arrivals)   / len(arrivals))   if arrivals   else '—',
                    'avg_departure':  _h_to_hhmm(sum(departures) / len(departures)) if departures else '—',
                    'avg_breaks_day': round(sum(break_cnts) / wd, 1)                if wd         else 0,
                    'avg_break_dur':  int(round(sum(break_durs) / len(break_durs))) if break_durs else 0,
                    'working_days':   wd,
                    'heatmap':        heatmap,
                    'hm_max':         hm_max,
                    'daily_log':      daily_log[-60:],   # cap at 60 most recent days
                }
    except Exception as _e:
        print(f"Shift pattern error: {_e}")

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

        # IQR-based outlier threshold (robust to skew, unlike mean*2)
        q1_tat, q3_tat = tat_vals.quantile(0.25), tat_vals.quantile(0.75)
        iqr_tat = q3_tat - q1_tat
        threshold = q3_tat + 1.5 * iqr_tat
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
        aging_rows = db.session.execute(text(f"""
            SELECT
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 24 THEN '0-24h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 48 THEN '24-48h'
                    WHEN EXTRACT(EPOCH FROM (NOW() - s.study_date::timestamp))/3600 <= 72 THEN '48-72h'
                    ELSE '72h+'
                END AS bucket,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_status ILIKE '%unread%'
              AND s.study_date BETWEEN :start AND :end
              {_sec_filters}
            GROUP BY 1, 2
            ORDER BY
                CASE bucket WHEN '0-24h' THEN 1 WHEN '24-48h' THEN 2 WHEN '48-72h' THEN 3 ELSE 4 END
        """), params).fetchall()
        for bucket, modality, cnt in aging_rows:
            unread_aging.append({'bucket': bucket, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        db.session.rollback()

    # Studies per shift
    shift_breakdown = []
    try:
        sc = _load_shift_config()
        shift_rows = db.session.execute(text(f"""
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :ms AND EXTRACT(HOUR FROM o.scheduled_datetime) < :me THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :as AND EXTRACT(HOUR FROM o.scheduled_datetime) < :ae THEN 'Afternoon'
                    ELSE 'Night'
                END AS shift,
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                COUNT(*) AS cnt
            FROM etl_orders o
            JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_date BETWEEN :start AND :end
              AND o.scheduled_datetime IS NOT NULL
              {_sec_filters}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """), {**params,
            "ms": sc['morning_start'],   "me": sc['morning_end'],
            "as": sc['afternoon_start'], "ae": sc['afternoon_end'],
        }).fetchall()
        for shift, modality, cnt in shift_rows:
            shift_breakdown.append({'shift': shift, 'modality': modality, 'cnt': int(cnt)})
    except Exception:
        db.session.rollback()

    # Addendum rate by radiologist
    addendum_data = {'overall_pct': 0.0, 'by_rad': []}
    try:
        add_rows = db.session.execute(text(f"""
            SELECT
                COALESCE(s.rep_final_signed_by, 'Unknown') AS radiologist,
                COUNT(*) AS total,
                SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END) AS addendum_count,
                ROUND(SUM(CASE WHEN s.rep_has_addendum THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS addendum_pct
            FROM etl_didb_studies s
            {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
            WHERE s.study_date BETWEEN :start AND :end
              AND s.rep_final_signed_by IS NOT NULL
              AND s.rep_final_timestamp IS NOT NULL
              {_sec_filters}
            GROUP BY 1
            HAVING COUNT(*) >= 5
            ORDER BY addendum_pct DESC
        """), params).fetchall()
        by_rad = [
            {'rad': r[0], 'total': int(r[1]), 'addendum_count': int(r[2]), 'pct': float(r[3] or 0)}
            for r in add_rows
        ]
        total_studies = sum(r['total'] for r in by_rad)
        total_addenda = sum(r['addendum_count'] for r in by_rad)
        overall_pct = round(total_addenda / total_studies * 100, 1) if total_studies > 0 else 0.0
        addendum_data = {'overall_pct': overall_pct, 'total_addenda': total_addenda, 'by_rad': by_rad}
    except Exception:
        db.session.rollback()

    # ── Technician monitoring (HL7 orders — 5 flag categories) ─────────────────
    tech_data = {
        'summary': {},
        'by_technician': [],
        'by_modality': [],
        'flagged': [],
        'never_done': [],
    }
    try:
        from datetime import datetime as _dt
        _now = _dt.utcnow()

        tech_rows = db.session.execute(text(f"""
            SELECT
                o.accession_number,
                o.modality,
                o.procedure_code,
                o.done_by,
                o.scheduled_datetime,
                o.done_at,
                COALESCE(p.duration_minutes, 30) AS proc_duration
            FROM hl7_orders o
            LEFT JOIN procedure_duration_map p
                   ON UPPER(TRIM(o.procedure_code)) = UPPER(TRIM(p.procedure_code))
            WHERE o.scheduled_datetime IS NOT NULL
              AND o.scheduled_datetime::date BETWEEN :start AND :end
              {"AND UPPER(TRIM(o.modality)) IN :modalities" if "modalities" in params else ""}
            ORDER BY o.modality, o.scheduled_datetime
        """), params).mappings().fetchall()

        if tech_rows:
            tdf = pd.DataFrame(tech_rows)
            tdf['proc_duration'] = pd.to_numeric(tdf['proc_duration'], errors='coerce').fillna(30)
            tdf['scheduled_datetime'] = pd.to_datetime(tdf['scheduled_datetime'])
            tdf['done_at'] = pd.to_datetime(tdf['done_at'], errors='coerce')
            tdf['tat_min'] = (tdf['done_at'] - tdf['scheduled_datetime']).dt.total_seconds() / 60.0

            completed = tdf[tdf['done_at'].notna()].copy()
            pending   = tdf[tdf['done_at'].isna()].copy()

            # ── Overlap detection: done_at falls after the next exam started ──
            overlap_accessions = set()
            for mod, grp in completed.groupby('modality'):
                grp = grp.sort_values('scheduled_datetime').reset_index()
                for i in range(len(grp) - 1):
                    cur  = grp.iloc[i]
                    nxt  = grp.iloc[i + 1]
                    if pd.notna(cur['done_at']) and cur['done_at'] > nxt['scheduled_datetime']:
                        overlap_accessions.add(cur['accession_number'])

            # ── Flag each completed exam ──────────────────────────────────────
            flagged_rows = []
            for _, r in completed.iterrows():
                flags = []
                tat   = r['tat_min']
                dur   = float(r['proc_duration'])
                if pd.isna(tat):
                    continue
                if tat < 0:
                    flags.append('before_scheduled')
                elif tat < dur * 0.5:
                    flags.append('too_early')
                if r['accession_number'] in overlap_accessions:
                    flags.append('overlap')
                if tat > dur * 2:
                    flags.append('too_late')
                flagged_rows.append({
                    'accession':    str(r.get('accession_number') or ''),
                    'modality':     str(r.get('modality') or ''),
                    'procedure':    str(r.get('procedure_code') or ''),
                    'technician':   str(r['done_by']) if pd.notna(r.get('done_by')) else '',
                    'scheduled_at': r['scheduled_datetime'].strftime('%Y-%m-%d %H:%M'),
                    'done_at':      r['done_at'].strftime('%H:%M'),
                    'tat_min':      round(float(tat), 1),
                    'proc_duration': int(dur),
                    'flags':        flags,
                })
            tech_data['flagged'] = sorted([r for r in flagged_rows if r['flags']], key=lambda x: len(x['flags']), reverse=True)

            # ── Never done: past due with no done_at ──────────────────────────
            for _, r in pending.iterrows():
                deadline = r['scheduled_datetime'] + pd.Timedelta(minutes=float(r['proc_duration']))
                if deadline < pd.Timestamp(_now):
                    tech_data['never_done'].append({
                        'accession':    str(r.get('accession_number') or ''),
                        'modality':     str(r.get('modality') or ''),
                        'procedure':    str(r.get('procedure_code') or ''),
                        'scheduled_at': r['scheduled_datetime'].strftime('%Y-%m-%d %H:%M'),
                        'overdue_min':  round((pd.Timestamp(_now) - deadline).total_seconds() / 60, 1),
                    })

            # ── Summary counts ────────────────────────────────────────────────
            flagged_accessions = {r['accession'] for r in tech_data['flagged']}
            tech_data['summary'] = {
                'total_scheduled':       len(tdf),
                'total_completed':       len(completed),
                'never_done':            len(tech_data['never_done']),
                'flag_before_scheduled': sum(1 for r in tech_data['flagged'] if 'before_scheduled' in r['flags']),
                'flag_too_early':        sum(1 for r in tech_data['flagged'] if 'too_early'        in r['flags']),
                'flag_overlap':          sum(1 for r in tech_data['flagged'] if 'overlap'          in r['flags']),
                'flag_too_late':         sum(1 for r in tech_data['flagged'] if 'too_late'         in r['flags']),
            }

            # ── Daily trend ───────────────────────────────────────────────────
            daily_trend = []
            if len(completed):
                completed = completed.copy()
                completed['_date'] = completed['scheduled_datetime'].dt.date
                for day, gdf in completed.groupby('_date'):
                    tats = gdf['tat_min'].dropna()
                    daily_trend.append({
                        'date':     str(day),
                        'avg_tat':  round(float(tats.mean()), 1) if len(tats) else 0,
                        'count':    len(gdf),
                        'flags':    int(gdf['accession_number'].isin(flagged_accessions).sum()),
                    })
                daily_trend.sort(key=lambda x: x['date'])
            tech_data['daily_trend'] = daily_trend

            def _skew_insight(avg, median):
                """Return a plain-English warning if avg/median diverge significantly."""
                if avg is None or median is None or median == 0:
                    return None
                ratio = avg / median
                if ratio >= 2.0:
                    return f"Avg is {ratio:.1f}× the median — a small number of very slow exams are inflating the average. Investigate outliers."
                if ratio >= 1.5:
                    return f"Avg is {ratio:.1f}× the median — some delayed exams are pulling the average up."
                return None

            # ── By technician (completed only) ────────────────────────────────
            for tech, gdf in completed[completed['done_by'].notna()].groupby('done_by'):
                tats = gdf['tat_min'].dropna()
                avg    = round(float(tats.mean()),   1) if len(tats) else None
                median = round(float(tats.median()), 1) if len(tats) else None
                tech_data['by_technician'].append({
                    'name':       str(tech),
                    'count':      len(gdf),
                    'avg_tat':    avg,
                    'median_tat': median,
                    'flags':      sum(1 for r in tech_data['flagged'] if r['technician'] == str(tech) and r['flags']),
                    'insight':    _skew_insight(avg, median),
                })
            tech_data['by_technician'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)

            # ── By modality ───────────────────────────────────────────────────
            for mod, gdf in completed[completed['modality'].notna()].groupby('modality'):
                tats = gdf['tat_min'].dropna()
                avg    = round(float(tats.mean()),   1) if len(tats) else None
                median = round(float(tats.median()), 1) if len(tats) else None
                tech_data['by_modality'].append({
                    'modality':   str(mod),
                    'count':      len(gdf),
                    'avg_tat':    avg,
                    'median_tat': median,
                    'insight':    _skew_insight(avg, median),
                })
            tech_data['by_modality'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)

            # ── Department-level insight ──────────────────────────────────────
            all_tats = completed['tat_min'].dropna()
            if len(all_tats):
                dept_avg    = round(float(all_tats.mean()),   1)
                dept_median = round(float(all_tats.median()), 1)
                tech_data['summary']['avg_tat']    = dept_avg
                tech_data['summary']['median_tat'] = dept_median
                tech_data['summary']['dept_insight'] = _skew_insight(dept_avg, dept_median)

    except Exception as _e:
        db.session.rollback()
        print(f"Technician TAT error: {_e}")

    # ── Statistical insights (pure Python — no external calls) ───────────
    tech_insights = []
    rad_insights  = []
    try:
        if tech_data.get('flagged') is not None:
            # Rebuild completed_df subset for insight engine
            _tech_rows = db.session.execute(text(f"""
                SELECT o.done_by, o.done_at, o.scheduled_datetime,
                       COALESCE(p.duration_minutes, 30) AS proc_duration, o.modality
                FROM hl7_orders o
                LEFT JOIN procedure_duration_map p
                       ON UPPER(TRIM(o.procedure_code)) = UPPER(TRIM(p.procedure_code))
                WHERE o.done_at IS NOT NULL
                  AND o.scheduled_datetime::date BETWEEN :start AND :end
                  {"AND UPPER(TRIM(o.modality)) IN :modalities" if "modalities" in params else ""}
            """), params).mappings().fetchall()
            if _tech_rows:
                _tdf = pd.DataFrame(_tech_rows)
                _tdf['done_at'] = pd.to_datetime(_tdf['done_at'], errors='coerce')
                _tdf['scheduled_datetime'] = pd.to_datetime(_tdf['scheduled_datetime'])
                _tdf['tat_min'] = (_tdf['done_at'] - _tdf['scheduled_datetime']).dt.total_seconds() / 60
                tech_insights = run_tech_insights(_tdf)
    except Exception as _ie:
        print(f"Tech insights error: {_ie}")

    try:
        _signing_df = None
        if ts_rows:
            _signing_df = pd.DataFrame(ts_rows, columns=['radiologist', 'ts'])
        rad_insights = run_rad_insights(rad_cards, _signing_df)
    except Exception as _ie:
        print(f"Rad insights error: {_ie}")

    result = ({
        "summary": {
            "total": len(df), "global_util": f"{(sum(r['avg'] * r.get('total_cap', 1) for r in matrix_rows) / sum(r.get('total_cap', 1) for r in matrix_rows) if matrix_rows and sum(r.get('total_cap', 1) for r in matrix_rows) > 0 else 0):.1f}%",
            "er_wait": f"{df[(df['patient_class'].str.contains('ER|Emergency', case=False, na=False)) & (df['total_tat_min'] > 0)]['total_tat_min'].mean():.1f}m" if 'patient_class' in df.columns and (df['patient_class'].str.contains('ER|Emergency', case=False, na=False) & (df['total_tat_min'] > 0)).any() else "0m",
            "high_stress_count": high_stress, "low_util_count": under_utilized,
            "work_hours": round(total_active_mins / 60, 1), "total_rvu": round(df['rvu'].sum(), 1),
            "tat_median": tat_median, "tat_p25": tat_p25, "tat_p75": tat_p75,
        },
        "matrix": matrix_rows, 
        "class_tat": df[df['total_tat_min'] > 0].groupby('patient_class')['total_tat_min'].mean().round(1).to_dict() if 'patient_class' in df.columns else {},
        "rad_cards": rad_cards,
        "modality_split": [{"name": k, "value": int(v)} for k, v in df['modality'].value_counts().items()] if 'modality' in df.columns else [], 
        "hourly_patterns": (lambda: {
            str(r[0]): int(r[1])
            for r in db.session.execute(text(f"""
                SELECT EXTRACT(HOUR FROM o.scheduled_datetime)::int AS hr, COUNT(*) AS cnt
                FROM etl_orders o
                JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
                {"LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))" if _sec_needs_mod_join else ""}
                WHERE s.study_date BETWEEN :start AND :end
                  AND o.scheduled_datetime IS NOT NULL
                  {_sec_filters}
                GROUP BY 1 ORDER BY 1
            """), params).fetchall()
        })(),
        "correlation": (lambda: (
            lambda raw: raw[_iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])][['proc_duration','total_tat_min']].values.tolist()
        )(df[(df['proc_duration']>0)&(df['total_tat_min']>0)]) if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else [])(),
        "pearson_r": (lambda: (
            lambda clean: round(clean['proc_duration'].corr(clean['total_tat_min']), 3)
            if len(clean) > 2 else None
        )((lambda raw: raw[_iqr_filter(raw['proc_duration']) & _iqr_filter(raw['total_tat_min'])])(
            df[(df['proc_duration']>0)&(df['total_tat_min']>0)]
        )) if 'proc_duration' in df.columns and 'total_tat_min' in df.columns else None)(),
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
        "shift_patterns":  shift_patterns,
        "tech_data":       tech_data,
        "tech_insights":   tech_insights,
        "rad_insights":    rad_insights,
    }, start, end)
    cache_put(25, form_data, result)
    return result

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    # Filter options are loaded asynchronously via /api/filter-options after
    # page render — do NOT query here, as DISTINCT on etl_didb_studies blocks
    # the entire page load.
    classes = locations = modalities = aetitles = []
    
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
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    data, _, _ = get_gold_standard_data(request.form)
    if not data: return "Error", 400
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['raw_df'].drop(columns=['study_date_dt'], errors='ignore').to_excel(writer, index=False, sheet_name='RawData')
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"RAYD_PRO_Export_{date.today()}.xlsx")

@report_25_bp.route("/report/25/export-technician", methods=["POST"])
@login_required
def export_technician_25():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    data, _, _ = get_gold_standard_data(request.form)
    if not data:
        return "Error", 400
    tech = data.get('tech_data', {})

    sections = []
    if tech.get('summary'):
        sections.append(pd.DataFrame([tech['summary']]).assign(_section='Summary'))
    if tech.get('by_technician'):
        sections.append(pd.DataFrame(tech['by_technician']).assign(_section='By Technician'))
    if tech.get('by_modality'):
        sections.append(pd.DataFrame(tech['by_modality']).assign(_section='By Modality'))
    if tech.get('flagged'):
        df_f = pd.DataFrame(tech['flagged'])
        df_f['flags'] = df_f['flags'].apply(lambda x: ', '.join(x))
        sections.append(df_f.assign(_section='Flagged'))
    if tech.get('never_done'):
        sections.append(pd.DataFrame(tech['never_done']).assign(_section='Never Done'))

    if not sections:
        return "No data", 400

    output = io.BytesIO()
    pd.concat(sections, ignore_index=True).to_csv(output, index=False)
    output.seek(0)
    return send_file(output, mimetype='text/csv', as_attachment=True,
                     download_name=f"RAYD_Tech_TAT_{date.today()}.csv")

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

@report_25_bp.route("/report/25/patient-journey")
@login_required
def patient_journey_api():
    from flask import jsonify as _json
    from datetime import datetime as _dt

    pid       = (request.args.get('pid', '') or '').strip()
    accession = (request.args.get('accession', '') or '').strip()

    if not pid and not accession:
        return _json({'studies': [], 'error': 'Provide patient ID or accession number'})

    try:
        accessions = set()

        # ── Find accessions by accession number ───────────────────────────────
        if accession:
            rows = db.session.execute(text(
                "SELECT DISTINCT accession_number FROM etl_didb_studies "
                "WHERE accession_number ILIKE :acc LIMIT 15"
            ), {'acc': f'%{accession}%'}).fetchall()
            accessions.update(r[0] for r in rows if r[0])

        # ── Find accessions by patient ID (hl7_orders, then etl_didb_studies) ─
        if pid:
            try:
                rows = db.session.execute(text(
                    "SELECT DISTINCT accession_number FROM hl7_orders "
                    "WHERE patient_id ILIKE :pid AND accession_number IS NOT NULL LIMIT 20"
                ), {'pid': f'%{pid}%'}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()
            try:
                rows = db.session.execute(text(
                    "SELECT DISTINCT accession_number FROM etl_didb_studies "
                    "WHERE patient_id ILIKE :pid LIMIT 20"
                ), {'pid': f'%{pid}%'}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()

        if not accessions:
            return _json({'studies': [], 'error': None, 'message': 'No matching studies found'})

        accn_list = list(accessions)[:15]

        # ── Batch fetch studies (1 query for all accessions) ─────────────────
        study_rows = db.session.execute(text("""
            SELECT DISTINCT ON (s.accession_number)
                s.accession_number,
                s.study_date::text                                                AS study_date,
                s.study_time,
                COALESCE(s.study_description, '')                                 AS study_description,
                COALESCE(m.modality, s.study_modality, 'Unknown')                 AS modality,
                COALESCE(s.patient_class, '')                                     AS patient_class,
                COALESCE(s.patient_location, '')                                  AS patient_location,
                s.insert_time,
                s.rep_prelim_timestamp,
                s.rep_transcribed_timestamp,
                s.rep_final_timestamp,
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.signing_physician_first_name,''), ' ',
                    COALESCE(s.signing_physician_last_name,'')
                )), '')                                                            AS radiologist,
                s.rep_final_signed_by
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.accession_number = ANY(:accns)
        """), {'accns': accn_list}).mappings().fetchall()
        studies_map = {r['accession_number']: dict(r) for r in study_rows}

        # ── Batch fetch hl7_orders (1 query for all accessions) ──────────────
        orders_map = {}  # accn -> list of order dicts
        try:
            order_rows = db.session.execute(text("""
                SELECT
                    accession_number,
                    received_at,
                    scheduled_datetime,
                    done_at,
                    done_by,
                    order_status,
                    COALESCE(procedure_text, procedure_code, '') AS procedure,
                    modality   AS order_modality,
                    patient_id AS order_pid
                FROM hl7_orders
                WHERE accession_number = ANY(:accns)
                ORDER BY accession_number, received_at NULLS LAST
            """), {'accns': accn_list}).mappings().fetchall()
            for r in order_rows:
                orders_map.setdefault(r['accession_number'], []).append(dict(r))
        except Exception:
            db.session.rollback()

        # ── Build timeline per accession (pure Python, no more DB calls) ─────
        def _ev(events, ts, ev_type, label, detail='', by=None):
            if ts is None:
                return
            events.append({
                'ts':     str(ts),
                'type':   ev_type,
                'label':  label,
                'detail': detail,
                'by':     str(by) if by else None,
            })

        results = []
        for accn in accn_list:
            study  = studies_map.get(accn)
            if not study:
                continue
            orders = orders_map.get(accn, [])

            events  = []
            pid_val = None
            for o in orders:
                pid_val = pid_val or o.get('order_pid')
                _ev(events, o.get('received_at'),       'order_received', 'Order Received',
                    o.get('procedure') or '')
                _ev(events, o.get('scheduled_datetime'), 'scheduled',      'Exam Scheduled',
                    f"Status: {o.get('order_status') or '?'}")
                _ev(events, o.get('done_at'),            'tech_done',      'Exam Completed by Tech',
                    f"Modality: {o.get('order_modality') or ''}",
                    o.get('done_by'))

            _ev(events, study.get('insert_time'),               'pacs_in',     'Arrived in PACS',
                f"Modality: {study.get('modality','')}")
            _ev(events, study.get('rep_prelim_timestamp'),      'prelim',      'Preliminary Report', '')
            _ev(events, study.get('rep_transcribed_timestamp'), 'transcribed', 'Transcribed', '')
            _ev(events, study.get('rep_final_timestamp'),       'final',       'Final Report Signed',
                '', study.get('radiologist') or study.get('rep_final_signed_by'))

            events.sort(key=lambda x: x['ts'])
            for i in range(1, len(events)):
                try:
                    t1 = _dt.fromisoformat(str(events[i-1]['ts']).replace('Z', '').split('.')[0])
                    t2 = _dt.fromisoformat(str(events[i]['ts']).replace('Z', '').split('.')[0])
                    events[i]['gap_min'] = round((t2 - t1).total_seconds() / 60)
                except Exception:
                    events[i]['gap_min'] = None

            results.append({
                'accession':        accn,
                'study_date':       study.get('study_date', ''),
                'modality':         study.get('modality', ''),
                'patient_id':       pid_val or '',
                'patient_class':    study.get('patient_class', ''),
                'patient_location': study.get('patient_location', ''),
                'description':      study.get('study_description', ''),
                'events':           events,
            })

        results.sort(key=lambda x: x['study_date'], reverse=True)
        return _json({'studies': results, 'error': None})

    except Exception as e:
        db.session.rollback()
        return _json({'studies': [], 'error': str(e)}), 500


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(25, report_25_bp, report_25, export_report_25)
