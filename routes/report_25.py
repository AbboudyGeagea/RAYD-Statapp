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
        pass

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
        pass

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
        pass

    # ── Technician TAT (from hl7_orders "Done" workflow) ────────────────
    tech_data = {'technicians': [], 'flagged': [], 'by_procedure': [], 'summary': {}}
    try:
        # Ensure done_at/done_by columns exist (added dynamically by live_feed dismiss)
        try:
            db.session.execute(text("""
                ALTER TABLE hl7_orders
                    ADD COLUMN IF NOT EXISTS done_at  TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS done_by  VARCHAR(100)
            """))
            db.session.commit()
        except Exception:
            db.session.rollback()

        # Build hl7-specific filters (hl7_orders uses ho. prefix, different columns)
        _ho_filters = ""
        if "modalities" in params:
            _ho_filters += " AND UPPER(TRIM(ho.modality)) IN :modalities"
        # Note: hl7_orders doesn't have patient_class/storing_ae/patient_location
        # so those filters are not applicable to technician TAT

        tech_rows = db.session.execute(text(f"""
            SELECT
                ho.accession_number,
                ho.patient_id,
                ho.procedure_code,
                ho.procedure_text,
                ho.modality,
                ho.done_by,
                ho.done_at,
                COALESCE(ho.scheduled_datetime, ho.received_at) AS start_time,
                ho.scheduled_datetime,
                ho.received_at,
                ROUND(EXTRACT(EPOCH FROM (
                    ho.done_at - COALESCE(ho.scheduled_datetime, ho.received_at)
                )) / 60.0, 1) AS actual_min,
                COALESCE(pm.duration_minutes, 15) AS expected_min
            FROM hl7_orders ho
            LEFT JOIN procedure_duration_map pm
                ON UPPER(TRIM(ho.procedure_code)) = UPPER(TRIM(pm.procedure_code))
            WHERE ho.order_status = 'CM'
              AND ho.done_at IS NOT NULL
              AND ho.done_by IS NOT NULL
              AND ho.done_at::date BETWEEN :start AND :end
              {_ho_filters}
            ORDER BY ho.done_at DESC
        """), params).mappings().fetchall()

        if tech_rows:
            tech_df = pd.DataFrame(tech_rows)
            tech_df['actual_min']   = pd.to_numeric(tech_df['actual_min'], errors='coerce').fillna(0)
            tech_df['expected_min'] = pd.to_numeric(tech_df['expected_min'], errors='coerce').fillna(15)

            # ── ER preemption detection ──────────────────────────────
            # For each completed exam, find ER/urgent cases that ran on the
            # same modality between scheduled_datetime and done_at.
            # Their total expected duration = ER delay (not the tech's fault).
            er_delay_map = {}  # accession -> {'er_delay_min': float, 'er_cases': int}
            try:
                er_rows = db.session.execute(text(f"""
                    WITH completed AS (
                        SELECT accession_number, modality,
                               COALESCE(scheduled_datetime, received_at) AS sched,
                               done_at
                        FROM hl7_orders
                        WHERE order_status = 'CM'
                          AND done_at IS NOT NULL
                          AND done_at::date BETWEEN :start AND :end
                          {"AND UPPER(TRIM(modality)) IN :modalities" if "modalities" in params else ""}
                    )
                    SELECT
                        c.accession_number,
                        COUNT(er.id) AS er_cases,
                        COALESCE(SUM(COALESCE(pm.duration_minutes, 15)), 0) AS er_delay_min
                    FROM completed c
                    JOIN hl7_orders er
                        ON UPPER(TRIM(er.modality)) = UPPER(TRIM(c.modality))
                       AND er.accession_number != c.accession_number
                       AND er.done_at IS NOT NULL
                       AND er.done_at > c.sched
                       AND er.done_at <= c.done_at
                    JOIN etl_didb_studies s
                        ON s.accession_number = er.accession_number
                       AND UPPER(COALESCE(s.patient_class, '')) IN ('ER', 'EMERGENCY', 'URGENT', 'U')
                    LEFT JOIN procedure_duration_map pm
                        ON UPPER(TRIM(er.procedure_code)) = UPPER(TRIM(pm.procedure_code))
                    GROUP BY c.accession_number
                """), params).fetchall()
                for acc, er_cases, er_delay in er_rows:
                    er_delay_map[acc] = {
                        'er_delay_min': float(er_delay),
                        'er_cases': int(er_cases),
                    }
            except Exception as _er_e:
                print(f"ER preemption detection error: {_er_e}")

            # Add ER delay columns
            tech_df['er_delay_min'] = tech_df['accession_number'].map(
                lambda a: er_delay_map.get(a, {}).get('er_delay_min', 0)
            )
            tech_df['er_cases'] = tech_df['accession_number'].map(
                lambda a: er_delay_map.get(a, {}).get('er_cases', 0)
            )
            tech_df['adjusted_min'] = (tech_df['actual_min'] - tech_df['er_delay_min']).clip(lower=0)

            # Flag uses ADJUSTED TAT — so ER delays don't penalise techs
            def _flag(row):
                if row['actual_min'] <= 0:
                    return 'invalid'
                adj = row['adjusted_min']
                ratio = adj / row['expected_min'] if row['expected_min'] > 0 else 1
                if row['er_delay_min'] > 0 and row['actual_min'] / row['expected_min'] > 2.0 and ratio <= 2.0:
                    return 'er_delayed'
                if ratio < 0.5:
                    return 'too_short'
                elif ratio > 2.0:
                    return 'too_long'
                return 'normal'

            tech_df['flag'] = tech_df.apply(_flag, axis=1)

            def _safe(val, default=0):
                """Convert pandas numeric to Python float, replacing NaN/inf."""
                v = float(val)
                return default if (v != v or v == float('inf') or v == float('-inf')) else round(v, 1)

            # ── Shift utilization per technician ──────────────────────
            # Per tech per day: span = first_start → last_done, work = sum(expected_min)
            tech_df['start_dt'] = pd.to_datetime(tech_df['start_time'], errors='coerce')
            tech_df['done_dt']  = pd.to_datetime(tech_df['done_at'], errors='coerce')
            tech_df['work_date'] = tech_df['done_dt'].dt.date

            tech_util_map = {}  # tech_name -> {span_min, work_min, util_pct, days}
            for tech_name, tg in tech_df[tech_df['done_dt'].notna() & tech_df['start_dt'].notna()].groupby('done_by'):
                day_spans = []
                day_works = []
                for _, day_df in tg.groupby('work_date'):
                    first_start = day_df['start_dt'].min()
                    last_done   = day_df['done_dt'].max()
                    span = (last_done - first_start).total_seconds() / 60.0
                    work = day_df['expected_min'].sum()
                    if span > 0:
                        day_spans.append(span)
                        day_works.append(float(work))
                total_span = sum(day_spans)
                total_work = sum(day_works)
                tech_util_map[tech_name] = {
                    'span_min': round(total_span, 1),
                    'work_min': round(total_work, 1),
                    'util_pct': round(total_work / total_span * 100, 1) if total_span > 0 else 0,
                    'idle_min': round(max(total_span - total_work, 0), 1),
                    'days': len(day_spans),
                }

            # Per-technician cards — use adjusted TAT for performance metrics
            for tech, tdf in tech_df.groupby('done_by'):
                valid = tdf[tdf['actual_min'] > 0]
                util_info = tech_util_map.get(tech, {})
                tech_data['technicians'].append({
                    'name': str(tech),
                    'total_exams': len(tdf),
                    'avg_min': _safe(valid['adjusted_min'].mean()) if len(valid) > 0 else 0,
                    'avg_raw_min': _safe(valid['actual_min'].mean()) if len(valid) > 0 else 0,
                    'median_min': _safe(valid['adjusted_min'].median()) if len(valid) > 0 else 0,
                    'total_er_delay': _safe(tdf['er_delay_min'].sum()),
                    'er_affected': int((tdf['er_delay_min'] > 0).sum()),
                    'too_short': int((tdf['flag'] == 'too_short').sum()),
                    'too_long': int((tdf['flag'] == 'too_long').sum()),
                    'er_delayed': int((tdf['flag'] == 'er_delayed').sum()),
                    'normal': int((tdf['flag'] == 'normal').sum()),
                    'util_pct': util_info.get('util_pct', 0),
                    'span_min': util_info.get('span_min', 0),
                    'work_min': util_info.get('work_min', 0),
                    'idle_min': util_info.get('idle_min', 0),
                    'active_days': util_info.get('days', 0),
                    'by_modality': [
                        {'mod': str(m), 'cnt': len(mdf), 'avg': _safe(mdf[mdf['actual_min'] > 0]['adjusted_min'].mean()) if (mdf['actual_min'] > 0).any() else 0}
                        for m, mdf in tdf.groupby('modality')
                    ],
                })

            # Flagged studies (abnormal + ER delayed)
            flagged = tech_df[tech_df['flag'].isin(['too_short', 'too_long', 'er_delayed'])].head(100)
            for _, r in flagged.iterrows():
                tech_data['flagged'].append({
                    'accession': str(r.get('accession_number') or ''),
                    'patient_id': str(r.get('patient_id') or ''),
                    'procedure': str(r.get('procedure_text') or r.get('procedure_code') or ''),
                    'modality': str(r.get('modality') or ''),
                    'technician': str(r.get('done_by') or ''),
                    'actual_min': _safe(r['actual_min']),
                    'adjusted_min': _safe(r['adjusted_min']),
                    'expected_min': int(r['expected_min']),
                    'er_delay_min': _safe(r['er_delay_min']),
                    'er_cases': int(r['er_cases']),
                    'flag': str(r['flag']),
                    'done_at': str(r['done_at'])[:16] if r.get('done_at') is not None else '',
                })

            # By procedure code — avg actual vs expected (uses adjusted)
            for proc, pdf in tech_df[tech_df['actual_min'] > 0].groupby('procedure_code'):
                tech_data['by_procedure'].append({
                    'code': str(proc),
                    'text': str(pdf.iloc[0].get('procedure_text') or proc),
                    'count': len(pdf),
                    'avg_actual': _safe(pdf['adjusted_min'].mean()),
                    'avg_raw': _safe(pdf['actual_min'].mean()),
                    'expected': int(pdf['expected_min'].mode().iloc[0]) if not pdf['expected_min'].mode().empty else int(pdf.iloc[0]['expected_min']),
                    'modality': str(pdf.iloc[0].get('modality') or ''),
                    'er_affected': int((pdf['er_delay_min'] > 0).sum()),
                })

            # ── Technician ↔ Modality rotation data ──────────────────
            # Sankey: technician → modality (weighted by exam count)
            sankey_links = []
            for tech_name, tg in tech_df.groupby('done_by'):
                for mod, mdf in tg.groupby('modality'):
                    sankey_links.append({
                        'source': str(tech_name),
                        'target': str(mod),
                        'value': len(mdf),
                    })
            sankey_nodes = (
                [{'name': str(t)} for t in tech_df['done_by'].unique()] +
                [{'name': str(m)} for m in tech_df['modality'].unique()]
            )
            tech_data['sankey'] = {'nodes': sankey_nodes, 'links': sankey_links}

            # Transitions: consecutive exams by same tech, ordered by time
            # Shows how techs rotate between modalities within a day
            transitions = {}  # (from_mod, to_mod) -> count
            sorted_df = tech_df[tech_df['done_dt'].notna()].sort_values(['done_by', 'work_date', 'done_dt'])
            for _, tg in sorted_df.groupby(['done_by', 'work_date']):
                mods = tg['modality'].tolist()
                for i in range(len(mods) - 1):
                    if str(mods[i]) != str(mods[i+1]):
                        key = (str(mods[i]), str(mods[i+1]))
                        transitions[key] = transitions.get(key, 0) + 1
            tech_data['transitions'] = [
                {'from': k[0], 'to': k[1], 'count': v}
                for k, v in sorted(transitions.items(), key=lambda x: -x[1])
            ]

            # Summary stats
            valid_all = tech_df[tech_df['actual_min'] > 0]
            tech_data['summary'] = {
                'total_completed': int(len(tech_df)),
                'total_technicians': int(tech_df['done_by'].nunique()),
                'avg_tat': _safe(valid_all['adjusted_min'].mean()) if len(valid_all) > 0 else 0,
                'avg_raw_tat': _safe(valid_all['actual_min'].mean()) if len(valid_all) > 0 else 0,
                'median_tat': _safe(valid_all['adjusted_min'].median()) if len(valid_all) > 0 else 0,
                'flagged_count': int((tech_df['flag'].isin(['too_short', 'too_long', 'er_delayed'])).sum()),
                'too_short_count': int((tech_df['flag'] == 'too_short').sum()),
                'too_long_count': int((tech_df['flag'] == 'too_long').sum()),
                'er_delayed_count': int((tech_df['flag'] == 'er_delayed').sum()),
                'total_er_delay_min': _safe(tech_df['er_delay_min'].sum()),
                'er_affected_exams': int((tech_df['er_delay_min'] > 0).sum()),
                'avg_util_pct': round(sum(v['work_min'] for v in tech_util_map.values()) / max(sum(v['span_min'] for v in tech_util_map.values()), 1) * 100, 1) if tech_util_map else 0,
                'total_span_hrs': round(sum(v['span_min'] for v in tech_util_map.values()) / 60, 1) if tech_util_map else 0,
                'total_work_hrs': round(sum(v['work_min'] for v in tech_util_map.values()) / 60, 1) if tech_util_map else 0,
                'total_idle_hrs': round(sum(v['idle_min'] for v in tech_util_map.values()) / 60, 1) if tech_util_map else 0,
            }

    except Exception as _e:
        db.session.rollback()
        print(f"Technician TAT error: {_e}")

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
        "hourly_patterns": {i: int(v) for i, v in pd.to_datetime(df['scheduled_datetime']).dt.hour.value_counts().sort_index().items()} if 'scheduled_datetime' in df.columns else {}, 
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
