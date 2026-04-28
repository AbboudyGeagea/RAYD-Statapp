import json
import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import text
from db import db

report_25_bp = Blueprint("report_25", __name__)

def resolve_dates(form):
    """Utility to handle date range selection with defaults."""
    today = date.today()
    start_str = form.get("start_date")
    end_str = form.get("end_date")
    
    start = datetime.strptime(start_str, "%Y-%m-%d").date() if start_str else (today - timedelta(days=30))
    end = datetime.strptime(end_str, "%Y-%m-%d").date() if end_str else today
    return start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    run_report = request.method == "POST"
    view_mode = request.form.get("view_mode", "business")
    start_date, end_date = resolve_dates(request.form)
    
    # Denominator factor for utilization (Total available minutes over the period)
    num_days = (end_date - start_date).days + 1
    
    data = {}
    outliers = []

    if run_report:
        # FINAL SQL: Using etl_patient_view and inclusive status list for testing/production
        sql = text("""
            SELECT 
                p.fallback_id as patient_id,
                s.accession_number,
                s.procedure_code,
                s.storing_ae,
                COALESCE(m.modality, 'UKN') as modality,
                COALESCE(m.daily_capacity_minutes, 480) as daily_cap,
                COALESCE(pdm.duration_minutes, 0) as duration,
                COALESCE(pdm.rvu_value, 0.0) as rvu,
                s.study_date,
                -- Patient Waiting Time: PACS Arrival vs Scheduled
                EXTRACT(EPOCH FROM (s.insert_time - o.scheduled_datetime))/60 as wait_time,
                -- Clinical TAT: Exam Start vs Final Signature (Falls back to NOW() for unread)
                EXTRACT(EPOCH FROM (COALESCE(s.rep_final_timestamp, NOW()) - s.study_date))/60 as tat_clinical,
                COALESCE(s.rep_final_signed_by, 'Unsigned') as radiologist
            FROM etl_didb_studies s
            -- Joins with explicit casting for Postgres type compatibility
            LEFT JOIN etl_orders o ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
            LEFT JOIN etl_patient_view p ON s.patient_db_uid::TEXT = p.patient_db_uid::TEXT
            LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
            LEFT JOIN procedure_duration_map pdm ON s.procedure_code = pdm.procedure_code
            WHERE s.study_date BETWEEN :start AND :end
            -- Inclusive status based on your sanity check results
            AND UPPER(s.study_status) IN ('FINAL', 'SIGNED', 'F', 'UNREAD', 'DICTATED', 'APPROVED')
        """)
        
        try:
            results = db.session.execute(sql, {"start": start_date, "end": end_date}).fetchall()
            raw_df = pd.DataFrame(results)

            if not raw_df.empty:
                # 1. Clean Data
                # Fill NAs to prevent math errors and clip negative waiting
                raw_df['wait_time'] = raw_df['wait_time'].fillna(0).clip(lower=0)
                raw_df['tat_clinical'] = raw_df['tat_clinical'].fillna(0)
                
                # Identify clinical TAT outliers (> 24 hours)
                threshold = 1440
                is_outlier = raw_df['tat_clinical'] > threshold
                outliers = raw_df[is_outlier][['accession_number', 'patient_id', 'tat_clinical']].to_dict('records')
                
                # Primary dataset for charts
                df_clean = raw_df[~is_outlier].copy()

                # 2. Shift Logic Filtering
                df_clean['hour'] = pd.to_datetime(df_clean['study_date']).dt.hour
                if view_mode == "business":
                    df = df_clean[(df_clean['hour'] >= 7) & (df_clean['hour'] < 18)]
                else:
                    df = df_clean[(df_clean['hour'] < 7) | (df_clean['hour'] >= 18)]

                if not df.empty:
                    # 3. Aggregations
                    mod_grp = df.groupby('modality').agg({
                        'duration': 'sum',
                        'daily_cap': 'first',
                        'rvu': 'sum',
                        'wait_time': 'mean',
                        'tat_clinical': 'mean'
                    })
                    
                    # Utilization Calculation
                    mod_grp['util_pct'] = (mod_grp['duration'] / (mod_grp['daily_cap'] * num_days)) * 100

                    # 4. Radiologist Performance Heatmap
                    heatmap_pt = df.pivot_table(
                        index='radiologist', 
                        columns='modality', 
                        values='tat_clinical', 
                        aggfunc='mean'
                    ).fillna(0).round(1)
                    
                    hn_data = [
                        [c_idx, r_idx, val] 
                        for r_idx, (name, row) in enumerate(heatmap_pt.iterrows()) 
                        for c_idx, val in enumerate(row)
                    ]

                    # 5. Pack data for frontend
                    data = {
                        "modality": {
                            "labels": mod_grp.index.tolist(),
                            "utilization": mod_grp['util_pct'].round(1).tolist(),
                            "waiting": mod_grp['wait_time'].round(1).tolist(),
                            "rvus": mod_grp['rvu'].round(1).tolist()
                        },
                        "heatmap": {
                            "rads": heatmap_pt.index.tolist(),
                            "mods": heatmap_pt.columns.tolist(),
                            "data": hn_data
                        }
                    }
        except Exception as e:
            print(f"Error executing Report 25: {e}")
            db.session.rollback()

    return render_template(
        "report_25.html",
        run_report=run_report,
        view_mode=view_mode,
        display_start=str(start_date),
        display_end=str(end_date),
        data=data,
        outliers=outliers
    )
@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    df, _, _ = get_dynamic_report_data(request.form)
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename=TAT_25_{date.today()}.csv"})
