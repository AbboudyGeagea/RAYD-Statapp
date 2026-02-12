import json
import pandas as pd
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_go_live_date

report_25_bp = Blueprint("report_25", __name__)

def format_duration(minutes):
    """Helper to convert minutes into a readable 'X days Y hours' format."""
    if pd.isna(minutes) or minutes <= 0:
        return "0 min"
    
    td = timedelta(minutes=float(minutes))
    days = td.days
    hours = td.seconds // 3600
    mins = (td.seconds // 60) % 60
    
    parts = []
    if days > 0: parts.append(f"{days}d")
    if hours > 0: parts.append(f"{hours}h")
    if days == 0 and hours == 0: parts.append(f"{mins}m")
    return " ".join(parts)

def get_dynamic_report_data(form_data):
    go_live = get_go_live_date() or date(2024, 1, 1)
    start = form_data.get("start_date") or go_live.strftime("%Y-%m-%d")
    end = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")
    
    sql = text("""
        SELECT 
            COALESCE(m.modality, 'UKN') as modality,
            COALESCE(s.rep_final_signed_by, 'Unsigned') as radiologist,
            s.procedure_code,
            s.rep_final_timestamp,
            s.study_date,
            s.accession_number,
            EXTRACT(EPOCH FROM (COALESCE(s.rep_final_timestamp, NOW()) - s.study_date))/60 as tat_minutes
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle
        WHERE s.study_date BETWEEN :start AND :end
          AND UPPER(s.study_status) IN ('FINAL', 'UNREAD', 'DICTATED', 'APPROVED', 'TRANSCRIBED')
    """)
    
    res = db.session.execute(sql, {"start": start, "end": end}).mappings().all()
    df = pd.DataFrame(res)
    return df, start, end

@report_25_bp.route("/report/25", methods=["GET", "POST"])
@login_required
def report_25():
    run_report = request.method == "POST"
    df, start_date, end_date = get_dynamic_report_data(request.form)
    
    data = {"modality_chart": {}, "rad_cards": [], "summary": {}}

    if run_report and not df.empty:
        try:
            # Numeric cleaning
            df['tat_minutes'] = pd.to_numeric(df['tat_minutes'], errors='coerce').fillna(0)
            df['tat_minutes'] = df['tat_minutes'].clip(lower=0)

            # NEW: Global Max vs Low TAT Summary
            data["summary"] = {
                "max": format_duration(df['tat_minutes'].max()),
                "min": format_duration(df['tat_minutes'].min()),
                "avg": format_duration(df['tat_minutes'].mean())
            }

            # 1. Modality Avg TAT Chart
            mod_grp = df.groupby('modality')['tat_minutes'].mean().round(1)
            data["modality_chart"] = {
                "labels": mod_grp.index.tolist(),
                "values": mod_grp.tolist()
            }

            # 2. Radiologist Cards
            for rad, rad_df in df.groupby('radiologist'):
                card = {
                    "name": rad,
                    "overall_tat": format_duration(rad_df['tat_minutes'].mean()),
                    "modalities": []
                }
                for mod, mod_df in rad_df.groupby('modality'):
                    mod_item = {
                        "name": mod, "tat": format_duration(mod_df['tat_minutes'].mean()), "procedures": []
                    }
                    proc_grp = mod_df.groupby('procedure_code')['tat_minutes'].mean()
                    for proc, proc_tat in proc_grp.items():
                        mod_item["procedures"].append({"code": proc, "tat": format_duration(proc_tat)})
                    card["modalities"].append(mod_item)
                data["rad_cards"].append(card)
        except Exception as e:
            print(f"Logic Error: {e}")

    return render_template(
        "report_25.html",
        run_report=run_report,
        display_start=start_date,
        display_end=end_date,
        data=data
    )

@report_25_bp.route("/report/25/export", methods=["POST"])
@login_required
def export_report_25():
    df, _, _ = get_dynamic_report_data(request.form)
    return Response(
        df.to_csv(index=False), 
        mimetype="text/csv", 
        headers={"Content-disposition": f"attachment; filename=TAT_DETAILED_{date.today()}.csv"}
    )
