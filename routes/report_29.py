import pandas as pd
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date

report_29_bp = Blueprint("report_29", __name__)


def get_report_data(form_data):
    go_live_result = get_etl_cutoff_date()
    default_start  = str(go_live_result) if go_live_result else "2025-01-01"
    default_end    = date.today().strftime('%Y-%m-%d')

    start = form_data.get("start_date") or default_start
    end   = form_data.get("end_date")   or default_end

    template_sql = db.session.execute(
        text("SELECT report_sql_query FROM report_template WHERE report_id = 29")
    ).scalar()

    if not template_sql:
        return pd.DataFrame(), start, end

    res = db.session.execute(text(template_sql), {"start": start, "end": end}).mappings().all()
    df  = pd.DataFrame(res)

    if not df.empty:
        df["total_gb"]    = pd.to_numeric(df["total_gb"],    errors="coerce").fillna(0)
        df["study_count"] = pd.to_numeric(df["study_count"], errors="coerce").fillna(0)
        # Exclude rows with 0 studies — replace(0,1) would produce misleading averages
        df = df[df["study_count"] > 0].copy()
        df["avg_mb_per_study"] = (
            df["total_gb"] * 1024 / df["study_count"]
        ).round(2)

    return df, start, end


@report_29_bp.route("/report/29", methods=["GET", "POST"])
@login_required
def report_29():
    run_report        = False
    stats             = {"total_tb": 0}
    modality_bar_json = {}
    tech_bar_json     = {}
    table_data        = []
    alerts            = []

    _, display_start, display_end = get_report_data({})

    if request.method == "POST":
        run_report = True
        df, display_start, display_end = get_report_data(request.form)

        if not df.empty:
            stats["total_tb"] = round(df["total_gb"].sum() / 1024, 2)

            # 1. Modality Storage Distribution
            mod_df = (
                df.groupby("modality")["total_gb"]
                .sum().sort_values(ascending=False).reset_index()
            )
            modality_bar_json = {
                "labels": mod_df["modality"].tolist(),
                "datasets": [{
                    "label": "GB",
                    "data":  mod_df["total_gb"].round(2).tolist(),
                    "backgroundColor": "#38ada9"
                }]
            }

            # 2. Top AE titles by avg MB/study (IQR filter to exclude freak outliers)
            ae_agg = (
                df.groupby("storing_ae")[["total_gb", "study_count"]]
                .sum().reset_index()
            )
            ae_agg["avg_mb"] = (
                ae_agg["total_gb"] * 1024 / ae_agg["study_count"]
            ).round(1)
            ae_mb = ae_agg["avg_mb"]
            if len(ae_mb) > 4:
                q1, q3 = ae_mb.quantile(0.25), ae_mb.quantile(0.75)
                iqr = q3 - q1
                ae_agg_filtered = ae_agg[ae_mb <= q3 + 1.5 * iqr]
                tech_outliers_removed = int(len(ae_agg) - len(ae_agg_filtered))
            else:
                ae_agg_filtered = ae_agg
                tech_outliers_removed = 0
            ae_agg_filtered = ae_agg_filtered.sort_values("avg_mb", ascending=False).head(15)
            tech_bar_json = {
                "labels": ae_agg_filtered["storing_ae"].tolist(),
                "data":   ae_agg_filtered["avg_mb"].tolist(),
                "outliers_removed": tech_outliers_removed
            }

            # 3. Alerts — procedure codes averaging > 500 MB/study (IQR filter first)
            proc_agg = (
                df.groupby("procedure_code")[["total_gb", "study_count"]]
                .sum().reset_index()
            )
            proc_agg["avg_mb"] = (
                proc_agg["total_gb"] * 1024 / proc_agg["study_count"]
            ).round(1)
            proc_mb = proc_agg["avg_mb"]
            if len(proc_mb) > 4:
                q1p, q3p = proc_mb.quantile(0.25), proc_mb.quantile(0.75)
                iqrp = q3p - q1p
                proc_agg = proc_agg[proc_mb <= q3p + 1.5 * iqrp]
            for _, row in proc_agg[proc_agg["avg_mb"] > 500].sort_values("avg_mb", ascending=False).head(5).iterrows():
                alerts.append({
                    "type": "critical",
                    "msg":  f"Storage Hog: {row['procedure_code']} ({row['avg_mb']} MB/avg)"
                })

            # 4. Table — aggregate by procedure + modality + AE
            table_df = (
                df.groupby(["procedure_code", "modality", "storing_ae"])
                .agg(study_count=("study_count", "sum"), total_gb=("total_gb", "sum"))
                .reset_index()
            )
            table_df["avg_mb_per_study"] = (
                table_df["total_gb"] * 1024 / table_df["study_count"]
            ).round(2)
            table_df = table_df.rename(columns={"storing_ae": "performing_technician"})
            table_data = table_df.sort_values("total_gb", ascending=False).to_dict(orient="records")

    return render_template(
        "report_29.html",
        report_name       = "Infrastructure & Storage Audit",
        run_report        = run_report,
        display_start     = display_start,
        display_end       = display_end,
        stats             = stats,
        modality_bar_json = modality_bar_json,
        tech_bar_json     = tech_bar_json,
        table_data        = table_data,
        alerts            = alerts,
    )


@report_29_bp.route("/report/29/export", methods=["POST"])
@login_required
def export_report_29():
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    df, start, end = get_report_data(request.form)
    if df.empty:
        return "No data to export", 400
    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=Storage_Audit_{start}_to_{end}.csv"}
    )
