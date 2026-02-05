from datetime import date, datetime, timedelta
import pandas as pd
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, ReportTemplate, get_go_live_date

report_22_bp = Blueprint("report_22", __name__)

# ---------------------------------------------------------
# Date logic
# ---------------------------------------------------------
def resolve_preset_dates(preset, today):
    if preset == "last_7_days":
        return today - timedelta(days=7), today
    if preset == "last_business_week":
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=4)
        return start, end
    if preset == "last_business_month":
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        start = last_prev_month.replace(day=1)
        return start, last_prev_month
    if preset == "saturdays_of_month":
        return today.replace(day=1), today
    if preset == "year_to_date":
        return date(today.year, 1, 1), today
    if preset == "last_year":
        return date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
    raise ValueError("Unknown preset")


def resolve_dates(form):
    today = date.today()
    go_live = get_go_live_date() or date(2000, 1, 1)
    mode = form.get("date_mode")

    if mode == "preset":
        return resolve_preset_dates(form.get("date_preset"), today)

    if mode == "manual":
        raw_start = form.get("start_date")
        raw_end = form.get("end_date")
        start = datetime.strptime(raw_start, "%Y-%m-%d").date() if raw_start else go_live
        end = datetime.strptime(raw_end, "%Y-%m-%d").date() if raw_end else today
        if (end - start).days > 730:
            raise ValueError("Manual date range exceeds 730 days")
        return start, end

    return go_live, today


# ---------------------------------------------------------
# SQL builder (only AND clauses)
# ---------------------------------------------------------
def build_sql(base_sql, start_date, end_date, form):
    sql = base_sql + "\nAND study_date BETWEEN :start_date AND :end_date"
    params = {"start_date": start_date, "end_date": end_date}

    if form.getlist("modality") and form.get("enable_modality"):
        sql += " AND modality IN :modalities"
        params["modalities"] = tuple(form.getlist("modality"))

    if form.getlist("patient_class") and form.get("enable_patient_class"):
        sql += " AND patient_class IN :patient_classes"
        params["patient_classes"] = tuple(form.getlist("patient_class"))

    if form.getlist("study_status") and form.get("enable_study_status"):
        sql += " AND study_status IN :study_status"
        params["study_status"] = tuple(form.getlist("study_status"))

    # Procedure code filter
    if form.getlist("procedure_code") and form.get("enable_procedure"):
        sql += " AND procedure_code IN :procedure_codes"
        params["procedure_codes"] = tuple(form.getlist("procedure_code"))

    # Referring physician filter
    if form.getlist("ref_phys") and form.get("enable_ref_phys"):
        sql += " AND (referring_physician_first_name || ' ' || coalesce(referring_physician_mid_name,'') || ' ' || coalesce(referring_physician_last_name,'')) IN :ref_phys"
        params["ref_phys"] = tuple(form.getlist("ref_phys"))

    return sql, params


# ---------------------------------------------------------
# MAIN REPORT
# ---------------------------------------------------------
@report_22_bp.route("/report/22", methods=["GET", "POST"])
@login_required
def report_22():
    report = db.session.get(ReportTemplate, 22)
    base_sql = report.report_sql_query.strip().rstrip(";")

    # Filters
    modalities = [r[0] for r in db.session.execute(text("SELECT DISTINCT modality FROM aetitle_modality_map ORDER BY 1"))]
    patient_classes = [r[0] for r in db.session.execute(text("SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL"))]
    study_status_list = [r[0] for r in db.session.execute(text("SELECT DISTINCT study_status FROM etl_didb_studies WHERE study_status IS NOT NULL"))]

    # Procedure code / referring physician for JS search
    procedure_codes = [r[0] for r in db.session.execute(text("SELECT DISTINCT procedure_code FROM etl_didb_studies WHERE procedure_code IS NOT NULL"))]
    ref_phys_list = [f"{r[0]} {r[1] or ''} {r[2] or ''}".strip() for r in db.session.execute(
        text("SELECT DISTINCT referring_physician_first_name, referring_physician_mid_name, referring_physician_last_name FROM etl_didb_studies"))]

    run_report = False
    rows = []
    chart_json = {}
    total_count = 0

    if request.method == "POST":
        run_report = True
        start_date, end_date = resolve_dates(request.form)
        sql, params = build_sql(base_sql, start_date, end_date, request.form)

        # Execute query safely
        result = db.session.execute(text(sql), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if not df.empty:
            # Null-safe referring physician concatenation
            df["ref_phys"] = (
                df["referring_physician_first_name"].fillna("") + " " +
                df["referring_physician_mid_name"].fillna("") + " " +
                df["referring_physician_last_name"].fillna("")
            ).str.strip()

            # Monthly studies
            df["study_month"] = pd.to_datetime(df["study_date"]).dt.to_period("M").astype(str)
            monthly = df.groupby("study_month").size()

            # Comparison window
            delta = (end_date - start_date).days
            prev_start = start_date - timedelta(days=delta)
            prev_end = start_date

            prev_result = db.session.execute(text(sql), {**params, "start_date": prev_start, "end_date": prev_end})
            prev_df = pd.DataFrame(prev_result.fetchall(), columns=prev_result.keys())

            # Null-safe ref_phys in prev_df
            if not prev_df.empty:
                prev_df["ref_phys"] = (
                    prev_df["referring_physician_first_name"].fillna("") + " " +
                    prev_df["referring_physician_mid_name"].fillna("") + " " +
                    prev_df["referring_physician_last_name"].fillna("")
                ).str.strip()

            # Metrics
            mod_now = df["modality"].value_counts()
            mod_prev = prev_df["modality"].value_counts() if not prev_df.empty else pd.Series(dtype=int)
            pc_now = df["patient_class"].value_counts()
            pc_prev = prev_df["patient_class"].value_counts() if not prev_df.empty else pd.Series(dtype=int)
            proc_mod = df.groupby(["procedure_code", "modality"]).size().sort_values(ascending=False).head(10)

            chart_json = {
                "monthly": {"labels": monthly.index.tolist(), "data": monthly.tolist()},
                "modality_compare": {"current": mod_now.to_dict(), "previous": mod_prev.to_dict()},
                "patient_class": {"current": pc_now.to_dict(), "previous": pc_prev.to_dict()},
                "top_proc_mod": {"labels": [f"{i[0]} ({i[1]})" for i in proc_mod.index], "data": proc_mod.tolist()}
            }

            rows = df.to_dict(orient="records")
            total_count = len(df)

    return render_template(
        "report_22.html",
        report_name="Report 22",
        modalities=modalities,
        patient_classes=patient_classes,
        study_status_list=study_status_list,
        procedure_codes=procedure_codes,
        ref_phys_list=ref_phys_list,
        run_report=run_report,
        rows=rows,
        chart_json=chart_json,
        total_count=total_count,
        display_start=str(date.today().replace(day=1)),
        display_end=str(date.today())
    )


# ---------------------------------------------------------
# EXPORT CSV
# ---------------------------------------------------------
@report_22_bp.route("/report/22/export", methods=["POST"])
@login_required
def export_report_22():
    report = db.session.get(ReportTemplate, 22)
    base_sql = report.report_sql_query.strip().rstrip(";")

    start_date, end_date = resolve_dates(request.form)
    sql, params = build_sql(base_sql, start_date, end_date, request.form)

    # Execute query safely
    result = db.session.execute(text(sql), params)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

    def generate():
        yield ",".join(df.columns) + "\n"
        for _, row in df.iterrows():
            yield ",".join(str(v).replace(",", " ") for v in row) + "\n"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=report_22.csv"}
    )

