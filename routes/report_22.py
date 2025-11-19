# report_22.py
import csv, io
from datetime import date
from flask import Blueprint, render_template, request, abort, Response
from flask_login import login_required
from sqlalchemy import text, bindparam

from db import db, ReportTemplate, get_go_live_date

report_22_bp = Blueprint("report_22_bp", __name__)


def as_bool(v):
    return str(v or "").lower() in ("1", "true", "yes", "on", "y")


def like_value(op, raw):
    if op == "equals":
        return raw
    if op == "contains":
        return f"%{raw}%"
    if op == "ends":
        return f"%{raw}"
    return f"{raw}%"


@report_22_bp.route("/report/22", methods=["GET", "POST"])
@login_required
def display_report():

    report = db.session.get(ReportTemplate, 22)
    if not report:
        abort(404)

    dropdown_data = {
        "Modality": [r[0] for r in db.session.execute(
            text("SELECT DISTINCT modality FROM aetitle_modality_map WHERE modality IS NOT NULL ORDER BY 1")
        )],
    }

    go_live = get_go_live_date()
    display_start = request.form.get("start_date") or str(go_live)
    display_end = request.form.get("end_date") or str(date.today())

    run_report = False
    rows = []
    total_count = 0
    chart_json = {"labels": [], "counts": [], "type": "doughnut"}

    if request.method == "POST":
        run_report = True

        base_sql = report.report_sql_query.strip().rstrip(";")

        where = ["study_date BETWEEN :start_date AND :end_date"]
        params = {"start_date": display_start, "end_date": display_end}

        final_sql = f"""
            SELECT *
            FROM ({base_sql}) base_q
            WHERE {' AND '.join(where)}
        """

        total_count = db.session.execute(
            text(f"SELECT COUNT(*) FROM ({final_sql}) c"),
            params
        ).scalar() or 0

        rows = db.session.execute(
            text(f"{final_sql} LIMIT 15"),
            params
        ).mappings().all()

    return render_template(
        "report_22.html",
        report=report,
        dropdown_data=dropdown_data,
        display_start=display_start,
        display_end=display_end,
        rows=rows,
        total_count=total_count,
        run_report=run_report,
        chart_json=chart_json
    )

