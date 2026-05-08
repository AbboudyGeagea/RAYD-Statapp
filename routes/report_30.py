"""
Report 30 — Patient CD / DVD Distribution
Queries cd_print_log directly. No report_template dependency.
"""
import json
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date

report_30_bp = Blueprint("report_30", __name__)

_SR = "AND COALESCE(study_modality, '') != 'SR'"


def _date_range(form_data):
    go_live = get_etl_cutoff_date()
    default_start = str(go_live) if go_live else "2025-01-01"
    default_end   = date.today().strftime('%Y-%m-%d')
    return (
        form_data.get("start_date") or default_start,
        form_data.get("end_date")   or default_end,
    )


def get_report_data(form_data):
    start, end = _date_range(form_data)
    p = {"start": start, "end": end}

    # ── KPIs ──────────────────────────────────────────────────────────
    r = db.session.execute(text(f"""
        SELECT
            COUNT(*)                           AS burn_events,
            COUNT(DISTINCT study_instance_uid) AS unique_studies,
            COUNT(DISTINCT patient_name)       AS unique_patients,
            SUM(COALESCE(number_of_copies, 1)) AS total_copies
        FROM cd_print_log
        WHERE burned_at::date BETWEEN :start AND :end {_SR}
    """), p).fetchone()
    stats = {
        "burn_events":     int(r[0]) if r and r[0] else 0,
        "unique_studies":  int(r[1]) if r and r[1] else 0,
        "unique_patients": int(r[2]) if r and r[2] else 0,
        "total_copies":    int(r[3]) if r and r[3] else 0,
    }
    stats["avg_copies"] = (
        round(stats["total_copies"] / stats["burn_events"], 1)
        if stats["burn_events"] else 0
    )

    # ── Monthly trend ─────────────────────────────────────────────────
    trend = db.session.execute(text(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', burned_at), 'Mon YYYY'),
            DATE_TRUNC('month', burned_at),
            COUNT(*),
            SUM(COALESCE(number_of_copies, 1))
        FROM cd_print_log
        WHERE burned_at::date BETWEEN :start AND :end {_SR}
        GROUP BY DATE_TRUNC('month', burned_at)
        ORDER BY 2
    """), p).fetchall()
    trend_json = {
        "labels": [row[0] for row in trend],
        "events": [int(row[2]) for row in trend],
        "copies": [int(row[3]) for row in trend],
    }

    # ── Media type (CD / DVD / …) ─────────────────────────────────────
    media = db.session.execute(text(f"""
        SELECT
            COALESCE(NULLIF(TRIM(media_type), ''), 'Unknown'),
            COUNT(*),
            SUM(COALESCE(number_of_copies, 1))
        FROM cd_print_log
        WHERE burned_at::date BETWEEN :start AND :end {_SR}
        GROUP BY 1
        ORDER BY 3 DESC
    """), p).fetchall()
    media_json = {
        "labels": [row[0] for row in media],
        "events": [int(row[1]) for row in media],
        "copies": [int(row[2]) for row in media],
    }

    # ── Modality breakdown ────────────────────────────────────────────
    mods = db.session.execute(text(f"""
        SELECT
            COALESCE(NULLIF(study_modality, ''), 'Unknown'),
            COUNT(*),
            SUM(COALESCE(number_of_copies, 1))
        FROM cd_print_log
        WHERE burned_at::date BETWEEN :start AND :end {_SR}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 12
    """), p).fetchall()
    modality_json = {
        "labels": [row[0] for row in mods],
        "events": [int(row[1]) for row in mods],
        "copies": [int(row[2]) for row in mods],
    }

    # ── Detail table: modality × media type ──────────────────────────
    tbl = db.session.execute(text(f"""
        SELECT
            COALESCE(NULLIF(TRIM(study_modality), ''), '—'),
            COALESCE(NULLIF(TRIM(media_type), ''), 'Unknown'),
            COUNT(*),
            SUM(COALESCE(number_of_copies, 1)),
            COUNT(DISTINCT study_instance_uid)
        FROM cd_print_log
        WHERE burned_at::date BETWEEN :start AND :end {_SR}
        GROUP BY 1, 2
        ORDER BY 3 DESC
    """), p).fetchall()
    table_data = [
        {
            "modality":       row[0],
            "media_type":     row[1],
            "burn_events":    int(row[2]),
            "total_copies":   int(row[3]),
            "unique_studies": int(row[4]),
        }
        for row in tbl
    ]

    return stats, trend_json, media_json, modality_json, table_data, start, end


@report_30_bp.route("/report/30", methods=["GET", "POST"])
@login_required
def report_30():
    run_report = False
    stats, trend_json, media_json, modality_json, table_data = {}, {}, {}, {}, []
    display_start, display_end = _date_range({})

    if "start_date" in request.values:
        run_report = True
        stats, trend_json, media_json, modality_json, table_data, display_start, display_end = \
            get_report_data(request.values)

    return render_template(
        "report_30.html",
        report_name   = "Patient Media Distribution",
        run_report    = run_report,
        display_start = display_start,
        display_end   = display_end,
        stats         = stats,
        trend_json    = json.dumps(trend_json),
        media_json    = json.dumps(media_json),
        modality_json = json.dumps(modality_json),
        table_data    = table_data,
    )


@report_30_bp.route("/report/30/export", methods=["POST"])
@login_required
def export_report_30():
    import csv, io
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403

    _, _, _, _, table_data, start, end = get_report_data(request.form)
    if not table_data:
        return "No data to export", 400

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["modality", "media_type", "burn_events", "total_copies", "unique_studies"])
    w.writeheader()
    w.writerows(table_data)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=CD_Distribution_{start}_to_{end}.csv"},
    )


from routes.report_registry import register_report
register_report(30, report_30_bp, report_30, export_report_30)
