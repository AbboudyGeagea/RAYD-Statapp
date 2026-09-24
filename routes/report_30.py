"""
Report 30 — Patient CD / DVD Distribution (CD Burn Audit)
Queries cd_burn_log (REST API data store). Updated to new schema.
"""
import json
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date

report_30_bp = Blueprint("report_30", __name__)


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
    # Using cd_burn_log: studies stored as JSONB array, unnest to count
    r = db.session.execute(text("""
        SELECT
            COUNT(*) AS burn_events,
            COUNT(DISTINCT s->>'study_uid') AS unique_studies,
            COUNT(DISTINCT patient_name) AS unique_patients,
            SUM(copies_count) AS total_copies,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_burns
        FROM cd_burn_log,
        LATERAL jsonb_array_elements(COALESCE(studies, '[]'::jsonb)) AS s
        WHERE DATE(timestamp) BETWEEN :start AND :end AND status = 'success'
    """), p).fetchone()

    stats = {
        "burn_events":       int(r[0]) if r and r[0] else 0,
        "unique_studies":    int(r[1]) if r and r[1] else 0,
        "unique_patients":   int(r[2]) if r and r[2] else 0,
        "total_copies":      int(r[3]) if r and r[3] else 0,
        "successful_burns":  int(r[4]) if r and r[4] else 0,
    }
    stats["avg_copies"] = (
        round(stats["total_copies"] / stats["burn_events"], 1)
        if stats["burn_events"] else 0
    )

    # ── Daily trend ───────────────────────────────────────────────────
    trend = db.session.execute(text("""
        SELECT
            TO_CHAR(DATE(timestamp), 'Mon DD, YYYY'),
            DATE(timestamp),
            COUNT(*),
            SUM(copies_count)
        FROM cd_burn_log
        WHERE DATE(timestamp) BETWEEN :start AND :end AND status = 'success'
        GROUP BY DATE(timestamp)
        ORDER BY 2
    """), p).fetchall()
    trend_json = {
        "labels": [row[0] for row in trend],
        "events": [int(row[2]) for row in trend],
        "copies": [int(row[3]) for row in trend],
    }

    # ── Disc format (CD / DVD / …) ─────────────────────────────────────
    media = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(TRIM(disc_format), ''), 'Unknown'),
            COUNT(*),
            SUM(copies_count)
        FROM cd_burn_log
        WHERE DATE(timestamp) BETWEEN :start AND :end AND status = 'success'
        GROUP BY 1
        ORDER BY 3 DESC
    """), p).fetchall()
    media_json = {
        "labels": [row[0] for row in media],
        "events": [int(row[1]) for row in media],
        "copies": [int(row[2]) for row in media],
    }

    # ── Modality breakdown (from JSONB studies) ──────────────────────
    mods = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(s->>'modality', ''), 'Unknown'),
            COUNT(*),
            SUM(cd.copies_count)
        FROM cd_burn_log cd,
        LATERAL jsonb_array_elements(COALESCE(cd.studies, '[]'::jsonb)) AS s
        WHERE DATE(cd.timestamp) BETWEEN :start AND :end AND cd.status = 'success'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 12
    """), p).fetchall()
    modality_json = {
        "labels": [row[0] for row in mods],
        "events": [int(row[1]) for row in mods],
        "copies": [int(row[2]) for row in mods],
    }

    # ── Detail table: modality × disc format ──────────────────────────
    tbl = db.session.execute(text("""
        SELECT
            COALESCE(NULLIF(s->>'modality', ''), '—'),
            COALESCE(NULLIF(TRIM(cd.disc_format), ''), 'Unknown'),
            COUNT(*),
            SUM(cd.copies_count),
            COUNT(DISTINCT s->>'study_uid')
        FROM cd_burn_log cd,
        LATERAL jsonb_array_elements(COALESCE(cd.studies, '[]'::jsonb)) AS s
        WHERE DATE(cd.timestamp) BETWEEN :start AND :end AND cd.status = 'success'
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
        report_name   = "CD / DVD Distribution (Burn Audit)",
        report_desc   = "Patient media distribution powered by CD burn REST API",
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
        headers={"Content-Disposition": f"attachment; filename=CD_Burn_Audit_{start}_to_{end}.csv"},
    )


from routes.report_registry import register_report
register_report(30, report_30_bp, report_30, export_report_30)
