"""
Report 30 — Patient CD / DVD Distribution (CD Burn Audit)
Queries cd_burn_log, filled by the burning station's REST API (routes/cd_log_route.py).
One cd_burn_log row = one disc burn; its studies sit in a JSONB array.
"""
import json
from datetime import date
from flask import Blueprint, render_template, request, Response
from flask_login import login_required
from sqlalchemy import text
from db import db, get_etl_cutoff_date

report_30_bp = Blueprint("report_30", __name__)

# Successful burns in the selected period (sargable on idx_cd_burn_log_timestamp)
_BURNS = """
    SELECT id, patient_id, patient_name, studies, disc_format,
           COALESCE(copies_count, 1) AS copies,
           timestamp
    FROM cd_burn_log
    WHERE timestamp >= CAST(:start AS date) AND timestamp < CAST(:end AS date) + 1
      AND status = 'success'
"""
# One row per (burn, study); SR (Structured Report) studies excluded
_BURN_STUDIES = f"""
    SELECT b.id, b.copies,
           COALESCE(NULLIF(TRIM(b.disc_format), ''), 'Unknown')    AS disc_format,
           COALESCE(NULLIF(TRIM(s->>'modality'), ''), 'Unknown')   AS modality,
           s->>'study_uid'                                         AS study_uid
    FROM ({_BURNS}) b
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.studies, '[]'::jsonb)) AS s
    WHERE COALESCE(s->>'modality', '') != 'SR'
"""


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
    # Burns and copies count once per disc, however many studies it holds.
    r = db.session.execute(text(f"""
        WITH b AS ({_BURNS}),
             bs AS ({_BURN_STUDIES})
        SELECT
            (SELECT COUNT(*) FROM b)                                                    AS burn_events,
            (SELECT COUNT(DISTINCT study_uid) FROM bs)                                  AS unique_studies,
            (SELECT COUNT(DISTINCT COALESCE(NULLIF(patient_id, ''), patient_name)) FROM b) AS unique_patients,
            (SELECT SUM(copies) FROM b)                                                 AS total_copies
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
            TO_CHAR(DATE_TRUNC('month', timestamp), 'Mon YYYY'),
            DATE_TRUNC('month', timestamp),
            COUNT(*),
            SUM(copies)
        FROM ({_BURNS}) b
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY 2
    """), p).fetchall()
    trend_json = {
        "labels": [row[0] for row in trend],
        "events": [int(row[2]) for row in trend],
        "copies": [int(row[3]) for row in trend],
    }

    # ── Disc format (CD / DVD / …) ────────────────────────────────────
    media = db.session.execute(text(f"""
        SELECT
            COALESCE(NULLIF(TRIM(disc_format), ''), 'Unknown'),
            COUNT(*),
            SUM(copies)
        FROM ({_BURNS}) b
        GROUP BY 1
        ORDER BY 3 DESC
    """), p).fetchall()
    media_json = {
        "labels": [row[0] for row in media],
        "events": [int(row[1]) for row in media],
        "copies": [int(row[2]) for row in media],
    }

    # ── Modality breakdown (burns holding at least one study of it) ───
    mods = db.session.execute(text(f"""
        SELECT modality, COUNT(*), SUM(copies)
        FROM (SELECT DISTINCT id, copies, modality FROM ({_BURN_STUDIES}) bs) bm
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 12
    """), p).fetchall()
    modality_json = {
        "labels": [row[0] for row in mods],
        "events": [int(row[1]) for row in mods],
        "copies": [int(row[2]) for row in mods],
    }

    # ── Detail table: modality × disc format ─────────────────────────
    tbl = db.session.execute(text(f"""
        WITH bs AS ({_BURN_STUDIES})
        SELECT b.modality, b.disc_format, b.burns, b.copies, u.studies
        FROM (
            SELECT modality, disc_format, COUNT(*) AS burns, SUM(copies) AS copies
            FROM (SELECT DISTINCT id, copies, disc_format, modality FROM bs) d
            GROUP BY 1, 2
        ) b
        JOIN (
            SELECT modality, disc_format, COUNT(DISTINCT study_uid) AS studies
            FROM bs
            GROUP BY 1, 2
        ) u USING (modality, disc_format)
        ORDER BY b.burns DESC
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
