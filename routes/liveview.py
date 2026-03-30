from flask import Blueprint, render_template, jsonify
from sqlalchemy import text
from db import db
from datetime import date

liveview_bp = Blueprint('liveview', __name__, url_prefix='/liveview')


@liveview_bp.route('/')
def liveview():
    return render_template('liveview.html')


@liveview_bp.route('/data')
def liveview_data():
    today = date.today().isoformat()

    try:
        # ── KPI counters ──────────────────────────────────────────────────────
        kpi = db.session.execute(text("""
            SELECT
                COUNT(*)                                                                           AS total_studies,
                COUNT(DISTINCT patient_db_uid)                                                     AS total_patients,
                COUNT(*) FILTER (WHERE study_has_report = true)                                    AS reports_signed,
                COUNT(*) FILTER (WHERE patient_class ILIKE '%IN%')                                 AS inpatient,
                COUNT(*) FILTER (WHERE patient_class ILIKE '%OUT%' OR patient_class ILIKE '%AMB%') AS outpatient
            FROM etl_didb_studies
            WHERE study_date = :today
        """), {'today': today}).fetchone()

        # ── Modality breakdown ────────────────────────────────────────────────
        modalities = db.session.execute(text("""
            SELECT
                UPPER(TRIM(study_modality)) AS modality,
                COUNT(*)                    AS count
            FROM etl_didb_studies
            WHERE study_date = :today
              AND study_modality IS NOT NULL
              AND TRIM(study_modality) != ''
            GROUP BY UPPER(TRIM(study_modality))
            ORDER BY count DESC
            LIMIT 12
        """), {'today': today}).fetchall()

        # ── Device utilization — simplified join to avoid UPPER/TRIM scan ────
        dow = date.today().isoweekday() - 1   # 0=Mon … 6=Sun
        devices = db.session.execute(text("""
            SELECT
                s.storing_ae,
                m.modality,
                SUM(COALESCE(pm.duration_minutes, 15)) AS used_mins,
                COALESCE(MAX(ws.std_opening_minutes), 720) AS capacity_mins
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m
                ON m.aetitle = s.storing_ae
            LEFT JOIN procedure_duration_map pm
                ON pm.procedure_code = s.procedure_code
            LEFT JOIN device_weekly_schedule ws
                ON ws.aetitle = s.storing_ae
               AND ws.day_of_week = :dow
            WHERE s.study_date = :today
              AND s.storing_ae IS NOT NULL
              AND s.storing_ae != ''
            GROUP BY s.storing_ae, m.modality
            ORDER BY used_mins DESC
        """), {'today': today, 'dow': dow}).fetchall()

        device_list = []
        for d in devices:
            used = d.used_mins or 0
            cap  = d.capacity_mins or 720
            pct  = round(min(used / cap * 100, 100), 1) if cap else 0
            device_list.append({
                'ae':            d.storing_ae,
                'modality':      d.modality or '—',
                'used_mins':     int(used),
                'capacity_mins': int(cap),
                'pct':           pct,
            })

        total_used  = sum(d['used_mins']     for d in device_list)
        total_cap   = sum(d['capacity_mins'] for d in device_list)
        overall_pct = round(total_used / total_cap * 100, 1) if total_cap else 0

        return jsonify({
            'kpi': {
                'total_studies':  kpi.total_studies  or 0,
                'total_patients': kpi.total_patients or 0,
                'reports_signed': kpi.reports_signed or 0,
                'inpatient':      kpi.inpatient      or 0,
                'outpatient':     kpi.outpatient     or 0,
            },
            'modalities':  [{'modality': m.modality, 'count': m.count} for m in modalities],
            'devices':     device_list,
            'overall_pct': overall_pct,
            'date':        today,
            'error':       None,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'kpi':         {'total_studies': 0, 'total_patients': 0, 'reports_signed': 0, 'inpatient': 0, 'outpatient': 0},
            'modalities':  [],
            'devices':     [],
            'overall_pct': 0,
            'date':        today,
            'error':       str(e),
        })
