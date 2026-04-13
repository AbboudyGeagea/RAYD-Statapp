from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required
from sqlalchemy import text
from db import db
from datetime import date

liveview_bp = Blueprint('liveview', __name__, url_prefix='/liveview')


@liveview_bp.route('/')
@login_required
def liveview():
    return render_template('liveview.html')


@liveview_bp.route('/rooms')
@login_required
def liveview_rooms():
    """Return list of rooms for the room selector."""
    rows = db.session.execute(text("""
        SELECT aetitle, modality, room_name
        FROM aetitle_modality_map
        WHERE room_name IS NOT NULL AND room_name != ''
        ORDER BY room_name
    """)).fetchall()
    return jsonify([
        {'aetitle': r.aetitle, 'modality': r.modality, 'room_name': r.room_name}
        for r in rows
    ])


@liveview_bp.route('/data')
@login_required
def liveview_data():
    today = date.today().isoformat()
    ae_filter = request.args.get('ae', '').strip()

    # Build optional AE title filter
    ae_clause = ""
    params = {'today': today}
    if ae_filter:
        ae_clause = "AND storing_ae = :ae"
        params['ae'] = ae_filter

    try:
        kpi = db.session.execute(text(f"""
            SELECT
                COUNT(*)                    AS total_studies,
                COUNT(DISTINCT patient_id)  AS total_patients,
                COUNT(*) FILTER (WHERE patient_class IS NOT NULL AND patient_class ILIKE '%%IN%%')  AS inpatient,
                COUNT(*) FILTER (WHERE patient_class IS NOT NULL AND (patient_class ILIKE '%%OUT%%' OR patient_class ILIKE '%%AMB%%')) AS outpatient,
                COUNT(*) FILTER (WHERE patient_class IS NOT NULL AND patient_class != '') AS has_class
            FROM hl7_scn_studies
            WHERE study_datetime::date = :today {ae_clause}
        """), params).fetchone()

        modalities = db.session.execute(text(f"""
            SELECT
                UPPER(TRIM(modality)) AS modality,
                COUNT(*)              AS count
            FROM hl7_scn_studies
            WHERE study_datetime::date = :today
              AND modality IS NOT NULL
              AND TRIM(modality) != ''
              {ae_clause}
            GROUP BY UPPER(TRIM(modality))
            ORDER BY count DESC
            LIMIT 12
        """), params).fetchall()

        dow = date.today().isoweekday() - 1
        params['dow'] = dow
        devices = db.session.execute(text(f"""
            SELECT
                COALESCE(s.storing_ae, 'UNKNOWN') AS storing_ae,
                COALESCE(m.modality, s.modality)  AS modality,
                SUM(COALESCE(pm.duration_minutes, 15)) AS used_mins,
                COALESCE(MAX(m.daily_capacity_minutes), MAX(ws.std_opening_minutes), 480) AS capacity_mins
            FROM hl7_scn_studies s
            LEFT JOIN aetitle_modality_map m
                ON m.aetitle = s.storing_ae
            LEFT JOIN procedure_duration_map pm
                ON pm.procedure_code = s.procedure_code
            LEFT JOIN device_weekly_schedule ws
                ON ws.aetitle = s.storing_ae
               AND ws.day_of_week = :dow
            WHERE s.study_datetime::date = :today
              {ae_clause}
            GROUP BY COALESCE(s.storing_ae, 'UNKNOWN'), COALESCE(m.modality, s.modality)
            ORDER BY used_mins DESC
        """), params).fetchall()

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

        kpi_data = {
            'total_studies':  kpi.total_studies  or 0,
            'total_patients': kpi.total_patients or 0,
        }
        if (kpi.has_class or 0) > 0:
            kpi_data['inpatient']  = kpi.inpatient  or 0
            kpi_data['outpatient'] = kpi.outpatient or 0

        return jsonify({
            'kpi': kpi_data,
            'modalities':  [{'modality': m.modality, 'count': m.count} for m in modalities],
            'devices':     device_list,
            'overall_pct': overall_pct,
            'date':        today,
            'error':       None,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'kpi':         {'total_studies': 0, 'total_patients': 0},
            'modalities':  [],
            'devices':     [],
            'overall_pct': 0,
            'date':        today,
            'error':       str(e),
        })
