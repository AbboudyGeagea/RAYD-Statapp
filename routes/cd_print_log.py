from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

cd_print_bp = Blueprint('cd_print', __name__)


@cd_print_bp.route('/cd-print-log')
@login_required
def cd_print_log():
    return render_template('cd_print_log.html')


@cd_print_bp.route('/api/cd-print-log')
@login_required
def api_cd_print_log():
    page     = max(1, int(request.args.get('page', 1)))
    per_page = 50
    offset   = (page - 1) * per_page

    date_from = request.args.get('date_from', '').strip()
    date_to   = request.args.get('date_to', '').strip()
    search    = request.args.get('search', '').strip()

    base_filters = []
    params = {}

    if date_from:
        base_filters.append("c.burned_at::date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        base_filters.append("c.burned_at::date <= :date_to")
        params["date_to"] = date_to
    if search:
        base_filters.append("""(
            c.patient_name    ILIKE :search
            OR c.accession_number ILIKE :search
            OR pv.patient_id  ILIKE :search
        )""")
        params["search"] = f"%{search}%"

    where_cte = ("WHERE " + " AND ".join(base_filters)) if base_filters else ""

    # One row per unique study, aggregated burn events
    sql_data = text(f"""
        WITH filtered AS (
            SELECT
                c.study_instance_uid,
                c.patient_name,
                c.accession_number,
                c.study_modality,
                c.study_db_uid,
                c.burned_at,
                COALESCE(c.number_of_copies, 1) AS copies,
                COALESCE(s.procedure_code, '') AS procedure_code,
                COALESCE(
                    (SELECT o.proc_text FROM etl_orders o
                     WHERE o.study_db_uid = c.study_db_uid
                       AND o.proc_text IS NOT NULL
                     LIMIT 1),
                    s.procedure_code, ''
                ) AS procedure_name,
                pv.patient_id
            FROM cd_print_log c
            LEFT JOIN etl_didb_studies s  ON s.study_db_uid    = c.study_db_uid
            LEFT JOIN etl_patient_view pv ON pv.patient_db_uid = s.patient_db_uid
            {where_cte}
        )
        SELECT
            study_instance_uid,
            patient_name,
            accession_number,
            study_modality,
            procedure_code,
            procedure_name,
            patient_id,
            COUNT(*)                           AS burn_count,
            SUM(copies)                        AS total_copies,
            JSON_AGG(
                JSON_BUILD_OBJECT('date', burned_at, 'copies', copies)
                ORDER BY burned_at
            )                                  AS burn_events,
            MAX(burned_at)                     AS last_burned
        FROM filtered
        GROUP BY study_instance_uid, patient_name, accession_number,
                 study_modality, procedure_code, procedure_name, patient_id
        ORDER BY last_burned DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)

    sql_count = text(f"""
        WITH filtered AS (
            SELECT c.study_instance_uid
            FROM cd_print_log c
            LEFT JOIN etl_didb_studies s  ON s.study_db_uid    = c.study_db_uid
            LEFT JOIN etl_patient_view pv ON pv.patient_db_uid = s.patient_db_uid
            {where_cte}
        )
        SELECT COUNT(DISTINCT study_instance_uid) FROM filtered
    """)

    params["limit"]  = per_page
    params["offset"] = offset
    rows = db.session.execute(sql_data, params).fetchall()

    count_params = {k: v for k, v in params.items() if k not in ('limit', 'offset')}
    total = db.session.execute(sql_count, count_params).scalar() or 0

    return jsonify({
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "rows": [
            {
                "study_instance_uid": r[0],
                "patient_name":       r[1],
                "accession_number":   r[2],
                "study_modality":     r[3],
                "procedure_code":     r[4],
                "procedure_name":     r[5],
                "patient_id":         r[6],
                "burn_count":         r[7],
                "total_copies":       int(r[8]) if r[8] else 0,
                "burn_events":        r[9],
                "last_burned":        r[10].isoformat() if r[10] else None,
            }
            for r in rows
        ]
    })


@cd_print_bp.route('/api/cd-print-log/trigger', methods=['POST'])
@login_required
def trigger_cd_sync():
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    try:
        from ETL_JOBS.etl_cd_surf import run_cd_surf_etl
        n = run_cd_surf_etl(db.engine)
        return jsonify({'ok': True, 'synced': n})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500
