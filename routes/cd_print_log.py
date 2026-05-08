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

    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')
    search    = request.args.get('search', '').strip()
    modality  = request.args.get('modality', '').strip()

    filters = []
    params  = {"limit": per_page, "offset": offset}

    if date_from:
        filters.append("burned_at::date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        filters.append("burned_at::date <= :date_to")
        params["date_to"] = date_to
    if search:
        filters.append("(patient_name ILIKE :search OR accession_number ILIKE :search)")
        params["search"] = f"%{search}%"
    if modality:
        filters.append("study_modality = :modality")
        params["modality"] = modality

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = db.session.execute(text(f"""
        SELECT task_id, patient_name, burned_at, study_modality,
               accession_number, study_date, reading_physician,
               media_type, number_of_copies, cd_status, study_db_uid
        FROM cd_print_log
        {where}
        ORDER BY burned_at DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    count_row = db.session.execute(text(f"""
        SELECT COUNT(*) FROM cd_print_log {where}
    """), {k: v for k, v in params.items() if k not in ('limit', 'offset')}).fetchone()

    total = count_row[0] if count_row else 0

    return jsonify({
        "total": total,
        "page": page,
        "per_page": per_page,
        "rows": [
            {
                "task_id":          r[0],
                "patient_name":     r[1],
                "burned_at":        r[2].isoformat() if r[2] else None,
                "study_modality":   r[3],
                "accession_number": r[4],
                "study_date":       r[5].isoformat() if r[5] else None,
                "reading_physician": r[6],
                "media_type":       r[7],
                "number_of_copies": r[8],
                "cd_status":        r[9],
                "study_db_uid":     r[10],
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


@cd_print_bp.route('/api/cd-print-log/stats')
@login_required
def cd_print_stats():
    row = db.session.execute(text("""
        SELECT
            COUNT(*)                                              AS total,
            COUNT(CASE WHEN burned_at >= NOW() - INTERVAL '30 days' THEN 1 END) AS last_30d,
            COUNT(CASE WHEN burned_at >= NOW() - INTERVAL '7 days'  THEN 1 END) AS last_7d,
            MAX(burned_at)                                        AS last_sync,
            COUNT(DISTINCT study_modality)                        AS modalities
        FROM cd_print_log
    """)).fetchone()

    modality_rows = db.session.execute(text("""
        SELECT study_modality, COUNT(*) AS cnt
        FROM cd_print_log
        WHERE study_modality IS NOT NULL
        GROUP BY study_modality
        ORDER BY cnt DESC
        LIMIT 10
    """)).fetchall()

    return jsonify({
        "total":       row[0],
        "last_30d":    row[1],
        "last_7d":     row[2],
        "last_sync":   row[3].isoformat() if row[3] else None,
        "modalities":  [{"modality": r[0], "count": r[1]} for r in modality_rows],
    })
