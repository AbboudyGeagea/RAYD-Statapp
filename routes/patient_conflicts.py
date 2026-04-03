import csv
import io
from flask import Blueprint, jsonify, Response
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

patient_conflicts_bp = Blueprint('patient_conflicts', __name__)

_CONFLICT_SQL = """
    SELECT
        patient_db_uid,
        id              AS patient_id,
        fallback_id,
        birth_date::text,
        sex,
        number_of_patient_studies AS studies,
        last_update::text
    FROM etl_patient_view
    WHERE fallback_id LIKE '%$$$%'
    ORDER BY last_update DESC NULLS LAST
"""


@patient_conflicts_bp.route('/patients/conflicts')
@login_required
def conflict_count():
    try:
        row = db.session.execute(text(
            "SELECT COUNT(*) FROM etl_patient_view WHERE fallback_id LIKE '%$$$%'"
        )).fetchone()
        count = int(row[0] or 0)

        preview = db.session.execute(text(_CONFLICT_SQL + " LIMIT 50")).mappings().fetchall()

        return jsonify({
            'count':   count,
            'preview': [dict(r) for r in preview],
            'error':   None,
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'count': 0, 'preview': [], 'error': str(e)})


@patient_conflicts_bp.route('/patients/conflicts/export')
@login_required
def conflict_export():
    rows = db.session.execute(text(_CONFLICT_SQL)).mappings().fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['patient_db_uid', 'patient_id', 'fallback_id',
                     'birth_date', 'sex', 'studies', 'last_update'])
    for r in rows:
        writer.writerow([r['patient_db_uid'], r['patient_id'], r['fallback_id'],
                         r['birth_date'], r['sex'], r['studies'], r['last_update']])

    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename="conflict_patients.csv"'}
    )
