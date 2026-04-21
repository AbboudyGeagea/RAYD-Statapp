from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page
from datetime import date
import json

hl7_orders_bp = Blueprint('hl7_orders', __name__)

DEFAULT_FIELD_MAP = [
    {"seg": "MSH", "fi": 9,  "ci": 0,  "label": "Message Type",        "db": "message_type"},
    {"seg": "MSH", "fi": 10, "ci": -1, "label": "Message Control ID",  "db": "message_id"},
    {"seg": "PID", "fi": 3,  "ci": 0,  "label": "Patient ID",          "db": "patient_id"},
    {"seg": "PID", "fi": 5,  "ci": 0,  "label": "Patient Family Name", "db": "patient_name"},
    {"seg": "PID", "fi": 7,  "ci": -1, "label": "Date of Birth",       "db": "date_of_birth"},
    {"seg": "PID", "fi": 8,  "ci": -1, "label": "Gender",              "db": "gender"},
    {"seg": "ORC", "fi": 1,  "ci": -1, "label": "Order Control",       "db": "order_status"},
    {"seg": "ORC", "fi": 2,  "ci": 0,  "label": "Placer Order #",      "db": "placer_order_number"},
    {"seg": "OBR", "fi": 2,  "ci": 0,  "label": "Accession Number",    "db": "accession_number"},
    {"seg": "OBR", "fi": 4,  "ci": 0,  "label": "Procedure Code",      "db": "procedure_code"},
    {"seg": "OBR", "fi": 4,  "ci": 1,  "label": "Procedure Text",      "db": "procedure_text"},
    {"seg": "OBR", "fi": 16, "ci": 0,  "label": "Ordering Physician",  "db": "ordering_physician"},
    {"seg": "OBR", "fi": 24, "ci": -1, "label": "Modality",            "db": "modality"},
    {"seg": "OBR", "fi": 36, "ci": -1, "label": "Scheduled DateTime",  "db": "scheduled_datetime"},
]


def _fetch_orders(date_str=None, modality=None, status=None):
    """Fetch hl7_orders with optional filters. Returns list of dicts."""
    filters = ["1=1", "(order_status IS NULL OR order_status <> 'CM')"]
    params  = {}

    if date_str:
        filters.append("DATE(scheduled_datetime) = :date OR DATE(received_at) = :date")
        params['date'] = date_str
    else:
        today = date.today().isoformat()
        filters.append("(DATE(scheduled_datetime) = :date OR DATE(received_at) = :date)")
        params['date'] = today

    if modality:
        filters.append("modality = :modality")
        params['modality'] = modality

    if status:
        filters.append("order_status = :status")
        params['status'] = status

    where = " AND ".join(filters)
    sql = text(f"""
        SELECT
            id, message_id, received_at,
            patient_id, patient_name, date_of_birth, gender,
            accession_number, placer_order_number,
            procedure_code, procedure_text,
            modality, scheduled_datetime,
            ordering_physician, order_status,
            message_type
        FROM hl7_orders
        WHERE {where}
        ORDER BY received_at DESC
        LIMIT 1000
    """)

    rows = db.session.execute(sql, params).fetchall()
    return [dict(r._mapping) for r in rows]


def _fetch_filter_options():
    """Get distinct modalities and statuses for filter dropdowns."""
    modalities = db.session.execute(
        text("SELECT DISTINCT modality FROM hl7_orders WHERE modality IS NOT NULL ORDER BY modality")
    ).fetchall()
    statuses = db.session.execute(
        text("SELECT DISTINCT order_status FROM hl7_orders WHERE order_status IS NOT NULL ORDER BY order_status")
    ).fetchall()
    return (
        [r[0] for r in modalities],
        [r[0] for r in statuses]
    )


@hl7_orders_bp.route('/hl7-orders')
@login_required
def hl7_orders_page():
    if not user_has_page(current_user, 'hl7_orders'):
        abort(403)
    date_str = request.args.get('date', date.today().isoformat())
    modality = request.args.get('modality', '')
    status   = request.args.get('status', '')

    orders              = _fetch_orders(date_str, modality or None, status or None)
    modalities, statuses = _fetch_filter_options()

    return render_template(
        'hl7_orders.html',
        orders     = orders,
        date_str   = date_str,
        modalities = modalities,
        statuses   = statuses,
        selected_modality = modality,
        selected_status   = status,
        total      = len(orders),
    )


@hl7_orders_bp.route('/hl7-orders/count')
@login_required
def hl7_orders_count():
    """Lightweight endpoint — today's order count only, used by sidebar badge."""
    today = date.today().isoformat()
    row = db.session.execute(
        text("""
            SELECT COUNT(*) FROM hl7_orders
            WHERE (DATE(scheduled_datetime) = :d OR DATE(received_at) = :d)
              AND (order_status IS NULL OR order_status <> 'CM')
        """),
        {"d": today}
    ).scalar()
    return jsonify({"count": int(row or 0)})


@hl7_orders_bp.route('/hl7-orders/field-map', methods=['GET'])
@login_required
def hl7_orders_get_field_map():
    """Return the active HL7→DB column mapping (from settings, else defaults)."""
    try:
        row = db.session.execute(
            text("SELECT value FROM settings WHERE key = 'hl7_field_map'")
        ).fetchone()
        mappings = json.loads(row[0]) if row else DEFAULT_FIELD_MAP
    except Exception:
        mappings = DEFAULT_FIELD_MAP
    return jsonify({"mappings": mappings})


@hl7_orders_bp.route('/hl7-orders/field-map', methods=['POST'])
@login_required
def hl7_orders_save_field_map():
    """Persist a custom HL7→DB column mapping (admin only)."""
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True)
    mappings = data.get('mappings', [])
    for m in mappings:
        if not all(k in m for k in ('seg', 'fi', 'ci', 'db')):
            return jsonify({'error': 'Invalid mapping entry — requires seg, fi, ci, db'}), 400
    val = json.dumps(mappings)
    try:
        existing = db.session.execute(
            text("SELECT 1 FROM settings WHERE key = 'hl7_field_map'")
        ).fetchone()
        if existing:
            db.session.execute(
                text("UPDATE settings SET value = :v WHERE key = 'hl7_field_map'"), {'v': val}
            )
        else:
            db.session.execute(
                text("INSERT INTO settings (key, value) VALUES ('hl7_field_map', :v)"), {'v': val}
            )
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True})


@hl7_orders_bp.route('/hl7-orders/data')
@login_required
def hl7_orders_data():
    if not user_has_page(current_user, 'hl7_orders'):
        abort(403)
    """JSON endpoint for live auto-refresh."""
    date_str = request.args.get('date', date.today().isoformat())
    modality = request.args.get('modality', '')
    status   = request.args.get('status', '')

    orders = _fetch_orders(date_str, modality or None, status or None)

    # Serialize datetimes
    for o in orders:
        for k, v in o.items():
            if hasattr(v, 'isoformat'):
                o[k] = v.isoformat()

    return jsonify({"orders": orders, "total": len(orders)})
