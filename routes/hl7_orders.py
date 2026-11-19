from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from utils.roles import can
from sqlalchemy import text
from db import db, user_has_page
from datetime import date
import json

hl7_orders_bp = Blueprint('hl7_orders', __name__)

DEFAULT_FIELD_MAP = [
    {"seg": "MSH", "fi": 8,  "ci": 0,  "label": "Message Type",        "db": "message_type"},
    {"seg": "MSH", "fi": 9,  "ci": -1, "label": "Message Control ID",  "db": "message_id"},
    {"seg": "PID", "fi": 3,  "ci": 0,  "label": "Patient ID",          "db": "patient_id"},
    {"seg": "PID", "fi": 5,  "ci": 0,  "label": "Patient Family Name", "db": "patient_name"},
    {"seg": "PID", "fi": 7,  "ci": -1, "label": "Date of Birth",       "db": "date_of_birth"},
    {"seg": "PID", "fi": 8,  "ci": -1, "label": "Gender",              "db": "gender"},
    {"seg": "ORC", "fi": 5,  "ci": -1, "label": "Order Status",        "db": "order_status"},
    {"seg": "ORC", "fi": 2,  "ci": 0,  "label": "Placer Order #",      "db": "placer_order_number"},
    {"seg": "OBR", "fi": 3,  "ci": 0,  "label": "Accession Number",    "db": "accession_number"},
    {"seg": "OBR", "fi": 4,  "ci": 0,  "label": "Procedure Code",      "db": "procedure_code"},
    {"seg": "OBR", "fi": 4,  "ci": 1,  "label": "Procedure Text",      "db": "procedure_text"},
    {"seg": "OBR", "fi": 16, "ci": 0,  "label": "Ordering Physician",  "db": "ordering_physician"},
    {"seg": "OBR", "fi": 24, "ci": -1, "label": "Modality",            "db": "modality"},
    {"seg": "OBR", "fi": 36, "ci": -1, "label": "Scheduled DateTime",  "db": "scheduled_datetime"},
]


def _hospital():
    """(engine, SQL, params): the database of the hospital picked in the header and the clause
    keeping its orders. The listener writes each HIS order to its hospital's database once that
    hospital has moved (item 13d, utils/hl7_site.py); before, every order of both hospitals is on
    the main database, hence the clause on the code the HIS sends (PV1-3.7). The page itself is in
    MAIN_BLUEPRINTS: its field map lives on the main database, which the listener reads."""
    from flask import session
    from utils import site_db
    from utils.his_ris import his_site_clause
    from utils.roles import granted_site_ids
    picked = session.get('site_id')
    if picked not in granted_site_ids(current_user):
        picked = site_db.default_site_id()
    sql, params = his_site_clause('hl7_orders', site_db.site_code(picked) or 'RH')
    return site_db.engine_for(picked), sql, params


def _rows(engine, statement, params=None):
    with engine.connect() as conn:
        return conn.execute(text(statement), params or {}).fetchall()


def _count_orders(date_str):
    """Return (performed, cancelled) for a given date. CA = cancelled, everything else = performed."""
    engine, site_sql, site_params = _hospital()
    row = _rows(engine, f"""
        SELECT
            COUNT(*) FILTER (WHERE UPPER(COALESCE(order_status, '')) != 'CA') AS performed,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(order_status, '')) = 'CA')  AS cancelled
        FROM hl7_orders
        WHERE (DATE(scheduled_datetime) = :d OR DATE(received_at) = :d) AND {site_sql}
    """, {"d": date_str, **site_params})[0]
    return (int(row[0] or 0), int(row[1] or 0))


def _fetch_orders(date_str=None, modality=None, status=None):
    """Fetch hl7_orders with optional filters. Returns list of dicts."""
    engine, site_sql, site_params = _hospital()
    filters = ["1=1", "(order_status IS NULL OR order_status <> 'CM')", site_sql]
    params  = dict(site_params)

    if date_str:
        filters.append("(DATE(scheduled_datetime) = :date OR DATE(received_at) = :date)")
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
    sql = f"""
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
    """
    return [dict(r._mapping) for r in _rows(engine, sql, params)]


def _fetch_filter_options():
    """Get distinct modalities and statuses for filter dropdowns."""
    engine, site_sql, site_params = _hospital()
    modalities = _rows(engine,
        f"SELECT DISTINCT modality FROM hl7_orders WHERE modality IS NOT NULL AND modality NOT IN ('SR', 'BMD') "
        f"AND {site_sql} ORDER BY modality", site_params)
    statuses = _rows(engine,
        f"SELECT DISTINCT order_status FROM hl7_orders WHERE order_status IS NOT NULL AND {site_sql} "
        f"ORDER BY order_status", site_params)
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
    performed, cancelled = _count_orders(date_str)

    return render_template(
        'hl7_orders.html',
        orders     = orders,
        date_str   = date_str,
        modalities = modalities,
        statuses   = statuses,
        selected_modality = modality,
        selected_status   = status,
        performed  = performed,
        cancelled  = cancelled,
    )


@hl7_orders_bp.route('/hl7-orders/count')
@login_required
def hl7_orders_count():
    """Lightweight endpoint — today's order counts, used by sidebar badge."""
    today = date.today().isoformat()
    performed, cancelled = _count_orders(today)
    return jsonify({"count": performed, "cancelled": cancelled})


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
    if not can(current_user, 'implementation'):
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
    performed, cancelled = _count_orders(date_str)

    # Serialize datetimes
    for o in orders:
        for k, v in o.items():
            if hasattr(v, 'isoformat'):
                o[k] = v.isoformat()

    return jsonify({"orders": orders, "total": performed, "cancelled": cancelled})
