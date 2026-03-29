from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page
from datetime import date

hl7_orders_bp = Blueprint('hl7_orders', __name__)


def _fetch_orders(date_str=None, modality=None, status=None):
    """Fetch hl7_orders with optional filters. Returns list of dicts."""
    filters = ["1=1"]
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
        text("SELECT COUNT(*) FROM hl7_orders WHERE DATE(scheduled_datetime) = :d OR DATE(received_at) = :d"),
        {"d": today}
    ).scalar()
    return jsonify({"count": int(row or 0)})


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
