from flask import Blueprint, request, jsonify, abort
from flask_login import login_required, current_user
import json, os, logging

etl_gear_bp = Blueprint('etl_gear', __name__)

_OVERRIDE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'ETL_JOBS', 'etl_settings_override.json'
)

@etl_gear_bp.route('/admin/etl-gear', methods=['POST'])
@login_required
def save_etl_gear():
    if current_user.role != 'admin':
        abort(403)
    try:
        data        = request.get_json()
        num_workers = int(data.get('num_workers', 4))
        batch_size  = int(data.get('batch_size', 5000))

        num_workers = max(1, min(8,     num_workers))
        batch_size  = max(1000, min(20000, batch_size))

        with open(_OVERRIDE_PATH, 'w') as f:
            json.dump({'num_workers': num_workers, 'batch_size': batch_size}, f, indent=4)

        return jsonify(ok=True)

    except Exception as e:
        logging.getLogger(__name__).error(f"ETL gear save error: {e}")
        return jsonify(ok=False, error="Failed to save settings"), 500
