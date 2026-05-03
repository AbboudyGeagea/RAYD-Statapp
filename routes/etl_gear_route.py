from flask import Blueprint, request, jsonify, abort
from flask_login import login_required, current_user
from utils.permissions import permission_required
import os, re

etl_gear_bp = Blueprint('etl_gear', __name__)

ETL_SETTINGS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'ETL_JOBS', 'etl_settings.py'
)

@etl_gear_bp.route('/admin/etl-gear', methods=['POST'])
@login_required
@permission_required('can_view_etl')
def save_etl_gear():
    if current_user.role != 'admin':
        abort(403)
    try:
        data        = request.get_json()
        num_workers = int(data.get('num_workers', 4))
        batch_size  = int(data.get('batch_size', 5000))

        # Clamp to safe ranges
        num_workers = max(1, min(8,     num_workers))
        batch_size  = max(1000, min(20000, batch_size))

        content = open(ETL_SETTINGS_PATH).read()

        content = re.sub(
            r"(['\"]num_workers['\"])\s*:\s*\d+",
            f"'num_workers': {num_workers}",
            content
        )
        content = re.sub(
            r"(['\"]batch_size['\"])\s*:\s*\d+",
            f"'batch_size': {batch_size}",
            content
        )

        open(ETL_SETTINGS_PATH, 'w').write(content)
        return jsonify(ok=True)

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"ETL gear save error: {e}")
        return jsonify(ok=False, error="Failed to save settings"), 500
