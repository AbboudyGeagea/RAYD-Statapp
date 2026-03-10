from flask import Blueprint, request, jsonify
import os, re

etl_gear_bp = Blueprint('etl_gear', __name__)

ETL_SETTINGS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'ETL_JOBS', 'etl_settings.py'
)

@etl_gear_bp.route('/admin/etl-gear', methods=['POST'])
def save_etl_gear():
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
        return jsonify(ok=False, error=str(e)), 500
