import os
import json
import glob
import sys
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

# Ensure ETL_JOBS is importable
_ETL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL_JOBS')
if _ETL_DIR not in sys.path:
    sys.path.insert(0, _ETL_DIR)

adapter_mapper_bp = Blueprint('adapter_mapper', __name__)
logger = logging.getLogger("ADAPTER_MAPPER")

_DUMPS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL_JOBS', 'schema_dumps')

def _admin_only():
    if current_user.role != 'admin':
        abort(403)

def _ensure_table():
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS adapter_mappings (
            id              SERIAL PRIMARY KEY,
            connection_name VARCHAR(100) NOT NULL,
            schema_owner    VARCHAR(100),
            dump_file       VARCHAR(255),
            mapping_json    JSONB,
            notes           TEXT,
            status          VARCHAR(20) DEFAULT 'draft',
            system_type     VARCHAR(20),
            target_db       VARCHAR(100),
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        )
    """))
    # Add columns for existing deployments — hardcoded DDL, no f-strings
    db.session.execute(text(
        "ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS system_type VARCHAR(20)"
    ))
    db.session.execute(text(
        "ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS target_db VARCHAR(100)"
    ))
    db.session.commit()


@adapter_mapper_bp.route('/admin/adapters')
@login_required
def adapter_mapper_page():
    _admin_only()
    _ensure_table()

    # Oracle connections from db_params
    connections = db.session.execute(
        text("SELECT name, host, sid, username, db_type FROM db_params WHERE db_type ILIKE '%oracle%' ORDER BY name")
    ).mappings().fetchall()

    # Saved mappings
    mappings = db.session.execute(
        text("SELECT id, connection_name, schema_owner, status, dump_file, system_type, target_db, created_at, updated_at FROM adapter_mappings ORDER BY updated_at DESC")
    ).mappings().fetchall()

    # Available dump files
    os.makedirs(_DUMPS_DIR, exist_ok=True)
    dump_files = sorted(
        [os.path.basename(f) for f in glob.glob(os.path.join(_DUMPS_DIR, '*.json'))],
        reverse=True
    )

    # System types from registry
    from ETL_JOBS.system_type_registry import get_all_types
    system_types = get_all_types()

    return render_template(
        'adapter_mapper.html',
        connections=[dict(c) for c in connections],
        mappings=[dict(m) for m in mappings],
        dump_files=dump_files,
        system_types=system_types,
    )


@adapter_mapper_bp.route('/admin/adapters/discover', methods=['POST'])
@login_required
def discover_schema():
    _admin_only()
    data            = request.get_json()
    connection_name = data.get('connection_name', '').strip()
    schema_owner    = data.get('schema_owner', '').strip()

    if not connection_name or not schema_owner:
        return jsonify({'error': 'connection_name and schema_owner are required'}), 400

    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL_JOBS'))
        from schema_discovery import run_discovery

        engine = db.engine
        result = run_discovery(engine, connection_name, schema_owner, _DUMPS_DIR)
        return jsonify({'ok': True, **result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@adapter_mapper_bp.route('/admin/adapters/dump/<filename>')
@login_required
def view_dump(filename):
    _admin_only()
    from werkzeug.utils import secure_filename as _sec
    filename = _sec(filename)
    if not filename:
        abort(400)
    filepath = os.path.join(_DUMPS_DIR, filename)
    if not os.path.exists(filepath):
        abort(404)
    with open(filepath) as f:
        data = json.load(f)
    return jsonify(data)


@adapter_mapper_bp.route('/admin/adapters/system-types')
@login_required
def list_system_types():
    """Return available system types + their standard table definitions."""
    _admin_only()
    from ETL_JOBS.system_type_registry import get_all_types, SYSTEM_TYPES

    types = get_all_types()
    # Enrich with table names for preview
    for t in types:
        st = SYSTEM_TYPES[t['key']]
        t['tables'] = [
            {'name': tbl, 'columns': len(defn['columns']), 'description': defn['description']}
            for tbl, defn in st['tables'].items()
        ]
    return jsonify(types)


@adapter_mapper_bp.route('/admin/adapters/auto-map', methods=['POST'])
@login_required
def auto_map_endpoint():
    """
    Strict auto-mapper. Reads a dump file + system type,
    matches source columns against standard schema.
    Returns draft mapping JSON for human review.
    """
    _admin_only()
    data = request.get_json()
    dump_file   = (data.get('dump_file') or '').strip()
    system_type = (data.get('system_type') or '').strip().upper()

    from werkzeug.utils import secure_filename as _sec
    dump_file = _sec(dump_file or '')
    if not dump_file:
        return jsonify({'error': 'Valid dump_file is required'}), 400
    if not system_type:
        return jsonify({'error': 'system_type is required'}), 400

    filepath = os.path.join(_DUMPS_DIR, dump_file)
    if not os.path.abspath(filepath).startswith(os.path.abspath(_DUMPS_DIR)):
        return jsonify({'error': 'Invalid file path'}), 400
    if not os.path.exists(filepath):
        return jsonify({'error': f'Dump file not found: {dump_file}'}), 404

    with open(filepath) as f:
        dump = json.load(f)

    from ETL_JOBS.auto_mapper import auto_map
    mapping = auto_map(dump, system_type)

    # Count stats for the response
    total_mapped = sum(len(t['columns']) for t in mapping['tables'])
    total_unmapped_src = sum(len(t['unmapped_sources']) for t in mapping['tables'])
    total_unmapped_tgt = sum(len(t['unmapped_targets']) for t in mapping['tables'])

    return jsonify({
        'ok': True,
        'mapping': mapping,
        'stats': {
            'tables': len(mapping['tables']),
            'mapped_columns': total_mapped,
            'unmapped_sources': total_unmapped_src,
            'unmapped_targets': total_unmapped_tgt,
        },
    })


@adapter_mapper_bp.route('/admin/adapters/mapping/save', methods=['POST'])
@login_required
def save_mapping():
    _admin_only()
    _ensure_table()
    data = request.get_json()

    connection_name = data.get('connection_name', '').strip()
    schema_owner    = data.get('schema_owner', '').strip()
    dump_file       = data.get('dump_file', '').strip()
    mapping_json    = data.get('mapping_json')
    notes           = data.get('notes', '').strip()
    system_type     = data.get('system_type', '').strip().upper() or None
    mapping_id      = data.get('id')  # if editing existing

    if not connection_name or not mapping_json:
        return jsonify({'error': 'connection_name and mapping_json are required'}), 400

    # Validate it's valid JSON structure
    if isinstance(mapping_json, str):
        try:
            mapping_json = json.loads(mapping_json)
        except json.JSONDecodeError as e:
            return jsonify({'error': f'Invalid JSON: {e}'}), 400

    # Determine target DB name
    target_db = None
    if system_type:
        from ETL_JOBS.system_type_registry import SYSTEM_TYPES
        st = SYSTEM_TYPES.get(system_type)
        if st:
            target_db = f"rayd_{st['db_name_suffix']}"

    if mapping_id:
        db.session.execute(text("""
            UPDATE adapter_mappings
            SET mapping_json=:mj, notes=:n, system_type=:st, target_db=:td,
                status='draft', updated_at=NOW()
            WHERE id=:id
        """), {"mj": json.dumps(mapping_json), "n": notes, "st": system_type,
               "td": target_db, "id": mapping_id})
    else:
        db.session.execute(text("""
            INSERT INTO adapter_mappings
                (connection_name, schema_owner, dump_file, mapping_json, notes, system_type, target_db)
            VALUES (:cn, :so, :df, :mj, :n, :st, :td)
        """), {
            "cn": connection_name, "so": schema_owner,
            "df": dump_file, "mj": json.dumps(mapping_json), "n": notes,
            "st": system_type, "td": target_db
        })

    db.session.commit()
    return jsonify({'ok': True})


@adapter_mapper_bp.route('/admin/adapters/mapping/<int:mapping_id>/confirm', methods=['POST'])
@login_required
def confirm_mapping(mapping_id):
    _admin_only()

    # Get system type for this mapping
    row = db.session.execute(
        text("SELECT system_type FROM adapter_mappings WHERE id=:id"),
        {"id": mapping_id}
    ).fetchone()

    if not row:
        return jsonify({'error': 'Mapping not found'}), 404

    # Provision the target database if system_type is set
    db_result = None
    if row[0]:
        try:
            from ETL_JOBS.db_provisioner import ensure_database
            db_result = ensure_database(db.engine, row[0])
            logger.info(f"Database provisioning: {db_result}")
        except Exception as e:
            logger.error(f"Database provisioning failed: {e}")
            return jsonify({'error': f'Database creation failed: {e}'}), 500

    db.session.execute(
        text("UPDATE adapter_mappings SET status='confirmed', updated_at=NOW() WHERE id=:id"),
        {"id": mapping_id}
    )
    db.session.commit()

    result = {'ok': True}
    if db_result:
        result['database'] = db_result
    return jsonify(result)


@adapter_mapper_bp.route('/admin/adapters/mapping/<int:mapping_id>/delete', methods=['POST'])
@login_required
def delete_mapping(mapping_id):
    _admin_only()
    db.session.execute(text("DELETE FROM adapter_mappings WHERE id=:id"), {"id": mapping_id})
    db.session.commit()
    return jsonify({'ok': True})


@adapter_mapper_bp.route('/admin/adapters/mapping/<int:mapping_id>')
@login_required
def get_mapping(mapping_id):
    _admin_only()
    row = db.session.execute(
        text("SELECT * FROM adapter_mappings WHERE id=:id"), {"id": mapping_id}
    ).mappings().fetchone()
    if not row:
        abort(404)
    r = dict(row)
    r['created_at'] = str(r.get('created_at', ''))
    r['updated_at'] = str(r.get('updated_at', ''))
    return jsonify(r)
