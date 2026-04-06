import os
import json
import glob
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

adapter_mapper_bp = Blueprint('adapter_mapper', __name__)

_DUMPS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL_JOBS', 'schema_dumps')

def _admin_only():
    if current_user.role != 'admin':
        abort(403)

def _ensure_table():
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS adapter_mappings (
            id            SERIAL PRIMARY KEY,
            connection_name VARCHAR(100) NOT NULL,
            schema_owner  VARCHAR(100),
            dump_file     VARCHAR(255),
            mapping_json  JSONB,
            notes         TEXT,
            status        VARCHAR(20) DEFAULT 'draft',
            created_at    TIMESTAMP DEFAULT NOW(),
            updated_at    TIMESTAMP DEFAULT NOW()
        )
    """))
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
        text("SELECT id, connection_name, schema_owner, status, dump_file, created_at, updated_at FROM adapter_mappings ORDER BY updated_at DESC")
    ).mappings().fetchall()

    # Available dump files
    os.makedirs(_DUMPS_DIR, exist_ok=True)
    dump_files = sorted(
        [os.path.basename(f) for f in glob.glob(os.path.join(_DUMPS_DIR, '*.json'))],
        reverse=True
    )

    return render_template(
        'adapter_mapper.html',
        connections=[dict(c) for c in connections],
        mappings=[dict(m) for m in mappings],
        dump_files=dump_files,
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
    # Prevent path traversal
    if '..' in filename or '/' in filename:
        abort(400)
    filepath = os.path.join(_DUMPS_DIR, filename)
    if not os.path.exists(filepath):
        abort(404)
    with open(filepath) as f:
        data = json.load(f)
    return jsonify(data)


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
    mapping_id      = data.get('id')  # if editing existing

    if not connection_name or not mapping_json:
        return jsonify({'error': 'connection_name and mapping_json are required'}), 400

    # Validate it's valid JSON structure
    if isinstance(mapping_json, str):
        try:
            mapping_json = json.loads(mapping_json)
        except json.JSONDecodeError as e:
            return jsonify({'error': f'Invalid JSON: {e}'}), 400

    if mapping_id:
        db.session.execute(text("""
            UPDATE adapter_mappings
            SET mapping_json=:mj, notes=:n, status='draft', updated_at=NOW()
            WHERE id=:id
        """), {"mj": json.dumps(mapping_json), "n": notes, "id": mapping_id})
    else:
        db.session.execute(text("""
            INSERT INTO adapter_mappings (connection_name, schema_owner, dump_file, mapping_json, notes)
            VALUES (:cn, :so, :df, :mj, :n)
        """), {
            "cn": connection_name, "so": schema_owner,
            "df": dump_file, "mj": json.dumps(mapping_json), "n": notes
        })

    db.session.commit()
    return jsonify({'ok': True})


@adapter_mapper_bp.route('/admin/adapters/mapping/<int:mapping_id>/confirm', methods=['POST'])
@login_required
def confirm_mapping(mapping_id):
    _admin_only()
    db.session.execute(
        text("UPDATE adapter_mappings SET status='confirmed', updated_at=NOW() WHERE id=:id"),
        {"id": mapping_id}
    )
    db.session.commit()
    return jsonify({'ok': True})


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
