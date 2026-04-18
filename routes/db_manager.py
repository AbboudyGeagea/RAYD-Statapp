import os
import sys
import json
import glob
import subprocess
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, abort, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

_ETL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL_JOBS')
if _ETL_DIR not in sys.path:
    sys.path.insert(0, _ETL_DIR)

_DUMPS_DIR = os.path.join(_ETL_DIR, 'schema_dumps')

db_manager_bp = Blueprint('db_manager', __name__)
logger = logging.getLogger("DB_MANAGER")

PRIMARY_CONN = 'oracle_PACS'   # protected — cannot be deleted


def _guard():
    if current_user.role != 'admin':
        abort(403)


def _ensure_mappings_table():
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
            target_action   VARCHAR(20) DEFAULT 'provision',
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        )
    """))
    for col, typedef in [
        ('system_type',   'VARCHAR(20)'),
        ('target_db',     'VARCHAR(100)'),
        ('target_action', "VARCHAR(20) DEFAULT 'provision'"),
    ]:
        db.session.execute(text(
            f"ALTER TABLE adapter_mappings ADD COLUMN IF NOT EXISTS {col} {typedef}"
        ))
    db.session.execute(text(
        "ALTER TABLE db_params ADD COLUMN IF NOT EXISTS owner VARCHAR(100)"
    ))
    db.session.commit()


# ── Main page ────────────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager')
@login_required
def db_manager_page():
    _guard()
    _ensure_mappings_table()

    connections = db.session.execute(
        text("SELECT * FROM db_params ORDER BY name")
    ).mappings().fetchall()

    mappings = db.session.execute(
        text("""SELECT id, connection_name, schema_owner, status, dump_file,
                       system_type, target_db, target_action, created_at, updated_at
                FROM adapter_mappings ORDER BY updated_at DESC""")
    ).mappings().fetchall()

    os.makedirs(_DUMPS_DIR, exist_ok=True)
    dump_files = sorted(
        [os.path.basename(f) for f in glob.glob(os.path.join(_DUMPS_DIR, '*.json'))],
        reverse=True
    )

    from ETL_JOBS.schema_discovery import check_drivers, DRIVER_REGISTRY
    drivers = check_drivers()

    try:
        from ETL_JOBS.system_type_registry import get_all_types
        system_types = get_all_types()
    except Exception:
        system_types = []

    primary = next((dict(c) for c in connections if c['name'] == PRIMARY_CONN), None)

    return render_template(
        'db_manager.html',
        connections=[dict(c) for c in connections],
        mappings=[dict(m) for m in mappings],
        dump_files=dump_files,
        drivers=drivers,
        driver_registry=DRIVER_REGISTRY,
        system_types=system_types,
        primary_conn=PRIMARY_CONN,
        primary=primary,
    )


# ── Connection CRUD ──────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager/connections/add', methods=['POST'])
@login_required
def add_connection():
    _guard()
    from utils.crypto import encrypt
    data = request.get_json() or {}

    name     = (data.get('name') or '').strip()
    db_type  = (data.get('db_type') or '').strip().lower()
    host     = (data.get('host') or '').strip()
    port     = data.get('port')
    sid      = (data.get('sid') or '').strip()
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    owner    = (data.get('owner') or '').strip()
    mode     = (data.get('mode') or '').strip().upper() or None

    if not name or not db_type or not host:
        return jsonify({'error': 'name, db_type and host are required'}), 400

    existing = db.session.execute(
        text("SELECT id FROM db_params WHERE name = :n"), {"n": name}
    ).fetchone()
    if existing:
        return jsonify({'error': f"Connection '{name}' already exists"}), 409

    enc_pw = encrypt(password) if password else ''
    db.session.execute(text("""
        INSERT INTO db_params (name, db_role, db_type, host, port, sid, username, password, mode, owner)
        VALUES (:name, 'source', :db_type, :host, :port, :sid, :username, :password, :mode, :owner)
    """), {
        "name": name, "db_type": db_type, "host": host,
        "port": int(port) if port else None,
        "sid": sid, "username": username, "password": enc_pw,
        "mode": mode, "owner": owner,
    })
    db.session.commit()
    return jsonify({'ok': True})


@db_manager_bp.route('/admin/db-manager/connections/<int:conn_id>/update', methods=['POST'])
@login_required
def update_connection(conn_id):
    _guard()
    from utils.crypto import encrypt, decrypt
    data = request.get_json() or {}

    row = db.session.execute(
        text("SELECT * FROM db_params WHERE id = :id"), {"id": conn_id}
    ).mappings().fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404

    is_primary = row['name'] == PRIMARY_CONN

    host     = (data.get('host') or '').strip()
    owner    = (data.get('owner') or '').strip()
    password_raw = (data.get('password') or '').strip()

    if is_primary:
        # Primary: only host, owner and password editable
        if not host:
            return jsonify({'error': 'Host is required'}), 400
        enc_pw = encrypt(password_raw) if password_raw else row['password']
        db.session.execute(text("""
            UPDATE db_params SET host=:host, owner=:owner, password=:pw, updated_at=NOW()
            WHERE id=:id
        """), {"host": host, "owner": owner, "pw": enc_pw, "id": conn_id})
    else:
        db_type  = (data.get('db_type') or row['db_type'] or '').strip().lower()
        port     = data.get('port')
        sid      = (data.get('sid') or '').strip()
        username = (data.get('username') or '').strip()
        mode     = (data.get('mode') or '').strip().upper() or None
        enc_pw   = encrypt(password_raw) if password_raw else row['password']

        db.session.execute(text("""
            UPDATE db_params
            SET host=:host, db_type=:db_type, port=:port, sid=:sid,
                username=:username, password=:pw, mode=:mode, owner=:owner, updated_at=NOW()
            WHERE id=:id
        """), {
            "host": host, "db_type": db_type,
            "port": int(port) if port else row['port'],
            "sid": sid or row['sid'], "username": username or row['username'],
            "pw": enc_pw, "mode": mode, "owner": owner, "id": conn_id,
        })

    db.session.commit()
    return jsonify({'ok': True})


@db_manager_bp.route('/admin/db-manager/connections/<int:conn_id>/delete', methods=['POST'])
@login_required
def delete_connection(conn_id):
    _guard()
    row = db.session.execute(
        text("SELECT name FROM db_params WHERE id = :id"), {"id": conn_id}
    ).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    if row[0] == PRIMARY_CONN:
        return jsonify({'error': 'Primary PACS connection cannot be deleted'}), 403
    db.session.execute(text("DELETE FROM db_params WHERE id = :id"), {"id": conn_id})
    db.session.commit()
    return jsonify({'ok': True})


@db_manager_bp.route('/admin/db-manager/connections/<int:conn_id>/test', methods=['POST'])
@login_required
def test_connection(conn_id):
    _guard()
    row = db.session.execute(
        text("SELECT name FROM db_params WHERE id = :id"), {"id": conn_id}
    ).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404

    from ETL_JOBS.schema_discovery import test_connection as _test
    ok, msg = _test(db.engine, row[0])
    return jsonify({'ok': ok, 'message': msg})


# ── Driver install ───────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager/drivers/install', methods=['POST'])
@login_required
def install_driver():
    _guard()
    data    = request.get_json() or {}
    package = (data.get('pip') or '').strip()
    if not package or any(c in package for c in (';', '&', '|', '`', '$')):
        return jsonify({'error': 'Invalid package name'}), 400

    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', package],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            return jsonify({'ok': True, 'output': result.stdout[-500:]})
        return jsonify({'ok': False, 'error': result.stderr[-500:]}), 500
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Install timed out after 120 s'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Schema discovery ─────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager/discover', methods=['POST'])
@login_required
def discover_schema():
    _guard()
    data            = request.get_json() or {}
    connection_name = (data.get('connection_name') or '').strip()
    schema_owner    = (data.get('schema_owner') or '').strip()

    if not connection_name or not schema_owner:
        return jsonify({'error': 'connection_name and schema_owner are required'}), 400

    try:
        from ETL_JOBS.schema_discovery import run_discovery
        result = run_discovery(db.engine, connection_name, schema_owner, _DUMPS_DIR)
        return jsonify({'ok': True, **result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Auto-map ─────────────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager/auto-map', methods=['POST'])
@login_required
def auto_map():
    _guard()
    data        = request.get_json() or {}
    dump_file   = (data.get('dump_file') or '').strip()
    system_type = (data.get('system_type') or '').strip().upper()

    if not dump_file or '..' in dump_file or '/' in dump_file:
        return jsonify({'error': 'Valid dump_file required'}), 400
    if not system_type:
        return jsonify({'error': 'system_type required'}), 400

    filepath = os.path.join(_DUMPS_DIR, dump_file)
    if not os.path.exists(filepath):
        return jsonify({'error': f'Dump not found: {dump_file}'}), 404

    with open(filepath) as f:
        dump = json.load(f)

    from ETL_JOBS.auto_mapper import auto_map as _auto_map
    mapping = _auto_map(dump, system_type)

    return jsonify({
        'ok': True,
        'mapping': mapping,
        'stats': {
            'tables': len(mapping['tables']),
            'mapped_columns':    sum(len(t['columns']) for t in mapping['tables']),
            'unmapped_sources':  sum(len(t['unmapped_sources']) for t in mapping['tables']),
            'unmapped_targets':  sum(len(t['unmapped_targets']) for t in mapping['tables']),
        },
    })


# ── Mapping CRUD ─────────────────────────────────────────────────────────────

@db_manager_bp.route('/admin/db-manager/mapping/save', methods=['POST'])
@login_required
def save_mapping():
    _guard()
    _ensure_mappings_table()
    data = request.get_json() or {}

    connection_name = (data.get('connection_name') or '').strip()
    schema_owner    = (data.get('schema_owner') or '').strip()
    dump_file       = (data.get('dump_file') or '').strip()
    mapping_json    = data.get('mapping_json')
    notes           = (data.get('notes') or '').strip()
    system_type     = (data.get('system_type') or '').strip().upper() or None
    target_action   = (data.get('target_action') or 'provision').strip()
    mapping_id      = data.get('id')

    if not connection_name or not mapping_json:
        return jsonify({'error': 'connection_name and mapping_json required'}), 400
    if isinstance(mapping_json, str):
        mapping_json = json.loads(mapping_json)

    target_db = None
    if system_type:
        try:
            from ETL_JOBS.system_type_registry import SYSTEM_TYPES
            st = SYSTEM_TYPES.get(system_type)
            if st:
                target_db = f"rayd_{st['db_name_suffix']}"
        except Exception:
            pass

    params = {
        "mj": json.dumps(mapping_json), "n": notes, "st": system_type,
        "td": target_db, "ta": target_action,
    }
    if mapping_id:
        params["id"] = mapping_id
        db.session.execute(text("""
            UPDATE adapter_mappings
            SET mapping_json=:mj, notes=:n, system_type=:st,
                target_db=:td, target_action=:ta, status='draft', updated_at=NOW()
            WHERE id=:id
        """), params)
    else:
        params.update({"cn": connection_name, "so": schema_owner, "df": dump_file})
        db.session.execute(text("""
            INSERT INTO adapter_mappings
                (connection_name, schema_owner, dump_file, mapping_json,
                 notes, system_type, target_db, target_action)
            VALUES (:cn, :so, :df, :mj, :n, :st, :td, :ta)
        """), params)

    db.session.commit()
    return jsonify({'ok': True})


@db_manager_bp.route('/admin/db-manager/mapping/<int:mapping_id>/confirm', methods=['POST'])
@login_required
def confirm_mapping(mapping_id):
    _guard()
    row = db.session.execute(
        text("SELECT system_type, target_action FROM adapter_mappings WHERE id=:id"),
        {"id": mapping_id}
    ).fetchone()
    if not row:
        return jsonify({'error': 'Mapping not found'}), 404

    system_type, target_action = row
    result = {}

    if target_action == 'provision' and system_type:
        try:
            from ETL_JOBS.db_provisioner import ensure_database
            result['database'] = ensure_database(db.engine, system_type)
        except Exception as e:
            return jsonify({'error': f'Provisioning failed: {e}'}), 500

    db.session.execute(
        text("UPDATE adapter_mappings SET status='confirmed', updated_at=NOW() WHERE id=:id"),
        {"id": mapping_id}
    )
    db.session.commit()
    return jsonify({'ok': True, **result})


@db_manager_bp.route('/admin/db-manager/mapping/<int:mapping_id>/delete', methods=['POST'])
@login_required
def delete_mapping(mapping_id):
    _guard()
    db.session.execute(text("DELETE FROM adapter_mappings WHERE id=:id"), {"id": mapping_id})
    db.session.commit()
    return jsonify({'ok': True})


@db_manager_bp.route('/admin/db-manager/mapping/<int:mapping_id>')
@login_required
def get_mapping(mapping_id):
    _guard()
    row = db.session.execute(
        text("SELECT * FROM adapter_mappings WHERE id=:id"), {"id": mapping_id}
    ).mappings().fetchone()
    if not row:
        abort(404)
    r = dict(row)
    r['created_at'] = str(r.get('created_at', ''))
    r['updated_at'] = str(r.get('updated_at', ''))
    return jsonify(r)


@db_manager_bp.route('/admin/db-manager/dump/<filename>')
@login_required
def view_dump(filename):
    _guard()
    from werkzeug.utils import secure_filename as _sec
    filename = _sec(filename)
    if not filename:
        abort(400)
    filepath = os.path.join(_DUMPS_DIR, filename)
    if not os.path.exists(filepath):
        abort(404)
    with open(filepath) as f:
        return jsonify(json.load(f))
