from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db
from utils.financial import effective_rate, invalidate_cache

financial_config_bp = Blueprint('financial_config', __name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _full_config() -> dict:
    rows = db.session.execute(text(
        "SELECT id, entity_type, entity_id, usd_per_rvu, notes, updated_at "
        "FROM financial_config ORDER BY entity_type, entity_id NULLS FIRST"
    )).fetchall()

    result: dict = {'global_rate': None, 'modality': [], 'procedure': []}
    for row in rows:
        r = dict(row._mapping)
        r['usd_per_rvu'] = float(r['usd_per_rvu'])
        r['updated_at']  = r['updated_at'].isoformat() if r['updated_at'] else None
        if r['entity_type'] == 'global':
            result['global_rate'] = r
        elif r['entity_type'] == 'modality':
            result['modality'].append(r)
        elif r['entity_type'] == 'procedure':
            result['procedure'].append(r)

    if result['global_rate'] is None:
        result['global_rate'] = {
            'usd_per_rvu': 40.0, 'notes': '', 'updated_at': None, 'entity_type': 'global',
        }
    return result


def _audit(action: str, entity_type: str, entity_id, old_val, new_val) -> None:
    db.session.execute(text(
        "INSERT INTO financial_audit_log "
        "(user_id, user_name, action, entity_type, entity_id, old_value, new_value, ip_address) "
        "VALUES (:uid, :uname, :action, :etype, :eid, :old, :new, :ip)"
    ), {
        'uid':    current_user.id,
        'uname':  current_user.username,
        'action': action,
        'etype':  entity_type,
        'eid':    entity_id,
        'old':    old_val,
        'new':    new_val,
        'ip':     request.remote_addr,
    })


def _validate_rate(body: dict) -> tuple:
    try:
        rate = float(body.get('usd_per_rvu', ''))
    except (TypeError, ValueError):
        return None, 'usd_per_rvu must be a number'
    if not (0 < rate < 1000):
        return None, 'usd_per_rvu must be between 0.01 and 999.99'
    return rate, None


# ── Page ───────────────────────────────────────────────────────────────────────

@financial_config_bp.route('/admin/financial-config')
@login_required
def financial_config_page():
    if current_user.role != 'admin':
        abort(403)
    fin_config = _full_config()
    return render_template('financial_config.html', fin_config=fin_config)


# ── Read API ───────────────────────────────────────────────────────────────────

@financial_config_bp.route('/api/financial/config')
@login_required
def api_get_config():
    if current_user.role != 'admin':
        abort(403)
    return jsonify(_full_config())


@financial_config_bp.route('/api/financial/preview')
@login_required
def api_preview():
    if current_user.role != 'admin':
        abort(403)
    modality = request.args.get('modality') or None
    proc     = request.args.get('procedure_code') or None
    rate     = effective_rate(modality, proc)
    return jsonify({'modality': modality, 'procedure_code': proc, 'usd_per_rvu': rate})


# ── Write API ──────────────────────────────────────────────────────────────────

@financial_config_bp.route('/api/financial/config/global', methods=['POST'])
@login_required
def api_set_global():
    if current_user.role != 'admin':
        abort(403)
    body = request.get_json(force=True) or {}
    rate, err = _validate_rate(body)
    if err:
        return jsonify(error=err), 400
    notes = str(body.get('notes', ''))[:500]

    old_row = db.session.execute(
        text("SELECT usd_per_rvu FROM financial_config WHERE entity_type = 'global'")
    ).fetchone()
    old_val = float(old_row[0]) if old_row else None

    if old_row:
        db.session.execute(text(
            "UPDATE financial_config SET usd_per_rvu=:rate, notes=:notes, updated_at=now() "
            "WHERE entity_type='global'"
        ), {'rate': rate, 'notes': notes})
        code = 200
    else:
        db.session.execute(text(
            "INSERT INTO financial_config (entity_type, entity_id, usd_per_rvu, notes) "
            "VALUES ('global', NULL, :rate, :notes)"
        ), {'rate': rate, 'notes': notes})
        code = 201

    _audit('SET_GLOBAL', 'global', None, old_val, rate)
    db.session.commit()
    invalidate_cache()
    return jsonify(usd_per_rvu=rate), code


@financial_config_bp.route('/api/financial/config/modality', methods=['POST'])
@login_required
def api_set_modality():
    if current_user.role != 'admin':
        abort(403)
    body     = request.get_json(force=True) or {}
    modality = str(body.get('modality', '')).upper().strip()
    if not modality:
        return jsonify(error='modality is required'), 400
    rate, err = _validate_rate(body)
    if err:
        return jsonify(error=err), 400
    notes = str(body.get('notes', ''))[:500]

    old_row = db.session.execute(
        text("SELECT usd_per_rvu FROM financial_config "
             "WHERE entity_type='modality' AND entity_id=:eid"),
        {'eid': modality}
    ).fetchone()
    old_val = float(old_row[0]) if old_row else None

    if old_row:
        db.session.execute(text(
            "UPDATE financial_config SET usd_per_rvu=:rate, notes=:notes, updated_at=now() "
            "WHERE entity_type='modality' AND entity_id=:eid"
        ), {'rate': rate, 'notes': notes, 'eid': modality})
        code = 200
    else:
        db.session.execute(text(
            "INSERT INTO financial_config (entity_type, entity_id, usd_per_rvu, notes) "
            "VALUES ('modality', :eid, :rate, :notes)"
        ), {'eid': modality, 'rate': rate, 'notes': notes})
        code = 201

    _audit('SET_MODALITY', 'modality', modality, old_val, rate)
    db.session.commit()
    invalidate_cache()
    return jsonify(modality=modality, usd_per_rvu=rate), code


@financial_config_bp.route('/api/financial/config/modality/<modality>', methods=['DELETE'])
@login_required
def api_delete_modality(modality):
    if current_user.role != 'admin':
        abort(403)
    modality = modality.upper().strip()
    old_row = db.session.execute(
        text("SELECT usd_per_rvu FROM financial_config "
             "WHERE entity_type='modality' AND entity_id=:eid"),
        {'eid': modality}
    ).fetchone()
    if not old_row:
        return jsonify(error='Override not found'), 404

    db.session.execute(
        text("DELETE FROM financial_config WHERE entity_type='modality' AND entity_id=:eid"),
        {'eid': modality}
    )
    _audit('DELETE_MODALITY', 'modality', modality, float(old_row[0]), None)
    db.session.commit()
    invalidate_cache()
    return '', 204


@financial_config_bp.route('/api/financial/config/procedure', methods=['POST'])
@login_required
def api_set_procedure():
    if current_user.role != 'admin':
        abort(403)
    body  = request.get_json(force=True) or {}
    pcode = str(body.get('procedure_code', '')).upper().strip()
    if not pcode:
        return jsonify(error='procedure_code is required'), 400
    rate, err = _validate_rate(body)
    if err:
        return jsonify(error=err), 400
    notes = str(body.get('notes', ''))[:500]

    old_row = db.session.execute(
        text("SELECT usd_per_rvu FROM financial_config "
             "WHERE entity_type='procedure' AND entity_id=:eid"),
        {'eid': pcode}
    ).fetchone()
    old_val = float(old_row[0]) if old_row else None

    if old_row:
        db.session.execute(text(
            "UPDATE financial_config SET usd_per_rvu=:rate, notes=:notes, updated_at=now() "
            "WHERE entity_type='procedure' AND entity_id=:eid"
        ), {'rate': rate, 'notes': notes, 'eid': pcode})
        code = 200
    else:
        db.session.execute(text(
            "INSERT INTO financial_config (entity_type, entity_id, usd_per_rvu, notes) "
            "VALUES ('procedure', :eid, :rate, :notes)"
        ), {'eid': pcode, 'rate': rate, 'notes': notes})
        code = 201

    _audit('SET_PROCEDURE', 'procedure', pcode, old_val, rate)
    db.session.commit()
    invalidate_cache()
    return jsonify(procedure_code=pcode, usd_per_rvu=rate), code


@financial_config_bp.route('/api/financial/config/procedure/<procedure_code>', methods=['DELETE'])
@login_required
def api_delete_procedure(procedure_code):
    if current_user.role != 'admin':
        abort(403)
    pcode = procedure_code.upper().strip()
    old_row = db.session.execute(
        text("SELECT usd_per_rvu FROM financial_config "
             "WHERE entity_type='procedure' AND entity_id=:eid"),
        {'eid': pcode}
    ).fetchone()
    if not old_row:
        return jsonify(error='Override not found'), 404

    db.session.execute(
        text("DELETE FROM financial_config WHERE entity_type='procedure' AND entity_id=:eid"),
        {'eid': pcode}
    )
    _audit('DELETE_PROCEDURE', 'procedure', pcode, float(old_row[0]), None)
    db.session.commit()
    invalidate_cache()
    return '', 204


# ── Audit API ──────────────────────────────────────────────────────────────────

@financial_config_bp.route('/api/audit/financial')
@login_required
def api_audit_log():
    if current_user.role != 'admin':
        abort(403)
    rows = db.session.execute(text(
        "SELECT id, user_name, action, entity_type, entity_id, "
        "       old_value, new_value, ip_address, created_at "
        "FROM financial_audit_log ORDER BY created_at DESC LIMIT 100"
    )).fetchall()
    result = []
    for row in rows:
        r = dict(row._mapping)
        r['created_at'] = r['created_at'].isoformat() if r['created_at'] else None
        r['old_value']  = float(r['old_value'])  if r['old_value']  is not None else None
        r['new_value']  = float(r['new_value'])  if r['new_value']  is not None else None
        result.append(r)
    return jsonify(result)
