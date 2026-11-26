"""
routes/report_presets.py — saved filter presets for the numbered reports (operator, 26 Nov:
"add same saved filters logic, from SU to report 22").

The Super Report's "Saved presets" (routes/super_report.py), made for any report on the shared
left panel (templates/_report_filter_panel.html): each person saves the panel as it stands
(dates, dropdowns, "Apply filter" ticks, Include / Exclude) under a name, sees their own list,
opens one (the page reloads with those filters, as Generate would) or deletes it. Report 22 first;
another report adds `{{ report_presets(NN) }}` under its panel and loads report_presets.js.

Stored in saved_reports like the Super Report's (base_report_id = the report's number, owner =
the person), in the database of the hospital picked, as every report's data is. A name saved
again replaces that preset. Only someone who may open the report may list or save its presets.
"""
import json
import re

from flask import Blueprint, abort, jsonify, request
from flask_login import current_user, login_required
from sqlalchemy import text

from db import db
from utils.roles import can_open_report

report_presets_bp = Blueprint('report_presets', __name__)

_KEY = re.compile(r'[A-Za-z0-9_]{1,60}')
MAX_KEYS, MAX_VALUES, MAX_LEN = 100, 300, 300


def _check(report_id):
    if not can_open_report(current_user, report_id):
        abort(403)


def clean_filters(raw):
    """The panel's fields as saved: {name: [values]}, plain names and short values only."""
    out = {}
    if not isinstance(raw, dict):
        return out
    for key, value in list(raw.items())[:MAX_KEYS]:
        if not _KEY.fullmatch(str(key)):
            continue
        values = value if isinstance(value, list) else [value]
        values = [str(v)[:MAX_LEN] for v in values if v not in (None, '')][:MAX_VALUES]
        if values:
            out[str(key)] = values
    return out


@report_presets_bp.route('/report/<int:report_id>/presets', methods=['GET'])
@login_required
def list_presets(report_id):
    _check(report_id)
    rows = db.session.execute(text("""
        SELECT id, name, filter_json, updated_at FROM saved_reports
         WHERE owner_user_id = :uid AND base_report_id = :rid
         ORDER BY updated_at DESC, id DESC
    """), {'uid': current_user.id, 'rid': report_id}).mappings().all()
    return jsonify([{
        'id': r['id'], 'name': r['name'],
        'filters': r['filter_json'] if isinstance(r['filter_json'], dict) else json.loads(r['filter_json'] or '{}'),
        'updated_at': r['updated_at'].strftime('%d/%m/%Y %H:%M') if r['updated_at'] else None,
    } for r in rows])


@report_presets_bp.route('/report/<int:report_id>/presets', methods=['POST'])
@login_required
def save_preset(report_id):
    _check(report_id)
    body = request.get_json(silent=True) or {}
    name = ' '.join(str(body.get('name') or '').split())[:120]
    if not name:
        return jsonify({'success': False, 'error': 'Name is required'}), 400
    filters = clean_filters(body.get('filters'))
    params = {'uid': current_user.id, 'rid': report_id, 'name': name, 'fj': json.dumps(filters)}
    saved_id = db.session.execute(text("""
        UPDATE saved_reports SET filter_json = CAST(:fj AS jsonb), updated_at = NOW()
         WHERE owner_user_id = :uid AND base_report_id = :rid AND name = :name
        RETURNING id
    """), params).scalar()
    if saved_id is None:
        saved_id = db.session.execute(text("""
            INSERT INTO saved_reports (name, owner_user_id, base_report_id, is_public, filter_json)
            VALUES (:name, :uid, :rid, FALSE, CAST(:fj AS jsonb)) RETURNING id
        """), params).scalar()
    db.session.commit()
    return jsonify({'success': True, 'id': saved_id, 'name': name})


@report_presets_bp.route('/report/<int:report_id>/presets/<int:preset_id>', methods=['DELETE'])
@login_required
def delete_preset(report_id, preset_id):
    _check(report_id)
    gone = db.session.execute(text("""
        DELETE FROM saved_reports WHERE id = :id AND owner_user_id = :uid AND base_report_id = :rid
    """), {'id': preset_id, 'uid': current_user.id, 'rid': report_id}).rowcount
    db.session.commit()
    if not gone:
        return jsonify({'success': False, 'error': 'Not found or not yours'}), 404
    return jsonify({'success': True})
