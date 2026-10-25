"""
routes/worklists.py — Dynamic Worklists (RAYD V2.0), replacing Custom Reports.

    /worklists                 the editor (saved tree · definition · SQL & results)
    /worklists/api/...         catalog, list, load, save, delete, versions,
                               compile, run, csv, options, shifts
    /reports/custom[/...]      the old Custom Reports address, now a redirect here

ACCESS
    page / run / personal worklists   the 'custom_reports' page permission (the key
                                      Custom Reports used, so licences, role defaults
                                      and existing grants carry over unchanged)
    edit a shared worklist            its owner, or su / implementation / administrator
    write SQL by hand                 su and implementation — the IT authors
    shift hours                       su / implementation / administrator

A personal worklist is visible to its owner only (and su). Every save is kept in
worklist_versions.
"""
import csv
import io
import json
import logging

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import text

from db import db, user_has_page
from utils import worklist_catalog as cat
from utils import worklist_engine as engine
from utils.worklist_engine import WorklistError

logger = logging.getLogger("WORKLISTS")

worklists_bp = Blueprint('worklists', __name__)

EDITOR_ROLES = ('su', 'implementation', 'administrator')
SQL_ROLES = ('su', 'implementation')


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role not in ('su', 'administrator') \
            and not user_has_page(current_user, 'custom_reports'):
        abort(403)


def _can_write_sql():
    return current_user.role in SQL_ROLES


def _can_edit(row):
    return current_user.role in EDITOR_ROLES or row['owner_id'] == current_user.id


def _can_see(row):
    return (row['visibility'] == 'shared' or row['owner_id'] == current_user.id
            or current_user.role == 'su')


def _row(worklist_id):
    r = db.session.execute(text("""
        SELECT w.*, u.username AS owner_name, ub.username AS updated_by_name
          FROM worklists w
          LEFT JOIN users u  ON u.id  = w.owner_id
          LEFT JOIN users ub ON ub.id = w.updated_by
         WHERE w.id = :id
    """), {'id': worklist_id}).mappings().first()
    if not r or not _can_see(r):
        abort(404)
    return dict(r)


def _body():
    return request.get_json(silent=True) or {}


def _error(msg, code=400):
    return jsonify({'error': str(msg)}), code


# ── the page ──────────────────────────────────────────────────────────────────

@worklists_bp.route('/worklists')
@login_required
def page():
    _require_access()
    return render_template('worklists.html', can_write_sql=_can_write_sql(),
                           can_edit_shifts=current_user.role in EDITOR_ROLES)


@worklists_bp.route('/reports/custom')
@worklists_bp.route('/reports/custom/<path:_rest>')
@login_required
def old_custom_reports(_rest=None):
    """Custom Reports is gone; its bookmarks land on the editor that replaced it."""
    return redirect(url_for('worklists.page'))


# ── catalog and library ───────────────────────────────────────────────────────

@worklists_bp.route('/worklists/api/catalog')
@login_required
def api_catalog():
    _require_access()
    out = cat.as_json()
    out['me'] = {'id': current_user.id, 'role': current_user.role,
                 'can_write_sql': _can_write_sql(),
                 'can_edit_shared': current_user.role in EDITOR_ROLES}
    return jsonify(out)


@worklists_bp.route('/worklists/api/list')
@login_required
def api_list():
    _require_access()
    rows = db.session.execute(text("""
        SELECT w.id, w.name, w.folder, w.visibility, w.dataset, w.version, w.owner_id,
               w.updated_at, u.username AS owner_name
          FROM worklists w
          LEFT JOIN users u ON u.id = w.owner_id
         WHERE w.visibility = 'shared' OR w.owner_id = :me OR :su
         ORDER BY w.visibility DESC, lower(w.folder), lower(w.name)
    """), {'me': current_user.id, 'su': current_user.role == 'su'}).mappings().all()
    return jsonify([{**dict(r), 'updated_at': r['updated_at'].isoformat(timespec='minutes'),
                     'can_edit': _can_edit(r)} for r in rows])


@worklists_bp.route('/worklists/api/<int:worklist_id>')
@login_required
def api_get(worklist_id):
    _require_access()
    r = _row(worklist_id)
    versions = db.session.execute(text("""
        SELECT v.version, v.name, v.saved_at, u.username AS saved_by
          FROM worklist_versions v LEFT JOIN users u ON u.id = v.saved_by
         WHERE v.worklist_id = :id ORDER BY v.version DESC
    """), {'id': worklist_id}).mappings().all()
    return jsonify({
        'id': r['id'], 'name': r['name'], 'description': r['description'],
        'folder': r['folder'], 'visibility': r['visibility'], 'dataset': r['dataset'],
        'definition': r['definition'], 'version': r['version'],
        'owner_name': r['owner_name'], 'updated_by_name': r['updated_by_name'],
        'updated_at': r['updated_at'].isoformat(timespec='minutes'),
        'can_edit': _can_edit(r),
        'versions': [{**dict(v), 'saved_at': v['saved_at'].isoformat(timespec='minutes')}
                     for v in versions],
    })


@worklists_bp.route('/worklists/api/<int:worklist_id>/versions/<int:version>')
@login_required
def api_version(worklist_id, version):
    _require_access()
    _row(worklist_id)
    v = db.session.execute(text("""
        SELECT name, definition FROM worklist_versions
         WHERE worklist_id = :id AND version = :v
    """), {'id': worklist_id, 'v': version}).mappings().first()
    if not v:
        abort(404)
    return jsonify(dict(v))


@worklists_bp.route('/worklists/api/save', methods=['POST'])
@login_required
def api_save():
    _require_access()
    b = _body()
    name = (b.get('name') or '').strip()[:200]
    folder = (b.get('folder') or 'General').strip()[:120] or 'General'
    visibility = b.get('visibility') or 'personal'
    defn = b.get('definition')
    if not name:
        return _error('a worklist needs a name')
    if visibility not in ('personal', 'shared'):
        return _error('visibility is personal or shared')
    if visibility == 'shared' and current_user.role not in EDITOR_ROLES:
        return _error('only su, implementation or administrator can share a worklist', 403)
    try:
        engine.validate_for_save(defn)
    except WorklistError as e:
        return _error(e)

    existing = _row(int(b['id'])) if b.get('id') else None
    if existing and not _can_edit(existing):
        return _error('you cannot edit this worklist — duplicate it into your personal folder', 403)
    # Only an IT author may introduce or change hand-written SQL. Anyone allowed to
    # edit may still rename or move a worklist that carries some.
    stored_sql = (existing or {}).get('definition', {}).get('sql') if existing else None
    if defn.get('sql') and defn.get('sql') != stored_sql and not _can_write_sql():
        return _error('only su or implementation users can write worklist SQL', 403)

    params = {'name': name, 'desc': (b.get('description') or '')[:2000], 'folder': folder,
              'vis': visibility, 'ds': defn['dataset'], 'def': json.dumps(defn),
              'me': current_user.id}
    try:
        if existing:
            row = db.session.execute(text("""
                UPDATE worklists SET name = :name, description = :desc, folder = :folder,
                       visibility = :vis, dataset = :ds, definition = CAST(:def AS jsonb),
                       version = version + 1, updated_at = NOW(), updated_by = :me
                 WHERE id = :id RETURNING id, version
            """), {**params, 'id': existing['id']}).first()
        else:
            row = db.session.execute(text("""
                INSERT INTO worklists (name, description, folder, visibility, owner_id,
                                       dataset, definition, updated_by)
                VALUES (:name, :desc, :folder, :vis, :me, :ds, CAST(:def AS jsonb), :me)
                RETURNING id, version
            """), params).first()
        db.session.execute(text("""
            INSERT INTO worklist_versions (worklist_id, version, name, definition, saved_by)
            VALUES (:id, :v, :name, CAST(:def AS jsonb), :me)
        """), {'id': row[0], 'v': row[1], 'name': name, 'def': params['def'],
               'me': current_user.id})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("worklist save failed")
        return _error('could not save the worklist', 500)
    return jsonify({'status': 'ok', 'id': row[0], 'version': row[1]})


@worklists_bp.route('/worklists/api/<int:worklist_id>/delete', methods=['POST'])
@login_required
def api_delete(worklist_id):
    _require_access()
    r = _row(worklist_id)
    if not _can_edit(r):
        return _error('you cannot delete this worklist', 403)
    db.session.execute(text("DELETE FROM worklists WHERE id = :id"), {'id': worklist_id})
    db.session.commit()
    return jsonify({'status': 'ok'})


# ── compile, run, export ──────────────────────────────────────────────────────

def _compiled_or_error(defn):
    try:
        return engine.compile_definition(defn), None
    except WorklistError as e:
        return None, _error(e)


def _hand_sql(defn):
    """Hand SQL in a definition runs for everyone; only writing it is restricted."""
    return (defn or {}).get('sql') or None


@worklists_bp.route('/worklists/api/compile', methods=['POST'])
@login_required
def api_compile():
    _require_access()
    defn = _body().get('definition')
    compiled, err = _compiled_or_error(defn)
    if err:
        return err
    return jsonify({'sql': compiled['sql'], 'params': engine.display_params(compiled['params']),
                    'editable_sql': engine.inline_sql(compiled)})


@worklists_bp.route('/worklists/api/run', methods=['POST'])
@login_required
def api_run():
    _require_access()
    defn = _body().get('definition')
    compiled, err = _compiled_or_error(defn)
    if err:
        return err
    hand = _hand_sql(defn)
    try:
        res = engine.run(compiled=None if hand else compiled, hand_sql=hand)
    except WorklistError as e:
        return _error(e)
    res.update({'kind': 'list' if hand else compiled['kind'],
                'dims': [] if hand else compiled['dims'],
                'measures': [] if hand else compiled['measures'],
                'column_labels': None if hand else compiled['columns'],
                'hand_sql': bool(hand), 'cap': engine.ROW_CAP})
    return jsonify(res)


@worklists_bp.route('/worklists/api/csv', methods=['POST'])
@login_required
def api_csv():
    _require_access()
    b = _body()
    defn = b.get('definition')
    compiled, err = _compiled_or_error(defn)
    if err:
        return err
    try:
        res = engine.run(compiled=None if _hand_sql(defn) else compiled,
                         hand_sql=_hand_sql(defn), cap=engine.CSV_CAP)
    except WorklistError as e:
        return _error(e)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(res['columns'])
    w.writerows(res['rows'])
    name = ''.join(ch if ch.isalnum() or ch in '-_ ' else '_'
                   for ch in (b.get('name') or 'worklist')).strip() or 'worklist'
    return Response('﻿' + buf.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename="{name}.csv"',
                             'X-Truncated': '1' if res['truncated'] else '0'})


@worklists_bp.route('/worklists/api/options')
@login_required
def api_options():
    _require_access()
    try:
        return jsonify(engine.options(request.args.get('dataset'), request.args.get('field')))
    except WorklistError as e:
        return _error(e)


# ── shift hours ───────────────────────────────────────────────────────────────

@worklists_bp.route('/worklists/api/shifts', methods=['GET', 'POST'])
@login_required
def api_shifts():
    _require_access()
    if request.method == 'POST':
        if current_user.role not in EDITOR_ROLES:
            return _error('only su, implementation or administrator can change shift hours', 403)
        shifts = _body().get('shifts')
        if not isinstance(shifts, list) or not shifts:
            return _error('send at least one shift')
        clean = []
        for i, s in enumerate(shifts):
            try:
                name = str(s['name']).strip()[:30]
                start, end = int(s['start_hour']), int(s['end_hour'])
            except (KeyError, TypeError, ValueError):
                return _error('each shift needs a name, a start hour and an end hour')
            if not name or not 0 <= start <= 23 or not 0 <= end <= 24:
                return _error(f'{name or "a shift"}: hours run 0–23 (end may be 24)')
            clean.append((name, start, end, i + 1))
        if len({c[0] for c in clean}) != len(clean):
            return _error('shift names must be unique')
        db.session.execute(text("DELETE FROM worklist_shifts"))
        for name, start, end, order in clean:
            db.session.execute(text("""
                INSERT INTO worklist_shifts (name, start_hour, end_hour, sort_order)
                VALUES (:n, :s, :e, :o)
            """), {'n': name, 's': start, 'e': end, 'o': order})
        db.session.commit()
    rows = db.session.execute(text(
        "SELECT name, start_hour, end_hour FROM worklist_shifts ORDER BY sort_order"
    )).mappings().all()
    return jsonify([dict(r) for r in rows])
