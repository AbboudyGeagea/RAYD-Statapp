"""
routes/groups_route.py — Permission group management (admin only).

Endpoints
---------
GET  /admin/groups                     — render admin page
GET  /admin/groups/api                 — list all groups (JSON)
POST /admin/groups/api                 — create group
PUT  /admin/groups/api/<id>            — update group name/desc/permissions
DEL  /admin/groups/api/<id>            — delete group (unlinks members first)
GET  /admin/groups/users               — all users with group + override info
POST /admin/users/<uid>/group          — assign user to a group (or clear)
POST /admin/users/<uid>/overrides      — set per-user permission overrides
GET  /admin/users/<uid>/overrides      — get current overrides for a user
"""

from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from db import db, PermissionGroup, User
from utils.permissions import ALL_PERMISSIONS, ALL_PERMISSION_KEYS

groups_bp = Blueprint('groups', __name__)


def _admin_only():
    if not current_user.is_authenticated or current_user.role != 'admin':
        abort(403)


# ── Page ──────────────────────────────────────────────────────────────────────
@groups_bp.route('/admin/groups')
@login_required
def groups_page():
    _admin_only()
    return render_template('permission_groups.html', all_permissions=ALL_PERMISSIONS)


# ── Group CRUD ────────────────────────────────────────────────────────────────
@groups_bp.route('/admin/groups/api', methods=['GET'])
@login_required
def groups_list():
    _admin_only()
    groups = PermissionGroup.query.order_by(PermissionGroup.name).all()
    return jsonify([_group_dict(g) for g in groups])


@groups_bp.route('/admin/groups/api', methods=['POST'])
@login_required
def groups_create():
    _admin_only()
    data = request.get_json(force=True)
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    if PermissionGroup.query.filter_by(name=name).first():
        return jsonify({'error': 'A group with that name already exists'}), 409

    perms = _build_permissions(data.get('permissions', {}))
    g = PermissionGroup(
        name=name,
        description=(data.get('description') or '').strip(),
        permissions=perms,
    )
    db.session.add(g)
    db.session.commit()
    return jsonify(_group_dict(g)), 201


@groups_bp.route('/admin/groups/api/<int:gid>', methods=['PUT'])
@login_required
def groups_update(gid):
    _admin_only()
    g = PermissionGroup.query.get_or_404(gid)
    data = request.get_json(force=True)

    if 'name' in data:
        name = data['name'].strip()
        clash = PermissionGroup.query.filter_by(name=name).first()
        if clash and clash.id != gid:
            return jsonify({'error': 'Name already taken'}), 409
        g.name = name

    if 'description' in data:
        g.description = (data['description'] or '').strip()

    if 'permissions' in data:
        g.permissions = _build_permissions(data['permissions'])

    db.session.commit()
    return jsonify(_group_dict(g))


@groups_bp.route('/admin/groups/api/<int:gid>', methods=['DELETE'])
@login_required
def groups_delete(gid):
    _admin_only()
    g = PermissionGroup.query.get_or_404(gid)
    User.query.filter_by(group_id=gid).update({'group_id': None},
                                               synchronize_session='fetch')
    db.session.delete(g)
    db.session.commit()
    return jsonify({'ok': True})


# ── User ↔ Group / Overrides ──────────────────────────────────────────────────
@groups_bp.route('/admin/groups/users', methods=['GET'])
@login_required
def users_for_groups():
    _admin_only()
    users = (User.query
             .filter(User.status != 'disabled')
             .order_by(User.username)
             .all())
    groups = PermissionGroup.query.order_by(PermissionGroup.name).all()
    return jsonify({
        'users': [_user_dict(u) for u in users],
        'groups': [{'id': g.id, 'name': g.name} for g in groups],
    })


@groups_bp.route('/admin/users/<int:uid>/group', methods=['POST'])
@login_required
def assign_user_group(uid):
    _admin_only()
    user = User.query.get_or_404(uid)
    data = request.get_json(force=True)
    gid = data.get('group_id')
    user.group_id = int(gid) if gid else None
    db.session.commit()
    return jsonify({'ok': True, 'group_id': user.group_id})


@groups_bp.route('/admin/users/<int:uid>/overrides', methods=['GET'])
@login_required
def get_user_overrides(uid):
    _admin_only()
    user = User.query.get_or_404(uid)
    return jsonify({
        'group_id':         user.group_id,
        'group_name':       user.group.name if user.group else None,
        'group_permissions': user.group.permissions if user.group else {},
        'overrides':        user.permission_overrides or {},
    })


@groups_bp.route('/admin/users/<int:uid>/overrides', methods=['POST'])
@login_required
def set_user_overrides(uid):
    _admin_only()
    user = User.query.get_or_404(uid)
    data = request.get_json(force=True)
    # Only store keys the caller sent; absent keys are cleared
    overrides = {}
    for key in ALL_PERMISSION_KEYS:
        if key in data:
            overrides[key] = data[key]
    user.permission_overrides = overrides
    db.session.commit()
    return jsonify({'ok': True, 'overrides': overrides})


# ── Helpers ───────────────────────────────────────────────────────────────────
def _group_dict(g):
    return {
        'id':           g.id,
        'name':         g.name,
        'description':  g.description or '',
        'permissions':  g.permissions or {},
        'member_count': User.query.filter_by(group_id=g.id).count(),
        'created_at':   g.created_at.isoformat() if g.created_at else None,
    }


def _user_dict(u):
    return {
        'id':        u.id,
        'username':  u.username,
        'full_name': u.full_name or '',
        'role':      u.role,
        'group_id':  u.group_id,
        'group_name': u.group.name if u.group else None,
        'overrides': u.permission_overrides or {},
    }


def _build_permissions(raw: dict) -> dict:
    perms = {}
    for key in ALL_PERMISSION_KEYS:
        if key == 'can_view_reports':
            val = raw.get(key, ['*'])
            perms[key] = val if isinstance(val, list) else (['*'] if val else [])
        else:
            perms[key] = bool(raw.get(key, False))
    return perms
