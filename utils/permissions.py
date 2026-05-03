"""
utils/permissions.py — Group-based permission resolution.

Resolution order for any permission key:
  1. admin role                → always True
  2. user.permission_overrides → if key present, that value wins
  3. user.group.permissions    → group default
  4. no group assigned         → True  (backward-compat: ungrouped users keep full access)
"""

# Canonical permission catalogue — (key, human label)
ALL_PERMISSIONS = [
    ('can_export',        'Export reports to CSV / Excel'),
    ('can_configure',     'Edit AE mappings, device schedules, procedure mappings'),
    ('can_manage_users',  'Add, edit and deactivate users (without full admin)'),
    ('can_view_finance',  'Access financial dashboard and config'),
    ('can_use_ai',        'Use the Qwen AI assistant'),
    ('can_view_etl',      'View ETL logs and PACS connection status'),
    ('can_view_reports',  'Access analytics reports  ("*" = all, or list of IDs)'),
]

ALL_PERMISSION_KEYS = [k for k, _ in ALL_PERMISSIONS]


def resolve_permission(user, permission: str) -> bool:
    if not getattr(user, 'is_authenticated', False):
        return False
    if getattr(user, 'role', None) == 'admin':
        return True

    overrides = getattr(user, 'permission_overrides', None) or {}
    if permission in overrides:
        val = overrides[permission]
        return _truthy(val)

    group = getattr(user, 'group', None)
    if group:
        val = (group.permissions or {}).get(permission)
        if val is None:
            return False
        return _truthy(val)

    return True  # ungrouped users: full access (backward-compatible)


def can_view_report(user, report_id: int) -> bool:
    if not getattr(user, 'is_authenticated', False):
        return False
    if getattr(user, 'role', None) == 'admin':
        return True

    overrides = getattr(user, 'permission_overrides', None) or {}
    if 'can_view_reports' in overrides:
        return _report_in(overrides['can_view_reports'], report_id)

    group = getattr(user, 'group', None)
    if group:
        return _report_in((group.permissions or {}).get('can_view_reports', ['*']), report_id)

    return True  # ungrouped → full access


def _truthy(val) -> bool:
    if isinstance(val, list):
        return len(val) > 0
    return bool(val)


def _report_in(allowed, report_id: int) -> bool:
    if allowed in ('*', True):
        return True
    if isinstance(allowed, list):
        return '*' in allowed or report_id in allowed
    return False


def permission_required(permission: str):
    """Route decorator: deny with 403 / redirect if user lacks the permission."""
    from functools import wraps
    from flask import redirect, url_for, flash, request, jsonify
    from flask_login import current_user

    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))
            if not resolve_permission(current_user, permission):
                if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return jsonify({'error': 'Permission denied', 'required': permission}), 403
                flash('You do not have permission to access this feature.', 'danger')
                return redirect(url_for('viewer.viewer_dashboard'))
            return f(*args, **kwargs)
        return decorated
    return decorator
