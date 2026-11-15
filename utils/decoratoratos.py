from functools import wraps
from flask import redirect, url_for, flash, abort
from flask_login import current_user

from utils.roles import can, role_of


def auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    # Checks the DB role via flask-login (utils/roles.py) — immune to session cookie forgery.
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not can(current_user, 'settings'):
            flash('Access denied. Administrator privileges required.', 'danger')
            return redirect(url_for('viewer.viewer_dashboard'))
        return f(*args, **kwargs)
    return decorated_function


def viewer_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or role_of(current_user) is None:
            flash('Access denied.', 'danger')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function
