"""
routes/site_controller.py — the site switch in the header (meeting of 2026-11-13, item 13a:
"a dropdown on every report switches between the two databases, selected users only").

Shown to an account that holds two or more sites with a database (utils/site_db.py). The
pick is kept in the Flask session; every following request reads that site's database.
"""
from flask import Blueprint, abort, redirect, request, url_for
from flask_login import current_user, login_required

from utils import site_db

site_bp = Blueprint('site', __name__)


@site_bp.route('/site/switch', methods=['POST'])
@login_required
def switch():
    raw = request.form.get('site_id') or (request.get_json(silent=True) or {}).get('site_id')
    try:
        site_id = int(raw)
    except (TypeError, ValueError):
        abort(400)
    if not site_db.choose(site_id):
        abort(403)
    from utils.audit import log_event
    log_event('site_switched', category='auth', detail={'site_id': site_id,
                                                       'site': site_db.site_code(site_id)})
    return redirect(_safe_next(request.form.get('next')))


def _safe_next(target):
    """Back where the switch was made, never off this site."""
    target = (target or '').strip()
    if target.startswith('/') and not target.startswith('//') and '\\' not in target:
        return target.rstrip('?') or '/'
    return url_for('viewer.viewer_dashboard')
