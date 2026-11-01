"""
routes/department_board.py — the Live Department board on HL7 data.

    /viewer/department              the board (a wall display: no sidebar, refreshes itself)
    /viewer/department/api/board    the board as JSON (utils/department_board.build_board)

A passive view: it reads brayn_study_state, the ladder the HL7 projector keeps from
the RIS messages, and changes nothing. It replaces live_feed's page on this branch:
that one reads the RIS SPS table through Oracle, which the HL7 distribution does not
have, so /viewer/live now redirects here.

ACCESS: the live_feed licence key and page permission, as the page it replaces.
"""
import logging

from flask import Blueprint, abort, jsonify, render_template, request
from flask_login import current_user, login_required

from db import db, user_has_page
from utils.department_board import build_board

logger = logging.getLogger("DEPARTMENT_BOARD")

department_board_bp = Blueprint('department_board', __name__)

REFRESH_SECONDS = 15


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if not user_has_page(current_user, 'live_feed'):
        abort(403)


@department_board_bp.route('/viewer/department')
@login_required
def page():
    _require_access()
    return render_template('department_board.html', refresh_seconds=REFRESH_SECONDS)


@department_board_bp.route('/viewer/department/api/board')
@login_required
def api_board():
    _require_access()
    site = (request.args.get('site') or '').strip() or None
    try:
        return jsonify(build_board(site=site))
    except Exception:
        db.session.rollback()
        logger.exception("department board failed")
        return jsonify({'error': 'The board could not be read. See the app log.'}), 500
