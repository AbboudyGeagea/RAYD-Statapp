"""
routes/department_board.py — the Live Department board on HL7 data.

    /viewer/department              the board (a wall display: no sidebar, refreshes itself)
    /viewer/department/api/board    the board as JSON (utils/department_board.build_board)
    /viewer/department/api/sites    RH and SJH side by side, per modality (item 13b)

A passive view: it reads brayn_study_state, the ladder the BRAYN shadow keeps from
the HL7 messages, and changes nothing. Ported from the HL7 branch unchanged; it
replaces live_feed's page (RIS SPS through Oracle) on LAUMC too, by the operator's
choice (2026-11-02), so /viewer/live redirects here. Empty while BRAYN is off.

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


@department_board_bp.route('/viewer/department/api/sites')
@login_required
def api_sites():
    """RH and SJH side by side, per modality (meeting of 2026-11-13, item 13b: "direct
    comparison only in the Super Report and the Live Department board, per modality"). For
    an account holding two or more sites that have a database (utils/site_db.py): the board
    is built on each site's own database and its per-modality summary returned side by
    side. Anyone else gets no sites."""
    from utils import site_db
    from utils.roles import all_sites
    _require_access()
    site_ids = sorted(site_db.selectable_site_ids(current_user))
    if len(site_ids) < 2:
        return jsonify({'sites': [], 'modalities': [], 'totals': {}})
    names = {sid: (code, name) for sid, code, name in all_sites()}
    codes = {sid: names[sid][0] if sid in names else str(sid) for sid in site_ids}
    boards = {}
    try:
        for sid in site_ids:
            with site_db.use_site(sid):
                boards[codes[sid]] = build_board()
    except Exception:
        db.session.rollback()
        logger.exception("department board sites failed")
        return jsonify({'error': 'The sites could not be read. See the app log.'}), 500
    by_mod = {}
    for code, board in boards.items():
        for lane in board['lanes']:
            by_mod.setdefault(lane['modality'], {})[code] = lane['summary']
    order = [lane['modality'] for b in boards.values() for lane in b['lanes']]
    seen = []
    for m in order:
        if m not in seen:
            seen.append(m)
    return jsonify({
        'sites': [{'code': codes[sid], 'name': names[sid][1] if sid in names else ''} for sid in site_ids],
        'modalities': [{'modality': m, 'by_site': by_mod[m]} for m in seen],
        'totals': {code: board['totals'] for code, board in boards.items()},
    })
