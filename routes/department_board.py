"""
routes/department_board.py — the Live Department board on HL7 data.

    /viewer/department              the board (a wall display: no sidebar, refreshes itself)
    /viewer/department/api/board    one hospital's board as JSON (?site=RH|SJH, utils/department_board)
    /viewer/department/api/limits   POST: the wait limit per modality and the "done too early"
                                    share (settings capability: Super user, CHT, Administrator)
    /viewer/department/api/sites    the account's hospitals side by side, per modality (item 13b)

A passive view: it reads brayn_study_state, the ladder the BRAYN shadow keeps from the HL7
messages, and changes nothing but its own limits. BRAYN's tables live in the main database
until BRAYN routes by site (item 13d), so this blueprint always reads the main database
(utils.site_db.MAIN_BLUEPRINTS) and the hospital is chosen on the board itself, among the
account's sites (operator, 2026-11-19). /viewer/live redirects here. Empty while BRAYN is off.

ACCESS: the live_feed licence key and page permission, as the page it replaces.
"""
import logging

from flask import Blueprint, abort, jsonify, render_template, request
from flask_login import current_user, login_required

from db import db, user_has_page
from utils.department_board import build_board, load_limits, save_limits

logger = logging.getLogger("DEPARTMENT_BOARD")

department_board_bp = Blueprint('department_board', __name__)

REFRESH_SECONDS = 15


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if not user_has_page(current_user, 'live_feed'):
        abort(403)


def _account_sites():
    """[{'code', 'name'}] of the hospitals the account holds, the default one first."""
    from utils.roles import all_sites, granted_site_ids
    held = granted_site_ids(current_user)
    return [{'code': code, 'name': name} for sid, code, name in all_sites() if sid in held]


def _chosen_site(sites):
    """The ?site= asked for when the account holds it, else the header's site, else the first."""
    codes = [s['code'] for s in sites]
    asked = (request.args.get('site') or '').strip().upper()
    if asked in codes:
        return asked
    try:
        from flask import session
        from utils import site_db
        here = site_db.site_code(session.get('site_id'))    # the header's pick
    except Exception:
        here = None
    return here if here in codes else (codes[0] if codes else None)


def _can_edit_limits():
    from utils.roles import can
    return can(current_user, 'settings')


@department_board_bp.route('/viewer/department')
@login_required
def page():
    _require_access()
    return render_template('department_board.html', refresh_seconds=REFRESH_SECONDS,
                           can_edit_limits=_can_edit_limits())


@department_board_bp.route('/viewer/department/api/board')
@login_required
def api_board():
    _require_access()
    sites = _account_sites()
    if not sites:
        return jsonify({'error': 'Your account holds no hospital.'}), 403
    try:
        board = build_board(site=_chosen_site(sites), sites=sites)
    except Exception:
        db.session.rollback()
        logger.exception("department board failed")
        return jsonify({'error': 'The board could not be read. See the app log.'}), 500
    board['can_edit_limits'] = _can_edit_limits()
    return jsonify(board)


@department_board_bp.route('/viewer/department/api/limits', methods=['POST'])
@login_required
def api_limits():
    _require_access()
    if not _can_edit_limits():
        abort(403)
    data = request.get_json(silent=True) or {}
    current = load_limits()
    try:
        limits = save_limits(data.get('wait_min') or {},
                             data.get('default_wait_min', current['default_wait_min']),
                             data.get('early_pct', current['early_pct']))
    except ValueError as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 400
    from utils.audit import log_event
    log_event('settings_change', category='settings', resource_type='department_board_limits',
              detail=limits)
    return jsonify({'limits': limits})


@department_board_bp.route('/viewer/department/api/sites')
@login_required
def api_sites():
    """The account's hospitals side by side, per modality (meeting of 2026-11-13, item 13b:
    "direct comparison only in the Super Report and the Live Department board, per
    modality"). Each hospital's board from the same BRAYN tables, the same definitions.
    An account holding one site gets none."""
    _require_access()
    sites = _account_sites()
    if len(sites) < 2:
        return jsonify({'sites': [], 'modalities': [], 'totals': {}})
    boards = {}
    try:
        for s in sites:
            boards[s['code']] = build_board(site=s['code'], sites=sites)
    except Exception:
        db.session.rollback()
        logger.exception("department board sites failed")
        return jsonify({'error': 'The sites could not be read. See the app log.'}), 500
    by_mod, seen = {}, []
    for code, board in boards.items():
        for lane in board['lanes']:
            by_mod.setdefault(lane['modality'], {})[code] = lane['summary']
            if lane['modality'] not in seen:
                seen.append(lane['modality'])
    return jsonify({
        'sites': sites,
        'modalities': [{'modality': m, 'by_site': by_mod[m]} for m in seen],
        'totals': {code: board['totals'] for code, board in boards.items()},
    })
