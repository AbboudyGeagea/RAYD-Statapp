"""
routes/email_server.py — Administration > Email server, admin only.

The one mail server RAYD sends through: Report Composer's scheduled emails and the
Sticky Notes monthly report both use it. It used to sit inside Report Composer >
Scheduled emails; it moved here (operator, 2026-11-07) because it is not the composer's.
The settings stay where they were (settings smtp_*, migrations 0138 and 0140), read and
written by utils/worklist_email.py, so nothing has to be entered again.

    /admin/email-server              the page
    /admin/email-server/api          the settings, each secret only as "one is saved"
    /admin/email-server/api/save     POST, save them
    /admin/email-server/api/test     POST, a message with no data in it

Not tied to a licence: whichever feature emails, it needs the server.
"""
import logging

from flask import Blueprint, abort, jsonify, render_template, request
from flask_login import current_user, login_required

from utils import worklist_email as mailer
from utils.worklist_email import ScheduleError

logger = logging.getLogger("EMAIL_SERVER")

email_server_bp = Blueprint('email_server', __name__, url_prefix='/admin/email-server')


def _require_admin():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role != 'admin':
        abort(403)


def _error(msg, code=400):
    return jsonify({'error': str(msg)}), code


def _state():
    cfg = mailer.mail_config()
    return {'mail': mailer.public_config(cfg), 'ready': mailer.is_ready(cfg)}


@email_server_bp.route('')
@login_required
def page():
    _require_admin()
    return render_template('email_server.html')


@email_server_bp.route('/api')
@login_required
def api_settings():
    _require_admin()
    return jsonify(_state())


@email_server_bp.route('/api/save', methods=['POST'])
@login_required
def api_save():
    _require_admin()
    try:
        mailer.save_mail_config(request.get_json(silent=True) or {})
    except ScheduleError as e:
        return _error(e)
    return jsonify({'status': 'ok', **_state()})


@email_server_bp.route('/api/test', methods=['POST'])
@login_required
def api_test():
    """A message with no data in it, to prove the relay takes mail for this address."""
    _require_admin()
    cfg = mailer.mail_config()
    if not mailer.is_ready(cfg):
        return _error('save the mail server first')
    try:
        to = mailer.clean_recipients((request.get_json(silent=True) or {}).get('to'), cfg)
        refused = mailer.send_mail(cfg, to, 'RAYD test email',
                                   'The mail server is set up correctly.\n\nSent by RAYD.')
    except ScheduleError as e:
        return _error(e)
    except Exception as e:
        logger.warning("test email failed: %s", e)
        return _error(f'the mail server did not take the message: {e}', 502)
    if refused:
        return _error('the mail server refused: ' + ', '.join(sorted(refused)), 502)
    return jsonify({'status': 'ok'})
