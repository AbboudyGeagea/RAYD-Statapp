"""
routes/crn_ack.py
────────────────────────────────────────────────────────────────
Public, token-based acknowledgment landing page for CRN (Critical Result
Notification). No login — the physician clicking the link IS the auth
(unguessable, one-time, 48h-expiring token). See utils/crn_dispatcher.py.

Registered as a core (always-on) blueprint in registry.py, not license-gated
— a referring physician outside RAYD must never see a "not licensed" page.
"""
from flask import Blueprint, render_template, request

from utils.crn_dispatcher import acknowledge

crn_ack_bp = Blueprint('crn_ack', __name__, url_prefix='/crn')


@crn_ack_bp.route('/ack/<token>')
def ack(token):
    result = acknowledge(token, ip=request.remote_addr)
    return render_template('crn_ack.html', result=result)
