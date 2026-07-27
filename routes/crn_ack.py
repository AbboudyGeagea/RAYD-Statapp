"""
routes/crn_ack.py
────────────────────────────────────────────────────────────────
Public, token-based acknowledgment landing page for CRN (Critical Result
Notification). No login — the physician clicking the link IS the auth
(unguessable, one-time, 48h-expiring token). See utils/crn_dispatcher.py.

Patient name / exam / critical-finding detail is shown HERE, behind the
token, not in the raw email/SMS/WhatsApp text — matches LAUMC_SCOPE.md's
CRN spec ("keep PHI out of message bodies (minimal + secure link)") and
keeps the notification itself safe to send over channels (SMS, personal
email) that aren't a secure clinical system. Fetched live at render time,
not duplicated into crn_notifications storage (accession_number + the
already-detected critical_keywords are the only pieces stored there).

Registered as a core (always-on) blueprint in registry.py, not license-gated
— a referring physician outside RAYD must never see a "not licensed" page.
"""
from flask import Blueprint, render_template, request
from sqlalchemy import text

from db import db
from utils.crn_dispatcher import acknowledge

crn_ack_bp = Blueprint('crn_ack', __name__, url_prefix='/crn')


@crn_ack_bp.route('/ack/<token>')
def ack(token):
    result = acknowledge(token, ip=request.remote_addr)

    detail = None
    if result in ('ok', 'already_acknowledged'):
        detail = db.session.execute(text("""
            SELECT n.accession_number, n.critical_keywords,
                   o.patient_name, COALESCE(o.procedure_text, r.procedure_name) AS exam_name
            FROM crn_notifications n
            LEFT JOIN hl7_orders o ON o.accession_number = n.accession_number
            LEFT JOIN hl7_oru_reports r ON r.id = n.report_id
            WHERE n.ack_token = :token
            ORDER BY o.received_at DESC
            LIMIT 1
        """), {"token": token}).mappings().fetchone()

    return render_template('crn_ack.html', result=result, detail=detail)
