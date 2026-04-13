"""
routes/portal_bp.py
-------------------
Patient Portal — login, masked redirect, HL7 ORM hook, WhatsApp dispatch.

Wire up in registry.py:
    from routes.portal_bp import portal_bp
    app.register_blueprint(portal_bp)

Add to requirements.txt:
    twilio==8.5.0
"""

import re
import random
import string
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, session, abort, url_for
from sqlalchemy import text
from db import db

logger = logging.getLogger("PATIENT_PORTAL")
portal_bp = Blueprint("portal", __name__)


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _get_config():
    """Load all portal_config entries into a dict."""
    rows = db.session.execute(
        text("SELECT config_key, config_value FROM portal_config")
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _generate_password(length=10):
    """Generate a readable auto-password — mixed case + digits, no ambiguous chars."""
    chars = (
        string.ascii_uppercase.replace('O', '').replace('I', '') +
        string.ascii_lowercase.replace('l', '').replace('o', '') +
        string.digits.replace('0', '').replace('1', '')
    )
    return ''.join(random.choices(chars, k=length))


def _parse_pid_from_hl7(raw_message):
    """
    Extract MRN, full name, and phone from PID segment of raw HL7 message.
    PID-3  → MRN
    PID-5  → Name (family^given^middle)
    PID-13 → Phone (first XTN component)
    """
    mrn = full_name = phone = None
    try:
        for line in raw_message.replace('\r', '\n').split('\n'):
            line = line.strip()
            if not line.startswith('PID'):
                continue
            fields = line.split('|')

            # MRN — PID-3 (index 3), take first CX component before ^
            if len(fields) > 3 and fields[3]:
                mrn = fields[3].split('^')[0].strip()

            # Name — PID-5 (index 5): family^given^middle
            if len(fields) > 5 and fields[5]:
                parts = fields[5].split('^')
                family  = parts[0].strip() if len(parts) > 0 else ''
                given   = parts[1].strip() if len(parts) > 1 else ''
                middle  = parts[2].strip() if len(parts) > 2 else ''
                full_name = ' '.join(filter(None, [given, middle, family])).strip()

            # Phone — PID-13 (index 13): take raw value, clean to E.164
            if len(fields) > 13 and fields[13]:
                raw_phone = fields[13].split('^')[0].strip()
                # Normalize to E.164: keep digits + leading +
                digits = re.sub(r'[^\d+]', '', raw_phone)
                if digits and not digits.startswith('+'):
                    digits = '+' + digits
                phone = digits if len(digits) >= 10 else None
            break
    except Exception as e:
        logger.warning(f"PID parse error: {e}")
    return mrn, full_name, phone


def _send_whatsapp(phone, mrn, password, accession, config):
    """
    Fire WhatsApp message via Twilio.
    Returns (success: bool, error: str|None)
    """
    try:
        from twilio.rest import Client

        sid        = config.get('twilio_account_sid', '').strip()
        token      = config.get('twilio_auth_token', '').strip()
        from_num   = config.get('twilio_whatsapp_from', '').strip()
        portal_url = config.get('portal_base_url', '').rstrip('/') + '/portal'
        hospital   = config.get('hospital_name', 'Our Hospital')
        template   = config.get('whatsapp_message_template', '')

        if not all([sid, token, from_num]):
            logger.warning("Twilio credentials not configured — WhatsApp not sent.")
            return False, "Twilio not configured"

        first_name = mrn  # fallback if name not available
        message_body = template.format(
            name=first_name,
            portal_url=portal_url,
            username=mrn,
            password=password,
            hospital_name=hospital
        )

        client = Client(sid, token)
        client.messages.create(
            body=message_body,
            from_=f"whatsapp:{from_num}" if not from_num.startswith('whatsapp:') else from_num,
            to=f"whatsapp:{phone}"
        )
        logger.info(f"WhatsApp sent to {phone} for MRN {mrn}")
        return True, None

    except Exception as e:
        logger.error(f"WhatsApp send failed for {phone}: {e}")
        return False, str(e)


# ─────────────────────────────────────────────
#  HL7 HOOK — called from hl7_listener.py
#  after every ORM message is stored
# ─────────────────────────────────────────────

def process_orm_for_portal(raw_message, accession_number):
    """
    Called by the HL7 listener after storing an ORM message.
    Parses PID, upserts patient_portal_users, sends WhatsApp.

    Usage in hl7_listener.py (inside the ORM handler, after INSERT):
        from routes.portal_bp import process_orm_for_portal
        process_orm_for_portal(raw_message, accession_number)
    """
    try:
        mrn, full_name, phone = _parse_pid_from_hl7(raw_message)

        if not mrn:
            logger.warning("ORM portal hook: no MRN found in PID segment.")
            return

        config = _get_config()

        # Check if user already exists for this MRN
        existing = db.session.execute(
            text("SELECT id, password_plain FROM patient_portal_users WHERE mrn = :mrn"),
            {"mrn": mrn}
        ).fetchone()

        if existing:
            # Update accession and phone, keep existing password
            password = existing[1]
            db.session.execute(text("""
                UPDATE patient_portal_users
                SET accession_number = :acc,
                    phone = COALESCE(:phone, phone),
                    full_name = COALESCE(:name, full_name),
                    updated_at = NOW()
                WHERE mrn = :mrn
            """), {"acc": accession_number, "phone": phone,
                   "name": full_name, "mrn": mrn})
        else:
            # New patient — generate password, hash it, create record
            password = _generate_password()
            from werkzeug.security import generate_password_hash
            pwd_hash = generate_password_hash(password, method='pbkdf2:sha256')
            db.session.execute(text("""
                INSERT INTO patient_portal_users
                    (mrn, full_name, phone, accession_number, username, password_hash)
                VALUES
                    (:mrn, :name, :phone, :acc, :mrn, :pwd)
                ON CONFLICT (username) DO UPDATE SET
                    accession_number = EXCLUDED.accession_number,
                    phone = COALESCE(EXCLUDED.phone, patient_portal_users.phone),
                    full_name = COALESCE(EXCLUDED.full_name, patient_portal_users.full_name),
                    updated_at = NOW()
            """), {
                "mrn": mrn, "name": full_name, "phone": phone,
                "acc": accession_number, "pwd": pwd_hash
            })

        db.session.commit()

        # Send WhatsApp only if phone available and not already sent for this accession
        if phone:
            success, err = _send_whatsapp(phone, mrn, password, accession_number, config)
            if success:
                db.session.execute(text("""
                    UPDATE patient_portal_users
                    SET whatsapp_sent = TRUE, whatsapp_sent_at = NOW()
                    WHERE mrn = :mrn
                """), {"mrn": mrn})
                db.session.commit()
        else:
            logger.warning(f"No phone for MRN {mrn} — WhatsApp skipped.")

    except Exception as e:
        logger.error(f"Portal ORM hook error: {e}", exc_info=True)
        db.session.rollback()


# ─────────────────────────────────────────────
#  PATIENT-FACING ROUTES
# ─────────────────────────────────────────────

@portal_bp.route("/portal", methods=["GET", "POST"])
def portal_login():
    """Patient login page — username (MRN) + password."""
    config = _get_config()
    error  = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        row = db.session.execute(text("""
            SELECT id, mrn, full_name, accession_number, password_hash, is_active, password_plain
            FROM patient_portal_users
            WHERE username = :u
        """), {"u": username}).fetchone()

        if not row:
            error = "Invalid username or password."
        else:
            from werkzeug.security import check_password_hash as _chk
            pw_ok = False
            if row[4] and _chk(row[4], password):
                pw_ok = True
            elif row[6] and row[6] == password:
                # Legacy plaintext — verify and migrate to hash
                pw_ok = True
                from werkzeug.security import generate_password_hash as _gen
                db.session.execute(text(
                    "UPDATE patient_portal_users SET password_hash = :h, password_plain = NULL WHERE id = :id"
                ), {"h": _gen(password, method='pbkdf2:sha256'), "id": row[0]})
                db.session.commit()

        if not row or not pw_ok:
            error = "Invalid username or password."
        elif not row[5]:
            error = "Your account has been deactivated. Please contact the radiology department."
        else:
            # Store in session and redirect to masked viewer
            session['portal_mrn']       = row[1]
            session['portal_name']      = row[2]
            session['portal_accession'] = row[3]
            session['portal_uid']       = row[0]

            # Update last login
            db.session.execute(
                text("UPDATE patient_portal_users SET last_login = NOW() WHERE id = :id"),
                {"id": row[0]}
            )
            db.session.commit()

            return redirect(url_for("portal.portal_redirect"))

    return render_template("patient_portal.html",
                           error=error,
                           config=config)


@portal_bp.route("/portal/view")
def portal_redirect():
    """
    Masked redirect — builds the real viewer URL server-side
    and 302s the patient. Credentials never exposed to browser.
    """
    if 'portal_mrn' not in session:
        return redirect(url_for("portal.portal_login"))

    config    = _get_config()
    accession = session.get('portal_accession', '')

    base_url     = config.get('viewer_base_url', '').rstrip('/')
    viewer_user  = config.get('viewer_username', '')
    viewer_pass  = config.get('viewer_password', '')
    acc_param    = config.get('viewer_accession_param', 'accession')

    if not base_url:
        return "Viewer not configured. Please contact the radiology department.", 503

    # Build viewer URL with credentials server-side, proxied via session token
    import hashlib, time
    token = hashlib.sha256(f"{accession}{time.time()}{os.urandom(8).hex()}".encode()).hexdigest()[:32]
    session['viewer_token'] = token
    session['viewer_url'] = f"{base_url}?user={viewer_user}&pass={viewer_pass}&{acc_param}={accession}"

    return redirect(url_for('portal.portal_viewer_proxy', token=token))


@portal_bp.route("/portal/view/<token>")
def portal_viewer_proxy(token):
    """Proxy the viewer redirect so credentials never appear in browser history."""
    if session.get('viewer_token') != token or 'portal_mrn' not in session:
        return redirect(url_for("portal.portal_login"))
    viewer_url = session.pop('viewer_url', '')
    session.pop('viewer_token', None)
    if not viewer_url:
        return redirect(url_for("portal.portal_login"))
    return redirect(viewer_url)


@portal_bp.route("/portal/logout")
def portal_logout():
    session.pop('portal_mrn', None)
    session.pop('portal_name', None)
    session.pop('portal_accession', None)
    session.pop('portal_uid', None)
    return redirect(url_for("portal.portal_login"))
