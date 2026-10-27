"""
routes/portal_bp.py
-------------------
Patient Portal — login, signed viewer link, HL7 ORM hook, WhatsApp dispatch.

Wire up in registry.py:
    from routes.portal_bp import portal_bp
    app.register_blueprint(portal_bp)

Add to requirements.txt:
    twilio==8.5.0

Viewer link contract (what the PACS viewer must check):
    payload   = f"{accession}|{mrn}|{expires}|{nonce}"
    signature = hex(HMAC-SHA256(viewer_signing_secret, payload))
    The viewer opens that one study, without a login, only if the signature
    matches and the current Unix time is before `expires`. It should also
    refuse a nonce it has already seen. The URL is built from
    viewer_url_template, e.g.
      https://pacs.example/viewer?acc={accession}&mrn={mrn}&exp={expires}&n={nonce}&sig={signature}
    No shared viewer account ever reaches the patient's browser, and
    changing the accession breaks the signature.
"""

import re
import hmac
import time
import secrets
import string
import hashlib
import logging
from urllib.parse import quote
from flask import Blueprint, render_template, request, redirect, session, url_for
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
from db import db

logger = logging.getLogger("PATIENT_PORTAL")
portal_bp = Blueprint("portal", __name__)

_MAX_FAILED_LOGINS       = 10       # then the account locks ...
_LOCKOUT_MINUTES         = 15       # ... for this long
_PATIENT_SESSION_SECONDS = 15 * 60  # a patient session only exists to reach the viewer
_VIEWER_LINK_TTL_DEFAULT = 300

# Checked when the username is unknown, so a miss takes as long as a hit.
_DUMMY_HASH = generate_password_hash(secrets.token_hex(16), method='pbkdf2:sha256')


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

PORTAL_ENCRYPTED_KEYS = frozenset({'viewer_signing_secret', 'twilio_auth_token'})


def _get_config():
    """Load all portal_config entries into a dict, decrypting sensitive fields."""
    from utils.crypto import decrypt as _dec
    rows = db.session.execute(
        text("SELECT config_key, config_value FROM portal_config")
    ).fetchall()
    return {
        r[0]: (_dec(r[1]) if r[0] in PORTAL_ENCRYPTED_KEYS else (r[1] or ''))
        for r in rows
    }


def _generate_password(length=12):
    """Generate a cryptographically random password — mixed case + digits, no ambiguous chars."""
    chars = (
        string.ascii_uppercase.replace('O', '').replace('I', '') +
        string.ascii_lowercase.replace('l', '').replace('o', '') +
        string.digits.replace('0', '').replace('1', '')
    )
    return ''.join(secrets.choice(chars) for _ in range(length))


def _fill(template, values):
    """
    Replace {key} for the given keys only. Never str.format(): its {x.attr}
    and {x[i]} syntax lets whoever writes the template read Python objects.
    """
    return re.sub(r'\{(\w+)\}', lambda m: str(values.get(m.group(1), m.group(0))), template or '')


def _password_ok(stored_hash, password):
    try:
        return check_password_hash(stored_hash or _DUMMY_HASH, password)
    except ValueError:
        # Placeholder hash from migration 0023 (old plaintext row): needs a reset.
        return False


def _mask(value):
    """Last 3 characters only — logs carry no phone number or MRN in clear."""
    s = str(value or '')
    return '***' + s[-3:] if len(s) > 3 else '***'


def _mask_digits(message):
    """Twilio errors quote the phone number; hide any long digit run."""
    return re.sub(r'\+?\d[\d \-]{5,}\d', '***', str(message))


def viewer_link_ttl(config):
    try:
        ttl = int(config.get('viewer_link_ttl_seconds') or _VIEWER_LINK_TTL_DEFAULT)
    except ValueError:
        ttl = _VIEWER_LINK_TTL_DEFAULT
    return max(30, min(ttl, 3600))


def build_viewer_link(config, accession, mrn, now=None):
    """Signed, expiring link to one study (contract in the module docstring). None if not configured."""
    template = (config.get('viewer_url_template') or '').strip()
    secret   = config.get('viewer_signing_secret') or ''
    if not template or not secret or not accession:
        return None
    expires   = int((now if now is not None else time.time()) + viewer_link_ttl(config))
    nonce     = secrets.token_hex(16)
    payload   = f"{accession}|{mrn}|{expires}|{nonce}"
    signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    values = {
        'accession': accession, 'mrn': mrn or '', 'expires': expires,
        'nonce': nonce, 'signature': signature,
    }
    return _fill(template, {k: quote(str(v), safe='') for k, v in values.items()})


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
        hospital   = config.get('hospital_name', '') or 'Our Hospital'
        template   = config.get('whatsapp_message_template', '')

        if not all([sid, token, from_num]):
            logger.warning("Twilio credentials not configured — WhatsApp not sent.")
            return False, "Twilio not configured"

        message_body = _fill(template, {
            'name':          mrn,  # the ORM name is not passed here; the MRN stands in
            'portal_url':    portal_url,
            'username':      mrn,
            'password':      password,
            'hospital_name': hospital,
        })

        client = Client(sid, token)
        client.messages.create(
            body=message_body,
            from_=f"whatsapp:{from_num}" if not from_num.startswith('whatsapp:') else from_num,
            to=f"whatsapp:{phone}"
        )
        logger.info(f"WhatsApp sent to {_mask(phone)}")
        return True, None

    except Exception as e:
        err = _mask_digits(e)
        logger.error(f"WhatsApp send failed for {_mask(phone)}: {err}")
        return False, err


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
    mrn = None  # initialised before try so the except block can reference it safely
    try:
        mrn, full_name, phone = _parse_pid_from_hl7(raw_message)

        if not mrn:
            logger.warning("ORM portal hook: no MRN found in PID segment.")
            return

        config = _get_config()

        # Atomically upsert the portal user record.
        # Always generate a password upfront; for existing rows the ON CONFLICT SET
        # clause deliberately omits password_hash, so the generated value is never
        # applied and the existing patient's login is preserved.
        password = _generate_password()
        pwd_hash = generate_password_hash(password, method='pbkdf2:sha256')

        result = db.session.execute(text("""
            INSERT INTO patient_portal_users
                (mrn, full_name, phone, accession_number, username, password_hash)
            VALUES
                (:mrn, :name, :phone, :acc, :mrn, :pwd)
            ON CONFLICT (mrn) DO UPDATE SET
                accession_number = EXCLUDED.accession_number,
                phone            = COALESCE(EXCLUDED.phone, patient_portal_users.phone),
                full_name        = COALESCE(EXCLUDED.full_name, patient_portal_users.full_name),
                updated_at       = NOW()
            RETURNING (xmax = 0) AS is_new_row
        """), {
            "mrn": mrn, "name": full_name, "phone": phone,
            "acc": accession_number, "pwd": pwd_hash
        })
        row = result.fetchone()
        is_new = bool(row[0]) if row else True
        db.session.commit()

        if not is_new:
            # Existing patient — credentials unchanged, no WhatsApp needed.
            logger.info(f"Portal: new accession for existing patient {_mask(mrn)} (no password change)")
            return

        # Send WhatsApp only for newly created patients (and only if phone is available)
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
            logger.warning(f"No phone for patient {_mask(mrn)} — WhatsApp skipped.")

    except Exception as e:
        logger.error(f"Portal ORM hook error for patient {_mask(mrn)}: {_mask_digits(e)}")
        db.session.rollback()
        # Persist to dead-letter table so the hook can be retried or investigated.
        try:
            db.session.execute(text("""
                INSERT INTO portal_failed_hooks
                    (mrn, accession_number, raw_message, error_message)
                VALUES (:mrn, :acc, :msg, :err)
            """), {
                "mrn": mrn,
                "acc": accession_number,
                "msg": (raw_message or "")[:4000],
                "err": str(e)[:500],
            })
            db.session.commit()
        except Exception:
            db.session.rollback()


# ─────────────────────────────────────────────
#  PATIENT-FACING ROUTES
# ─────────────────────────────────────────────

def _end_patient_session():
    for key in ('portal_uid', 'portal_login_at'):
        session.pop(key, None)


@portal_bp.after_request
def _no_store(response):
    # Patient pages and the signed viewer redirect must never be cached.
    response.headers['Cache-Control'] = 'no-store'
    return response


@portal_bp.route("/portal", methods=["GET", "POST"])
def portal_login():
    """Patient login page — username (MRN) + password."""
    config = _get_config()
    error  = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        row = db.session.execute(text("""
            SELECT id, password_hash, is_active,
                   (locked_until IS NOT NULL AND locked_until > NOW()) AS locked
            FROM patient_portal_users
            WHERE username = :u
        """), {"u": username}).fetchone()

        if row and row.locked:
            error = f"Too many failed attempts. Please wait {_LOCKOUT_MINUTES} minutes and try again."
        else:
            pw_ok = _password_ok(row.password_hash if row else None, password)
            if not row or not pw_ok:
                error = "Invalid username or password."
                if row:
                    db.session.execute(text("""
                        UPDATE patient_portal_users
                        SET failed_login_count = CASE WHEN failed_login_count + 1 >= :max
                                                      THEN 0 ELSE failed_login_count + 1 END,
                            locked_until       = CASE WHEN failed_login_count + 1 >= :max
                                                      THEN NOW() + make_interval(mins => :mins)
                                                      ELSE locked_until END
                        WHERE id = :id
                    """), {"id": row.id, "max": _MAX_FAILED_LOGINS, "mins": _LOCKOUT_MINUTES})
                    db.session.commit()
            elif not row.is_active:
                error = "Your account has been deactivated. Please contact the radiology department."
            else:
                db.session.execute(text("""
                    UPDATE patient_portal_users
                    SET last_login = NOW(), failed_login_count = 0, locked_until = NULL
                    WHERE id = :id
                """), {"id": row.id})
                db.session.commit()

                # Fresh session: nothing carried over from before the login.
                session.clear()
                session['portal_uid']      = row.id
                session['portal_login_at'] = int(time.time())
                return redirect(url_for("portal.portal_redirect"))

    return render_template("patient_portal.html",
                           error=error,
                           config=config)


@portal_bp.route("/portal/view")
def portal_redirect():
    """Send the logged-in patient to their own study through a signed, expiring link."""
    uid      = session.get('portal_uid')
    login_at = session.get('portal_login_at', 0)
    if not uid or time.time() - login_at > _PATIENT_SESSION_SECONDS:
        _end_patient_session()
        return redirect(url_for("portal.portal_login"))

    row = db.session.execute(text("""
        SELECT mrn, accession_number, is_active
        FROM patient_portal_users WHERE id = :id
    """), {"id": uid}).fetchone()
    if not row or not row.is_active:
        _end_patient_session()
        return redirect(url_for("portal.portal_login"))

    link = build_viewer_link(_get_config(), row.accession_number, row.mrn)
    if not link:
        return "Viewer not configured. Please contact the radiology department.", 503
    return redirect(link)


@portal_bp.route("/portal/logout")
def portal_logout():
    _end_patient_session()
    return redirect(url_for("portal.portal_login"))
