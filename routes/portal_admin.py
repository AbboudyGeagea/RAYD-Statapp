"""
routes/portal_admin.py
----------------------
Admin management for the patient portal:
  - /admin/portal          → user list, search, status toggle
  - /admin/portal/reset/<id> → reset password + resend WhatsApp
  - /admin/portal/config   → edit portal_config settings

Wire up in registry.py:
    from routes.portal_admin import portal_admin_bp
    app.register_blueprint(portal_admin_bp)
"""

import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page
from routes.portal_bp import _generate_password, _send_whatsapp, _get_config

logger = logging.getLogger("PORTAL_ADMIN")
portal_admin_bp = Blueprint("portal_admin", __name__)


def _require_portal_access():
    """Abort 403 for demo users (always) and non-permitted non-admins."""
    from sqlalchemy import text as _t
    demo_row = db.session.execute(
        _t("SELECT value FROM settings WHERE key = 'demo_user'")
    ).fetchone()
    demo_username = (demo_row[0] or '').strip() if demo_row else ''
    if demo_username and current_user.username == demo_username:
        abort(403)
    if current_user.role != 'admin' and not user_has_page(current_user, 'patient_portal'):
        abort(403)


@portal_admin_bp.route("/admin/portal")
@login_required
def portal_users():
    _require_portal_access()
    """Patient portal user list with search and filters."""
    search  = request.args.get("q", "").strip()
    status  = request.args.get("status", "all")
    page    = int(request.args.get("page", 1))
    per_page = 50

    filters = ["1=1"]
    params  = {}

    if search:
        filters.append("(mrn ILIKE :q OR full_name ILIKE :q OR phone ILIKE :q OR accession_number ILIKE :q)")
        params["q"] = f"%{search}%"

    if status == "active":
        filters.append("is_active = TRUE")
    elif status == "inactive":
        filters.append("is_active = FALSE")
    elif status == "no_whatsapp":
        filters.append("(whatsapp_sent = FALSE OR whatsapp_sent IS NULL)")

    where = " AND ".join(filters)
    offset = (page - 1) * per_page

    users = db.session.execute(text(f"""
        SELECT id, mrn, full_name, phone, accession_number,
               username, password_plain, is_active,
               last_login, whatsapp_sent, whatsapp_sent_at, created_at
        FROM patient_portal_users
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT :lim OFFSET :off
    """), {**params, "lim": per_page, "off": offset}).mappings().fetchall()

    total = db.session.execute(
        text(f"SELECT COUNT(*) FROM patient_portal_users WHERE {where}"), params
    ).scalar()

    stats = db.session.execute(text("""
        SELECT
            COUNT(*)                                          AS total,
            COUNT(*) FILTER (WHERE is_active = TRUE)         AS active,
            COUNT(*) FILTER (WHERE whatsapp_sent = TRUE)     AS sent,
            COUNT(*) FILTER (WHERE last_login IS NOT NULL)   AS logged_in
        FROM patient_portal_users
    """)).mappings().fetchone()

    return render_template("portal_admin.html",
                           users=users,
                           stats=stats,
                           search=search,
                           status_filter=status,
                           page=page,
                           per_page=per_page,
                           total=total,
                           total_pages=(total + per_page - 1) // per_page)


@portal_admin_bp.route("/admin/portal/reset/<int:user_id>", methods=["POST"])
@login_required
def reset_password(user_id):
    _require_portal_access()
    """Generate new password, update DB, resend WhatsApp."""
    row = db.session.execute(
        text("SELECT * FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).mappings().fetchone()

    if not row:
        return jsonify({"success": False, "error": "User not found"}), 404

    new_password = _generate_password()
    config       = _get_config()

    db.session.execute(text("""
        UPDATE patient_portal_users
        SET password_plain   = :pwd,
            whatsapp_sent    = FALSE,
            updated_at       = NOW()
        WHERE id = :id
    """), {"pwd": new_password, "id": user_id})
    db.session.commit()

    # Resend WhatsApp
    wa_success = False
    wa_error   = None
    if row['phone']:
        wa_success, wa_error = _send_whatsapp(
            row['phone'], row['mrn'], new_password,
            row['accession_number'], config
        )
        if wa_success:
            db.session.execute(text("""
                UPDATE patient_portal_users
                SET whatsapp_sent = TRUE, whatsapp_sent_at = NOW()
                WHERE id = :id
            """), {"id": user_id})
            db.session.commit()

    return jsonify({
        "success":      True,
        "new_password": new_password,
        "whatsapp_sent": wa_success,
        "whatsapp_error": wa_error
    })


@portal_admin_bp.route("/admin/portal/toggle/<int:user_id>", methods=["POST"])
@login_required
def toggle_user(user_id):
    _require_portal_access()
    """Activate or deactivate a portal user."""
    row = db.session.execute(
        text("SELECT is_active FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).fetchone()

    if not row:
        return jsonify({"success": False}), 404

    new_status = not row[0]
    db.session.execute(
        text("UPDATE patient_portal_users SET is_active = :s, updated_at = NOW() WHERE id = :id"),
        {"s": new_status, "id": user_id}
    )
    db.session.commit()
    return jsonify({"success": True, "is_active": new_status})


@portal_admin_bp.route("/admin/portal/resend/<int:user_id>", methods=["POST"])
@login_required
def resend_whatsapp(user_id):
    _require_portal_access()
    """Resend WhatsApp with existing password."""
    row = db.session.execute(
        text("SELECT * FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).mappings().fetchone()

    if not row or not row['phone']:
        return jsonify({"success": False, "error": "No phone number on file"}), 400

    config = _get_config()
    success, error = _send_whatsapp(
        row['phone'], row['mrn'], row['password_plain'],
        row['accession_number'], config
    )

    if success:
        db.session.execute(text("""
            UPDATE patient_portal_users
            SET whatsapp_sent = TRUE, whatsapp_sent_at = NOW()
            WHERE id = :id
        """), {"id": user_id})
        db.session.commit()

    return jsonify({"success": success, "error": error})

# ── ADD THIS ROUTE to routes/portal_admin.py ──────────────────────────────
# Place it anywhere after the existing imports, before or after portal_config()

@portal_admin_bp.route("/admin/portal/test-whatsapp", methods=["POST"])
@login_required
def test_whatsapp():
    _require_portal_access()
    """Send a test WhatsApp message to verify Twilio credentials."""
    data   = request.get_json()
    phone  = (data or {}).get("phone", "").strip()
    if not phone:
        return jsonify({"success": False, "error": "No phone number provided"})

    config = _get_config()
    success, error = _send_whatsapp(
        phone=phone,
        mrn="TEST",
        password="TestPass123",
        accession="TEST-001",
        config=config
    )
    return jsonify({"success": success, "error": error})

@portal_admin_bp.route("/admin/portal/config", methods=["GET", "POST"])
@login_required
def portal_config():
    _require_portal_access()
    """View and edit portal configuration."""
    if request.method == "POST":
        for key, value in request.form.items():
            if key.startswith("cfg_"):
                config_key = key[4:]  # strip cfg_ prefix
                db.session.execute(text("""
                    UPDATE portal_config
                    SET config_value = :v, updated_at = NOW()
                    WHERE config_key = :k
                """), {"v": value.strip(), "k": config_key})
        db.session.commit()
        flash("Configuration saved.", "success")
        return redirect(url_for("portal_admin.portal_config"))

    configs = db.session.execute(
        text("SELECT config_key, config_value, description FROM portal_config ORDER BY id")
    ).mappings().fetchall()

    # Group configs for display
    sensitive = ['viewer_username', 'viewer_password', 'twilio_auth_token', 'twilio_account_sid']
    return render_template("portal_config.html", configs=configs, sensitive=sensitive)
