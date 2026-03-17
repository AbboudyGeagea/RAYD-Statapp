# ============================================================
#  RAYD Patient Portal — HL7 Listener Integration
#  Add these changes to your existing hl7_listener.py
# ============================================================

# ── STEP 1: Add import at top of hl7_listener.py ────────────

from routes.portal_bp import process_orm_for_portal


# ── STEP 2: Find where ORM messages are committed to DB ─────
#
# In your existing listener, after you do:
#   conn.execute(INSERT INTO hl7_orders ...)
#   conn.commit()  (or session.commit())
#
# Add this call immediately after the commit:

# ---- PASTE THIS BLOCK after your existing ORM INSERT -------

def _handle_orm_portal_hook(raw_message, accession_number):
    """
    Call this after every successful ORM INSERT into hl7_orders.
    Wraps in try/except so portal errors never break the HL7 listener.
    """
    try:
        process_orm_for_portal(raw_message, accession_number)
    except Exception as e:
        import logging
        logging.getLogger("HL7_LISTENER").error(
            f"Portal hook error (non-fatal): {e}", exc_info=True
        )

# ---- END PASTE BLOCK ----------------------------------------


# ── STEP 3: Example of where to call it ─────────────────────
#
# Your existing ORM handler probably looks something like:
#
#   if msg_type.startswith('ORM'):
#       accession = ...extract from ORC/OBR...
#       db.session.execute(text(INSERT INTO hl7_orders ...), {...})
#       db.session.commit()
#       # ← ADD THIS LINE:
#       _handle_orm_portal_hook(raw_message_string, accession)
#
# The raw_message_string is the full HL7 text before parsing.
# The accession is whatever you already extract for hl7_orders.accession_number


# ── STEP 4: Add twilio to requirements.txt ──────────────────
#
#   twilio==8.5.0


# ── STEP 5: Register blueprints in routes/registry.py ───────
#
#   from routes.portal_bp    import portal_bp
#   from routes.portal_admin import portal_admin_bp
#
#   app.register_blueprint(portal_bp)
#   app.register_blueprint(portal_admin_bp)


# ── STEP 6: Run migration ────────────────────────────────────
#
#   docker cp migration_portal.sql rayd_db:/migration_portal.sql
#   docker exec rayd_db psql -U etl_user -d etl_db -f /migration_portal.sql


# ── STEP 7: Configure via admin panel ───────────────────────
#
#   https://your-server/admin/portal/config
#
#   Fill in:
#     viewer_base_url       → https://viewer.hospital.com/login
#     viewer_username       → (hardcoded viewer login)
#     viewer_password       → (hardcoded viewer password)
#     viewer_accession_param → accession  (or whatever param name viewer expects)
#     hospital_name         → (shown on patient login page)
#     portal_base_url       → https://rayd.hospital.com
#     twilio_account_sid    → (from Twilio console)
#     twilio_auth_token     → (from Twilio console)
#     twilio_whatsapp_from  → whatsapp:+14155238886  (Twilio sandbox or approved number)
#     whatsapp_message_template → customize as needed


# ── FULL FLOW SUMMARY ────────────────────────────────────────
#
#  ORM arrives on port 6661
#      ↓
#  Existing listener stores to hl7_orders (unchanged)
#      ↓
#  _handle_orm_portal_hook() called
#      ↓
#  PID segment parsed → MRN, name, phone extracted
#      ↓
#  patient_portal_users upserted
#  (new patient → password generated)
#  (existing patient → accession updated, password kept)
#      ↓
#  Twilio WhatsApp fired:
#  "Hello [Name], your results are ready.
#   URL: https://rayd.hospital.com/portal
#   Username: [MRN]
#   Password: [auto-generated]"
#      ↓
#  Patient opens /portal → enters username + password
#      ↓
#  RAYD validates → builds:
#  https://viewer.hospital.com/login?user=ADMIN&pass=ADMIN&accession=ACC001
#      ↓
#  302 redirect → patient lands on viewer
#  Credentials never visible to patient browser
