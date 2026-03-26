# routes/registry.py
from .auth_controller    import auth_bp
from .admin_controller   import admin_bp
from .viewer_controller  import viewer_bp
from .mapping_controller import mapping_bp
from .saved_reports      import saved_reports_bp
from .report_controller  import report_bp
from .report_22          import report_22_bp
from .report_23          import report_23_bp
from .report_25          import report_25_bp
from .report_27          import report_27_bp
from .report_29          import report_29_bp
from routes.hl7_orders       import hl7_orders_bp
from routes.etl_gear_route   import etl_gear_bp
from routes.report_ai        import report_ai_bp
from routes.portal_bp        import portal_bp
from routes.portal_admin     import portal_admin_bp
from routes.super_report     import super_report_bp
from routes.capacity_ladder  import capacity_ladder_bp
from routes.live_feed        import live_feed_bp
from routes.preferences      import preferences_bp
from routes.docs             import docs_bp


def register_blueprints(app):
    # ── Core (always registered) ──────────────────────────────
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp,         url_prefix='/admin')
    app.register_blueprint(viewer_bp,        url_prefix='/viewer')
    app.register_blueprint(mapping_bp)
    app.register_blueprint(report_bp)
    app.register_blueprint(saved_reports_bp, url_prefix='/saved')
    app.register_blueprint(report_ai_bp)
    app.register_blueprint(super_report_bp)
    app.register_blueprint(capacity_ladder_bp)
    app.register_blueprint(preferences_bp)
    app.register_blueprint(docs_bp)

    # ── Reports ───────────────────────────────────────────────
    app.register_blueprint(report_22_bp)
    app.register_blueprint(report_23_bp)
    app.register_blueprint(report_25_bp)
    app.register_blueprint(report_27_bp)
    app.register_blueprint(report_29_bp)
    app.register_blueprint(hl7_orders_bp)
    app.register_blueprint(etl_gear_bp)

    # ── Patient Portal (optional) ─────────────────────────────
    if app.config.get("PATIENT_PORTAL_ENABLED", True):
        app.register_blueprint(portal_bp)
        app.register_blueprint(portal_admin_bp)

    # ── Live AE Feed (optional) ───────────────────────────────
    if app.config.get("LIVE_FEED_ENABLED", True):
        app.register_blueprint(live_feed_bp)

    # ── BitNet AI Assistant (optional) ────────────────────────
    if app.config.get("BITNET_ENABLED", True):
        try:
            from routes.bitnet_service import bitnet_bp
            app.register_blueprint(bitnet_bp)
            app.logger.info("✅ BitNet AI Assistant enabled")
        except ImportError as e:
            app.logger.warning(f"⚠ BitNet import failed: {e}")
