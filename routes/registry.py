# routes/registry.py
import json
import logging
from datetime import date
from .auth_controller    import auth_bp
from .admin_controller   import admin_bp
from .viewer_controller  import viewer_bp
from .mapping_controller import mapping_bp
from .saved_reports      import saved_reports_bp
from .report_controller  import report_bp
# Report modules self-register on import — just import them to trigger registration
import routes.report_22   # noqa: F401
import routes.report_23   # noqa: F401
import routes.report_25   # noqa: F401
import routes.report_27   # noqa: F401
import routes.report_29   # noqa: F401
from routes.report_registry import get_all_reports, get_report_ids
from routes.api_controller   import api_bp
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
from routes.er_dashboard     import er_bp
from routes.oru_analytics    import oru_bp
from routes.db_manager       import db_manager_bp
from routes.referring_intel  import referring_intel_bp
from routes.financial_config    import financial_config_bp
from routes.financial_dashboard import financial_dashboard_bp

logger = logging.getLogger("REGISTRY")

# ── Default license (everything ON, no limits) ───────────────
# Reports list is populated dynamically from registered reports
DEFAULT_LICENSE = {
    "tier": "enterprise",
    "reports": get_report_ids(),
    "ai_report": True,
    "capacity_ladder": True,
    "er_dashboard": True,
    "patient_portal": True,
    "live_feed": True,
    "hl7_orders": True,
    "oru_analytics": True,
    "saved_reports": True,
    "bitnet_ai": True,
    "export": True,
    "adapter_mapper": True,
    "super_report": True,
    "referring_intel": True,
    "max_users": 0,          # 0 = unlimited
    "max_sessions": 0,       # 0 = unlimited concurrent sessions
    "expires": "",            # "" = never, else "YYYY-MM-DD"
    "max_studies_per_report": 0,  # 0 = unlimited, else cap rows
}

# ── Tier presets (used by install.sh) ────────────────────────
TIER_PRESETS = {
    "basic": {
        "tier": "basic",
        "reports": [22],
        "ai_report": False,
        "capacity_ladder": False,
        "er_dashboard": False,
        "patient_portal": False,
        "live_feed": True,
        "hl7_orders": True,
        "oru_analytics": False,
        "saved_reports": False,
        "bitnet_ai": False,
        "export": False,
        "adapter_mapper": False,
        "max_users": 5,
        "max_sessions": 2,
        "expires": "",
        "max_studies_per_report": 5000,
    },
    "professional": {
        "tier": "professional",
        "reports": get_report_ids(),
        "ai_report": False,
        "capacity_ladder": True,
        "er_dashboard": True,
        "patient_portal": False,
        "live_feed": True,
        "hl7_orders": True,
        "oru_analytics": True,
        "saved_reports": True,
        "bitnet_ai": False,
        "export": True,
        "adapter_mapper": False,
        "max_users": 0,
        "max_sessions": 0,
        "expires": "",
        "max_studies_per_report": 0,
    },
    "enterprise": DEFAULT_LICENSE.copy(),
}


def _load_license(app):
    """Read license JSON from settings table. Falls back to full access."""
    try:
        from sqlalchemy import text
        from db import db
        with app.app_context():
            row = db.session.execute(
                text("SELECT value FROM settings WHERE key = 'license'")
            ).fetchone()
            if row:
                lic = json.loads(row[0])
                merged = DEFAULT_LICENSE.copy()
                merged.update(lic)
                return merged
    except Exception as e:
        logger.warning(f"Could not load license: {e}")
    return DEFAULT_LICENSE.copy()


def check_license_limit(app, check):
    """
    Runtime license checks called from routes.
    Returns (ok: bool, message: str).

    Usage:
        ok, msg = check_license_limit(current_app, 'export')
        if not ok: return jsonify(error=msg), 403
    """
    lic = app.config.get('LICENSE', DEFAULT_LICENSE)

    # ── Expiry ────────────────────────────────────────────
    if check == 'expired' or check != 'expired':
        exp = lic.get('expires', '')
        if exp:
            try:
                if date.today() > date.fromisoformat(exp):
                    return False, f"License expired on {exp}. Contact your vendor."
            except ValueError:
                pass

    if check == 'export':
        if not lic.get('export', False):
            return False, "Export is not included in your license tier."

    elif check == 'max_users':
        limit = lic.get('max_users', 0)
        if limit > 0:
            from db import User, db
            count = db.session.execute(
                __import__('sqlalchemy').text("SELECT COUNT(*) FROM users")
            ).scalar()
            if count >= limit:
                return False, f"User limit reached ({limit}). Upgrade your license to add more users."

    elif check == 'max_sessions':
        limit = lic.get('max_sessions', 0)
        if limit > 0:
            from db import db
            count = db.session.execute(
                __import__('sqlalchemy').text("SELECT COUNT(*) FROM active_sessions")
            ).scalar()
            if count >= limit:
                return False, f"All {limit} concurrent seats are in use. Try again later or upgrade your license."

    elif check == 'report':
        # Pass report_id as second arg via check_license_limit(app, 'report:25')
        pass

    return True, ""


def check_report_licensed(app, report_id):
    """Check if a specific report ID is licensed."""
    lic = app.config.get('LICENSE', DEFAULT_LICENSE)
    exp = lic.get('expires', '')
    if exp:
        try:
            if date.today() > date.fromisoformat(exp):
                return False, f"License expired on {exp}."
        except ValueError:
            pass
    if report_id not in lic.get('reports', []):
        return False, f"Report {report_id} is not included in your license."
    return True, ""


def get_study_limit(app):
    """Return max studies per report, or 0 for unlimited."""
    lic = app.config.get('LICENSE', DEFAULT_LICENSE)
    return lic.get('max_studies_per_report', 0)


def _register_not_licensed_route(app, url, feature_name, tier):
    """Register a stub route that renders the 'not licensed' page instead of a 404."""
    from flask import render_template
    from flask_login import login_required
    # Use url as part of the endpoint name to avoid conflicts
    endpoint = f"not_licensed_{url.replace('/', '_').strip('_')}"
    def _view():
        return render_template('not_licensed.html', feature_name=feature_name, tier=tier), 403
    _view.__name__ = endpoint
    app.add_url_rule(url, endpoint=endpoint, view_func=login_required(_view))


def register_blueprints(app):
    lic = _load_license(app)
    app.config['LICENSE'] = lic
    logger.info(f"License tier: {lic.get('tier', 'unknown')} — reports: {lic.get('reports', [])}")

    # ── Check expiry at startup ───────────────────────────────
    exp = lic.get('expires', '')
    if exp:
        try:
            if date.today() > date.fromisoformat(exp):
                logger.warning(f"LICENSE EXPIRED on {exp} — running in restricted mode")
        except ValueError:
            pass

    # ── Core (always registered) ──────────────────────────────
    app.register_blueprint(api_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp,         url_prefix='/admin')
    app.register_blueprint(viewer_bp,        url_prefix='/viewer')
    app.register_blueprint(mapping_bp)
    app.register_blueprint(report_bp)
    app.register_blueprint(preferences_bp)
    app.register_blueprint(docs_bp)
    app.register_blueprint(etl_gear_bp)
    app.register_blueprint(financial_config_bp)
    app.register_blueprint(financial_dashboard_bp)

    # ── Licensed reports (auto-discovered from report_registry) ─
    licensed_reports = lic.get('reports', [])
    for rid, reg in get_all_reports().items():
        if rid in licensed_reports:
            app.register_blueprint(reg['bp'])
            logger.info(f"  Report {rid}: enabled")
        else:
            logger.info(f"  Report {rid}: not licensed — skipped")

    # ── Licensed features ─────────────────────────────────────
    # Each entry: feature_key → (blueprint, kwargs, [(fallback_url, display_name), ...])
    # Fallback routes show a "not licensed" page instead of a 404.
    feature_map = {
        'ai_report':       (report_ai_bp,       {}, [('/report/ai',                   'AI Report')]),
        'capacity_ladder': (capacity_ladder_bp,  {}, [('/viewer/capacity-ladder',      'Capacity Ladder')]),
        'er_dashboard':    (er_bp,               {}, [('/er',                          'ER Dashboard')]),
        'oru_analytics':   (oru_bp,              {}, [('/oru',                         'ORU Analytics')]),
        'saved_reports':   (saved_reports_bp,    {'url_prefix': '/saved'}, []),
        'hl7_orders':      (hl7_orders_bp,       {}, [('/hl7/orders',                  'HL7 Orders')]),
        'adapter_mapper':  (db_manager_bp,        {}, [('/admin/db-manager',            'DB Manager')]),
        'super_report':    (super_report_bp,     {}, [('/viewer/super-report-page',    'Super Report'),
                                                      ('/viewer/super-report',         'Super Report')]),
        'referring_intel': (referring_intel_bp,  {}, [('/viewer/referring-intel',      'Referring Intel')]),
    }
    for feature, (bp, kwargs, fallbacks) in feature_map.items():
        if lic.get(feature, False):
            app.register_blueprint(bp, **kwargs)
            logger.info(f"  {feature}: enabled")
        else:
            logger.info(f"  {feature}: not licensed — skipped")
            # Register stub routes so users see a clear message instead of 404
            tier = lic.get('tier', 'current')
            for url, display_name in fallbacks:
                _register_not_licensed_route(app, url, display_name, tier)

    # ── Patient Portal (license + config flag) ────────────────
    if lic.get('patient_portal', False) and app.config.get("PATIENT_PORTAL_ENABLED", True):
        app.register_blueprint(portal_bp)
        app.register_blueprint(portal_admin_bp)
        logger.info("  patient_portal: enabled")

    # ── Live AE Feed (license + config flag) ──────────────────
    if lic.get('live_feed', False) and app.config.get("LIVE_FEED_ENABLED", True):
        app.register_blueprint(live_feed_bp)
        logger.info("  live_feed: enabled")

    # ── BitNet AI Assistant (license + config flag) ───────────
    if lic.get('bitnet_ai', False) and app.config.get("BITNET_ENABLED", True):
        try:
            from routes.bitnet_service import bitnet_bp
            app.register_blueprint(bitnet_bp)
            logger.info("  bitnet_ai: enabled")
        except ImportError as e:
            logger.warning(f"  bitnet_ai: import failed: {e}")

    # ── Inject license into templates ─────────────────────────
    @app.context_processor
    def inject_license():
        return {
            "license":           lic,
            "portal_registered": "portal_admin" in app.blueprints,
        }
