#routes/registry.py
from .auth_controller import auth_bp
from .admin_controller import admin_bp
from .viewer_controller import viewer_bp
from .mapping_controller import mapping_bp
from .saved_reports import saved_reports_bp
from .report_controller import report_bp
from .report_22 import report_22_bp
from .report_23 import report_23_bp
from .report_25 import report_25_bp
from .report_27 import report_27_bp
from .report_29 import report_29_bp
from routes.hl7_orders import hl7_orders_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(viewer_bp, url_prefix='/viewer')
    app.register_blueprint(mapping_bp)   # prefix already in blueprint
    app.register_blueprint(report_bp)
    app.register_blueprint(saved_reports_bp, url_prefix='/saved')

    # legacy / direct testing
    app.register_blueprint(report_22_bp)
    app.register_blueprint(report_23_bp)
    app.register_blueprint(report_25_bp)
    app.register_blueprint(report_27_bp)
    app.register_blueprint(report_29_bp)
    app.register_blueprint(hl7_orders_bp)
