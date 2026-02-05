from .auth_controller import auth_bp
from .admin_controller import admin_bp
from .viewer_controller import viewer_bp
from .mapping_controller import mapping_bp
from .saved_reports import saved_reports_bp
from .report_22 import report_22_bp
from .report_23 import report_23_bp
from .api_controller import api_bp
from .report_controller import report_bp
from .mapper_editor_controller import mapper_editor_bp


def register_blueprints(app):
    """Register all application blueprints. Blueprints define their own prefixes."""
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(viewer_bp)
    app.register_blueprint(mapping_bp)
    app.register_blueprint(saved_reports_bp)
    app.register_blueprint(report_22_bp)
    app.register_blueprint(report_23_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(report_bp)
    app.register_blueprint(mapper_editor_bp)
