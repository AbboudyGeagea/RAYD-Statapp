# routes/__init__.py
from .auth_controller import auth_bp
from .admin_controller import admin_bp
from .viewer_controller import viewer_bp
from .mapping_controller import mapping_bp
from .saved_reports import saved_reports_bp
from .report_22 import report_22_bp
from .report_23 import report_23_bp



def register_blueprints(app):
    app.register_blueprint(auth_bp)          
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(viewer_bp, url_prefix='/viewer')
    app.register_blueprint(mapping_bp, url_prefix='/mapping')
    app.register_blueprint(saved_reports_bp, url_prefix='/saved')
    app.register_blueprint(report_22_bp)
    app.register_blueprint(report_23_bp)
