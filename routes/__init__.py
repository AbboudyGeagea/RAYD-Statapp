from .auth_controller import auth_bp
from .admin_controller import admin_bp
from .viewer_controller import viewer_bp
from .mapper_editor_controller import mapper_editor_bp
from .saved_reports import saved_reports_bp
from .report_controller import report_bp
from .report_22 import report_22_bp
from .report_23 import report_23_bp
from .report_27 import report_27_bp
from .report_25 import report_25_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(viewer_bp, url_prefix='/viewer')
    app.register_blueprint(mapper_editor_bp, url_prefix='/mapper-editor')
    app.register_blueprint(report_bp, url_prefix='/report')
    app.register_blueprint(saved_reports_bp, url_prefix='/saved')

    # Individual reports for direct testing or legacy access
    app.register_blueprint(report_22_bp)
    app.register_blueprint(report_23_bp)
    app.register_blueprint(report_27_bp)
    from .report_27 import report_25_bp

