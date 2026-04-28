from flask import Blueprint, render_template, redirect, url_for, session, flash, request, current_app 
from datetime import date, datetime
from db import ReportTemplate, ReportAccessControl, get_enabled_reports_for_user, db 
from flask_login import login_required, current_user # Kept these imports
# from utils.decoratoratos import auth_required, viewer_required # REMOVED: Redundant
from functools import wraps # REMOVED: No longer needed for custom decorator

# RENAMED blueprint name to 'viewer' so endpoints like url_for('viewer.viewer_dashboard') work
viewer_bp = Blueprint('viewer', __name__, url_prefix='/viewer', template_folder='templates')


@viewer_bp.route('/mapping')
@login_required # Use the decorator imported from flask_login
def mapping_page():
    """
    Shared mapping page (accessible by both admin and viewer).
    """
    # Note: Explicit role check may be added here if you want to block other roles, 
    # but for viewer/admin, this is fine.
    return render_template('mapping.html', page_title="Mapping Configuration")


@viewer_bp.route('/')
@login_required # Use the decorator imported from flask_login
def index():
    """
    Redirect based on role.
    """
    # FIX: Use current_user.role instead of session.get('role')
    if current_user.role == 'admin':
        return redirect(url_for('admin.admin_dashboard'))
    # Corrected redirect to use the new blueprint name 'viewer'
    return redirect(url_for('viewer.viewer_dashboard'))


# NOTE: Since viewer_required was not provided, I assume it behaves like login_required 
# plus a role check. We replace it with login_required and a role check inside the function.
@viewer_bp.route('/dashboard', methods=['GET'])
@login_required # Use the decorator imported from flask_login
def viewer_dashboard():
    """
    Show enabled reports for current viewer user only.
    Provide default dates: GO_LIVE_DATE (if present in app config) otherwise Jan 1 2023, and today.
    """
    # FIX: Add an explicit role check now that we rely only on login_required
    if current_user.role not in ('viewer', 'admin'):
        flash('Unauthorized access.', 'danger')
        return redirect(url_for('viewer.index')) 
        
    # FIX: Use current_user.id instead of session.get('user_id')
    user_id = current_user.id
    
    # Fetch reports using the utility function from db.py
    enabled_reports = get_enabled_reports_for_user(user_id)
    
    # Get go-live date from current_app.config
    go_live_date = current_app.config.get('GO_LIVE_DATE', date(2023, 1, 1))
    
    default_start = go_live_date.strftime('%Y-%m-%d')
    default_end = date.today().strftime('%Y-%m-%d')
    
    return render_template(
        'viewer_dashboard.html',
        page_title='Dashboard',
        reports=enabled_reports,
        default_start=default_start,
        default_end=default_end
    )


@viewer_bp.route('/generate', methods=['POST'])
@login_required # Use the decorator imported from flask_login
def generate_report():
    """
    Handle viewer's report generation request (placeholder).
    """
    # FIX: Add an explicit role check
    if current_user.role not in ('viewer', 'admin'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 403
        
    # FIX: Use current_user.id instead of session.get('user_id')
    user_id = current_user.id 
    
    report_id = request.form.get('report_id')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    if not report_id:
        flash("Please select a report.", "warning")
        return redirect(url_for('viewer.viewer_dashboard'))

    # check access for this user + report
    access_row = ReportAccessControl.query.filter_by(user_id=user_id, report_template_id=report_id, is_enabled=True).first()
    if not access_row:
        flash("You do not have access to the selected report.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # validate dates (YYYY-MM-DD expected from browser date input)
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        flash("Invalid date format; use YYYY-MM-DD.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    # Placeholder: replace with actual report execution logic (SQL execution, safe paramization)
    tmpl = ReportTemplate.query.filter_by(id=report_id).first()
    if not tmpl:
        flash("Report template not found.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))
    
    flash(f"Report '{tmpl.report_name}' generated successfully from {s} to {e}. (Placeholder)", "info")
    return redirect(url_for('viewer.viewer_dashboard'))
