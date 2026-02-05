# utils/report_access.py
from flask import abort
from flask_login import current_user
from db import db, ReportAccessControl

def user_can_access_report(report_id: int) -> bool:
    """
    Evaluates if the current user has permission for a specific report ID.
    Always returns True for admins.
    """
    # 1. Admin Bypass
    if current_user.is_authenticated and current_user.role == 'admin':
        return True

    # 2. Check DB permissions
    # We use .filter() with == 1 to ensure compatibility with various SQL backends 
    # (SQLite uses 1/0, Postgres/Oracle might use True/False)
    access = ReportAccessControl.query.filter_by(
        user_id=current_user.id,
        report_template_id=report_id
    ).filter(ReportAccessControl.is_enabled == True).first()

    return access is not None


def enforce_report_access(report_id: int):
    """
    Hard gatekeeper. Call this at the start of every report route.
    Aborts with 403 if access is denied.
    """
    if not current_user.is_authenticated:
        abort(401)

    if not user_can_access_report(report_id):
        # This matches your "gentle 403" requirement
        abort(403, description=f"Access Denied: You do not have permission to view Report {report_id}.")