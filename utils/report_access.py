# utils/report_access.py

from flask_login import current_user
from flask import abort


def enforce_report_access(allowed: bool):
    """
    Simple hard gate.
    """
    if current_user.role == "admin":
        return

    if not allowed:
        abort(403, description="This report is not enabled for your account.")

