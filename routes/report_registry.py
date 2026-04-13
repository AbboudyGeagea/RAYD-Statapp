# routes/report_registry.py
"""
Central registry for all report modules.

To add a new report (e.g. report 40):
  1. Create routes/report_40.py with blueprint, view function, and export function
  2. At the bottom of report_40.py, add:
       from routes.report_registry import register_report
       register_report(40, report_40_bp, report_40, export_report_40)
  3. That's it. No other files need to change.

The registry is consumed by:
  - routes/registry.py        → blueprint registration + license gating
  - routes/viewer_controller.py → report rendering + export dispatch
  - routes/auth_controller.py  → default report access for new users
"""

_REPORTS = {}  # {report_id: {"bp": blueprint, "view": fn, "export": fn}}


def register_report(report_id, blueprint, view_fn, export_fn=None):
    """Register a report module. Called at import time by each report file."""
    _REPORTS[report_id] = {
        "bp": blueprint,
        "view": view_fn,
        "export": export_fn,
    }


def get_all_reports():
    """Return dict of all registered reports: {id: {bp, view, export}}."""
    return _REPORTS


def get_report_ids():
    """Return sorted list of all registered report IDs."""
    return sorted(_REPORTS.keys())


def get_report(report_id):
    """Return a single report's registration, or None."""
    return _REPORTS.get(report_id)
