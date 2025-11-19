# routes/report_23.py

from flask import Blueprint, render_template, request
from flask_login import login_required
from utils.report_access import enforce_report_access

report_23_bp = Blueprint("report_23", __name__)


@report_23_bp.route("/report/23",methods=["GET", "POST"])
@login_required
def report_23():

    REPORT_ID = 23

    # 🔒 HARD BLOCK — URL tricks die here
    enforce_report_access(REPORT_ID)

    # If we reached here → user is allowed
    # Load data safely
    data = {
        "message": "Report 23 data loaded successfully"
    }

    return render_template("report_23.html", data=data)

