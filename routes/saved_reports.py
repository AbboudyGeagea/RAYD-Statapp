# routes/saved_reports.py
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from db import db, SavedReport, ReportTemplate, ReportAccessControl
from datetime import datetime
import json

saved_reports_bp = Blueprint('saved_reports', __name__, url_prefix='/saved')

# List saved reports available to current user (own + public + reports they have access to)
@saved_reports_bp.route('/list', methods=['GET'])
@login_required
def list_saved_reports():
    # Own saved reports
    own = SavedReport.query.filter_by(owner_user_id=current_user.id).all()
    # Public saved reports
    public = SavedReport.query.filter_by(is_public=True).all()
    # Optionally combine (dedupe by id)
    combined = {r.id: r for r in (own + public)}.values()
    return jsonify({"status":"success", "saved_reports":[r.to_dict() for r in combined]})

# Create / save a new variant
@saved_reports_bp.route('/create', methods=['POST'])
@login_required
def create_saved_report():
    payload = request.get_json() or {}
    name = payload.get('name')
    base_report_id = payload.get('base_report_id')
    filter_json = payload.get('filter_json', {})
    is_public = bool(payload.get('is_public', False))
    if not name or not base_report_id:
        return jsonify({"status":"error","message":"name and base_report_id required"}), 400

    # optional: verify user has access to base_report
    def user_has_base_access(uid, rid):
        # admin can access everything
        if current_user.role == 'admin':
            return True
        access = ReportAccessControl.query.filter_by(user_id=uid, report_template_id=rid, is_enabled=True).first()
        return access is not None

    if not user_has_base_access(current_user.id, base_report_id):
        return jsonify({"status":"error","message":"No access to base report"}), 403

    # Optionally generate SQL now (you can implement a builder). For now leave generated_sql NULL.
    sr = SavedReport(
        name=name,
        owner_user_id=current_user.id,
        base_report_id=base_report_id,
        filter_json=filter_json,
        is_public=is_public
    )
    try:
        db.session.add(sr)
        db.session.commit()
        return jsonify({"status":"success","saved_report":sr.to_dict()})
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Failed to create saved report")
        return jsonify({"status":"error","message":str(e)}), 500

# Run a saved report (execute generated_sql if exists else build SQL from base + filters)
@saved_reports_bp.route('/run/<int:saved_id>', methods=['POST'])
@login_required
def run_saved_report(saved_id):
    payload = request.get_json() or {}
    # allow override dates or filter merge if provided
    extra_filters = payload.get('override_filters', {})

    sr = SavedReport.query.get(saved_id)
    if not sr:
        return jsonify({"status":"error","message":"Saved report not found"}), 404

    # Access: owner, public, or admin or having base report access
    if not (sr.is_public or sr.owner_user_id == current_user.id or current_user.role == 'admin'):
        # check base_report access for viewers
        access = ReportAccessControl.query.filter_by(user_id=current_user.id, report_template_id=sr.base_report_id, is_enabled=True).first()
        if not access:
            return jsonify({"status":"error","message":"Access denied"}), 403

    # If generated_sql exists, run it directly (recommended)
    sql = sr.generated_sql
    # If not generated_sql, we expect the app to have a builder function
    if not sql:
        # naive builder example: append filters as WHERE clauses (YOU SHOULD REPLACE WITH YOUR SAFER BUILDER)
        base = ReportTemplate.query.get(sr.base_report_id)
        if not base or not base.report_sql_query:
            return jsonify({"status":"error","message":"Base report SQL missing"}), 500

        # Merge JSON filters with overrides (shallow)
        filters = sr.filter_json.copy() if sr.filter_json else {}
        filters.update(extra_filters or {})

        # Example: we expect the base SQL to contain a placeholder like /*WHERE*/ which we will replace
        final_where = "1=1"
        where_clauses = []
        # VERY IMPORTANT: use bound params; this naive builder only demonstrates idea
        params = {}
        idx = 0
        for k, v in filters.items():
            idx += 1
            param_name = f"p{idx}"
            # This is a heuristic mapping — adapt to your real columns
            where_clauses.append(f"{k} = :{param_name}")
            params[param_name] = v

        if where_clauses:
            final_where = " AND ".join(where_clauses)
        sql = base.report_sql_query.replace("/*WHERE*/", f"WHERE {final_where}") if "/*WHERE*/" in base.report_sql_query else base.report_sql_query + f" WHERE {final_where}"

    # Execute SQL (use db.session.execute with params if you created paramized SQL)
    try:
        result = db.session.execute(sql, params if 'params' in locals() else {})
        rows = [dict(r._mapping) for r in result]
        return jsonify({"status":"success","rows":rows})
    except Exception as e:
        current_app.logger.exception("Failed running saved report")
        return jsonify({"status":"error","message":"Failed executing report"}), 500

# Delete saved report
@saved_reports_bp.route('/delete/<int:saved_id>', methods=['DELETE'])
@login_required
def delete_saved_report(saved_id):
    sr = SavedReport.query.get(saved_id)
    if not sr:
        return jsonify({"status":"error","message":"Not found"}), 404
    if not (sr.owner_user_id == current_user.id or current_user.role == 'admin'):
        return jsonify({"status":"error","message":"Forbidden"}), 403
    try:
        db.session.delete(sr)
        db.session.commit()
        return jsonify({"status":"success"})
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Failed deleting saved report")
        return jsonify({"status":"error","message":str(e)}), 500
