# report_controller.py
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from datetime import date
import pandas as pd
from sqlalchemy import text

from db import (
    db,
    ReportTemplate,
    ReportDimension,
    ReportAccessControl,
    GoLiveDate
)

report_bp = Blueprint('report', __name__, url_prefix='/report')


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def parse_iso_date(value):
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def get_go_live_date():
    rec = GoLiveDate.query.first()
    return rec.go_live_date if rec else None


# --------------------------------------------------
# SQL Builder
# --------------------------------------------------
def build_final_sql(base_sql, dimensions, filters, start_date, end_date):
    where = []
    joins = []
    params = {}

    added_joins = set()

    # Mandatory date filter (contract)
    where.append("study_date BETWEEN :start_date AND :end_date")
    params["start_date"] = start_date
    params["end_date"] = end_date

    for dim in dimensions:
        if not dim.allows_filtering:
            continue

        value = filters.get(dim.dimension_key)
        if value in (None, "", [], {}):
            continue

        # Column resolution
        if dim.source_type == "fact":
            col = dim.fact_column

        elif dim.source_type == "mapping":
            if dim.mapping_table not in added_joins:
                joins.append(
                    f"""
                    LEFT JOIN {dim.mapping_table}
                    ON {dim.mapping_table}.{dim.mapping_key_column}
                    = {dim.fact_column}
                    """
                )
                added_joins.add(dim.mapping_table)

            col = f"{dim.mapping_table}.{dim.mapping_value_column}"

        else:
            continue

        # Multi-select
        if dim.is_multi_select and isinstance(value, list):
            where.append(f"{col} = ANY(:{dim.dimension_key})")
            params[dim.dimension_key] = value
        else:
            where.append(f"{col} = :{dim.dimension_key}")
            params[dim.dimension_key] = value

    sql = base_sql.strip()

    if joins:
        sql += "\n" + "\n".join(joins)

    if where:
        sql += "\nWHERE " + " AND ".join(where)

    return sql, params


# --------------------------------------------------
# Generate Report
# --------------------------------------------------
@report_bp.route('/generate', methods=['POST'])
@login_required
def generate_report():
    try:
        payload = request.get_json(force=True)
        report_id = payload.get("report_id")

        if not report_id:
            return jsonify(error="report_id is required"), 400

        report = ReportTemplate.query.get(report_id)
        if not report:
            return jsonify(error="Report not found"), 404

        # Access control
        if current_user.role != "admin":
            allowed = ReportAccessControl.query.filter_by(
                user_id=current_user.id,
                report_template_id=report_id,
                is_enabled=True
            ).first()

            if not allowed:
                return jsonify(error="Access denied"), 403

        # Dates
        start_date = parse_iso_date(payload.get("start_date")) or get_go_live_date()
        end_date = parse_iso_date(payload.get("end_date")) or date.today()

        if not start_date or start_date > end_date:
            return jsonify(error="Invalid date range"), 400

        # Dimensions
        dimensions = (
            ReportDimension.query
            .filter_by(report_template_id=report_id, is_active=True)
            .order_by(ReportDimension.sort_order)
            .all()
        )

        filters = payload.get("filters", {})

        sql, params = build_final_sql(
            report.report_sql_query,
            dimensions,
            filters,
            start_date,
            end_date
        )

        result = db.session.execute(text(sql), params)
        df = pd.DataFrame(result.mappings())

        return jsonify(
            status="success",
            rows=df.to_dict(orient="records"),
            count=len(df)
        )

    except Exception:
        current_app.logger.exception("Report generation failed")
        return jsonify(error="Could not fetch data"), 500

