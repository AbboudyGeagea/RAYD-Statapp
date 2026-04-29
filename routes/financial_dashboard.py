from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from datetime import date, timedelta
from db import db
from utils.financial import _get_config, effective_rate

financial_dashboard_bp = Blueprint('financial_dashboard', __name__)

# ── Shared SQL fragments ───────────────────────────────────────────────────────
_STUDY_BASE = """
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
    LEFT JOIN etl_orders o           ON o.study_db_uid = s.study_db_uid
    LEFT JOIN procedure_duration_map pdm
           ON UPPER(TRIM(o.proc_id)) = UPPER(TRIM(pdm.procedure_code))
    WHERE s.study_date BETWEEN :start AND :end
      AND COALESCE(UPPER(TRIM(COALESCE(m.modality, s.study_modality, ''))), '') != 'SR'
      AND s.study_has_report = true
"""

_MOD_EXPR = "UPPER(TRIM(COALESCE(m.modality, s.study_modality, 'Unknown')))"


def _apply_rates(rows, cfg, mod_col='modality', proc_col=None, rvu_col='total_rvu'):
    """Attach effective_rate and revenue_usd to each row dict."""
    result = []
    for row in rows:
        d = dict(row._mapping)
        mod  = d.get(mod_col)
        proc = d.get(proc_col) if proc_col else None
        rvu  = float(d.get(rvu_col) or 0)
        rate = effective_rate(modality=mod, procedure_code=proc)
        d['rate']        = rate
        d['revenue_usd'] = round(rvu * rate, 2)
        d[rvu_col]       = round(rvu, 2)
        result.append(d)
    return result


def _collect(start: str, end: str) -> dict:
    cfg = _get_config()

    # ── By modality ─────────────────────────────────────────────────────────
    mod_rows = db.session.execute(text(f"""
        SELECT
            {_MOD_EXPR}                           AS modality,
            COUNT(DISTINCT s.study_db_uid)         AS study_count,
            COALESCE(SUM(pdm.rvu_value), 0)        AS total_rvu
        {_STUDY_BASE}
        GROUP BY 1
        ORDER BY total_rvu DESC
    """), {'start': start, 'end': end}).fetchall()
    by_modality = _apply_rates(mod_rows, cfg)

    # ── KPI totals ───────────────────────────────────────────────────────────
    total_revenue = sum(r['revenue_usd'] for r in by_modality)
    total_rvu     = sum(r['total_rvu']   for r in by_modality)
    total_studies = sum(r['study_count'] for r in by_modality)
    rev_per_study = round(total_revenue / total_studies, 2) if total_studies else 0

    # ── Monthly trend (always last 13 months, stacked by modality) ──────────
    trend_rows = db.session.execute(text(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', s.study_date), 'YYYY-MM') AS month,
            {_MOD_EXPR}                                           AS modality,
            COALESCE(SUM(pdm.rvu_value), 0)                       AS total_rvu
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
        LEFT JOIN etl_orders o           ON o.study_db_uid = s.study_db_uid
        LEFT JOIN procedure_duration_map pdm
               ON UPPER(TRIM(o.proc_id)) = UPPER(TRIM(pdm.procedure_code))
        WHERE s.study_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
          AND COALESCE(UPPER(TRIM(COALESCE(m.modality, s.study_modality, ''))), '') != 'SR'
          AND s.study_has_report = true
        GROUP BY 1, 2
        ORDER BY 1, 2
    """), {}).fetchall()

    # Aggregate by month, applying per-modality rates
    month_map: dict = {}
    for row in trend_rows:
        mo  = row.month
        mod = row.modality
        rvu = float(row.total_rvu or 0)
        rev = round(rvu * effective_rate(modality=mod), 2)
        if mo not in month_map:
            month_map[mo] = {'month': mo, 'revenue_usd': 0, 'total_rvu': 0}
        month_map[mo]['revenue_usd'] = round(month_map[mo]['revenue_usd'] + rev, 2)
        month_map[mo]['total_rvu']   = round(month_map[mo]['total_rvu']   + rvu, 2)
    monthly_trend = sorted(month_map.values(), key=lambda x: x['month'])

    # ── By physician (top 10) ────────────────────────────────────────────────
    phys_rows = db.session.execute(text(f"""
        SELECT
            COALESCE(
                TRIM(s.reading_physician_last_name || ' ' || s.reading_physician_first_name),
                'Unassigned'
            )                                     AS physician,
            COUNT(DISTINCT s.study_db_uid)        AS study_count,
            {_MOD_EXPR}                           AS modality,
            COALESCE(SUM(pdm.rvu_value), 0)       AS total_rvu
        {_STUDY_BASE}
          AND s.reading_physician_last_name IS NOT NULL
        GROUP BY 1, 3
        ORDER BY total_rvu DESC
        LIMIT 10
    """), {'start': start, 'end': end}).fetchall()
    by_physician = _apply_rates(phys_rows, cfg)

    # ── Top procedures (top 15) ──────────────────────────────────────────────
    proc_rows = db.session.execute(text(f"""
        SELECT
            UPPER(TRIM(o.proc_id))                 AS procedure_code,
            {_MOD_EXPR}                            AS modality,
            COUNT(DISTINCT s.study_db_uid)         AS study_count,
            COALESCE(SUM(pdm.rvu_value), 0)        AS total_rvu
        {_STUDY_BASE}
          AND o.proc_id IS NOT NULL
          AND TRIM(o.proc_id) != ''
        GROUP BY 1, 2
        ORDER BY total_rvu DESC
        LIMIT 15
    """), {'start': start, 'end': end}).fetchall()
    by_procedure = _apply_rates(proc_rows, cfg, proc_col='procedure_code')

    return {
        'kpi': {
            'total_revenue':  total_revenue,
            'total_rvu':      round(total_rvu, 1),
            'total_studies':  total_studies,
            'rev_per_study':  rev_per_study,
            'global_rate':    cfg['global'],
        },
        'by_modality':   by_modality,
        'monthly_trend': monthly_trend,
        'by_physician':  by_physician,
        'by_procedure':  by_procedure,
    }


# ── Page ───────────────────────────────────────────────────────────────────────

@financial_dashboard_bp.route('/financial/revenue')
@login_required
def financial_dashboard_page():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        from flask import abort
        abort(403)
    today = date.today()
    start = request.args.get('start', today.replace(day=1).isoformat())
    end   = request.args.get('end',   today.isoformat())
    data  = _collect(start, end)
    return render_template('financial_dashboard.html', data=data, start=start, end=end)


# ── JSON refresh (date change without page reload) ─────────────────────────────

@financial_dashboard_bp.route('/api/financial/dashboard')
@login_required
def api_financial_dashboard():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        from flask import abort
        abort(403)
    today = date.today()
    start = request.args.get('start', today.replace(day=1).isoformat())
    end   = request.args.get('end',   today.isoformat())
    return jsonify(_collect(start, end))
