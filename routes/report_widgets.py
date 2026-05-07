"""
routes/report_widgets.py
Widget query functions for the custom report composer.

Each widget_* function receives:
    db      — SQLAlchemy db instance
    filters — dict: {date_from, date_to, modality, physician_id, patient_class}
    config  — dict: widget-specific options (top_n, chart_type, group_by, ...)

Returns a plain dict that is JSON-serialisable.
Financial widgets are flagged with FINANCIAL = True on the function.
"""

from sqlalchemy import text

# ── Common SQL fragments ──────────────────────────────────────────────────────

_BASE_JOIN = """
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
"""

_WHERE = """
    WHERE s.study_date BETWEEN :date_from AND :date_to
      AND COALESCE(m.modality, s.study_modality, '') != 'SR'
      AND (:modality::text     IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
      AND (:physician_id::bigint IS NULL OR s.reading_physician_id = :physician_id)
      AND (:patient_class::text IS NULL OR s.patient_class = :patient_class)
"""

_MOD_EXPR = "COALESCE(m.modality, s.study_modality, 'Unknown')"


def _p(filters):
    """Build the standard SQL parameter dict from the global filters."""
    return {
        "date_from":    filters.get("date_from"),
        "date_to":      filters.get("date_to"),
        "modality":     filters.get("modality") or None,
        "physician_id": int(filters["physician_id"]) if filters.get("physician_id") else None,
        "patient_class": filters.get("patient_class") or None,
    }


def _pct(part, total):
    return round(part / total * 100, 1) if total else 0


# ── Catalogue (widget key → metadata) ────────────────────────────────────────
# Used by the composer palette.

WIDGET_CATALOGUE = [
    {
        "key":         "study_count",
        "label":       "Study Volume",
        "icon":        "bi-bar-chart-fill",
        "color":       "#60a5fa",
        "description": "Total studies with trend vs prior period",
        "financial":   False,
        "config_keys": [("group_by", "Period", "select", ["day", "week", "month"])],
    },
    {
        "key":         "modality_split",
        "label":       "Modality Breakdown",
        "icon":        "bi-pie-chart-fill",
        "color":       "#a78bfa",
        "description": "Studies per modality — bar or pie chart",
        "financial":   False,
        "config_keys": [("chart_type", "Chart", "select", ["bar", "pie"])],
    },
    {
        "key":         "physician_perf",
        "label":       "Physician Performance",
        "icon":        "bi-person-badge-fill",
        "color":       "#34d399",
        "description": "Studies & avg TAT per radiologist",
        "financial":   False,
        "config_keys": [("top_n", "Top N", "number", 10)],
    },
    {
        "key":         "tat_summary",
        "label":       "TAT Summary",
        "icon":        "bi-clock-history",
        "color":       "#f59e0b",
        "description": "Avg / Median / P90 turnaround time",
        "financial":   False,
        "config_keys": [],
    },
    {
        "key":         "patient_class",
        "label":       "Patient Class Split",
        "icon":        "bi-people-fill",
        "color":       "#fb923c",
        "description": "ER / IP / OP / Other breakdown",
        "financial":   False,
        "config_keys": [],
    },
    {
        "key":         "shift_breakdown",
        "label":       "Shift Breakdown",
        "icon":        "bi-sun-fill",
        "color":       "#fbbf24",
        "description": "Studies per shift (morning / afternoon / night)",
        "financial":   False,
        "config_keys": [],
    },
    {
        "key":         "device_util",
        "label":       "Device Utilisation",
        "icon":        "bi-hdd-stack-fill",
        "color":       "#22d3ee",
        "description": "Study load per AE title / device",
        "financial":   False,
        "config_keys": [("top_n", "Top N", "number", 10)],
    },
    {
        "key":         "report_status",
        "label":       "Report Status",
        "icon":        "bi-file-earmark-check-fill",
        "color":       "#4ade80",
        "description": "Signed / unsigned / pending counts",
        "financial":   False,
        "config_keys": [],
    },
    {
        "key":         "referring_phys",
        "label":       "Referring Physicians",
        "icon":        "bi-person-lines-fill",
        "color":       "#e879f9",
        "description": "Top referring physicians by volume",
        "financial":   False,
        "config_keys": [("top_n", "Top N", "number", 10)],
    },
    # ── Financial ────────────────────────────────────────────────────────────
    {
        "key":         "rvu_summary",
        "label":       "RVU / Revenue Summary",
        "icon":        "bi-currency-dollar",
        "color":       "#f87171",
        "description": "Total RVU, revenue, revenue per study",
        "financial":   True,
        "config_keys": [],
    },
    {
        "key":         "revenue_by_modality",
        "label":       "Revenue by Modality",
        "icon":        "bi-graph-up-arrow",
        "color":       "#f87171",
        "description": "Revenue & RVU breakdown per modality",
        "financial":   True,
        "config_keys": [],
    },
    {
        "key":         "revenue_by_physician",
        "label":       "Revenue by Physician",
        "icon":        "bi-person-fill-up",
        "color":       "#f87171",
        "description": "Revenue & RVU breakdown per radiologist",
        "financial":   True,
        "config_keys": [("top_n", "Top N", "number", 10)],
    },
]

FINANCIAL_KEYS = {w["key"] for w in WIDGET_CATALOGUE if w["financial"]}
WIDGET_META    = {w["key"]: w for w in WIDGET_CATALOGUE}


# ── Non-financial widgets ─────────────────────────────────────────────────────

def widget_study_count(db, filters, config):
    p = _p(filters)
    group_by = config.get("group_by", "month")

    trunc_map = {"day": "day", "week": "week", "month": "month"}
    trunc = trunc_map.get(group_by, "month")

    # Trend: prior period of same length
    from datetime import datetime, timedelta
    try:
        d_from = datetime.fromisoformat(p["date_from"]).date()
        d_to   = datetime.fromisoformat(p["date_to"]).date()
        delta  = (d_to - d_from).days + 1
        prev_from = str(d_from - timedelta(days=delta))
        prev_to   = str(d_from - timedelta(days=1))
    except Exception:
        prev_from = prev_to = p["date_from"]

    total = db.session.execute(text(f"""
        SELECT COUNT(*) {_BASE_JOIN} {_WHERE}
    """), p).scalar() or 0

    prev_params = {**p, "date_from": prev_from, "date_to": prev_to}
    prev_total = db.session.execute(text(f"""
        SELECT COUNT(*) {_BASE_JOIN} {_WHERE}
    """), prev_params).scalar() or 0

    trend_pct = round((total - prev_total) / prev_total * 100, 1) if prev_total else None

    by_period = db.session.execute(text(f"""
        SELECT TO_CHAR(DATE_TRUNC('{trunc}', s.study_date), 'YYYY-MM-DD') AS label,
               COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
        GROUP BY 1 ORDER BY 1
    """), p).fetchall()

    return {
        "total":      total,
        "prev_total": prev_total,
        "trend_pct":  trend_pct,
        "group_by":   group_by,
        "by_period":  [{"label": r.label, "count": r.count} for r in by_period],
    }


def widget_modality_split(db, filters, config):
    p = _p(filters)
    rows = db.session.execute(text(f"""
        SELECT {_MOD_EXPR} AS modality, COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """), p).fetchall()
    total = sum(r.count for r in rows)
    return {
        "chart_type": config.get("chart_type", "bar"),
        "rows": [{"modality": r.modality, "count": r.count, "pct": _pct(r.count, total)} for r in rows],
    }


def widget_physician_perf(db, filters, config):
    p = _p(filters)
    top_n = int(config.get("top_n") or 10)
    rows = db.session.execute(text(f"""
        SELECT s.reading_physician_first_name AS first_name,
               s.reading_physician_last_name  AS last_name,
               COUNT(*)                        AS studies,
               ROUND(AVG(
                   EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date::timestamp)) / 3600.0
               )::numeric, 1)                  AS avg_tat_h
        {_BASE_JOIN} {_WHERE}
          AND s.reading_physician_last_name IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n}).fetchall()
    return {
        "top_n": top_n,
        "rows": [
            {
                "name":      f"{r.first_name or ''} {r.last_name or ''}".strip(),
                "studies":   r.studies,
                "avg_tat_h": float(r.avg_tat_h) if r.avg_tat_h is not None else None,
            }
            for r in rows
        ],
    }


def widget_tat_summary(db, filters, config):
    p = _p(filters)

    def _tat(ts_col, date_col):
        return f"EXTRACT(EPOCH FROM ({ts_col} - {date_col}::timestamp)) / 3600.0"

    prelim = db.session.execute(text(f"""
        SELECT ROUND(AVG({_tat('s.rep_prelim_timestamp','s.study_date')})::numeric,1)           AS avg_h,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY {_tat('s.rep_prelim_timestamp','s.study_date')})::numeric,1)        AS median_h,
               ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (
                   ORDER BY {_tat('s.rep_prelim_timestamp','s.study_date')})::numeric,1)        AS p90_h
        {_BASE_JOIN} {_WHERE}
          AND s.rep_prelim_timestamp IS NOT NULL
          AND s.rep_prelim_timestamp > s.study_date::timestamp
    """), p).fetchone()

    final = db.session.execute(text(f"""
        SELECT ROUND(AVG({_tat('s.rep_final_timestamp','s.study_date')})::numeric,1)            AS avg_h,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY {_tat('s.rep_final_timestamp','s.study_date')})::numeric,1)         AS median_h,
               ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (
                   ORDER BY {_tat('s.rep_final_timestamp','s.study_date')})::numeric,1)         AS p90_h
        {_BASE_JOIN} {_WHERE}
          AND s.rep_final_timestamp IS NOT NULL
          AND s.rep_final_timestamp > s.study_date::timestamp
    """), p).fetchone()

    def _row(r):
        if not r:
            return {"avg_h": None, "median_h": None, "p90_h": None}
        return {"avg_h": float(r.avg_h) if r.avg_h else None,
                "median_h": float(r.median_h) if r.median_h else None,
                "p90_h": float(r.p90_h) if r.p90_h else None}

    return {"prelim": _row(prelim), "final": _row(final)}


def widget_patient_class(db, filters, config):
    p = _p(filters)
    rows = db.session.execute(text(f"""
        SELECT COALESCE(s.patient_class, 'Unknown') AS patient_class, COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """), p).fetchall()
    total = sum(r.count for r in rows)
    return {"rows": [{"patient_class": r.patient_class, "count": r.count, "pct": _pct(r.count, total)} for r in rows]}


def widget_shift_breakdown(db, filters, config):
    p = _p(filters)
    # Read shift times from settings
    shift_rows = db.session.execute(text("""
        SELECT key, value FROM settings
        WHERE key IN ('shift_morning_start','shift_morning_end',
                      'shift_afternoon_start','shift_afternoon_end',
                      'shift_night_start','shift_night_end')
    """)).fetchall()
    shifts = {r.key: r.value for r in shift_rows}
    m_start = shifts.get("shift_morning_start",   "07:00")
    m_end   = shifts.get("shift_morning_end",     "14:59")
    a_start = shifts.get("shift_afternoon_start", "15:00")
    a_end   = shifts.get("shift_afternoon_end",   "21:59")

    rows = db.session.execute(text(f"""
        SELECT
            CASE
                WHEN CAST(s.rep_final_timestamp AS TIME) BETWEEN :m_start::time AND :m_end::time
                    THEN 'Morning'
                WHEN CAST(s.rep_final_timestamp AS TIME) BETWEEN :a_start::time AND :a_end::time
                    THEN 'Afternoon'
                ELSE 'Night'
            END AS shift,
            COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
          AND s.rep_final_timestamp IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """), {**p, "m_start": m_start, "m_end": m_end, "a_start": a_start, "a_end": a_end}).fetchall()
    total = sum(r.count for r in rows)
    return {"rows": [{"shift": r.shift, "count": r.count, "pct": _pct(r.count, total)} for r in rows]}


def widget_device_util(db, filters, config):
    p = _p(filters)
    top_n = int(config.get("top_n") or 10)
    rows = db.session.execute(text(f"""
        SELECT s.storing_ae AS ae_title, {_MOD_EXPR} AS modality, COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
          AND s.storing_ae IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n}).fetchall()
    return {"top_n": top_n, "rows": [{"ae_title": r.ae_title, "modality": r.modality, "count": r.count} for r in rows]}


def widget_report_status(db, filters, config):
    p = _p(filters)
    rows = db.session.execute(text(f"""
        SELECT COALESCE(s.report_status, 'Unknown') AS status, COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """), p).fetchall()
    total = sum(r.count for r in rows)
    return {"rows": [{"status": r.status, "count": r.count, "pct": _pct(r.count, total)} for r in rows]}


def widget_referring_phys(db, filters, config):
    p = _p(filters)
    top_n = int(config.get("top_n") or 10)
    rows = db.session.execute(text(f"""
        SELECT s.referring_physician_first_name AS first_name,
               s.referring_physician_last_name  AS last_name,
               COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
          AND s.referring_physician_last_name IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n}).fetchall()
    return {
        "top_n": top_n,
        "rows": [{"name": f"{r.first_name or ''} {r.last_name or ''}".strip(), "count": r.count} for r in rows],
    }


# ── Financial widgets ─────────────────────────────────────────────────────────

_FIN_JOIN = """
    FROM etl_didb_studies s
    LEFT JOIN aetitle_modality_map m    ON m.aetitle = s.storing_ae
    LEFT JOIN etl_orders o              ON o.study_db_uid = s.study_db_uid
    LEFT JOIN procedure_duration_map pdm
           ON UPPER(TRIM(o.proc_id)) = UPPER(TRIM(pdm.procedure_code))
"""

_FIN_WHERE = """
    WHERE s.study_date BETWEEN :date_from AND :date_to
      AND COALESCE(UPPER(TRIM(COALESCE(m.modality, s.study_modality, ''))), '') != 'SR'
      AND s.study_has_report = true
      AND (:modality::text      IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
      AND (:physician_id::bigint IS NULL OR s.reading_physician_id = :physician_id)
      AND (:patient_class::text  IS NULL OR s.patient_class = :patient_class)
"""


def _rvu_rate(modality):
    try:
        from utils.financial import effective_rate
        return effective_rate(modality=modality)
    except Exception:
        return 0.0


def widget_rvu_summary(db, filters, config):
    p = _p(filters)
    row = db.session.execute(text(f"""
        SELECT COUNT(DISTINCT s.study_db_uid)  AS total_studies,
               COALESCE(SUM(pdm.rvu_value), 0) AS total_rvu
        {_FIN_JOIN} {_FIN_WHERE}
    """), p).fetchone()
    total_studies = row.total_studies or 0
    total_rvu     = float(row.total_rvu or 0)

    mod_rows = db.session.execute(text(f"""
        SELECT {_MOD_EXPR} AS modality, COALESCE(SUM(pdm.rvu_value), 0) AS rvu
        {_FIN_JOIN} {_FIN_WHERE}
        GROUP BY 1
    """), p).fetchall()

    total_revenue = sum(_rvu_rate(r.modality) * float(r.rvu) for r in mod_rows)
    rev_per_study = round(total_revenue / total_studies, 2) if total_studies else 0

    return {
        "total_studies":  total_studies,
        "total_rvu":      round(total_rvu, 2),
        "total_revenue":  round(total_revenue, 2),
        "rev_per_study":  rev_per_study,
    }


def widget_revenue_by_modality(db, filters, config):
    p = _p(filters)
    rows = db.session.execute(text(f"""
        SELECT {_MOD_EXPR} AS modality,
               COUNT(DISTINCT s.study_db_uid)  AS study_count,
               COALESCE(SUM(pdm.rvu_value), 0) AS total_rvu
        {_FIN_JOIN} {_FIN_WHERE}
        GROUP BY 1 ORDER BY 3 DESC
    """), p).fetchall()
    result = []
    for r in rows:
        rate    = _rvu_rate(r.modality)
        rvu     = float(r.total_rvu)
        revenue = round(rate * rvu, 2)
        result.append({"modality": r.modality, "study_count": r.study_count,
                       "total_rvu": round(rvu, 2), "rate": rate, "revenue_usd": revenue})
    return {"rows": result}


def widget_revenue_by_physician(db, filters, config):
    p = _p(filters)
    top_n = int(config.get("top_n") or 10)
    rows = db.session.execute(text(f"""
        SELECT s.reading_physician_first_name AS first_name,
               s.reading_physician_last_name  AS last_name,
               COUNT(DISTINCT s.study_db_uid) AS studies,
               COALESCE(SUM(pdm.rvu_value), 0) AS total_rvu,
               {_MOD_EXPR}                    AS top_modality
        {_FIN_JOIN} {_FIN_WHERE}
          AND s.reading_physician_last_name IS NOT NULL
        GROUP BY 1, 2, 5
        ORDER BY 4 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n}).fetchall()
    result = []
    for r in rows:
        rate    = _rvu_rate(r.top_modality)
        rvu     = float(r.total_rvu)
        revenue = round(rate * rvu, 2)
        result.append({
            "name":        f"{r.first_name or ''} {r.last_name or ''}".strip(),
            "studies":     r.studies,
            "total_rvu":   round(rvu, 2),
            "revenue_usd": revenue,
        })
    return {"top_n": top_n, "rows": result}


# ── Dispatch table ────────────────────────────────────────────────────────────

_DISPATCH = {
    "study_count":          widget_study_count,
    "modality_split":       widget_modality_split,
    "physician_perf":       widget_physician_perf,
    "tat_summary":          widget_tat_summary,
    "patient_class":        widget_patient_class,
    "shift_breakdown":      widget_shift_breakdown,
    "device_util":          widget_device_util,
    "report_status":        widget_report_status,
    "referring_phys":       widget_referring_phys,
    "rvu_summary":          widget_rvu_summary,
    "revenue_by_modality":  widget_revenue_by_modality,
    "revenue_by_physician": widget_revenue_by_physician,
}


def run_widget(db, section_type, filters, config):
    """Run a single widget. Returns data dict or raises."""
    fn = _DISPATCH.get(section_type)
    if not fn:
        raise ValueError(f"Unknown widget type: {section_type}")
    return fn(db, filters, config)
