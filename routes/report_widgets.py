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
from utils.site_resolver import default_site

# ── Common SQL fragments ──────────────────────────────────────────────────────

_BASE_JOIN = """
    FROM etl_didb_studies s
    LEFT JOIN LATERAL (
        SELECT modality FROM aetitle_modality_map
        WHERE UPPER(TRIM(aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
    ) m ON TRUE
"""

_WHERE = """
    WHERE s.study_date BETWEEN :date_from AND :date_to
      AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
      AND (CAST(:modality AS TEXT)      IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
      AND (CAST(:physician_id AS TEXT) IS NULL OR s.reading_physician_id = :physician_id)
      AND (CAST(:patient_class AS TEXT)  IS NULL OR UPPER(s.patient_class) = UPPER(:patient_class))
"""

_MOD_EXPR = "COALESCE(m.modality, s.study_modality, 'Unknown')"

# ── Known demo/placeholder pollution ─────────────────────────────────────────
# docs/LAUMC_NEXT_SESSION.md's GOTCHAS section documents this as already loaded,
# faithfully, into etl_didb_studies: "Placeholders != NULL: 'Referring, Generic'
# (accession 4101031379966/...68 -- seen again in the RESOURCE_ID sample,
# type-6/3 rows with value 'SCH'), test patients, CSH service account ->
# exclusion list, separate from NULL handling. Not yet actually filtered out of
# any loaded table -- exclusion is a report-time filter TODO." No report in this
# codebase implements that filter yet (checked report_22/25/27/super_report/
# oru_analytics/etc.) -- applied here for the two widgets the operator actually
# observed contaminated with it (physician_perf, rad_modality_matrix). A widget
# using these must LEFT JOIN etl_patient_view aliased `pv` (ON pv.patient_db_uid
# = s.patient_db_uid) and pass its own resolved-physician-name SQL expression to
# _no_demo_name_sql().
_DEMO_ACCESSIONS_SQL = "COALESCE(TRIM(s.accession_number), '') NOT IN ('4101031379966', '4101031379968')"
_DEMO_PATIENT_SQL    = "COALESCE(pv.id, '') NOT ILIKE '%test%'"


def _no_demo_name_sql(name_expr):
    """WHERE fragment excluding placeholder/service-account physician names
    (the "Referring, Generic" placeholder and the CSH/SCH service-account
    codes from the gotcha above) from a resolved-name SQL expression."""
    return (
        f" AND UPPER({name_expr}) NOT IN ('CSH', 'SCH')"
        f" AND {name_expr} NOT ILIKE '%generic%'"
        f" AND {name_expr} NOT ILIKE '%referring%'"
    )


def _p(filters):
    """Build the standard SQL parameter dict from the global filters."""
    return {
        "date_from":    filters.get("date_from"),
        "date_to":      filters.get("date_to"),
        "modality":     filters.get("modality") or None,
        # reading_physician_id is TEXT (some sites' PACS emits a composite ID, not a
        # pure integer) — keep the filter value as a string, don't force int().
        "physician_id": str(filters["physician_id"]) if filters.get("physician_id") else None,
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
    {
        "key":         "rad_modality_matrix",
        "label":       "Radiologist Workload Matrix",
        "icon":        "bi-person-check-fill",
        "color":       "#60a5fa",
        "description": "Reports per radiologist — toggle between modality and AE title breakdown",
        "financial":   False,
        "config_keys": [],
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

    # Why a small/recent date range came back completely empty: at LAUMC
    # radiologists sign in the RIS, and PACS's own etl_didb_studies.
    # reading_physician_* fields are not reliably synced back for recent
    # studies -- the exact same root cause report_25 hit and fixed in
    # migration 0070 (confirmed live against Oracle, 2026-07-26), not yet
    # rolled out to every report per docs/LAUMC_NEXT_SESSION.md. Every row was
    # failing the old "reading_physician_last_name IS NOT NULL" gate. Falls
    # back to the RIS-sourced hl7_oru_reports row (1:1 per accession_number,
    # enforced by the ux_hl7_oru_reports_accession unique index -- safe to
    # LEFT JOIN without fear of fan-out) and resolves its physician_id (a raw
    # RIS code) to a real name via std_resources_ris.resource_id, the same
    # join report_36.get_kpi_detailed_reading() already uses for this exact
    # problem (moved there from report_25 2026-07-31). Separately,
    # s.rep_final_timestamp (PACS) is itself sparse/unreliable on this install
    # (operator, 2026-07-27) -- s.rep_study_last_composed_ts is the PACS field
    # confirmed reliable here (report_25.py's standard anchor); inserted ahead
    # of it below, RIS fallback (o.result_datetime) kept last and untouched.
    _name = (
        "COALESCE("
        "NULLIF(TRIM(CONCAT(s.reading_physician_first_name, ' ', s.reading_physician_last_name)), ''), "
        "res.common_name, o.physician_id)"
    )
    rows = db.session.execute(text(f"""
        SELECT
            {_name} AS name,
            COUNT(*) AS studies,
            ROUND(AVG(
                EXTRACT(EPOCH FROM (
                    COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) - s.study_date::timestamp
                )) / 3600.0
            )::numeric, 1) AS avg_tat_h
        {_BASE_JOIN}
        LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number
        LEFT JOIN std_resources_ris res ON res.resource_id = o.physician_id
        LEFT JOIN etl_patient_view pv ON pv.patient_db_uid = s.patient_db_uid
        {_WHERE}
          AND {_name} IS NOT NULL
          AND {_DEMO_ACCESSIONS_SQL}
          AND {_DEMO_PATIENT_SQL}
          {_no_demo_name_sql(_name)}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n}).fetchall()
    return {
        "top_n": top_n,
        "rows": [
            {
                "name":      r.name,
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

    # rep_final_timestamp (PACS) is sparse/unreliable on this install (operator,
    # 2026-07-27) -- rep_study_last_composed_ts is the PACS field confirmed
    # reliable here and already the standard TAT anchor in report_25.py.
    _final_ts = "COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp)"
    final = db.session.execute(text(f"""
        SELECT ROUND(AVG({_tat(_final_ts,'s.study_date')})::numeric,1)            AS avg_h,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY {_tat(_final_ts,'s.study_date')})::numeric,1)         AS median_h,
               ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (
                   ORDER BY {_tat(_final_ts,'s.study_date')})::numeric,1)         AS p90_h
        {_BASE_JOIN} {_WHERE}
          AND {_final_ts} IS NOT NULL
          AND {_final_ts} > s.study_date::timestamp
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
    # Raw s.patient_class text has many near-duplicate variants for the same
    # real category (e.g. "Inpatient", "INPT", "IN", "I" all mean the same
    # thing) -- grouping on the raw column was producing far more distinct rows
    # than there are real buckets, contradicting this widget's own catalogue
    # description ("ER / IP / OP / Other breakdown"). Bucketed using the same
    # broad prefix rules report_25's _kpi_class_bucket() already established
    # for this exact data (accession prefix 2XE = ER; patient_class IN/I/IP =
    # Inpatient; OUT/AMB/O/OP = Outpatient; everything else = Other).
    rows = db.session.execute(text(f"""
        SELECT
            CASE
                WHEN UPPER(COALESCE(s.accession_number, '')) LIKE '2XE%' THEN 'ER'
                WHEN UPPER(COALESCE(s.patient_class, '')) LIKE 'IN%'
                  OR UPPER(s.patient_class) IN ('I', 'IP') THEN 'IP'
                WHEN UPPER(COALESCE(s.patient_class, '')) LIKE 'OUT%'
                  OR UPPER(COALESCE(s.patient_class, '')) LIKE 'AMB%'
                  OR UPPER(s.patient_class) IN ('O', 'OP') THEN 'OP'
                ELSE 'Other'
            END AS patient_class,
            COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """), p).fetchall()
    total = sum(r.count for r in rows)
    return {"rows": [{"patient_class": r.patient_class, "count": r.count, "pct": _pct(r.count, total)} for r in rows]}


def widget_shift_breakdown(db, filters, config):
    p = _p(filters)

    # Shift hours are stored as integers by the save-shifts form (e.g. 7, 15, 23).
    shift_rows = db.session.execute(text("""
        SELECT key, value FROM settings
        WHERE key IN ('shift_morning_start','shift_morning_end',
                      'shift_afternoon_start','shift_afternoon_end')
    """)).fetchall()
    sc = {}
    for r in shift_rows:
        try:
            sc[r.key] = int(r.value)
        except (ValueError, TypeError):
            pass

    ms  = sc.get("shift_morning_start",    7)
    me  = sc.get("shift_morning_end",     15)
    a_s = sc.get("shift_afternoon_start", 15)
    a_e = sc.get("shift_afternoon_end",   23)

    # Build optional secondary filters (only when values are set)
    sec = ""
    sec_params = {}
    if p.get("patient_class"):
        sec += " AND UPPER(s.patient_class) = UPPER(:patient_class)"
        sec_params["patient_class"] = p["patient_class"]
    if p.get("physician_id"):
        sec += " AND s.reading_physician_id = :physician_id"
        sec_params["physician_id"] = p["physician_id"]
    if p.get("modality"):
        sec += " AND COALESCE(m.modality, s.study_modality) = :modality"
        sec_params["modality"] = p["modality"]

    rows = db.session.execute(text(f"""
        SELECT
            CASE
                WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :ms
                 AND EXTRACT(HOUR FROM o.scheduled_datetime) <  :me  THEN 'Morning'
                WHEN EXTRACT(HOUR FROM o.scheduled_datetime) >= :a_s
                 AND EXTRACT(HOUR FROM o.scheduled_datetime) <  :a_e THEN 'Afternoon'
                ELSE 'Night'
            END AS shift,
            COUNT(DISTINCT s.study_db_uid) AS count
        FROM etl_orders o
        JOIN etl_didb_studies s ON s.study_db_uid = o.study_db_uid
        LEFT JOIN LATERAL (
            SELECT modality FROM aetitle_modality_map
            WHERE UPPER(TRIM(aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
        ) m ON TRUE
        WHERE s.study_date BETWEEN :date_from AND :date_to
          AND o.scheduled_datetime IS NOT NULL
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
          {sec}
        GROUP BY 1
        ORDER BY 2 DESC
    """), {"date_from": p["date_from"], "date_to": p["date_to"],
           "ms": ms, "me": me, "a_s": a_s, "a_e": a_e,
           **sec_params}).fetchall()

    total = sum(r.count for r in rows)
    return {
        "rows": [
            {"shift": r.shift, "count": r.count, "pct": _pct(r.count, total)}
            for r in rows
        ]
    }


_WEEKDAY_LABELS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def _device_weekday_utilization(db, p, aes):
    """
    Per-AE, per-weekday utilisation % for widget_device_util -- the same
    capacity-vs-load approach report_34's device utilization matrix uses
    (docs/LAUMC_NEXT_SESSION.md punch-list #13 asked for this breakdown here
    too, e.g. "80% Monday, 65% Tuesday..."): weekly opening-minutes capacity
    (device_weekly_schedule, falling back to aetitle_modality_map.
    daily_capacity_minutes, then a flat 480) vs actual minutes used (std_pps
    measured start/end where available, falling back per-cell to the
    procedure_duration_map estimate -- report_34's exact fallback chain).
    `aes` must already be upper-cased.
    """
    # How many Mondays/Tuesdays/etc. actually fall inside the selected range --
    # capacity for the period = opening_minutes x occurrences of that weekday.
    # ISODOW is 1=Mon..7=Sun; -1 converts to device_weekly_schedule's own
    # 0=Mon..6=Sun convention (CLAUDE.md).
    occ_rows = db.session.execute(text("""
        SELECT (EXTRACT(ISODOW FROM d) - 1)::int AS dow, COUNT(*) AS occ
        FROM generate_series(CAST(:date_from AS DATE), CAST(:date_to AS DATE), interval '1 day') d
        GROUP BY 1
    """), {"date_from": p["date_from"], "date_to": p["date_to"]}).fetchall()
    occurrences = {r.dow: r.occ for r in occ_rows}

    sched_rows = db.session.execute(text("""
        SELECT UPPER(TRIM(ws.aetitle)) AS ae, ws.day_of_week AS dow,
               COALESCE(ws.std_opening_minutes, m.daily_capacity_minutes, 480) AS opening_min
        FROM device_weekly_schedule ws
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle))
        WHERE UPPER(TRIM(ws.aetitle)) = ANY(:aes)
    """), {"aes": aes}).fetchall()
    schedule = {(r.ae, r.dow): float(r.opening_min) for r in sched_rows}

    flat_cap_rows = db.session.execute(text("""
        SELECT UPPER(TRIM(aetitle)) AS ae, COALESCE(daily_capacity_minutes, 480) AS cap
        FROM aetitle_modality_map WHERE UPPER(TRIM(aetitle)) = ANY(:aes)
    """), {"aes": aes}).fetchall()
    flat_capacity = {r.ae: float(r.cap) for r in flat_cap_rows}

    # Actual measured minutes (std_pps), respecting the same modality/
    # patient_class filters as the rest of the widget. std_pps may not exist
    # / be empty on non-RIS sites -- the estimate fallback below covers that.
    actual = {}
    try:
        pps_rows = db.session.execute(text("""
            SELECT UPPER(TRIM(pps.performing_ae_title)) AS ae,
                   (EXTRACT(ISODOW FROM pps.start_datetime) - 1)::int AS dow,
                   SUM(EXTRACT(EPOCH FROM (pps.end_datetime - pps.start_datetime)) / 60) AS mins
            FROM std_pps pps
            JOIN etl_didb_studies s ON s.study_db_uid = pps.study_db_uid
            LEFT JOIN LATERAL (
                SELECT modality FROM aetitle_modality_map
                WHERE UPPER(TRIM(aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
            ) m ON TRUE
            WHERE pps.start_datetime BETWEEN :date_from AND :date_to
              AND pps.end_datetime IS NOT NULL AND pps.end_datetime > pps.start_datetime
              AND UPPER(TRIM(pps.performing_ae_title)) = ANY(:aes)
              AND (CAST(:modality AS TEXT) IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
              AND (CAST(:patient_class AS TEXT) IS NULL OR UPPER(s.patient_class) = UPPER(:patient_class))
            GROUP BY 1, 2
        """), {**p, "aes": aes}).fetchall()
        actual = {(r.ae, r.dow): float(r.mins or 0) for r in pps_rows}
    except Exception:
        db.session.rollback()

    # Estimated minutes (procedure_duration_map.duration_minutes, default 15
    # min/study -- same default migration 0070 uses) for AE/weekday cells with
    # no PPS actuals. Joins procedure_duration_map straight off
    # s.procedure_code (etl_didb_studies already carries it) -- NOT via
    # etl_orders. etl_orders.study_db_uid has no unique constraint (CLAUDE.md's
    # "Expensive CTEs" rule flags etl_orders scans needing MODE() WITHIN GROUP
    # dedup for exactly this reason: multiple order rows commonly reference the
    # same study_db_uid, e.g. amendment/order_control history rows), so the
    # previous `LEFT JOIN etl_orders o ON o.study_db_uid = s.study_db_uid` fanned
    # a single study out into N duplicate rows before the SUM(), inflating load
    # by however many order rows matched -- the root cause of the >100% (up to
    # ~580%) utilisation values the operator saw. report_25.get_gold_standard_data()
    # / report_34's port of it (the reference implementation) never join
    # etl_orders for this calculation at all; they read s.procedure_code
    # directly. Matched here.
    est_rows = db.session.execute(text("""
        SELECT UPPER(TRIM(s.storing_ae)) AS ae,
               (EXTRACT(ISODOW FROM s.study_date) - 1)::int AS dow,
               SUM(COALESCE(pdm.duration_minutes, 15)) AS mins
        FROM etl_didb_studies s
        LEFT JOIN LATERAL (
            SELECT modality FROM aetitle_modality_map
            WHERE UPPER(TRIM(aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
        ) m ON TRUE
        LEFT JOIN procedure_duration_map pdm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pdm.procedure_code))
        WHERE s.study_date BETWEEN :date_from AND :date_to
          AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
          AND UPPER(TRIM(s.storing_ae)) = ANY(:aes)
          AND (CAST(:modality AS TEXT) IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
          AND (CAST(:patient_class AS TEXT) IS NULL OR UPPER(s.patient_class) = UPPER(:patient_class))
        GROUP BY 1, 2
    """), {**p, "aes": aes}).fetchall()
    estimate = {(r.ae, r.dow): float(r.mins or 0) for r in est_rows}

    matrix = []
    for ae in aes:
        days, total_load, total_cap = [], 0.0, 0.0
        for dow in range(7):
            load = actual.get((ae, dow))
            if load is None:
                load = estimate.get((ae, dow), 0.0)
            opening = schedule.get((ae, dow), flat_capacity.get(ae, 480.0))
            cap = opening * occurrences.get(dow, 0)
            pct = round(load / cap * 100, 1) if cap > 0 else 0.0
            days.append({"weekday": _WEEKDAY_LABELS[dow], "pct": pct, "minutes": round(load, 1)})
            total_load += load
            total_cap += cap
        avg_pct = round(total_load / total_cap * 100, 1) if total_cap > 0 else 0.0
        matrix.append({"ae_title": ae, "avg_pct": avg_pct, "days": days})

    matrix.sort(key=lambda m: m["avg_pct"], reverse=True)
    return matrix


def widget_device_util(db, filters, config):
    p = _p(filters)
    top_n = int(config.get("top_n") or 10)

    # LAUMC site rule (operator instruction, 2026-07-26 -- full rationale in
    # report_25.get_gold_standard_data()): reports show RH (main site) only,
    # SJH (satellite) excluded, resolved via the device (storing_ae ->
    # aetitle_modality_map.site_id) since etl_didb_studies.site_id is never
    # populated. default_site() resolves to None (filter skipped, not applied)
    # on a non-LAUMC/single-site install. This widget had no site-scope filter
    # at all -- added here to match report_25/31-35/34's established pattern
    # (previously SJH devices like SJHCSAPWFMFIR leaked into the table and,
    # via _device_weekday_utilization, into the weekday matrix too).
    rh_site_id = default_site()
    site_filter = ""
    site_params = {}
    if rh_site_id is not None:
        site_params["rh_site_id"] = rh_site_id
        site_filter = (
            " AND UPPER(TRIM(s.storing_ae)) IN "
            "(SELECT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE site_id = :rh_site_id)"
        )

    rows = db.session.execute(text(f"""
        SELECT s.storing_ae AS ae_title, {_MOD_EXPR} AS modality, COUNT(*) AS count
        {_BASE_JOIN} {_WHERE}
          AND s.storing_ae IS NOT NULL
          {site_filter}
        GROUP BY 1, 2 ORDER BY 3 DESC
        LIMIT :top_n
    """), {**p, "top_n": top_n, **site_params}).fetchall()

    # Per-weekday utilisation matrix (operator ask: match report_34's style),
    # limited to the same top-N AEs already shown in the table above.
    top_aes = sorted({r.ae_title.upper().strip() for r in rows if r.ae_title})
    weekday_matrix = _device_weekday_utilization(db, p, top_aes) if top_aes else []

    return {
        "top_n": top_n,
        "rows": [{"ae_title": r.ae_title, "modality": r.modality, "count": r.count} for r in rows],
        "weekday_matrix": weekday_matrix,
    }


def widget_report_status(db, filters, config):
    p = _p(filters)
    # Was reading s.report_status, a separate PACS column from s.study_status --
    # report_status isn't reliably populated at LAUMC (unrelated to the
    # RIS-vs-PACS TAT gap fixed elsewhere; this field itself is just mostly
    # NULL/blank at this site), which is why almost every row fell to
    # 'Unknown'. Every other place in this codebase that reports read/report
    # workflow status (report_22's status breakdown, report_25 and
    # viewer_controller's "unread" detection: `study_status ILIKE '%unread%'`,
    # report_cache's dropdown) reads s.study_status instead, and
    # ETL_JOBS/schema_discovery.py documents it as carrying real
    # UNREAD/READ/REPORTED values. Switched to match.
    rows = db.session.execute(text(f"""
        SELECT COALESCE(NULLIF(TRIM(s.study_status), ''), 'Unknown') AS status, COUNT(*) AS count
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
    LEFT JOIN LATERAL (
        SELECT modality FROM aetitle_modality_map
        WHERE UPPER(TRIM(aetitle)) = UPPER(TRIM(s.storing_ae)) LIMIT 1
    ) m ON TRUE
    LEFT JOIN etl_orders o              ON o.study_db_uid = s.study_db_uid
    LEFT JOIN procedure_duration_map pdm
           ON UPPER(TRIM(o.proc_id)) = UPPER(TRIM(pdm.procedure_code))
"""

_FIN_WHERE = """
    WHERE s.study_date BETWEEN :date_from AND :date_to
      AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'OT')
      AND s.study_has_report = true
      AND (CAST(:modality AS TEXT)      IS NULL OR COALESCE(m.modality, s.study_modality) = :modality)
      AND (CAST(:physician_id AS TEXT) IS NULL OR s.reading_physician_id = :physician_id)
      AND (CAST(:patient_class AS TEXT)  IS NULL OR UPPER(s.patient_class) = UPPER(:patient_class))
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
        SELECT COUNT(DISTINCT s.study_db_uid)                                    AS total_studies,
               COALESCE(SUM(pdm.clinical_rvu + pdm.technical_rvu), 0)            AS total_rvu
        {_FIN_JOIN} {_FIN_WHERE}
    """), p).fetchone()
    total_studies = row.total_studies or 0
    total_rvu     = float(row.total_rvu or 0)

    mod_rows = db.session.execute(text(f"""
        SELECT {_MOD_EXPR} AS modality,
               COALESCE(SUM(pdm.clinical_rvu + pdm.technical_rvu), 0) AS rvu
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
               COUNT(DISTINCT s.study_db_uid)         AS study_count,
               COALESCE(SUM(pdm.technical_rvu), 0)    AS total_rvu
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
               COUNT(DISTINCT s.study_db_uid)      AS studies,
               COALESCE(SUM(pdm.clinical_rvu), 0)   AS total_rvu,
               {_MOD_EXPR}                           AS top_modality
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


def widget_rad_modality_matrix(db, filters, config):
    p = _p(filters)
    # Same demo/test-data contamination as widget_physician_perf, fixed the
    # same way (see _DEMO_ACCESSIONS_SQL / _no_demo_name_sql above). Also
    # extends the RIS-report fallback (o.physician_id -> std_resources_ris)
    # into the radiologist-name COALESCE chain and into the "has a report"
    # gate (COALESCE(s.rep_final_timestamp, o.result_datetime)) -- this widget
    # had the identical `s.rep_final_timestamp IS NOT NULL` gate that was the
    # real root cause of physician_perf coming back empty, so it carries the
    # same latent bug even though the operator only reported the contamination
    # symptom for this one. rep_final_signed_by/rep_final_timestamp (PACS) are
    # themselves sparse/unreliable on this install (operator, 2026-07-27) --
    # rep_study_last_composed_by/_ts is the PACS field confirmed reliable here
    # (report_25.py's standard anchor); inserted ahead of them below, RIS
    # fallback (res.common_name / o.physician_id / o.result_datetime) kept
    # last and untouched.
    _RAD_BASE_RW = ("COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
                    "s.signing_physician_last_name)),''), s.rep_study_last_composed_by, "
                    "s.rep_final_signed_by, res.common_name, o.physician_id, 'Unknown')")
    _PAM_RW = ("LEFT JOIN physician_alias_map pam "
               "ON pam.dismissed = false "
               "AND pam.alias = COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
               "s.signing_physician_last_name)),''), s.rep_study_last_composed_by, "
               "s.rep_final_signed_by, res.common_name, o.physician_id)")
    _RAD = f"COALESCE(pam.canonical_name, {_RAD_BASE_RW})"
    _RIS_JOINS_RW = (
        "LEFT JOIN hl7_oru_reports o ON o.accession_number = s.accession_number "
        "LEFT JOIN std_resources_ris res ON res.resource_id = o.physician_id "
        "LEFT JOIN etl_patient_view pv ON pv.patient_db_uid = s.patient_db_uid"
    )
    _demo_excl = _no_demo_name_sql(_RAD)

    by_modality = db.session.execute(text(f"""
        SELECT {_RAD} AS radiologist, {_MOD_EXPR} AS dim, COUNT(DISTINCT s.study_db_uid) AS cnt
        {_BASE_JOIN}
        {_RIS_JOINS_RW}
        {_PAM_RW}
        {_WHERE}
          AND {_RAD} NOT IN ('','Unknown')
          AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NOT NULL
          AND {_DEMO_ACCESSIONS_SQL}
          AND {_DEMO_PATIENT_SQL}
          {_demo_excl}
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), p).fetchall()

    by_aetitle = db.session.execute(text(f"""
        SELECT {_RAD} AS radiologist, COALESCE(s.storing_ae,'Unknown') AS dim, COUNT(DISTINCT s.study_db_uid) AS cnt
        {_BASE_JOIN}
        {_RIS_JOINS_RW}
        {_PAM_RW}
        {_WHERE}
          AND {_RAD} NOT IN ('','Unknown')
          AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp, o.result_datetime) IS NOT NULL
          AND {_DEMO_ACCESSIONS_SQL}
          AND {_DEMO_PATIENT_SQL}
          {_demo_excl}
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), p).fetchall()

    return {
        "by_modality": [{"radiologist": r[0], "dim": r[1], "cnt": r[2]} for r in by_modality],
        "by_aetitle":  [{"radiologist": r[0], "dim": r[1], "cnt": r[2]} for r in by_aetitle],
    }


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
    "rad_modality_matrix":    widget_rad_modality_matrix,
}


def run_widget(db, section_type, filters, config):
    """Run a single widget. Returns data dict or raises."""
    fn = _DISPATCH.get(section_type)
    if not fn:
        raise ValueError(f"Unknown widget type: {section_type}")
    return fn(db, filters, config)
