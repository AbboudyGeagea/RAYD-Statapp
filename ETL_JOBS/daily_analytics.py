#!/usr/bin/env python3
"""
ETL_JOBS/daily_analytics.py
----------------------------
Nightly analytics job — scheduled via Docker cron at 5:15 AM.
Runs after the ETL daily jobs complete (max 5 min).

Computes executive KPIs for three standard periods:
  last_30d  — last 30 days vs prior 30 days
  last_90d  — last 90 days vs prior 90 days
  ytd       — year-to-date vs same period last year

Stores one row per period in analytics_snapshots.
Prunes rows older than 30 days to keep the table lean.

Run manually:  python ETL_JOBS/daily_analytics.py
"""

import os
import sys
import json
import logging
from datetime import date, datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANALYTICS] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── DB connection ─────────────────────────────────────────────────────────────

def _engine():
    url = os.environ.get("DATABASE_URL") or os.environ.get("SQLALCHEMY_DATABASE_URI")
    if not url:
        user   = os.environ.get("POSTGRES_USER", "etl_user")
        pw     = os.environ.get("POSTGRES_PASSWORD", "")
        host   = os.environ.get("POSTGRES_HOST", "db")
        port   = os.environ.get("POSTGRES_PORT", "5432")
        dbname = os.environ.get("POSTGRES_DB", "etl_db")
        url    = f"postgresql://{user}:{pw}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True)


# ── Period definitions ────────────────────────────────────────────────────────

def _periods():
    today = date.today()
    yest  = today - timedelta(days=1)

    def period(n):
        end   = yest
        start = end - timedelta(days=n - 1)
        pe    = start - timedelta(days=1)
        ps    = pe - timedelta(days=n - 1)
        return start, end, ps, pe

    s30, e30, ps30, pe30 = period(30)
    s90, e90, ps90, pe90 = period(90)

    ytd_start  = date(today.year, 1, 1)
    ytd_end    = yest
    prev_start = date(today.year - 1, 1, 1)
    # Same calendar position last year, handling leap-year edge
    try:
        prev_end = date(today.year - 1, ytd_end.month, ytd_end.day)
    except ValueError:
        prev_end = date(today.year - 1, 2, 28)

    return [
        ("last_30d", s30, e30, ps30, pe30),
        ("last_90d", s90, e90, ps90, pe90),
        ("ytd",      ytd_start, ytd_end, prev_start, prev_end),
    ]


# ── KPI collection ────────────────────────────────────────────────────────────

def _collect(conn, start, end):
    p = {"s": str(start), "e": str(end)}

    kpis = conn.execute(text("""
        SELECT
            COUNT(DISTINCT s.study_db_uid)   AS total_studies,
            COUNT(DISTINCT s.patient_db_uid) AS total_patients,
            SUM(s.number_of_study_images)    AS total_images,
            COUNT(*) FILTER (WHERE s.study_has_report = TRUE) AS studies_with_report,
            COUNT(DISTINCT s.study_date)     AS active_days
        FROM etl_didb_studies s
        WHERE s.study_date BETWEEN :s AND :e
    """), p).mappings().fetchone()

    tat = conn.execute(text("""
        SELECT ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (rep_final_timestamp - insert_time)) / 60.0
            )::numeric, 1
        ) AS median_tat_min
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
          AND insert_time IS NOT NULL
          AND rep_final_timestamp IS NOT NULL
    """), p).fetchone()

    orders = conn.execute(text("""
        SELECT COUNT(*)                              AS total,
               COUNT(*) FILTER (WHERE has_study)    AS fulfilled
        FROM etl_orders
        WHERE scheduled_datetime::date BETWEEN :s AND :e
    """), p).mappings().fetchone()

    storage = conn.execute(text("""
        SELECT COALESCE(SUM(total_gb), 0) AS total_gb
        FROM summary_storage_daily
        WHERE study_date BETWEEN :s AND :e
    """), p).fetchone()

    physicians = conn.execute(text("""
        SELECT TRIM(CONCAT(referring_physician_first_name, ' ',
                           referring_physician_last_name)) AS name,
               COUNT(*) AS cnt
        FROM etl_didb_studies
        WHERE study_date BETWEEN :s AND :e
          AND referring_physician_last_name IS NOT NULL
          AND referring_physician_last_name != ''
        GROUP BY 1 ORDER BY cnt DESC LIMIT 5
    """), p).fetchall()

    modalities = conn.execute(text("""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               COUNT(*) AS cnt
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON m.aetitle = s.storing_ae
        WHERE s.study_date BETWEEN :s AND :e
        GROUP BY 1 ORDER BY cnt DESC LIMIT 5
    """), p).fetchall()

    total      = int(kpis["total_studies"] or 0)
    active_days = max(int(kpis["active_days"] or 1), 1)

    return {
        "total_studies":       total,
        "total_patients":      int(kpis["total_patients"] or 0),
        "total_images":        int(kpis["total_images"] or 0),
        "studies_with_report": int(kpis["studies_with_report"] or 0),
        "avg_per_day":         round(total / active_days, 1),
        "median_tat_min":      float(tat[0]) if tat and tat[0] else None,
        "orders_total":        int(orders["total"] or 0),
        "orders_fulfilled":    int(orders["fulfilled"] or 0),
        "storage_gb":          float(storage[0] or 0),
        "physicians":          [{"name": r[0], "cnt": int(r[1])} for r in physicians],
        "modalities":          [{"name": r[0], "cnt": int(r[1])} for r in modalities],
    }


# ── Text generation ───────────────────────────────────────────────────────────

def _pct(cur, prev):
    try:
        c, p = float(cur or 0), float(prev or 0)
        return round((c - p) / p * 100, 1) if p else None
    except Exception:
        return None

def _chg(p):
    if p is None: return ""
    sign = "+" if p >= 0 else ""
    arrow = " ↑↑" if p > 20 else " ↑" if p > 0 else " ↓↓" if p < -20 else " ↓"
    return f", {sign}{p}%{arrow} vs prior"

def _fmt(n):
    try:    return f"{int(n):,}"
    except: return "—"


def _briefing(label, cur, prev, start, end):
    label_map = {"last_30d": "Last 30 Days", "last_90d": "Last 90 Days", "ytd": "Year to Date"}
    today_str = datetime.now().strftime("%d %b %Y")
    lines = [f"DAILY BRIEFING — {today_str}  |  {label_map[label]}", ""]

    # Volume
    total   = cur["total_studies"]
    s_chg   = _pct(total, prev["total_studies"])
    avg     = cur["avg_per_day"]
    lines.append(f"Volume: {_fmt(total)} studies{_chg(s_chg)}. Throughput {avg:.1f}/day.")

    # TAT
    tat = cur["median_tat_min"]
    if tat:
        tat_h = tat / 60
        if tat > 1440:
            lines.append(f"TAT: {tat_h:.1f}h median — exceeds 24h target. Immediate action needed.")
        elif tat > 480:
            lines.append(f"TAT: {tat_h:.1f}h median — above 8h inpatient guideline.")
        else:
            lines.append(f"TAT: {tat:.0f} min median — within benchmark.")
    else:
        lines.append("TAT: Reporting timestamps not yet populated.")

    # Orders
    ot = cur["orders_total"]
    of = cur["orders_fulfilled"]
    if ot > 0:
        ff   = round(of / ot * 100, 1)
        note = "Strong." if ff >= 90 else "Below target." if ff >= 80 else "Critical — investigate."
        lines.append(f"Orders: {ff}% fulfillment ({_fmt(of)} of {_fmt(ot)}). {note}")

    # Storage
    gb     = cur["storage_gb"]
    st_chg = _pct(gb, prev["storage_gb"])
    tb     = gb / 1024
    lines.append(f"Storage: {tb:.2f} TB consumed{_chg(st_chg)}.")

    # Reporting coverage
    rep = cur["studies_with_report"]
    if total > 0:
        cov  = round(rep / total * 100, 1) if rep else 0
        note = "Strong." if cov >= 90 else "Backlog building." if cov >= 70 else "Significant backlog — action needed."
        lines.append(f"Coverage: {cov}% of studies have signed reports. {note}")

    lines.append("")

    # Top physicians
    if cur["physicians"]:
        phys = " · ".join(f"{p['name']} {_fmt(p['cnt'])}" for p in cur["physicians"][:4])
        lines.append(f"Top Referring: {phys}")

    # Modality mix
    if cur["modalities"] and total > 0:
        mods = " · ".join(
            f"{m['name']} {round(m['cnt'] / total * 100)}%"
            for m in cur["modalities"][:4]
        )
        lines.append(f"Modalities: {mods}")

    # Alerts
    alerts = []
    if tat and tat > 480:
        alerts.append(f"TAT {tat/60:.1f}h exceeds guideline — reporting backlog suspected")
    if total > 0:
        cov = round(rep / total * 100, 1) if rep else 0
        if cov < 90:
            alerts.append(f"Reporting coverage {cov}% is below the 90% target")
    if ot > 0:
        ff = round(of / ot * 100, 1)
        if ff < 85:
            alerts.append(f"Order fulfillment {ff}% is below the 85% threshold")
    if st_chg is not None and st_chg > 30:
        alerts.append(f"Storage grew {st_chg:+.1f}% — review archival policy")
    if s_chg is not None and s_chg < -20:
        alerts.append(f"Study volume fell {s_chg:.1f}% — investigate scheduling gaps")

    if alerts:
        lines.append("")
        lines.append("ALERTS")
        for a in alerts:
            lines.append(f"— {a}")

    return "\n".join(lines)


# ── Ensure table exists ───────────────────────────────────────────────────────

_ENSURE_TABLE = """
CREATE TABLE IF NOT EXISTS analytics_snapshots (
    id            SERIAL PRIMARY KEY,
    period_label  VARCHAR(20)  NOT NULL,
    period_start  DATE         NOT NULL,
    period_end    DATE         NOT NULL,
    computed_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    data_json     JSONB,
    narrative     TEXT,
    status        VARCHAR(20)  NOT NULL DEFAULT 'ok'
);
CREATE INDEX IF NOT EXISTS idx_snapshots_label_time
    ON analytics_snapshots (period_label, computed_at DESC);
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def run(engine=None):
    engine = engine or _engine()
    with engine.connect() as conn:
        conn.execute(text(_ENSURE_TABLE))
        conn.commit()

        for label, start, end, prior_start, prior_end in _periods():
            logger.info(f"{label}: {start} → {end}  vs  {prior_start} → {prior_end}")
            try:
                cur  = _collect(conn, start, end)
                prev = _collect(conn, prior_start, prior_end)
                narrative = _briefing(label, cur, prev, start, end)

                conn.execute(text("""
                    INSERT INTO analytics_snapshots
                        (period_label, period_start, period_end, data_json, narrative, status)
                    VALUES (:lbl, :ps, :pe, :dj, :narr, 'ok')
                """), {
                    "lbl":  label,
                    "ps":   str(start),
                    "pe":   str(end),
                    "dj":   json.dumps({**cur, "prior": prev}),
                    "narr": narrative,
                })
                conn.commit()
                logger.info(f"  ✓ stored")

            except Exception as exc:
                logger.error(f"  ✗ failed: {exc}", exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Prune snapshots older than 30 days
        conn.execute(text("""
            DELETE FROM analytics_snapshots
            WHERE computed_at < NOW() - INTERVAL '30 days'
        """))
        conn.commit()
        logger.info("Pruned snapshots older than 30 days.")


if __name__ == "__main__":
    logger.info("=== Daily Analytics Job Starting ===")
    run()
    logger.info("=== Done ===")
