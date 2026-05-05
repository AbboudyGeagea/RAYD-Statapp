"""
routes/report_cache.py
──────────────────────
Simple in-memory TTL cache for report query results.

Keyed on (report_id, MD5 of form params). Avoids redundant DB hits
when the same report is run twice with identical settings within TTL.

Usage:
    from routes.report_cache import cache_get, cache_put

    def get_data(form_data):
        cached = cache_get(25, form_data)
        if cached:
            return cached
        # ... compute result ...
        cache_put(25, form_data, result)
        return result
"""
import hashlib
import json
import time

_store: dict = {}
_TTL = 300       # 5 minutes
_MAX_SIZE = 200  # max entries before eviction


def _make_key(report_id: int, form_data) -> str:
    try:
        # Use to_dict(flat=False) to capture multi-value keys (e.g. multi-select dropdowns).
        # Falls back to dict() for plain dicts passed directly.
        if hasattr(form_data, 'to_dict'):
            raw = form_data.to_dict(flat=False)
        else:
            raw = {k: [v] for k, v in dict(form_data).items()}
        serialized = json.dumps(sorted(raw.items()), default=str, sort_keys=True)
    except Exception:
        serialized = str(form_data)
    h = hashlib.md5(serialized.encode()).hexdigest()
    return f"r{report_id}:{h}"


def cache_get(report_id: int, form_data):
    """Return cached result tuple or None if missing/expired."""
    key = _make_key(report_id, form_data)
    entry = _store.get(key)
    if entry and (time.time() - entry["ts"]) < _TTL:
        return entry["data"]
    return None


def cache_put(report_id: int, form_data, data) -> None:
    """Store result. Evicts oldest entry if over _MAX_SIZE."""
    key = _make_key(report_id, form_data)
    _store[key] = {"data": data, "ts": time.time()}
    if len(_store) > _MAX_SIZE:
        oldest = min(_store, key=lambda k: _store[k]["ts"])
        del _store[oldest]


def cache_invalidate(report_id: int = None) -> int:
    """Remove entries for a report_id, or all if None. Returns count removed."""
    if report_id is None:
        count = len(_store)
        _store.clear()
        return count
    prefix = f"r{report_id}:"
    keys = [k for k in list(_store) if k.startswith(prefix)]
    for k in keys:
        del _store[k]
    return len(keys)


# ── Filter-options cache ───────────────────────────────────────────────────
# Shared across all report pages. Keyed by a fixed string; TTL = 5 minutes.
# Avoids running SELECT DISTINCT on etl_didb_studies on every page load.

_FILTER_KEY = "__filter_options__"
_FILTER_TTL = 300  # seconds


def get_filter_options(db) -> dict:
    """
    Return {classes, locations, modalities, aetitles, statuses, sex_values}
    from cache, re-querying only when the TTL has expired.
    Each field is fetched independently so one failure never blanks the rest.
    """
    import logging
    from sqlalchemy import text

    log = logging.getLogger(__name__)

    entry = _store.get(_FILTER_KEY)
    if entry and (time.time() - entry["ts"]) < _FILTER_TTL:
        return entry["data"]

    data = {"classes": [], "locations": [], "statuses": [], "aetitles": [], "modalities": [], "sex_values": []}

    _QUERIES = {
        "classes":    "SELECT ARRAY_AGG(DISTINCT patient_class   ORDER BY patient_class)   FROM etl_didb_studies WHERE patient_class   IS NOT NULL",
        "locations":  "SELECT ARRAY_AGG(DISTINCT patient_location ORDER BY patient_location) FROM etl_didb_studies WHERE patient_location IS NOT NULL",
        "statuses":   "SELECT ARRAY_AGG(DISTINCT study_status    ORDER BY study_status)    FROM etl_didb_studies WHERE study_status    IS NOT NULL",
        "aetitles":   "SELECT ARRAY_AGG(DISTINCT storing_ae      ORDER BY storing_ae)      FROM etl_didb_studies WHERE storing_ae      IS NOT NULL",
        "modalities": "SELECT ARRAY_AGG(DISTINCT modality        ORDER BY modality)        FROM aetitle_modality_map WHERE modality IS NOT NULL AND modality != 'SR'",
        "sex_values": "SELECT ARRAY_AGG(DISTINCT sex             ORDER BY sex)             FROM etl_patient_view WHERE sex IS NOT NULL",
    }

    for key, sql in _QUERIES.items():
        try:
            row = db.session.execute(text(sql)).fetchone()
            data[key] = list(row[0]) if row and row[0] else []
        except Exception as exc:
            log.error("filter_options[%s] failed: %s", key, exc)
            db.session.rollback()

    _store[_FILTER_KEY] = {"data": data, "ts": time.time()}
    return data
