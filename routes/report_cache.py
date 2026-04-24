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
import logging
import time

logger = logging.getLogger("report_cache")

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
    """
    from sqlalchemy import text

    entry = _store.get(_FILTER_KEY)
    if entry and (time.time() - entry["ts"]) < _FILTER_TTL:
        return entry["data"]

    def _distinct(sql):
        try:
            rows = db.session.execute(text(sql)).fetchall()
            return sorted([r[0] for r in rows if r[0] is not None and str(r[0]).strip() != ''])
        except Exception as e:
            logger.error("filter_options query failed [%.80s]: %s", sql, e, exc_info=True)
            db.session.rollback()
            return []

    data = {
        "classes":    _distinct("SELECT DISTINCT patient_class   FROM etl_didb_studies   WHERE patient_class    IS NOT NULL"),
        "locations":  _distinct("SELECT DISTINCT patient_location FROM etl_didb_studies  WHERE patient_location IS NOT NULL"),
        "statuses":   _distinct("SELECT DISTINCT study_status    FROM etl_didb_studies   WHERE study_status     IS NOT NULL"),
        "aetitles":   _distinct("SELECT DISTINCT storing_ae      FROM etl_didb_studies   WHERE storing_ae       IS NOT NULL"),
        "modalities": _distinct("SELECT DISTINCT modality        FROM aetitle_modality_map WHERE modality       IS NOT NULL"),
        "sex_values": _distinct("SELECT DISTINCT sex             FROM etl_patient_view   WHERE sex              IS NOT NULL"),
    }

    # Only cache when at least one key has data — never cache a complete failure
    if any(data.values()):
        _store[_FILTER_KEY] = {"data": data, "ts": time.time()}
    return data
