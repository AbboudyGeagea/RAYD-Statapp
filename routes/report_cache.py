"""
routes/report_cache.py
──────────────────────
Thread-safe in-memory TTL cache for report query results.

Keyed on (report_id, MD5 of form params). Avoids redundant DB hits
when the same report is run twice with identical settings within TTL.

A background daemon thread runs every 5 minutes to evict entries whose
TTL has expired (lazy eviction on read also still applies). This prevents
unbounded memory growth in long-running Gunicorn workers.

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
import threading

logger = logging.getLogger(__name__)

_store: dict = {}
_lock = threading.Lock()
_TTL = 300       # 5 minutes
_MAX_SIZE = 200  # max entries before LRU eviction


def _make_key(report_id: int, form_data) -> str:
    try:
        # Use to_dict(flat=False) to capture multi-value keys (e.g. multi-select dropdowns).
        # Falls back to dict() for plain dicts passed directly.
        if hasattr(form_data, 'to_dict'):
            raw = form_data.to_dict(flat=False)
        else:
            raw = {k: [v] for k, v in dict(form_data).items()}
        raw_sorted = {k: sorted(v) if isinstance(v, list) else v for k, v in raw.items()}
        serialized = json.dumps(sorted(raw_sorted.items()), default=str, sort_keys=True)
    except Exception:
        serialized = str(form_data)
    h = hashlib.md5(serialized.encode()).hexdigest()
    return f"r{report_id}:{h}"


def _evict_expired() -> int:
    """Remove all entries past their TTL. Returns count removed. Thread-safe."""
    now = time.time()
    with _lock:
        expired = [k for k, v in _store.items() if now - v["ts"] >= _TTL]
        for k in expired:
            del _store[k]
    return len(expired)


def cache_get(report_id: int, form_data):
    """Return cached result tuple or None if missing/expired."""
    key = _make_key(report_id, form_data)
    with _lock:
        entry = _store.get(key)
    if entry and (time.time() - entry["ts"]) < _TTL:
        return entry["data"]
    return None


def cache_put(report_id: int, form_data, data) -> None:
    """Store result. Evicts oldest entry if over _MAX_SIZE."""
    key = _make_key(report_id, form_data)
    now = time.time()
    with _lock:
        _store[key] = {"data": data, "ts": now}
        if len(_store) > _MAX_SIZE:
            oldest = min(_store, key=lambda k: _store[k]["ts"])
            del _store[oldest]


def cache_invalidate(report_id: int = None) -> int:
    """Remove entries for a report_id, or all if None. Returns count removed."""
    with _lock:
        if report_id is None:
            count = len(_store)
            _store.clear()
            return count
        prefix = f"r{report_id}:"
        keys = [k for k in list(_store) if k.startswith(prefix)]
        for k in keys:
            del _store[k]
        return len(keys)


# ── Background eviction thread ─────────────────────────────────────────────
# Runs every 5 minutes to remove expired entries. Daemon so it never blocks
# interpreter shutdown. This prevents unbounded cache growth when entries
# expire but are never re-requested (i.e. no lazy-eviction opportunity).

def _eviction_loop(interval: int = 300) -> None:
    while True:
        time.sleep(interval)
        try:
            removed = _evict_expired()
            if removed:
                logger.debug("report_cache: evicted %d expired entries", removed)
        except Exception:
            logger.exception("report_cache: eviction loop error")


_eviction_thread = threading.Thread(
    target=_eviction_loop,
    kwargs={"interval": _TTL},
    name="report-cache-eviction",
    daemon=True,
)
_eviction_thread.start()


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
    from sqlalchemy import text

    with _lock:
        entry = _store.get(_FILTER_KEY)

    if entry and (time.time() - entry["ts"]) < _FILTER_TTL:
        return entry["data"]

    data = {"classes": [], "locations": [], "statuses": [], "aetitles": [], "modalities": [], "sex_values": []}

    # NOTE: values are TRIM'd (and deduped post-TRIM) so what's offered in the
    # dropdown exactly matches what report filters search for. Report filter
    # WHERE clauses (e.g. routes/report_22.py:get_where_params) compare with
    # UPPER(TRIM(column)) = UPPER(TRIM(:param)) specifically because ETL-sourced
    # text columns (Oracle CHAR-padding, free-typed AE titles, etc.) carry
    # incidental leading/trailing whitespace. Without TRIM here, the dropdown
    # could offer a whitespace-variant value that matches zero (or the wrong
    # subset of) rows even though the filter itself is case-insensitive.
    _QUERIES = {
        "classes":    "SELECT ARRAY_AGG(DISTINCT TRIM(patient_class)   ORDER BY TRIM(patient_class))   FROM etl_didb_studies WHERE patient_class   IS NOT NULL AND TRIM(patient_class)   != ''",
        "locations":  "SELECT ARRAY_AGG(DISTINCT TRIM(patient_location) ORDER BY TRIM(patient_location)) FROM etl_didb_studies WHERE patient_location IS NOT NULL AND TRIM(patient_location) != ''",
        "statuses":   "SELECT ARRAY_AGG(DISTINCT TRIM(study_status)    ORDER BY TRIM(study_status))    FROM etl_didb_studies WHERE study_status    IS NOT NULL AND TRIM(study_status)    != ''",
        "aetitles":   "SELECT ARRAY_AGG(DISTINCT TRIM(storing_ae)      ORDER BY TRIM(storing_ae))      FROM etl_didb_studies WHERE storing_ae      IS NOT NULL AND TRIM(storing_ae)      != ''",
        "modalities": "SELECT ARRAY_AGG(DISTINCT TRIM(modality)        ORDER BY TRIM(modality))        FROM aetitle_modality_map WHERE modality IS NOT NULL AND modality != 'SR'",
        "sex_values": "SELECT ARRAY_AGG(DISTINCT TRIM(sex)             ORDER BY TRIM(sex))             FROM etl_patient_view WHERE sex IS NOT NULL AND TRIM(sex) != ''",
    }

    for key, sql in _QUERIES.items():
        try:
            row = db.session.execute(text(sql)).fetchone()
            data[key] = list(row[0]) if row and row[0] else []
        except Exception as exc:
            logger.error("filter_options[%s] failed: %s", key, exc)
            db.session.rollback()

    with _lock:
        _store[_FILTER_KEY] = {"data": data, "ts": time.time()}
    return data
