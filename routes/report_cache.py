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
