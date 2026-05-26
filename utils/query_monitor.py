"""
utils/query_monitor.py
──────────────────────
SQLAlchemy event listener that times every DB query within a Flask request
and records one audit row per request on after_request.

Only records endpoints matching /report/*, /viewer/briefing,
/viewer/yesterday, /er, /revenue, /hl7*, /super-report, /report/ai.
Skips: /health, /readiness, /static, /ai/*, /admin, /login, /logout.
"""
import re
import time
import logging
from flask import g, request, has_request_context
from sqlalchemy import event

logger = logging.getLogger("QUERY_MONITOR")

_RECORD_PATTERNS = re.compile(
    r'^/(report/|viewer/(briefing|yesterday|dashboard)|er$|revenue|'
    r'hl7|super-report|report/ai)',
    re.IGNORECASE
)


def _should_record(path: str) -> bool:
    return bool(_RECORD_PATTERNS.match(path))


def init_query_monitor(app, engine):
    """Call once from create_app() after db is initialised."""

    @event.listens_for(engine, "before_cursor_execute")
    def _before(conn, cursor, statement, parameters, context, executemany):
        if not has_request_context():
            return
        if not hasattr(g, '_qm_queries'):
            g._qm_queries = []
            g._qm_req_start = time.monotonic()
        g._qm_queries.append({'_t': time.monotonic()})

    @event.listens_for(engine, "after_cursor_execute")
    def _after(conn, cursor, statement, parameters, context, executemany):
        if not has_request_context() or not hasattr(g, '_qm_queries'):
            return
        # find the last entry without an end time
        for q in reversed(g._qm_queries):
            if 'duration_ms' not in q:
                q['duration_ms'] = round((time.monotonic() - q['_t']) * 1000, 1)
                try:
                    q['rows'] = cursor.rowcount if cursor.rowcount >= 0 else 0
                except Exception:
                    q['rows'] = 0
                del q['_t']
                break

    @app.after_request
    def _save_audit(response):
        try:
            if not has_request_context():
                return response
            if not _should_record(request.path):
                return response
            if not hasattr(g, '_qm_queries') or not g._qm_queries:
                return response

            total_ms = round((time.monotonic() - g._qm_req_start) * 1000, 1)
            queries  = [q for q in g._qm_queries if 'duration_ms' in q]

            # Extract filters from form + args (the WHERE clause drivers)
            filters = {}
            try:
                filters.update({k: v for k, v in request.args.items()})
                if request.method == 'POST':
                    filters.update({k: v for k, v in request.form.items()})
                # Remove CSRF token — not useful
                filters.pop('csrf_token', None)
            except Exception:
                pass

            # Detect cache hit: if no queries fired for a report route it was cached
            cache_hit = len(queries) == 0

            # Parse report_id from path e.g. /report/22 → 22
            report_id = None
            m = re.search(r'/report/(\d+)', request.path)
            if m:
                try:
                    report_id = int(m.group(1))
                except Exception:
                    pass

            from flask_login import current_user
            uid   = current_user.id       if current_user.is_authenticated else None
            uname = current_user.username if current_user.is_authenticated else None

            from db import db
            from sqlalchemy import text
            db.session.execute(text("""
                INSERT INTO query_audit_log
                    (endpoint, report_id, total_ms, query_count, queries,
                     filters, user_id, username, ip_address, http_status, cache_hit)
                VALUES
                    (:ep, :rid, :tms, :qc, :queries::jsonb,
                     :filters::jsonb, :uid, :uname, :ip, :status, :cache)
            """), {
                "ep":      request.path,
                "rid":     report_id,
                "tms":     total_ms,
                "qc":      len(queries),
                "queries": __import__('json').dumps(queries),
                "filters": __import__('json').dumps(filters),
                "uid":     uid,
                "uname":   uname,
                "ip":      request.remote_addr,
                "status":  response.status_code,
                "cache":   cache_hit,
            })
            db.session.commit()
        except Exception as e:
            try:
                db.session.rollback()
            except Exception:
                pass
            logger.debug(f"Query monitor write failed: {e}")
        return response
