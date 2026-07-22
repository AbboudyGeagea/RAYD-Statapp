"""
utils/rate_limit.py
───────────────────
Flask-Limiter instance, shared across blueprints.

Defined here (not in a blueprint) so route modules can import the decorator without
a circular import back to the app factory. Wired up in app.py: `limiter.init_app(app)`.

Storage — why "memory://" is correct here:
    Gunicorn runs with `--workers 1` (see scripts/entrypoint.sh — a single worker is
    also what keeps APScheduler from double-firing). With one process there is exactly
    one counter, so the in-memory backend enforces the limit accurately and needs no
    Redis. If the worker count is ever raised, EACH worker would keep its own counter
    and the effective limit becomes 5 x N per minute — at that point switch
    storage_uri to a shared backend (redis://...) or limits become advisory.
    Counters also reset on restart, which is acceptable for brute-force protection.

Client IP — why get_remote_address is correct here:
    nginx sets X-Forwarded-For (nginx/conf.d/default.conf) and app.py wraps the WSGI
    app in ProxyFix(x_for=1), so request.remote_addr is the real client IP rather than
    the nginx container's address. Without that chain every user would share a single
    bucket and 5 attempts/min would lock out the whole site.
"""

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# No app here — bound later via init_app() in the factory.
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri="memory://",   # explicit: silences the "no storage configured" warning
    # No global default_limits: only the routes we explicitly decorate are limited,
    # so normal application traffic is untouched.
)
