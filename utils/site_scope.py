"""
utils/site_scope.py
───────────────────
Per-request site scoping for multi-site RLS (LAUMC).

*** NOT WIRED IN YET — inert until activated. See "Activation" below. ***

How the security model fits together
------------------------------------
  * migration 0050 creates role `rayd_app` and fail-closed RLS policies that filter
    every site-scoped table by:  site_id = ANY(current_setting('rayd.site_scope'))
  * Background jobs (ETL, NLP worker, MLLP listener, APScheduler) keep connecting as
    the table OWNER (etl_user), which bypasses RLS — they must see all sites.
  * Web requests connect as `rayd_app` and MUST set the scope on every request, or
    the policies match zero rows (fail-closed: empty dashboard, never a leak).

Effective scope = (sites the user is granted, via user_sites)
                ∩ (the site the user picked in the header picker)

An admin with no user_sites rows is treated as all-sites.

Activation (next session — needs a live DB to verify):
  1. Configure a second SQLAlchemy engine bound to the `rayd_app` role and route
     web requests to it (background jobs keep using db.engine / etl_user).
  2. Call install(app) from create_app() AFTER login manager is set up.
  3. Verify the leak-test matrix: RH-only user, SJH-only user, both-sites admin.
"""

import logging

logger = logging.getLogger("SITE_SCOPE")

SESSION_KEY = "site_selection"   # flask session key holding the picker value


def granted_site_ids(user):
    """Canonical site ids this user may see. Admin with no explicit grants = all sites."""
    from sqlalchemy import text
    from db import db
    try:
        rows = db.session.execute(
            text("SELECT site_id FROM user_sites WHERE user_id = :uid"),
            {"uid": user.id},
        ).fetchall()
        ids = [r[0] for r in rows]
        if ids:
            return ids
        if getattr(user, "role", None) == "admin":
            from utils.site_resolver import all_site_ids
            return all_site_ids()
        return []
    except Exception as e:
        logger.error(f"granted_site_ids failed for user={getattr(user,'id',None)}: {e}")
        return []          # fail-closed: no grants resolved -> sees nothing


def effective_scope(user, selection=None):
    """
    Resolve the scope to apply this request.

    selection: None/'all' -> every granted site; otherwise a canonical site id.
    The intersection is what makes the picker safe: a user tampering with the
    selection can never widen beyond their grants.
    """
    granted = set(granted_site_ids(user))
    if not granted:
        return []
    if selection in (None, "", "all"):
        return sorted(granted)
    try:
        sel = int(selection)
    except (TypeError, ValueError):
        return sorted(granted)
    return sorted(granted & {sel})     # intersection — the safety property


def apply_scope(conn, site_ids):
    """
    Set the RLS scope on a connection for the current transaction.
    Empty list -> set '' so policies match nothing (explicit fail-closed).
    """
    from sqlalchemy import text
    csv = ",".join(str(i) for i in site_ids)
    conn.execute(text("SELECT set_config('rayd.site_scope', :v, true)"), {"v": csv})


def install(app):
    """
    Register the before_request hook that applies each user's scope.
    NOT called yet — see module docstring "Activation".
    """
    from flask import session as flask_session, request
    from flask_login import current_user

    @app.before_request
    def _apply_site_scope():
        if request.path.startswith("/static/") or not current_user.is_authenticated:
            return
        from db import db
        scope = effective_scope(current_user, flask_session.get(SESSION_KEY))
        try:
            apply_scope(db.session.connection(), scope)
        except Exception as e:
            logger.error(f"site scope apply failed: {e}")
            raise      # fail-closed: never serve a request with an unset scope

    logger.info("site_scope: before_request hook installed")
