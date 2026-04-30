"""
utils/audit.py — fire-and-forget system event logger.

Call log_event() from any route to write one audit row and immediately
commit its own transaction, independent of the caller's DB state.
Never raises — silently swallows errors so route handlers are unaffected.
"""


def log_event(action, category='system', resource_type=None, detail=None):
    try:
        from flask import request
        from flask_login import current_user
        from db import db, UserAuditLog

        actor = None
        try:
            if current_user.is_authenticated:
                actor = current_user.id
        except Exception:
            pass

        ip = None
        try:
            ip = request.remote_addr
        except Exception:
            pass

        db.session.add(UserAuditLog(
            actor_user_id=actor,
            action=action,
            event_category=category,
            resource_type=resource_type,
            detail=detail,
            ip_address=ip,
        ))
        db.session.commit()
    except Exception:
        try:
            from db import db
            db.session.rollback()
        except Exception:
            pass
