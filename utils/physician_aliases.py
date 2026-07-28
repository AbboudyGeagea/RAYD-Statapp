def get_alias_dict():
    """Return {alias: canonical_name} for all non-dismissed approved mappings."""
    from db import db
    from sqlalchemy import text
    rows = db.session.execute(
        text("SELECT alias, canonical_name FROM physician_alias_map WHERE dismissed = false")
    ).fetchall()
    return {r[0]: r[1] for r in rows}
