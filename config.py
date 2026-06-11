"""
config.py
─────────────────────────────────────────────────────────────────
RAYD Feature Configuration
All settings are driven by environment variables so each deployment
can be customised without touching code.

Usage in app.py:
    from config import config
    app.config.from_object(config)

Usage in Python:
    from flask import current_app
    if current_app.config["LIVE_FEED_ENABLED"]:
        ...
"""

import os


def _bool(key: str, default: bool = False) -> bool:
    """Read an env var as boolean. Accepts true/1/yes (case-insensitive)."""
    val = os.environ.get(key, "").strip().lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    return default


class Config:
    # ── Core ──────────────────────────────────────────────────
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production")
    TZ         = os.environ.get("TZ", "Asia/Beirut")

    # ── Database ──────────────────────────────────────────────
    POSTGRES_USER     = os.environ.get("POSTGRES_USER",     "etl_user")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
    POSTGRES_HOST     = os.environ.get("POSTGRES_HOST",     "db")
    POSTGRES_PORT     = os.environ.get("POSTGRES_PORT",     "5432")
    POSTGRES_DB       = os.environ.get("POSTGRES_DB",       "etl_db")

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # ── HL7 / Patient Portal ──────────────────────────────────
    HL7_ENABLED            = _bool("HL7_ENABLED",            default=True)
    PATIENT_PORTAL_ENABLED = _bool("PATIENT_PORTAL_ENABLED", default=True)

    # ── Live AE Feed ──────────────────────────────────────────
    LIVE_FEED_ENABLED = _bool("LIVE_FEED_ENABLED", default=True)



# Single instance imported everywhere
config = Config()
