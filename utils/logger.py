"""
utils/logger.py
───────────────
Centralized structured logging for RAYD-Statapp.

Log files:  /opt/rayd/logs/<sidebar-folder>/<module>.log
Rotation:   every 7 days (1 backup kept, then purged)
Override:   set RAYD_LOG_DIR env var to change the root path

Call setup_logging() once at app startup (app.py does this).
All existing logging.getLogger(__name__) calls need no changes —
they propagate to root and are routed automatically.

Sidebar folder → module prefix mapping
  dashboard      routes.viewer_controller, routes.er_dashboard
  reports        routes.report_22 … report_29, routes.super_report
  etl            ETL_JOBS.*
  admin          routes.admin, routes.user_management, …
  db_manager     routes.db_manager
  mapping        routes.mapping_controller
  live_feed      routes.live_feed
  hl7            routes.hl7_orders_route
  portal         routes.portal
  oru            routes.oru_analytics, nlp_worker
  referring_intel routes.referring_intel
  ai             routes.bitnet_service, routes.ai_teaching
  financial      routes.financial_dashboard
  auth           auth.*
  app            everything else (db, report_cache, …)
"""
import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOG_ROOT = os.environ.get("RAYD_LOG_DIR", "/opt/rayd/logs")

_MODULE_FOLDER_MAP: dict = {
    # Dashboard
    "routes.viewer_controller":        "dashboard",
    "routes.er_dashboard":             "dashboard",
    # Reports
    "routes.report_22":                "reports",
    "routes.report_23":                "reports",
    "routes.report_25":                "reports",
    "routes.report_27":                "reports",
    "routes.report_29":                "reports",
    "routes.super_report":             "reports",
    # ORU / NLP
    "routes.oru_analytics":            "oru",
    "nlp_worker":                      "oru",
    # HL7
    "routes.hl7_orders_route":         "hl7",
    "hl7_listener":                    "hl7",
    # AI
    "routes.bitnet_service":           "ai",
    "routes.ai_teaching":              "ai",
    # Financial
    "routes.financial_dashboard":      "financial",
    # Mapping / config
    "routes.mapping_controller":       "mapping",
    # Admin / user management
    "routes.admin":                    "admin",
    "routes.user_management":          "admin",
    "routes.activity_log":             "admin",
    "routes.scheduling":               "admin",
    "routes.hl7_forward":              "admin",
    # DB Manager
    "routes.db_manager":               "db_manager",
    # Live feed
    "routes.live_feed":                "live_feed",
    # Patient portal
    "routes.portal":                   "portal",
    # Referring intel
    "routes.referring_intel":          "referring_intel",
    # ETL (prefix match catches all sub-modules)
    "ETL_JOBS":                        "etl",
    # Auth
    "auth":                            "auth",
    # Core app (catch-all)
    "routes.report_cache":             "app",
    "db":                              "app",
    "app":                             "app",
    "APP":                             "app",
    "config":                          "app",
    "utils":                           "app",
}

_FORMAT   = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_initialized = False


def _resolve_folder(name: str) -> str:
    """Map a logger name (usually __name__) to a sidebar-tab folder."""
    if name in _MODULE_FOLDER_MAP:
        return _MODULE_FOLDER_MAP[name]
    best_len, folder = 0, "app"
    for prefix, f in _MODULE_FOLDER_MAP.items():
        if (name == prefix or name.startswith(prefix + ".")) and len(prefix) > best_len:
            best_len, folder = len(prefix), f
    return folder


class _RoutingFileHandler(logging.Handler):
    """
    Root-level handler that lazily opens per-module rotating log files.

    Every log record that propagates to root is routed to:
        LOG_ROOT/<folder>/<last-dotted-component-of-logger-name>.log

    Files rotate every 7 days; 1 old copy is kept before purging.
    If the log directory is not writable (e.g. dev environment without
    the Docker volume mount), the handler silently discards the record.
    """

    def __init__(self) -> None:
        super().__init__()
        self._cache: dict = {}

    def _open(self, folder: str, stem: str):
        key = f"{folder}/{stem}"
        if key in self._cache:
            return self._cache[key]
        log_path = os.path.join(LOG_ROOT, folder, f"{stem}.log")
        handler = None
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            handler = TimedRotatingFileHandler(
                log_path,
                when="D",
                interval=7,
                backupCount=1,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
        except OSError:
            pass
        self._cache[key] = handler
        return handler

    def emit(self, record: logging.LogRecord) -> None:
        folder = _resolve_folder(record.name)
        stem   = record.name.split(".")[-1]
        h = self._open(folder, stem)
        if h:
            try:
                h.emit(record)
            except Exception:
                self.handleError(record)


def setup_logging(level: int = logging.INFO) -> None:
    """
    Install console + file-routing handlers on the root logger.
    Safe to call multiple times — no-op after the first call.
    """
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter(_FORMAT, datefmt=_DATE_FMT)

    # Console handler — Docker / systemd / gunicorn capture stdout
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    # File routing handler
    root.addHandler(_RoutingFileHandler())


def get_logger(name: str) -> logging.Logger:
    """
    Convenience wrapper around logging.getLogger().
    Ensures setup_logging() has run before returning the logger.
    """
    if not _initialized:
        setup_logging()
    return logging.getLogger(name)
