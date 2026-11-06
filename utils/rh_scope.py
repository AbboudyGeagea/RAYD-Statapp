"""
utils/rh_scope.py — RH only for everything people read, with nothing deleted (LAUMC).

Operator rule, 2026-11-05: SJH appears in nothing, and SJH rows stay in the database.

Migration 0145 holds a filtered view per table in schema rh, under the table's own name. This
hook starts every transaction of a web request with

    SET LOCAL search_path TO rh, public

so every unqualified table name in every query finds the view, whichever route wrote it. LOCAL
ends with the transaction, so a pooled connection never carries it into other work.

Who sees what:
    web requests             the views
    rh_scope() blocks        the views: background jobs that produce something people read
    everything else          the real tables: ETL, HL7 listener, NLP worker, CRN dispatch, BRAYN

Exempt blueprints see the real tables too. CRN's admin screens are there because a notification
already sent has to stay workable (retry, phone acknowledgement) until the operator decides
whether CRN stops for SJH.

To turn this off: RAYD_RH_ONLY=0 and a restart. No data has to change.

In code that runs during a request, create tables as public.x: a table created with no schema
lands in the first schema on the path, which would be rh.
"""
import contextlib
import logging
import os
from contextvars import ContextVar

from flask import has_request_context, request
from sqlalchemy import event, text

logger = logging.getLogger("RH_SCOPE")

EXEMPT_BLUEPRINTS = {'crn_admin'}

_forced = ContextVar('rh_scope_forced', default=False)


@contextlib.contextmanager
def rh_scope():
    """Run a background job through the views, because people read what it produces."""
    token = _forced.set(True)
    try:
        yield
    finally:
        _forced.reset(token)


def _applies():
    if _forced.get():
        return True
    return has_request_context() and request.blueprint not in EXEMPT_BLUEPRINTS


def enabled():
    return os.getenv('RAYD_RH_ONLY', '1').strip().lower() not in ('0', 'false', 'no', 'off')


def install(engine):
    """Attach the hook to an engine. Returns whether it was attached."""
    if not enabled():
        logger.warning("RAYD_RH_ONLY is off: web requests read every site")
        return False

    # search_path skips a schema that does not exist, without an error, so a failed migration
    # would otherwise show every site with nothing in the log.
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('rh.etl_didb_studies') IS NOT NULL")).scalar():
            logger.error("Schema rh is missing (migration 0145): web requests read every site")

    @event.listens_for(engine, 'begin')
    def _apply(conn):
        if _applies():
            conn.exec_driver_sql("SET LOCAL search_path TO rh, public")

    logger.info("RH only: web requests read schema rh")
    return True
