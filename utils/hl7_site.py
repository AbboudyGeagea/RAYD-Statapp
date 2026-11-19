"""
utils/hl7_site.py — which hospital's database an HL7 message's rows go to (LAUMC to-do item 13d,
operator 2026-11-19: "yes, before the move").

    HIS order (SAP_P ORM)   its hospital: PV1-3.7 (1000 RH, 2000 SJH = sites.hl7_building), else an
                            SJH location (PV1-3.1) or entering unit (ORC-17) ending in -J; written
                            to that hospital's database (hl7_listener.py)
    ORU report (CARESTREAM) stays on the main database: it carries no hospital (no PV1, no ORC;
                            production sample 19 Nov), and the NLP worker and CRN read the main
                            database only. The nightly site route moves SJH's reports, with their
                            NLP analysis, by accession (ETL_JOBS/etl_site_route.py).
    RIS status events       BRAYN's only, on the main database (the Live board reads them there).
    PACS completions        none arrive at LAUMC (19 Nov: SAP_P, RIS and CARESTREAM only).

A hospital is routed to only once it has its own database AND its one-time move has recorded
itself (settings site_route_moved_<CODE> on the main database): before that every row stays on the
main database, as before, and the move copies them. Anything that lands on the main database
anyway (no hospital in the message) is moved by the nightly site route when it is SJH's.
Fail-safe: any error routes to the main database; the listener never loses a message over this.
"""
import logging
import time

from sqlalchemy import text

logger = logging.getLogger('HL7_SITE')

SJH_SUFFIX = '-J'
_TTL = 60
_cache = {'at': 0.0, 'value': None}


def _routes():
    """({his code: site id}, SJH's site id or None) for the hospitals routed to now."""
    now = time.time()
    if _cache['value'] is not None and now - _cache['at'] < _TTL:
        return _cache['value']
    value = ({}, None)
    try:
        from utils import site_db
        if site_db.has_site_databases():
            from db import db
            default = site_db.default_site_id()
            with db.engine.connect() as conn:                     # always the main database
                sites = conn.execute(text(
                    "SELECT id, code, hl7_building FROM sites WHERE active IS NOT FALSE")).all()
                moved = {k for (k,) in conn.execute(text(
                    "SELECT key FROM settings WHERE key LIKE 'site_route_moved_%'"))}
            served = site_db.database_site_ids()
            by_code, sjh = {}, None
            for sid, code, building in sites:
                if sid == default or sid not in served or f'site_route_moved_{(code or "").upper()}' not in moved:
                    continue
                if building and building.strip():
                    by_code[building.strip()] = sid
                if (code or '').upper() == 'SJH':
                    sjh = sid
            value = (by_code, sjh)
    except Exception:
        logger.exception("HL7 routing table not read: everything stays on the main database")
        value = ({}, None)
    _cache.update(at=now, value=value)
    return value


def reset():
    _cache.update(at=0.0, value=None)


def _field(segments, name, index):
    for s in segments:
        if s.startswith(name + '|'):
            parts = s.split('|')
            return parts[index] if len(parts) > index else ''
    return ''


def his_order_site(segments):
    """The site whose database an HIS order goes to, or None for the main database."""
    by_code, sjh = _routes()
    if not by_code and sjh is None:
        return None
    pv1_3 = _field(segments, 'PV1', 3).split('^')
    facility = pv1_3[6].strip() if len(pv1_3) > 6 else ''
    if facility:
        return by_code.get(facility)                          # RH's code, or unknown: main
    location = (pv1_3[0] if pv1_3 else '').strip().upper()
    unit = _field(segments, 'ORC', 17).split('^')[0].strip().upper()
    if sjh is not None and (location.endswith(SJH_SUFFIX) or unit.endswith(SJH_SUFFIX)):
        return sjh
    return None
