"""
utils/site_resolver.py
──────────────────────
Resolves each source system's site vocabulary to RAYD's canonical `sites.id`.

Background: LAUMC is multi-site (RH main, SJH satellite). Each source labels the
site differently, and the `sites` table (migration 0046) is the single translator:

    canonical site | PACS DB (didb_studies.site_id) | RIS DB issuer | RIS org_struct | HL7 PV1 building
    ---------------|-------------------------------|---------------|----------------|-----------------
    RH  (main)     | '0'                           | 'SAP_PROD'    | '3926'         | '1000'
    SJH (satellite)| '1'                           | 'SAP_SJH'     | '5320'         | '2000'

Each ingestion path calls ONLY its own resolver:
    * PACS study ETL          → resolve_pacs(site_id)
    * RIS worklist/orders ETL → resolve_ris_issuer(issuer)  or  resolve_ris_org(org_structure_key)
    * MLLP listener (HL7)     → resolve_hl7_building(pv1_building)

Values that don't match fall back to the default site (the site flagged
is_default in `sites`, e.g. an existing single-site PACS that stamps '0'
everywhere) — or None if no default is configured. Source values may change at
the ~2027 RIS/PACS upgrade; only the `sites` rows change, never this code.

The map is small and rarely changes, so it is cached in-process for 5 minutes.
"""

import time
import logging

logger = logging.getLogger("SITE_RESOLVER")

_CACHE_TTL = 300  # seconds
_cache = None
_cache_loaded_at = 0.0


def _load_map(app=None):
    """Load the sites map into four lookup dicts + the default id. Never raises."""
    from sqlalchemy import text
    try:
        from db import db
        rows = db.session.execute(text("""
            SELECT id, pacs_site_id, ris_issuer, ris_org_struct, hl7_building, is_default
            FROM sites
            WHERE active
        """)).fetchall()
    except Exception as e:
        logger.error(f"site map load failed: {e}")
        return {"pacs": {}, "issuer": {}, "org": {}, "building": {}, "default": None}

    m = {"pacs": {}, "issuer": {}, "org": {}, "building": {}, "default": None}
    for sid, pacs, issuer, org, bldg, is_default in rows:
        if pacs   is not None: m["pacs"][str(pacs).strip()]        = sid
        if issuer is not None: m["issuer"][str(issuer).strip().upper()] = sid
        if org    is not None: m["org"][str(org).strip()]          = sid
        if bldg   is not None: m["building"][str(bldg).strip()]    = sid
        if is_default:         m["default"] = sid
    return m


def _get_map(app=None, force=False):
    global _cache, _cache_loaded_at
    if force or _cache is None or (time.time() - _cache_loaded_at) > _CACHE_TTL:
        _cache = _load_map(app)
        _cache_loaded_at = time.time()
    return _cache


def invalidate_cache():
    """Call after editing the sites table so the next resolve reloads."""
    global _cache
    _cache = None


def _resolve(kind, value):
    """Generic lookup with default fallback. Returns canonical site id or None."""
    if value is None:
        return _get_map()["default"]
    key = str(value).strip()
    if kind == "issuer":
        key = key.upper()
    sid = _get_map()[kind].get(key)
    if sid is not None:
        return sid
    # Unknown value → default site, but log once-ish so genuinely new codes surface.
    default = _get_map()["default"]
    logger.warning(f"site_resolver: unmapped {kind} value {value!r} → default site {default}")
    return default


def resolve_pacs(site_id_value):
    """PACS didb_studies.site_id ('0','1',...) → canonical site id."""
    return _resolve("pacs", site_id_value)


def resolve_ris_issuer(issuer):
    """RIS ISSUER_OF_PLACER_ORDER_NUMBER ('SAP_PROD','SAP_SJH') → canonical site id."""
    return _resolve("issuer", issuer)


def resolve_ris_org(org_structure_key):
    """RIS ORG_STRUCTURE_KEY ('3926','5320') → canonical site id (worklist site source)."""
    return _resolve("org", org_structure_key)


def resolve_hl7_building(building):
    """HL7 PV1 building code ('1000','2000') → canonical site id."""
    return _resolve("building", building)


def default_site():
    """The site used when a source row carries no marker (single-site fallback)."""
    return _get_map()["default"]


def all_site_ids():
    """All active canonical site ids — used to set an all-sites RLS scope for admins."""
    m = _get_map()
    ids = set()
    for kind in ("pacs", "issuer", "org", "building"):
        ids.update(m[kind].values())
    if m["default"] is not None:
        ids.add(m["default"])
    return sorted(ids)
