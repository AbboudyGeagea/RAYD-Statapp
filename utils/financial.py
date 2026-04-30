from __future__ import annotations
import time
from typing import Optional
import pandas as pd

_cache: dict = {'data': None, 'ts': 0.0}
_TTL = 300  # 5 minutes


def _load() -> dict:
    try:
        from db import db
        from sqlalchemy import text
        rows = db.session.execute(
            text("SELECT entity_type, entity_id, usd_per_rvu FROM financial_config")
        ).fetchall()
        cfg: dict = {'global': 40.0, 'modality': {}, 'procedure': {}}
        for etype, eid, rate in rows:
            val = float(rate)
            if etype == 'global':
                cfg['global'] = val
            elif etype == 'modality' and eid:
                cfg['modality'][eid.upper()] = val
            elif etype == 'procedure' and eid:
                cfg['procedure'][eid.upper()] = val
        return cfg
    except Exception:
        return {'global': 40.0, 'modality': {}, 'procedure': {}}


def _get_config() -> dict:
    now = time.monotonic()
    if _cache['data'] is not None and (now - _cache['ts']) < _TTL:
        return _cache['data']
    _cache['data'] = _load()
    _cache['ts'] = now
    return _cache['data']


def invalidate_cache() -> None:
    _cache['data'] = None
    _cache['ts'] = 0.0


def effective_rate(modality: Optional[str] = None, procedure_code: Optional[str] = None) -> float:
    """Return USD/RVU applying override precedence: procedure > modality > global."""
    cfg = _get_config()
    if procedure_code:
        rate = cfg['procedure'].get(procedure_code.upper().strip())
        if rate is not None:
            return rate
    if modality:
        rate = cfg['modality'].get(modality.upper().strip())
        if rate is not None:
            return rate
    return cfg['global']


def rvu_to_usd(
    rvu_value,
    modality: Optional[str] = None,
    procedure_code: Optional[str] = None,
) -> Optional[float]:
    if rvu_value is None:
        return None
    try:
        return round(float(rvu_value) * effective_rate(modality, procedure_code), 2)
    except (TypeError, ValueError):
        return None


def add_revenue_column(
    df: pd.DataFrame,
    rvu_column: str = 'rvu',
    modality_column: str = 'modality',
    procedure_column: str = 'procedure_code',
) -> pd.DataFrame:
    """Add a revenue_usd column to df by multiplying rvu_column by the effective rate."""
    if rvu_column not in df.columns:
        return df
    mod_col  = modality_column  if modality_column  in df.columns else None
    proc_col = procedure_column if procedure_column in df.columns else None
    try:
        df = df.copy()
        df['revenue_usd'] = df.apply(
            lambda r: rvu_to_usd(
                r[rvu_column],
                modality=r[mod_col]  if mod_col  else None,
                procedure_code=r[proc_col] if proc_col else None,
            ),
            axis=1,
        )
    except Exception:
        df['revenue_usd'] = None
    return df
