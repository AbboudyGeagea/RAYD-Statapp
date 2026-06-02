def _pct(cur, prev):
    try:
        c, p = float(cur or 0), float(prev or 0)
        return round((c - p) / p * 100, 1) if p else None
    except Exception:
        return None


def _fmt(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"
