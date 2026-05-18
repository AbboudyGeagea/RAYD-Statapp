import re
import difflib


def get_alias_dict():
    """Return {alias: canonical_name} for all non-dismissed approved mappings."""
    from db import db
    from sqlalchemy import text
    rows = db.session.execute(
        text("SELECT alias, canonical_name FROM physician_alias_map WHERE dismissed = false")
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _normalize_tokens(name: str) -> list[str]:
    """Strip @domain, split on spaces/dots/underscores, return uppercase tokens."""
    name = re.sub(r'@\w+$', '', name)
    tokens = re.split(r'[\s._\-]+', name.strip())
    return [t.upper() for t in tokens if t]


def detect_suggestions():
    """
    Auto-detect likely duplicate physician name pairs from etl_didb_studies.
    Returns list of dicts:
      {variants: [name, ...], confidence: int (0-100),
       report_counts: {name: int}, suggested_canonical: str}

    Logic:
    1. Pull all distinct rep_final_signed_by values with report counts.
    2. Exclude names already in physician_alias_map (approved OR dismissed).
    3. For each pair, compute best token-to-token SequenceMatcher ratio.
    4. If one name has a single-char token (initial like "A"), verify it matches
       the first letter of any token in the other name.
    5. Pairs with similarity >= 0.75 AND initial check passed are grouped.
    6. Suggested canonical = variant with most reports.
    """
    from db import db
    from sqlalchemy import text

    rows = db.session.execute(text("""
        SELECT rep_final_signed_by, COUNT(*) AS cnt
        FROM etl_didb_studies
        WHERE rep_final_signed_by IS NOT NULL
          AND TRIM(rep_final_signed_by) != ''
        GROUP BY rep_final_signed_by
        ORDER BY cnt DESC
    """)).fetchall()

    count_map = {r[0]: int(r[1]) for r in rows}

    existing = {r[0] for r in db.session.execute(
        text("SELECT alias FROM physician_alias_map")
    ).fetchall()}

    names = [n for n in count_map if n not in existing]

    seen = set()
    suggestions = []

    for i, n1 in enumerate(names):
        if n1 in seen:
            continue
        t1 = _normalize_tokens(n1)
        group = [n1]
        seen.add(n1)
        best_conf = 0

        for n2 in names[i + 1:]:
            if n2 in seen:
                continue
            if n1.upper().strip() == n2.upper().strip():
                continue
            t2 = _normalize_tokens(n2)

            # Best pairwise token similarity
            best_sim = 0.0
            for tok1 in t1:
                for tok2 in t2:
                    sim = difflib.SequenceMatcher(None, tok1, tok2).ratio()
                    if sim > best_sim:
                        best_sim = sim

            # Initial cross-check: if either side has a single-char token,
            # it must match the first letter of some token on the other side.
            def _has_initial(tokens):
                return any(len(t) == 1 for t in tokens)

            def _initial_ok(tokens_with_initial, other_tokens):
                for t in tokens_with_initial:
                    if len(t) == 1:
                        if not any(o.startswith(t) for o in other_tokens):
                            return False
                return True

            initial_ok = True
            if _has_initial(t1):
                initial_ok = _initial_ok(t1, t2)
            elif _has_initial(t2):
                initial_ok = _initial_ok(t2, t1)

            if best_sim >= 0.75 and initial_ok:
                group.append(n2)
                seen.add(n2)
                if best_sim > best_conf:
                    best_conf = best_sim

        if len(group) > 1:
            report_counts = {n: count_map[n] for n in group}
            canonical = max(report_counts, key=report_counts.get)
            suggestions.append({
                'variants': group,
                'confidence': round(best_conf * 100),
                'report_counts': report_counts,
                'suggested_canonical': canonical,
            })

    suggestions.sort(key=lambda x: x['confidence'], reverse=True)
    return suggestions
