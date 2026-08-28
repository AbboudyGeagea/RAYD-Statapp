"""
utils/radiologist_resolve.py
-----------------------------
Shared SQL-string helpers for resolving a raw signing-radiologist expression
(a PACS/RIS composed-by column, or a hand-built fallback chain) through
`physician_alias_map` to a curated display name.

Every call site keeps its OWN base expression (the field-priority chain is
tuned per report, sometimes as the result of a specific bug fix -- e.g.
report_25.py's rad_volume_matrix deliberately drops the PACS signing_physician_*
fields that super_report.py's chain still uses first). Only the join/COALESCE
wrapping around that expression is identical everywhere, so that's all this
module shares -- it does not dictate which raw columns to prefer.
"""


def rad_alias_join_sql(base_expr, join_alias="pam"):
    """LEFT JOIN physician_alias_map, matched against the caller's base_expr."""
    return (
        f"LEFT JOIN physician_alias_map {join_alias} "
        f"ON {join_alias}.dismissed = false AND {join_alias}.alias = {base_expr}"
    )


def rad_display_sql(base_expr, join_alias="pam"):
    """Prefer the curated canonical name, else fall back to the raw base_expr."""
    return f"COALESCE({join_alias}.canonical_name, {base_expr})"


def rad_ok_sql(base_expr, join_alias="pam", ts_expr=None):
    """Predicate excluding rows with no name / an empty or 'Unknown' resolved name.

    ts_expr, if given, is ANDed in as an additional "must have a completion
    timestamp" requirement (matches super_report.py's _RAD_OK behavior).
    """
    display = rad_display_sql(base_expr, join_alias)
    clause = f"{base_expr} IS NOT NULL AND {display} NOT IN ('', 'Unknown')"
    if ts_expr:
        clause += f" AND {ts_expr} IS NOT NULL"
    return clause
