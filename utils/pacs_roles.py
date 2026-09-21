"""
utils/pacs_roles.py
-------------------
One shared definition of "what role is this signing login" -- 'radiologists' or
'residents' -- for report 36's Resident vs. Radiologist TAT split and report 25's
RES/RAD badges on the Radiologist Workload Matrix. Both used to carry their own
copy of the same CTE (routes/report_36.py, routes/report_25.py); they now build it
from here so the precedence rules can't drift apart.

Sources, in precedence order:

1. pacs_user_role_override (migration 0123) -- operator-entered correction. PACS has
   residents sitting in its 'radiologists' security group because that group is what
   grants reading permission, and nothing in the group data distinguishes them; this
   table is the only thing that can. Wins outright.
2. std_pacs_user_groups (migration 0087) -- real PACS reading-permission group
   membership. NOT RIS's resource_role_key, which was over-granted to every user as a
   workaround for an installation-time RIS permissions bug (see
   ETL_JOBS/etl_ris_resources.py) and has already misclassified a real radiologist as
   Resident -- precisely the direction that would make this worse.

Within source 2, 'residents' beats 'radiologists' when a login somehow holds both.
Measured on LAUMC 2026-09-21, nobody currently does (the only multi-row logins are the
same role across two domains, e.g. radiologists@ad + radiologists@SJ), but the DISTINCT
ON makes the outcome deterministic rather than planner-dependent, and guarantees ONE
row per login -- without that, a future dual membership would silently duplicate every
one of that person's signed events into both panels and double-count the totals.

Group names are matched case/whitespace-insensitively. The site suffix lives in
group_domain, not group_name (verified on LAUMC), so there is deliberately no '@'
stripping on the group side -- only on the login side, where the signer column really
does carry 'first.last@domain'.

The emitted CTE yields (login_id, role): login_id UPPER with no domain suffix, role
exactly 'radiologists' or 'residents' -- the literals routes and templates compare
against (see report_36.html's ROLE_BADGE).
"""

_ROLE_LOOKUP_BODY = """
    SELECT DISTINCT ON (login_id) login_id, role
    FROM (
        SELECT UPPER(BTRIM(o.login_id))   AS login_id,
               LOWER(BTRIM(o.role))       AS role,
               1                          AS src_rank
        FROM pacs_user_role_override o
        WHERE o.login_id IS NOT NULL

        UNION ALL

        SELECT UPPER(BTRIM(g.login_id))   AS login_id,
               LOWER(BTRIM(g.group_name)) AS role,
               2                          AS src_rank
        FROM std_pacs_user_groups g
        WHERE g.login_id IS NOT NULL
          AND LOWER(BTRIM(g.group_name)) IN ('radiologists', 'residents')
    ) src
    ORDER BY login_id,
             src_rank,
             CASE role WHEN 'residents' THEN 0 ELSE 1 END
"""


def role_lookup_cte(name="role_lookup"):
    """The named CTE resolving a PACS login to its role, one row per login.

    Returned as a bare `name AS ( ... )` fragment for the caller to drop into its own
    WITH list. Contains no `{}`, so it is safe to inject into a str.format() template.
    """
    return f"{name} AS ({_ROLE_LOOKUP_BODY}    )"


def role_precedence(existing, incoming):
    """Pick the winner when two rows collide on the same key in Python.

    Report 25 keys its role map by the alias-canonicalized *display* name, so two
    distinct logins with different roles can land on one key -- `physician_alias_map`
    collapsing a migrated name variant onto its canonical form. Mirrors the SQL's
    residents-beats-radiologists rule instead of letting last-row-wins decide it.
    """
    if not incoming:
        return existing
    if existing == "residents":
        return existing
    return incoming
