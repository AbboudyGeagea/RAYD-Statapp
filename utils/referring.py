"""
utils/referring.py
------------------
One definition of the referring physician's display name, shared by every
report that lists or ranks referring doctors.

Mazloum's PACS stores the full name in referring_physician_first_name for
~78% of studies and leaves the last name empty (measured 2026-10-05), so a
name is first + last, whichever parts are filled. Never filter on the last
name alone: that silently drops most of the volume.

"EXTERNAL DOC" is a placeholder for outside referrers, not a person. It
arrives in two spellings (first name only, and first + last both
"EXTERNAL DOC") and collapses to EXTERNAL_LABEL. Physician rankings leave it
out and report its count separately.
"""

EXTERNAL_LABEL = 'External referrals'


def ref_name_sql(alias='s', empty="''"):
    """SQL expression for the referring physician's name.

    alias -- table alias of etl_didb_studies, or None for unqualified columns.
    empty -- SQL literal returned when no name is recorded ("''" or "'Unknown'").
    """
    p = f"{alias}." if alias else ""
    first = f"NULLIF(TRIM({p}referring_physician_first_name), '')"
    last  = f"NULLIF(TRIM({p}referring_physician_last_name), '')"
    return (
        f"(CASE WHEN UPPER({first}) = 'EXTERNAL DOC' OR UPPER({last}) = 'EXTERNAL DOC'"
        f" THEN '{EXTERNAL_LABEL}'"
        f" ELSE COALESCE(NULLIF(CONCAT_WS(' ', {first}, {last}), ''), {empty}) END)"
    )


def ranked_ref_sql(alias='s'):
    """SQL predicate: the study has a named, real (non-external) referring physician."""
    return f"{ref_name_sql(alias)} NOT IN ('', '{EXTERNAL_LABEL}')"
