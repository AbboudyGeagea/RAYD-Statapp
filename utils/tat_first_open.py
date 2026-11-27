"""
utils/tat_first_open.py
-----------------------
Radiologist TAT at CHN: from the FIRST OPEN to the FINAL report (hospital request, 2026-11-26).
Replaces "final − midnight of the study date", which counted the hours before the scan.

FIRST OPEN is LAUMC's urgent emergent definition: the first time a reader displayed the
images or locked the study for reading, to the second, from the PACS audit trail copy
(etl_pacs_audit_events, ETL_JOBS/etl_pacs_audit.py). Only opens up to the final signature
count, so a study reopened after it was signed keeps its real first open.

READERS are the PACS logins that have signed a final report (rep_final_signed_by), so a
technologist checking images or a clinician in Vue Motion never starts the clock. Audit
logins carry a domain (dbustros@dn); both sides are compared without it.

A study is timed only when the copy holds all of its opens: it reached PACS (insert_time)
on or after the copy's first day and was finalized before the copy's last moment. Outside
that, or with no reader open before the final, it is NOT MEASURED: counted aside, never
given a guessed start.

Use: put READERS_CTE in the query's WITH list, FIRST_OPEN_JOIN after the FROM (studies
aliased `s`), select IN_AUDIT and TAT_MIN, and bind audit_params().
"""
from ETL_JOBS.etl_pacs_audit import coverage

READERS_CTE = """readers AS MATERIALIZED (
        SELECT DISTINCT UPPER(SPLIT_PART(BTRIM(rep_final_signed_by), '@', 1)) AS login
        FROM etl_didb_studies
        WHERE NULLIF(BTRIM(rep_final_signed_by), '') IS NOT NULL
    )"""

# The audit copy holds every open of this study (binds :audit_from, :audit_until)
IN_AUDIT = ("COALESCE(s.insert_time >= CAST(:audit_from AS timestamp)"
            " AND s.rep_final_timestamp <= CAST(:audit_until AS timestamp), FALSE)")

FIRST_OPEN_JOIN = f"""LEFT JOIN LATERAL (
        SELECT MIN(e.event_at) AS first_open_at
        FROM etl_pacs_audit_events e
        JOIN readers r ON r.login = UPPER(SPLIT_PART(e.user_name, '@', 1))
        WHERE {IN_AUDIT}
          AND e.study_instance_uid = BTRIM(s.study_instance_uid)
          AND e.event_at <= s.rep_final_timestamp
    ) fo ON TRUE"""

# Minutes from first open to final; NULL when not measured
TAT_MIN = "EXTRACT(EPOCH FROM (s.rep_final_timestamp - fo.first_open_at)) / 60"


def audit_params(session):
    """{'audit_from', 'audit_until'}: what the audit copy holds (both None before its first run)."""
    start, end = coverage(session)
    return {"audit_from": start, "audit_until": end}


def audit_window(session):
    """The copy's span as text for the page, or None before its first run."""
    start, end = coverage(session)
    if start is None:
        return None
    return {"from": start.strftime("%Y-%m-%d"), "until": end.strftime("%Y-%m-%d %H:%M")}
