"""
utils/etl_freshness.py
----------------------
"Is the data behind this report actually being refreshed?"

Written after LAUMC 2026-09-21: RAYD_ETL_PHASES on that install read
`1,2,2b,3,...,14,15` and simply stopped there, so phases 16 (PACS user groups),
17 (RIS worklist status events) and 18 (RIS device schedule) had not run since a
one-off manual invocation on 1-2 August. Every report reading those tables kept
rendering happily off seven-week-old data with nothing on screen to say so --
the tables were populated, just frozen, which is the failure mode no empty-state
message catches.

A report declares the etl_job_log job names it depends on; this returns the ones
whose last SUCCESS is older than max_age_days (or that have never succeeded at
all). Freshness is measured on SUCCESS specifically: a job failing every night is
running, but it is not refreshing anything.

Deliberately advisory, never fatal -- a report with stale inputs still renders
its numbers, with a banner saying how old they are. Anything that raises in here
is swallowed and reported as "nothing stale", because a broken freshness check
must not be able to take down a working report.
"""
import logging
from datetime import datetime

from sqlalchemy import text

logger = logging.getLogger(__name__)

# The full sync runs nightly, so two clear days without a success means at least
# one whole run was missed or the job isn't scheduled at all. Tight enough to
# catch a dropped phase within a couple of days, loose enough that a single
# skipped night doesn't cry wolf at every viewer.
DEFAULT_MAX_AGE_DAYS = 2


def stale_feeds(db, feeds, max_age_days=DEFAULT_MAX_AGE_DAYS):
    """Return the stale entries among `feeds`, worst first.

    `feeds` maps an etl_job_log.job_name to the human label shown to the reader,
    e.g. {"RIS_WORKLIST_EXAM_DONE_ETL": "RIS exam-done times (RIS TAT anchor)"}.

    Each returned dict: {job_name, label, last_success (datetime|None),
    age_days (int|None), never_ran (bool)}. A job with no successful run ever
    sorts first and reports age_days None rather than a misleading huge number.
    """
    if not feeds:
        return []

    try:
        rows = db.session.execute(text("""
            SELECT job_name, MAX(start_time) AS last_success
            FROM etl_job_log
            WHERE job_name = ANY(:names) AND status = 'SUCCESS'
            GROUP BY job_name
        """), {"names": list(feeds)}).mappings().all()
    except Exception:
        logger.exception("ETL freshness check failed")
        try:
            db.session.rollback()
        except Exception:
            pass
        return []

    last_by_job = {r["job_name"]: r["last_success"] for r in rows}
    now = datetime.now()
    stale = []

    for job_name, label in feeds.items():
        last = last_by_job.get(job_name)
        if last is None:
            stale.append({"job_name": job_name, "label": label, "last_success": None,
                          "age_days": None, "never_ran": True})
            continue
        age_days = (now - last).days
        if age_days >= max_age_days:
            stale.append({"job_name": job_name, "label": label, "last_success": last,
                          "age_days": age_days, "never_ran": False})

    # Never-ran first, then oldest first.
    stale.sort(key=lambda s: (0 if s["never_ran"] else 1, -(s["age_days"] or 0)))
    return stale
