# etl_settings.py
#
# Tuned up 2026-07-27 (LAUMC): both the RAYD app server and the PACS Oracle server sit
# at low utilization even during full raw-images/image-locations runs (12 vCPU / 50GB RAM
# mostly idle; Oracle ~25% CPU, modest network) — headroom on both ends supports pushing
# throughput harder, especially on a low-traffic window (Sunday). Values below are a
# moderate (~2x) step up, not a max-out: revert to the previous values (halve
# oracle_prefetch/batch_size/commit_every) if either server starts showing real load.

ETL_GEAR = {
    'num_workers': 6,          # NOTE: declared but not currently wired to any real
                                # threading — no ETL job actually parallelizes upsert
                                # work yet. Left as-is; not a live lever today.
    'batch_size': 20000,       # Rows per Oracle fetchmany() / Postgres upsert flush unit
    'oracle_prefetch': 20000,  # cursor.arraysize — Oracle network round-trip buffer size.
                                # Should stay >= batch_size (fetchmany asks the driver for
                                # N rows; arraysize controls how many it actually fetches
                                # per round trip under the hood).
    'commit_every': 200000,    # Rows buffered in Python before a Postgres flush.
    'chunk_size': 1000,        # Study IDs per Oracle WHERE study_db_uid IN (...) query.
                                # NOT a tunable — this is Oracle's own hard ceiling
                                # (ORA-01795: maximum number of expressions in a list is
                                # 1000), which applies to bind-variable IN-lists exactly
                                # like literal ones. Raising this breaks the query outright
                                # rather than just being riskier; documented here so nobody
                                # "optimizes" it up by mistake.
    'log_interval': 10000,     # Progress logging frequency
}
