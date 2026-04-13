"""
Phase 9: Ensemble clustering of procedure codes/descriptions.

Pipeline
--------
1. Pull all unclustered procedure codes + representative descriptions from DB
2. TF-IDF character n-gram (2-4) vectorization on "<code> <description>"
3. Cosine distance matrix (n × n)
4. Bootstrap ensemble — 20 subsamples × 3 algorithms = 60 independent runs
     - HDBSCAN          (density-based, native outlier label -1)
     - AgglomerativeClustering (average linkage, distance_threshold=0.45)
     - AffinityPropagation (message-passing, damping=0.7)
5. Co-association matrix: M[i,j] = fraction of runs where i,j were same cluster
6. Threshold 0.75 → stable pair edges → Union-Find connected components
7. Components with ≥ 2 members → ai_suggested groups
   Singletons → unclustered (kept in procedure_duration_map, no group)
8. Old ai_suggested non-approved groups are refreshed on each run
9. Results written to procedure_canonical_groups / procedure_canonical_members

Compute profile (n ≈ 1000):
  Memory: ~100 MB peak (3 × n² float32 matrices)
  Time:   30–60 seconds
"""

import numpy as np
from collections import defaultdict
from sqlalchemy import text


# ── Constants ────────────────────────────────────────────────────────────────
N_BOOTSTRAP      = 20      # subsampling iterations per algorithm
SUBSAMPLE_RATIO  = 0.80    # 80 % of codes per bootstrap draw
CO_THRESHOLD     = 0.75    # minimum co-association score to be in same cluster
AGG_DIST_THRESH  = 0.45    # cosine distance cut for AgglomerativeClustering
AP_DAMPING       = 0.70    # AffinityPropagation damping (0.5–1.0)


# ── Main entry point ─────────────────────────────────────────────────────────
def run_phase9_clustering(conn, logger):
    """Called from etl_runner._perform_migration after Phase 8."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_distances
        from sklearn.cluster import HDBSCAN, AgglomerativeClustering, AffinityPropagation
    except ImportError as e:
        logger.warning(f"Phase 9 skipped — scikit-learn not available: {e}")
        return

    # ── 1. Fetch unclustered codes ────────────────────────────────────────
    rows = conn.execute(text("""
        SELECT
            p.procedure_code,
            COALESCE(d.proc_text, p.procedure_code) AS description
        FROM procedure_duration_map p
        LEFT JOIN (
            SELECT
                UPPER(TRIM(proc_id))                                      AS procedure_code,
                MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(proc_text)))     AS proc_text
            FROM etl_orders
            WHERE proc_id  IS NOT NULL AND TRIM(proc_id)  != ''
              AND proc_text IS NOT NULL AND TRIM(proc_text) != ''
            GROUP BY UPPER(TRIM(proc_id))
        ) d ON d.procedure_code = p.procedure_code
        WHERE p.procedure_code NOT IN (
            SELECT procedure_code FROM procedure_canonical_members
        )
        ORDER BY p.procedure_code
    """)).fetchall()

    if len(rows) < 4:
        logger.info("Phase 9 — fewer than 4 unclustered procedures, skipping.")
        return

    codes = [r[0] for r in rows]
    descs = [r[1] for r in rows]
    n     = len(codes)
    logger.info(f"Phase 9 — clustering {n} unclustered procedure codes...")

    # ── 2. Vectorize ──────────────────────────────────────────────────────
    corpus = [f"{c} {d}" for c, d in zip(codes, descs)]
    vec    = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4), min_df=1)
    X      = vec.fit_transform(corpus)

    # ── 3. Distance matrix ────────────────────────────────────────────────
    dist = cosine_distances(X).astype(np.float32)
    np.clip(dist, 0.0, 1.0, out=dist)
    np.fill_diagonal(dist, 0.0)

    # ── 4. Bootstrap ensemble ─────────────────────────────────────────────
    # co_count[i,j]  = times i and j were in same non-noise cluster
    # co_denom[i,j]  = times both i and j were in the same subsample (× 3 algos)
    co_count = np.zeros((n, n), dtype=np.float32)
    co_denom = np.zeros((n, n), dtype=np.float32)

    rng = np.random.default_rng(seed=42)

    for boot in range(N_BOOTSTRAP):
        size     = max(4, int(n * SUBSAMPLE_RATIO))
        idx      = np.sort(rng.choice(n, size=size, replace=False))
        sub_dist = dist[np.ix_(idx, idx)]
        sub_sim  = np.clip(1.0 - sub_dist, 0.0, 1.0)

        # Every pair in this subsample gets 3 algorithm votes
        outer_idx = np.ix_(idx, idx)
        co_denom[outer_idx] += 3.0

        for algo_name, algo_labels in _run_algorithms(sub_dist, sub_sim, logger, boot):
            _accumulate(co_count, idx, algo_labels)

        logger.debug(f"Phase 9 — bootstrap {boot+1}/{N_BOOTSTRAP} done")

    # ── 5. Co-association matrix ──────────────────────────────────────────
    with np.errstate(divide='ignore', invalid='ignore'):
        co_assoc = np.where(co_denom > 0, co_count / co_denom, 0.0)
    np.fill_diagonal(co_assoc, 0.0)

    # ── 6. Threshold + Union-Find ─────────────────────────────────────────
    adj    = co_assoc >= CO_THRESHOLD
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    rows_i, cols_j = np.where(adj)
    for i, j in zip(rows_i, cols_j):
        if i < j:
            union(int(i), int(j))

    buckets = defaultdict(list)
    for i in range(n):
        buckets[find(i)].append(i)

    clusters     = {r: m for r, m in buckets.items() if len(m) >= 2}
    n_unclustered = sum(1 for m in buckets.values() if len(m) == 1)

    logger.info(
        f"Phase 9 — {len(clusters)} clusters, {n_unclustered} unclustered "
        f"(threshold={CO_THRESHOLD}, bootstrap={N_BOOTSTRAP}×3)"
    )

    # ── 7. Wipe stale AI suggestions, write new ones ──────────────────────
    conn.execute(text("""
        DELETE FROM procedure_canonical_groups
        WHERE source = 'ai_suggested' AND approved = FALSE
    """))

    inserted = 0
    for root, member_idx in clusters.items():
        member_codes = [codes[i] for i in member_idx]
        member_descs = [descs[i] for i in member_idx]

        # Canonical name placeholder: longest description (most descriptive)
        placeholder = max(member_descs, key=len)

        # Average co-association score for this cluster
        avg_conf = float(np.mean([
            co_assoc[i, j]
            for ii, i in enumerate(member_idx)
            for j in member_idx[ii+1:]
        ])) if len(member_idx) > 1 else 0.0

        row = conn.execute(text("""
            INSERT INTO procedure_canonical_groups
                (canonical_name, approved, source, cluster_confidence, detected_at)
            VALUES (:name, FALSE, 'ai_suggested', :conf, NOW())
            RETURNING id
        """), {"name": placeholder, "conf": round(avg_conf, 3)}).fetchone()
        gid = row[0]

        for i in member_idx:
            member_conf = float(np.mean([
                co_assoc[i, j] for j in member_idx if j != i
            ]))
            conn.execute(text("""
                INSERT INTO procedure_canonical_members
                    (procedure_code, group_id, similarity_score)
                VALUES (:code, :gid, :score)
                ON CONFLICT (procedure_code) DO NOTHING
            """), {"code": codes[i], "gid": gid, "score": round(member_conf, 3)})

        inserted += 1

    logger.info(f"Phase 9 — {inserted} AI-suggested groups written.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_algorithms(sub_dist, sub_sim, logger, boot):
    """Run the 3 algorithms on a subsample distance/similarity matrix.
    Yields (algo_name, labels_array) for each."""
    from sklearn.cluster import HDBSCAN, AgglomerativeClustering, AffinityPropagation

    # HDBSCAN
    try:
        labels = HDBSCAN(
            min_cluster_size=2,
            min_samples=1,
            metric='precomputed',
            cluster_selection_method='eom',
        ).fit_predict(sub_dist.astype(np.float64))
        yield 'hdbscan', labels
    except Exception as e:
        logger.debug(f"Phase 9 boot {boot} HDBSCAN failed: {e}")
        yield 'hdbscan', np.full(len(sub_dist), -1)

    # AgglomerativeClustering (average linkage, distance threshold)
    try:
        labels = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=AGG_DIST_THRESH,
            metric='precomputed',
            linkage='average',
        ).fit_predict(sub_dist.astype(np.float64))
        yield 'agglomerative', labels
    except Exception as e:
        logger.debug(f"Phase 9 boot {boot} Agglomerative failed: {e}")
        yield 'agglomerative', np.zeros(len(sub_dist), dtype=int)

    # AffinityPropagation (needs similarity, not distance)
    try:
        labels = AffinityPropagation(
            affinity='precomputed',
            damping=AP_DAMPING,
            max_iter=200,
            convergence_iter=15,
            random_state=boot,
        ).fit_predict(sub_sim.astype(np.float64))
        yield 'affinity', labels
    except Exception as e:
        logger.debug(f"Phase 9 boot {boot} AffinityProp failed: {e}")
        yield 'affinity', np.zeros(len(sub_dist), dtype=int)


def _accumulate(co_count, idx, labels):
    """For each non-noise cluster in labels, add 1 to co_count for every pair."""
    for lbl in set(labels):
        if lbl == -1:
            continue
        members = idx[labels == lbl]
        if len(members) < 2:
            continue
        outer = np.ix_(members, members)
        co_count[outer] += 1.0
