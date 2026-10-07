#!/usr/bin/env python3
"""
RAYD — ORU NLP Worker
Standalone medspaCy batch processor. Runs as a separate Docker container
so medspaCy's RAM footprint and native deps are isolated from the main app.

Polls hl7_oru_reports every 60 seconds, processes unanalyzed rows in chunks,
writes results to hl7_oru_analysis.

Also polls oru_nlp_jobs every few seconds for on-demand TF-IDF/K-means
clustering runs requested from /oru/nlp/process (routes/oru_analytics.py) --
that route just enqueues a row and returns immediately; this worker does the
actual clustering (moved here from nlp_processor.py -> clustering.py so the
main app never blocks a request thread on it).
"""
import os
import time
import json
import re
from collections import deque
import psycopg2
import psycopg2.extras

import clustering

# ── Multi-pattern matching (Aho-Corasick) ─────────────────────────────────────
# The rule-based fallback used to run one independent str.find() sweep per
# phrase (~150 phrases x up to 8000 chars, per report). A single combined regex
# alternation would be faster but only reports non-overlapping matches, which
# silently drops shorter phrases nested inside longer ones (e.g. the CRITICAL
# keyword "effusion" inside the DIAGNOSES phrase "pleural effusion") -- a real
# risk for a clinical critical-findings feed. Aho-Corasick finds every
# occurrence of every pattern, including overlapping ones, in one O(text
# length) pass, so it's a strict speedup with no change in what gets matched.

class _AhoCorasick:
    """Minimal Aho-Corasick automaton for multi-pattern substring search."""

    def __init__(self, patterns):
        self._goto = [{}]
        self._fail = [0]
        self._output = [[]]
        for p in patterns:
            self._add(p)
        self._build_fail_links()

    def _add(self, pattern):
        node = 0
        for ch in pattern:
            nxt = self._goto[node].get(ch)
            if nxt is None:
                self._goto.append({})
                self._fail.append(0)
                self._output.append([])
                nxt = len(self._goto) - 1
                self._goto[node][ch] = nxt
            node = nxt
        self._output[node].append(pattern)

    def _build_fail_links(self):
        queue = deque()
        root = 0
        for ch, nxt in self._goto[root].items():
            self._fail[nxt] = root
            queue.append(nxt)
        while queue:
            node = queue.popleft()
            for ch, nxt in list(self._goto[node].items()):
                queue.append(nxt)
                f = self._fail[node]
                while f != root and ch not in self._goto[f]:
                    f = self._fail[f]
                target = self._goto[f].get(ch, root)
                self._fail[nxt] = target if target != nxt else root
                self._output[nxt] = self._output[nxt] + self._output[self._fail[nxt]]

    def find_all(self, text):
        """Yield (start_index, pattern) for every occurrence of every pattern."""
        node = 0
        for i, ch in enumerate(text):
            while node and ch not in self._goto[node]:
                node = self._fail[node]
            node = self._goto[node].get(ch, 0)
            for pattern in self._output[node]:
                yield i - len(pattern) + 1, pattern


# ── Negation helpers ──────────────────────────────────────────────────────────

NEGATION_PREFIXES = [
    'no ', 'not ', 'without ', 'negative for ', 'negative ',
    'no evidence of ', 'no evidence for ',
    'no sign of ', 'no signs of ',
    'no finding of ', 'no findings of ',
    'no suggestion of ', 'no history of ',
    'absence of ', 'absent ', 'free of ',
    'ruled out', 'no acute ', 'no definite ', 'no demonstrable ',
    'denies ', 'denied ', 'no identified ',
]

def _is_negated(t, match_start, window=80):
    segment = t[max(0, match_start - window):match_start]
    for sep in ('.', '\n', ';', '?', '!'):
        last_sep = segment.rfind(sep)
        if last_sep != -1:
            segment = segment[last_sep + 1:]
    return any(neg in segment for neg in NEGATION_PREFIXES)


# ── Critical keyword groups ───────────────────────────────────────────────────
# Kept as a code constant (unlike DIAGNOSES below) -- these feed medspaCy's
# target matcher and the rule-based fallback, and already have a separate
# user-extension mechanism (settings 'oru_crit:*' rows, main app only).

CRITICAL = [
    'pneumothorax','hemorrhage','haemorrhage','haematoma','hematoma',
    'pulmonary embolism','aortic dissection','stroke','infarct','infarction',
    'fracture','mass','malignancy','malignant','tumor','tumour','carcinoma',
    'thrombosis','obstruction','perforation','rupture','aneurysm','abscess',
    'appendicitis','ischemia','ischaemia','neoplasm','metastasis','metastases',
    'occlusion','stenosis','dissection','embolism','pneumonia','effusion',
]

# ── Diagnosis vocabulary — loaded from oru_diagnosis_vocabulary (DB-configurable,
# see migration 0103) instead of a hardcoded constant. Populated once at startup
# by _load_vocabulary(); picking up an admin's edit requires restarting this
# container (`docker compose restart rayd-nlp`) -- no rebuild/redeploy needed.

DIAGNOSES = []          # [(phrase, canonical_label), ...]
_BENIGN_LABELS = set()  # canonical labels considered benign
_PHRASE_AUTOMATON = None

def _load_vocabulary(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
        cur.execute("""
            SELECT phrase, canonical_label, is_benign
            FROM oru_diagnosis_vocabulary
            WHERE active = TRUE
            ORDER BY id
        """)
        rows = cur.fetchall()
    diagnoses = [(r.phrase, r.canonical_label) for r in rows]
    benign = {r.canonical_label for r in rows if r.is_benign}
    return diagnoses, benign


def _init_vocabulary():
    global DIAGNOSES, _BENIGN_LABELS, _PHRASE_AUTOMATON
    conn = _get_conn()
    try:
        DIAGNOSES, _BENIGN_LABELS = _load_vocabulary(conn)
    finally:
        conn.close()
    all_phrases = sorted({p for p, _ in DIAGNOSES} | set(CRITICAL))
    _PHRASE_AUTOMATON = _AhoCorasick(all_phrases)
    print(f"[NLP Worker] Vocabulary loaded — {len(DIAGNOSES)} diagnosis phrases, "
          f"{len(CRITICAL)} critical keywords.")


# Bump when the model or vocabulary changes. Stale rows are only re-analysed
# where a requeue step says so (see _requeue_non_rule_reports).
# v2 = ConTextRule("non") removed (2026-10-07).
# v3 = ’ normalised; "pas d'", "absence d'", "is/was excluded", "mais" (2026-10-07).
_NLP_MODEL_VERSION = 'medspacy-v3'

# ai_nlp_cache.nlp_version (migration 0126). Rows with any other value are
# recomputed by the next on-demand job; rows with none (written before negation
# was resolved) are also hidden from the ORU page.
_CLUSTER_VERSION = 'tfidf-negation-v2'   # v2 = medspacy-v3 cues

_CHUNK           = 500
_BATCH_LIMIT      = 2000
_POLL_SECONDS      = 60
_JOB_POLL_SECONDS  = 5
_BATCH_EVERY_TICKS = _POLL_SECONDS // _JOB_POLL_SECONDS


# ── medspaCy ──────────────────────────────────────────────────────────────────

_NLP = None
_NLP_WORKERS = max(1, int((os.cpu_count() or 4) * 0.75))


def _same_clause(target, modifier, span_between):
    """ConText on_modifies callback for the post-posed "excluded" cues: negate only
    a finding in the same clause, so "Large pleural effusion, pulmonary embolism
    is excluded." keeps the effusion."""
    return not any(tok.text in (',', ';') for tok in span_between)


def _load_medspacy():
    global _NLP
    if _NLP is not None:
        return _NLP
    try:
        import medspacy
        from medspacy.target_matcher import TargetRule
        from medspacy.context import ConTextRule

        nlp = medspacy.load(enable=["sentencizer", "medspacy_target_matcher", "medspacy_context"])

        target_matcher = nlp.get_pipe("medspacy_target_matcher")
        seen, rules = set(), []
        for phrase, _ in DIAGNOSES:
            if phrase not in seen:
                rules.append(TargetRule(phrase, "FINDING"))
                seen.add(phrase)
        for kw in CRITICAL:
            if kw not in seen:
                rules.append(TargetRule(kw, "FINDING"))
                seen.add(kw)
        target_matcher.add(rules)

        # "non" is deliberately NOT one of the French cues: spaCy splits
        # "Non-contrast" into "Non" + "-" + "contrast", so a FORWARD "non" rule
        # negated every finding after "Non contrast CT"
        # ("Non contrast CT of the head demonstrates acute hemorrhage" -> nothing).
        # Same fix as HL7 c2691fcf and Mazloum 13dbf21c.
        context = nlp.get_pipe("medspacy_context")
        context.add([
            ConTextRule("pas de",        "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("sans",          "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("absence de",    "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("aucun",         "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("aucune",        "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("négatif pour",  "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("négatif",       "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("exclu",         "NEGATED_EXISTENCE", direction="BIDIRECTIONAL"),
            ConTextRule("écarté",        "NEGATED_EXISTENCE", direction="BIDIRECTIONAL"),
            # Elided forms: spaCy splits "d'épanchement" into d + ' + épanchement, so
            # "pas de" never matched and "Pas d'épanchement pleural" was flagged as
            # a pleural effusion. (’ is normalised to ' by _clean_text.)
            ConTextRule("pas d'",        "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("absence d'",    "NEGATED_EXISTENCE", direction="FORWARD"),
            # French "but", as the stock English "but": "Pas de fracture mais
            # épanchement pleural important" negated the effusion too.
            ConTextRule("mais",          "NEGATED_EXISTENCE", direction="TERMINATE"),
        ])

        # medspaCy's stock rules know "ruled out" but not "excluded", so "Pulmonary
        # embolism is excluded." was flagged as a pulmonary embolism. Only the
        # explicit forms: a bare "excluded" would also match "cannot be excluded"
        # and negate a possible finding.
        context.add([
            ConTextRule(cue, "NEGATED_EXISTENCE", direction="BACKWARD", on_modifies=_same_clause)
            for cue in ("is excluded", "was excluded", "are excluded", "were excluded",
                        "has been excluded", "have been excluded")
        ])

        _NLP = nlp
        print("[NLP Worker] medspaCy loaded — clinical NLP active.")
    except Exception as e:
        print(f"[NLP Worker] medspaCy unavailable ({e}) — rule-based fallback active.")
    return _NLP


def _affirmed_phrases_rule_based(t):
    """Single Aho-Corasick pass over the text; the existing negation-window
    check still runs per match (unchanged semantics from the old str.find
    loop — a phrase is affirmed if ANY of its occurrences is unnegated)."""
    found = set()
    for pos, phrase in _PHRASE_AUTOMATON.find_all(t):
        if phrase in found:
            continue
        if not _is_negated(t, pos):
            found.add(phrase)
    return found


def _clean_text(t):
    # spaCy keeps "d’épanchement" (typographic apostrophe) as ONE token, so neither
    # the finding nor a negation cue around it was ever matched.
    return (t or '').lower().replace('’', "'")[:8000]


def _affirmed_phrases_batch(texts):
    if not texts:
        return []
    cleaned = [_clean_text(t) for t in texts]
    nlp = _load_medspacy()

    if nlp is not None:
        def _docs_to_sets(docs):
            return [
                {ent.text.lower() for ent in doc.ents
                 if not ent._.is_negated and not ent._.is_historical}
                for doc in docs
            ]
        try:
            return _docs_to_sets(nlp.pipe(cleaned, batch_size=64, n_process=_NLP_WORKERS))
        except Exception:
            try:
                return _docs_to_sets(nlp.pipe(cleaned, batch_size=64, n_process=1))
            except Exception:
                pass

    return [_affirmed_phrases_rule_based(t) for t in cleaned]


# ConText categories that run_batch() treats as "not a current finding".
_MASKED_CONTEXT = ('NEGATED_EXISTENCE', 'HISTORICAL')


def _negation_resolved_texts(texts):
    """The texts with every negated or historical mention removed, decided by the
    same medspaCy pipeline as run_batch(), for the TF-IDF/K-means jobs. Removes
    each cue and its whole ConText scope, not just vocabulary findings, so "no
    evidence of spondylolisthesis" also drops a word the vocabulary does not
    know. TERMINATE/PSEUDO modifiers carry a category too ("but" is
    NEGATED_EXISTENCE) and only limit other scopes, so they are skipped.

    Raises if medspaCy is unavailable: the job then fails visibly instead of
    classifying on the rule-based fallback."""
    nlp = _load_medspacy()
    if nlp is None:
        raise RuntimeError("medspaCy is not available in the NLP worker, so negations "
                           "cannot be resolved. The analysis was not run.")
    cleaned = [_clean_text(t) for t in texts]
    resolved = []
    # n_process=1: doc._.context_graph is read here, in this process.
    for doc in nlp.pipe(cleaned, batch_size=64, n_process=1):
        drop = set()
        for m in doc._.context_graph.modifiers:
            if m.category in _MASKED_CONTEXT and m.direction.upper() not in ('TERMINATE', 'PSEUDO'):
                drop.update(range(*m.modifier_span))
                # Per token, so a rule's on_modifies limit (_same_clause) holds here too.
                drop.update(i for i in range(*m.scope_span) if m.on_modifies(doc[i:i + 1]))
        for ent in doc.ents:
            if ent._.is_negated or ent._.is_historical:
                drop.update(range(ent.start, ent.end))
        resolved.append(''.join(tok.text_with_ws for tok in doc if tok.i not in drop))
    return resolved


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'db'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )


# ── Batch processing (medspaCy negation-aware analysis) ───────────────────────

def run_batch():
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute("""
                SELECT r.id, r.impression_text, r.report_text
                FROM   hl7_oru_reports r
                LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
                WHERE  a.id IS NULL
                ORDER  BY r.received_at DESC
                LIMIT  %s
            """, (_BATCH_LIMIT,))
            rows = cur.fetchall()

        if not rows:
            return

        total, committed = len(rows), 0

        for chunk_start in range(0, total, _CHUNK):
            chunk  = rows[chunk_start:chunk_start + _CHUNK]
            texts  = [(r.impression_text or r.report_text or '') for r in chunk]
            affirmed_list = _affirmed_phrases_batch(texts)

            for r, affirmed in zip(chunk, affirmed_list):
                seen, labels = set(), []
                for phrase, label in DIAGNOSES:
                    if label in _BENIGN_LABELS or label in seen:
                        continue
                    if phrase in affirmed:
                        seen.add(label)
                        labels.append(label)
                pg_array = '{' + ','.join(labels) + '}'
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO hl7_oru_analysis
                                (report_id, affirmed_labels, is_critical, nlp_version, analyzed_at)
                            VALUES (%s, %s::TEXT[], %s, %s, NOW())
                            ON CONFLICT (report_id) DO NOTHING
                        """, (r.id, pg_array, len(labels) > 0, _NLP_MODEL_VERSION))
                    # Commit per row: a failure on one row must only roll back that
                    # row, not every prior success in this chunk (previously a
                    # single rollback() here discarded the whole chunk-so-far).
                    conn.commit()
                    committed += 1
                except Exception as e:
                    print(f"[NLP Worker] Row {r.id} error: {e}")
                    conn.rollback()
                    continue

        print(f"[NLP Worker] Batch complete — {committed}/{total} reports analyzed.")
    finally:
        conn.close()


# ── On-demand clustering jobs (oru_nlp_jobs) ──────────────────────────────────

def run_pending_jobs():
    conn = _get_conn()
    job = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute("""
                SELECT id, days FROM oru_nlp_jobs
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            job = cur.fetchone()
            if job:
                cur.execute("""
                    UPDATE oru_nlp_jobs SET status = 'running', started_at = NOW()
                    WHERE id = %s
                """, (job.id,))
        conn.commit()
    except Exception as e:
        print(f"[NLP Worker] Job claim error: {e}")
        conn.rollback()
        conn.close()
        return

    if not job:
        conn.close()
        return

    try:
        _process_job(conn, job.id, job.days)
    finally:
        conn.close()


def _process_job(conn, job_id, days):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute("""
                SELECT o.id, o.report_text, o.impression_text
                FROM hl7_oru_reports o
                LEFT JOIN ai_nlp_cache c ON c.source_id = o.id
                WHERE (c.id IS NULL OR c.nlp_version IS DISTINCT FROM %s)
                  AND o.received_at >= NOW() - (%s || ' days')::INTERVAL
                  AND o.report_text IS NOT NULL
                  AND TRIM(o.report_text) != ''
                ORDER BY o.received_at DESC
                LIMIT 500
            """, (_CLUSTER_VERSION, days))
            rows = cur.fetchall()

        if not rows:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE oru_nlp_jobs
                    SET status = 'done', processed_count = 0, cluster_count = 0,
                        message = 'Nothing new to process.', finished_at = NOW()
                    WHERE id = %s
                """, (job_id,))
            conn.commit()
            return

        texts = [(r.impression_text or r.report_text or '').strip() for r in rows]
        resolved = _negation_resolved_texts(texts)
        records = [
            {'id': r.id, 'text': t, 'resolved': res}
            for r, t, res in zip(rows, texts, resolved)
        ]
        results, cluster_labels = clustering.process_reports(records)

        # Cluster labels first so ai_nlp_cache.cluster_label can reference them.
        with conn.cursor() as cur:
            for cid, label in enumerate(cluster_labels):
                cur.execute("""
                    INSERT INTO oru_cluster_labels (cluster_id, label, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (cluster_id) DO UPDATE SET
                        label = EXCLUDED.label, updated_at = NOW()
                """, (cid, label))
        conn.commit()

        saved = 0
        for res in results:
            cid = res['cluster_id']
            label = cluster_labels[cid] if cid is not None and cid < len(cluster_labels) else None
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO ai_nlp_cache
                            (source_id, classification, keywords, cluster_id, cluster_label,
                             severity_score, nlp_version, processed_at)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, NOW())
                        ON CONFLICT (source_id) DO UPDATE SET
                            classification = EXCLUDED.classification,
                            keywords       = EXCLUDED.keywords,
                            cluster_id     = EXCLUDED.cluster_id,
                            cluster_label  = EXCLUDED.cluster_label,
                            severity_score = EXCLUDED.severity_score,
                            nlp_version    = EXCLUDED.nlp_version,
                            processed_at   = NOW()
                    """, (res['id'], res['classification'], json.dumps(res['keywords']),
                          cid, label, res['severity_score'], _CLUSTER_VERSION))
                # Commit per row (item 10 fix — same reasoning as run_batch()).
                conn.commit()
                saved += 1
            except Exception as e:
                print(f"[NLP Worker] ai_nlp_cache row {res['id']} error: {e}")
                conn.rollback()
                continue

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE oru_nlp_jobs
                SET status = 'done', processed_count = %s, cluster_count = %s,
                    message = %s, finished_at = NOW()
                WHERE id = %s
            """, (saved, len(cluster_labels),
                  f'Processed {saved} reports into {len(cluster_labels)} clusters.', job_id))
        conn.commit()
        print(f"[NLP Worker] Job {job_id} done — {saved} reports, {len(cluster_labels)} clusters.")

    except Exception as e:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE oru_nlp_jobs SET status = 'error', error_message = %s, finished_at = NOW()
                    WHERE id = %s
                """, (str(e)[:2000], job_id))
            conn.commit()
        except Exception:
            conn.rollback()
        print(f"[NLP Worker] Job {job_id} error: {e}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def _requeue_non_rule_reports():
    """Drop v1 analysis rows of reports containing the word "non", so run_batch
    re-analyses them without the removed ConTextRule("non") (it negated every
    finding after "Non contrast CT"). Done here rather than in a migration so
    the re-analysis can only ever run on this fixed worker. Touches v1 rows only,
    so it is a no-op once they are gone. Connects as the table owner, so the
    site RLS policy (migration 0050) does not hide any rows."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(r"""
                DELETE FROM hl7_oru_analysis a
                USING  hl7_oru_reports r
                WHERE  a.report_id = r.id
                  AND  a.nlp_version = 'medspacy-v1'
                  AND  COALESCE(NULLIF(r.impression_text, ''), r.report_text, '') ~* '\mnon\M'
            """)
            requeued = cur.rowcount
        conn.commit()
        if requeued:
            print(f"[NLP Worker] Requeued {requeued} report(s) analysed with the old \"non\" rule.")
    finally:
        conn.close()


def _requeue_v3_cue_reports():
    """Drop v1/v2 analysis rows of reports that v3 reads differently (a typographic
    apostrophe, "pas d'", "absence d'", "... excluded", "mais"), so run_batch
    re-analyses them and the critical findings log is corrected both ways. Skipped
    while medspaCy is down: the rule-based fallback lacks these cues and would
    stamp the old result v3. Touches v1/v2 rows only, so it is a no-op once they
    are gone."""
    if _NLP is None:
        print("[NLP Worker] medspaCy not loaded -- v3 requeue postponed to the next start.")
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(r"""
                DELETE FROM hl7_oru_analysis a
                USING  hl7_oru_reports r
                WHERE  a.report_id = r.id
                  AND  a.nlp_version IN ('medspacy-v1', 'medspacy-v2')
                  AND  COALESCE(NULLIF(r.impression_text, ''), r.report_text, '')
                       ~* '’|\m(pas|absence)\s+d\s*''|\mexcluded\M|\mmais\M'
            """)
            requeued = cur.rowcount
        conn.commit()
        if requeued:
            print(f"[NLP Worker] Requeued {requeued} report(s) for the v3 negation cues.")
    finally:
        conn.close()


def main():
    print("[NLP Worker] Starting up...")

    # Wait for PostgreSQL to be ready
    while True:
        try:
            c = _get_conn()
            c.close()
            break
        except Exception as e:
            print(f"[NLP Worker] DB not ready ({e}) — retrying in 5s")
            time.sleep(5)

    print("[NLP Worker] DB ready.")
    _init_vocabulary()
    _load_medspacy()
    try:
        _requeue_non_rule_reports()
    except Exception as e:
        print(f"[NLP Worker] Requeue of \"non\"-rule reports failed: {e}")
    try:
        _requeue_v3_cue_reports()
    except Exception as e:
        print(f"[NLP Worker] Requeue of v3-cue reports failed: {e}")
    print(f"[NLP Worker] Polling jobs every {_JOB_POLL_SECONDS}s, "
          f"medspaCy batch every {_POLL_SECONDS}s.")

    tick = 0
    while True:
        try:
            run_pending_jobs()
        except Exception as e:
            print(f"[NLP Worker] Job poll error: {e}")

        if tick % _BATCH_EVERY_TICKS == 0:
            try:
                run_batch()
            except Exception as e:
                print(f"[NLP Worker] Batch error: {e}")

        tick += 1
        time.sleep(_JOB_POLL_SECONDS)


if __name__ == '__main__':
    main()
