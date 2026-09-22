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

    # ENGLISH ONLY (operator decision, 2026-09-22). French cues were added here
    # briefly and removed the same day: 'non ' matched the standard radiology
    # phrase "Non contrast CT" and suppressed everything after it —
    # "Non contrast CT of the head demonstrates acute hemorrhage" scored as
    # NOTHING. A missed critical finding costs more than an extra one, so an
    # unused-language cue that can over-negate has no place on this path.
]

# Cues that follow the finding instead of preceding it. A backward-only scan can
# never see these, which is why 'ruled out' sat in the list above doing nothing —
# English puts it after the finding ("Pneumothorax was ruled out"), not before.
NEGATION_POSTFIXES = [
    'ruled out', 'is absent', 'are absent', 'was excluded', 'were excluded',
    'not seen', 'not identified', 'not visualized', 'not visualised',
    'not demonstrated', 'not present', 'excluded',
    ': none', ':none', ': nil', ': negative', ': absent',
]


def _is_negated(t, match_start, window=80, match_end=None, fwd_window=40):
    """
    Whether the finding at match_start is negated.

    Scans BACKWARD for a prefix cue, and — when match_end is supplied — FORWARD
    for a postfix cue. Both scans stop at a sentence boundary so a cue cannot
    reach across a full stop and suppress an unrelated finding in the next
    sentence; over-negating is a false NEGATIVE, which is the expensive direction
    clinically.

    The forward scan is narrower than the backward one on purpose: postfix cues
    sit immediately after the finding ("X: none", "X was ruled out"), whereas a
    prefix cue can be separated from it by a longer noun phrase.
    """
    seps = ('.', '\n', ';', '?', '!')

    segment = t[max(0, match_start - window):match_start]
    for sep in seps:
        last_sep = segment.rfind(sep)
        if last_sep != -1:
            segment = segment[last_sep + 1:]
    if any(neg in segment for neg in NEGATION_PREFIXES):
        return True

    if match_end is not None:
        tail = t[match_end:match_end + fwd_window]
        for sep in seps:
            first_sep = tail.find(sep)
            if first_sep != -1:
                tail = tail[:first_sep]
        if any(neg in tail for neg in NEGATION_POSTFIXES):
            return True

    return False


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


# Bump when the model or vocabulary changes — triggers re-analysis of stale rows.
#
# Two values, because which engine produced a row is a property of the row, not of
# the build: the rule-based fallback can run at any time and scores worse on
# negation, so rows it produced must be identifiable and re-analysable once
# medspaCy is healthy again. Query for the fallback value to find them:
#   SELECT count(*) FROM hl7_oru_analysis WHERE nlp_version = 'rulebased-v1';
_ENGINE_MEDSPACY = 'medspacy-v1'
_ENGINE_FALLBACK = 'rulebased-v1'
_NLP_MODEL_VERSION = _ENGINE_MEDSPACY      # kept for callers that import it

_CHUNK           = 500
_BATCH_LIMIT      = 2000
_POLL_SECONDS      = 60
_JOB_POLL_SECONDS  = 5
_BATCH_EVERY_TICKS = _POLL_SECONDS // _JOB_POLL_SECONDS

# On-demand TF-IDF/K-means clustering jobs (oru_nlp_jobs, /oru/nlp/process).
# Was 500 -- on an install with a large historical HL7 backlog that meant
# ~1000 button clicks to clear a ~500k-report queue. A single clustering call
# over this many short reports still finishes in well under a minute.
_JOB_LIMIT = 5000


# ── medspaCy ──────────────────────────────────────────────────────────────────

_NLP = None
_NLP_WORKERS = max(1, int((os.cpu_count() or 4) * 0.75))


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

        # ENGLISH ONLY (operator decision, 2026-09-22): the reporting audience is
        # English, so medspaCy's built-in English ConText rules are the whole rule
        # set and no language extensions are registered.
        #
        # A French rule block used to live here and was REMOVED because one of its
        # rules was actively harmful on English text:
        #
        #     ConTextRule("non", "NEGATED_EXISTENCE", direction="FORWARD")
        #
        # spaCy tokenises "Non-contrast" as "Non" + "-" + "contrast", so that rule
        # matched the standard radiology phrase "Non contrast CT" / "Non-contrast CT"
        # and negated everything after it. Measured 2026-09-22:
        #
        #     "Non contrast CT of the head demonstrates acute hemorrhage."  -> NOTHING
        #     "Non-contrast CT demonstrates hemorrhage."                    -> NOTHING
        #
        # That is a silently MISSED critical finding on one of the most common
        # phrasings in CT reporting — the expensive direction of error. The other
        # French rules were harmless but useless here, and went with it.
        #
        # If a French-reporting site is ever onboarded, restore them from git
        # history WITHOUT "non", and add the French diagnosis phrases to
        # oru_diagnosis_vocabulary at the same time — the target terms are English
        # too, so French cues alone would not have detected anything to negate.
        #
        # What IS registered: a small English supplement for post-posed cues the
        # stock rule set misses. medspaCy already handles "X was ruled out" but not
        # "X excluded", which scored a false positive on the 2026-09-22 corpus.
        # BACKWARD because these cues follow the finding they negate.
        context = nlp.get_pipe("medspacy_context")
        context.add([
            ConTextRule("excluded",     "NEGATED_EXISTENCE", direction="BACKWARD"),
            ConTextRule("is excluded",  "NEGATED_EXISTENCE", direction="BACKWARD"),
            ConTextRule("was excluded", "NEGATED_EXISTENCE", direction="BACKWARD"),
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
        if not _is_negated(t, pos, match_end=pos + len(phrase)):
            found.add(phrase)
    return found


def _affirmed_phrases_batch(texts):
    """
    Returns (affirmed_sets, engine) — the engine being which path actually ran.

    THE ENGINE IS RETURNED, NOT ASSUMED. This used to fall through to the
    rule-based path on any exception, silently, and the caller stamped every row
    'medspacy-v1' regardless. A batch degraded by a transient spaCy
    multiprocessing failure was therefore indistinguishable in the data from a
    genuine medspaCy analysis — while scoring measurably worse on negation
    (9/14 vs 14/14 on the 2026-09-22 corpus, all five errors false positives).

    "The NLP analysed this and found nothing" must never be indistinguishable
    from "the NLP could not look properly", which is the same rule RAY7 follows
    with RAY7_DEGRADED. Recording the engine is what makes a degraded run
    findable afterwards instead of permanently invisible.
    """
    if not texts:
        return [], _ENGINE_MEDSPACY
    cleaned = [(t or '').lower()[:8000] for t in texts]
    nlp = _load_medspacy()

    if nlp is not None:
        def _docs_to_sets(docs):
            return [
                {ent.text.lower() for ent in doc.ents
                 if not ent._.is_negated and not ent._.is_historical}
                for doc in docs
            ]
        try:
            return _docs_to_sets(
                nlp.pipe(cleaned, batch_size=64, n_process=_NLP_WORKERS)), _ENGINE_MEDSPACY
        except Exception as exc:
            # Loud, not silent. Multiprocessing is the usual casualty under
            # memory pressure and single-process almost always succeeds, so this
            # is a recoverable degradation worth seeing in the logs.
            print(f"[NLP Worker] medspaCy n_process={_NLP_WORKERS} failed ({exc}); "
                  f"retrying single-process.")
            try:
                return _docs_to_sets(
                    nlp.pipe(cleaned, batch_size=64, n_process=1)), _ENGINE_MEDSPACY
            except Exception as exc2:
                print(f"[NLP Worker] medspaCy single-process ALSO failed ({exc2}); "
                      f"falling back to rule-based matching for {len(cleaned)} report(s). "
                      f"Negation accuracy is reduced — rows stamped '{_ENGINE_FALLBACK}'.")

    return [_affirmed_phrases_rule_based(t) for t in cleaned], _ENGINE_FALLBACK


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
            affirmed_list, engine = _affirmed_phrases_batch(texts)

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
                        """, (r.id, pg_array, len(labels) > 0, engine))
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
        date_filter = "AND o.received_at >= NOW() - (%s || ' days')::INTERVAL" if days is not None else ""
        params = (days, _JOB_LIMIT) if days is not None else (_JOB_LIMIT,)
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute(f"""
                SELECT o.id, o.report_text, o.impression_text
                FROM hl7_oru_reports o
                LEFT JOIN ai_nlp_cache c ON c.source_id = o.id
                WHERE c.id IS NULL
                  {date_filter}
                  AND o.report_text IS NOT NULL
                  AND TRIM(o.report_text) != ''
                ORDER BY o.received_at DESC
                LIMIT %s
            """, params)
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

        records = [
            {'id': r.id, 'report_text': r.report_text, 'impression_text': r.impression_text}
            for r in rows
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
                            (source_id, classification, keywords, cluster_id, cluster_label, severity_score, processed_at)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s, NOW())
                        ON CONFLICT (source_id) DO UPDATE SET
                            classification = EXCLUDED.classification,
                            keywords       = EXCLUDED.keywords,
                            cluster_id     = EXCLUDED.cluster_id,
                            cluster_label  = EXCLUDED.cluster_label,
                            severity_score = EXCLUDED.severity_score,
                            processed_at   = NOW()
                    """, (res['id'], res['classification'], json.dumps(res['keywords']),
                          cid, label, res['severity_score']))
                # Commit per row (item 10 fix — same reasoning as run_batch()).
                conn.commit()
                saved += 1
            except Exception as e:
                print(f"[NLP Worker] ai_nlp_cache row {res['id']} error: {e}")
                conn.rollback()
                continue

        # Backfill jobs (days IS NULL) have no date window to exhaust, so one
        # _JOB_LIMIT-sized chunk is never "the whole backlog" -- chain another
        # pending job to pick up where this one left off. Only chain on real
        # progress (saved > 0): a poison row that fails every attempt would
        # otherwise reproduce itself in the next chunk's WHERE c.id IS NULL
        # filter forever.
        next_job_id = None
        if days is None and len(rows) == _JOB_LIMIT and saved > 0:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                cur.execute("""
                    INSERT INTO oru_nlp_jobs (status, days, requested_by)
                    VALUES ('pending', NULL, (SELECT requested_by FROM oru_nlp_jobs WHERE id = %s))
                    RETURNING id
                """, (job_id,))
                next_job_id = cur.fetchone().id

        message = f'Processed {saved} reports into {len(cluster_labels)} clusters.'
        if next_job_id:
            message += ' More pending — next batch queued.'
        elif saved == 0:
            message += ' All rows in this batch failed — see worker logs.'

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE oru_nlp_jobs
                SET status = 'done', processed_count = %s, cluster_count = %s,
                    message = %s, next_job_id = %s, finished_at = NOW()
                WHERE id = %s
            """, (saved, len(cluster_labels), message, next_job_id, job_id))
        conn.commit()
        print(f"[NLP Worker] Job {job_id} done — {saved} reports, {len(cluster_labels)} clusters."
              + (f" Chained job {next_job_id}." if next_job_id else ""))

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
