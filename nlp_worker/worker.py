#!/usr/bin/env python3
"""
RAYD — ORU NLP Worker
Standalone medspaCy batch processor. Runs as a separate Docker container
so medspaCy's RAM footprint and native deps are isolated from the main app.

Polls hl7_oru_reports every 60 seconds, processes unanalyzed rows in chunks,
writes results to hl7_oru_analysis.

Also keeps the TF-IDF/K-means analysis (ai_nlp_cache) up to date on its own
(run_scoring), and polls oru_nlp_jobs every few seconds for "Rebuild clusters"
requests from /oru/nlp/process (routes/oru_analytics.py), so the main app never
blocks a request thread on it.
"""
import os
import sys
import time
import json
import re
import hashlib
import secrets
from collections import deque
from datetime import datetime
import psycopg2
import psycopg2.errors
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
# v4 = "no interval change in X" no longer negates X (2026-10-07).
_NLP_MODEL_VERSION = 'medspacy-v4'

# ai_nlp_cache.nlp_version (migration 0059). Rows with any other value are
# recomputed by the next on-demand job; rows with none (written before negation
# was resolved) are also hidden from the ORU page.
_CLUSTER_VERSION = 'tfidf-negation-v3'   # v2 = medspacy-v3 cues, v3 = medspacy-v4

_CHUNK           = 500
_BATCH_LIMIT      = 2000
_POLL_SECONDS      = 60
_JOB_POLL_SECONDS  = 5


# ── medspaCy ──────────────────────────────────────────────────────────────────

_NLP = None
_NLP_WORKERS = max(1, int((os.cpu_count() or 4) * 0.75))


# "No interval change in the lung mass", "Pas de modification de l'épanchement":
# the finding is still there, but "no" / "pas de" negated it, so a known mass or
# effusion vanished from the critical findings log. As PSEUDO rules these phrases
# win over the shorter cue inside them (the matcher keeps the longest match),
# modify nothing, and end the scope of an earlier cue in the same sentence.
_NO_CHANGE_PHRASES = [
    f"{neg} {adj}{noun}"
    for neg in ("no", "without", "no evidence of")
    for adj in ("", "interval ", "significant ", "significant interval ", "appreciable ", "definite ")
    for noun in ("change", "changes", "increase", "decrease", "growth", "progression", "worsening")
] + [
    f"{neg} {noun}"
    for neg in ("pas de", "pas d'", "sans")
    for noun in ("modification", "modifications", "changement", "évolution", "aggravation",
                 "augmentation", "majoration", "progression")
]


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
        # PyRuSH, medspaCy's sentence splitter, logs every sentence of every report
        # at DEBUG through loguru by default: full report text in the container log.
        # Keep only warnings and errors.
        try:
            from loguru import logger as _loguru
            _loguru.remove()
            _loguru.add(sys.stderr, level='WARNING')
        except ImportError:
            pass

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

        # French cues stay: Mazloum's feed is bilingual. "non" is deliberately NOT
        # one of them: spaCy splits "Non-contrast" into "Non" + "-" + "contrast",
        # so a FORWARD "non" rule negated every finding after "Non contrast CT"
        # ("Non contrast CT of the head demonstrates acute hemorrhage" -> nothing).
        # Same fix as HL7 c2691fcf.
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
        context.add([
            ConTextRule(p, "NEGATED_EXISTENCE", direction="PSEUDO") for p in _NO_CHANGE_PHRASES
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


# ── CRN marker detection ──────────────────────────────────────────────────────
# A radiologist writes the agreed marker (settings.crn_marker, default "CRN") in a
# report; each such report gets one crn_notifications row plus a 'detected' event.
# Only reports received at or after settings.crn_live_since count (empty = CRN off),
# so switching CRN on, or re-analysing old reports, never pages about past studies.

_REF_ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'   # no 0/O/1/I: read aloud safely


def _crn_ref_code():
    pick = lambda n: ''.join(secrets.choice(_REF_ALPHABET) for _ in range(n))
    return f'CRN-{pick(4)}-{pick(4)}'


def _crn_settings(conn):
    """(live_since, marker). live_since is None while CRN is off."""
    with conn.cursor() as cur:
        cur.execute("SELECT key, value FROM settings WHERE key IN ('crn_live_since', 'crn_marker')")
        cfg = dict(cur.fetchall())
    marker = (cfg.get('crn_marker') or '').strip()
    raw = (cfg.get('crn_live_since') or '').strip()
    if not raw or not marker:
        return None, marker
    try:
        return datetime.fromisoformat(raw), marker
    except ValueError:
        print(f"[NLP Worker] CRN off: crn_live_since {raw!r} is not an ISO timestamp.")
        return None, marker


def _detect_crn_markers(conn, rows):
    """Record each report in `rows` that carries the marker as a whole word, exact
    case. One notification per report (UNIQUE report_id), so a report analysed
    twice never notifies twice. Returns the number of new notifications."""
    live_since, marker = _crn_settings(conn)
    if live_since is None:
        return 0
    pattern = re.compile(r'(?<!\w)' + re.escape(marker) + r'(?!\w)')
    created = 0
    for r in rows:
        if r.received_at is None or r.received_at < live_since:
            continue
        text = r.impression_text or r.report_text or ''
        if not pattern.search(text):
            continue
        notification_id = None
        for _ in range(3):   # retry only on a ref_code collision (32^8 codes)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO crn_notifications
                            (ref_code, report_id, accession_number, patient_id, marker,
                             signing_radiologist, report_signed_at, report_received_at,
                             report_fingerprint)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (report_id) DO NOTHING
                        RETURNING id
                    """, (_crn_ref_code(), r.id, r.accession_number, r.patient_id, marker,
                          r.physician_id, r.result_datetime, r.received_at,
                          hashlib.sha256(text.encode('utf-8')).hexdigest()))
                    got = cur.fetchone()
                    if got:
                        notification_id = got[0]
                        cur.execute("""
                            INSERT INTO crn_events (notification_id, event_type, detail)
                            VALUES (%s, 'detected', %s)
                        """, (notification_id, json.dumps({
                            'marker': marker,
                            'report_received_at': r.received_at.isoformat(),
                        })))
                conn.commit()
                break
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
        if notification_id:
            created += 1
            print(f"[NLP Worker] CRN marker in report {r.id} (accession {r.accession_number}) "
                  f"-> notification {notification_id}")
    return created


# ── Batch processing (medspaCy negation-aware analysis) ───────────────────────

def run_batch():
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute("""
                SELECT r.id, r.impression_text, r.report_text,
                       r.accession_number, r.patient_id, r.physician_id,
                       r.result_datetime, r.received_at
                FROM   hl7_oru_reports r
                LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
                WHERE  a.id IS NULL
                ORDER  BY r.received_at DESC
                LIMIT  %s
            """, (_BATCH_LIMIT,))
            rows = cur.fetchall()

        if not rows:
            return

        # Before the NLP pass, so a medspaCy failure cannot delay a critical result.
        try:
            _detect_crn_markers(conn, rows)
        except Exception as e:
            conn.rollback()
            print(f"[NLP Worker] CRN marker detection error: {e}")

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


# ── TF-IDF/K-means analysis (ai_nlp_cache), automatic ────────────────────────
# run_scoring() keeps every report scored, newest first, against the current
# cluster model (oru_cluster_models). The model is refitted when there is none,
# when _CLUSTER_VERSION changes, on the first run of each calendar month, and when
# an admin asks (oru_nlp_jobs, "Rebuild clusters"). A refit makes every row stale,
# so the whole archive is re-scored in the background (~100 reports/s per core).

_FIT_SAMPLE           = 10000   # most recent reports the model is fitted on
_SCORE_BUDGET_SECONDS = 40      # per call, so primary analysis and jobs keep running

_MODEL_CACHE = {'id': None, 'model': None}


def _fit_cluster_model(conn, reason):
    """Fit a model on the most recent _FIT_SAMPLE reports and store it. Returns the
    new model id, or None when there is too little text."""
    print(f"[NLP Worker] Fitting cluster model ({reason})...")
    with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
        cur.execute("""
            SELECT impression_text, report_text FROM hl7_oru_reports
            WHERE report_text IS NOT NULL AND TRIM(report_text) != ''
            ORDER BY received_at DESC
            LIMIT %s
        """, (_FIT_SAMPLE,))
        rows = cur.fetchall()
    texts = [(r.impression_text or r.report_text or '').strip() for r in rows]
    data = clustering.fit_model(_negation_resolved_texts(texts))
    if data is None:
        print("[NLP Worker] Too little report text to fit a cluster model.")
        return None
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO oru_cluster_models
                (nlp_version, sample_size, terms, idf, centroids, labels)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
            RETURNING id
        """, (_CLUSTER_VERSION, len(texts), json.dumps(data['terms']), json.dumps(data['idf']),
              json.dumps(data['centroids']), json.dumps(data['labels'])))
        model_id = cur.fetchone()[0]
    conn.commit()
    print(f"[NLP Worker] Cluster model {model_id}: {len(data['centroids'])} clusters "
          f"from {len(texts)} reports. All reports will be re-scored in the background.")
    return model_id


def _current_model(conn):
    """(id, ClusterModel) of the model to score against, refitting first when it
    is missing, from an older _CLUSTER_VERSION, or from a previous month."""
    with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
        cur.execute("""
            SELECT id, nlp_version, fitted_at < date_trunc('month', NOW()) AS last_month
            FROM oru_cluster_models ORDER BY id DESC LIMIT 1
        """)
        latest = cur.fetchone()
    model_id = latest.id if latest else None
    if latest is None or latest.nlp_version != _CLUSTER_VERSION or latest.last_month:
        reason = ('no model yet' if latest is None
                  else 'analysis version changed' if latest.nlp_version != _CLUSTER_VERSION
                  else 'monthly rebuild')
        model_id = _fit_cluster_model(conn, reason) or model_id
    if model_id is None:
        return None, None
    if _MODEL_CACHE['id'] != model_id:
        with conn.cursor() as cur:
            cur.execute("SELECT terms, idf, centroids, labels FROM oru_cluster_models WHERE id = %s",
                        (model_id,))
            terms, idf, centroids, labels = cur.fetchone()
        _MODEL_CACHE.update(id=model_id, model=clustering.ClusterModel(
            {'terms': terms, 'idf': idf, 'centroids': centroids, 'labels': labels}))
    return model_id, _MODEL_CACHE['model']


def run_scoring():
    """Score pending reports for up to _SCORE_BUDGET_SECONDS. Returns True when it
    stopped on the time budget with more reports waiting."""
    conn = _get_conn()
    try:
        model_id, model = _current_model(conn)
        deadline = time.monotonic() + _SCORE_BUDGET_SECONDS
        while time.monotonic() < deadline:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                cur.execute("""
                    SELECT o.id, o.report_text, o.impression_text
                    FROM hl7_oru_reports o
                    LEFT JOIN ai_nlp_cache c ON c.source_id = o.id
                    WHERE o.report_text IS NOT NULL AND TRIM(o.report_text) != ''
                      AND (c.id IS NULL
                           OR c.nlp_version IS DISTINCT FROM %s
                           OR c.cluster_model_id IS DISTINCT FROM %s)
                    ORDER BY o.received_at DESC
                    LIMIT %s
                """, (_CLUSTER_VERSION, model_id, _CHUNK))
                rows = cur.fetchall()
            if not rows:
                return False

            texts = [(r.impression_text or r.report_text or '').strip() for r in rows]
            resolved = _negation_resolved_texts(texts)
            values = []
            for r, t, res in zip(rows, texts, resolved):
                s = clustering.score_report(t, res, model)
                values.append((r.id, s['classification'], json.dumps(s['keywords']), s['cluster_id'],
                               s['cluster_label'], s['severity_score'], _CLUSTER_VERSION, model_id))
            _write_scores(conn, values)
        return True
    finally:
        conn.close()


_UPSERT_SCORE = """
    INSERT INTO ai_nlp_cache
        (source_id, classification, keywords, cluster_id, cluster_label,
         severity_score, nlp_version, cluster_model_id, processed_at)
    VALUES %s
    ON CONFLICT (source_id) DO UPDATE SET
        classification   = EXCLUDED.classification,
        keywords         = EXCLUDED.keywords,
        cluster_id       = EXCLUDED.cluster_id,
        cluster_label    = EXCLUDED.cluster_label,
        severity_score   = EXCLUDED.severity_score,
        nlp_version      = EXCLUDED.nlp_version,
        cluster_model_id = EXCLUDED.cluster_model_id,
        processed_at     = NOW()
"""
_SCORE_TEMPLATE = "(%s, %s, %s::jsonb, %s, %s, %s, %s, %s, NOW())"


def _write_scores(conn, values):
    """One statement per chunk; if it fails, row by row so one bad row cannot
    hold back the rest of the chunk."""
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _UPSERT_SCORE, values, template=_SCORE_TEMPLATE)
        conn.commit()
        return
    except Exception as e:
        conn.rollback()
        print(f"[NLP Worker] ai_nlp_cache chunk write failed ({e}); retrying row by row.")
    for v in values:
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _UPSERT_SCORE, [v], template=_SCORE_TEMPLATE)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[NLP Worker] ai_nlp_cache row {v[0]} error: {e}")


# ── "Rebuild clusters" requests (oru_nlp_jobs) ────────────────────────────────

def run_pending_jobs():
    conn = _get_conn()
    job = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute("""
                SELECT id FROM oru_nlp_jobs
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
        _rebuild_job(conn, job.id)
    finally:
        conn.close()


def _rebuild_job(conn, job_id):
    try:
        model_id = _fit_cluster_model(conn, f'requested, job {job_id}')
        if model_id is None:
            status, message = 'done', 'Too little report text to build clusters.'
            clusters = 0
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT jsonb_array_length(centroids) FROM oru_cluster_models WHERE id = %s",
                            (model_id,))
                clusters = cur.fetchone()[0]
            status = 'done'
            message = (f'Clusters rebuilt: {clusters} clusters. Every report is being re-scored '
                       f'in the background, newest first.')
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE oru_nlp_jobs
                SET status = %s, processed_count = 0, cluster_count = %s,
                    message = %s, finished_at = NOW()
                WHERE id = %s
            """, (status, clusters, message, job_id))
        conn.commit()
        print(f"[NLP Worker] Job {job_id}: {message}")
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
    so it is a no-op once they are gone."""
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


# Reports a later model reads differently: (stale versions, Postgres regex on the
# analysed text, what changed). Broad on purpose -- re-analysing a report that
# comes out the same costs one more pass; missing one leaves a wrong row in the
# critical findings log.
_REQUEUES = [
    (['medspacy-v1', 'medspacy-v2'],
     r"’|\m(pas|absence)\s+d\s*'|\mexcluded\M|\mmais\M",
     'v3 negation cues'),
    (['medspacy-v1', 'medspacy-v2', 'medspacy-v3'],
     r"\m(no|without|pas|sans)\M[^.;\n]{0,40}\m(change|changes|increase|decrease|growth|progression|"
     r"worsening|modifications?|changement|[ée]volution|aggravation|augmentation|majoration)\M",
     'v4 "no change" phrases'),
]


def _requeue_changed_reports():
    """Drop analysis rows that a later model reads differently (_REQUEUES), so
    run_batch re-analyses them and the critical findings log is corrected both
    ways. Skipped while medspaCy is down: the rule-based fallback lacks these
    rules and would stamp the old result with the new version. Touches stale
    versions only, so it is a no-op once they are gone."""
    if _NLP is None:
        print("[NLP Worker] medspaCy not loaded -- requeue postponed to the next start.")
        return
    conn = _get_conn()
    try:
        for versions, pattern, what in _REQUEUES:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM hl7_oru_analysis a
                    USING  hl7_oru_reports r
                    WHERE  a.report_id = r.id
                      AND  a.nlp_version = ANY(%s)
                      AND  COALESCE(NULLIF(r.impression_text, ''), r.report_text, '') ~* %s
                """, (versions, pattern))
                requeued = cur.rowcount
            conn.commit()
            if requeued:
                print(f"[NLP Worker] Requeued {requeued} report(s) for the {what}.")
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
        _requeue_changed_reports()
    except Exception as e:
        print(f"[NLP Worker] Requeue of reports for the new rules failed: {e}")
    print(f"[NLP Worker] Polling jobs every {_JOB_POLL_SECONDS}s, "
          f"medspaCy batch and TF-IDF scoring every {_POLL_SECONDS}s.")

    # Wall-clock schedule, not tick counting: while a scoring backlog keeps each
    # loop busy for _SCORE_BUDGET_SECONDS, run_batch (critical findings) must
    # still run every _POLL_SECONDS.
    next_batch = 0.0
    backlog = False
    while True:
        try:
            run_pending_jobs()
        except Exception as e:
            print(f"[NLP Worker] Job poll error: {e}")

        due = time.monotonic() >= next_batch
        if due:
            next_batch = time.monotonic() + _POLL_SECONDS
            try:
                run_batch()
            except Exception as e:
                print(f"[NLP Worker] Batch error: {e}")

        if due or backlog:
            try:
                backlog = run_scoring()
            except Exception as e:
                backlog = False
                print(f"[NLP Worker] Scoring error: {e}")

        time.sleep(_JOB_POLL_SECONDS)


if __name__ == '__main__':
    main()
