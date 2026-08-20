import re
import time
import json
import hashlib
from collections import Counter, deque
from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from sqlalchemy import and_, text
from db import db

oru_bp = Blueprint('oru', __name__, url_prefix='/oru')

# ── Stop-word list ─────────────────────────────────────────────────────────────
STOP = {
    # English
    'the','a','an','and','or','but','in','on','at','to','for','of','with',
    'is','are','was','were','be','been','being','have','has','had','do',
    'does','did','will','would','could','should','may','might','can','not',
    'no','nor','so','yet','both','either','neither','each','few','more',
    'most','other','some','such','than','too','very','just','as','until',
    'while','if','then','that','this','these','those','it','its','also',
    'there','their','they','from','by','about','into','through','during',
    'above','below','between','out','off','over','under','again','further',
    'all','any','both','each','own','same','than','s','t','re','ll','ve',
    # French
    'le','la','les','un','une','des','du','de','en','et','ou','mais','donc',
    'or','ni','car','que','qui','quoi','dont','où','ce','cet','cette','ces',
    'mon','ton','son','ma','ta','sa','nos','vos','leur','leurs','mes','tes','ses',
    'je','tu','il','elle','nous','vous','ils','elles','me','te','se','lui',
    'sur','sous','dans','par','pour','avec','sans','entre','vers','chez',
    'plus','moins','très','bien','pas','peu','trop','tout','tous','toute','toutes',
    'est','sont','être','avoir','faire','dit','ainsi','lors','puis',
    'aux','au','aucun','aucune','autre','autres','même','comme',
    'cela','ceci','celui','celle','ceux','celles','ici','là',
    'après','avant','pendant','depuis','quand','comment','pourquoi',
    'aussi','encore','toujours','jamais','rien','chaque','chacun',
    # Radiology boilerplate
    'findings','finding','noted','note','seen','identified','demonstrated',
    'shows','shown','appear','appears','within','without','normal','limits',
    'unremarkable','study','examination','image','images','view','views',
    'patient','clinical','indication','technique','comparison','no','noted',
    'exam','report','result','results','history','correlation','please',
    'however','additionally','furthermore','consistent','consistent','level',
    'mild','moderate','severe','significant','evidence','acute','chronic',
    'bilateral','unilateral','right','left','upper','lower','middle','mid',
    'anterior','posterior','medial','lateral','superior','inferior','present',
}

# ── Critical keyword groups ────────────────────────────────────────────────────
CRITICAL = [
    'pneumothorax','hemorrhage','haemorrhage','haematoma','hematoma',
    'pulmonary embolism','aortic dissection','stroke','infarct','infarction',
    'fracture','mass','malignancy','malignant','tumor','tumour','carcinoma',
    'thrombosis','obstruction','perforation','rupture','aneurysm','abscess',
    'appendicitis','ischemia','ischaemia','neoplasm','metastasis','metastases',
    'occlusion','stenosis','dissection','embolism','pneumonia','effusion',
]

def _get_all_critical_keywords():
    """Return CRITICAL list merged with any custom keywords stored in settings."""
    try:
        rows = db.session.execute(
            text("SELECT key FROM settings WHERE key LIKE 'oru_crit:%'")
        ).fetchall()
        custom = [r[0][len('oru_crit:'):].lower() for r in rows]
    except Exception:
        custom = []
    return list(set(CRITICAL) | set(custom))

# ── Normal classifiers ────────────────────────────────────────────────────────
NORMAL_PHRASES = [
    'no acute','unremarkable','within normal','normal study',
    'no significant','no abnormality','no evidence of acute',
    'no pathological','no active disease','normal limits',
]
_NORMAL_PATTERN = '|'.join(re.escape(p) for p in NORMAL_PHRASES)

# ── Multi-pattern matching (Aho-Corasick) ─────────────────────────────────────
# The rule-based negation fallback used to run one independent str.find() sweep
# per phrase (~150 phrases x up to 8000 chars). A single combined regex
# alternation would be faster but only reports non-overlapping matches, which
# silently drops shorter phrases nested inside longer ones (e.g. the CRITICAL
# keyword "effusion" inside the DIAGNOSES phrase "pleural effusion") -- a real
# risk for a clinical critical-findings feed. Aho-Corasick finds every
# occurrence of every pattern, including overlapping ones, in one O(text
# length) pass, so it's a strict speedup with no change in what gets matched.
# (Mirrored in nlp_worker/worker.py -- same duplication convention as the rest
# of the negation/vocabulary logic between the two files.)

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


# ── Rule-based negation fallback ─────────────────────────────────────────────
# Used when medspacy is unavailable. Checks a backward character window within
# the same sentence for any negation prefix before the matched keyword.
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

def _any_unnegated(t, keyword):
    """Used for the small, per-request custom-keyword search (settings-driven
    'oru_crit:*' list) -- not the ~150-phrase DIAGNOSES/CRITICAL vocabulary,
    which goes through the Aho-Corasick automaton below instead."""
    idx = t.find(keyword)
    while idx != -1:
        if not _is_negated(t, idx):
            return True
        idx = t.find(keyword, idx + len(keyword))
    return False


# ── Diagnosis vocabulary — DB-configurable (oru_diagnosis_vocabulary, migration
# 0048) instead of a hardcoded constant, so admins can add/edit mappings without
# a code change. Cached in-process with a short TTL since this is looked up on
# every /oru/data request. rule_version is derived from the vocabulary content
# itself (not a manually-bumped constant), so hl7_oru_rule_cache rows are
# automatically treated as stale whenever the vocabulary changes.

_diag_cache = {'rows': None, 'benign': None, 'automaton': None, 'version': None, 'loaded_at': 0}
_DIAG_TTL = 300  # seconds

def _get_diagnoses():
    """Returns (diagnoses, benign_labels, automaton, rule_version)."""
    now = time.time()
    if _diag_cache['rows'] is None or now - _diag_cache['loaded_at'] > _DIAG_TTL:
        try:
            rows = db.session.execute(text("""
                SELECT phrase, canonical_label, is_benign
                FROM oru_diagnosis_vocabulary
                WHERE active = TRUE
                ORDER BY id
            """)).fetchall()
        except Exception:
            rows = []
        diagnoses = [(r.phrase, r.canonical_label) for r in rows]
        benign = {r.canonical_label for r in rows if r.is_benign}
        all_phrases = sorted({p for p, _ in diagnoses} | set(CRITICAL))
        version_src = '|'.join(all_phrases).encode('utf-8')
        _diag_cache.update(
            rows=diagnoses,
            benign=benign,
            automaton=_AhoCorasick(all_phrases),
            version='rule-v2:' + hashlib.md5(version_src).hexdigest()[:12],
            loaded_at=now,
        )
    return _diag_cache['rows'], _diag_cache['benign'], _diag_cache['automaton'], _diag_cache['version']


def _invalidate_diagnoses_cache():
    _diag_cache['loaded_at'] = 0


def _affirmed_phrases_rule_based(t, automaton):
    """Single Aho-Corasick pass over the text; the negation-window check still
    runs per match (a phrase is affirmed if ANY of its occurrences is
    unnegated -- same semantics as the old per-phrase str.find loop)."""
    found = set()
    for pos, phrase in automaton.find_all(t):
        if phrase in found:
            continue
        if not _is_negated(t, pos):
            found.add(phrase)
    return found


def _affirmed_phrases(text):
    """Single-text rule-based lookup. Batch NLP is handled by the nlp-worker container."""
    if not text:
        return set()
    _, _, automaton, _ = _get_diagnoses()
    t = text.lower()[:8000]
    return _affirmed_phrases_rule_based(t, automaton)


def _affirmed_phrases_batch(texts):
    """
    Rule-based fallback for any reports not yet processed by the nlp-worker container.
    The worker handles medspaCy; this keeps the main app free of that dependency.
    """
    if not texts:
        return []
    _, _, automaton, _ = _get_diagnoses()
    def _rb(t):
        t = (t or '').lower()[:8000]
        return _affirmed_phrases_rule_based(t, automaton)
    return [_rb(t) for t in texts]


# Deduplicate: for each canonical label keep count of reports mentioning it
# (multiple phrases mapping to same label are OR'd per report, not summed)
def _count_diagnoses(affirmed_list, top_n=50):
    """
    Count diagnosis labels across a list of pre-computed affirmed-phrase sets.
    Expects output of _affirmed_phrases_batch() or all_affirmed built in oru_data().
    top_n=None returns every counted label (used when the caller merges these
    counts into a larger aggregate before truncating).
    """
    diagnoses, _, _, _ = _get_diagnoses()
    label_counts = Counter()
    for affirmed in affirmed_list:
        if not affirmed:
            continue
        seen_labels = set()
        for phrase, label in diagnoses:
            if label in seen_labels:
                continue
            if phrase in affirmed or label in affirmed:
                seen_labels.add(label)
                label_counts[label] += 1
    items = label_counts.most_common(top_n) if top_n else label_counts.most_common()
    return [{'word': label, 'count': cnt} for label, cnt in items]


def _tokenize(text):
    """Lowercase, extract Unicode letter sequences ≥ 3 chars, remove stop words.
    Supports French accented characters (é, è, ê, à, â, ç, œ, etc.)."""
    if not text:
        return []
    words = re.findall(r"[^\W\d_]+", text.lower(), re.UNICODE)
    return [w for w in words if len(w) >= 3 and w not in STOP]


# ── Section parser ─────────────────────────────────────────────────────────────
_SEC_PATTERNS = [
    ('technique',   re.compile(r'(?im)^\s*(technique[s]?|protocole|acquisition)\s*:?[ \t]*$')),
    ('findings',    re.compile(r'(?im)^\s*(r[eé]sultat[s]?|description|findings?|compte[- ]rendu|constatations?|analyse)\s*:?[ \t]*$')),
    ('conclusion',  re.compile(r'(?im)^\s*(conclusion[s]?|impression[s]?|avis|synth[eè]se|diagnostic|interpr[eé]tation)\s*:?[ \t]*$')),
    # Inline headers: "TECHNIQUE: blah blah"
    ('technique',   re.compile(r'(?im)^\s*(technique[s]?|protocole)\s*:\s*(?=\S)')),
    ('findings',    re.compile(r'(?im)^\s*(r[eé]sultat[s]?|description|findings?|compte[- ]rendu)\s*:\s*(?=\S)')),
    ('conclusion',  re.compile(r'(?im)^\s*(conclusion[s]?|impression[s]?|avis|diagnostic)\s*:\s*(?=\S)')),
]

def _parse_sections(text):
    """
    Split a radiology report into technique / findings / conclusion.
    Falls back to putting everything in 'findings' when no headers are found.
    Returns dict with keys: technique, findings, conclusion (all stripped strings).
    """
    if not text:
        return {'technique': '', 'findings': '', 'conclusion': ''}

    markers = []  # (char_pos, content_start, section_key)
    for key, pat in _SEC_PATTERNS:
        for m in pat.finditer(text):
            markers.append((m.start(), m.end(), key))

    if not markers:
        return {'technique': '', 'findings': text.strip(), 'conclusion': ''}

    markers.sort(key=lambda x: x[0])
    result = {'technique': '', 'findings': '', 'conclusion': ''}
    for i, (_, content_start, key) in enumerate(markers):
        next_pos = markers[i + 1][0] if i + 1 < len(markers) else len(text)
        chunk = text[content_start:next_pos].strip()
        if chunk and not result[key]:   # first match wins
            result[key] = chunk

    # If nothing landed in findings, fall back to full text
    if not result['findings'] and not result['technique'] and not result['conclusion']:
        result['findings'] = text.strip()
    return result


def _best_text(row):
    """Return the most meaningful text from a report row, stripping whitespace."""
    imp = (row.impression_text or '').strip()
    rep = (row.report_text or '').strip()
    return imp or rep


# ── Shared date-range / procedure-code filter (item 4/5 consolidation) ────────

def _date_proc_conditions(date_from, date_to, proc, alias, days_default=30, days_cap=365):
    """
    Build an already-aliased, parameterized WHERE clause for the date-range /
    procedure-code filter shared by /data, /section-gaps, /sections, and
    /nlp/results. Returns (clause_str, params, days).

    Conditions are correctly table-qualified from construction (no post-hoc
    string surgery like the old `.replace('received_at', 'r.received_at')`
    patch), and combined via SQLAlchemy's and_() rather than a manual
    ' AND '.join() of raw fragments.
    """
    conditions, params = [], {}
    if date_from and date_to:
        conditions.append(text(
            f"COALESCE({alias}.result_datetime, {alias}.received_at) "
            f"BETWEEN :date_from AND (CAST(:date_to AS DATE) + INTERVAL '1 day')"
        ))
        params['date_from'] = date_from
        params['date_to'] = date_to
        days = None
    else:
        days = min(int(request.args.get('days', days_default)), days_cap)
        conditions.append(text(
            f"COALESCE({alias}.result_datetime, {alias}.received_at) "
            f">= NOW() - CAST(:interval AS INTERVAL)"
        ))
        params['interval'] = f'{days} days'
    if proc:
        conditions.append(text(f"UPPER(TRIM({alias}.procedure_code)) = UPPER(:proc)"))
        params['proc'] = proc
    return str(and_(*conditions)), params, days


def _oru_report_ids(where_clause, params, limit=None, offset=0):
    """Ordered list of hl7_oru_reports.id (most recent first) matching a filter
    already built against the 'r' alias. Used to drive a bounded detail fetch
    instead of pulling every matching row's full columns."""
    p = dict(params, offset=offset)
    limit_sql = ""
    if limit:
        p['limit'] = limit
        limit_sql = "LIMIT :limit"
    rows = db.session.execute(text(f"""
        SELECT r.id FROM hl7_oru_reports r
        WHERE {where_clause}
        ORDER BY r.received_at DESC
        {limit_sql} OFFSET :offset
    """), p).fetchall()
    return [row.id for row in rows]


# ── Routes ─────────────────────────────────────────────────────────────────────

@oru_bp.route('/')
@login_required
def oru_page():
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'oru'):
        from flask import abort
        abort(403)
    procedures = db.session.execute(text("""
        SELECT DISTINCT
            UPPER(TRIM(procedure_code)) AS code,
            INITCAP(LOWER(TRIM(procedure_name))) AS name
        FROM hl7_oru_reports
        WHERE procedure_code IS NOT NULL AND TRIM(procedure_code) != ''
        ORDER BY name
    """)).fetchall()
    # go_live_date: the earliest HL7 ORU report ever received -- used as the page's
    # default date-range start (operator instruction, 2026-08-01: default start date
    # should be the integration's go-live date, not a rolling last-30-days window).
    min_date_row = db.session.execute(text(
        "SELECT MIN(received_at)::date AS d FROM hl7_oru_reports"
    )).fetchone()
    go_live_date = min_date_row.d.isoformat() if min_date_row and min_date_row.d else None
    return render_template('oru_analytics.html', procedures=procedures, go_live_date=go_live_date)


@oru_bp.route('/data')
@login_required
def oru_data():
    proc    = request.args.get('proc', '').strip()
    top_n   = int(request.args.get('top', 40))
    limit   = min(int(request.args.get('limit', 200)), 1000)
    offset  = max(int(request.args.get('offset', 0)), 0)

    date_from = request.args.get('date_from', '').strip()
    date_to   = request.args.get('date_to', '').strip()
    # Filter by the report's actual date (result_datetime), not received_at, which is
    # only an ingestion timestamp and can drift from when the report actually
    # happened if a row is ever re-touched. received_at is only the fallback for
    # the rare row missing result_datetime entirely.
    where_clause, params, days = _date_proc_conditions(date_from, date_to, proc, alias='r')

    from utils.audit import log_event
    log_event('oru_accessed', category='report', resource_type='oru_analytics',
              detail={'days': days, 'proc': proc or None})

    diagnoses, benign_labels, automaton, rule_version = _get_diagnoses()

    # ── Aggregates over the FULL filtered range — no row materialization ─────
    agg_row = db.session.execute(text(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(r.impression_text), ''), r.report_text) ~* :normal_pattern
            ) AS normal
        FROM hl7_oru_reports r
        WHERE {where_clause}
    """), {**params, 'normal_pattern': _NORMAL_PATTERN}).fetchone()
    total = agg_row.total or 0
    normal_count = agg_row.normal or 0
    abnormal_count = total - normal_count

    modality_rows = db.session.execute(text(f"""
        SELECT UPPER(COALESCE(NULLIF(TRIM(r.modality), ''), NULLIF(TRIM(ho.modality), ''), 'UNK')) AS modality,
               COUNT(*) AS cnt
        FROM hl7_oru_reports r
        LEFT JOIN LATERAL (
            SELECT modality FROM hl7_orders
            WHERE accession_number = r.accession_number
              AND modality IS NOT NULL AND TRIM(modality) != ''
            LIMIT 1
        ) ho ON true
        WHERE {where_clause}
        GROUP BY 1
        ORDER BY cnt DESC
    """), params).fetchall()
    modalities = [{'modality': row.modality, 'count': row.cnt} for row in modality_rows]

    proc_rows = db.session.execute(text(f"""
        SELECT UPPER(TRIM(r.procedure_code)) AS code,
               COALESCE(NULLIF(TRIM(r.procedure_name), ''), TRIM(r.procedure_code), 'Unknown') AS name,
               COUNT(*) AS cnt
        FROM hl7_oru_reports r
        WHERE {where_clause} AND r.procedure_code IS NOT NULL AND TRIM(r.procedure_code) != ''
        GROUP BY 1, 2
        ORDER BY cnt DESC
        LIMIT 10
    """), params).fetchall()
    top_procs = [{'code': row.code, 'name': row.name, 'count': row.cnt} for row in proc_rows]

    phys_rows = db.session.execute(text(f"""
        SELECT TRIM(r.physician_id) AS pid, COUNT(*) AS cnt
        FROM hl7_oru_reports r
        WHERE {where_clause} AND r.physician_id IS NOT NULL AND TRIM(r.physician_id) != ''
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 10
    """), params).fetchall()
    physicians = [{'id': row.pid, 'count': row.cnt} for row in phys_rows]

    # ── Diagnosis frequency (word cloud) — SQL aggregate over already-analyzed
    # reports (hl7_oru_analysis.affirmed_labels never contains benign labels —
    # the worker skips them on write) plus the existing rule-cache/live-fallback
    # path, scoped ONLY to the reports neither table has analysis for yet (a
    # backlog bounded by ingestion rate, not by how wide the date filter is).
    analyzed_label_rows = db.session.execute(text(f"""
        SELECT label, COUNT(DISTINCT report_id) AS cnt FROM (
            SELECT r.id AS report_id, unnest(a.affirmed_labels) AS label
            FROM hl7_oru_reports r
            JOIN hl7_oru_analysis a ON a.report_id = r.id
            WHERE {where_clause}
        ) x
        GROUP BY label
    """), params).fetchall()
    label_counts = Counter({row.label: row.cnt for row in analyzed_label_rows})

    pending_rows = db.session.execute(text(f"""
        SELECT r.id AS report_id, r.report_text, r.impression_text
        FROM hl7_oru_reports r
        LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
        WHERE {where_clause} AND a.id IS NULL
    """), params).fetchall()

    if pending_rows:
        pending_ids = [row.report_id for row in pending_rows]
        try:
            cached_rows = db.session.execute(text("""
                SELECT report_id, affirmed_labels FROM hl7_oru_rule_cache
                WHERE report_id = ANY(:ids) AND rule_version = :ver
            """), {'ids': pending_ids, 'ver': rule_version}).fetchall()
            cached_by_id = {row.report_id: set(row.affirmed_labels) for row in cached_rows}
        except Exception:
            cached_by_id = {}

        pending_affirmed = []
        to_compute = [row for row in pending_rows if row.report_id not in cached_by_id]
        pending_affirmed.extend(cached_by_id.values())

        if to_compute:
            try:
                texts = [_best_text(row) for row in to_compute]
                computed = _affirmed_phrases_batch(texts)
            except Exception:
                computed = None
            if computed:
                pending_affirmed.extend(computed)
                try:
                    for row, affirmed in zip(to_compute, computed):
                        db.session.execute(text("""
                            INSERT INTO hl7_oru_rule_cache (report_id, affirmed_labels, rule_version, computed_at)
                            VALUES (:rid, :labels, :ver, NOW())
                            ON CONFLICT (report_id) DO UPDATE SET
                                affirmed_labels = EXCLUDED.affirmed_labels,
                                rule_version    = EXCLUDED.rule_version,
                                computed_at     = NOW()
                        """), {"rid": row.report_id, "labels": list(affirmed), "ver": rule_version})
                    db.session.commit()
                except Exception:
                    db.session.rollback()  # best-effort cache write — response is unaffected

        if pending_affirmed:
            for item in _count_diagnoses(pending_affirmed, top_n=None):
                label_counts[item['word']] += item['count']

    cloud_words = [{'word': label, 'count': cnt} for label, cnt in label_counts.most_common(top_n)]

    # ── Paginated detail rows — critical findings log + report detail ────────
    ids = _oru_report_ids(where_clause, params, limit=limit, offset=offset)

    detail_rows = []
    if ids:
        detail_rows = db.session.execute(text("""
            SELECT r.id AS report_id, r.procedure_code, r.procedure_name,
                   COALESCE(NULLIF(TRIM(r.modality), ''), NULLIF(TRIM(ho.modality), ''), 'UNK') AS modality,
                   r.physician_id,
                   r.patient_id, r.accession_number,
                   r.report_text, r.impression_text, r.result_datetime, r.received_at,
                   a.affirmed_labels
            FROM   hl7_oru_reports r
            LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
            LEFT JOIN LATERAL (
                SELECT modality FROM hl7_orders
                WHERE accession_number = r.accession_number
                  AND modality IS NOT NULL AND TRIM(modality) != ''
                LIMIT 1
            ) ho ON true
            WHERE r.id = ANY(:ids)
            ORDER  BY r.received_at DESC
        """), {'ids': ids}).fetchall()

    # ── Build affirmed-label sets for the paginated set — stored analysis first,
    # then the rule cache, only falling all the way back to live computation for
    # reports neither has yet (see migration 0046 for why the cache exists).
    analyzed_affirmed = {
        i: set(row.affirmed_labels)
        for i, row in enumerate(detail_rows)
        if row.affirmed_labels is not None
    }
    detail_pending_indices = [i for i, row in enumerate(detail_rows) if row.affirmed_labels is None]

    if detail_pending_indices:
        try:
            cached_rows = db.session.execute(text("""
                SELECT report_id, affirmed_labels FROM hl7_oru_rule_cache
                WHERE report_id = ANY(:ids) AND rule_version = :ver
            """), {
                "ids": [detail_rows[i].report_id for i in detail_pending_indices],
                "ver": rule_version,
            }).fetchall()
            cached_by_id = {row.report_id: set(row.affirmed_labels) for row in cached_rows}
        except Exception:
            cached_by_id = {}

        for i in detail_pending_indices:
            if detail_rows[i].report_id in cached_by_id:
                analyzed_affirmed[i] = cached_by_id[detail_rows[i].report_id]

        to_compute = [i for i in detail_pending_indices if detail_rows[i].report_id not in cached_by_id]
        computed = None
        if to_compute:
            try:
                texts    = [_best_text(detail_rows[i]) for i in to_compute]
                computed = _affirmed_phrases_batch(texts)
                for i, affirmed in zip(to_compute, computed):
                    analyzed_affirmed[i] = affirmed
            except Exception:
                computed = None

        if computed:
            try:
                for i, affirmed in zip(to_compute, computed):
                    db.session.execute(text("""
                        INSERT INTO hl7_oru_rule_cache (report_id, affirmed_labels, rule_version, computed_at)
                        VALUES (:rid, :labels, :ver, NOW())
                        ON CONFLICT (report_id) DO UPDATE SET
                            affirmed_labels = EXCLUDED.affirmed_labels,
                            rule_version    = EXCLUDED.rule_version,
                            computed_at     = NOW()
                    """), {"rid": detail_rows[i].report_id, "labels": list(affirmed), "ver": rule_version})
                db.session.commit()
            except Exception:
                db.session.rollback()

    detail_affirmed = [analyzed_affirmed.get(i, set()) for i in range(len(detail_rows))]

    # ── Critical findings (most recent 20 within the paginated set) ─────────
    custom_kws = _get_all_critical_keywords()
    critical_log = []
    for r, affirmed in zip(detail_rows, detail_affirmed):
        seen, hits = set(), []
        for phrase, label in diagnoses:
            if label in benign_labels or label in seen:
                continue
            if phrase in affirmed or label in affirmed:
                seen.add(label)
                hits.append(label)
        # Custom keywords: real-time text search — not stored in analysis table
        if not hits and custom_kws:
            tl = (_best_text(r) or '').lower()
            hits = [kw for kw in custom_kws if _any_unnegated(tl, kw)]
        if hits:
            critical_log.append({
                'procedure_code':   (r.procedure_code or '—').upper().strip(),
                'procedure':        (r.procedure_name or r.procedure_code or '—').strip(),
                'modality':         (r.modality or '—').upper(),
                'keywords':         hits[:5],
                'patient_id':       r.patient_id or '—',
                'accession_number': r.accession_number or '—',
                'date':             r.result_datetime.strftime('%Y-%m-%d') if r.result_datetime else (
                                    r.received_at.strftime('%Y-%m-%d') if r.received_at else '—'),
                'physician_id':     r.physician_id or '—',
                'received_at':      r.received_at.strftime('%Y-%m-%d %H:%M') if r.received_at else '—',
                'report_text':      (r.impression_text or r.report_text or '').strip(),
            })
    critical_log = critical_log[:20]

    return jsonify({
        'total':          total,
        'normal':         normal_count,
        'abnormal':       abnormal_count,
        'cloud':          cloud_words,
        'modalities':     modalities,
        'top_procs':      top_procs,
        'critical_log':   critical_log,
        'physicians':     physicians,
        'days':           days,
        'limit':          limit,
        'offset':         offset,
        'returned':       len(detail_rows),
    })


# ── Section gap audit ─────────────────────────────────────────────────────────

@oru_bp.route('/section-gaps')
@login_required
def oru_section_gaps():
    """
    For each report, parse sections and flag which are empty.
    Returns per-section counts and a per-physician breakdown for manager export.
    """
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'oru'):
        from flask import abort
        abort(403)

    proc   = request.args.get('proc', '').strip()

    date_from = request.args.get('date_from', '').strip()
    date_to   = request.args.get('date_to', '').strip()
    # See oru_data()'s comment: filter by result_datetime, the report's real date.
    where_clause, params, days = _date_proc_conditions(date_from, date_to, proc, alias='hl7_oru_reports')

    rows = db.session.execute(text(
        f"""SELECT physician_id, procedure_code, procedure_name,
                   report_text, impression_text,
                   to_char(received_at, 'YYYY-MM-DD HH24:MI') AS received_at
            FROM hl7_oru_reports WHERE {where_clause}
            ORDER BY COALESCE(result_datetime, received_at) DESC"""
    ), params).fetchall()

    total = len(rows)

    # {physician: count} per missing section
    empty_tech  = Counter()
    empty_find  = Counter()
    empty_concl = Counter()

    for r in rows:
        txt  = _best_text(r)
        sec  = _parse_sections(txt)
        phys = (r.physician_id or 'UNKNOWN').strip()
        if not sec['technique']:
            empty_tech[phys]  += 1
        if not sec['findings']:
            empty_find[phys]  += 1
        if not sec['conclusion']:
            empty_concl[phys] += 1

    def _list(counter):
        return [{'physician': p, 'count': c} for p, c in counter.most_common()]

    return jsonify({
        'total':              total,
        'empty_technique':    sum(empty_tech.values()),
        'empty_findings':     sum(empty_find.values()),
        'empty_conclusion':   sum(empty_concl.values()),
        'docs_empty_technique':  _list(empty_tech),
        'docs_empty_findings':   _list(empty_find),
        'docs_empty_conclusion': _list(empty_concl),
        'days': days,
    })


# ── Section frequency ─────────────────────────────────────────────────────────

@oru_bp.route('/sections')
@login_required
def oru_sections():
    """
    Parse every report into technique / findings / conclusion sections,
    then return the top token frequencies for each section as treemap data.
    """
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'oru'):
        from flask import abort
        abort(403)

    proc   = request.args.get('proc', '').strip()
    top_n  = 40

    date_from = request.args.get('date_from', '').strip()
    date_to   = request.args.get('date_to', '').strip()
    # See oru_data()'s comment: filter by result_datetime, not received_at.
    where_clause, params, _days = _date_proc_conditions(date_from, date_to, proc, alias='hl7_oru_reports')

    rows = db.session.execute(text(
        f"SELECT report_text, impression_text FROM hl7_oru_reports WHERE {where_clause}"
    ), params).fetchall()

    tech_counter   = Counter()
    find_counter   = Counter()
    concl_counter  = Counter()

    for r in rows:
        txt = _best_text(r)
        sec = _parse_sections(txt)
        tech_counter.update(_tokenize(sec['technique']))
        find_counter.update(_tokenize(sec['findings']))
        concl_counter.update(_tokenize(sec['conclusion']))

    def _top(counter):
        return [{'word': w, 'count': c} for w, c in counter.most_common(top_n)]

    return jsonify({
        'technique':   _top(tech_counter),
        'findings':    _top(find_counter),
        'conclusion':  _top(concl_counter),
    })


# ── NLP status ────────────────────────────────────────────────────────────────

@oru_bp.route('/nlp/status')
@login_required
def nlp_status():
    total = db.session.execute(
        text("SELECT COUNT(*) FROM hl7_oru_reports WHERE report_text IS NOT NULL")
    ).scalar() or 0

    processed = db.session.execute(
        text("SELECT COUNT(*) FROM ai_nlp_cache")
    ).scalar() or 0

    return jsonify({
        'total':     total,
        'processed': processed,
        'pending':   max(total - processed, 0),
    })


# ── NLP processing (on-demand, triggered by user, runs in the background) ─────
# The route only enqueues a job; nlp_worker/worker.py (the rayd_nlp container,
# already polling every 60s for medspaCy analysis) picks up pending rows and
# runs the actual TF-IDF/K-means clustering, so this request thread is never
# blocked on it.

@oru_bp.route('/nlp/process', methods=['POST'])
@login_required
def nlp_process():
    if current_user.role != 'admin':
        from flask import abort
        abort(403)

    data = request.get_json(force=True) or {}
    days = min(int(data.get('days', 90)), 365)

    row = db.session.execute(text("""
        INSERT INTO oru_nlp_jobs (status, days, requested_by)
        VALUES ('pending', :days, :uid)
        RETURNING id
    """), {'days': days, 'uid': current_user.id}).fetchone()
    db.session.commit()

    return jsonify({'job_id': row.id, 'status': 'pending'}), 202


@oru_bp.route('/nlp/job/<int:job_id>')
@login_required
def nlp_job_status(job_id):
    if current_user.role != 'admin':
        from flask import abort
        abort(403)

    row = db.session.execute(text("""
        SELECT id, status, processed_count, cluster_count, message, error_message, finished_at
        FROM oru_nlp_jobs WHERE id = :id
    """), {'id': job_id}).fetchone()
    if not row:
        abort(404)

    return jsonify({
        'job_id':          row.id,
        'status':          row.status,
        'processed_count': row.processed_count,
        'cluster_count':   row.cluster_count,
        'message':         row.message,
        'error_message':   row.error_message,
        'finished_at':     row.finished_at.strftime('%Y-%m-%d %H:%M:%S') if row.finished_at else None,
    })


# ── NLP analytics results ─────────────────────────────────────────────────────

@oru_bp.route('/nlp/results')
@login_required
def nlp_results():
    proc    = request.args.get('proc', '').strip()

    date_from = request.args.get('date_from', '').strip()
    date_to   = request.args.get('date_to', '').strip()
    # See oru_data()'s comment: filter by result_datetime, not received_at.
    where_clause, params, _days = _date_proc_conditions(date_from, date_to, proc, alias='o', days_default=90)

    rows = db.session.execute(text(f"""
        SELECT
            c.classification,
            c.cluster_id,
            c.cluster_label,
            c.severity_score,
            c.keywords,
            o.modality,
            o.procedure_name,
            o.procedure_code,
            o.physician_id
        FROM ai_nlp_cache c
        JOIN hl7_oru_reports o ON o.id = c.source_id
        WHERE {where_clause}
    """), params).fetchall()

    if not rows:
        return jsonify({'has_data': False})

    # Classification distribution
    cls_counter = Counter(r.classification for r in rows)

    # Cluster distribution with labels — cluster_label is now written directly
    # onto each ai_nlp_cache row by the worker (migration 0049), so no
    # more matching a settings-blob array back to a cluster_id by index.
    cluster_rows = db.session.execute(text(f"""
        SELECT c.cluster_id, c.cluster_label, COUNT(*) AS cnt,
               ROUND(AVG(c.severity_score)::numeric, 2) AS avg_sev
        FROM ai_nlp_cache c
        JOIN hl7_oru_reports o ON o.id = c.source_id
        WHERE {where_clause}
        GROUP BY c.cluster_id, c.cluster_label
        ORDER BY cnt DESC
    """), params).fetchall()

    # Top keywords across all reports (from NLP extraction)
    kw_counter = Counter()
    for r in rows:
        try:
            kws = r.keywords if isinstance(r.keywords, list) else json.loads(r.keywords or '[]')
            kw_counter.update(kws)
        except Exception:
            pass

    # Severity histogram (buckets 1-5)
    sev_buckets = [0, 0, 0, 0, 0]
    for r in rows:
        if r.severity_score is not None:
            bucket = min(int(float(r.severity_score)) - 1, 4)
            sev_buckets[max(bucket, 0)] += 1

    # Classification by modality
    cls_by_mod = {}
    for r in rows:
        mod = (r.modality or 'UNK').upper().strip()
        cls_by_mod.setdefault(mod, Counter())[r.classification] += 1

    return jsonify({
        'has_data':    True,
        'total':       len(rows),
        'classification': {
            'normal':     cls_counter.get('normal', 0),
            'borderline': cls_counter.get('borderline', 0),
            'critical':   cls_counter.get('critical', 0),
        },
        'clusters': [
            {
                'id':      r.cluster_id,
                'label':   r.cluster_label or f'Cluster {r.cluster_id}',
                'count':   r.cnt,
                'avg_sev': float(r.avg_sev or 0),
            }
            for r in cluster_rows
        ],
        'top_keywords': [
            {'word': w, 'count': c} for w, c in kw_counter.most_common(80)
        ],
        'severity_histogram': sev_buckets,
        'cls_by_modality': {
            mod: dict(cnts) for mod, cnts in cls_by_mod.items()
        },
    })


# ── Custom Critical Keywords management ────────────────────────────────────────

@oru_bp.route('/critical-keywords')
@login_required
def get_critical_keywords():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    try:
        rows = db.session.execute(
            text("SELECT key FROM settings WHERE key LIKE 'oru_crit:%' ORDER BY key")
        ).fetchall()
        custom = [r[0][len('oru_crit:'):] for r in rows]
    except Exception:
        custom = []
    return jsonify({'builtin': CRITICAL, 'custom': custom})


@oru_bp.route('/critical-keywords', methods=['POST'])
@login_required
def add_critical_keyword():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    data = request.get_json(silent=True) or {}
    word = (data.get('word') or '').strip().lower()
    if not word or len(word) > 80:
        return jsonify({'error': 'Invalid keyword'}), 400
    key = f'oru_crit:{word}'
    try:
        db.session.execute(
            text("INSERT INTO settings (key, value) VALUES (:k, '1') ON CONFLICT (key) DO NOTHING"),
            {'k': key}
        )
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True, 'word': word})


@oru_bp.route('/critical-keywords/<path:word>', methods=['DELETE'])
@login_required
def delete_critical_keyword(word):
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    word = word.strip().lower()
    key = f'oru_crit:{word}'
    try:
        db.session.execute(text("DELETE FROM settings WHERE key = :k"), {'k': key})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True})


@oru_bp.route('/keyword-suggestions')
@login_required
def keyword_suggestions():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    import re
    from collections import Counter

    all_critical = set(_get_all_critical_keywords())

    _STOP = {
        # English functional
        'the','a','an','and','or','but','is','are','was','were','be','been','being',
        'have','has','had','do','does','did','will','would','could','should','may',
        'might','shall','can','not','no','nor','so','yet','both','either','neither',
        'for','of','to','in','on','at','by','with','as','this','that','these','those',
        'it','its','he','she','they','we','you','from','into','through','during',
        'before','after','above','below','between','out','off','over','under','also',
        'any','each','few','more','most','other','some','such','only','same','than',
        'too','very','just','because','while','although','however','therefore',
        'there','here','then','when','where','which','who','what','how','if',
        # English medical generic
        'normal','noted','seen','show','shows','showing','demonstrated','identifying',
        'identified','appears','appear','present','presents','presented','patient',
        'findings','finding','study','examination','exam','report','result','results',
        'image','images','area','areas','level','levels','size','aspect','aspects',
        'region','regions','right','left','side','bilateral','upper','lower','noted',
        'anterior','posterior','lateral','medial','mild','moderate','severe',
        'significant','unremarkable','within','without','consistent','compatible',
        'suggestive','noted','noted','seen','visualized','visualised','compare',
        'comparison','measure','measuring','measures','measured','note','please',
        'including','included','following','above','below','interval','change',
        # French functional
        'le','la','les','un','une','des','du','de','et','ou','mais','donc','or',
        'ni','car','est','sont','était','étaient','ont','été','avoir','être',
        'ce','cette','ces','se','si','ne','pas','plus','très','bien','avec','sans',
        'pour','par','sur','sous','dans','en','au','aux','qui','que','dont','où',
        'mon','ton','son','notre','votre','leur','mes','tes','ses','nos','vos','leurs',
        'je','tu','il','elle','nous','vous','ils','elles','on','lui','eux',
        # French medical generic
        'examen','patient','patients','résultats','résultat','conclusion','noter',
        'noté','notée','montre','montrent','présente','présence','absence','aspect',
        'montrant','révèle','révélant','objectivant','objectivé','visible','visualisé',
        'mesurant','mesure','niveau','niveaux','zone','zones','côté','droit','gauche',
        'bilatéral','supérieur','inférieur','antérieur','postérieur','latéral','médial',
        'normal','normale','normaux','normales','sans','avec','dans','entre','après',
        'avant','lors','aucun','aucune','type','types','bonne','bon','bien','franc',
        # Units / numbers noise
        'mm','cm','ml','mg','mmhg','mhz','khz','mev','msec','sec',
    }

    try:
        rows = db.session.execute(text("""
            SELECT COALESCE(impression_text, '') || ' ' || COALESCE(report_text, '')
            FROM hl7_oru_reports
            WHERE received_at > NOW() - INTERVAL '6 months'
            ORDER BY received_at DESC
            LIMIT 600
        """)).fetchall()
    except Exception:
        return jsonify({'suggestions': []})

    counter = Counter()
    word_re = re.compile(r"[a-zA-ZÀ-ÿ\-]{4,}")
    for (blob,) in rows:
        if not blob:
            continue
        for w in word_re.findall(blob.lower()):
            if w not in _STOP and w not in all_critical and len(w) >= 4:
                counter[w] += 1

    suggestions = [
        {'word': w, 'count': c}
        for w, c in counter.most_common(30)
        if c >= 3
    ]
    return jsonify({'suggestions': suggestions})


# ── Diagnosis vocabulary management (admin) ─────────────────────────────────────
# phrase -> canonical label mapping used by the word cloud / critical findings
# log, DB-configurable since migration 0048 instead of a hardcoded constant.

@oru_bp.route('/diagnosis-vocabulary')
@login_required
def get_diagnosis_vocabulary():
    if current_user.role != 'admin':
        abort(403)
    rows = db.session.execute(text("""
        SELECT id, phrase, canonical_label, is_benign, active
        FROM oru_diagnosis_vocabulary
        ORDER BY canonical_label, phrase
    """)).fetchall()
    return jsonify({'vocabulary': [
        {'id': r.id, 'phrase': r.phrase, 'canonical_label': r.canonical_label,
         'is_benign': r.is_benign, 'active': r.active}
        for r in rows
    ]})


@oru_bp.route('/diagnosis-vocabulary', methods=['POST'])
@login_required
def add_diagnosis_vocabulary():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(silent=True) or {}
    phrase = (data.get('phrase') or '').strip().lower()
    label  = (data.get('canonical_label') or '').strip()
    is_benign = bool(data.get('is_benign', False))
    if not phrase or len(phrase) > 200 or not label or len(label) > 100:
        return jsonify({'error': 'phrase and canonical_label are required'}), 400
    try:
        db.session.execute(text("""
            INSERT INTO oru_diagnosis_vocabulary (phrase, canonical_label, is_benign)
            VALUES (:phrase, :label, :benign)
            ON CONFLICT (phrase) DO UPDATE SET
                canonical_label = EXCLUDED.canonical_label,
                is_benign       = EXCLUDED.is_benign,
                active          = TRUE,
                updated_at      = NOW()
        """), {'phrase': phrase, 'label': label, 'benign': is_benign})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    _invalidate_diagnoses_cache()
    return jsonify({'ok': True, 'phrase': phrase, 'canonical_label': label})


@oru_bp.route('/diagnosis-vocabulary/<int:vocab_id>', methods=['DELETE'])
@login_required
def delete_diagnosis_vocabulary(vocab_id):
    if current_user.role != 'admin':
        abort(403)
    try:
        db.session.execute(text("DELETE FROM oru_diagnosis_vocabulary WHERE id = :id"), {'id': vocab_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    _invalidate_diagnoses_cache()
    return jsonify({'ok': True})
