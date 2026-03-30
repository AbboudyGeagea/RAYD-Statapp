"""
nlp_processor.py
─────────────────────────────────────────────────────────────────────────────
Pure Python NLP for ORU radiology report analysis.
No background threads. No external API. No GPU.

Pipeline:
  1. TF-IDF vectorisation of impression / report text
  2. K-means clustering (k auto-selected, max 8)
  3. Per-report classification  → normal / borderline / critical
  4. Per-report severity score  → 1.0–5.0
  5. Per-report keyword list    → top TF-IDF terms

Negation caveat: TF-IDF does not handle negation ("no fracture" still
contains the token "fracture"). Classification uses phrase-level matching
on the raw text for normal/critical anchors before falling back to token
scores — this gives reasonable precision but is not clinical-grade.
"""

import re
import json
import logging
import numpy as np
from collections import Counter

logger = logging.getLogger("NLP")

# ── Stop words ────────────────────────────────────────────────────────────────
_STOP = frozenset({
    # English
    'the','a','an','and','or','but','in','on','at','to','for','of','with',
    'is','are','was','were','be','been','being','have','has','had','do',
    'does','did','will','would','could','should','may','might','can','not',
    'no','nor','so','yet','both','either','neither','each','few','more',
    'most','other','some','such','than','too','very','just','as','until',
    'while','if','then','that','this','these','those','it','its','also',
    'there','their','they','from','by','about','into','through','during',
    'above','below','between','out','off','over','under','again','further',
    'all','any','own','same','s','t','re','ll','ve','d','m',
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
    # Radiology boilerplate — high frequency, low signal
    'findings','finding','noted','note','seen','identified','demonstrated',
    'shows','shown','appear','appears','within','without','normal','limits',
    'unremarkable','study','examination','image','images','view','views',
    'patient','clinical','indication','technique','comparison','exam',
    'report','result','results','history','correlation','level','present',
    'please','however','additionally','furthermore','consistent','overall',
    'mild','moderate','severe','significant','evidence','acute','chronic',
    'bilateral','unilateral','right','left','upper','lower','middle','mid',
    'anterior','posterior','medial','lateral','superior','inferior','area',
    'region','noted','given','including','following','since','without',
    'no','negative','positive','assessment','impression','conclusion',
})

# ── Phrase-level normal anchors (checked BEFORE token scoring) ────────────────
_NORMAL_PHRASES = [
    'no acute', 'unremarkable', 'within normal limits', 'normal study',
    'no significant', 'no abnormality', 'no evidence of acute',
    'no pathological', 'no active disease', 'no acute cardiopulmonary',
    'clear and expanded', 'no pleural effusion', 'no pneumothorax',
    'no fracture identified', 'no bony abnormality', 'no intracranial',
    'stable appearance', 'no interval change', 'grossly normal',
]

# ── Critical term weights (token level) ──────────────────────────────────────
_CRITICAL_WEIGHTS = {
    'pneumothorax': 5, 'hemorrhage': 5, 'haemorrhage': 5, 'hematoma': 4,
    'haematoma': 4, 'embolism': 5, 'dissection': 5, 'infarct': 5,
    'infarction': 5, 'stroke': 5, 'rupture': 5, 'perforation': 5,
    'obstruction': 3, 'thrombosis': 4, 'occlusion': 4, 'stenosis': 3,
    'aneurysm': 4, 'abscess': 4, 'appendicitis': 4, 'ischemia': 4,
    'ischaemia': 4, 'neoplasm': 4, 'malignancy': 4, 'malignant': 4,
    'carcinoma': 4, 'metastasis': 4, 'metastases': 4, 'tumor': 3,
    'tumour': 3, 'mass': 3, 'fracture': 3, 'dislocation': 3,
    'effusion': 2, 'consolidation': 2, 'pneumonia': 3, 'empyema': 4,
    'tamponade': 5, 'pericardial': 2, 'aortic': 2, 'pulmonary': 1,
}

# ── Severity mapping ──────────────────────────────────────────────────────────
_SEVERITY_MAP = {
    'normal':     1.0,
    'borderline': 2.5,
    'critical':   4.5,
}
_CRITICAL_SCORE_OVERRIDE = {
    k: min(v * 0.6, 5.0) for k, v in _CRITICAL_WEIGHTS.items() if v >= 4
}


# ── Tokeniser ─────────────────────────────────────────────────────────────────
def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    words = re.findall(r"[a-zA-Z']+", text.lower())
    return [w for w in words if len(w) >= 3 and w not in _STOP]


# ── Single-report classification ──────────────────────────────────────────────
def classify_report(text: str) -> tuple[str, float]:
    """
    Returns (classification, severity_score).
    classification: 'normal' | 'borderline' | 'critical'
    severity_score: 1.0 – 5.0
    """
    if not text:
        return 'borderline', 2.5

    lower = text.lower()

    # 1. Phrase-level normal detection (high precision)
    normal_hits = sum(1 for p in _NORMAL_PHRASES if p in lower)
    if normal_hits >= 2:
        return 'normal', 1.0

    # 2. Token-level critical scoring
    tokens = _tokenize(text)
    tok_counter = Counter(tokens)
    critical_score = sum(
        _CRITICAL_WEIGHTS.get(tok, 0) * min(cnt, 2)
        for tok, cnt in tok_counter.items()
    )

    if critical_score >= 5:
        severity = min(1.0 + critical_score * 0.4, 5.0)
        return 'critical', round(severity, 1)

    if critical_score >= 2:
        return 'borderline', round(2.0 + critical_score * 0.2, 1)

    # 3. Single normal phrase with no critical terms → normal
    if normal_hits >= 1:
        return 'normal', 1.5

    return 'borderline', 2.5


# ── Keyword extraction (TF-IDF on single doc vs corpus) ──────────────────────
def extract_keywords(text: str, corpus_idf: dict, top_n: int = 8) -> list[str]:
    """
    Score each token by (term_freq_in_doc × idf_from_corpus).
    Returns top_n medical terms.
    """
    tokens = _tokenize(text)
    if not tokens:
        return []
    tf = Counter(tokens)
    total = len(tokens)
    scored = {
        tok: (cnt / total) * corpus_idf.get(tok, 1.0)
        for tok, cnt in tf.items()
    }
    return [w for w, _ in sorted(scored.items(), key=lambda x: -x[1])][:top_n]


# ── Corpus IDF builder ────────────────────────────────────────────────────────
def build_idf(texts: list[str]) -> dict[str, float]:
    """Compute IDF over a list of documents."""
    n = len(texts)
    if n == 0:
        return {}
    df = Counter()
    for t in texts:
        df.update(set(_tokenize(t)))
    return {
        term: np.log((1 + n) / (1 + count)) + 1.0
        for term, count in df.items()
    }


# ── Clustering ────────────────────────────────────────────────────────────────
def cluster_reports(texts: list[str], max_k: int = 8) -> tuple[list[int], list[str]]:
    """
    TF-IDF + K-means clustering.
    Auto-selects k (3 ≤ k ≤ max_k) using inertia elbow.
    Returns (cluster_ids, cluster_labels).
    cluster_labels[i] = human-readable label for cluster i (top 3 TF-IDF terms).
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import normalize
    except ImportError:
        logger.error("scikit-learn not installed")
        return [0] * len(texts), ['unclassified']

    if len(texts) < 6:
        return [0] * len(texts), ['insufficient data']

    # Vectorise
    vec = TfidfVectorizer(
        tokenizer=_tokenize,
        token_pattern=None,
        min_df=2,
        max_df=0.85,
        sublinear_tf=True,
        max_features=2000,
    )
    try:
        X = vec.fit_transform(texts)
    except ValueError:
        return [0] * len(texts), ['unclassified']

    X_norm = normalize(X, norm='l2')
    terms  = vec.get_feature_names_out()

    # Auto-select k via inertia elbow (cheap — max 8 iterations of KMeans)
    k_range = range(3, min(max_k + 1, len(texts) // 3 + 1))
    if not k_range or len(texts) < 9:
        k = 3
    else:
        inertias = []
        for k_try in k_range:
            km = KMeans(n_clusters=k_try, n_init=5, max_iter=100, random_state=42)
            km.fit(X_norm)
            inertias.append(km.inertia_)

        # Simple elbow: pick k where relative improvement drops below 15%
        k = list(k_range)[0]
        for i in range(1, len(inertias)):
            drop = (inertias[i - 1] - inertias[i]) / (inertias[0] + 1e-9)
            if drop < 0.15:
                k = list(k_range)[i]
                break

    # Final clustering
    km = KMeans(n_clusters=k, n_init=10, max_iter=300, random_state=42)
    labels = km.fit_predict(X_norm).tolist()

    # Cluster labels: top 3 terms per centroid
    cluster_labels = []
    for center in km.cluster_centers_:
        top_idx = center.argsort()[::-1][:3]
        top_terms = [terms[i].title() for i in top_idx if terms[i] not in _STOP]
        cluster_labels.append(' · '.join(top_terms) if top_terms else 'Mixed')

    return labels, cluster_labels


# ── Main batch processor ──────────────────────────────────────────────────────
def process_reports(records: list[dict]) -> tuple[list[dict], list[str]]:
    """
    records: list of {id, report_text, impression_text}
    Returns:
      results  — list of {id, classification, severity_score, keywords, cluster_id}
      cluster_labels — label per cluster index
    """
    if not records:
        return [], []

    texts = [
        (r.get('impression_text') or r.get('report_text') or '').strip()
        for r in records
    ]

    # Build corpus IDF once
    idf = build_idf(texts)

    # Classify + keyword extract per report
    results = []
    for r, text in zip(records, texts):
        cls, sev = classify_report(text)
        kws = extract_keywords(text, idf)
        results.append({
            'id':             r['id'],
            'classification': cls,
            'severity_score': sev,
            'keywords':       kws,
            'cluster_id':     None,   # filled after clustering
        })

    # Cluster (needs at least 6 docs)
    cluster_ids, cluster_labels = cluster_reports(texts)
    for res, cid in zip(results, cluster_ids):
        res['cluster_id'] = cid

    return results, cluster_labels
