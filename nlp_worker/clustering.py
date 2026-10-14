"""
clustering.py
─────────────────────────────────────────────────────────────────────────────
Pure Python NLP for ORU radiology report analysis.
No background threads. No external API. No GPU.

Pipeline:
  1. fit_model: TF-IDF + K-means (k auto-selected, max 8) on a sample of
     reports, stored by the worker in oru_cluster_models
  2. score_report, per report:
     classification  → normal / borderline / critical
     severity score  → 1.0–5.0
     keyword list    → top TF-IDF terms (the model's idf)
     cluster         → nearest centroid of the stored model

Negation: TF-IDF cannot tell "no fracture" from "fracture", so it never sees
the raw text. worker._negation_resolved_texts() runs each report through the
same medspaCy pipeline as the primary analysis and removes every negated or
historical mention; steps 1-2 count words only in that resolved text. The raw
text is used for one thing: the normal anchors, which are negations themselves
("no acute", "no pneumothorax").
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
    """Unicode-aware tokenizer — preserves French accented chars (é, è, ç, œ …)."""
    if not text:
        return []
    words = re.findall(r"[^\W\d_]+", text.lower(), re.UNICODE)
    return [w for w in words if len(w) >= 3 and w not in _STOP]


# ── Single-report classification ──────────────────────────────────────────────
def classify_report(text: str, resolved: str) -> tuple[str, float]:
    """
    Returns (classification, severity_score).
    classification: 'normal' | 'borderline' | 'critical'
    severity_score: 1.0 – 5.0
    text:     the report as received (normal anchors only)
    resolved: the report with negated / historical mentions removed
    """
    if not text:
        return 'borderline', 2.5

    lower = text.lower()
    normal_hits = sum(1 for p in _NORMAL_PHRASES if p in lower)

    # 1. Token-level critical scoring, affirmed mentions only. Runs before the
    #    normal anchors so that "No pneumothorax. No pleural effusion. Large
    #    hemorrhage." is critical, not normal.
    tok_counter = Counter(_tokenize(resolved))
    critical_score = sum(
        _CRITICAL_WEIGHTS.get(tok, 0) * min(cnt, 2)
        for tok, cnt in tok_counter.items()
    )

    if critical_score >= 5:
        severity = min(1.0 + critical_score * 0.4, 5.0)
        return 'critical', round(severity, 1)

    # 2. Phrase-level normal detection, or nothing affirmed at all
    #    ("No fracture or dislocation.")
    if normal_hits >= 2 or not tok_counter:
        return 'normal', 1.0

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


# ── Cluster model ─────────────────────────────────────────────────────────────
# One model, fitted on a sample and stored in oru_cluster_models, so every
# report is assigned against the same clusters and a cluster means the same thing
# in any date range. (Fitting K-means per processing batch gave each batch its own
# unrelated "cluster 0".)
_NO_FINDINGS_LABEL = 'No affirmed findings'   # nothing left once negations are removed
_OTHER_LABEL       = 'Other wording'          # words, but none in the model's vocabulary


def fit_model(resolved: list[str], max_k: int = 8) -> dict | None:
    """
    TF-IDF + K-means over negation-resolved texts, k auto-selected (3 ≤ k ≤ max_k)
    by inertia elbow. Returns a JSON-able {terms, idf, centroids, labels}, or None
    when there is too little text. labels has two entries after the K-means
    clusters: _NO_FINDINGS_LABEL and _OTHER_LABEL (see ClusterModel.assign).
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.cluster import KMeans

    worded = [t for t in resolved if _tokenize(t)]
    if len(worded) < 6:
        return None
    vec = TfidfVectorizer(
        tokenizer=_tokenize,
        token_pattern=None,
        min_df=2,
        max_df=0.85,
        sublinear_tf=True,
        max_features=2000,
    )
    try:
        X = vec.fit_transform(worded)   # rows already L2-normalised
    except ValueError:
        return None
    # Reports whose words all fell outside the vocabulary are zero vectors; they
    # would only drag a centroid towards the origin.
    X = X[X.getnnz(axis=1) > 0]
    n = X.shape[0]
    if n < 6:
        return None

    k_range = range(3, min(max_k + 1, n // 3 + 1))
    if not k_range or n < 9:
        k = 3
    else:
        inertias = []
        for k_try in k_range:
            km = KMeans(n_clusters=k_try, n_init=5, max_iter=100, random_state=42)
            km.fit(X)
            inertias.append(km.inertia_)
        # Simple elbow: pick k where relative improvement drops below 15%
        k = list(k_range)[0]
        for i in range(1, len(inertias)):
            drop = (inertias[i - 1] - inertias[i]) / (inertias[0] + 1e-9)
            if drop < 0.15:
                k = list(k_range)[i]
                break

    km = KMeans(n_clusters=k, n_init=10, max_iter=300, random_state=42).fit(X)
    terms = vec.get_feature_names_out()
    labels = []
    for center in km.cluster_centers_:
        top = [terms[i].title() for i in center.argsort()[::-1][:3] if center[i] > 0]
        labels.append(' · '.join(top) if top else 'Mixed')
    return {
        'terms':     terms.tolist(),
        'idf':       [round(float(x), 6) for x in vec.idf_],
        'centroids': [[round(float(x), 6) for x in c] for c in km.cluster_centers_],
        'labels':    labels + [_NO_FINDINGS_LABEL, _OTHER_LABEL],
    }


class ClusterModel:
    """A fitted model (fit_model's dict) applied to one report at a time."""

    def __init__(self, data: dict):
        self.index = {t: i for i, t in enumerate(data['terms'])}
        self.idf = np.asarray(data['idf'], dtype=float)
        self.idf_by_term = dict(zip(data['terms'], data['idf']))
        self.centroids = np.asarray(data['centroids'], dtype=float)
        self.labels = data['labels']

    def assign(self, resolved: str) -> int:
        """Nearest centroid, with the same TF-IDF weighting as the fit
        (sublinear tf × idf, L2-normalised)."""
        tokens = _tokenize(resolved)
        if not tokens:
            return len(self.centroids)           # _NO_FINDINGS_LABEL
        counts = Counter(t for t in tokens if t in self.index)
        if not counts:
            return len(self.centroids) + 1       # _OTHER_LABEL
        v = np.zeros(len(self.idf))
        for t, c in counts.items():
            i = self.index[t]
            v[i] = (1.0 + np.log(c)) * self.idf[i]
        v /= np.linalg.norm(v)
        return int(np.argmin(((self.centroids - v) ** 2).sum(axis=1)))


def score_report(text: str, resolved: str, model: ClusterModel | None) -> dict:
    """
    text     — impression (full report when there is none), as received
    resolved — the same text with negated / historical mentions removed
               (worker._negation_resolved_texts)
    Returns {classification, severity_score, keywords, cluster_id, cluster_label};
    cluster_id / cluster_label are None until a model exists.
    """
    cls, sev = classify_report(text, resolved)
    cid = model.assign(resolved) if model else None
    return {
        'classification': cls,
        'severity_score': sev,
        'keywords':       extract_keywords(resolved, model.idf_by_term if model else {}),
        'cluster_id':     cid,
        'cluster_label':  model.labels[cid] if model else None,
    }


