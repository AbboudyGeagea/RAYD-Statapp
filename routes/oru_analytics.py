import os
import re
import json
from collections import Counter
from flask import Blueprint, render_template, jsonify, request, abort
from flask_login import login_required, current_user
from sqlalchemy import text
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
    idx = t.find(keyword)
    while idx != -1:
        if not _is_negated(t, idx):
            return True
        idx = t.find(keyword, idx + len(keyword))
    return False


# ── MedSpaCy — loaded once at startup, falls back to rule-based if unavailable ─
_NLP = None

def _load_medspacy():
    global _NLP
    if _NLP is not None:
        return _NLP
    try:
        import medspacy
        from medspacy.target_matcher import TargetRule
        from medspacy.context import ConTextRule

        nlp = medspacy.load(enable=["sentencizer", "medspacy_target_matcher", "medspacy_context"])

        # Register every phrase we track as a findable target
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

        # Add French negation rules — default ConText only covers English
        context = nlp.get_pipe("medspacy_context")
        context.add([
            ConTextRule("pas de",        "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("sans",          "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("absence de",    "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("aucun",         "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("aucune",        "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("négatif pour",  "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("négatif",       "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("non",           "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("exclu",         "NEGATED_EXISTENCE", direction="BIDIRECTIONAL"),
            ConTextRule("écarté",        "NEGATED_EXISTENCE", direction="BIDIRECTIONAL"),
        ])

        _NLP = nlp
        print("[ORU] MedSpaCy loaded — clinical NLP active.")
    except Exception as e:
        print(f"[ORU] MedSpaCy unavailable ({e}) — rule-based negation fallback active.")
    return _NLP


def _affirmed_phrases(text):
    """
    Single-text wrapper — used for one-off lookups.
    For bulk processing always prefer _affirmed_phrases_batch().
    """
    if not text:
        return set()
    t = text.lower()
    nlp = _load_medspacy()
    if nlp is not None:
        try:
            doc = nlp(t[:8000])
            return {
                ent.text.lower()
                for ent in doc.ents
                if not ent._.is_negated and not ent._.is_historical
            }
        except Exception:
            pass
    return {phrase for phrase, _ in DIAGNOSES if _any_unnegated(t, phrase)} | \
           {kw for kw in CRITICAL if _any_unnegated(t, kw)}


# 75% of available cores — leaves headroom for Flask, DB, and the AI server
_NLP_WORKERS = max(1, int((os.cpu_count() or 4) * 0.75))


def _affirmed_phrases_batch(texts):
    """
    Process a list of texts in one shot using nlp.pipe().
    Saturates available CPU cores via n_process.
    Returns a list of sets — one per input text — of affirmed phrase strings.
    Falls back to rule-based if medspacy is unavailable or multiprocessing fails.
    """
    if not texts:
        return []

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
                nlp.pipe(cleaned, batch_size=64, n_process=_NLP_WORKERS)
            )
        except Exception:
            try:
                return _docs_to_sets(
                    nlp.pipe(cleaned, batch_size=64, n_process=1)
                )
            except Exception:
                pass

    # Rule-based fallback — still runs per-text but no repeated nlp() calls
    def _rb(t):
        return {phrase for phrase, _ in DIAGNOSES if _any_unnegated(t, phrase)} | \
               {kw for kw in CRITICAL if _any_unnegated(t, kw)}
    return [_rb(t) for t in cleaned]


# Bump this string whenever the NLP model or DIAGNOSES vocabulary changes
# so stale rows in hl7_oru_analysis can be detected and re-processed.
_NLP_MODEL_VERSION = 'medspacy-v1'


_NLP_CHUNK = 500


def run_oru_nlp_batch():
    """
    Scheduled job (every 60 min): process unanalyzed hl7_oru_reports in chunks.
    Checks cpu_guard between chunks — stops immediately if an AI request arrives.
    Commits after each chunk so progress is never lost on an early exit.
    """
    from utils.cpu_guard import is_ai_active

    if is_ai_active():
        print("[ORU NLP] AI active at start — skipping this cycle.")
        return

    # Lower OS scheduling priority while running
    try:
        os.nice(10)
    except Exception:
        pass

    try:
        rows = db.session.execute(text("""
            SELECT r.id, r.impression_text, r.report_text
            FROM   hl7_oru_reports r
            LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
            WHERE  a.id IS NULL
            ORDER  BY r.received_at DESC
            LIMIT  2000
        """)).fetchall()
    except Exception as e:
        print(f"[ORU NLP] Could not query pending reports: {e}")
        return

    if not rows:
        return

    total, committed = len(rows), 0

    for chunk_start in range(0, total, _NLP_CHUNK):
        if is_ai_active():
            print(f"[ORU NLP] AI became active — stopped at {committed}/{total}, resuming next cycle.")
            return

        chunk         = rows[chunk_start:chunk_start + _NLP_CHUNK]
        texts         = [(r.impression_text or r.report_text or '') for r in chunk]
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
                db.session.execute(text("""
                    INSERT INTO hl7_oru_analysis
                        (report_id, affirmed_labels, is_critical, nlp_version, analyzed_at)
                    VALUES
                        (:rid, :labels::TEXT[], :critical, :ver, NOW())
                    ON CONFLICT (report_id) DO NOTHING
                """), {'rid': r.id, 'labels': pg_array,
                       'critical': len(labels) > 0, 'ver': _NLP_MODEL_VERSION})
            except Exception:
                db.session.rollback()
                continue

        try:
            db.session.commit()
            committed += len(chunk)
        except Exception as e:
            db.session.rollback()
            print(f"[ORU NLP] Commit error at chunk {chunk_start}: {e}")
            return

    print(f"[ORU NLP] Batch complete — {committed}/{total} reports analyzed.")


# ── Diagnosis vocabulary: (match_phrase, canonical_label)
# Multiple phrases can share the same label — counted per-report (not per-word)
DIAGNOSES = [
    # Pulmonary / Chest
    ('pneumothorax',         'Pneumothorax'),
    ('pulmonary embolism',   'Pulmonary Embolism'),
    ('pleural effusion',     'Pleural Effusion'),
    ('épanchement pleural',  'Pleural Effusion'),
    ('consolidation',        'Consolidation'),
    ('pneumonia',            'Pneumonia'),
    ('pneumonie',            'Pneumonia'),
    ('atelectasis',          'Atelectasis'),
    ('atélectasie',          'Atelectasis'),
    ('emphysema',            'Emphysema'),
    ('pulmonary edema',      'Pulmonary Edema'),
    ('pulmonary oedema',     'Pulmonary Edema'),
    ('oedème pulmonaire',    'Pulmonary Edema'),
    ('hemothorax',           'Hemothorax'),
    ('haemothorax',          'Hemothorax'),
    ('cardiomegaly',         'Cardiomegaly'),
    ('pericardial effusion', 'Pericardial Effusion'),
    ('aortic dissection',    'Aortic Dissection'),
    ('aortic aneurysm',      'Aortic Aneurysm'),
    # Neuro / Brain
    ('intracranial hemorrhage', 'Intracranial Hemorrhage'),
    ('intracranial haemorrhage','Intracranial Hemorrhage'),
    ('subdural hematoma',    'Subdural Hematoma'),
    ('subdural haematoma',   'Subdural Hematoma'),
    ('epidural hematoma',    'Epidural Hematoma'),
    ('subarachnoid hemorrhage','Subarachnoid Hemorrhage'),
    ('hemorrhage',           'Hemorrhage'),
    ('haemorrhage',          'Hemorrhage'),
    ('hematoma',             'Hematoma'),
    ('haematoma',            'Hematoma'),
    ('stroke',               'Stroke'),
    ('infarction',           'Infarction'),
    ('infarct',              'Infarction'),
    ('ischemia',             'Ischemia'),
    ('ischaemia',            'Ischemia'),
    ('aneurysm',             'Aneurysm'),
    ('hydrocephalus',        'Hydrocephalus'),
    ('midline shift',        'Midline Shift'),
    # Abdomen / GI
    ('appendicitis',         'Appendicitis'),
    ('cholecystitis',        'Cholecystitis'),
    ('cholelithiasis',       'Cholelithiasis'),
    ('gallstone',            'Gallstone'),
    ('bowel obstruction',    'Bowel Obstruction'),
    ('obstruction',          'Obstruction'),
    ('perforation',          'Perforation'),
    ('abscess',              'Abscess'),
    ('hepatomegaly',         'Hepatomegaly'),
    ('splenomegaly',         'Splenomegaly'),
    ('ascites',              'Ascites'),
    ('pancreatitis',         'Pancreatitis'),
    ('diverticulitis',       'Diverticulitis'),
    ('hernia',               'Hernia'),
    # Vascular
    ('deep vein thrombosis', 'DVT'),
    ('dvt',                  'DVT'),
    ('thrombosis',           'Thrombosis'),
    ('thrombose',            'Thrombosis'),
    ('occlusion',            'Occlusion'),
    ('stenosis',             'Stenosis'),
    ('sténose',              'Stenosis'),
    ('dissection',           'Dissection'),
    ('embolism',             'Embolism'),
    ('embolus',              'Embolism'),
    # MSK / Trauma
    ('fracture',             'Fracture'),
    ('dislocation',          'Dislocation'),
    ('luxation',             'Dislocation'),
    ('osteoporosis',         'Osteoporosis'),
    ('arthritis',            'Arthritis'),
    ('arthrose',             'Arthritis'),
    ('osteomyelitis',        'Osteomyelitis'),
    ('spondylosis',          'Spondylosis'),
    ('disc herniation',      'Disc Herniation'),
    ('disk herniation',      'Disc Herniation'),
    ('herniated disc',       'Disc Herniation'),
    ('spinal stenosis',      'Spinal Stenosis'),
    # Oncology
    ('metastasis',           'Metastasis'),
    ('metastases',           'Metastasis'),
    ('métastase',            'Metastasis'),
    ('malignancy',           'Malignancy'),
    ('malignant',            'Malignancy'),
    ('carcinoma',            'Carcinoma'),
    ('carcinome',            'Carcinoma'),
    ('lymphoma',             'Lymphoma'),
    ('lymphome',             'Lymphoma'),
    ('adenoma',              'Adenoma'),
    ('adénome',              'Adenoma'),
    ('neoplasm',             'Neoplasm'),
    ('tumor',                'Tumor / Mass'),
    ('tumour',               'Tumor / Mass'),
    ('tumeur',               'Tumor / Mass'),
    ('mass',                 'Tumor / Mass'),
    ('nodule',               'Nodule'),
    ('lesion',               'Lesion'),
    ('lésion',               'Lesion'),
    ('cyst',                 'Cyst'),
    ('kyste',                'Cyst'),
    # Kidney / Urinary
    ('hydronephrosis',       'Hydronephrosis'),
    ('nephrolithiasis',      'Nephrolithiasis'),
    ('urolithiasis',         'Nephrolithiasis'),
    ('renal calculus',       'Renal Calculus'),
    ('kidney stone',         'Renal Calculus'),
    # Infection / Inflammation
    ('empyema',              'Empyema'),
    ('cellulitis',           'Cellulitis'),
    ('osteomyelitis',        'Osteomyelitis'),
    # Normal / Benign
    ('no acute',             'No Acute Finding'),
    ('unremarkable',         'Unremarkable'),
    ('within normal limits', 'Normal'),
    ('sans particularité',   'Normal'),
    ('normal study',         'Normal'),
]

# Deduplicate: for each canonical label keep count of reports mentioning it
# (multiple phrases mapping to same label are OR'd per report, not summed)
def _count_diagnoses(affirmed_list, top_n=50):
    """
    Count diagnosis labels across a list of pre-computed affirmed-phrase sets.
    Expects output of _affirmed_phrases_batch() or all_affirmed built in oru_data().
    """
    label_counts = Counter()
    for affirmed in affirmed_list:
        if not affirmed:
            continue
        seen_labels = set()
        for phrase, label in DIAGNOSES:
            if label in seen_labels:
                continue
            if phrase in affirmed:
                seen_labels.add(label)
                label_counts[label] += 1
    return [
        {'word': label, 'count': cnt}
        for label, cnt in label_counts.most_common(top_n)
    ]


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


def _is_normal(text):
    if not text:
        return False
    t = text.lower()
    return any(p in t for p in NORMAL_PHRASES)


# Labels considered benign — excluded from the critical findings log
_BENIGN_LABELS = {'No Acute Finding', 'Unremarkable', 'Normal'}

def _best_text(row):
    """Return the most meaningful text from a report row, stripping whitespace."""
    imp = (row.impression_text or '').strip()
    rep = (row.report_text or '').strip()
    return imp or rep

def _matched_diagnoses(text):
    """
    Return list of canonical diagnosis labels affirmed in text, excluding benign ones.
    Negated and historical mentions are excluded via medspacy (or rule-based fallback).
    Uses the same DIAGNOSES vocabulary as the treemap so both panels are consistent.
    """
    if not text:
        return []
    affirmed = _affirmed_phrases(text)
    seen, found = set(), []
    for phrase, label in DIAGNOSES:
        if label in _BENIGN_LABELS or label in seen:
            continue
        if phrase in affirmed:
            seen.add(label)
            found.append(label)
    return found


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
    return render_template('oru_analytics.html', procedures=procedures)


@oru_bp.route('/data')
@login_required
def oru_data():
    proc    = request.args.get('proc', '').strip()
    days    = min(int(request.args.get('days', 30)), 365)
    top_n   = int(request.args.get('top', 40))

    where = ["received_at >= NOW() - INTERVAL :interval"]
    params = {'interval': f'{days} days'}
    if proc:
        where.append("UPPER(TRIM(procedure_code)) = UPPER(:proc)")
        params['proc'] = proc

    where_sql = ' AND '.join(where)

    # LEFT JOIN pre-computed analysis — analyzed rows skip NLP entirely
    rows = db.session.execute(text(f"""
        SELECT r.procedure_code, r.procedure_name, r.modality,
               r.physician_id, r.patient_id, r.accession_number,
               r.report_text, r.impression_text, r.result_datetime, r.received_at,
               a.affirmed_labels
        FROM   hl7_oru_reports r
        LEFT JOIN hl7_oru_analysis a ON a.report_id = r.id
        WHERE  {where_sql.replace('received_at', 'r.received_at').replace('procedure_code', 'r.procedure_code')}
        ORDER  BY r.received_at DESC
    """), params).fetchall()

    total        = len(rows)
    normal_count = sum(1 for r in rows if _is_normal(r.impression_text or r.report_text))
    abnormal_count = total - normal_count

    # ── Build affirmed-label sets — stored for analyzed rows, live NLP for pending ──
    analyzed_affirmed = {
        i: set(r.affirmed_labels)
        for i, r in enumerate(rows)
        if r.affirmed_labels is not None
    }
    pending_indices = [i for i, r in enumerate(rows) if r.affirmed_labels is None]
    if pending_indices:
        pending_texts    = [_best_text(rows[i]) for i in pending_indices]
        pending_affirmed = _affirmed_phrases_batch(pending_texts)
        for i, affirmed in zip(pending_indices, pending_affirmed):
            analyzed_affirmed[i] = affirmed

    all_affirmed = [analyzed_affirmed.get(i, set()) for i in range(len(rows))]

    # ── Diagnosis frequency (word cloud) — purely from pre-computed sets ─────
    cloud_words = _count_diagnoses(all_affirmed, top_n=top_n)

    # ── Modality breakdown ────────────────────────────────────────────────────
    mod_counter = Counter(
        (r.modality or 'UNK').upper().strip() for r in rows if r.modality
    )
    modalities = [{'modality': m, 'count': c} for m, c in mod_counter.most_common()]

    # ── Top procedures ────────────────────────────────────────────────────────
    proc_counter = Counter()
    for r in rows:
        key = (r.procedure_code or '').upper().strip()
        name = (r.procedure_name or r.procedure_code or 'Unknown').strip()
        if key:
            proc_counter[(key, name)] += 1
    top_procs = [
        {'code': k, 'name': n, 'count': c}
        for (k, n), c in proc_counter.most_common(10)
    ]

    # ── Critical findings (most recent 20) ───────────────────────────────────
    custom_kws = _get_all_critical_keywords()
    critical_log = []
    for r, affirmed in zip(rows, all_affirmed):
        seen, hits = set(), []
        for phrase, label in DIAGNOSES:
            if label in _BENIGN_LABELS or label in seen:
                continue
            if phrase in affirmed:
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
            })
    critical_log = critical_log[:20]

    # ── Physician activity (anonymised) ──────────────────────────────────────
    phys_counter = Counter(
        (r.physician_id or 'UNKNOWN').strip() for r in rows if r.physician_id
    )
    physicians = [
        {'id': pid, 'count': c}
        for pid, c in phys_counter.most_common(10)
    ]

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

    days   = min(int(request.args.get('days', 30)), 365)
    proc   = request.args.get('proc', '').strip()

    where  = ["received_at >= NOW() - INTERVAL :interval"]
    params = {'interval': f'{days} days'}
    if proc:
        where.append("UPPER(TRIM(procedure_code)) = UPPER(:proc)")
        params['proc'] = proc

    rows = db.session.execute(text(
        f"""SELECT physician_id, procedure_code, procedure_name,
                   report_text, impression_text,
                   to_char(received_at, 'YYYY-MM-DD HH24:MI') AS received_at
            FROM hl7_oru_reports WHERE {' AND '.join(where)}
            ORDER BY received_at DESC"""
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

    days   = min(int(request.args.get('days', 30)), 365)
    proc   = request.args.get('proc', '').strip()
    top_n  = 40

    where  = ["received_at >= NOW() - INTERVAL :interval"]
    params = {'interval': f'{days} days'}
    if proc:
        where.append("UPPER(TRIM(procedure_code)) = UPPER(:proc)")
        params['proc'] = proc

    rows = db.session.execute(text(
        f"SELECT report_text, impression_text FROM hl7_oru_reports WHERE {' AND '.join(where)}"
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


# ── NLP processing (on-demand, triggered by user) ─────────────────────────────

@oru_bp.route('/nlp/process', methods=['POST'])
@login_required
def nlp_process():
    if current_user.role != 'admin':
        from flask import abort
        abort(403)

    data = request.get_json(force=True)
    days = min(int(data.get('days', 90)), 365)

    # Fetch unprocessed reports only
    rows = db.session.execute(text("""
        SELECT o.id, o.report_text, o.impression_text
        FROM hl7_oru_reports o
        LEFT JOIN ai_nlp_cache c ON c.source_id = o.id
        WHERE c.id IS NULL
          AND o.received_at >= NOW() - INTERVAL :interval
          AND o.report_text IS NOT NULL
          AND TRIM(o.report_text) != ''
        ORDER BY o.received_at DESC
        LIMIT 500
    """), {'interval': f'{days} days'}).fetchall()

    if not rows:
        return jsonify({'processed': 0, 'message': 'Nothing new to process.'})

    records = [{'id': r.id, 'report_text': r.report_text, 'impression_text': r.impression_text}
               for r in rows]

    try:
        from nlp_processor import process_reports
        results, cluster_labels = process_reports(records)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Persist cluster labels (store in settings table for display)
    try:
        labels_json = json.dumps(cluster_labels)
        exists = db.session.execute(
            text("SELECT 1 FROM settings WHERE key = 'nlp_cluster_labels'")
        ).fetchone()
        if exists:
            db.session.execute(
                text("UPDATE settings SET value = :v WHERE key = 'nlp_cluster_labels'"),
                {'v': labels_json}
            )
        else:
            db.session.execute(
                text("INSERT INTO settings (key, value) VALUES ('nlp_cluster_labels', :v)"),
                {'v': labels_json}
            )
    except Exception:
        pass  # non-fatal

    # Upsert NLP results
    saved = 0
    for res in results:
        try:
            db.session.execute(text("""
                INSERT INTO ai_nlp_cache
                    (source_id, classification, keywords, cluster_id, severity_score, processed_at)
                VALUES
                    (:sid, :cls, :kws::jsonb, :cid, :sev, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    classification = EXCLUDED.classification,
                    keywords       = EXCLUDED.keywords,
                    cluster_id     = EXCLUDED.cluster_id,
                    severity_score = EXCLUDED.severity_score,
                    processed_at   = NOW()
            """), {
                'sid': res['id'],
                'cls': res['classification'],
                'kws': json.dumps(res['keywords']),
                'cid': res['cluster_id'],
                'sev': res['severity_score'],
            })
            saved += 1
        except Exception:
            db.session.rollback()
            continue

    db.session.commit()
    return jsonify({
        'processed':      saved,
        'cluster_labels': cluster_labels,
        'message':        f'Processed {saved} reports into {len(cluster_labels)} clusters.',
    })


# ── NLP analytics results ─────────────────────────────────────────────────────

@oru_bp.route('/nlp/results')
@login_required
def nlp_results():
    proc    = request.args.get('proc', '').strip()
    days    = min(int(request.args.get('days', 90)), 365)

    where_extra = ''
    params = {'interval': f'{days} days'}
    if proc:
        where_extra = "AND UPPER(TRIM(o.procedure_code)) = UPPER(:proc)"
        params['proc'] = proc

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
        WHERE o.received_at >= NOW() - INTERVAL :interval
          {where_extra}
    """), params).fetchall()

    if not rows:
        return jsonify({'has_data': False})

    # Classification distribution
    cls_counter = Counter(r.classification for r in rows)

    # Cluster distribution with labels
    cluster_rows = db.session.execute(text("""
        SELECT c.cluster_id, c.cluster_label, COUNT(*) AS cnt,
               ROUND(AVG(c.severity_score)::numeric, 2) AS avg_sev
        FROM ai_nlp_cache c
        JOIN hl7_oru_reports o ON o.id = c.source_id
        WHERE o.received_at >= NOW() - INTERVAL :interval
        GROUP BY c.cluster_id, c.cluster_label
        ORDER BY cnt DESC
    """), {'interval': f'{days} days'}).fetchall()

    # Load cluster labels from settings (set during last processing run)
    labels_row = db.session.execute(
        text("SELECT value FROM settings WHERE key = 'nlp_cluster_labels'")
    ).fetchone()
    stored_labels = json.loads(labels_row.value) if labels_row else []

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
                'label':   r.cluster_label or (stored_labels[r.cluster_id] if r.cluster_id is not None and r.cluster_id < len(stored_labels) else f'Cluster {r.cluster_id}'),
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
    from db import user_has_page
    if current_user.role != 'admin' and not user_has_page(current_user, 'oru'):
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
    if current_user.role != 'admin':
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
    if current_user.role != 'admin':
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
