#!/usr/bin/env python3
"""
RAYD — ORU NLP Worker
Standalone medspaCy batch processor. Runs as a separate Docker container
so medspaCy's RAM footprint and native deps are isolated from the main app.

Polls hl7_oru_reports every 60 seconds, processes unanalyzed rows in chunks,
writes results to hl7_oru_analysis.
"""
import os
import time
import psycopg2
import psycopg2.extras

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

def _any_unnegated(t, keyword):
    idx = t.find(keyword)
    while idx != -1:
        if not _is_negated(t, idx):
            return True
        idx = t.find(keyword, idx + len(keyword))
    return False


# ── Critical keyword groups ───────────────────────────────────────────────────

CRITICAL = [
    'pneumothorax','hemorrhage','haemorrhage','haematoma','hematoma',
    'pulmonary embolism','aortic dissection','stroke','infarct','infarction',
    'fracture','mass','malignancy','malignant','tumor','tumour','carcinoma',
    'thrombosis','obstruction','perforation','rupture','aneurysm','abscess',
    'appendicitis','ischemia','ischaemia','neoplasm','metastasis','metastases',
    'occlusion','stenosis','dissection','embolism','pneumonia','effusion',
]

# ── Diagnosis vocabulary: (match_phrase, canonical_label) ────────────────────

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
    # Normal / Benign
    ('no acute',             'No Acute Finding'),
    ('unremarkable',         'Unremarkable'),
    ('within normal limits', 'Normal'),
    ('sans particularité',   'Normal'),
    ('normal study',         'Normal'),
]

_BENIGN_LABELS = {'No Acute Finding', 'Unremarkable', 'Normal'}

# Bump when the model or vocabulary changes — triggers re-analysis of stale rows
_NLP_MODEL_VERSION = 'medspacy-v1'

_CHUNK        = 500
_BATCH_LIMIT  = 2000
_POLL_SECONDS = 60


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
        print("[NLP Worker] medspaCy loaded — clinical NLP active.")
    except Exception as e:
        print(f"[NLP Worker] medspaCy unavailable ({e}) — rule-based fallback active.")
    return _NLP


def _affirmed_phrases_batch(texts):
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
            return _docs_to_sets(nlp.pipe(cleaned, batch_size=64, n_process=_NLP_WORKERS))
        except Exception:
            try:
                return _docs_to_sets(nlp.pipe(cleaned, batch_size=64, n_process=1))
            except Exception:
                pass

    def _rb(t):
        return {phrase for phrase, _ in DIAGNOSES if _any_unnegated(t, phrase)} | \
               {kw for kw in CRITICAL if _any_unnegated(t, kw)}
    return [_rb(t) for t in cleaned]


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'db'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )


# ── Batch processing ──────────────────────────────────────────────────────────

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

            with conn.cursor() as cur:
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
                        cur.execute("""
                            INSERT INTO hl7_oru_analysis
                                (report_id, affirmed_labels, is_critical, nlp_version, analyzed_at)
                            VALUES (%s, %s::TEXT[], %s, %s, NOW())
                            ON CONFLICT (report_id) DO NOTHING
                        """, (r.id, pg_array, len(labels) > 0, _NLP_MODEL_VERSION))
                    except Exception as e:
                        print(f"[NLP Worker] Row {r.id} error: {e}")
                        conn.rollback()
                        continue

            conn.commit()
            committed += len(chunk)

        print(f"[NLP Worker] Batch complete — {committed}/{total} reports analyzed.")
    finally:
        conn.close()


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
    _load_medspacy()
    print(f"[NLP Worker] Polling every {_POLL_SECONDS}s.")

    while True:
        try:
            run_batch()
        except Exception as e:
            print(f"[NLP Worker] Batch error: {e}")
        time.sleep(_POLL_SECONDS)


if __name__ == '__main__':
    main()
