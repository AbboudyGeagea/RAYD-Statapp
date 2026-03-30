import re
import json
from collections import Counter
from flask import Blueprint, render_template, jsonify, request
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

# ── Normal classifiers ────────────────────────────────────────────────────────
NORMAL_PHRASES = [
    'no acute','unremarkable','within normal','normal study',
    'no significant','no abnormality','no evidence of acute',
    'no pathological','no active disease','normal limits',
]

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
def _count_diagnoses(rows, top_n=50):
    """
    For each row scan impression_text (fallback: report_text).
    Returns [{label, count}] sorted descending, limited to top_n.
    Labels with count == 0 are excluded.
    """
    label_counts = Counter()
    for r in rows:
        text = (r.impression_text or r.report_text or '').lower()
        if not text:
            continue
        seen_labels = set()
        for phrase, label in DIAGNOSES:
            if label not in seen_labels and phrase in text:
                seen_labels.add(label)
                label_counts[label] += 1
    return [
        {'word': label, 'count': cnt}
        for label, cnt in label_counts.most_common(top_n)
    ]


def _tokenize(text):
    """Lowercase, extract alpha words ≥ 3 chars, remove stop words."""
    if not text:
        return []
    words = re.findall(r"[a-zA-Z']+", text.lower())
    return [w for w in words if len(w) >= 3 and w not in STOP]


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
    Return list of canonical diagnosis labels found in text, excluding benign ones.
    Uses the same DIAGNOSES vocabulary as the treemap so both panels are consistent.
    """
    if not text:
        return []
    t = text.lower()
    seen, found = set(), []
    for phrase, label in DIAGNOSES:
        if label in _BENIGN_LABELS:
            continue
        if label not in seen and phrase in t:
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

    rows = db.session.execute(text(f"""
        SELECT procedure_code, procedure_name, modality,
               physician_id, report_text, impression_text, received_at
        FROM hl7_oru_reports
        WHERE {where_sql}
        ORDER BY received_at DESC
    """), params).fetchall()

    total        = len(rows)
    normal_count = sum(1 for r in rows if _is_normal(r.impression_text or r.report_text))
    abnormal_count = total - normal_count

    # ── Diagnosis frequency (reports per finding) ────────────────────────────
    cloud_words = _count_diagnoses(rows, top_n=top_n)

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
    critical_log = []
    for r in rows:
        text = _best_text(r)
        hits = _matched_diagnoses(text)
        if hits:
            critical_log.append({
                'procedure':   (r.procedure_name or r.procedure_code or '—').strip(),
                'modality':    (r.modality or '—').upper(),
                'keywords':    hits[:5],
                'received_at': r.received_at.strftime('%Y-%m-%d %H:%M') if r.received_at else '—',
                'snippet':     text[:220],
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
