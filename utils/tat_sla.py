"""
utils/tat_sla.py
----------------
Radiologist TAT service-level thresholds, shared by every report that shows
turnaround time broken down by patient class.

Policy (operator instruction, 2026-09-19): a signed report is "within SLA" when
its turnaround time is at or under

    Inpatient   24 h
    Urgent/ER   24 h   (same target as inpatient, per operator)
    Outpatient  48 h

Thresholds are NOT hardcoded -- they live in `settings` rows keyed
'rad_tat_sla_hours:<BUCKET>' (hours, not minutes -- the unit is in the key name
so a value read straight out of the settings table is unambiguous). This is the
same prefixed-settings-key convention already used by 'jci_threshold:<CODE>'
(routes/report_35.py) and 'oru_crit:<keyword>' (routes/oru_analytics.py).
Migration 0121 seeds the three rows above. A bucket whose row is missing, empty
or unparseable falls back to _DEFAULT_SLA_HOURS; a bucket explicitly set to an
empty string is treated as "no target" and gets no pass/fail judgement at all
(same convention report_35 uses for JCI tiers with no configured threshold).

Patient-class vocabulary is likewise configurable, reusing the SAME
'pc_inpatient' / 'pc_outpatient' / 'pc_emergency' settings rows that
routes/super_report.py already reads -- there is no second vocabulary to keep in
sync. Raw patient_class values differ per install (no CHECK constraint on the
column), which is why this is config and not a literal.

Reports differ in what they call these buckets on screen ('IN'/'Urg'/'Out' in
Report 33, 'Inpatient'/'ER'/'Outpatient' in Report 25's TAT-anchor matrix), so
everything here is keyed on the canonical codes IN / URG / OUT and
resolve_bucket() maps a display label back to one.
"""
import logging

logger = logging.getLogger("tat_sla")

BUCKET_IN = 'IN'
BUCKET_URG = 'URG'
BUCKET_OUT = 'OUT'
BUCKETS = (BUCKET_IN, BUCKET_URG, BUCKET_OUT)

BUCKET_LABELS = {BUCKET_IN: 'Inpatient', BUCKET_URG: 'Urgent / ER', BUCKET_OUT: 'Outpatient'}

_SLA_SETTING_PREFIX = 'rad_tat_sla_hours:'
_DEFAULT_SLA_HOURS = {BUCKET_IN: 24.0, BUCKET_URG: 24.0, BUCKET_OUT: 48.0}

# Same setting keys and same defaults as routes/super_report.py's _pc_filter()
_VOCAB_SETTING_KEYS = {
    BUCKET_IN:  'pc_inpatient',
    BUCKET_OUT: 'pc_outpatient',
    BUCKET_URG: 'pc_emergency',
}
_DEFAULT_CLASS_VOCAB = {
    BUCKET_IN:  'I,IP,INPAT,INPATIENT,INN',
    BUCKET_OUT: 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY',
    BUCKET_URG: 'E,EP,ER,EMERGENCY,URG,URGENT',
}

# Report 33's own ER convention: an accession number starting '2XE' marks an ER
# study on this install. Kept here so every report agrees on it.
_ER_ACCESSION_PREFIX = '2XE'

# Display labels other reports already use, mapped back onto the canonical codes
# so a threshold set once applies wherever that label is rendered.
_LABEL_ALIASES = {
    'IN': BUCKET_IN, 'INPATIENT': BUCKET_IN, 'INPAT': BUCKET_IN, 'I': BUCKET_IN,
    'OUT': BUCKET_OUT, 'OUTPATIENT': BUCKET_OUT, 'OUTPAT': BUCKET_OUT, 'O': BUCKET_OUT,
    'URG': BUCKET_URG, 'URGENT': BUCKET_URG, 'ER': BUCKET_URG, 'EMERGENCY': BUCKET_URG,
}


def _settings_rows(like_pattern=None, keys=None):
    """Read settings rows, tolerating a missing/failed settings query."""
    from db import db
    from sqlalchemy import text
    try:
        if like_pattern:
            rows = db.session.execute(
                text("SELECT key, value FROM settings WHERE key LIKE :p"), {"p": like_pattern}
            ).fetchall()
        else:
            rows = db.session.execute(
                text("SELECT key, value FROM settings WHERE key IN :keys"), {"keys": tuple(keys)}
            ).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        logger.exception("Failed to read TAT SLA settings — falling back to defaults")
        try:
            db.session.rollback()
        except Exception:
            pass
        return {}


def get_sla_hours():
    """
    Return {bucket: hours_or_None} for IN / URG / OUT.

    None means "no target configured" — callers must render the raw distribution
    for that bucket and make no pass/fail judgement, rather than substituting a
    number of their own.
    """
    stored = _settings_rows(like_pattern=_SLA_SETTING_PREFIX + '%')
    out = {}
    for bucket in BUCKETS:
        raw = stored.get(_SLA_SETTING_PREFIX + bucket)
        if raw is None:
            out[bucket] = _DEFAULT_SLA_HOURS[bucket]
            continue
        raw = str(raw).strip()
        if raw == '':
            out[bucket] = None          # explicitly cleared -> no pass/fail
            continue
        try:
            out[bucket] = float(raw)
        except (TypeError, ValueError):
            logger.warning("Unparseable %s%s=%r — using default", _SLA_SETTING_PREFIX, bucket, raw)
            out[bucket] = _DEFAULT_SLA_HOURS[bucket]
    return out


def get_sla_minutes():
    """get_sla_hours() in minutes — most reports carry TAT as total_tat_min."""
    return {b: (h * 60.0 if h is not None else None) for b, h in get_sla_hours().items()}


def get_class_vocab():
    """Return {bucket: set(UPPERCASE patient_class codes)}."""
    stored = _settings_rows(keys=list(_VOCAB_SETTING_KEYS.values()))
    vocab = {}
    for bucket, key in _VOCAB_SETTING_KEYS.items():
        raw = stored.get(key)
        if raw is None or not str(raw).strip():
            raw = _DEFAULT_CLASS_VOCAB[bucket]
        vocab[bucket] = {v.strip().upper() for v in str(raw).split(',') if v.strip()}
    return vocab


def format_hours(hours):
    """'24h' / '4.5h' / '—' — one spelling of a target across every template."""
    if hours is None:
        return '—'
    return f"{hours:g}h"


def resolve_bucket(label):
    """Map a display label ('Inpatient', 'Out', 'ER', ...) to a canonical bucket."""
    if label is None:
        return None
    return _LABEL_ALIASES.get(str(label).strip().upper())


def classify(patient_class, accession_number=None, patient_location=None, vocab=None):
    """
    Classify one study into IN / URG / OUT, or None when it can't be placed.

    Checked most-specific first: the '2XE' accession prefix, then
    patient_location = 'ER', then the configured patient_class vocabulary
    (urgent before inpatient before outpatient). Unclassified studies return
    None and must be excluded from compliance figures rather than guessed —
    counting an unknown class against a 24 h or 48 h target would silently
    invent a result.

    Pass `vocab` (from get_class_vocab()) when classifying many rows so the
    settings read happens once instead of per row.
    """
    if vocab is None:
        vocab = get_class_vocab()

    acc = str(accession_number or '').strip().upper()
    if acc.startswith(_ER_ACCESSION_PREFIX):
        return BUCKET_URG

    loc = str(patient_location or '').strip().upper()
    if loc == 'ER':
        return BUCKET_URG

    pc = str(patient_class or '').strip().upper()
    if not pc:
        return None
    for bucket in (BUCKET_URG, BUCKET_IN, BUCKET_OUT):
        if pc in vocab[bucket]:
            return bucket
    # Broad fallback for installs whose class strings are descriptive rather than
    # coded ("Emergency Dept", "In-Patient"), matching er_dashboard.py's ILIKE style.
    for bucket in (BUCKET_URG, BUCKET_IN, BUCKET_OUT):
        if any(code in pc for code in vocab[bucket] if len(code) > 2):
            return bucket
    return None


def classify_series(df, class_col='patient_class', accession_col=None, location_col=None):
    """
    Vectorised classify() over a DataFrame — returns a Series of bucket codes
    (object dtype, None where unclassifiable). Reads the vocabulary once.
    """
    import pandas as pd
    if df is None or len(df) == 0:
        # df.apply(axis=1) on an empty frame returns a DataFrame, not a Series,
        # and assigning that to a column raises — so short-circuit instead.
        return pd.Series([], dtype=object)

    vocab = get_class_vocab()

    def _row(r):
        return classify(
            r.get(class_col),
            accession_number=r.get(accession_col) if accession_col else None,
            patient_location=r.get(location_col) if location_col else None,
            vocab=vocab,
        )

    return df.apply(_row, axis=1)


def evaluate(tat_value, bucket, unit='min'):
    """
    True (within SLA) / False (breached) / None (no target, or no usable TAT).
    `unit` is 'min' or 'h', matching whatever the caller's TAT column carries.
    """
    if bucket is None or tat_value is None:
        return None
    targets = get_sla_minutes() if unit == 'min' else get_sla_hours()
    target = targets.get(bucket)
    if target is None:
        return None
    try:
        val = float(tat_value)
    except (TypeError, ValueError):
        return None
    if val <= 0:
        return None      # unsigned / nonsensical TAT — not a breach, just no data
    return val <= target


def compliance(df, tat_col, bucket_col, unit='min', targets=None):
    """
    Per-bucket + overall SLA compliance for an already-classified DataFrame.

    Only rows with a positive TAT and a resolved bucket count — the same
    "non-null TAT" convention the reports already apply to their averages, so
    the denominator here matches the study counts shown next to it.

    Pass `targets` (from get_sla_minutes()/get_sla_hours()) when calling this in
    a loop — e.g. once per radiologist — so the settings read happens once for
    the whole report instead of once per group.

    Returns:
        {
          'unit': 'min'|'h',
          'targets': {bucket: target_in_`unit`_or_None},
          'by_bucket': [ {bucket, label, target, n, within, breached, pct}, ... ],
          'overall': {n, within, breached, pct},     # buckets with a target only
        }
    """
    if targets is None:
        targets = get_sla_minutes() if unit == 'min' else get_sla_hours()
    result = {
        'unit': unit,
        'targets': targets,
        'by_bucket': [],
        'overall': {'n': 0, 'within': 0, 'breached': 0, 'pct': None},
    }
    if df is None or len(df) == 0 or tat_col not in df.columns or bucket_col not in df.columns:
        return result

    import pandas as pd
    vals = pd.to_numeric(df[tat_col], errors='coerce')
    mask = vals.notna() & (vals > 0) & df[bucket_col].notna()
    scoped = df[mask].copy()
    scoped['_tat'] = vals[mask]

    tot_n = tot_within = 0
    for bucket in BUCKETS:
        bdf = scoped[scoped[bucket_col] == bucket]
        n = int(len(bdf))
        target = targets.get(bucket)
        if n == 0 and target is None:
            continue
        within = int((bdf['_tat'] <= target).sum()) if (target is not None and n) else 0
        target_h = None if target is None else (target / 60.0 if unit == 'min' else target)
        result['by_bucket'].append({
            'bucket': bucket,
            'label': BUCKET_LABELS[bucket],
            'target': target,
            # Pre-formatted so four templates don't each reinvent "24" vs "24.0"
            # vs "24.5h" in Jinja.
            'target_h': target_h,
            'target_label': format_hours(target_h),
            'n': n,
            'within': within if target is not None else None,
            'breached': (n - within) if target is not None else None,
            'pct': round(within / n * 100, 1) if (target is not None and n) else None,
        })
        if target is not None:
            tot_n += n
            tot_within += within

    result['overall'] = {
        'n': tot_n,
        'within': tot_within,
        'breached': tot_n - tot_within,
        'pct': round(tot_within / tot_n * 100, 1) if tot_n else None,
    }
    return result


def sla_hours_case_sql(bucket_expr, label_map):
    """
    Build a SQL CASE expression giving each row its SLA target in HOURS, for the
    reports that bucket patient class in SQL rather than pandas (report_25's
    TAT-anchor matrix). `label_map` maps the SQL expression's own output labels
    to canonical buckets, e.g. {'Inpatient': 'IN', 'ER': 'URG', 'Outpatient': 'OUT'}.

    Values are interpolated as literal floats (never user input — they come from
    get_sla_hours(), which float()s every settings value), and a bucket with no
    configured target yields NULL so comparisons against it are NULL, not false.
    """
    hours = get_sla_hours()
    whens = []
    for label, bucket in label_map.items():
        target = hours.get(bucket)
        safe_label = str(label).replace("'", "''")
        whens.append(f"WHEN '{safe_label}' THEN {float(target)}" if target is not None
                     else f"WHEN '{safe_label}' THEN NULL")
    if not whens:
        return "NULL"
    return f"CASE {bucket_expr} " + " ".join(whens) + " ELSE NULL END"
