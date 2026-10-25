"""
tests/hl7kit.py
────────────────────────────────────────────────────────────────
HL7 message construction for the test suite.

SEGMENTS ARE BUILT BY FIELD NUMBER, never by writing pipes by hand — the same rule
scripts/hl7_scenarios.py follows, for the same reason: hand-counting delimiters to
land a value on ORC-19 is how a fixture ends up encoding the bug it was meant to
catch. MSH's off-by-one (MSH-1 is the separator) is absorbed in seg().

Timestamps are LOCAL and naive, like the hospital's senders: ts(90) is "90 minutes
ago" on this machine's clock, formatted YYYYMMDDHHMMSS.

"Minutes ago" means REAL elapsed minutes, not wall-clock arithmetic. Across a DST
change the two differ by an hour (Beirut left summer time at midnight on
2026-10-25: 48 wall-clock hours before noon that day were 49 real hours), and
Postgres, measuring NOW() against a zoned timestamp, counts real time — correctly.
"""
import itertools
from datetime import datetime, timedelta, timezone

SB, EB, CR = b'\x0b', b'\x1c', b'\x0d'

_ctrl = itertools.count(1)


def next_ctrl(prefix='T'):
    return f'{prefix}{next(_ctrl):07d}'


def seg(name, fields=None):
    """Build a segment by FIELD NUMBER."""
    fields = {k: v for k, v in (fields or {}).items() if v is not None}
    top = max(fields) if fields else 1
    parts = [''] * (top + 1)
    parts[0] = name
    for num, val in fields.items():
        parts[num - 1 if name == 'MSH' else num] = str(val)
    return '|'.join(parts)


def message(*segments):
    return '\r'.join(s for s in segments if s) + '\r'


def frame(raw, encoding='utf-8'):
    return SB + raw.encode(encoding) + EB + CR


# ── time ──────────────────────────────────────────────────────────────────────

def now():
    return datetime.now().replace(microsecond=0)


def ago(minutes):
    real = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return real.astimezone().replace(tzinfo=None, microsecond=0)


def hl7(dt):
    return dt.strftime('%Y%m%d%H%M%S') if dt else None


def ts(minutes_ago):
    return hl7(ago(minutes_ago))


# ── segments ──────────────────────────────────────────────────────────────────

def msh(app='RIS', ctrl=None, mtype='ORM^O01', when=None, facility='SITE', version='2.4'):
    return seg('MSH', {2: r'^~\&', 3: app, 4: facility, 5: 'RAYD', 6: 'RAD',
                       7: when or ts(0), 9: mtype, 10: ctrl or next_ctrl(), 11: 'P',
                       12: version})


def pid(pat='P0001', name='DOE^JANE^M', dob='19850312', sex='F', pid18=None):
    return seg('PID', {1: '1', 3: f'{pat}^^^HIS' if pat else None, 5: name, 7: dob,
                       8: sex, 18: pid18})


def pv1(cls='O', loc='RAD^CT-ROOM-2^^RH', ref='20000191^WAKIM^GERARD', visit=None):
    return seg('PV1', {1: '1', 2: cls, 3: loc, 8: ref, 19: visit})


def orc(control='SC', placer=None, acc=None, status=None, when=None, action_by=None,
        ordering=None):
    return seg('ORC', {1: control, 2: f'{placer}^HIS' if placer else None,
                       3: f'{acc}^HIS' if acc else None, 5: status, 9: when,
                       12: ordering, 19: action_by})


def obr(acc=None, placer=None, code='CTABD', desc='CT ABDOMEN AND PELVIS', ae='CT64_RH',
        mod='CT', result_dt=None, result_status=None, obr7=None, signer=None, extra=None):
    fields = {1: '1', 2: f'{placer}^HIS' if placer else None,
              3: f'{acc}^HIS' if acc else None,
              4: f'{code}^{desc}' if code else None, 7: obr7, 21: ae, 22: result_dt,
              24: mod, 25: result_status, 57: signer}
    fields.update(extra or {})
    return seg('OBR', fields)


def obx(value, status='F', vtype='TX', obs_id='REPORT', set_id=1):
    return seg('OBX', {1: set_id, 2: vtype, 3: obs_id, 5: value, 11: status})


# ── whole messages ────────────────────────────────────────────────────────────

PATIENT = 'P0001'


def adt(event='A08', pat=PATIENT, name='DOE^JANE^M', dob='19850312', sex='F', cls='O',
        loc='EM^ROOM3^^RH', when=None, app='SAP_HIS', ctrl=None, visit=None):
    return message(msh(app, ctrl, f'ADT^{event}', when or ts(0)),
                   pid(pat, name, dob, sex),
                   seg('PV1', {1: '1', 2: cls, 3: loc, 19: visit}))


def order(placer, acc=None, pat=PATIENT, when=None, app='SAP_HIS', ctrl=None,
          code='CTABD', desc='CT ABDOMEN AND PELVIS', mod='CT', ae=None, cls='O',
          loc='RAD^CT-ROOM-2^^RH', ref='20000191^WAKIM^GERARD', visit=None,
          ordering=None, msh_when=None):
    when = when or ts(0)
    return message(msh(app, ctrl, 'ORM^O01', msh_when or when),
                   pid(pat),
                   pv1(cls, loc, ref, visit),
                   orc('NW', placer=placer, acc=acc, when=when, ordering=ordering),
                   obr(acc=acc, placer=placer, code=code, desc=desc, ae=ae, mod=mod))


def status(code, acc, when=None, pat=PATIENT, app='RIS', ctrl=None, placer=None,
           ae='CT64_RH', mod='CT', proc='CTABD', desc='CT ABDOMEN AND PELVIS',
           tech='TECH01^HADDAD^RIMA', cls='O', loc='RAD^CT-ROOM-2^^RH',
           ref='20000191^WAKIM^GERARD', visit=None, msh_when=None, control='SC',
           orc9=True):
    """A RIS lifecycle event (ORC-5 = code) for one accession."""
    when = when or ts(0)
    return message(msh(app, ctrl, 'ORM^O01', msh_when or when),
                   pid(pat),
                   pv1(cls, loc, ref, visit),
                   orc(control, placer=placer, acc=acc, status=code,
                       when=when if orc9 else None, action_by=tech),
                   obr(acc=acc, placer=placer, code=proc, desc=desc, ae=ae, mod=mod))


def report(acc, when=None, result_status='F', pat=PATIENT, app='PACS', ctrl=None,
           signer_email='rad@sjh.com', signer='Dany&Abou Chedid',
           text='No acute abnormality.', impression='Normal study.', mod='CT',
           code='CTABD', desc='CT ABDO', obr25=None, obx_statuses=None, pv1_seg=None,
           msh_when=None):
    """An ORU^R01. obx_statuses overrides the per-OBX OBX-11 values."""
    when = when or ts(0)
    statuses = obx_statuses if obx_statuses is not None else [result_status, result_status]
    obxs = []
    values = [text, impression]
    ids = ['REPORT', 'IMPRESSION']
    for i, st in enumerate(statuses):
        obxs.append(obx(values[i % 2], st, obs_id=ids[i % 2], set_id=i + 1))
    stamp = f'{signer_email}&{signer}^^{when}' if signer_email else None
    return message(msh(app, ctrl, 'ORU^R01', msh_when or when),
                   pid(pat),
                   pv1_seg,
                   obr(acc=acc, code=code, desc=desc, ae=None, mod=mod, result_dt=when,
                       result_status=obr25, signer=stamp),
                   *obxs)


def lifecycle(acc, start_minutes_ago=300, codes=('SC', 'AR', 'IP', 'CM'), step=20, **kw):
    """[(code, raw)] for one study's RIS events, oldest first, `step` minutes apart."""
    out = []
    for i, code in enumerate(codes):
        out.append((code, status(code, acc, ts(start_minutes_ago - i * step), **kw)))
    return out
