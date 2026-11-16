"""
utils/crn_sms.py
────────────────
SMS sending for CRN. The provider comes from settings.crn_sms_provider:

  log   (default) the message is recorded in the CRN history and the app log but
        not sent. Used until the hospital's SMS provider is set up, and in tests.
  etec  e-tec SMS (e-tecsms.com). One POST to api.php per batch of up to
        BATCH_SIZE numbers, comma separated in `to`, with the username, password
        and sender name from Admin > CRN > Settings (crn_sms_username,
        crn_sms_password stored encrypted, crn_sms_sender).

  email LAUMC's email-to-SMS gateway (operator, 2026-11-16): one email per mobile through
        the site's Email Server (Administration > Email Server, the relay Report
        Composer's schedules use, Microsoft 365 included). The gateway turns it into an
        SMS. The address and subject are templates (crn_sms_email_to,
        crn_sms_email_subject) with the mobile as {number} (70123456), {intl}
        (96170123456) or {e164} (+96170123456), so "{number}@sms.example.com" and a fixed
        address with the number in the subject both work. The gateway's domain must be
        among the mail server's allowed domains, as for every email RAYD sends.

e-tec answers HTTP 200 whatever happens; a refusal is a text starting with "ERR"
(e.g. "ERR: Not authenticated"). Any other answer counts as sent and is kept as
the message id, so the CRN history shows exactly what e-tec said.
"""
import logging
import re

import requests

from utils.crypto import decrypt

logger = logging.getLogger("CRN_SMS")

PROVIDERS = {'log': 'Dry run (recorded, not sent)', 'etec': 'e-tec SMS',
             'email': 'Email to SMS (through the Email Server)'}
EMAIL_FIELDS = ('number', 'intl', 'e164')
SAMPLE_MOBILE = '+96170123456'
_FIELD = re.compile(r'\{(\w*)\}')
ETEC_URL = 'https://e-tecsms.com/api.php'
BATCH_SIZE = 50            # e-tec takes 50 to 60 numbers per request
TIMEOUT_SECONDS = 15


def etec_number(e164):
    """+9613123456 -> 03123456, +96170123456 -> 70123456: the local form of e-tec's
    examples. A foreign number goes as 00 + its international digits."""
    digits = (e164 or '').lstrip('+')
    if digits.startswith('961'):
        national = digits[3:]
        return '0' + national if len(national) == 7 else national
    return '00' + digits


def etec_configured(cfg):
    return all((cfg.get(k) or '').strip() for k in ('crn_sms_username', 'crn_sms_password', 'crn_sms_sender'))


def _etec_post(cfg, numbers, body):
    """One request. Returns (ok, e-tec's answer or the error)."""
    try:
        r = requests.post(ETEC_URL, timeout=TIMEOUT_SECONDS, data={
            'username': cfg['crn_sms_username'], 'password': decrypt(cfg['crn_sms_password']),
            'action': 'sendsms', 'from': cfg['crn_sms_sender'],
            'to': ','.join(etec_number(n) for n in numbers), 'text': body,
        })
    except requests.RequestException as e:
        return False, f'e-tec SMS unreachable ({e.__class__.__name__})'
    answer = r.text.strip()[:200]
    if r.status_code != 200:
        return False, f'e-tec SMS answered HTTP {r.status_code}: {answer}'
    if not answer or answer.upper().startswith('ERR'):
        return False, f'e-tec SMS refused: {answer or "empty answer"}'
    return True, answer


def send_sms(cfg, to, body):
    """Send one text to a number or to a list of numbers (same text for all).
    Returns {'ok', 'provider', 'message_id', 'dry_run', 'error'}. With several
    batches, ok only if every batch went; error names the ones that did not."""
    provider = cfg.get('crn_sms_provider') or 'log'
    numbers = [to] if isinstance(to, str) else list(dict.fromkeys(to))
    result = {'ok': False, 'provider': provider, 'message_id': None, 'dry_run': False, 'error': None}
    if provider == 'log':
        logger.info(f"[CRN SMS dry run] to={','.join(numbers)} chars={len(body)}")
        return {**result, 'ok': True, 'dry_run': True}
    if provider == 'email':
        return _send_email(cfg, numbers, body, result)
    if provider != 'etec':
        return {**result, 'error': f'unknown SMS provider {provider!r}'}
    if not etec_configured(cfg):
        return {**result, 'error': 'e-tec SMS username, password or sender name is not set'}
    if not numbers:
        return {**result, 'error': 'no number to send to'}

    answers, errors = [], []
    for i in range(0, len(numbers), BATCH_SIZE):
        batch = numbers[i:i + BATCH_SIZE]
        ok, answer = _etec_post(cfg, batch, body)
        if len(numbers) > BATCH_SIZE:
            answer = f'numbers {i + 1}-{i + len(batch)}: {answer}'
        (answers if ok else errors).append(answer)
        logger.log(logging.INFO if ok else logging.WARNING,
                   f"[CRN SMS e-tec] {len(batch)} number(s) chars={len(body)}: {answer}")
    return {**result, 'ok': not errors, 'message_id': ' | '.join(answers) or None,
            'error': ' | '.join(errors) or None}


# ── email to SMS ──────────────────────────────────────────────────────────────

def email_fields(e164):
    """The ways a mobile can be written into the gateway's address or subject."""
    digits = (e164 or '').lstrip('+')
    return {'number': etec_number(e164), 'intl': digits, 'e164': '+' + digits}


def _render(template, e164):
    fields = email_fields(e164)
    return _FIELD.sub(lambda m: fields.get(m.group(1), m.group(0)), template or '').strip()


def email_target(cfg, e164):
    """(address, subject) for one mobile."""
    return _render(cfg.get('crn_sms_email_to'), e164), _render(cfg.get('crn_sms_email_subject'), e164)


def email_problem(cfg, mail=None):
    """Why email to SMS cannot send with these settings, in words for the admin; None when
    it can. mail: the Email Server settings (read when not given)."""
    from utils import worklist_email as mailer
    to_t = (cfg.get('crn_sms_email_to') or '').strip()
    subject_t = (cfg.get('crn_sms_email_subject') or '').strip()
    if not to_t:
        return 'Email to SMS: enter the gateway address, e.g. {number}@sms.example.com'
    used = _FIELD.findall(to_t) + _FIELD.findall(subject_t)
    unknown = sorted({f for f in used if f not in EMAIL_FIELDS})
    if unknown:
        return ('Email to SMS: the address and subject may only use {number}, {intl} or {e164} '
                f'(not {", ".join("{" + u + "}" for u in unknown)})')
    if not any(f in EMAIL_FIELDS for f in used):
        return 'Email to SMS: put the mobile in the address or the subject: {number}, {intl} or {e164}'
    address, _ = email_target(cfg, SAMPLE_MOBILE)
    if not mailer._ADDRESS.fullmatch(address):
        return f'Email to SMS: {address!r} is not an email address'
    mail = mail if mail is not None else mailer.mail_config()
    if not mailer.is_ready(mail):
        return 'Email to SMS: the Email Server is not set up (Administration > Email Server)'
    domain = address.rsplit('@', 1)[1].lower()
    if domain not in mailer.allowed_domains(mail):
        return (f'Email to SMS: add {domain} to the allowed domains on the Email Server '
                '(Administration > Email Server)')
    return None


def _send_email(cfg, numbers, body, result):
    from utils import worklist_email as mailer
    mail = mailer.mail_config()
    problem = email_problem(cfg, mail)
    if problem:
        return {**result, 'error': problem}
    if not numbers:
        return {**result, 'error': 'no number to send to'}
    sent, errors = [], []
    for number in numbers:
        address, subject = email_target(cfg, number)
        try:
            refused = mailer.send_mail(mail, [address], subject, body)
        except Exception as e:              # the relay refused or could not be reached
            errors.append(f'{address}: {str(e)[:200]}')
            continue
        if refused:
            errors.append(f'{address}: refused by the mail server')
        else:
            sent.append(address)
        logger.log(logging.INFO if not refused else logging.WARNING,
                   f"[CRN SMS email] to={address} chars={len(body)} refused={bool(refused)}")
    return {**result, 'ok': not errors, 'message_id': ('emailed ' + ', '.join(sent)) if sent else None,
            'error': ' | '.join(errors) or None}
