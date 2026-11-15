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

e-tec answers HTTP 200 whatever happens; a refusal is a text starting with "ERR"
(e.g. "ERR: Not authenticated"). Any other answer counts as sent and is kept as
the message id, so the CRN history shows exactly what e-tec said.
"""
import logging

import requests

from utils.crypto import decrypt

logger = logging.getLogger("CRN_SMS")

PROVIDERS = {'log': 'Dry run (recorded, not sent)', 'etec': 'e-tec SMS'}
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
