"""
utils/crn_sms.py
────────────────
SMS sending for CRN. The provider comes from settings.crn_sms_provider:

  log   (default) the message is recorded in the CRN history and the app log but
        not sent. Used until the hospital's SMS provider is set up, and in tests.

Real providers (Twilio, or a local SMS gateway with an HTTPS API) plug in here,
behind the same send_sms() contract.
"""
import logging

logger = logging.getLogger("CRN_SMS")


def send_sms(provider, to, body):
    """Send one SMS. Returns {'ok', 'provider', 'message_id', 'dry_run', 'error'}."""
    if provider == 'log':
        logger.info(f"[CRN SMS dry run] to={to} chars={len(body)}")
        return {'ok': True, 'provider': 'log', 'message_id': None, 'dry_run': True, 'error': None}
    return {'ok': False, 'provider': provider, 'message_id': None, 'dry_run': False,
            'error': f'unknown SMS provider {provider!r}'}
