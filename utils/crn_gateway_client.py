"""
utils/crn_gateway_client.py
───────────────────────────
RAYD's side of the CRN gateway API (crn_gateway/app.py). RAYD always calls the
gateway; the gateway never connects into the hospital.

  push_page()     place a recipient's notification page before their SMS goes out
  fetch_events()  collect opens and acknowledgements after a cursor

The shared API key lives in settings.crn_gateway_api_key, encrypted with
utils/crypto like the other external credentials. A plain value is accepted too
until the CRN admin settings page writes it encrypted.
"""
import requests

from utils.crypto import decrypt

TIMEOUT_SECONDS = 10


def api_key(cfg):
    raw = (cfg.get('crn_gateway_api_key') or '').strip()
    if raw.startswith('gAAAA'):        # a Fernet token: stored encrypted
        return decrypt(raw)
    return raw


def configured(cfg):
    return bool((cfg.get('crn_gateway_url') or '').strip()) and bool(api_key(cfg))


def _url(cfg, path):
    return cfg['crn_gateway_url'].strip().rstrip('/') + path


def _headers(cfg):
    return {'Authorization': f'Bearer {api_key(cfg)}'}


def push_page(cfg, token_hash, ref_code, recipient_id, expires_at_iso, payload):
    """Place one recipient's page. Returns (ok, error)."""
    try:
        r = requests.post(_url(cfg, '/api/pages'), headers=_headers(cfg), timeout=TIMEOUT_SECONDS, json={
            'token_hash': token_hash, 'ref_code': ref_code, 'recipient_id': recipient_id,
            'expires_at': expires_at_iso, 'payload': payload,
        })
    except requests.RequestException as e:
        return False, f'gateway unreachable ({e.__class__.__name__})'
    if r.status_code == 200:
        return True, None
    return False, f'gateway answered HTTP {r.status_code}'


def fetch_events(cfg, after):
    """Gateway events with id > after, oldest first (at most 500 per call)."""
    r = requests.get(_url(cfg, '/api/events'), headers=_headers(cfg), params={'after': after},
                     timeout=TIMEOUT_SECONDS)
    r.raise_for_status()
    return r.json().get('events', [])
