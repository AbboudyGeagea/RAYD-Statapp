"""
utils/crypto.py — Symmetric encryption for secrets stored in the database.
Uses Fernet (AES-128-CBC + HMAC-SHA256).

Key precedence: ENCRYPTION_KEY env var (preferred) → SECRET_KEY (fallback).
Set ENCRYPTION_KEY in production to decouple DB credential encryption from
Flask session signing.
"""
import base64
import hashlib
import logging
import os

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)


def _get_fernet():
    # Prefer a dedicated encryption key so session key and DB key are independent.
    secret = os.environ.get('ENCRYPTION_KEY') or os.environ.get('SECRET_KEY', '')
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
    return Fernet(key)


def encrypt(plaintext: str) -> str:
    if not plaintext:
        return ''
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    if not ciphertext:
        return ''
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        # Pre-migration plaintext fallback — credentials stored before encryption was added.
        # Log a warning so this is visible; remove once all passwords are re-encrypted.
        logger.warning("decrypt(): Fernet token invalid — returning raw value (pre-migration plaintext?)")
        return ciphertext
    except Exception as e:
        logger.error(f"decrypt(): unexpected error — {e}")
        return ciphertext
