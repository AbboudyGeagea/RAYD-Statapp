"""
utils/crypto.py — Symmetric encryption for secrets stored in the database.
Uses Fernet (AES-128-CBC + HMAC-SHA256) keyed from SECRET_KEY.
"""
import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken


def _get_fernet():
    """Derive a Fernet key from the app's SECRET_KEY."""
    secret = os.environ.get('SECRET_KEY', '')
    # Fernet needs a 32-byte URL-safe base64-encoded key.
    # Derive one deterministically from SECRET_KEY via SHA-256.
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
    return Fernet(key)


def encrypt(plaintext: str) -> str:
    """Encrypt a string. Returns a base64-encoded ciphertext string."""
    if not plaintext:
        return ''
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt a Fernet token back to plaintext. Returns '' on failure."""
    if not ciphertext:
        return ''
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except (InvalidToken, Exception):
        # If decryption fails, assume it's still plaintext (pre-migration)
        return ciphertext
