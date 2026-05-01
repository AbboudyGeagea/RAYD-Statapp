"""
utils/hl7_forward.py
────────────────────
Fire-and-forget MLLP forwarding of raw HL7 messages.

Called from arrive_order() and arrive_hl7() after the SC→AR transition.
Reads host/port/enabled from the settings table on every call (5-min cache),
so config changes take effect without a restart.

The raw message is sent exactly as stored — never modified.
"""
import socket
import threading
import logging
import time

logger = logging.getLogger("HL7_FORWARD")

MLLP_START = b'\x0b'       # VT  — start of block
MLLP_END   = b'\x1c\x0d'  # FS + CR — end of block
TIMEOUT    = 5             # seconds for connect + read

_cfg_cache     = None
_cfg_loaded_at = 0.0
_CFG_TTL       = 300  # 5-minute cache


def _load_config(app):
    try:
        with app.app_context():
            from sqlalchemy import text
            from db import db
            rows = db.session.execute(text("""
                SELECT key, value FROM settings
                WHERE key IN ('hl7_forward_host','hl7_forward_port','hl7_forward_enabled')
            """)).fetchall()
            return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.error(f"HL7 forward config read failed: {e}")
        return {}


def _get_config(app):
    global _cfg_cache, _cfg_loaded_at
    if _cfg_cache is None or (time.time() - _cfg_loaded_at) > _CFG_TTL:
        _cfg_cache     = _load_config(app)
        _cfg_loaded_at = time.time()
    return _cfg_cache


def invalidate_cache():
    """Call after saving new config so the next send picks it up immediately."""
    global _cfg_cache
    _cfg_cache = None


def forward_message(raw_message, app, order_id=None):
    """
    Send raw_message to the configured MLLP destination in a background thread.
    Returns immediately — never blocks the caller.
    Does nothing (silently) if forwarding is disabled or unconfigured.
    """
    if not raw_message:
        return

    def _send():
        cfg = _get_config(app)
        if cfg.get('hl7_forward_enabled', '0').lower() not in ('1', 'true', 'yes'):
            return
        host = (cfg.get('hl7_forward_host') or '').strip()
        port_str = (cfg.get('hl7_forward_port') or '').strip()
        if not host or not port_str:
            return
        try:
            port = int(port_str)
        except ValueError:
            logger.error(f"HL7 forward: invalid port '{port_str}'")
            return

        try:
            payload = MLLP_START + raw_message.encode('utf-8', errors='replace') + MLLP_END
            with socket.create_connection((host, port), timeout=TIMEOUT) as sock:
                sock.sendall(payload)
                sock.settimeout(TIMEOUT)
                try:
                    ack = sock.recv(4096)
                    logger.info(f"HL7 forward OK → {host}:{port} order={order_id} ack_len={len(ack)}")
                except socket.timeout:
                    logger.warning(f"HL7 forward sent (no ACK received) → {host}:{port} order={order_id}")
        except Exception as e:
            logger.error(f"HL7 forward FAILED → {host}:{port} order={order_id}: {e}")

    threading.Thread(target=_send, daemon=True, name=f"HL7-FWD-{order_id}").start()


def test_forward(host, port, raw_message):
    """
    Synchronous test send — used by the admin config page.
    Returns (ok: bool, message: str).
    """
    if not raw_message:
        return False, "No sample message available."
    try:
        port = int(port)
        payload = MLLP_START + raw_message.encode('utf-8', errors='replace') + MLLP_END
        with socket.create_connection((host, port), timeout=TIMEOUT) as sock:
            sock.sendall(payload)
            sock.settimeout(TIMEOUT)
            try:
                ack = sock.recv(4096)
                ack_str = ack.strip(MLLP_START).strip(MLLP_END).decode('utf-8', errors='replace')
                return True, f"ACK received ({len(ack)} bytes): {ack_str[:120]}"
            except socket.timeout:
                return True, "Message sent — no ACK received within 5 s (destination may not respond)."
    except Exception as e:
        return False, str(e)
