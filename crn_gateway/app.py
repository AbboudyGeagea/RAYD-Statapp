"""
crn_gateway/app.py
──────────────────
CRN gateway: the only part of CRN reachable from the internet. It runs on its own
VM in the DMZ and serves one thing to doctors: the notification page behind each
SMS link, with an "I acknowledge" button.

It never connects into the hospital. RAYD (inside) calls it:
  POST /api/pages        place or refresh a notification page
  GET  /api/events       collect opens and acknowledgements (cursor ?after=<id>)
  GET  /api/health
Doctors call:
  GET  /n/<token>        the page (each open is logged: time, IP, browser)
  POST /n/<token>/ack    acknowledge (optional comment)

Storage is one SQLite file: pages hold the patient data encrypted with
CRN_GATEWAY_DATA_KEY and are deleted CRN_GATEWAY_RETENTION_DAYS after
acknowledgement, or when their link expires. The permanent record stays in RAYD.
Pages are looked up by the SHA-256 of the link token; the gateway never stores
the token itself. Acknowledging one recipient's page acknowledges the
notification for every recipient (same reference code).

Environment:
  CRN_GATEWAY_API_KEY        shared secret RAYD sends as "Authorization: Bearer ..."
  CRN_GATEWAY_DATA_KEY       Fernet key for page data at rest
                             (python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  CRN_GATEWAY_API_ALLOW      comma-separated IPs allowed to call /api (RAYD's address); empty = any
  CRN_GATEWAY_TRUST_PROXY    1 when behind the TLS reverse proxy, so the client IP comes from X-Forwarded-For
  CRN_GATEWAY_DB             SQLite path (default /data/gateway.db)
  CRN_GATEWAY_RETENTION_DAYS default 7
"""
import hashlib
import hmac
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet, InvalidToken
from flask import Flask, abort, g, jsonify, render_template, request

DB_PATH = os.environ.get('CRN_GATEWAY_DB', '/data/gateway.db')
RETENTION_DAYS = int(os.environ.get('CRN_GATEWAY_RETENTION_DAYS', '7'))
MAX_COMMENT = 500

app = Flask(__name__, root_path=os.path.dirname(os.path.abspath(__file__)))


# ── storage ───────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
    token_hash       TEXT PRIMARY KEY,
    ref_code         TEXT NOT NULL,
    recipient_id     INTEGER NOT NULL,
    payload_enc      TEXT,
    expires_at       TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    acknowledged_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pages_ref ON pages (ref_code);
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash    TEXT NOT NULL,
    ref_code      TEXT NOT NULL,
    recipient_id  INTEGER NOT NULL,
    event_type    TEXT NOT NULL,
    at            TEXT NOT NULL,
    ip            TEXT,
    user_agent    TEXT,
    comment       TEXT
);
"""


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec='seconds')


def db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH, timeout=10)
        g.db.row_factory = sqlite3.Row
        g.db.executescript(SCHEMA)
    return g.db


@app.teardown_appcontext
def _close(_exc):
    conn = g.pop('db', None)
    if conn is not None:
        conn.close()


def _fernet():
    key = os.environ.get('CRN_GATEWAY_DATA_KEY', '')
    if not key:
        raise RuntimeError('CRN_GATEWAY_DATA_KEY is not set')
    return Fernet(key.encode())


def _purge(conn):
    """Delete page data once it is no longer needed: expired, or acknowledged
    more than RETENTION_DAYS ago. Events stay until RAYD has collected them."""
    now = _now()
    conn.execute("DELETE FROM pages WHERE expires_at < ?", (_iso(now),))
    conn.execute("DELETE FROM pages WHERE acknowledged_at IS NOT NULL AND acknowledged_at < ?",
                 (_iso(now - timedelta(days=RETENTION_DAYS)),))
    conn.commit()


# ── security helpers ──────────────────────────────────────────────────────────

def _client_ip():
    if os.environ.get('CRN_GATEWAY_TRUST_PROXY') == '1':
        forwarded = request.headers.get('X-Forwarded-For', '')
        if forwarded:
            return forwarded.split(',')[0].strip()
    return request.remote_addr


def _require_rayd():
    allow = [a.strip() for a in os.environ.get('CRN_GATEWAY_API_ALLOW', '').split(',') if a.strip()]
    if allow and _client_ip() not in allow:
        abort(403)
    key = os.environ.get('CRN_GATEWAY_API_KEY', '')
    sent = request.headers.get('Authorization', '')
    if not key or not hmac.compare_digest(sent, f'Bearer {key}'):
        abort(401)


@app.after_request
def _headers(resp):
    # The token is in the URL: never leak it as a Referer, never cache the page.
    resp.headers['Referrer-Policy'] = 'no-referrer'
    resp.headers['Cache-Control'] = 'no-store'
    resp.headers['X-Frame-Options'] = 'DENY'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['Content-Security-Policy'] = "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:"
    return resp


# ── RAYD API ──────────────────────────────────────────────────────────────────

@app.route('/api/health')
def health():
    _require_rayd()
    db().execute('SELECT 1')
    return jsonify(ok=True, time=_iso(_now()))


@app.route('/api/pages', methods=['POST'])
def put_page():
    """Place (or refresh) one recipient's page. Idempotent on token_hash."""
    _require_rayd()
    body = request.get_json(silent=True) or {}
    try:
        token_hash = str(body['token_hash'])
        ref_code = str(body['ref_code'])
        recipient_id = int(body['recipient_id'])
        expires_at = datetime.fromisoformat(body['expires_at'])
        payload = body['payload']
    except (KeyError, TypeError, ValueError):
        abort(400)
    if len(token_hash) != 64 or not isinstance(payload, dict):
        abort(400)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    conn = db()
    acked = conn.execute("SELECT MIN(acknowledged_at) FROM pages WHERE ref_code = ?", (ref_code,)).fetchone()[0]
    conn.execute("""
        INSERT INTO pages (token_hash, ref_code, recipient_id, payload_enc, expires_at, created_at, acknowledged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (token_hash) DO UPDATE SET payload_enc = excluded.payload_enc,
                                              expires_at  = excluded.expires_at
    """, (token_hash, ref_code, recipient_id,
          _fernet().encrypt(json.dumps(payload).encode()).decode(),
          _iso(expires_at), _iso(_now()), acked))
    conn.commit()
    return jsonify(ok=True)


@app.route('/api/events')
def get_events():
    """Opens and acknowledgements after ?after=<id>, oldest first, at most 500."""
    _require_rayd()
    after = request.args.get('after', '0')
    try:
        after = int(after)
    except ValueError:
        abort(400)
    conn = db()
    _purge(conn)
    rows = conn.execute("""
        SELECT id, token_hash, ref_code, recipient_id, event_type, at, ip, user_agent, comment
        FROM events WHERE id > ? ORDER BY id LIMIT 500
    """, (after,)).fetchall()
    return jsonify(events=[dict(r) for r in rows])


# ── doctor-facing page ────────────────────────────────────────────────────────

def _page(token):
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    row = db().execute("SELECT * FROM pages WHERE token_hash = ?", (token_hash,)).fetchone()
    if row is None or row['payload_enc'] is None:
        return token_hash, None, None
    if datetime.fromisoformat(row['expires_at']) < _now():
        return token_hash, row, None
    try:
        payload = json.loads(_fernet().decrypt(row['payload_enc'].encode()))
    except InvalidToken:
        return token_hash, row, None
    return token_hash, row, payload


def _log(row, event_type, comment=None):
    db().execute("""
        INSERT INTO events (token_hash, ref_code, recipient_id, event_type, at, ip, user_agent, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (row['token_hash'], row['ref_code'], row['recipient_id'], event_type, _iso(_now()),
          _client_ip(), (request.headers.get('User-Agent') or '')[:300], comment))
    db().commit()


@app.route('/n/<token>')
def show(token):
    _token_hash, row, payload = _page(token)
    if payload is None:
        return render_template('message.html', title='Link not available',
                               text='This link has expired or is not valid. '
                                    'Please contact the radiology department.'), 404
    _log(row, 'opened')
    acked = db().execute("SELECT MIN(acknowledged_at) FROM pages WHERE ref_code = ?",
                         (row['ref_code'],)).fetchone()[0]
    logo = os.path.exists(os.path.join(app.static_folder, 'logo.png'))   # hospital logo, optional
    return render_template('page.html', p=payload, ref=row['ref_code'], token=token,
                           acknowledged_at=acked, logo=logo)


@app.route('/n/<token>/ack', methods=['POST'])
def acknowledge(token):
    _token_hash, row, payload = _page(token)
    if payload is None:
        return render_template('message.html', title='Link not available',
                               text='This link has expired or is not valid. '
                                    'Please contact the radiology department.'), 404
    conn = db()
    already = conn.execute("SELECT MIN(acknowledged_at) FROM pages WHERE ref_code = ?",
                           (row['ref_code'],)).fetchone()[0]
    if not already:
        comment = (request.form.get('comment') or '').strip()[:MAX_COMMENT] or None
        now = _iso(_now())
        conn.execute("UPDATE pages SET acknowledged_at = ? WHERE ref_code = ? AND acknowledged_at IS NULL",
                     (now, row['ref_code']))
        conn.commit()
        _log(row, 'acknowledged', comment)
    return render_template('message.html', title='Acknowledged',
                           text=f"Thank you. Critical result {row['ref_code']} is recorded as acknowledged.")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8443)
