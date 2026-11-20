"""
tests/test_startup_lock_timeouts.py — the app starts even when a query left running by the previous
container holds a table (2026-11-20: update.sh hung on "Waiting for app to respond": the startup's
ALTER TABLE ... IF NOT EXISTS and the rh views refresh waited for ever for their locks).
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def read(name):
    return open(os.path.join(ROOT, name), encoding='utf-8').read()


def test_every_startup_schema_transaction_sets_a_short_lock_timeout():
    src = read('app.py')
    body = src[src.index('    def _startup_schema():'):src.index('\n    _startup_schema()\n')]
    tries = re.findall(r'\n            try:\n(.*)\n', body)
    assert len(tries) >= 8
    assert all("SET LOCAL lock_timeout" in first for first in tries), tries


def test_the_views_refresh_at_startup_gives_up_on_a_lock():
    src = read('db_migrations.py')
    body = src[src.index('def _refresh_rh_views'):src.index('def _run_statements')]
    assert body.index("SET LOCAL lock_timeout") < body.index('rh_refresh_views()')


def test_update_sh_cannot_hang_on_the_health_check():
    src = read('update.sh')
    checks = re.findall(r'docker exec rayd_service curl[^\n]*', src)
    assert checks and all('--max-time' in c for c in checks)
    assert 'pg_blocking_pids' in src                    # says what blocks the app when it does not answer
