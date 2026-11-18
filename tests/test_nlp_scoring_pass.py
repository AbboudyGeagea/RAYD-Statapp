"""
tests/test_nlp_scoring_pass.py
────────────────────────────────────────────────────────────────
The NLP worker's re-scoring after "Rebuild clusters" (nlp_worker/worker.py, 19 Nov 2026). On
18 Nov a rebuild made all 508,352 ORU reports stale and the old loop, which found each next
chunk by scanning past every report already done, ran Postgres at full CPU for hours and slowed
every report page. These pin down the new pass: it walks down the ids from where it stopped,
scores everything exactly once per model, picks up new reports at the top, and starts over for
a new model. The NLP itself (medspaCy, the clustering) is stubbed: only the walk is tested.
"""
import os
import sys
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from sqlalchemy import text

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def worker(app, database_url, monkeypatch):
    u = urlparse(database_url)
    for k, v in (('POSTGRES_HOST', u.hostname), ('POSTGRES_PORT', str(u.port or 5432)),
                 ('POSTGRES_DB', u.path.lstrip('/')), ('POSTGRES_USER', u.username),
                 ('POSTGRES_PASSWORD', u.password or '')):
        monkeypatch.setenv(k, v)
    sys.path.insert(0, os.path.join(ROOT, 'nlp_worker'))
    try:
        import worker as w
    finally:
        sys.path.pop(0)
    monkeypatch.setattr(w, '_CHUNK', 7)
    monkeypatch.setattr(w, '_SCORE_PAUSE_SECONDS', 0)
    monkeypatch.setattr(w, '_negation_resolved_texts', lambda texts: texts)
    monkeypatch.setattr(w.clustering, 'score_report', lambda t, res, model: {
        'classification': 'normal', 'keywords': [], 'cluster_id': 0, 'cluster_label': 'x', 'severity_score': 0})
    model = {'id': None}
    monkeypatch.setattr(w, '_current_model', lambda conn: (model['id'], object()))
    w._SCAN.update(model=None, below=None, done=False, recheck_at=0.0)
    w.model = model
    yield w
    w._SCAN.update(model=None, below=None, done=False, recheck_at=0.0)


def sql(app, statement, **params):
    from db import db
    with app.app_context():
        res = db.session.execute(text(statement), params)
        out = res.fetchall() if res.returns_rows else None
        db.session.commit()
        return out


def new_model(app):
    return sql(app, """INSERT INTO oru_cluster_models (nlp_version, sample_size, terms, idf, centroids, labels)
                       VALUES ('tfidf-negation-v3', 1, '[]', '[]', '[]', '[]') RETURNING id""")[0][0]


def reports(app, first, last):
    sql(app, """INSERT INTO hl7_oru_reports (id, accession_number, report_text)
                SELECT g, 'A' || g, 'report ' || g FROM generate_series(:a, :b) g""", a=first, b=last)


def scored(app, model_id):
    return sql(app, "SELECT COUNT(*) FROM ai_nlp_cache WHERE cluster_model_id = :m", m=model_id)[0][0]


def test_a_rebuild_is_walked_once_down_the_ids_and_new_reports_join_at_the_top(app, worker, monkeypatch):
    reports(app, 1, 40)
    worker.model['id'] = new_model(app)
    m = worker.model['id']
    monkeypatch.setattr(worker, '_SCORE_BUDGET_SECONDS', 3600)
    assert worker.run_scoring() is False                 # the whole pass in one call
    assert scored(app, m) == 40 and worker._SCAN['done']

    reports(app, 41, 45)                                 # arrive after the pass
    worker.run_scoring()
    assert scored(app, m) == 45                          # found at the top, no new pass
    assert worker._SCAN['done']


def test_the_pass_resumes_below_where_it_stopped(app, worker, monkeypatch):
    reports(app, 1, 30)
    worker.model['id'] = new_model(app)
    m = worker.model['id']
    clock = iter(range(0, 10 ** 6, 10))                  # every check of the time moves 10 s on
    monkeypatch.setattr(worker, 'time', SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda s: None))
    monkeypatch.setattr(worker, '_SCORE_BUDGET_SECONDS', 15)
    assert worker.run_scoring() is True                  # stopped on the budget, more to do
    first = scored(app, m)
    assert 0 < first < 30
    below = worker._SCAN['below']
    assert sql(app, "SELECT MIN(source_id) FROM ai_nlp_cache WHERE cluster_model_id = :m", m=m)[0][0] == below
    while worker.run_scoring():
        pass
    assert scored(app, m) == 30
    assert sql(app, "SELECT COUNT(*) FROM ai_nlp_cache")[0][0] == 30          # each report once


def test_a_new_model_starts_a_new_pass_from_the_top(app, worker, monkeypatch):
    reports(app, 1, 20)
    worker.model['id'] = new_model(app)
    monkeypatch.setattr(worker, '_SCORE_BUDGET_SECONDS', 3600)
    worker.run_scoring()
    worker.model['id'] = m2 = new_model(app)
    worker.run_scoring()
    assert scored(app, m2) == 20 and worker._SCAN['model'] == m2
