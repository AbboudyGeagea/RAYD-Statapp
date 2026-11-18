"""
tests/test_report_29_speed.py
────────────────────────────────────────────────────────────────
Report 29, Infrastructure & Storage Audit (operator, 19 Nov 2026: "extremely slow to open and to
load data"). Opening the page ran the whole report once only to learn the default dates, and the
report fetched one row per day per procedure and device to add them up again in pandas. Now the
page opens without touching summary_storage_daily, Generate runs one query of totals, and the
Excel/CSV export keeps the per-day detail.
"""
import pytest
from sqlalchemy import event, text

ADMIN = {'X-Test-Role': 'administrator'}


@pytest.fixture
def storage_queries(app):
    from db import db
    seen = []

    def before(conn, cursor, statement, params, context, executemany):
        if 'summary_storage_daily' in statement and statement.lstrip().upper().startswith('SELECT'):
            seen.append(statement)

    with app.app_context():
        event.listen(db.engine, 'before_cursor_execute', before)
        db.session.execute(text("""
            INSERT INTO summary_storage_daily (id, study_date, modality, procedure_code, total_gb, study_count, storing_ae)
            VALUES (1, '2026-10-01', 'CT', 'CT01', 2.0, 4, 'CTX1'), (2, '2026-10-02', 'CT', 'CT01', 1.0, 2, 'CTX1'),
                   (3, '2026-10-02', 'MR', 'MR01', 3.0, 1, 'MRX1')"""))
        db.session.commit()
        yield seen
        event.remove(db.engine, 'before_cursor_execute', before)


def test_opening_the_page_runs_no_storage_query(client, storage_queries):
    assert client.get('/report/29', headers=ADMIN).status_code == 200
    assert storage_queries == []


def test_generate_fetches_totals_not_days(client, storage_queries):
    html = client.get('/report/29', headers=ADMIN,
                      query_string={'start_date': '2026-10-01', 'end_date': '2026-10-31'}).get_data(as_text=True)
    assert len(storage_queries) == 1
    assert 'study_date' not in storage_queries[0].split('GROUP BY')[1]
    assert 'Critical' in html or 'Optimized' in html or 'PROTO_DATA' in html


def test_the_totals_add_up_per_procedure_and_device(storage_queries):
    from routes.report_29 import get_report_data
    df, _s, _e = get_report_data({'start_date': '2026-10-01', 'end_date': '2026-10-31'})
    ct = df[df.modality == 'CT'].iloc[0]
    assert len(df) == 2 and ct.study_count == 6 and float(ct.total_gb) == 3.0


def test_the_export_keeps_one_row_per_day(client, storage_queries):
    resp = client.post('/report/29/export', headers=ADMIN, data={'start_date': '2026-10-01', 'end_date': '2026-10-31'})
    assert resp.status_code == 200
    lines = resp.get_data(as_text=True).strip().splitlines()
    assert 'study_date' in lines[0].split(',') and len(lines) == 4    # header + 3 day rows
