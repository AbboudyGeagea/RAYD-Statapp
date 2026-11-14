"""
tests/test_report_27_hourly.py — item 6 of the meeting of 2026-11-13: Report 27's hourly
load shows, per hour, the average per day over the range of exams scheduled and exams
started (RIS status Started), and the busiest day is counted on started exams, Monday to
Friday ranked, Saturday and Sunday apart.
"""
import pytest
from sqlalchemy import text

pd = pytest.importorskip('pandas')


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def test_started_time_comes_from_the_ris_status_history():
    from routes.report_27 import get_report_data
    sql("""INSERT INTO etl_orders (order_dbid, accession_number, order_status, scheduled_datetime)
           VALUES (1, 'A1', 'CM', '2026-11-02 09:00'), (2, 'A2', 'SC', '2026-11-02 10:00')""")
    sql("""INSERT INTO worklist_status_history (site_worklist_key, stage, source_time)
           VALUES (1, 'arrived', '2026-11-02 09:05'), (1, 'in_progress', '2026-11-02 09:40')""")
    df = get_report_data('2026-11-02', '2026-11-02').set_index('order_dbid')
    assert str(df.loc[1, 'started_at']) == '2026-11-02 09:40:00'   # Started, not Arrived
    assert pd.isna(df.loc[2, 'started_at'])                       # never started


def test_hourly_load_is_an_average_per_day():
    from routes.report_27 import _hourly_load
    df = pd.DataFrame({
        'scheduled_datetime': pd.to_datetime(['2026-11-02 09:00', '2026-11-03 09:30', '2026-11-03 10:00']),
        'started_at': pd.to_datetime(['2026-11-02 09:50', '2026-11-03 10:20', None]),
    })
    h = _hourly_load(df, '2026-11-02', '2026-11-03')            # 2 days
    assert h['days'] == 2
    assert (h['scheduled'][9], h['scheduled'][10]) == (1.0, 0.5)
    assert (h['started'][9], h['started'][10]) == (0.5, 0.5)


def test_busiest_days_rank_weekdays_and_keep_the_weekend_apart():
    from routes.report_27 import _busiest_days
    # Mon 2 Nov to Sun 8 Nov 2026: one of each weekday
    started = ['2026-11-04 09:00'] * 3 + ['2026-11-02 09:00'] * 2 + ['2026-11-07 09:00', '2026-11-08 02:00']
    df = pd.DataFrame({'started_at': pd.to_datetime(started)})
    b = _busiest_days(df, '2026-11-02', '2026-11-08')
    assert [w['day'] for w in b['weekdays'][:2]] == ['Wednesday', 'Monday']
    assert len(b['weekdays']) == 5
    assert (b['saturday'], b['sunday']) == (1.0, 1.0)
