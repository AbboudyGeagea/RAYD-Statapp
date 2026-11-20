"""
tests/test_report_27_peak_hours.py — Report 27 (RIS orders vs PACS), operator 2026-11-20: under
the hourly load, each modality's peak hour per day of the week (on exams started); the Modality
Distribution (AE) and Demographics charts removed.
"""
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}


def test_the_peak_hour_of_each_modality_on_each_weekday():
    from routes.report_27 import _peak_hours
    started = (['2026-10-05 10:05', '2026-10-05 10:40', '2026-10-05 10:55', '2026-10-05 11:10',     # Mon: CT
                '2026-10-12 10:20'] +                                                              # next Mon
               ['2026-10-06 14:00', '2026-10-06 14:30', '2026-10-06 09:00'])                       # Tue: MR
    df = pd.DataFrame({'modality': ['CT'] * 5 + ['MR'] * 3,
                       'started_at': started})
    df = pd.concat([df, pd.DataFrame({'modality': ['CT'], 'started_at': [None]})])     # not started yet
    ph = _peak_hours(df, '2026-10-05', '2026-10-18')                                     # two of each weekday
    assert ph['weekdays'][0] == 'Mon' and [r['modality'] for r in ph['rows']] == ['CT', 'MR']
    ct, mr = ph['rows']
    assert ct['cells'][0] == {'hour': 10, 'count': 4, 'avg': 2.0, 'next': 11}           # 4 at 10:00 over 2 Mondays
    assert ct['cells'][1] is None and ct['total'] == 5
    assert mr['cells'][1] == {'hour': 14, 'count': 2, 'avg': 1.0, 'next': 9}
    assert _peak_hours(df[df['started_at'].isna()], '2026-10-05', '2026-10-18') is None


def test_a_tie_goes_to_the_earlier_hour():
    from routes.report_27 import _peak_hours
    df = pd.DataFrame({'modality': ['US', 'US'], 'started_at': ['2026-10-05 15:00', '2026-10-05 08:00']})
    assert _peak_hours(df, '2026-10-05', '2026-10-05')['rows'][0]['cells'][0]['hour'] == 8


@pytest.fixture
def orders(app):
    from db import db
    with app.app_context():
        for i, (mod, when, start) in enumerate((('CT', '2026-10-05 09:00', '2026-10-05 10:10'),
                                                ('CT', '2026-10-05 09:30', '2026-10-05 10:40'),
                                                ('MR', '2026-10-06 13:00', '2026-10-06 14:05'))):
            db.session.execute(text("""INSERT INTO etl_orders (order_dbid, scheduled_datetime, modality, order_status)
                                       VALUES (:o, :s, :m, 'exam_done')"""), {'o': 7100 + i, 's': when, 'm': mod})
            db.session.execute(text("""INSERT INTO worklist_status_history (site_worklist_key, stage, source_time)
                                       VALUES (:o, 'in_progress', :t)"""), {'o': 7100 + i, 't': start})
        db.session.commit()
        yield
        db.session.execute(text("DELETE FROM worklist_status_history WHERE site_worklist_key BETWEEN 7100 AND 7199"))
        db.session.commit()


def test_the_page_shows_the_peak_hours_and_no_longer_the_two_charts(client, orders):
    html = client.get('/report/27', headers=ADMIN,
                      query_string={'start_date': '2026-10-05', 'end_date': '2026-10-11'}).get_data(as_text=True)
    assert 'Peak Hours per Modality and Day' in html and 'data-explain="r27-peak-hours"' in html
    peak = html[html.index('id="peak-hours-card"'):html.index('<!-- Row 1b')]
    assert '>CT <span' in peak and '>MR <span' in peak and '10:00</div>' in peak and '14:00</div>' in peak
    # under the hourly load, above the weekly pattern
    assert html.index('Hourly Load (Volume Trend)') < html.index('Peak Hours per Modality') < html.index('Busiest Day &amp; Time')
    for gone in ('Demographics (Age', 'Modality Distribution (AE)', 'chart_demo', 'chart_ae', 'r27-demo', 'r27-ae-mix'):
        assert gone not in html, gone
    assert 'Order Status Mix' in html and 'Busiest Day of Month' in html
