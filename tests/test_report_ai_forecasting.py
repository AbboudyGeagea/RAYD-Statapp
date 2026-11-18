"""
tests/test_report_ai_forecasting.py
────────────────────────────────────────────────────────────────
Report AI's forecasts (operator, 2026-11-18; migration 0169, utils/forecasting.py): Prophet with
the holiday calendar plus a sequential correction, trained every night, stored, and shown by
routes/report_ai.py. These pin down what the operator asked for: the week (Saturdays and Sundays
far below weekdays), holidays, the period right after following the last days, the calendar
editable by administrators only and shared by both hospitals, the next-day check, and referrers
judged by a test instead of a straight line.
"""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from utils import forecasting

ADMIN = {'X-Test-Role': 'administrator'}
USER = {'X-Test-Role': 'user'}
SHAPE = (1.04, 1.0, 1.0, 1.02, 0.95, 0.35, 0.24)       # LAUMC's week, measured 2019-2026

needs_prophet = pytest.mark.skipif(forecasting._prophet_class() is None, reason='prophet not installed')


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


def scalar(statement, **params):
    from db import db
    return db.session.execute(text(statement), params).scalar()


def synthetic(end, days=420, closures=(), level=200.0, jump_from=None, jump=1.0, seed=7):
    """A daily series with LAUMC's week, closures at 0.3, and an optional jump in level."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range(end - timedelta(days=days - 1), end)
    vals = []
    for d in idx:
        v = level * SHAPE[d.dayofweek]
        if d in closures:
            v *= 0.3
        if jump_from is not None and d >= jump_from:
            v *= jump
        vals.append(rng.poisson(v))
    return pd.Series(vals, index=idx, dtype=float)


def weekday_closures(end, days, every=23):
    """Closures on weekdays, about one every three weeks, through the history and the future."""
    start = end - timedelta(days=days)
    out = []
    d = pd.Timestamp(start)
    while d <= pd.Timestamp(end + timedelta(days=60)):
        if d.dayofweek <= 4:
            out.append(d)
            d += timedelta(days=every)
        else:
            d += timedelta(days=1)
    return out


def calendar_of(closures, events=()):
    rows = [{'start_date': d.date(), 'end_date': d.date(), 'name': 'Holiday', 'kind': 'closure'} for d in closures]
    rows += [{'start_date': a, 'end_date': b, 'name': 'Event', 'kind': 'event'} for a, b in events]
    return forecasting.CalendarDays(rows)


# ── the calendar ──────────────────────────────────────────────────────────────

def test_the_seeded_calendar_knows_what_the_data_showed():
    rows = {(str(r[0]), r[1]): (r[2], str(r[3])) for r in
            __import__('db').db.session.execute(text(
                "SELECT start_date, name, kind, end_date FROM ai_calendar")).fetchall()}
    assert rows[('2026-04-06', 'Easter Monday (Catholic)')][0] == 'closure'     # not in the official list
    assert rows[('2026-04-13', 'Easter Monday (Orthodox)')][0] == 'closure'
    assert rows[('2026-03-19', 'Eid al-Fitr')][0] == 'closure'                  # a day before the estimate
    assert rows[('2026-06-25', 'Ashura')][0] == 'no_effect'
    assert rows[('2026-03-25', 'Feast of the Annunciation (worked)')][0] == 'no_effect'
    assert rows[('2026-12-24', 'Christmas Eve (half day)')][0] == 'half_day'
    assert rows[('2025-12-02', "Pope's visit (public holiday)")][0] == 'closure'
    assert rows[('2026-11-23', 'Independence Day (moved to Monday)')][0] == 'closure'
    assert rows[('2024-09-23', 'War (autumn 2024)')] == ('event', '2024-11-30')


def test_calendar_days_split_closures_by_weekday():
    cal = forecasting.CalendarDays([
        {'start_date': date(2026, 11, 20), 'end_date': date(2026, 11, 22), 'name': 'Strike', 'kind': 'closure'},
        {'start_date': date(2026, 12, 24), 'end_date': date(2026, 12, 24), 'name': 'Eve', 'kind': 'half_day'},
        {'start_date': date(2026, 6, 25), 'end_date': date(2026, 6, 25), 'name': 'Ashura', 'kind': 'no_effect'},
        {'start_date': date(2024, 9, 23), 'end_date': date(2024, 9, 25), 'name': 'War', 'kind': 'event'},
    ])
    assert cal.kind[pd.Timestamp('2026-11-20')] == 'closure_wd'      # Friday
    assert cal.kind[pd.Timestamp('2026-11-21')] == 'closure_sat'
    assert cal.kind[pd.Timestamp('2026-11-22')] == 'closure_sun'
    assert cal.kind[pd.Timestamp('2026-12-24')] == 'half_day'
    assert pd.Timestamp('2026-06-25') not in cal.kind                 # listed, but the model ignores it
    assert {pd.Timestamp('2024-09-23'), pd.Timestamp('2024-09-25')} <= cal.events


# ── the model ─────────────────────────────────────────────────────────────────

def _check_week_and_holidays(fc, info, cal, end):
    fc = fc.set_index('ds')
    normal = fc[~fc.index.isin(list(cal.kind))]
    wk = normal.yhat.groupby(normal.index.dayofweek).mean()
    weekdays = wk[[0, 1, 2, 3, 4]].mean()
    assert wk[5] / weekdays == pytest.approx(0.35, abs=0.08)       # Saturday
    assert wk[6] / weekdays == pytest.approx(0.24, abs=0.08)       # Sunday
    future_closure = [d for d in cal.kind if d > pd.Timestamp(end) and d in fc.index and d.dayofweek <= 4][0]
    same_day = normal[normal.index.dayofweek == future_closure.dayofweek].yhat.mean()
    assert fc.yhat[future_closure] / same_day == pytest.approx(0.3, abs=0.15)
    assert info['weekday'][5] == pytest.approx(0.35, abs=0.08)


@needs_prophet
def test_prophet_learns_the_week_and_the_holidays():
    end = date(2026, 10, 31)
    closures = weekday_closures(end, 420)
    y = synthetic(end, closures=closures)
    cal = calendar_of(closures)
    fc, info = forecasting.fit_forecast(y, cal, 60, yearly=False)
    assert info['model'] == forecasting.MODEL_PROPHET
    assert len(fc) == 60 and fc.ds.iloc[0] == pd.Timestamp(end + timedelta(days=1))
    assert (fc.yhat_lower <= fc.yhat).all() and (fc.yhat <= fc.yhat_upper).all()
    _check_week_and_holidays(fc, info, cal, end)
    assert info['holiday_effect']['closure_wd'] == pytest.approx(0.3, abs=0.12)
    assert info['upcoming'] and info['upcoming'][0]['kind'] == 'closure'


def test_without_prophet_the_weekday_average_keeps_the_week_and_the_holidays(monkeypatch):
    monkeypatch.setattr(forecasting, '_prophet_class', lambda: None)
    end = date(2026, 10, 31)
    closures = weekday_closures(end, 420)
    y = synthetic(end, closures=closures)
    cal = calendar_of(closures)
    fc, info = forecasting.fit_forecast(y, cal, 60, yearly=False)
    assert info['model'] == forecasting.MODEL_FALLBACK
    assert info['fallback_reason'] == 'Prophet is not installed'
    _check_week_and_holidays(fc, info, cal, end)


def test_event_periods_stay_out_of_training(monkeypatch):
    """A war or a lockdown is not the hospital's normal: its days are not learnt from."""
    monkeypatch.setattr(forecasting, '_prophet_class', lambda: None)
    end = date(2026, 10, 31)
    y = synthetic(end, days=200)
    y[-14:] = y[-14:] * 0.2                                         # the last two weeks in a crisis
    marked = calendar_of([], events=[(end - timedelta(days=13), end)])
    unmarked = calendar_of([])
    tuesday = lambda fc: fc.set_index('ds').yhat.pipe(lambda s: s[s.index.dayofweek == 1].iloc[0])
    assert tuesday(forecasting.fit_forecast(y, marked, 14, yearly=False)[0]) > 150     # a normal Tuesday
    assert tuesday(forecasting.fit_forecast(y, unmarked, 14, yearly=False)[0]) < 150   # the crisis learnt


@needs_prophet
def test_the_sequential_correction_follows_the_last_days_and_fades():
    """The operator: every period should discover the period right after it. Busy and quiet spells
    last several days, as in LAUMC's data (the correction learns how long from the history: a
    spell is not extrapolated where spells never lasted). The last ten days ran 20 % high: Prophet
    alone does not bend that late; the correction pulls the next days up, less and less."""
    end = date(2026, 10, 31)
    rng = np.random.default_rng(11)
    idx = pd.date_range(end - timedelta(days=419), end)
    e = np.zeros(len(idx))
    for i in range(1, len(idx)):                                    # spells: log error AR(1), 0.85
        e[i] = 0.85 * e[i - 1] + rng.normal(0, 0.05)
    e[-10:] = np.log(1.2)                                           # the last ten days, 20 % high
    y = pd.Series([200 * SHAPE[d.dayofweek] * np.exp(e[i]) for i, d in enumerate(idx)], index=idx).round()
    cal = calendar_of([])
    with_ar, info = forecasting.fit_forecast(y, cal, 30, yearly=False, sequential=True)
    plain, _ = forecasting.fit_forecast(y, cal, 30, yearly=False, sequential=False)
    ratio = (with_ar.yhat / plain.yhat).values
    assert info['correction_now'] > 0.08
    assert ratio[0] > 1.08
    assert ratio[-1] < 1.03 < ratio[0]                              # the pull fades
    recent = y[-10:]
    level = recent[recent.index.dayofweek <= 4].mean()
    first_weekday = with_ar[with_ar.ds.dt.dayofweek <= 4].iloc[0]
    plain_weekday = plain[plain.ds.dt.dayofweek <= 4].iloc[0]
    assert abs(first_weekday.yhat - level) < abs(plain_weekday.yhat - level)


def test_ar_correction_is_zero_without_a_pattern():
    rng = np.random.default_rng(3)
    corr, coefs = forecasting._ar_correction(rng.normal(0, 0.1, 400), 30)
    assert coefs is not None and np.abs(corr).max() < 0.08
    corr, coefs = forecasting._ar_correction(np.zeros(30), 10)      # too short to learn from
    assert coefs is None and not corr.any()


def test_holidays_are_never_unusual_days():
    from routes.report_ai import _detect_anomalies
    dates = [str(date(2026, 9, 1) + timedelta(days=i)) for i in range(56)]
    values = [200 if pd.Timestamp(d).dayofweek <= 4 else 60 for d in dates]
    values[10] = 40                                                 # a closed Friday
    assert _detect_anomalies(values, dates=dates)[10]
    assert not _detect_anomalies(values, dates=dates, skip={dates[10]})[10]


# ── stored every night, checked the next day ──────────────────────────────────

def insert_days(start, end, ae='CTX9', base=0):
    """Studies per day with LAUMC's week: 20 on weekdays, 7 on Saturdays, 5 on Sundays."""
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_instance_uid, storing_ae,
                                         study_date, study_modality, procedure_code,
                                         referring_physician_first_name, referring_physician_last_name)
           SELECT :b + ROW_NUMBER() OVER (), :b + ROW_NUMBER() OVER (), '1.9.' || (:b + ROW_NUMBER() OVER ()),
                  :ae, d::date, 'CT', 'CT01', 'Ref', CHR(65 + (k % 3))
           FROM generate_series(CAST(:s AS date), CAST(:e AS date), interval '1 day') d,
                LATERAL generate_series(1, CASE EXTRACT(ISODOW FROM d) WHEN 6 THEN 7 WHEN 7 THEN 5 ELSE 20 END) k""",
        s=start, e=end, ae=ae, b=base)


TODAY = date(2026, 11, 2)


@pytest.fixture
def trained():
    insert_days(TODAY - timedelta(days=200), TODAY - timedelta(days=1))
    return forecasting.run_for_site(None, today=TODAY)


def test_run_stores_every_series_with_its_model(trained):
    assert trained['series'] == 2 and trained['failed'] == 0          # volume, device:CTX9 (no storage rows)
    rows = __import__('db').db.session.execute(text(
        "SELECT series_key, trained_to, info FROM ai_forecast_run ORDER BY 1")).fetchall()
    assert [r[0] for r in rows] == ['device:CTX9', 'volume']
    assert all(r[1] == TODAY - timedelta(days=1) for r in rows)
    assert scalar("SELECT COUNT(*) FROM ai_forecast WHERE series_key = 'volume'") == forecasting.HORIZON['volume']
    assert scalar("SELECT MIN(ds) FROM ai_forecast WHERE series_key = 'volume'") == TODAY
    assert scalar("SELECT COUNT(*) FROM ai_forecast WHERE series_key = 'device:CTX9'") == forecasting.HORIZON['device']
    # the week survives storage: the first Saturday far below the first Tuesday
    fc = dict(__import__('db').db.session.execute(text(
        "SELECT ds, yhat FROM ai_forecast WHERE series_key = 'volume' ORDER BY ds")).fetchall())
    sat = next(v for d, v in fc.items() if d.weekday() == 5)
    tue = next(v for d, v in fc.items() if d.weekday() == 1)
    assert sat < tue * 0.6


def test_the_next_run_checks_what_the_model_said_against_what_came(trained):
    said = scalar("SELECT yhat FROM ai_forecast WHERE series_key = 'volume' AND ds = :d", d=TODAY)
    insert_days(TODAY, TODAY, base=900000)
    forecasting.run_for_site(None, today=TODAY + timedelta(days=1))
    row = __import__('db').db.session.execute(text(
        "SELECT forecast, actual, made_on FROM ai_forecast_check WHERE series_key = 'volume' AND ds = :d"),
        {'d': TODAY}).fetchone()
    expected_actual = 20 if TODAY.weekday() <= 4 else (7 if TODAY.weekday() == 5 else 5)
    assert row[0] == pytest.approx(said, abs=0.01)
    assert row[1] == expected_actual
    assert row[2] == TODAY - timedelta(days=1)
    assert scalar("SELECT MIN(ds) FROM ai_forecast WHERE series_key = 'volume'") == TODAY + timedelta(days=1)


def test_a_device_gone_quiet_loses_its_forecast(trained):
    sql("DELETE FROM etl_didb_studies WHERE storing_ae = 'CTX9' AND study_date >= :d", d=TODAY - timedelta(days=40))
    forecasting.run_for_site(None, today=TODAY)
    assert scalar("SELECT COUNT(*) FROM ai_forecast_run WHERE series_key = 'device:CTX9'") == 0
    assert scalar("SELECT COUNT(*) FROM ai_forecast WHERE series_key = 'device:CTX9'") == 0


# ── the page ──────────────────────────────────────────────────────────────────

def test_the_page_shows_the_stored_forecast_and_explains_every_figure(client, trained):
    qs = {'start_date': str(TODAY - timedelta(days=60)), 'end_date': str(TODAY - timedelta(days=1))}
    html = client.get('/report/ai', headers=ADMIN, query_string={**qs, 'tab': 'volume'}).get_data(as_text=True)
    assert 'js/chart-explanations.js' in html
    assert forecasting.MODEL_PROPHET in html or forecasting.MODEL_FALLBACK in html
    for key in ('ai-report', 'ai-model', 'ai-calendar', 'ai-volume-total', 'ai-volume-avg', 'ai-volume-next',
                'ai-volume-growth', 'ai-volume-chart', 'ai-volume-mod'):
        assert f'data-explain="{key}"' in html
    data = client.get('/report/ai/panel/volume', headers=ADMIN, query_string=qs).get_json()['data']
    assert len(data['chart']['forecast_dates']) == forecasting.HORIZON['volume']
    assert data['chart']['forecast_dates'][0] == str(TODAY)
    assert len(data['chart']['forecast_lower']) == len(data['chart']['forecast_upper']) == forecasting.HORIZON['volume']
    assert data['totals']['expected'] > 0 and data['meta']['trained_to'] == str(TODAY - timedelta(days=1))
    for section, keys in (('storage', ()), ('util', ('ai-util-anomalies', 'ai-util-chart')),
                          ('physician', ('ai-phys-table', 'ai-phys-chart'))):
        resp = client.get(f'/report/ai/panel/{section}', headers=ADMIN, query_string=qs)
        assert resp.status_code == 200
        for key in keys:
            assert f'data-explain="{key}"' in resp.get_json()['html']
    util = client.get('/report/ai/panel/util', headers=ADMIN, query_string=qs).get_json()['data']
    ctx9 = next(a for a in util['ae_list'] if a['ae'] == 'CTX9')
    assert len(ctx9['chart']['forecast_dates']) == forecasting.HORIZON['device']


def test_an_old_end_date_shows_no_forecast(client, trained):
    qs = {'start_date': str(TODAY - timedelta(days=90)), 'end_date': str(TODAY - timedelta(days=30))}
    data = client.get('/report/ai/panel/volume', headers=ADMIN, query_string=qs).get_json()['data']
    assert data['chart']['forecast_dates'] == [] and data['totals'] is None


# ── the calendar and retrain: administrators only ─────────────────────────────

def test_the_calendar_is_edited_by_administrators_and_reaches_both_databases(client):
    from utils import site_db
    body = {'start_date': '2026-12-03', 'end_date': '2026-12-04', 'name': 'General strike', 'kind': 'closure'}
    assert client.post('/report/ai/calendar', headers=USER, json=body).status_code == 403
    resp = client.post('/report/ai/calendar', headers=ADMIN, json=body)
    assert resp.status_code == 200, resp.get_data(as_text=True)
    cid = resp.get_json()['row']['id']
    sjh = scalar("SELECT id FROM sites WHERE code = 'SJH'")
    with site_db.engine_for(sjh).connect() as conn:
        assert conn.execute(text("SELECT kind FROM ai_calendar WHERE id = :i"), {'i': cid}).scalar() == 'closure'
    assert client.get('/report/ai/calendar', headers=USER).status_code == 403      # no Report AI page
    rows = client.get('/report/ai/calendar', headers=ADMIN).get_json()
    assert any(r['name'] == 'General strike' and r['source'] == 'user' for r in rows['rows']) and rows['can_edit']
    assert client.post(f'/report/ai/calendar/{cid}/delete', headers=USER).status_code == 403
    assert client.post(f'/report/ai/calendar/{cid}/delete', headers=ADMIN).status_code == 200
    with site_db.engine_for(sjh).connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM ai_calendar WHERE id = :i"), {'i': cid}).scalar() == 0


@pytest.mark.parametrize('body, message', [
    ({'start_date': '2026-12-05', 'end_date': '2026-12-01', 'name': 'x', 'kind': 'closure'}, 'end date'),
    ({'start_date': '2026-12-05', 'name': '', 'kind': 'closure'}, 'name'),
    ({'start_date': '2026-12-05', 'name': 'x', 'kind': 'holiday'}, 'kind'),
    ({'start_date': 'tomorrow', 'name': 'x', 'kind': 'closure'}, 'YYYY-MM-DD'),
])
def test_the_calendar_refuses_what_it_cannot_use(client, body, message):
    resp = client.post('/report/ai/calendar', headers=ADMIN, json=body)
    assert resp.status_code == 400 and message in resp.get_json()['message']


def test_retrain_is_for_administrators_and_runs_in_the_background(client, monkeypatch):
    calls = []
    monkeypatch.setattr(forecasting, 'start_background', lambda app, site_id: calls.append(site_id) or True)
    assert client.post('/report/ai/retrain', headers=USER).status_code == 403
    assert client.post('/report/ai/retrain', headers=ADMIN).get_json() == {'status': 'started'}
    assert len(calls) == 1


# ── referrers: their real numbers against the same weeks a year earlier ───────

def test_referrers_show_every_real_change_with_its_strength():
    """Operator, 2026-11-19: the share test read 'all normal with a difference up to 20 %'. Every
    change from 10 % now shows, graded; few studies get a question mark, not 'normal'."""
    last = {'Dr steady': 200, 'Dr slight': 200, 'Dr clear': 200, 'Dr sharp': 200, 'Dr up': 200,
            'Dr stopped': 60, 'Dr small': 12}
    now = {'Dr steady': 190, 'Dr slight': 170, 'Dr clear': 150, 'Dr sharp': 100, 'Dr up': 250,
           'Dr stopped': 3, 'Dr small': 8, 'Dr new': 25}
    out = forecasting.referrer_trends(now, last)
    assert (out['Dr steady']['status'], out['Dr steady']['change_pct']) == ('steady', -5)
    assert (out['Dr slight']['status'], out['Dr slight']['grade']) == ('declining', 'slight')   # -15 %: shown
    assert not out['Dr slight']['listed']                                                        # lists from 20 %
    assert out['Dr clear']['grade'] == 'clear' and out['Dr clear']['listed'] and out['Dr clear']['diff'] == -50
    assert out['Dr sharp']['grade'] == 'sharp'
    assert (out['Dr up']['status'], out['Dr up']['grade']) == ('growing', 'clear') and out['Dr up']['listed']
    assert out['Dr stopped']['status'] == 'stopped' and out['Dr stopped']['listed']
    assert out['Dr new']['status'] == 'new' and out['Dr new']['listed']
    assert out['Dr small']['status'] == 'declining' and not out['Dr small']['certain']           # -33 % of 12


def test_referrer_windows_are_13_weeks_and_the_same_weeks_a_year_earlier():
    r0, r1, l0, l1 = forecasting.referrer_windows('2026-11-17')
    assert r1 == date(2026, 11, 17) and (r1 - r0).days == 13 * 7 - 1
    assert (r0 - l0).days == 364 and (r1 - l1).days == 364 and l0.weekday() == r0.weekday()


def test_the_referrer_tab_counts_both_windows(client):
    end = date(2026, 11, 17)
    r0, r1, l0, l1 = forecasting.referrer_windows(end)

    def refer(last, first_day, n, base):
        sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_instance_uid, storing_ae,
                                             study_date, study_modality, referring_physician_first_name,
                                             referring_physician_last_name)
               SELECT :b + k, :b + k, '1.8.' || (:b + k), 'CTX9', CAST(:d AS date) + (k % 60), 'CT', 'Dr', :l
               FROM generate_series(1, :n) k""", b=base, d=first_day, n=n, l=last)
    refer('Down', l0, 120, 1000)        # 120 then
    refer('Down', r0, 80, 2000)         # 80 now: -33 %
    refer('Same', l0, 100, 3000)
    refer('Same', r0, 104, 4000)        # +4 %: steady
    data = client.get('/report/ai/panel/physician', headers=ADMIN,
                      query_string={'start_date': str(l0), 'end_date': str(end)}).get_json()['data']
    rows = {p['name']: p['trend'] for p in data['physicians']}
    assert rows['Dr Down']['recent'] == 80 and rows['Dr Down']['last_year'] == 120
    assert (rows['Dr Down']['status'], rows['Dr Down']['grade'], rows['Dr Down']['diff']) == ('declining', 'clear', -40)
    assert rows['Dr Same']['status'] == 'steady'
    assert [c['name'] for c in data['churning']] == ['Dr Down']
    assert data['hospital'] == {'recent': 184, 'last_year': 220, 'change_pct': -16}
    html = client.get('/report/ai/panel/physician', headers=ADMIN,
                      query_string={'start_date': str(l0), 'end_date': str(end)}).get_json()['html']
    assert '-33% clear' in html and 'steady' in html
