"""
tests/test_report_ai_review.py — Report AI after the review of 2026-11-20 (routes/report_ai.py,
templates/report_ai.html): dates, the capacity setting, device forecasts and capacities, the cache,
unusual days, the thresholds, errors shown as errors, and the page's markup.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}
D0 = date(2026, 10, 5)                     # a Monday


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


_uid = iter(range(80_000, 99_000))


def study(day, ae, modality='CT'):
    uid = next(_uid)
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date, study_modality)
           VALUES (:u, :u, :ae, :d, :m)""", u=uid, ae=ae, d=day, m=modality)


def device(ae, modality='CT', capacity=None):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, daily_capacity_minutes, site_id)
           SELECT :ae, :m, :c, id FROM sites WHERE code = 'RH'""", ae=ae, m=modality, c=capacity)


# ── 1.1 / 1.2 ─────────────────────────────────────────────────────────────────

def test_empty_or_unreadable_dates_take_the_defaults():
    from routes.report_ai import _resolve_date_range
    from db import get_report_start_date
    first = (get_report_start_date() or date(2025, 1, 1)).isoformat()
    today = date.today().isoformat()
    assert _resolve_date_range({'start_date': '', 'end_date': ''}) == (first, today)
    assert _resolve_date_range({'start_date': 'banana', 'end_date': ' 2026-10-09 '}) == (first, '2026-10-09')
    assert _resolve_date_range({'start_date': '2026-10-09', 'end_date': '2026-10-01'}) == ('2026-10-01', '2026-10-09')


def test_the_page_opens_with_empty_dates(client):
    assert client.get('/report/ai?start_date=&end_date=', headers=ADMIN).status_code == 200
    assert client.get('/report/ai?start_date=&end_date=&tab=util', headers=ADMIN).status_code == 200


@pytest.mark.parametrize('kwargs, code', [
    ({'data': 'not json', 'content_type': 'text/plain'}, 400),
    ({'json': {'capacity_gb': 'abc'}}, 400),
    ({'json': {'capacity_gb': 'nan'}}, 400),
    ({'json': {'capacity_gb': -5}}, 400),
    ({'json': {'capacity_gb': '71,680'}}, 200),
])
def test_the_capacity_must_be_a_positive_number(client, kwargs, code):
    r = client.post('/report/ai/storage-capacity', headers=ADMIN, **kwargs)
    assert r.status_code == code, r.get_json()
    if code == 200:
        assert r.get_json()['capacity_gb'] == 71680.0


# ── 1.3 / 1.5 / 1.10 device forecasts, capacities, thresholds ────────────────

def test_a_device_forecast_is_found_whatever_the_case_of_its_key():
    from routes.report_ai import _get_utilization_intelligence
    device('CT99', capacity=600)
    study(D0, 'CT99')
    sql("""INSERT INTO ai_forecast_run (series_key, model, trained_from, trained_to, run_at, info)
           VALUES ('device:ct99', 'prophet', :f, :t, now(), '{}')""", f=D0 - timedelta(days=200), t=D0)
    sql("INSERT INTO ai_forecast (series_key, ds, yhat, yhat_lower, yhat_upper) VALUES ('device:ct99', :d, 300, 200, 400)",
        d=D0 + timedelta(days=1))
    data = _get_utilization_intelligence(D0.isoformat(), D0.isoformat())
    ae = next(a for a in data['ae_list'] if a['ae'] == 'CT99')
    assert ae['forecast_avg_util'] == 50.0 and ae['meta']['model'] == 'prophet'


def test_one_capacity_per_device_and_weekday_by_a_stated_priority():
    """'ct77' and 'CT77' can both be rows of the map: the lowest id wins, unless the RIS gives
    opening minutes for that weekday."""
    from routes.report_ai import _get_utilization_intelligence
    device('ct77', capacity=600)
    device('CT77', capacity=300)
    study(D0, 'CT77')                                     # 15 minutes (no procedure duration)
    u = _get_utilization_intelligence(D0.isoformat(), D0.isoformat())
    assert next(a for a in u['ae_list'] if a['ae'] == 'CT77')['avg_util'] == 2.5          # 15 / 600
    sql("INSERT INTO std_device_weekly_availability (aetitle, day_of_week, available_minutes) VALUES ('CT77', 0, 150)")
    u = _get_utilization_intelligence(D0.isoformat(), (D0 + timedelta(days=0)).isoformat() + '')
    # the cache holds the first answer for the same dates: ask for the next week's Monday
    study(D0 + timedelta(days=7), 'CT77')
    u = _get_utilization_intelligence((D0 + timedelta(days=7)).isoformat(), (D0 + timedelta(days=7)).isoformat())
    assert next(a for a in u['ae_list'] if a['ae'] == 'CT77')['avg_util'] == 10.0         # 15 / 150 (RIS)


def test_a_study_counts_once_whatever_the_case_of_its_device_rows():
    """Found with 1.5: the case-blind join to the device map counted a study once per map row."""
    from routes.report_ai import _get_volume_intelligence
    from utils import forecasting
    device('ct66')
    device('CT66')
    for i in range(6):
        study(D0 + timedelta(days=i), 'CT66')
    v = _get_volume_intelligence(D0.isoformat(), (D0 + timedelta(days=5)).isoformat())
    assert v['total'] == 6 and v['modality_split'] == [{'name': 'CT', 'value': 6}]
    for q in (forecasting.VOLUME_SQL, forecasting.DEVICE_EST_SQL, forecasting.DEVICE_ACT_SQL):
        assert 'LIMIT 1)' in q and 'LEFT JOIN aetitle_modality_map' not in q


def test_the_utilization_limits_are_settings():
    from routes.report_ai import _get_utilization_intelligence
    device('CT55', capacity=600)
    study(D0, 'CT55')                                     # 2.5 %
    u = _get_utilization_intelligence(D0.isoformat(), D0.isoformat())
    assert u['high_pct'] == 85 and u['low_pct'] == 30 and 'CT55' in str(u['low_util'])
    sql("INSERT INTO settings (key, value) VALUES ('ai_util_high_warn_pct', '2'), ('ai_util_low_warn_pct', 'x') "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    d1 = D0 + timedelta(days=1)
    study(d1, 'CT55')
    u = _get_utilization_intelligence(d1.isoformat(), d1.isoformat())
    assert u['high_pct'] == 2 and u['low_pct'] == 30 and 'CT55' in str(u['high_stress'])
    assert 'above 2% on average' in u['explanation']


# ── 1.8 the cache ─────────────────────────────────────────────────────────────

def test_status_calls_keep_the_cache_and_a_finished_run_renews_it(client, monkeypatch):
    from routes import report_ai
    from utils import forecasting
    report_ai._ai_cache_put('volume', '2026-01-01', '2026-01-31', {'x': 1})
    for _ in range(3):
        assert client.get('/report/ai/status', headers=ADMIN).status_code == 200
    assert report_ai._ai_cache_get('volume', '2026-01-01', '2026-01-31') == {'x': 1}
    monkeypatch.setattr(forecasting, '_generation', forecasting.generation() + 1)     # a run finished
    assert report_ai._ai_cache_get('volume', '2026-01-01', '2026-01-31') is None


# ── block 2 ───────────────────────────────────────────────────────────────────

def test_a_section_that_fails_says_so_instead_of_no_data(client, monkeypatch):
    """2.1"""
    from routes import report_ai

    def boom(start, end):
        raise RuntimeError('broken query')
    monkeypatch.setitem(report_ai._SECTION_CONFIG, 'volume', ('volume', boom, '_ai_panel_volume.html'))
    r = client.get('/report/ai/panel/volume', headers=ADMIN).get_json()
    assert r['error'] is True and r['data'] is None and 'Could not compute' in r['html']
    page = client.get('/report/ai?tab=volume', headers=ADMIN).get_data(as_text=True)
    assert 'Could not compute' in page and '>ERROR</span>' in page and 'broken query' not in page


@pytest.mark.parametrize('section', ['storage', 'volume', 'util', 'physician'])
def test_every_panel_renders_alone_with_or_without_data(client, section):
    """2.3: the partials use only data.<key>; with no data they say so (no Jinja error)."""
    r = client.get(f'/report/ai/panel/{section}', headers=ADMIN)
    assert r.status_code == 200
    body = r.get_json()
    assert body['error'] is False and body['data'] is None
    assert 'data available' in body['html'] or 'No storage data' in body['html']


def test_the_hospital_figure_says_it_counts_every_study():
    """2.2"""
    from routes.report_ai import _explain_physician
    d = {'window': {'recent_from': '2026-08-01', 'to': '2026-10-31', 'last_from': '2025-08-01', 'last_to': '2025-10-31'},
         'hospital': {'recent': 1000, 'last_year': 900, 'change_pct': 11},
         'counts': {'declining': 1, 'stopped': 0, 'growing': 2, 'new': 0, 'steady': 5}, 'churning': [], 'growing': []}
    assert 'every study, with or without a named referrer' in _explain_physician(d)


def test_the_page_script_guards_against_double_fetches_and_merged_charts():
    """2.4, 2.5, 2.6 (the page itself is checked in a browser)"""
    import os
    page = open(os.path.join(os.path.dirname(__file__), '..', 'templates', 'report_ai.html'), encoding='utf-8').read()
    assert 'const inflight = {}' in page and 'inflight[key] = _loadSection' in page
    assert ".replace(/,/g, '')" in page and 'Number.isFinite(val)' in page
    assert page.count('}, true);') >= 2


# ── block 3 ───────────────────────────────────────────────────────────────────

def test_the_tabs_and_panels_come_from_one_mapping_and_take_the_keyboard(client):
    """3.1 one loop over _SECTION_CONFIG, 3.2 D always a dict, 3.3 the delete URL from the registry,
    3.4 the dialog's date pickers made when it opens, 3.5 tabs as buttons in a tablist."""
    html = client.get('/report/ai?tab=volume', headers=ADMIN).get_data(as_text=True)
    assert 'role="tablist"' in html
    for tab in ('storage', 'volume', 'util', 'physician'):
        assert f'id="tab-{tab}"' in html and f'aria-controls="panel-{tab}"' in html
        assert f'id="panel-{tab}"' in html and f'aria-labelledby="tab-{tab}"' in html
    assert html.count('<button type="button" class="tab') == 4
    assert 'aria-selected="true" aria-controls="panel-volume"' in html
    assert 'const D = {' in html and "if data else" not in html
    assert '/report/ai/calendar/987654321/delete' in html and ".replace('/0/'" not in html
    assert ':not(#calStart):not(#calEnd)' in html and 'appendTo: dlg' in html


def test_the_storage_capacity_is_read_once_per_request(client):
    """3.7"""
    from sqlalchemy import event
    from db import db
    seen = []
    f = lambda conn, cursor, statement, *a: seen.append(statement)
    event.listen(db.engine, 'before_cursor_execute', f)
    try:
        assert client.get('/report/ai?tab=storage&start_date=2026-06-01&end_date=2026-06-30', headers=ADMIN).status_code == 200
    finally:
        event.remove(db.engine, 'before_cursor_execute', f)
    assert sum("key = 'storage_capacity_gb'" in q for q in seen) == 1


def test_the_referrer_name_expression_is_defined_before_use():
    """3.6"""
    import inspect
    from routes import report_ai
    src = inspect.getsource(report_ai)
    assert src.index('_PHYSICIAN = ') < src.index('def _get_physician_intelligence')


# ── block 4 ───────────────────────────────────────────────────────────────────

def test_figures_ahead_only_with_the_forecast_shown_what_the_model_learnt_always():
    """4.1"""
    from routes.report_ai import _get_volume_intelligence
    device('CT44')
    for i in range(6):
        study(D0 + timedelta(days=i), 'CT44')
    sql("""INSERT INTO ai_forecast_run (series_key, model, trained_from, trained_to, run_at, info)
           VALUES ('volume', 'prophet', '2025-01-01', '2026-11-01', now(), CAST(:i AS jsonb))""",
        i='{"growth_per_year": 0.05, "weekday": [1, 1, 1, 1, 1, 0.35, 0.24], "correction_now": 0.08, '
          '"totals": {"days": 90, "expected": 100, "low": 90, "high": 110}, '
          '"upcoming": [{"date": "2026-11-22", "name": "Independence Day", "expected": 40, "kind": "closure"}]}')
    v = _get_volume_intelligence(D0.isoformat(), (D0 + timedelta(days=5)).isoformat())     # before trained_to
    assert v['totals'] is None and v['upcoming'] == [] and v['correction_now'] is None
    assert v['growth_per_year'] == 0.05 and v['weekday_ratio'][5] == 0.35


def test_the_storage_explanation_survives_a_zero_capacity():
    """4.2"""
    from routes.report_ai import _explain_storage
    text_ = _explain_storage({'current_gb': 512.0, 'capacity_gb': 0, 'daily_growth_gb': None})
    assert '512 GB of 10,240 GB' in text_


def test_the_theme_is_set_by_the_server():
    """4.5"""
    import os
    page = open(os.path.join(os.path.dirname(__file__), '..', 'templates', 'report_ai.html'), encoding='utf-8').read()
    assert "{{ ' light' if ui_theme == 'light' else '' }}" in page
    body_end = page.index("document.addEventListener('DOMContentLoaded'")
    assert 'applyTheme("{{ ui_theme }}")' in page[body_end:]


# ── 1.9 unusual days ──────────────────────────────────────────────────────────

def test_a_day_is_measured_against_the_other_days():
    """Five Mondays 99, 101, 99, 101, 120: counted in its own mean and spread (104 ± 8.05) the
    120 was within 2 sd; against the other four (100 ± 1) it is far out."""
    from routes.report_ai import _detect_anomalies
    days = [(D0 + timedelta(weeks=i)).isoformat() for i in range(5)]
    assert _detect_anomalies([99, 101, 99, 101, 120], dates=days) == [False, False, False, False, True]
    assert _detect_anomalies([99, 101, 99, 101, 120]) == [False, False, False, False, True]
    assert _detect_anomalies([5, 5, 5, 5, 5], dates=days) == [False] * 5
