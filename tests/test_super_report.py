"""
tests/test_super_report.py — the Super Report's route (routes/super_report.py): its dates, the
ER-delayed count, the comparison window in the narrative, its caches and settings (review of
2026-11-20).
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}
D1 = date(2026, 10, 5)


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def device(ae, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           SELECT :ae, :m, id FROM sites WHERE code = :s""", ae=ae, m=modality, s=site)


_uid = iter(range(70_000, 90_000))


def study(day, ae, modality, patient_class='O', tat_min=10):
    uid = next(_uid)
    arrived = datetime.combine(day, datetime.min.time()) + timedelta(hours=8)
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date, study_modality,
                                         patient_class, insert_time, rep_study_last_composed_ts, study_has_report)
           VALUES (:u, :u, :ae, :d, :m, :pc, :t, :c, TRUE)""",
        u=uid, ae=ae, d=day, m=modality, pc=patient_class, t=arrived, c=arrived + timedelta(minutes=tat_min))
    return uid


def site_id(code):
    from db import db
    return db.session.execute(text("SELECT id FROM sites WHERE code = :c"), {'c': code}).scalar()


# ── 1.3 dates ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('query', [
    'start=banana&end=2026-10-11',
    'start=2026-10-05&end=2026-13-40',
    'start=2026-01-10&end=2026-01-01',                                  # end before start
    'start=2026-10-05&end=2026-10-11&cmp_start=2026-09-30&cmp_end=2026-09-01',
    'start=2026-10-05&end=2026-10-11&cmp_start=nope&cmp_end=2026-09-30',
])
def test_a_date_that_does_not_make_sense_is_refused(client, query):
    r = client.get(f'/viewer/super-report?{query}', headers=ADMIN)
    assert r.status_code == 400 and r.get_json()['error']


# ── 1.5 the narrative names the comparison window ────────────────────────────

def test_the_narrative_names_the_comparison_window_not_the_current_one(client, app):
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        for d in (D1, D1 + timedelta(days=1)):
            study(d, 'CTX1', 'CT')
        study(date(2026, 9, 10), 'CTX1', 'CT')
    r = client.get('/viewer/super-report?start=2026-10-05&end=2026-10-11'
                   '&cmp_start=2026-09-01&cmp_end=2026-09-30', headers=ADMIN).get_json()
    overview = next(s for s in r['narrative'] if s['title'] == 'Overview')['bullets'][0]
    assert 'vs prior 30-day period' in overview, overview
    assert (r['delta_days'], r['cmp_days']) == (7, 30)
    # the automatic window: the same length, ending the day before start
    r = client.get('/viewer/super-report?start=2026-10-05&end=2026-10-11', headers=ADMIN).get_json()
    assert (r['cmp_start'], r['cmp_end'], r['cmp_days']) == ('2026-09-28', '2026-10-04', 7)


# ── 1.2 ER-delayed counts only days with ER volume in the page's own selection ─

def test_er_delayed_counts_only_er_days_of_the_selected_site_and_modality(app):
    from routes.super_report import _collect_data
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        device('MRX1', 'MR', 'RH')
        device('CTS1', 'CT', 'SJH')
        d1, d2, d3 = D1, D1 + timedelta(days=1), D1 + timedelta(days=2)
        study(d1, 'CTX1', 'CT', 'E')          # ER CT on RH: a day that counts
        study(d2, 'MRX1', 'MR', 'E')          # ER, but MR
        study(d3, 'CTS1', 'CT', 'E')          # ER, but SJH
        for d in (d1, d2, d3):                # each day: three quick non-ER CT on RH, one slow
            for _ in range(3):
                study(d, 'CTX1', 'CT', 'O', tat_min=10)
            study(d, 'CTX1', 'CT', 'O', tat_min=600)
        data = _collect_data(d1.isoformat(), d3.isoformat(), {'modality': ['CT']}, site_id('RH'))
        assert data['ae_ops']['er_delayed'] == 1        # the slow study of d1 only (was 3)
        assert data['tat']['er_reported_count'] == 1


def test_the_patient_class_lists_are_bound_parameters(app):
    """A class name with a quote in it is matched as data, not spliced into the SQL."""
    from routes.super_report import _collect_data
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        sql("INSERT INTO settings (key, value) VALUES ('pc_emergency', 'E,O''R') "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
        study(D1, 'CTX1', 'CT', "O'R")
        study(D1, 'CTX1', 'CT', 'O')
        data = _collect_data(D1.isoformat(), D1.isoformat(), {}, site_id('RH'))
        assert data['tat']['er_reported_count'] == 1 and data['tat']['non_er_reported_count'] == 1
    import inspect
    from routes import super_report
    assert "replace(\"'\"" not in inspect.getsource(super_report) and 'chr(39)' not in inspect.getsource(super_report)


# ── block 2 ───────────────────────────────────────────────────────────────────

class Statements:
    """Every SQL statement the engine runs inside the block."""
    def __enter__(self):
        from sqlalchemy import event
        from db import db
        self.engine, self.seen = db.engine, []
        self._f = lambda conn, cursor, statement, *a: self.seen.append(statement)
        event.listen(self.engine, 'before_cursor_execute', self._f)
        return self

    def __exit__(self, *exc):
        from sqlalchemy import event
        event.remove(self.engine, 'before_cursor_execute', self._f)


def test_the_dead_filters_are_neither_read_nor_offered(client, app):
    """2.1: series modality / body part / protocol were read and never applied; age group was
    offered by nothing (the age filter is the min / max range)."""
    from routes import super_report
    with app.test_request_context('/viewer/super-report?series_modality=CT&age_group=0-18&modality=MR'):
        f = super_report._request_filters()
    assert 'series_modality' not in f and 'age_group' not in f and f['modality'] == ['MR']
    lists = client.get('/viewer/super-report/filters', headers=ADMIN).get_json()
    assert not {'series_modality', 'body_part_series', 'protocol_name', 'age_group'} & set(lists)


def test_migration_0180_takes_the_dead_filters_out_of_the_presets(app):
    import os
    from db import db
    with app.app_context():
        uid = db.session.execute(text("""INSERT INTO users (username, email, password_hash, role, status)
                                         VALUES ('sr_preset', 'sr@x.test', 'x', 'user', 'active') RETURNING id""")).scalar()
        db.session.commit()
        sql("""INSERT INTO saved_reports (name, owner_user_id, base_report_id, filter_json, updated_at)
               VALUES ('old', :u, NULL, CAST(:f AS jsonb), '2026-01-01 10:00')""", u=uid,
            f='{"modality": ["CT"], "series_modality": ["MR"], "protocol_name": "X", "age_group": ["0-18"]}')
        path = os.path.join(os.path.dirname(__file__), '..', 'migrations', '0180_super_report_presets_dead_filters.sql')
        sql(open(path, encoding='utf-8').read())
        row = db.session.execute(text("SELECT filter_json, updated_at FROM saved_reports WHERE name = 'old'")).mappings().one()
        assert row['filter_json'] == {'modality': ['CT']}
        assert row['updated_at'].replace(tzinfo=None) == datetime(2026, 1, 1, 10, 0)   # order kept
        sql("DELETE FROM saved_reports WHERE owner_user_id = :u", u=uid)
        sql("DELETE FROM users WHERE id = :u", u=uid)


def test_the_comparison_period_skips_what_the_page_never_reads(app, capsys):
    """2.2: for the comparison period the page reads the critical count and the utilization
    (their change badges); the rest of the CRN summary and the wet reads are not computed."""
    from routes.super_report import _collect_data
    with app.app_context():
        with Statements() as full:
            a = _collect_data('2026-03-01', '2026-03-07', {}, site_id('RH'))
        with Statements() as light:
            b = _collect_data('2026-04-01', '2026-04-07', {}, site_id('RH'), include_rollups=False)
    assert set(b['crn']) == {'critical_count'} and b['resident_wetreads'] is None
    assert 'available' in b['utilization'] and a['resident_wetreads'] is not None
    assert len(light.seen) < len(full.seen)
    with capsys.disabled():
        print(f"\n  statements per period: current {len(full.seen)}, comparison {len(light.seen)}")


def test_the_orders_site_probe_runs_once_per_process_and_database(app):
    """2.3: a yes is kept; a no is asked again (the ETL may fill the column later)."""
    from routes import super_report
    probe = 'FROM etl_orders WHERE site_id IS NOT NULL'
    with app.app_context():
        super_report._ORDERS_SITE_KNOWN.clear()
        with Statements() as s:
            assert super_report._orders_have_site() is False
            assert super_report._orders_have_site() is False
        assert sum(probe in q for q in s.seen) == 2
        sql("INSERT INTO etl_orders (order_dbid, site_id) VALUES (991, :s)", s=site_id('RH'))
        with Statements() as s:
            for _ in range(3):
                assert super_report._orders_have_site() is True
        assert sum(probe in q for q in s.seen) == 1
        super_report._ORDERS_SITE_KNOWN.clear()


def test_the_same_choices_in_another_order_hit_the_same_cache_entry(client, app, monkeypatch):
    """2.5: [CT, MR] and [MR, CT] are one report."""
    from routes import super_report
    calls = []
    real = super_report._collect_data
    monkeypatch.setattr(super_report, '_collect_data', lambda *a, **k: calls.append(a) or real(*a, **k))
    base = '/viewer/super-report?start=2026-05-04&end=2026-05-10'
    client.get(base + '&modality=CT&modality=MR', headers=ADMIN)
    assert len(calls) == 2                                   # current and comparison period
    client.get(base + '&modality=MR&modality=CT', headers=ADMIN)
    assert len(calls) == 2                                   # served from the cache


def test_the_narrative_thresholds_are_settings(app):
    """2.6: each threshold is a settings key, the old literal when absent."""
    from routes.super_report import _generate_narrative
    cur = {'kpis': {'total_studies': 100, 'total_patients': 90, 'total_images': 1000, 'active_aes': 3},
           'orders': {'total': 10, 'fulfilled': 10, 'fulfillment_pct': 100},
           'storage': {'total_gb': 10, 'avg_gb_per_day': 1.5, 'by_modality': []},
           'volume': {'avg_per_day': 10, 'top_modalities': []}, 'physicians': [],
           'demographics': {'male': 50, 'female': 50},
           'tat': {'median_tat_min': 600, 'reported_count': 80, 'by_modality': []}}
    prev = {**cur, 'kpis': {**cur['kpis'], 'total_studies': 100}}
    text_of = lambda: ' | '.join(b for s in _generate_narrative(cur, prev, 'a', 'b', 'c', 'd', 7) for b in s['bullets'])
    with app.app_context():
        t = text_of()
        assert 'above the 8-hour inpatient guideline' in t and 'below 90% target' in t
        assert 'Storage intake elevated' not in t
        sql("""INSERT INTO settings (key, value) VALUES ('sr_tat_inpatient_warn_min', '720'),
               ('sr_coverage_warn_pct', '85'), ('sr_storage_gb_day_warn', '1'), ('sr_volume_drop_warn_pct', 'oops')
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""")
        t = text_of()
        assert 'within acceptable benchmark' in t                       # 600 min < 12 h
        assert 'Coverage below 85% — significant reporting backlog' in t
        assert 'Storage intake elevated at 1.5 GB/day' in t


def _page_js():
    import os
    return open(os.path.join(os.path.dirname(__file__), '..', 'static', 'js', 'super_report.js'), encoding='utf-8').read()


def test_the_filter_banner_and_the_cards_escape_what_they_show():
    """2.4: chip values come from the database."""
    js = _page_js()
    assert 'title="${c.label}' not in js and '${c.label}: ${c.value}</span>' not in js
    assert 'title="${k.cur||' not in js


# ── block 3 ───────────────────────────────────────────────────────────────────

def test_the_page_script_is_a_static_file_loaded_deferred(client):
    """3.1 / 3.2: no inline page script left, no Jinja inside the moved one, dead window._fd gone."""
    html = client.get('/viewer/super-report-page', headers=ADMIN).get_data(as_text=True)
    assert 'js/super_report.js" defer></script>' in html
    assert 'function runSuperReport' not in html
    js = _page_js()
    assert 'function runSuperReport' in js and '{{' not in js and '{%' not in js
    assert 'window._fd' not in js and 'function setPicked' in js        # setPicked loads presets


def test_the_narrative_writes_numbers_as_the_cards_do():
    """4.1: Python's _fmt / _pct / _fp and the page's fmt / pct give the same text (the page's
    side is checked in a browser: '—' for none, en-US separators, an exact half to even)."""
    from routes.super_report import _fp
    from utils.stats import _fmt, _pct
    assert _fp(_pct(109, 80)) == '+36.2%'          # 36.25 exactly: to even, as pct() does now
    assert _fp(_pct(105, 100)) == '+5%' and _fp(_pct(90, 100)) == '-10%'
    assert _fp(0.0) == '+0%' and _fp(-0.0) == '+0%' and _fp(-12.5) == '-12.5%' and _fp(None) is None
    assert _fmt(None) == '—' and _fmt(0) == '0' and _fmt(-1234) == '-1,234' and _fmt(1234.9) == '1,234'
    js = _page_js()
    assert "toLocaleString('en-US')" in js and 'toLocaleString()' not in js


def test_the_page_buttons_and_muted_text_use_css():
    """4.3: hover and keyboard focus in CSS, not onmouseover; muted text readable on the dark page."""
    import os
    page = open(os.path.join(os.path.dirname(__file__), '..', 'templates', 'super_report.html'), encoding='utf-8').read()
    assert 'onmouseover' not in page and 'onmouseout' not in page
    assert '.sr-export-btn:hover, .sr-export-btn:focus-visible' in page
    import re
    assert not re.search(r'style="[^"]*color:\s*#475569', page) and '#475569' not in _page_js()
    assert page.count('data-explain=') == page.count('role="button" tabindex="0"')   # icons take focus


def test_the_super_report_reads_the_utilization_through_the_report_34_wrapper():
    """3.3"""
    import inspect
    from routes import super_report
    src = inspect.getsource(super_report._get_utilization_summary)
    assert 'utilization_data(start, end)' in src and 'get_device_utilization_data' not in src.split('"""')[-1]
