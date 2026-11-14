"""
tests/test_report_filter_panel.py — every report's left panel is the shared one
(templates/_report_filter_panel.html, CLAUDE.md rule 8), moved one report at a time
(LAUMC to-do item 16). For each report: the panel is drawn by the macro, its dropdowns
use the parameter names the report's filters read, a choice shows as chosen at once,
an unticked filter stays unticked after Generate, and the button says Generate.
"""
import pytest

ADMIN = {'X-Test-Role': 'admin'}
PERIOD = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}

# report page -> (dropdown name, tick name) pairs its panel must carry, and its Excel route
FOUR = (('patient_class', 'class_enabled'), ('modality', 'mod_enabled'),
        ('aetitle', 'ae_enabled'), ('patient_location', 'loc_enabled'))
REPORTS = {
    '/report/22': ((('f_status', 'f_status_active'), ('f_mod', 'f_mod_active'), ('f_ae', 'f_ae_active')),
                   None),                          # its CSV button shows once there is data
    '/report/25': (FOUR, '/report/25/export'),
    '/report/34': (FOUR, None),                    # its download shows once there is data
    '/report/35': ((('modality', 'mod_enabled'),), '/report/35/export'),
    '/report/36': (FOUR, None),
}


def modality(pairs):
    """The (dropdown, tick) pair of the report's modality filter."""
    return next(p for p in pairs if 'mod' in p[0])


@pytest.fixture
def ctx(app):
    with app.app_context():
        yield


def panel(client, url, **query):
    page = client.get(url, headers=ADMIN, query_string=query)
    assert page.status_code == 200, page.get_data(as_text=True)[:500]
    html = page.get_data(as_text=True)
    assert html.count('aria-label="Report filters"') == 1, 'not exactly one shared panel'
    return html, html[html.index('aria-label="Report filters"'):html.index('</aside>')]


@pytest.mark.parametrize('url', REPORTS)
def test_the_report_draws_the_shared_panel(client, ctx, url):
    pairs, export = REPORTS[url]
    mod, mod_tick = modality(pairs)
    html, side = panel(client, url, **PERIOD, **{mod: 'CT', mod_tick: 'on'})
    assert 'name="start_date" value="2026-10-01"' in side and 'name="end_date" value="2026-10-31"' in side
    for name, toggle in pairs:
        assert f'name="{name}" multiple' in side and f'name="{toggle}"' in side
    assert '<option value="CT" selected>CT</option>' in side        # the choice shows at once
    assert 'class="rfp-generate">Generate</button>' in side
    if export:
        assert f"rfpExport('{export}')" in side


@pytest.mark.parametrize('url', REPORTS)
def test_an_unticked_filter_stays_unticked(client, ctx, url):
    pairs, _ = REPORTS[url]
    _, mod_tick = modality(pairs)
    _, side = panel(client, url, **PERIOD, **{mod_tick: 'on'})
    for name, toggle in pairs:
        ticked = f'name="{toggle}" class="rfp-tick" checked' in side
        assert ticked == (toggle == mod_tick), toggle


@pytest.mark.parametrize('url', REPORTS)
def test_every_tick_is_on_before_the_first_generate(client, ctx, url):
    pairs, _ = REPORTS[url]
    _, side = panel(client, url)
    for _name, toggle in pairs:
        assert f'name="{toggle}" class="rfp-tick" checked' in side, toggle


def test_report_25_keeps_its_open_tab(client, ctx):
    _, side = panel(client, '/report/25', **PERIOD, active_tab='peak')
    assert 'name="active_tab" id="rfp-hidden-active_tab" value="peak"' in side


def test_report_22_keeps_include_or_exclude(client, ctx):
    _, side = panel(client, '/report/22', **PERIOD, f_mod='CT', f_mod_active='on', f_mod_mode='exclude')
    assert 'name="f_mod_mode" class="rfp-mode is-exclude"' in side
    assert '<option value="exclude" selected>Exclude</option>' in side
    assert 'name="f_ae_mode" class="rfp-mode"' in side             # the others stay Include


def test_the_super_report_draws_the_shared_panel(client, ctx):
    """The Super Report runs in the page (Generate fetches /viewer/super-report), so its
    panel is drawn empty and filled from /viewer/super-report/filters; each dropdown is
    named as the query reads it, with its own tick."""
    from routes.super_report import _PANEL_FILTERS
    html, side = panel(client, '/viewer/super-report-page')
    for name, _label in _PANEL_FILTERS:
        assert f'name="{name}" multiple' in side and f'name="{name}_on" class="rfp-tick" checked' in side
    for control in ('id="cmp-start"', 'id="age-min"', 'id="f-has_report"', 'id="presets-list"'):
        assert control in side
    assert 'class="rfp-generate">Generate</button>' in side
    # the query never applied these two, so they are no longer offered
    assert 'protocol_name' not in side and 'body_part_series' not in side
    # the page builds its query from the panel and honours the ticks
    assert "addEventListener('submit'" in html and '_on"]' in html


def test_the_super_report_query_reads_every_panel_name():
    """Every dropdown the Super Report panel offers is a parameter its query applies."""
    import inspect
    from routes import super_report
    read = inspect.getsource(super_report.super_report)
    where = inspect.getsource(super_report._build_where) + inspect.getsource(super_report._collect_data)
    for name, _label in super_report._PANEL_FILTERS:
        assert f'"{name}"' in read, name
        assert f'"{name}"' in where or f"'{name}'" in where, name


def test_the_export_button_posts_with_the_csrf_token():
    """The panel's Excel button sends a POST (the export routes accept nothing else) and
    adds the CSRF token, which the page's own helper only adds to POST forms."""
    import os
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            'templates', '_report_filter_panel.html'), encoding='utf-8').read()
    body = src[src.index('function rfpExport'):]
    assert "setAttribute('method', 'POST')" in body and "'csrf_token'" in body


def test_panel_filters_reads_what_sidebar_filters_reads():
    """One name per parameter: the panel and the SQL filter cannot drift apart."""
    from werkzeug.datastructures import MultiDict
    from utils.report_filters import panel_filters, PANEL_FILTERS
    form = MultiDict([('start_date', '2026-10-01'), ('modality', 'CT'), ('modality', 'MR'),
                      ('mod_enabled', 'on'), ('aetitle', '')])
    by_name = {f['name']: f for f in panel_filters(form)}
    assert list(by_name) == list(PANEL_FILTERS)
    assert by_name['modality']['selected'] == ['CT', 'MR'] and by_name['modality']['enabled']
    assert by_name['aetitle']['selected'] == [] and not by_name['aetitle']['enabled']
    assert by_name['modality']['key'] == 'modalities'
