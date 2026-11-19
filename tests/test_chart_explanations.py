"""
tests/test_chart_explanations.py
────────────────────────────────────────────────────────────────
Item 12 of the meeting of 2026-11-13: an info icon on every chart, opening a note written
for LAUMC's own queries (static/js/chart-explanations.js). These keep the icons and the
notes in step as pages are added: every key a template names has a full note, and every
page with an icon loads the script.
"""
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(ROOT, 'static', 'js', 'chart-explanations.js')
TEMPLATES = os.path.join(ROOT, 'templates')
PARTS = ('title', 'purpose', 'valueMeaning', 'interpretation')


def notes():
    """{key: block of text} for every explanation in the script."""
    src = open(JS, encoding='utf-8').read()
    body = src.split('explanations: {', 1)[1].split('\n  },\n', 1)[0]
    found = list(re.finditer(r"^\s*'([a-z0-9-]+)':\s*\{", body, re.M))
    return {m.group(1): body[m.end():(found[i + 1].start() if i + 1 < len(found) else len(body))]
            for i, m in enumerate(found)}


def template_keys():
    """{template file: set of data-explain keys it uses}: written out, or passed to the page's
    info('key', ...) macro (which writes data-explain="{{ key }}")."""
    out = {}
    for name in os.listdir(TEMPLATES):
        if name.endswith('.html'):
            text = open(os.path.join(TEMPLATES, name), encoding='utf-8').read()
            keys = {k for k in re.findall(r'data-explain="([^"]+)"', text) if '{' not in k}
            keys |= set(re.findall(r"""\binfo\(\s*'([a-z0-9-]+)'""", text))
            if keys:
                out[name] = (keys, text)
    return out


def test_every_note_is_complete():
    found = notes()
    assert found, 'no explanations found in chart-explanations.js'
    for key, block in found.items():
        for part in PARTS:
            least = 3 if part == 'title' else 12      # a title can be one word
            assert re.search(rf"\b{part}:\s*'[^']{{{least},}}", block), f'{key} has no {part}'


def unescaped_quote_lines(src):
    """Line numbers of note parts whose single-quoted string holds a bare apostrophe."""
    part = re.compile(r"^\s*(?:%s):\s*'(.*)',?\s*$" % '|'.join(PARTS))
    checked, bad = 0, []
    for no, line in enumerate(src.splitlines(), 1):
        m = part.match(line)
        if m:
            checked += 1
            if re.search(r"(?<!\\)'", m.group(1)):
                bad.append(no)
    return checked, bad


def test_every_note_is_one_valid_string():
    """19 Nov: "procedure's" and "room's" unescaped closed a note's string early, so the script
    stopped loading and every (i) on every page was empty. Write \\' or ’ inside a note."""
    checked, bad = unescaped_quote_lines(open(JS, encoding='utf-8').read())
    assert checked > 100, 'the note lines were not found: update unescaped_quote_lines'
    assert not bad, f'unescaped apostrophe in static/js/chart-explanations.js, lines {bad}'
    assert unescaped_quote_lines("    valueMeaning: 'else the procedure's only room',")[1] == [1]


def test_every_icon_has_a_note():
    known = set(notes())
    for name, (keys, _text) in template_keys().items():
        assert keys <= known, f'{name} names explanations with no text: {sorted(keys - known)}'


def _pages_including(partial):
    """Templates that {% include %} the partial."""
    out = []
    for name in os.listdir(TEMPLATES):
        if name.endswith('.html'):
            text = open(os.path.join(TEMPLATES, name), encoding='utf-8').read()
            if re.search(r"""\{%\s*include\s+['"]""" + re.escape(partial) + r"""['"]""", text):
                out.append((name, text))
    return out


def test_every_page_with_an_icon_loads_the_script():
    for name, (_keys, text) in template_keys().items():
        if name.startswith('_'):
            # a partial has no <head>: every page that includes it loads the script
            pages = _pages_including(name)
            assert pages, f'{name} has icons but no page includes it'
            for page, page_text in pages:
                assert 'js/chart-explanations.js' in page_text, \
                    f'{page} includes {name}, which has icons, but does not load the script'
        else:
            assert 'js/chart-explanations.js' in text, f'{name} has icons but does not load the script'


@pytest.fixture
def ctx(app):
    with app.app_context():
        yield


def test_super_report_shows_the_icons(client, ctx):
    html = client.get('/viewer/super-report-page', headers={'X-Test-Role': 'administrator'}).get_data(as_text=True)
    assert 'js/chart-explanations.js' in html
    for key in ('sr-briefing', 'sr-tat', 'sr-orders', 'sr-rad-matrix', 'r23-conflicts'):
        assert f'data-explain="{key}"' in html


def test_report_40_explains_its_title_every_figure_and_chart(client, ctx):
    """Operator, 2026-11-18: an (i) next to every new report's name, chart and figure."""
    html = client.get('/report/40', headers={'X-Test-Role': 'administrator'},       # Generate pressed
                      query_string={'start_date': '2026-10-01', 'end_date': '2026-10-31'}).get_data(as_text=True)
    for key in ('r40-report', 'r40-kpi-exams', 'r40-kpi-median', 'r40-kpi-p90', 'r40-kpi-same-day',
                'r40-kpi-ahead', 'r40-kpi-cancelled', 'r40-kpi-rescheduled', 'r40-kpi-after'):
        assert f'data-explain="{key}"' in html


def test_report_22_shows_the_icons(client, ctx):
    html = client.get('/report/22', headers={'X-Test-Role': 'administrator'},
                      query_string={'start_date': '2026-10-01', 'end_date': '2026-10-31'}
                      ).get_data(as_text=True)
    assert 'js/chart-explanations.js' in html
    for key in ('r22-volume', 'r22-tree', 'r22-top-referrers', 'r22-status'):
        assert f'data-explain="{key}"' in html
