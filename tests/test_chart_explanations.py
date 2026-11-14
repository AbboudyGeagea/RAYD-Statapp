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
    """{template file: set of data-explain keys it uses}"""
    out = {}
    for name in os.listdir(TEMPLATES):
        if name.endswith('.html'):
            text = open(os.path.join(TEMPLATES, name), encoding='utf-8').read()
            keys = set(re.findall(r'data-explain="([^"]+)"', text))
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


def test_report_22_shows_the_icons(client, ctx):
    html = client.get('/report/22', headers={'X-Test-Role': 'admin'},
                      query_string={'start_date': '2026-10-01', 'end_date': '2026-10-31'}
                      ).get_data(as_text=True)
    assert 'js/chart-explanations.js' in html
    for key in ('r22-volume', 'r22-tree', 'r22-top-referrers', 'r22-status'):
        assert f'data-explain="{key}"' in html
