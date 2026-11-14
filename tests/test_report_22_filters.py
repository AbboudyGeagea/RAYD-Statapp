"""
tests/test_report_22_filters.py
────────────────────────────────────────────────────────────────
Report 22's sidebar filters (meeting of 2026-11-13: "filtering on one or more AE titles
shows 0"), checked through the real route and the real dropdown endpoint.

What the operator sees is the dropdown and the totals, so the checks are sums over
what the dropdown offers rather than single cases:

    every AE title offered returns studies
    the AE titles, one at a time, add up to the unfiltered total
    include X + exclude X = the unfiltered total, for every filter
    picking every value offered = the unfiltered total

The data carries what production carries: a study whose AE is stored in lower case
with a trailing space, a device whose mapped AE ends in a tab (str.strip() and SQL
TRIM disagree on it), an unmapped AE, a device of the other site, and a double-read
mammogram on PACS site '1'.
"""
import pytest

from test_report_22_volume import ctx, sql, device, study, report  # noqa: F401

ADMIN = {'X-Test-Role': 'admin'}
OCTOBER = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}
TAB_AE = 'CT2\t'


def total(client, **filters):
    return sum(report(client, **filters)['stat_c'].values())


def offered(client, key):
    response = client.get('/api/filter-options', headers=ADMIN)
    assert response.status_code == 200
    return response.get_json()[key]


def ae(client, values, mode='include'):
    return total(client, f_ae_active='on', f_ae=values, f_ae_mode=mode)


@pytest.fixture
def site_mix():
    device('MR750W', 'MR')
    device('CT1', 'CT')
    device(TAB_AE, 'CT')
    device('SENO1', 'MG')
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('SJH_CT', 'CT', (SELECT id FROM sites WHERE NOT is_default LIMIT 1))""")
    study(1, 'MR750W')
    study(2, 'mr750w ', modality='MR')
    study(3, 'CT1', modality='CT')
    study(4, 'CT1', modality='CT')
    study(5, TAB_AE, modality='CT')
    study(6, 'SENO1', modality='MG', pacs_site='1')
    study(7, 'UNMAPPED_AE', modality='US')
    study(8, 'SJH_CT', modality='CT', pacs_site='1')
    sql("UPDATE etl_didb_studies SET study_status = 'READ' WHERE study_db_uid IN (3, 6)")
    return 6   # studies 1-6 are RH; 7 has no device, 8 is the other site's


# ── AE title ────────────────────────────────────────────────────────────────────

def test_every_ae_offered_returns_studies(client, site_mix):
    for value in offered(client, 'aetitles'):
        assert ae(client, value) > 0, f'{value!r} is offered and shows 0'


def test_an_ae_no_report_can_count_is_not_offered(client, site_mix):
    aes = offered(client, 'aetitles')
    assert 'UNMAPPED_AE' not in aes
    assert 'SJH_CT' not in aes


def test_the_aes_add_up_to_the_total(client, site_mix):
    assert total(client) == site_mix
    assert sum(ae(client, v) for v in offered(client, 'aetitles')) == site_mix


def test_include_and_exclude_add_up_for_every_ae(client, site_mix):
    for value in offered(client, 'aetitles'):
        assert ae(client, value) + ae(client, value, 'exclude') == site_mix, value


def test_picking_every_ae_offered_is_the_total(client, site_mix):
    assert ae(client, offered(client, 'aetitles')) == site_mix


def test_several_aes_together(client, site_mix):
    assert ae(client, ['MR750W', 'CT1']) == 4
    assert ae(client, ['MR750W', 'CT1'], 'exclude') == 2


def test_case_and_padding_do_not_matter(client, site_mix):
    assert ae(client, 'MR750W') == 2
    assert ae(client, 'mr750w') == 2
    assert ae(client, ' Mr750W ') == 2


def test_an_ae_ending_in_a_tab_round_trips(client, site_mix):
    """The dropdown offers TRIM(storing_ae), which keeps the tab. The filter used to
    clean the pick with str.strip(), which drops it, and then matched nothing."""
    assert TAB_AE in offered(client, 'aetitles')
    assert ae(client, TAB_AE) == 1


def test_a_double_read_mammogram_is_found_by_its_device(client, site_mix):
    assert ae(client, 'SENO1') == 1


def test_an_old_single_value_bookmark_still_works(client, site_mix):
    assert total(client, f_ae_active='on', f_ae='CT1') == 2


def test_the_filter_is_ignored_when_its_tick_is_off(client, site_mix):
    assert total(client, f_ae='CT1') == site_mix


def test_an_empty_pick_is_ignored(client, site_mix):
    assert total(client, f_ae_active='on', f_ae=['', ' ']) == site_mix


# ── the AE filter reaches every part of the page ───────────────────────────────

def test_the_ae_filter_reaches_every_section(client, site_mix):
    d = report(client, f_ae_active='on', f_ae='CT1')
    assert sum(d['stat_c'].values()) == 2
    assert d['volume']['total_studies'] == 2
    assert [r['modality'] for r in d['volume']['rows']] == ['CT']
    assert sum(g['cnt'] for g in d['proc_per_modality']['groups']['CT']) == 2
    assert d['tree_data'][0]['name'] == 'TOTAL\n2'


def test_the_ae_filter_reaches_the_export(client, site_mix):
    response = client.post('/report/22/export', headers=ADMIN,
                           data={**OCTOBER, 'f_ae_active': 'on', 'f_ae': ['CT1', 'mr750w']})
    assert response.status_code == 200
    assert len(response.get_data(as_text=True).strip().splitlines()) == 1 + 4


def test_the_ae_filter_reaches_the_status_drilldown(client, site_mix):
    response = client.post('/report/22/status-drilldown', headers=ADMIN,
                           data={**OCTOBER, 'status': 'APPROVED',
                                 'f_ae_active': 'on', 'f_ae': 'CT1'})
    assert response.status_code == 200
    assert response.get_json()['count'] == 1


# ── status and modality ─────────────────────────────────────────────────────────

def test_every_status_offered_returns_studies_and_they_add_up(client, site_mix):
    statuses = offered(client, 'statuses')
    assert set(statuses) == {'APPROVED', 'READ'}
    counts = [total(client, f_status_active='on', f_status=s) for s in statuses]
    assert all(counts)
    assert sum(counts) == site_mix


def test_include_and_exclude_add_up_for_every_status(client, site_mix):
    for s in offered(client, 'statuses'):
        inc = total(client, f_status_active='on', f_status=s)
        exc = total(client, f_status_active='on', f_status=s, f_status_mode='exclude')
        assert inc + exc == site_mix, s


def test_the_modalities_add_up_and_exclude_complements(client, site_mix):
    modalities = offered(client, 'modalities')
    assert {'MR', 'CT', 'MG'} <= set(modalities)
    assert sum(total(client, f_mod_active='on', f_mod=m) for m in modalities) == site_mix
    assert total(client, f_mod_active='on', f_mod='ct') == 3
    assert total(client, f_mod_active='on', f_mod='CT', f_mod_mode='exclude') == 3


def test_filters_combine(client, site_mix):
    assert total(client, f_mod_active='on', f_mod='CT',
                 f_status_active='on', f_status='READ') == 1
    assert total(client, f_mod_active='on', f_mod='CT',
                 f_ae_active='on', f_ae='CT1', f_ae_mode='exclude') == 1
