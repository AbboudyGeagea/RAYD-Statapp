"""
tests/test_report_22_volume.py
────────────────────────────────────────────────────────────────
Report 22 was reconciled device by device against production PACS on 2026-11-05 for
October 2026. These pin what that settled, through the real route:

    LAUMCWFM1AR   Reports without images on PACS site '2', which is not a real site.
                  Counted aside, as on Report 23, not as a "PACS" modality row.
    SENO1         A mammography device at RH. Double reading stamps most of its studies
                  with PACS site '1', so site comes from the device and not from PACS.
                  Its studies are on RH RIS orders.
    Exams         637 studies - 2 repeated accessions + 124 linked RIS exams = 759.
"""
import json
import re

import pytest
from sqlalchemy import text

OCTOBER = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}
IN_OCTOBER = '2026-10-15'
ROUTER = 'LAUMCWFM1AR'


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


def device(aetitle, modality):
    """An RH device. Seeded rows are overwritten so a test states what it relies on."""
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES (:a, :m, (SELECT id FROM sites WHERE is_default LIMIT 1))
           ON CONFLICT (aetitle) DO UPDATE
           SET modality = EXCLUDED.modality, site_id = EXCLUDED.site_id""",
        a=aetitle, m=modality)


def study(uid, ae, accession=None, modality='MR', pacs_site='0', day=IN_OCTOBER, images=10):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, pacs_site_id_raw, number_of_study_images, study_status)
           VALUES (:u, :u, :iuid, :a, :ae, :d, :m, :site, :img, 'APPROVED')""",
        u=uid, iuid=f'1.2.3.{uid}', a=accession or f'A{uid}', ae=ae, d=day, m=modality,
        site=pacs_site, img=images)


def order(key, study_uid, accession, status='CM', modality='MR'):
    sql("""INSERT INTO etl_orders (order_dbid, study_db_uid, accession_number, order_status, modality)
           VALUES (:k, :s, :a, :st, :m)""",
        k=key, s=study_uid, a=accession, st=status, m=modality)


def page(client, **filters):
    response = client.get('/report/22', query_string={**OCTOBER, **filters},
                          headers={'X-Test-Role': 'admin'})
    assert response.status_code == 200
    return response.get_data(as_text=True)


def report(client, **filters):
    """The data the page hands to its own script."""
    return json.loads(re.search(r'const d = (.*);', page(client, **filters)).group(1))


def row(volume, modality):
    return next((r for r in volume['rows'] if r['modality'] == modality), None)


@pytest.fixture
def scanner_and_router():
    device('MR750W', 'MR')
    device(ROUTER, 'PACS')
    study(1, 'MR750W')
    study(2, ROUTER, modality='SR', pacs_site='2')
    study(3, ROUTER, modality='RF', pacs_site='2')


def test_the_router_is_counted_aside_and_not_as_a_modality(client, scanner_and_router):
    volume = report(client)['volume']
    assert row(volume, 'PACS') is None
    assert row(volume, 'MR')['studies'] == [1]
    assert volume['total_studies'] == 1
    assert volume['total_exams'] == 1
    assert volume['reports_without_images'] == 2


def test_the_router_stays_out_of_the_rest_of_the_page(client, scanner_and_router):
    assert sum(report(client)['stat_c'].values()) == 1


def test_the_page_shows_the_count_aside(client, scanner_and_router):
    assert 'Reports without images: 2' in page(client)


def test_nothing_is_shown_aside_when_there_is_nothing_to_show(client):
    device('MR750W', 'MR')
    study(1, 'MR750W')
    html = page(client)
    assert report(client)['volume']['reports_without_images'] == 0
    assert 'Reports without images:' not in html


def test_only_the_date_range_applies_to_the_count_aside(client, scanner_and_router):
    study(4, ROUTER, modality='SR', pacs_site='2', day='2026-09-30')
    filtered = report(client, f_mod_active='on', f_mod='MR')['volume']
    assert filtered['reports_without_images'] == 2


def test_the_router_stays_out_of_the_study_export(client, scanner_and_router):
    response = client.post('/report/22/export', data=OCTOBER, headers={'X-Test-Role': 'admin'})
    assert response.status_code == 200
    # the header line plus the one scanner study
    assert len(response.get_data(as_text=True).strip().splitlines()) == 2


def test_a_double_read_mammogram_counts_for_the_device_site(client):
    device('SENO1', 'MG')
    study(1, 'SENO1', modality='MG', pacs_site='0')
    study(2, 'SENO1', modality='MG', pacs_site='1')
    assert row(report(client)['volume'], 'MG')['studies'] == [2]


def test_a_device_at_the_other_site_stays_out(client):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('SJH_CT', 'CT', (SELECT id FROM sites WHERE NOT is_default LIMIT 1))""")
    study(1, 'SJH_CT', modality='CT', pacs_site='1')
    assert report(client)['volume']['total_studies'] == 0


def test_studies_and_exams_reconcile(client):
    """Studies, less repeated accessions, plus linked exams: how October's 759 ties out."""
    device('MR750W', 'MR')
    device('MR450W', 'MR')
    study(1, 'MR750W', accession='A')          # a study with a linked sibling
    order(1, 1, 'A')
    order(2, 1, 'A-LINKED')
    study(2, 'MR450W', accession='A', images=5)  # a second copy of the same accession
    study(3, 'MR750W', accession='B')          # one exam
    order(3, 3, 'B')
    study(4, 'MR750W', accession='C')          # no finished exam: still counts once
    order(4, 4, 'C', status='CA')
    volume = report(client)['volume']
    assert volume['total_studies'] == 4
    assert volume['total_exams'] == 4 - 1 + 1
