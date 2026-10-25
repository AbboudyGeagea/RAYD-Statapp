"""
Dynamic Worklists: the rpt datasets, the compiler, the sandbox they run in, and the
editor's API.

The sandbox tests are the ones that matter most. Worklist SQL — including SQL an IT
user writes by hand — must run as the per-database rpt_ role of migration 0136 and
be unable to read anything outside rpt.*, write anything, switch role, or run away.
"""
import csv
import io
import re
import time
from datetime import date, datetime, timedelta

import pytest

import hl7kit as k
from utils import worklist_catalog as cat
from utils import worklist_engine as eng
from utils.worklist_engine import WorklistError

BASE = k.now()
SU, OTHER, IMPL, ADMIN = 4242, 4343, 4444, 4545


def at(m):
    return BASE - timedelta(minutes=m)


@pytest.fixture(autouse=True)
def users(pipe):
    pipe.exec("""
        INSERT INTO users (id, username, password_hash, role) VALUES
            (4242, 't-su', 'x', 'su'), (4343, 't-user', 'x', 'user'),
            (4444, 't-impl', 'x', 'implementation'), (4545, 't-admin', 'x', 'administrator')
        ON CONFLICT (id) DO NOTHING""")
    yield
    pipe.exec("DELETE FROM user_page_permissions WHERE user_id IN (4242, 4343, 4444, 4545)")
    pipe.exec("DELETE FROM users WHERE id IN (4242, 4343, 4444, 4545)")


def run(app, defn, **kw):
    """Compile + run in the sandbox; rows as dicts."""
    with app.app_context():
        compiled = eng.compile_definition(defn)
        res = eng.run(compiled=compiled, **kw)
    return [dict(zip(res['columns'], r)) for r in res['rows']]


def hand(app, sql, cap=eng.ROW_CAP):
    with app.app_context():
        return eng.run(hand_sql=sql, cap=cap)


def exam(pipe, acc, start=300, pat='P0001', ae='CT64_RH', mod='CT', tech='TECH01^HADDAD^RIMA',
         report=True, **kw):
    for i, code in enumerate(('SC', 'AR', 'IP', 'CM')):
        pipe.send(k.status(code, acc, k.hl7(at(start - 20 * i)), pat=pat, ae=ae, mod=mod,
                           tech=tech, **kw))
    if report:
        pipe.send(k.report(acc, k.hl7(at(start - 120)), pat=pat, mod=mod))


def h(client, method, url, role='su', uid=SU, **kw):
    return getattr(client, method)(url, headers={'X-Test-Role': role, 'X-Test-Uid': str(uid)},
                                   **kw)


EXAMS_LIST = {'dataset': 'exams', 'criteria': {}, 'output': {'kind': 'list'}}


# ── the catalog matches the views ─────────────────────────────────────────────

@pytest.mark.parametrize('dataset', list(cat.DATASETS))
def test_every_catalog_column_exists_in_its_view(pipe, dataset):
    view = cat.DATASETS[dataset]['view'].split('.')[1]
    cols = {r['c'] for r in pipe.q("SELECT column_name AS c FROM information_schema.columns "
                                   "WHERE table_schema = 'rpt' AND table_name = :t", t=view)}
    wanted = {f['col'] for f in cat.fields_for(dataset)} | {c for c, _ in cat.COLUMNS[dataset]}
    assert wanted - cols == set()


@pytest.mark.parametrize('dataset,measure', [(d, m) for d in cat.DATASETS
                                             for m, _ in cat.measures_for(d)])
def test_every_measure_runs(app, pipe, dataset, measure):
    run(app, {'dataset': dataset, 'criteria': {},
              'output': {'kind': 'table', 'measures': [measure]}})


@pytest.mark.parametrize('dataset,dim', [(d, x) for d in cat.DATASETS
                                         for x, _ in cat.dimensions_for(d)])
def test_every_grouping_runs(app, pipe, dataset, dim):
    exam(pipe, 'A1')
    rows = run(app, {'dataset': dataset, 'criteria': {},
                     'output': {'kind': 'table', 'rows': dim, 'measures': ['count']}})
    assert all(r['row'] is not None for r in rows)


SAMPLE = {'date': {'mode': 'quick', 'quick': 'last_30_days'}, 'multi': {'values': ['X']},
          'text': {'op': 'contains', 'value': 'x'}, 'yesno': {'value': 'yes'},
          'range': {'min': 0, 'max': 10}}


@pytest.mark.parametrize('dataset,fid', [(d, f['id']) for d in cat.DATASETS
                                         for f in cat.fields_for(d)])
def test_every_criterion_compiles_and_runs(app, pipe, dataset, fid):
    f = cat.FIELD[fid]
    c = SAMPLE[f['type']]
    if f['type'] == 'multi' and isinstance(f.get('options'), list):
        c = {'values': [f['options'][0][0]]}
    run(app, {'dataset': dataset, 'criteria': {fid: c}, 'output': {'kind': 'list'}})


EXAMPLES = ['Exams per modality, by month', 'Technicians: AM shift, first week of the month',
            'Report turnaround by radiologist', 'Orders not performed', 'Emergency exams today',
            'Exams with open data-quality notes']


@pytest.mark.parametrize('name', EXAMPLES)
def test_seeded_example_runs(app, pipe, name):
    w = pipe.one("SELECT definition FROM worklists WHERE folder = 'Examples' AND name = :n", n=name)
    assert w is not None
    exam(pipe, 'A1')
    run(app, w['definition'])


def test_examples_are_shared_and_versioned(pipe):
    assert pipe.count('worklists', "folder = 'Examples' AND visibility = 'shared'") == len(EXAMPLES)
    assert pipe.count('worklist_versions') >= len(EXAMPLES)


# ── what the datasets say about a study ───────────────────────────────────────

@pytest.fixture
def one_study(pipe):
    pipe.send(k.adt('A08', when=k.hl7(at(400))))
    pipe.send(k.order('PL1', when=k.hl7(at(320))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(300)), placer='PL1'))
    pipe.send(k.status('AR', 'A1', k.hl7(at(280))))
    pipe.send(k.status('IP', 'A1', k.hl7(at(260))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(240))))
    pipe.send(k.report('A1', k.hl7(at(180))))
    return pipe


def exam_row(pipe, acc='A1'):
    return pipe.one("SELECT * FROM rpt.exams WHERE accession = :a", a=acc)


def _shift(hour):
    for name, s, e in (('AM', 7, 15), ('PM', 15, 23), ('NIGHT', 23, 7)):
        if (s < e and s <= hour < e) or (s >= e and (hour >= s or hour < e)):
            return name


@pytest.mark.parametrize('column,want', [
    ('patient_id', 'P0001'), ('sex', 'F'), ('patient_type', 'Outpatient'),
    ('patient_location', 'RAD'), ('modality', 'CT'), ('device', 'CT64_RH'),
    ('room', 'CT-ROOM-2'), ('procedure_code', 'CTABD'), ('procedure', 'CT ABDOMEN AND PELVIS'),
    ('status', 'Signed final'), ('technician', 'RIMA HADDAD'), ('radiologist', 'rad@sjh.com'),
    ('referring', 'GERARD WAKIM'), ('has_report', True), ('critical', False),
    ('open_quality_notes', 0), ('stalled', False), ('placer_order_number', 'PL1'),
])
def test_exams_columns(one_study, column, want):
    assert exam_row(one_study)[column] == want


def test_exams_times_and_durations(one_study):
    r = exam_row(one_study)
    assert (r['exam_at'], r['performed_at'], r['ordered_at']) == (at(240), at(260), at(320))
    assert float(r['wait_minutes']) == 20.0 and float(r['exam_minutes']) == 20.0
    assert float(r['report_tat_hours']) == 1.0
    assert r['report_final_at'] == at(180)


def test_exams_calendar_fields(one_study):
    r = exam_row(one_study)
    d = at(240)
    assert r['exam_date'] == d.date()
    assert r['week_of_month'] == (d.day - 1) // 7 + 1
    assert r['weekday'] == d.isoweekday()
    assert r['hour'] == at(260).hour
    assert r['shift'] == _shift(at(260).hour)


def test_exams_age(one_study):
    r = exam_row(one_study)
    born = date(1985, 3, 12)
    d = at(240).date()
    years = d.year - born.year - ((d.month, d.day) < (born.month, born.day))
    assert r['age'] == years and r['age_band'] == '40-64'


def test_shift_follows_the_shift_table(app, pipe, client, one_study):
    hour = at(260).hour
    h(client, 'post', '/worklists/api/shifts', json={'shifts': [
        {'name': 'ALLDAY', 'start_hour': 0, 'end_hour': 24}]})
    assert exam_row(one_study)['shift'] == 'ALLDAY'
    h(client, 'post', '/worklists/api/shifts', json={'shifts': [
        {'name': 'WRAP', 'start_hour': (hour + 1) % 24, 'end_hour': hour + 1 if hour + 1 <= 23 else 24}]})
    assert exam_row(one_study)['shift'] == 'WRAP'


def test_open_quality_note_shows_in_the_dataset(pipe):
    pipe.send(k.status('AR', 'A1', k.hl7(at(100))))
    pipe.send(k.status('CM', 'A1', k.hl7(at(150))))
    assert exam_row(pipe)['open_quality_notes'] >= 1


def test_stall_shows_in_the_dataset(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(400)))
    pipe.send(k.status('AR', 'A1', k.ts(300)))
    pipe.sweep()
    assert exam_row(pipe)['stalled'] is True


def test_cancelled_study(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(100)))
    pipe.send(k.status('CA', 'A1', k.ts(90)))
    assert exam_row(pipe)['status'] == 'Cancelled'


def test_an_order_alone_is_not_an_exam(pipe):
    pipe.send(k.order('PL9', when=k.ts(30)))
    assert pipe.count('rpt.exams') == 0
    o = pipe.one("SELECT * FROM rpt.orders")
    assert (o['status'], o['scheduled'], o['performed'], o['accession']) == \
           ('Not scheduled', False, False, None)


def test_sr_is_excluded(pipe):
    pipe.send(k.status('CM', 'A-SR', k.ts(10), mod='SR', ae='NOT_MAPPED'))
    assert pipe.count('rpt.exams') == 0


def test_orders_follow_their_study(one_study):
    o = one_study.one("SELECT * FROM rpt.orders")
    assert (o['status'], o['scheduled'], o['performed'], o['reported'], o['accession']) == \
           ('Reported', True, True, True, 'A1')
    assert float(o['order_to_exam_hours']) == round(80 / 60, 2)
    assert o['referring'] == 'GERARD WAKIM'


def test_reports_dataset(one_study):
    r = one_study.one("SELECT * FROM rpt.reports")
    assert (r['accession'], r['radiologist'], r['signature'], r['modality']) == \
           ('A1', 'rad@sjh.com', 'Final', 'CT')
    assert float(r['report_tat_hours']) == 1.0


# ── criteria do what they say ─────────────────────────────────────────────────

@pytest.fixture
def two(pipe):
    # The referring doctor is projected from the HIS order (or the report's PV1-8),
    # so each study gets its order.
    pipe.send(k.order('PL-CT', acc='CT1', pat='PA', when=k.hl7(at(330))))
    pipe.send(k.order('PL-MR', acc='MR1', pat='PB', mod='MR', when=k.hl7(at(230)),
                      ref='99^HADDAD^SAMIR'))
    exam(pipe, 'CT1', start=300, pat='PA', ae='CT64_RH', mod='CT')
    exam(pipe, 'MR1', start=200, pat='PB', ae='MR1', mod='MR', report=False)
    return pipe


def accs(rows):
    return sorted(r['accession'] for r in rows)


def ex(criteria, **out):
    return {'dataset': 'exams', 'criteria': criteria,
            'output': {'kind': 'list', 'columns': ['accession', 'exam_at'], **out}}


def test_multi(app, two):
    assert accs(run(app, ex({'modality': {'values': ['MR']}}))) == ['MR1']
    assert accs(run(app, ex({'modality': {'values': ['MR', 'CT']}}))) == ['CT1', 'MR1']


def test_blank_selects_missing_values(app, two):
    assert accs(run(app, ex({'site': {'values': ['(blank)']}}))) == ['CT1', 'MR1']
    assert accs(run(app, ex({'radiologist': {'values': ['(blank)']}}))) == ['MR1']


@pytest.mark.parametrize('op,value,want', [
    ('contains', 'wakim', ['CT1']), ('contains', 'HADD', ['MR1']), ('equals', 'gerard wakim', ['CT1']),
    ('starts with', 'Samir', ['MR1']), ('does not contain', 'wakim', ['MR1']),
    ('contains', '%', []), ('contains', '_', []),
])
def test_text(app, two, op, value, want):
    assert accs(run(app, ex({'referring': {'op': op, 'value': value}}))) == want


@pytest.mark.parametrize('value,want', [('yes', ['CT1']), ('no', ['MR1']), ('any', ['CT1', 'MR1'])])
def test_yesno(app, two, value, want):
    assert accs(run(app, ex({'has_report': {'value': value}}))) == want


def test_range(app, two):
    assert accs(run(app, ex({'exam_minutes': {'min': 20, 'max': 20}}))) == ['CT1', 'MR1']
    assert accs(run(app, ex({'exam_minutes': {'min': 21}}))) == []
    assert accs(run(app, ex({'report_tat_hours': {'max': 2}}))) == ['CT1']


@pytest.mark.parametrize('crit,want', [
    ({'mode': 'relative', 'dir': 'prev', 'n': 200, 'unit': 'minutes'}, None),
    ({'mode': 'relative', 'dir': 'prev', 'n': 4, 'unit': 'hours'}, ['MR1']),
    ({'mode': 'relative', 'dir': 'prev', 'n': 6, 'unit': 'hours'}, ['CT1', 'MR1']),
    ({'mode': 'relative', 'dir': 'next', 'n': 3, 'unit': 'days'}, []),
    ({'mode': 'none'}, ['CT1', 'MR1']),
])
def test_relative_dates(app, two, crit, want):
    if want is None:
        with pytest.raises(WorklistError):
            run(app, ex({'exam_at': crit}))
        return
    assert accs(run(app, ex({'exam_at': crit}))) == want


def test_fixed_dates_include_the_last_day(app, two):
    d = at(240).date().isoformat()
    assert 'CT1' in accs(run(app, ex({'exam_at': {'mode': 'fixed', 'from': d, 'to': d}})))


def test_fixed_dates_in_either_order(app, two):
    d = at(240).date()
    a, b = (d + timedelta(1)).isoformat(), (d - timedelta(1)).isoformat()
    assert 'CT1' in accs(run(app, ex({'exam_at': {'mode': 'fixed', 'from': a, 'to': b}})))


@pytest.mark.parametrize('quick', [q for q, _ in cat.QUICK_DATES])
def test_quick_dates_bound_sensibly(quick):
    a, b = eng.date_bounds({'mode': 'quick', 'quick': quick}, datetime(2026, 10, 25, 13, 0))
    assert a < b
    if quick in ('today', 'this_week', 'this_month', 'this_year', 'last_7_days', 'last_30_days'):
        assert a <= datetime(2026, 10, 25, 13, 0) < b
    else:
        assert b <= datetime(2026, 10, 25)


@pytest.mark.parametrize('quick,span', [
    ('last_month', (datetime(2026, 9, 1), datetime(2026, 10, 1))),
    ('last_3_months', (datetime(2026, 7, 1), datetime(2026, 10, 1))),
    ('this_week', (datetime(2026, 10, 19), datetime(2026, 10, 26))),
    ('last_week', (datetime(2026, 10, 12), datetime(2026, 10, 19))),
    ('yesterday', (datetime(2026, 10, 24), datetime(2026, 10, 25))),
    ('last_year', (datetime(2025, 1, 1), datetime(2026, 1, 1))),
])
def test_quick_date_spans(quick, span):
    assert eng.date_bounds({'mode': 'quick', 'quick': quick}, datetime(2026, 10, 25, 13)) == span


def test_january_last_month_wraps_the_year():
    assert eng.date_bounds({'mode': 'quick', 'quick': 'last_month'}, datetime(2026, 1, 5)) == \
           (datetime(2025, 12, 1), datetime(2026, 1, 1))


def test_group_and_measures(app, two):
    rows = run(app, {'dataset': 'exams', 'criteria': {},
                     'output': {'kind': 'table', 'rows': 'modality',
                                'measures': ['count', 'reported', 'median_exam_minutes']}})
    assert rows == [{'row': 'CT', 'count': 1, 'reported': 1, 'median_exam_minutes': 20.0},
                    {'row': 'MR', 'count': 1, 'reported': 0, 'median_exam_minutes': 20.0}]


def test_two_dimensions_keep_one_measure(app, two):
    rows = run(app, {'dataset': 'exams', 'criteria': {},
                     'output': {'kind': 'chart', 'rows': 'modality', 'cols': 'patient_type',
                                'measures': ['count', 'reported']}})
    assert rows == [{'row': 'CT', 'col': 'Outpatient', 'count': 1},
                    {'row': 'MR', 'col': 'Outpatient', 'count': 1}]


def test_no_grouping_is_one_row(app, two):
    assert run(app, {'dataset': 'exams', 'criteria': {},
                     'output': {'kind': 'table', 'measures': ['count', 'patients']}}) == \
           [{'count': 2, 'patients': 2}]


def test_list_columns_and_sort(app, two):
    rows = run(app, ex({}, columns=['accession', 'modality'], sort='accession', desc=False))
    assert [list(r) for r in rows] == [['accession', 'modality']] * 2
    assert [r['accession'] for r in rows] == ['CT1', 'MR1']
    rows = run(app, ex({}, columns=['accession'], sort='accession', desc=True))
    assert [r['accession'] for r in rows] == ['MR1', 'CT1']


def test_date_bucket_grouping(app, two):
    rows = run(app, {'dataset': 'exams', 'criteria': {},
                     'output': {'kind': 'table', 'rows': 'exam_at:month', 'measures': ['count']}})
    assert rows == [{'row': at(240).strftime('%Y-%m'), 'count': 2}] or \
        sum(r['count'] for r in rows) == 2


# ── the compiler refuses what the catalog does not offer ──────────────────────

@pytest.mark.parametrize('defn,msg', [
    ({'dataset': 'users'}, 'unknown dataset'),
    ({'dataset': 'reports', 'criteria': {'room': {'values': ['x']}}}, 'not a field'),
    ({'dataset': 'exams', 'criteria': {'modality; DROP TABLE users': {}}}, 'not a field'),
    ({'dataset': 'exams', 'output': {'kind': 'table', 'rows': 'password_hash'}}, 'cannot group'),
    ({'dataset': 'exams', 'output': {'kind': 'table', 'measures': ['sum(password)']}}, 'not a measure'),
    ({'dataset': 'exams', 'output': {'kind': 'list', 'columns': ['password_hash']}}, 'not a column'),
    ({'dataset': 'exams', 'output': {'kind': 'list', 'sort': 'random()'}}, 'cannot sort'),
    ({'dataset': 'exams', 'output': {'kind': 'pie'}}, 'unknown output'),
    ({'dataset': 'exams', 'criteria': {'exam_at': {'mode': 'quick', 'quick': 'someday'}}}, 'quick'),
    ({'dataset': 'exams', 'criteria': {'exam_at': {'mode': 'fixed', 'from': 'x', 'to': 'y'}}}, 'YYYY'),
    ({'dataset': 'exams', 'criteria': {'exam_at': {'mode': 'relative', 'n': 0}}}, 'between'),
    ({'dataset': 'exams', 'criteria': {'age': {'min': 'old'}}}, 'not a number'),
    ({'dataset': 'exams', 'criteria': {'week_of_month': {'values': [9]}}}, 'not one of'),
    ({'dataset': 'exams', 'criteria': {'referring': {'op': 'matches', 'value': 'x'}}}, 'operator'),
    ({'dataset': 'exams', 'criteria': {'has_report': {'value': 'maybe'}}}, 'yes or no'),
    ('not an object', 'object'),
])
def test_compiler_refuses(defn, msg):
    with pytest.raises(WorklistError) as e:
        eng.compile_definition(defn)
    assert msg in str(e.value)


def test_values_never_reach_the_sql_text(app, two):
    evil = "x'); DROP TABLE users; --"
    defn = ex({'modality': {'values': [evil]}, 'referring': {'value': evil}})
    compiled = eng.compile_definition(defn)
    assert evil not in compiled['sql'] and 'DROP' not in compiled['sql']
    assert run(app, defn) == []
    assert two.count('users') >= 4


def test_editable_sql_inlines_values_safely(app, two):
    compiled = eng.compile_definition(ex({'referring': {'value': "O'Brien"},
                                          'modality': {'values': ['CT']}}))
    sql = eng.inline_sql(compiled)
    assert "'%O''Brien%'" in sql and "ARRAY['CT']" in sql and ':' not in sql.replace('::', '')
    assert hand(app, sql)['rows'] == []


def test_editable_sql_runs_like_the_generated_one(app, two):
    defn = ex({'modality': {'values': ['CT']},
               'exam_at': {'mode': 'relative', 'dir': 'prev', 'n': 2, 'unit': 'days'}})
    compiled = eng.compile_definition(defn)
    with app.app_context():
        a = eng.run(compiled=compiled)['rows']
        b = eng.run(hand_sql=eng.inline_sql(compiled))['rows']
    assert a == b and len(a) == 1


# ── the sandbox ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize('sql', [
    'SELECT * FROM users',
    'SELECT * FROM public.hl7_message_archive',
    'SELECT password_hash FROM public.users',
    "SELECT value FROM public.settings WHERE key = 'worklist_db_password'",
    "SELECT set_config('role', 'etl_user', false)",
    "SELECT set_config('session_authorization', 'etl_user', false)",
    "SELECT hl7_surrogate_id('study', 'X')",
    "SELECT pg_read_file('/etc/passwd')",
    'WITH x AS (DELETE FROM rpt.exams RETURNING *) SELECT * FROM x',
    'SELECT 1; DELETE FROM public.worklists',
    "SELECT * FROM dblink('dbname=etl_db', 'select 1') AS t(a int)",
])
def test_sandbox_refuses(app, two, sql):
    with pytest.raises(WorklistError):
        hand(app, sql)
    assert two.count('worklists') >= len(EXAMPLES)


@pytest.mark.parametrize('sql', ['SET ROLE etl_user', 'RESET ROLE', 'DELETE FROM rpt.exams',
                                 "INSERT INTO worklists (name) VALUES ('x')", '', 'DROP TABLE users'])
def test_not_a_select_is_refused_up_front(app, sql):
    with pytest.raises(WorklistError):
        hand(app, sql)


def test_sandbox_runs_as_the_report_role(app, pipe):
    role = pipe.scalar("SELECT value FROM settings WHERE key = 'worklist_db_role'")
    assert role.startswith('rpt_')
    assert hand(app, 'SELECT current_user, current_setting(\'transaction_read_only\')')['rows'] \
        == [[role, 'on']]


def test_hand_sql_reads_the_views_unqualified(app, two):
    rows = hand(app, 'SELECT accession FROM exams ORDER BY 1')['rows']
    assert rows == [['CT1'], ['MR1']]


def test_hand_sql_keeps_percent_and_colon_literals(app):
    assert hand(app, "SELECT '50%' AS a, '10:30' AS b, 1::int AS c")['rows'] == [['50%', '10:30', 1]]


def test_hand_sql_with_comments_and_trailing_semicolon(app, two):
    assert hand(app, "-- my worklist\nSELECT count(*) FROM rpt.exams;  ")['rows'] == [[2]]


def test_row_cap(app):
    res = hand(app, 'SELECT g FROM generate_series(1, 5000) g', cap=100)
    assert len(res['rows']) == 100 and res['truncated'] is True


def test_runaway_query_is_stopped(app, monkeypatch):
    monkeypatch.setattr(eng, 'STATEMENT_TIMEOUT_MS', 500)
    t0 = time.time()
    with pytest.raises(WorklistError) as e:
        hand(app, 'SELECT pg_sleep(5)')
    assert 'stopped' in str(e.value) and time.time() - t0 < 4


def test_options_come_from_the_data(app, two):
    with app.app_context():
        assert [o['value'] for o in eng.options('exams', 'modality')] == ['CT', 'MR']
        assert [o['value'] for o in eng.options('exams', 'weekday')] == [1, 2, 3, 4, 5, 6, 7]
        with pytest.raises(WorklistError):
            eng.options('exams', 'password_hash')


# ── the API ───────────────────────────────────────────────────────────────────

def test_anonymous_is_refused(client):
    assert client.get('/worklists/api/catalog').status_code == 401


@pytest.mark.parametrize('role,code', [('su', 200), ('administrator', 200),
                                       ('implementation', 200), ('user', 403)])
def test_role_access(client, role, code):
    assert h(client, 'get', '/worklists/api/catalog', role=role).status_code == code


def test_user_granted_the_page_gets_in(pipe, client):
    pipe.exec("INSERT INTO user_page_permissions (user_id, page_key, is_enabled) "
              "VALUES (4343, 'custom_reports', TRUE)")
    assert h(client, 'get', '/worklists/api/catalog', role='user', uid=OTHER).status_code == 200


def test_catalog(client):
    c = h(client, 'get', '/worklists/api/catalog').get_json()
    assert set(c['datasets']) == {'exams', 'orders', 'reports'}
    assert c['me']['can_write_sql'] is True
    assert any(f['id'] == 'technician' for f in c['fields'])


def save(client, body, role='implementation', uid=IMPL):
    return h(client, 'post', '/worklists/api/save', role=role, uid=uid, json=body)


NEW = {'name': 'Mine', 'folder': 'Tests', 'visibility': 'personal',
       'definition': {'dataset': 'exams', 'criteria': {'modality': {'values': ['CT']}},
                      'output': {'kind': 'list'}}}


def test_save_and_version(client):
    r = save(client, NEW).get_json()
    assert r['version'] == 1
    again = save(client, {**NEW, 'id': r['id'], 'name': 'Mine v2'}).get_json()
    assert again == {'status': 'ok', 'id': r['id'], 'version': 2}
    got = h(client, 'get', f"/worklists/api/{r['id']}", role='implementation', uid=IMPL).get_json()
    assert got['name'] == 'Mine v2' and [v['version'] for v in got['versions']] == [2, 1]
    v1 = h(client, 'get', f"/worklists/api/{r['id']}/versions/1", role='implementation',
           uid=IMPL).get_json()
    assert v1['name'] == 'Mine'


def test_save_refuses_a_broken_definition(client):
    bad = {**NEW, 'definition': {'dataset': 'exams', 'criteria': {'nope': {}}}}
    assert save(client, bad).status_code == 400


def test_save_needs_a_name(client):
    assert save(client, {**NEW, 'name': '  '}).status_code == 400


def test_personal_is_private(client):
    wid = save(client, NEW).get_json()['id']
    listed = lambda role, uid: [w['id'] for w in h(client, 'get', '/worklists/api/list', role=role,
                                                   uid=uid).get_json()]
    assert wid in listed('implementation', IMPL)
    assert wid not in listed('administrator', ADMIN)
    assert wid in listed('su', SU)
    assert h(client, 'get', f'/worklists/api/{wid}', role='administrator',
             uid=ADMIN).status_code == 404


def test_shared_is_visible_to_all(client):
    wid = save(client, {**NEW, 'visibility': 'shared'}).get_json()['id']
    assert wid in [w['id'] for w in h(client, 'get', '/worklists/api/list', role='administrator',
                                      uid=ADMIN).get_json()]


def test_a_plain_user_cannot_share_or_edit_shared(pipe, client):
    pipe.exec("INSERT INTO user_page_permissions (user_id, page_key, is_enabled) "
              "VALUES (4343, 'custom_reports', TRUE)")
    assert save(client, {**NEW, 'visibility': 'shared'}, role='user', uid=OTHER).status_code == 403
    example = pipe.scalar("SELECT id FROM worklists WHERE folder = 'Examples' LIMIT 1")
    r = save(client, {**NEW, 'id': example}, role='user', uid=OTHER)
    assert r.status_code == 403
    assert save(client, NEW, role='user', uid=OTHER).status_code == 200      # personal is fine


def test_only_it_authors_write_sql(client):
    with_sql = {**NEW, 'definition': {**NEW['definition'], 'sql': 'SELECT 1'}}
    assert save(client, with_sql, role='administrator', uid=ADMIN).status_code == 403
    assert save(client, with_sql, role='implementation', uid=IMPL).status_code == 200


def test_renaming_keeps_existing_sql_without_sql_rights(client):
    with_sql = {**NEW, 'visibility': 'shared', 'definition': {**NEW['definition'], 'sql': 'SELECT 1'}}
    wid = save(client, with_sql).get_json()['id']
    r = save(client, {**with_sql, 'id': wid, 'name': 'Renamed'}, role='administrator', uid=ADMIN)
    assert r.status_code == 200


def test_delete(client):
    wid = save(client, {**NEW, 'visibility': 'shared'}).get_json()['id']
    assert h(client, 'post', f'/worklists/api/{wid}/delete', role='user', uid=OTHER).status_code == 403
    assert h(client, 'post', f'/worklists/api/{wid}/delete', role='implementation',
             uid=IMPL).status_code == 200
    assert h(client, 'get', f'/worklists/api/{wid}').status_code == 404


def test_run_endpoint(client, two):
    r = h(client, 'post', '/worklists/api/run', json={'definition': NEW['definition']}).get_json()
    assert r['kind'] == 'list' and [row[r['columns'].index('accession')] for row in r['rows']] == ['CT1']


def test_run_endpoint_reports_a_bad_definition(client):
    r = h(client, 'post', '/worklists/api/run', json={'definition': {'dataset': 'x'}})
    assert r.status_code == 400 and 'unknown dataset' in r.get_json()['error']


def test_run_endpoint_with_hand_sql(client, two):
    r = h(client, 'post', '/worklists/api/run',
          json={'definition': {**NEW['definition'], 'sql': 'SELECT count(*) AS n FROM exams'}})
    assert r.get_json()['rows'] == [[2]]


def test_run_endpoint_refuses_escape_attempts(client):
    r = h(client, 'post', '/worklists/api/run',
          json={'definition': {**NEW['definition'], 'sql': 'SELECT * FROM public.users'}})
    assert r.status_code == 400 and 'permission denied' in r.get_json()['error']


def test_compile_endpoint(client):
    r = h(client, 'post', '/worklists/api/compile',
          json={'definition': {'dataset': 'exams', 'criteria': {'referring': {'value': 'wakim'}},
                               'output': {'kind': 'list'}}}).get_json()
    assert ':referring' in r['sql'] and 'wakim' not in r['sql']
    assert r['params'] == {'referring': '%wakim%'} and "'%wakim%'" in r['editable_sql']


def test_csv_export(client, two):
    r = h(client, 'post', '/worklists/api/csv', json={'definition': ex({}), 'name': 'My list'})
    assert r.status_code == 200 and r.mimetype == 'text/csv'
    assert 'My list.csv' in r.headers['Content-Disposition']
    rows = list(csv.reader(io.StringIO(r.data.decode('utf-8-sig'))))
    assert rows[0] == ['accession', 'exam_at'] and len(rows) == 3


def test_options_endpoint(client, two):
    r = h(client, 'get', '/worklists/api/options?dataset=exams&field=modality').get_json()
    assert [o['value'] for o in r] == ['CT', 'MR']


@pytest.mark.parametrize('role,code', [('implementation', 200), ('administrator', 200),
                                       ('su', 200)])
def test_shift_hours_editable_by_editors(client, role, code):
    r = h(client, 'post', '/worklists/api/shifts', role=role,
          json={'shifts': [{'name': 'DAY', 'start_hour': 8, 'end_hour': 20},
                           {'name': 'NIGHT', 'start_hour': 20, 'end_hour': 8}]})
    assert r.status_code == code and [s['name'] for s in r.get_json()] == ['DAY', 'NIGHT']


def test_shift_hours_not_for_plain_users(pipe, client):
    pipe.exec("INSERT INTO user_page_permissions (user_id, page_key, is_enabled) "
              "VALUES (4343, 'custom_reports', TRUE)")
    assert h(client, 'post', '/worklists/api/shifts', role='user', uid=OTHER,
             json={'shifts': [{'name': 'X', 'start_hour': 0, 'end_hour': 24}]}).status_code == 403


@pytest.mark.parametrize('shifts', [[], [{'name': 'X'}], [{'name': 'X', 'start_hour': 25, 'end_hour': 3}],
                                    [{'name': 'A', 'start_hour': 1, 'end_hour': 2},
                                     {'name': 'A', 'start_hour': 3, 'end_hour': 4}]])
def test_shift_hours_validated(client, shifts):
    assert h(client, 'post', '/worklists/api/shifts', json={'shifts': shifts}).status_code == 400
    assert len(h(client, 'get', '/worklists/api/shifts').get_json()) == 3


@pytest.mark.parametrize('url', ['/reports/custom', '/reports/custom/5/edit', '/reports/custom/new'])
def test_old_custom_reports_addresses_redirect(client, url):
    r = h(client, 'get', url)
    assert r.status_code == 302 and r.headers['Location'].endswith('/worklists')


def test_a_hundred_studies_aggregate_quickly(app, pipe):
    for i in range(100):
        pipe.send(k.status('CM', f'V{i:03d}', k.ts(60 + i), pat=f'P{i:03d}'))
    t0 = time.time()
    rows = run(app, {'dataset': 'exams', 'criteria': {'exam_at': {'mode': 'quick',
                                                                  'quick': 'last_7_days'}},
                     'output': {'kind': 'table', 'rows': 'modality', 'measures': ['count']}})
    assert rows == [{'row': 'CT', 'count': 100}] and time.time() - t0 < 3


# ── the page: Save at the bottom, a new worklist empty (operator, 2026-10-25) ──

def test_page_has_save_in_the_bottom_bar_only(client):
    body = h(client, 'get', '/worklists').data.decode('utf-8')
    bar = body.split('id="actionbar"', 1)[1].split('</div>', 1)[0]
    assert 'id="btn-save"' in bar and 'id="btn-run2"' in bar
    assert body.count('id="btn-save"') == 1
    assert 'position: sticky; bottom: 0' in body


def test_page_marks_the_required_fields(client):
    body = h(client, 'get', '/worklists').data.decode('utf-8')
    for wrapper in ('id="fw-name"', 'id="fw-ds"'):
        assert wrapper in body
    assert body.count('class="req-tag">Required') >= 2


def test_generated_sql_is_read_only_and_writing_is_a_separate_mode(client):
    body = h(client, 'get', '/worklists').data.decode('utf-8')
    assert '<pre class="sql" id="sql-view">' in body           # shown, never editable
    assert 'data-mode="gen"' in body and 'data-mode="hand"' in body
    assert 'btn-edit-sql' not in body and 'id="btn-ref"' in body


# ── the SQL reference: what a worklist written in SQL may read ────────────────

def test_every_view_column_is_documented(pipe):
    """A column added to an rpt view without a description fails here rather than
    reaching an engineer undocumented ("a very perfect documentation ... or disable
    it", operator 2026-10-25). Stale descriptions of removed columns fail too."""
    for key, ds in cat.DATASETS.items():
        view = ds['view'].split('.', 1)[1]
        cols = [r['column_name'] for r in pipe.q(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'rpt' AND table_name = :t ORDER BY ordinal_position", t=view)]
        docs = cat.COLUMN_DOCS[key]
        assert cols and set(cols) == set(docs), (view, sorted(set(cols) ^ set(docs)))
        assert all(len(docs[c].strip()) > 10 for c in cols), view


@pytest.mark.parametrize('i', range(len(cat.SQL_EXAMPLES)))
def test_every_example_runs_in_the_sandbox(app, two, i):
    res = hand(app, cat.SQL_EXAMPLES[i]['sql'])
    assert res['columns']


def test_sql_reference_endpoint(client):
    r = h(client, 'get', '/worklists/api/sql-reference').get_json()
    assert [v['view'] for v in r['views']] == ['rpt.exams', 'rpt.orders', 'rpt.reports']
    for v in r['views']:
        assert v['columns'] and all(c['type'] and c['about'] for c in v['columns'])
    exams = {c['name']: c for c in r['views'][0]['columns']}
    assert exams['exam_at']['type'] == 'date & time' and exams['stalled']['type'] == 'true / false'
    titles = [x['title'] for x in r['rules']]
    assert {'What you can read', 'One query', 'Values', 'Dates and times', 'Limits'} <= set(titles)
    limits = next(x['text'] for x in r['rules'] if x['title'] == 'Limits')
    assert f'{eng.ROW_CAP:,}' in limits and f'{eng.CSV_CAP:,}' in limits
    assert len(r['examples']) == len(cat.SQL_EXAMPLES)


def test_sql_reference_needs_page_access(client):
    assert client.get('/worklists/api/sql-reference').status_code == 401
    assert h(client, 'get', '/worklists/api/sql-reference', role='user', uid=OTHER).status_code == 403


SQL_ONLY = {'name': 'Written in SQL', 'folder': 'Tests', 'visibility': 'personal',
            'definition': {'dataset': None, 'criteria': {}, 'ask': [],
                           'output': {'kind': None}, 'sql': 'SELECT count(*) AS n FROM exams'}}


def test_a_worklist_written_in_sql_needs_no_dataset(pipe, client):
    r = save(client, SQL_ONLY)
    assert r.status_code == 200
    wid = r.get_json()['id']
    assert pipe.scalar("SELECT dataset FROM worklists WHERE id = :i", i=wid) == 'sql'
    got = h(client, 'get', f'/worklists/api/{wid}', role='implementation', uid=IMPL).get_json()
    assert got['definition']['sql'] == SQL_ONLY['definition']['sql']


def test_a_worklist_written_in_sql_is_still_for_it_authors_only(client):
    assert save(client, SQL_ONLY, role='administrator', uid=ADMIN).status_code == 403


def test_run_and_csv_of_sql_without_a_definition(client, two):
    r = h(client, 'post', '/worklists/api/run', json={'definition': SQL_ONLY['definition']})
    assert r.status_code == 200 and r.get_json()['rows'] == [[2]]
    c = h(client, 'post', '/worklists/api/csv', json={'definition': SQL_ONLY['definition'], 'name': 'x'})
    assert c.status_code == 200 and c.data.decode('utf-8-sig').splitlines() == ['n', '2']


def test_a_built_worklist_still_needs_its_dataset(client):
    empty = {**NEW, 'definition': {'dataset': None, 'criteria': {}, 'output': {'kind': 'list'}}}
    r = save(client, empty)
    assert r.status_code == 400 and 'dataset' in r.get_json()['error']


def test_empty_sql_is_refused(client):
    bad = {**SQL_ONLY, 'definition': {**SQL_ONLY['definition'], 'sql': '   '}}
    assert save(client, bad).status_code == 400


def test_confirmations_use_sweetalert2(client):
    """Every question the editor asks goes through SweetAlert2, like the rest of RAYD
    (operator, 2026-10-25). The browser's own confirm() stays only as the fallback
    for an install that cannot load the library."""
    body = h(client, 'get', '/worklists').data.decode('utf-8')
    assert 'sweetalert2@11' in body and 'Swal.fire' in body
    # The whole page, shared header and sidebar included: a fallback is written
    # window.confirm(...), so a bare confirm( / alert( / prompt( is a native dialog.
    scripts = ''.join(re.findall(r'<script\b[^>]*>(.*?)</script>', body, re.S))
    natives = re.findall(r'(?<![.\w])(confirm|alert|prompt)\(', scripts)
    assert natives == [], natives
