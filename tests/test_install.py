"""
A fresh install: schema.sql plus every migration, the way docker-entrypoint and
run_migrations() build it on a new server.

run_migrations() logs a failed file and carries on, so a broken migration does not
stop the app booting — it just leaves the schema short, silently. These tests are
the place that failure becomes loud.
"""
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS = sorted(f for f in os.listdir(os.path.join(ROOT, 'migrations')) if f.endswith('.sql'))


def test_every_migration_applies_on_a_fresh_database(pipe):
    applied = {r['name'] for r in pipe.q("SELECT name FROM schema_migrations")}
    missing = [f for f in MIGRATIONS if f not in applied]
    assert missing == [], f"migrations that failed on a fresh install: {missing}"


@pytest.mark.parametrize('table', [
    'hl7_message_archive', 'hl7_surrogate_keys', 'hl7_status_map', 'hl7_study_events',
    'hl7_patients', 'ray7_rules', 'ray7_findings', 'ray7_study_state',
    'hl7_field_mappings', 'hl7_field_targets', 'ray7_ladder_profile',
    'hl7_result_status_map', 'hl7_orders', 'hl7_oru_reports', 'hl7_scn_studies',
    'etl_didb_studies', 'etl_patient_view', 'etl_orders', 'std_worklist_arrivals',
    'std_worklist_exam_done', 'std_worklist_scheduled', 'std_pps',
    'std_pps_person_reference', 'std_resources_ris', 'aetitle_modality_map',
    'settings', 'cd_burn_log', 'users',
])
def test_table_exists(pipe, table):
    assert pipe.scalar("SELECT to_regclass(:t) IS NOT NULL", t=f'public.{table}')


EXPECTED_RULES = [
    'EXACT_REDELIVERY', 'CONTROL_ID_REUSE', 'LOGICAL_DUPLICATE', 'REPEATED_TRANSITION',
    'LADDER_REGRESSION', 'SKIPPED_RUNG', 'TIME_CONTRADICTION', 'FUTURE_EVENT',
    'ORPHAN_EVENT', 'UNKNOWN_STATUS_CODE', 'MISSING_IDENTIFIER', 'UNKNOWN_PROCEDURE',
    'UNKNOWN_AETITLE', 'UNKNOWN_PERFORMER', 'PLACEHOLDER_DATA', 'STALLED_SCHEDULED',
    'STALLED_ARRIVED', 'STALLED_STARTED', 'UNREPORTED', 'ORPHAN_ORDER',
    'RAY7_BUDGET_EXCEEDED', 'RAY7_DEGRADED', 'OUT_OF_SEQUENCE_DELIVERY',
    'UNEXPECTED_RUNG', 'UNKNOWN_RESULT_STATUS', 'STALLED_UNSIGNED',
]


@pytest.mark.parametrize('code', EXPECTED_RULES)
def test_rule_is_seeded(pipe, code):
    assert pipe.count('ray7_rules', "rule_code = :c AND modality = ''", c=code) == 1


@pytest.mark.parametrize('code,state,rank', [
    ('SC', 'scheduled', 40), ('AR', 'arrived', 60), ('IP', 'started', 70),
    ('CM', 'completed', 100), ('CA', 'cancelled', -1), ('DC', 'cancelled', -1),
    ('ER', 'cancelled', -1),
])
def test_status_map_seed(pipe, code, state, rank):
    row = pipe.one("SELECT canonical_state, ladder_rank FROM hl7_status_map "
                   "WHERE sending_app = '' AND order_control = '' AND order_status = :c", c=code)
    assert row == {'canonical_state': state, 'ladder_rank': rank}


@pytest.mark.parametrize('code,state,rank', [
    ('W', 'signed_prelim', 120), ('P', 'signed_prelim', 120),
    ('F', 'signed_final', 140), ('C', 'signed_final', 140),
])
def test_result_status_map_seed(pipe, code, state, rank):
    row = pipe.one("SELECT canonical_state, ladder_rank FROM hl7_result_status_map "
                   "WHERE sending_app = '' AND result_status = :c", c=code)
    assert row == {'canonical_state': state, 'ladder_rank': rank}


def test_ladder_profile_ships_entirely_off(pipe):
    rows = pipe.q("SELECT rung, ladder_rank, expected, enforce_order FROM ray7_ladder_profile "
                  "ORDER BY ladder_rank")
    assert [(r['rung'], r['ladder_rank']) for r in rows] == [
        ('ordered', 20), ('scheduled', 40), ('arrived', 60), ('started', 70),
        ('completed', 100), ('signed_prelim', 120), ('signed_final', 140)]
    assert not any(r['expected'] or r['enforce_order'] for r in rows)


def test_install_seeds_no_users(pipe):
    """a7ec82bf stopped seeding default users; a fresh install must have none."""
    assert pipe.count('users') == 0


# ── the surrogate-key function every etl_* primary key comes from ─────────────

def test_surrogate_is_stable(pipe):
    a = pipe.scalar("SELECT hl7_surrogate_id('study', 'ACC1')")
    b = pipe.scalar("SELECT hl7_surrogate_id('study', 'ACC1')")
    assert a == b and a >= 9_000_000_000


def test_surrogate_trims_the_natural_key(pipe):
    assert (pipe.scalar("SELECT hl7_surrogate_id('study', '  ACC1 ')")
            == pipe.scalar("SELECT hl7_surrogate_id('study', 'ACC1')"))


@pytest.mark.parametrize('blank', [None, '', '   '])
def test_surrogate_of_blank_is_null(pipe, blank):
    assert pipe.scalar("SELECT hl7_surrogate_id('study', :k)", k=blank) is None


def test_surrogate_is_unique_across_entities(pipe):
    ids = [pipe.scalar("SELECT hl7_surrogate_id(:e, 'SAME')", e=e)
           for e in ('study', 'patient', 'order', 'worklist', 'pps', 'staff', 'pps_ref')]
    assert len(set(ids)) == len(ids)


def test_surrogate_rejects_unknown_entity(pipe):
    from sqlalchemy.exc import IntegrityError
    with pytest.raises(IntegrityError):
        pipe.scalar("SELECT hl7_surrogate_id('nonsense', 'X')")
