"""
tests/test_report_composer_name.py — the report editor is called Report Composer.

Philips uses its earlier name (operator, 2026-11-04), so that name must appear nowhere.
This code moves between the HL7, Bekaa and LAUMC branches, so a port could bring it back:
these tests fail if it does. Renaming a migration file is also not free, because the
database knows a migration by its file name, so they check that it does not run twice.
"""
import os
import re

from sqlalchemy import text

import db_migrations
from conftest import ROOT

# Neither of these contains the name, so this file does not trip its own check.
EARLIER_NAME = re.compile(r'dynamic[ _-]?worklist', re.I)
OLD_FILE = '0136_' + 'dynamic' + '_worklists.sql'
NEW_FILE = '0136_report_composer.sql'
TEXT_FILES = ('.py', '.html', '.js', '.css', '.sql', '.md', '.sh', '.txt', '.yml', '.yaml', '.json', '.conf')
NOT_OURS = {'.git', '__pycache__', 'node_modules', 'venv', '.venv'}


def test_the_earlier_name_is_in_no_file_and_no_file_name():
    found = []
    for base, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in NOT_OURS]
        for name in dirs + files:
            if EARLIER_NAME.search(name):
                found.append(os.path.relpath(os.path.join(base, name), ROOT))
        for name in files:
            if name.lower().endswith(TEXT_FILES):
                with open(os.path.join(base, name), encoding='utf-8', errors='ignore') as f:
                    if EARLIER_NAME.search(f.read()):
                        found.append(os.path.relpath(os.path.join(base, name), ROOT))
    assert found == []


def _scalar(db, sql, **params):
    return db.session.execute(text(sql), params).scalar()


def test_the_renamed_migration_does_not_run_again(app):
    """A database that ran it under the old name keeps it as run. It rotates the reporting
    role's password each time it runs, so an unchanged password proves it did not."""
    from db import db
    with app.app_context():
        password = _scalar(db, "SELECT value FROM settings WHERE key = 'worklist_db_password'")
        db.session.execute(text("UPDATE schema_migrations SET name = :old WHERE name = :new"),
                           {'old': OLD_FILE, 'new': NEW_FILE})
        db.session.commit()

    db_migrations.run_migrations(app)

    with app.app_context():
        names = set(db.session.execute(text("SELECT name FROM schema_migrations")).scalars())
        assert NEW_FILE in names and OLD_FILE not in names
        assert _scalar(db, "SELECT value FROM settings WHERE key = 'worklist_db_password'") == password


def test_a_fresh_install_never_stores_the_earlier_name(app):
    from db import db
    with app.app_context():
        rows = db.session.execute(text("SELECT section, keywords, content FROM ai_doc_chunks")).fetchall()
    assert not [r for r in rows if any(EARLIER_NAME.search(v or '') for v in r)]


def test_a_database_that_has_it_is_cleaned_including_an_operators_own_wording(app):
    """What migration 0139 finds on a database that ran 0136 before it was reworded."""
    from db import db
    old = 'Dynamic' + ' Worklists'
    seeded = (f'{old} (sidebar: Analytics) replaced Custom Reports. Admins can share worklists '
              'and write their SQL by hand.')
    with app.app_context():
        # All in one transaction, rolled back: nothing resets this table between tests.
        try:
            db.session.execute(text("DELETE FROM ai_doc_chunks WHERE section = 'Report Composer'"))
            db.session.execute(text("""
                INSERT INTO ai_doc_chunks (section, keywords, content, updated_at) VALUES
                  (:s, :k, :seeded, NOW()),
                  ('Operator notes', 'mine', :own, NOW())
            """), {'s': old, 'k': f'worklist,{old.lower()},custom report', 'seeded': seeded,
                   'own': f'Ask the PACS admin before editing {old}.'})

            with open(os.path.join(db_migrations.MIGRATIONS_DIR, '0139_report_composer_name.sql')) as f:
                for statement in db_migrations._split_sql(f.read()):
                    db.session.execute(text(statement))

            rows = {r[0]: r for r in db.session.execute(text(
                "SELECT section, keywords, content FROM ai_doc_chunks")).fetchall()}
            assert not [r for r in rows.values() if any(EARLIER_NAME.search(v or '') for v in r)]
            assert rows['Report Composer'][1] == 'report composer,scheduled email,worklist,custom report'
            assert rows['Report Composer'][2].startswith('Report Composer (sidebar: Analytics)')
            assert rows['Report Composer'][2].endswith('email saved reports on a schedule.')
            assert rows['Operator notes'][1] == 'mine'
            assert rows['Operator notes'][2] == 'Ask the PACS admin before editing Report Composer.'
        finally:
            db.session.rollback()
