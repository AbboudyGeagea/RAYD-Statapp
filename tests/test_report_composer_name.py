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
