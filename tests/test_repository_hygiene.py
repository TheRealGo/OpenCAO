from __future__ import annotations

import runpy
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "source, rejected",
    [
        ("import sqlite3\nwith sqlite3.connect('test.db') as connection: pass", True),
        ("import sqlite3 as sql\nwith sql.connect('test.db') as connection: pass", True),
        ("from sqlite3 import connect as db\nwith db('test.db') as connection: pass", True),
        (
            "import sqlite3\nfrom contextlib import closing\n"
            "with closing(sqlite3.connect('test.db')) as connection, connection: pass",
            False,
        ),
        ("with database.connection_scope() as connection: pass", False),
        ("import sqlite3\nwith connection: pass", False),
    ],
)
def test_sqlite_ownership_guard_recognizes_transaction_only_contexts(
    tmp_path: Path, source: str, rejected: bool
) -> None:
    check = runpy.run_path(
        str(Path(__file__).resolve().parents[1] / "tools" / "check_repository_hygiene.py")
    )["_sqlite_resource_errors"]
    fixture = tmp_path / "fixture.py"
    fixture.write_text(source, encoding="utf-8")
    errors = check([fixture])
    assert bool(errors) is rejected
    if rejected:
        assert "does not close the connection" in errors[0]


def test_repository_has_no_unreachable_or_orphaned_surfaces() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / "tools" / "check_repository_hygiene.py")],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
