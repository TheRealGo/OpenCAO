from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from cao_control_plane import cli
from cao_control_plane import database as database_module
from cao_control_plane.config import Settings
from cao_control_plane.database import (
    APPLICATION_ID,
    Database,
    backup_sqlite_database,
    inspect_sqlite_database,
)


def _schema_28_database(settings: Settings) -> Path:
    database = Database(settings)
    with database.transaction() as connection:
        connection.execute("DROP INDEX IF EXISTS runtime_enrollment_tickets_attempt_generation_idx")
        connection.execute("ALTER TABLE runtime_enrollment_tickets DROP COLUMN attempt_id")
        connection.execute("DELETE FROM schema_migrations WHERE version = 29")
        connection.execute("UPDATE metadata SET value = '28' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 28")
    return settings.database_path


def test_backup_cli_never_initializes_or_migrates_the_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_dir = tmp_path / "state"
    settings = Settings(state_dir=state_dir)
    source = _schema_28_database(settings)
    source_before = source.read_bytes()
    source_inode = source.stat().st_ino
    config = tmp_path / "config.toml"
    config.write_text(f'[server]\nstate_dir = "{state_dir}"\n', encoding="utf-8")
    destination = tmp_path / "backup" / "before-upgrade.sqlite3"

    class ForbiddenDatabase:
        def __init__(self, _settings: Settings) -> None:
            raise AssertionError("backup must not initialize Database")

    monkeypatch.setattr(cli, "Database", ForbiddenDatabase)
    args = cli.build_parser().parse_args(["--config", str(config), "backup", str(destination)])

    assert cli.run(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == 28
    assert len(result["sha256"]) == 64
    assert source.stat().st_ino == source_inode
    assert source.read_bytes() == source_before
    assert inspect_sqlite_database(source).user_version == 28
    assert inspect_sqlite_database(destination).user_version == 28
    with closing(sqlite3.connect(destination)) as connection:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(runtime_enrollment_tickets)")
        }
    assert "attempt_id" not in columns


def test_online_backup_includes_commits_but_excludes_an_open_transaction(
    settings: Settings,
    tmp_path: Path,
) -> None:
    database = Database(settings)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES('backup_committed_probe', 'yes')"
        )

    writer = sqlite3.connect(settings.database_path)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO metadata(key, value) VALUES('backup_uncommitted_probe', 'no')")
        destination = tmp_path / "online.sqlite3"
        backup = backup_sqlite_database(settings.database_path, destination)
    finally:
        writer.rollback()
        writer.close()

    assert backup.source_identity == backup.backup_identity
    assert backup.backup_identity.application_id == APPLICATION_ID
    with closing(sqlite3.connect(destination)) as connection:
        values = dict(
            connection.execute(
                "SELECT key, value FROM metadata WHERE key LIKE 'backup_%_probe' ORDER BY key"
            )
        )
    assert values == {"backup_committed_probe": "yes"}


def test_backup_rejects_untrusted_identity_and_existing_destination(
    tmp_path: Path,
) -> None:
    source = tmp_path / "foreign.sqlite3"
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO metadata(key, value) VALUES('schema_version', '29')")
    source.chmod(0o600)
    destination = tmp_path / "backup.sqlite3"

    with pytest.raises(ValueError, match="healthy CAO"):
        backup_sqlite_database(source, destination)

    settings = Settings(state_dir=tmp_path / "state")
    Database(settings)
    destination.write_bytes(b"preserve")
    destination.chmod(0o600)
    with pytest.raises(FileExistsError, match="already exists"):
        backup_sqlite_database(
            settings.database_path,
            destination,
            replace=False,
        )
    assert destination.read_bytes() == b"preserve"


def test_no_replace_backup_does_not_clobber_a_racing_destination(
    settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    Database(settings)
    destination = tmp_path / "racing.sqlite3"
    original_link = database_module.os.link

    def racing_link(
        source: Path,
        target: Path,
        *,
        follow_symlinks: bool = True,
    ) -> None:
        target.write_bytes(b"concurrent-owner-file")
        target.chmod(0o600)
        original_link(source, target, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(database_module.os, "link", racing_link)

    with pytest.raises(FileExistsError):
        backup_sqlite_database(
            settings.database_path,
            destination,
            replace=False,
        )
    assert destination.read_bytes() == b"concurrent-owner-file"
