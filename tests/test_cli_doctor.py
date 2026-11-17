from __future__ import annotations

import json
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

from cao_control_plane import cli
from cao_control_plane.cli import _doctor


def _diagnostic_snapshot(settings, counts):
    # A reduced historical schema suffices for these status-count cases.
    # Doctor must observe it without initializing or upgrading the source.
    with closing(sqlite3.connect(settings.database_path)) as connection, connection:
        connection.executescript(
            "CREATE TABLE effect_operations(status TEXT);"
            "CREATE TABLE message_deliveries(state TEXT);"
            "CREATE TABLE a2a_push_deliveries(state TEXT);"
            "CREATE TABLE principals(id TEXT);"
            "CREATE TABLE runtime_sessions(id TEXT);"
        )
        for table, column, state, key in (
            ("effect_operations", "status", "started", "started_effects"),
            ("effect_operations", "status", "unknown", "unknown_effects"),
            ("message_deliveries", "state", "dispatched", "unknown_deliveries"),
            ("message_deliveries", "state", "dead", "dead_deliveries"),
            ("a2a_push_deliveries", "state", "dead", "dead_pushes"),
        ):
            connection.executemany(
                f"INSERT INTO {table}({column}) VALUES(?)", [(state,)] * counts.get(key, 0)
            )
    settings.database_path.chmod(0o600)
    return _doctor(settings)


def test_doctor_blocks_unknown_outcomes_and_degrades_terminal_history(settings) -> None:
    result = _diagnostic_snapshot(
        settings,
        {
            "started_effects": 1,
            "unknown_effects": 2,
            "unknown_deliveries": 3,
            "dead_deliveries": 4,
            "dead_pushes": 5,
        },
    )

    assert result["ok"] is False
    assert result["degraded"] is True
    assert result["blocking_conditions"] == [
        "unknown_effect_outcome",
        "unknown_delivery_outcome",
        "dead_push_delivery",
    ]
    assert result["degraded_conditions"] == [
        "effect_in_progress",
        "dead_delivery_history",
    ]
    assert result["unresolved_effect_operations"] == 3


def test_doctor_reports_expected_dead_delivery_history_without_blocking(settings) -> None:
    result = _diagnostic_snapshot(settings, {"dead_deliveries": 2})

    assert result["ok"] is True
    assert result["degraded"] is True
    assert result["blocking_conditions"] == []
    assert result["degraded_conditions"] == ["dead_delivery_history"]


def _doctor_args(tmp_path: Path, settings):
    config = tmp_path / "doctor.toml"
    config.write_text(
        f'[server]\nstate_dir = "{settings.state_dir}"\n'
        f'[runtime]\nruntime_launch_dir = "{settings.runtime_launch_dir}"\n',
        encoding="utf-8",
    )
    return ["--config", str(config), "doctor"]


def test_doctor_observes_committed_state_while_a_writer_is_active(system, tmp_path, capsys):
    service = system["service"]
    settings = system["settings"]
    args = _doctor_args(tmp_path, settings)
    service.db.execute("UPDATE runtime_sessions SET lease_expires_at='2000-01-01T00:00:00Z'")
    before = service.db.fetchone("SELECT MAX(sequence) AS n FROM events")["n"]
    runtime_before = service.db.fetchall("SELECT id,state,lease_expires_at FROM runtime_sessions")
    with closing(service.db.connect()) as writer:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE metadata SET value='uncommitted' WHERE key='schema_version'")
        try:
            assert cli.main(args) == 0
            result = json.loads(capsys.readouterr().out)
            assert result["ok"] is True
            assert result["runtimes"] == len(runtime_before)
            assert writer.in_transaction
        finally:
            writer.rollback()
    assert service.db.fetchone("SELECT MAX(sequence) AS n FROM events")["n"] == before
    assert [tuple(row) for row in service.db.fetchall(
        "SELECT id,state,lease_expires_at FROM runtime_sessions"
    )] == [tuple(row) for row in runtime_before]


def test_doctor_diagnoses_permissions_without_repairing_them(system, tmp_path, capsys):
    settings = system["settings"]
    args = _doctor_args(tmp_path, settings)
    settings.state_dir.chmod(0o755)
    settings.database_path.chmod(0o644)
    assert cli.main(args) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is False
    assert result["permissions"][str(settings.state_dir)]["ok"] is False
    assert result["permissions"][str(settings.database_path)]["ok"] is False
    assert stat.S_IMODE(settings.state_dir.stat().st_mode) == 0o755
    assert stat.S_IMODE(settings.database_path.stat().st_mode) == 0o644


def test_doctor_does_not_initialize_missing_state(settings, tmp_path, capsys):
    from dataclasses import replace

    missing = replace(settings, state_dir=tmp_path / "missing", runtime_launch_dir=tmp_path / "missing-launch")
    assert cli.main(_doctor_args(tmp_path, missing)) == 1
    assert not missing.state_dir.exists()
    assert not missing.runtime_launch_dir.exists()
    assert capsys.readouterr().err
