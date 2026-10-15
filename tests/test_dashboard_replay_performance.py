from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from test_dashboard_reading import _assign, _report

from cao_control_plane import dashboard, projection
from cao_control_plane.dashboard import DashboardCursor, DashboardReadModel, DashboardResyncRequired
from cao_control_plane.database import SCHEMA_VERSION, Database


def _event(connection, identity, scope):
    connection.execute(
        """INSERT INTO events(
               id, event_type, aggregate_type, aggregate_id, operator_scope, data_json, created_at
           ) VALUES (?, 'work.changed', 'work_item', 'private-reference', ?, ?, ?)""",
        (identity, scope, '{"private": "must not be loaded"}', "2026-01-01T00:00:00Z"),
    )


def test_replay_reads_only_bounded_public_envelopes_and_skips_hidden_history(system, monkeypatch):
    database = system["service"].db
    model = DashboardReadModel(system["service"])
    cursor = model.snapshot()["cursor"]
    with database.transaction() as connection:
        for index in range(5000):
            _event(connection, f"hidden-{index}", "system")
        _event(connection, "visible-first", "production")
        first_visible = connection.execute("SELECT MAX(sequence) FROM events").fetchone()[0]
        _event(connection, "visible-second", "production")
        _event(connection, "hidden-tail", "acceptance-test")
        last_sequence = connection.execute("SELECT MAX(sequence) FROM events").fetchone()[0]

    def forbidden(*args, **kwargs):
        pytest.fail("Replay reconstructed Work or ran a full projection audit")

    monkeypatch.setattr(dashboard, "build_projection", forbidden)
    monkeypatch.setattr(system["service"], "list_events", forbidden)
    original_connect = database.connect
    steps = 0
    columns = {
        "control_authority": {"singleton", "mode", "generation"},
        "events": {"", "sequence", "event_type", "aggregate_type", "created_at", "operator_scope"},
    }

    def authorize(action, table, column, *_):
        if action == sqlite3.SQLITE_READ and column not in columns.get(table, set()):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def progress():
        nonlocal steps
        steps += 100
        return int(steps > 2000)

    def connect():
        connection = original_connect()
        connection.set_authorizer(authorize)
        connection.set_progress_handler(progress, 100)
        return connection

    monkeypatch.setattr(database, "connect", connect)
    first = model.history(after=cursor, limit=1)
    assert isinstance(first, dict)
    assert len(first["items"]) == 1
    assert first["has_more"] is True
    assert DashboardCursor.decode(first["next_cursor"]).sequence == first_visible
    second = model.history(after=first["next_cursor"], limit=1)
    assert isinstance(second, dict)
    assert len(second["items"]) == 1
    assert second["items"][0]["cursor"] != first["items"][0]["cursor"]
    assert second["has_more"] is False
    assert DashboardCursor.decode(second["next_cursor"]).sequence == last_sequence
    quiet = model.history(after=second["next_cursor"])
    assert isinstance(quiet, dict)
    assert quiet["items"] == []
    assert quiet["next_cursor"] == second["next_cursor"]


def test_replay_generation_bounds_and_events_share_one_read_snapshot(system, monkeypatch):
    database = system["service"].db
    model = DashboardReadModel(system["service"])
    before = model.snapshot()["cursor"]
    original_connect = database.connect
    committed = False

    def trace(sql):
        nonlocal committed
        if "AS first_sequence" in sql and not committed:
            committed = True
            with closing(original_connect()) as writer, writer:
                writer.execute("BEGIN")
                writer.execute(
                    "UPDATE control_authority SET generation = generation + 1 WHERE singleton = 1"
                )
                _event(writer, "concurrent-visible", "production")

    def connect():
        connection = original_connect()
        connection.set_trace_callback(trace)
        return connection

    monkeypatch.setattr(database, "connect", connect)
    first = model.history(after=before)
    assert committed
    assert isinstance(first, dict)
    assert first["items"] == []
    assert first["next_cursor"] == before
    second = model.history(after=before)
    assert isinstance(second, DashboardResyncRequired)
    assert second.reason == "authority-generation-changed"
    assert second.authority_generation == DashboardCursor.decode(before).authority_generation + 1
    assert DashboardCursor.decode(second.cursor).sequence > DashboardCursor.decode(before).sequence


def test_attempt_activity_does_not_scan_unrelated_message_history(system):
    work = _assign(system)
    _report(system, work, "Current progress")
    database = system["service"].db
    with database.transaction() as connection:
        connection.executemany(
            """INSERT INTO messages(
                   id, sender_id, kind, payload_json, payload_digest, message_digest, created_at
               ) VALUES (?, ?, 'system', '{}', 'digest', 'digest', '2026-01-01T00:00:00Z')""",
            [(f"unrelated-{index}", system["cao"]["id"]) for index in range(5000)],
        )
    steps = 0

    def progress():
        nonlocal steps
        steps += 100
        return int(steps > 2000)

    with database.connection_scope() as connection:
        connection.set_progress_handler(progress, 100)
        activity = projection._operator_attempt_activity_projection(
            connection,
            attempt_id=work["current_attempt"]["id"],
            worker_id=system["worker"]["id"],
            as_of="2026-01-01T00:00:00Z",
        )
    assert activity["last_worker_activity_at"] is not None
    assert activity["status_request_state"] is None


def test_schema45_upgrade_installs_read_indexes_without_changing_projection(system):
    work = _assign(system)
    _report(system, work, "Retained progress")
    database = system["service"].db
    indexes = {
        "messages_attempt_kind_sequence_idx", "messages_attempt_sender_sequence_idx",
        "messages_causation_kind_idx", "artifacts_attempt_created_idx",
        "events_operator_scope_sequence_idx",
    }
    as_of = "2026-01-01T00:00:00Z"
    before = projection.build_projection(database, as_of=as_of)
    with database.transaction() as connection:
        for index in indexes:
            connection.execute(f"DROP INDEX {index}")
        connection.execute("UPDATE metadata SET value = '45' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 45")
        connection.execute("DELETE FROM schema_migrations WHERE version > 45")
    migrated = Database(system["settings"])
    installed = {
        row["name"] for row in migrated.fetchall("SELECT name FROM sqlite_master WHERE type='index'")
    }
    assert indexes <= installed
    assert migrated.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    after = projection.build_projection(migrated, as_of=as_of)
    assert after.as_dict() == before.as_dict()
    reopened = Database(system["settings"])
    assert projection.build_projection(reopened, as_of=as_of).as_dict() == after.as_dict()
