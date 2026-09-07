from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from cao_control_plane.database import SCHEMA_VERSION, Database
from cao_control_plane.models import (
    AckInput,
    GoalRevision,
    MessageKind,
    ReportInput,
    WorkAssignment,
)
from cao_control_plane.service import ControlPlane


def test_persistence_across_service_restart(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Persist",
            objective="Survive restart",
            acceptance=["Still readable"],
        ),
    )
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    assert restarted.get_work(work["id"])["objective"] == "Survive restart"
    assert restarted.authenticate(system["worker_token"])["id"] == system["worker"]["id"]


def test_message_ack_is_durable_and_ordered(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Inbox",
            objective="Check order",
            acceptance=["Ordered"],
        ),
    )
    service.send_message(
        system["cao"],
        [system["worker"]["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"message": "Second"},
        work_item_id=work["id"],
        attempt_id=work["current_attempt"]["id"],
        goal_version=work["goal_version"],
    )
    inbox = service.get_inbox(system["worker"])
    assert [item["sequence"] for item in inbox["items"]] == sorted(
        item["sequence"] for item in inbox["items"]
    )
    first = inbox["items"][0]
    service.acknowledge(system["worker"], AckInput(message_ids=[first["id"]]))
    unread = service.get_inbox(system["worker"])
    assert first["id"] not in {item["id"] for item in unread["items"]}
    all_items = service.get_inbox(system["worker"], include_acknowledged=True)
    assert any(item["id"] == first["id"] and item["acknowledged"] for item in all_items["items"])


def test_report_idempotency_does_not_duplicate_message(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Idempotency",
            objective="Report once",
            acceptance=["Only one report"],
        ),
    )
    request = ReportInput(
        kind="progress",
        expected_goal_version=1,
        expected_generation=work["generation"],
        expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
        expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
        summary="halfway",
        idempotency_key="progress-1",
    )
    first = service.report(system["worker"], work["current_attempt"]["id"], request)
    second = service.report(system["worker"], work["current_attempt"]["id"], request)
    assert first["updated_at"] == second["updated_at"]
    count = service.db.fetchone("SELECT COUNT(*) AS count FROM messages WHERE kind = 'progress'")
    assert count["count"] == 1


def test_goal_revision_history_is_immutable(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Versioned",
            objective="v1",
            acceptance=["v1"],
        ),
    )
    result = service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=1,
            objective="v2",
            maturity="defined",
            acceptance=["v2"],
            reason="revision",
        ),
    )
    assert [item["version"] for item in result["goal_history"]] == [1, 2]
    assert result["goal_history"][0]["objective"] == "v1"


def test_database_backup_and_integrity(system, tmp_path):
    destination = tmp_path / "backup.sqlite3"
    system["service"].db.backup(destination)
    assert destination.exists()
    assert system["service"].db.integrity_check()["ok"] is True


def test_owned_read_connections_close_eagerly(system, tmp_path, monkeypatch):
    database = system["service"].db
    opened: list[sqlite3.Connection] = []
    original_connect = database.connect

    def tracked_connect() -> sqlite3.Connection:
        connection = original_connect()
        opened.append(connection)
        return connection

    monkeypatch.setattr(database, "connect", tracked_connect)

    assert database.fetchone("SELECT 1 AS value")["value"] == 1
    assert database.fetchall("SELECT 1 AS value")[0]["value"] == 1
    assert database.integrity_check()["ok"] is True
    database.backup(tmp_path / "eager-close.sqlite3")
    database.initialize()

    assert opened
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_direct_connection_context_closes_descriptor(system) -> None:
    connection = system["service"].db.connect()

    with connection:
        assert connection.execute("SELECT 1").fetchone()[0] == 1

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")


def test_schema_v1_is_migrated_in_place(system):
    database = system["service"].db
    database.execute("DROP INDEX IF EXISTS events_aggregate_idx")
    database.execute("UPDATE metadata SET value = '1' WHERE key = 'schema_version'")
    with database.connect() as connection:
        connection.execute("PRAGMA user_version = 1")
    restarted = Database(system["settings"])
    with restarted.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()[0] == str(SCHEMA_VERSION)
        indexes = {
            row["name"] for row in connection.execute("PRAGMA index_list(events)").fetchall()
        }
        assert "events_aggregate_idx" in indexes


def test_backup_is_private_and_replaceable(system, tmp_path):
    import stat

    destination = tmp_path / "nested" / "backup.sqlite3"
    first = system["service"].db.backup(destination)
    second = system["service"].db.backup(destination)
    assert first == second == destination.resolve()
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert not list(destination.parent.glob("*.tmp"))


def test_work_query_keyset_pagination_is_stable(system):
    service = system["service"]
    ids: list[str] = []
    for index in range(5):
        work = service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title=f"Page {index}",
                objective=f"Paginate {index}",
                acceptance=[f"Done {index}"],
                priority=50,
                idempotency_key=f"page-{index}",
            ),
        )
        ids.append(work["id"])

    from cao_control_plane.models import QueryInput

    first = service.query_work(QueryInput(limit=2), system["cao"])
    second = service.query_work(QueryInput(limit=2, cursor=first["next_cursor"]), system["cao"])
    third = service.query_work(QueryInput(limit=2, cursor=second["next_cursor"]), system["cao"])
    observed = [item["id"] for page in (first, second, third) for item in page["items"]]
    assert len(observed) == len(set(observed)) == 5
    assert set(observed) == set(ids)
    assert third["next_cursor"] is None


def test_backup_rejects_live_database_path(system):
    import pytest

    with pytest.raises(ValueError):
        system["service"].db.backup(system["service"].db.path)


def test_bootstrap_is_idempotent_across_service_instances(system):
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    result = restarted.bootstrap()
    assert result["created"] is False
    assert {item["role"] for item in result["principals"]} >= {"cao", "user"}


def test_retention_validation_and_canonical_callback_key(settings):
    from dataclasses import replace

    import pytest

    with pytest.raises(ValueError):
        replace(settings, event_retention_days=0)
    with pytest.raises(ValueError):
        replace(settings, message_retention_days=0)
    assert settings.callback_key_path.name == "callback-secrets.key"


def test_canonical_a2a_blocking_timeout_environment_wins(monkeypatch, tmp_path):
    from cao_control_plane.config import Settings

    monkeypatch.setenv("CAO_A2A_CONFIG", str(tmp_path / "missing.toml"))
    monkeypatch.setenv("CAO_A2A_STATE_DIR", str(tmp_path / "state-env"))
    monkeypatch.setenv("CAO_A2A_BLOCKING_TIMEOUT_SECONDS", "20")
    loaded = Settings.load()
    assert loaded.a2a_blocking_timeout_seconds == 20


def test_schema_v2_without_ownership_columns_is_migrated(tmp_path):
    import sqlite3
    from dataclasses import replace

    from cao_control_plane.config import Settings

    legacy_settings = replace(Settings(), state_dir=tmp_path / "legacy-state")
    legacy_settings.ensure_directories()
    path = legacy_settings.database_path
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO metadata(key, value) VALUES('schema_version', '2');
            CREATE TABLE principals(
                id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL,
                token_hash TEXT NOT NULL, enabled INTEGER NOT NULL,
                metadata_json TEXT NOT NULL, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE work_items(
                id TEXT PRIMARY KEY, title TEXT NOT NULL, objective TEXT NOT NULL,
                maturity TEXT NOT NULL, goal_version INTEGER NOT NULL,
                acceptance_json TEXT NOT NULL, non_goals_json TEXT NOT NULL,
                state TEXT NOT NULL, priority INTEGER NOT NULL,
                created_by TEXT NOT NULL, assigned_worker_id TEXT NOT NULL,
                attention_owner TEXT NOT NULL, metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            """
        )
        for principal_id, role in (("cao-1", "cao"), ("user-1", "user"), ("worker-1", "worker")):
            connection.execute(
                "INSERT INTO principals VALUES(?, ?, ?, 'hash', 1, '{}', '2026-01-01Z', '2026-01-01Z')",
                (principal_id, principal_id, role),
            )
        connection.execute(
            """
            INSERT INTO work_items VALUES(
                'work-1', 'Legacy', 'Migrate', 'defined', 1,
                '["done"]', '[]', 'active', 50,
                'cao-1', 'worker-1', 'worker', '{}', '2026-01-01Z', '2026-01-01Z'
            )
            """
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()

    migrated = Database(legacy_settings)
    with migrated.connect() as connection:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(work_items)")}
        row = connection.execute(
            "SELECT requester_id, supervisor_id FROM work_items WHERE id = 'work-1'"
        ).fetchone()
        assert {"requester_id", "supervisor_id"} <= columns
        assert row["requester_id"] == "user-1"
        assert row["supervisor_id"] == "cao-1"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_concurrent_assignment_idempotency_is_atomic(system, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from cao_control_plane.models import WorkAssignment

    service = system["service"]
    # Force every caller past the optimistic read. The transaction-scoped
    # recheck is the correctness boundary under concurrent retries.
    monkeypatch.setattr(service, "_idempotent_get", lambda *args, **kwargs: None)
    request = WorkAssignment(
        worker_id=system["worker"]["id"],
        title="Concurrent retry",
        objective="Apply this assignment exactly once",
        acceptance=["Only one WorkItem exists"],
        idempotency_key="concurrent-assignment",
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda _: service.assign_work(system["cao"], request),
                range(8),
            )
        )

    assert len({result["id"] for result in results}) == 1
    count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM work_items WHERE title = ?",
        (request.title,),
    )
    assert count["count"] == 1


def test_runtime_registration_reactivates_unread_dead_delivery(system):
    from conftest import enroll_worker_runtime

    from cao_control_plane.models import WorkAssignment

    service = system["service"]
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Resume delivery",
            objective="Deliver after the Worker runtime returns",
            acceptance=["Delivery is re-armed"],
        ),
    )
    message = service.get_inbox(system["worker"])["items"][0]
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5, "
            "reactivation_policy = 'retryable' WHERE message_id = ?",
            (message["id"],),
        )

    service.stop_runtime(system["cao"], system["runtime"]["id"])
    runtime, _, _ = enroll_worker_runtime(
        service,
        system["cao"],
        system["worker"]["id"],
        metadata={"command": ["/usr/bin/true"]},
    )
    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?",
        (message["id"],),
    )
    assert delivery["state"] == "queued"
    assert delivery["attempts"] == 0
    assert delivery["runtime_session_id"] == runtime["id"]
    assert delivery["reactivation_policy"] == "terminal"


def test_schema42_migration_classifies_only_transport_exhaustion_as_retryable(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Legacy dead-letter classification",
            objective="Migrate transport and semantic closure without guessing.",
            acceptance=["Only bounded transport exhaustion can reactivate."],
        ),
    )
    attempt = work["current_attempt"]
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (attempt["id"],),
    )
    assert assignment is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5, "
            "last_error = 'runtime_unavailable' WHERE message_id = ?",
            (assignment["id"],),
        )
        service._event(
            connection,
            "runtime.message_dead",
            "message",
            str(assignment["id"]),
            "",
            {"attempts": 5, "failure_code": "runtime_unavailable"},
        )
        cancel = service._message(
            connection,
            sender_id=system["cao"]["id"],
            recipient_id=system["worker"]["id"],
            kind=MessageKind.CANCEL,
            payload={"reason": "legacy semantic closure"},
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5, "
            "last_error = 'terminal_work_headless_epoch_closed' WHERE message_id = ?",
            (cancel["id"],),
        )
        service._event(
            connection,
            "runtime.message_dead",
            "message",
            str(cancel["id"]),
            "",
            {"attempts": 5, "failure_code": "runtime_unavailable"},
        )
        service._event(
            connection,
            "message.delivery_superseded",
            "message",
            str(cancel["id"]),
            "",
            {"reason_code": "terminal_work_headless_epoch_closed"},
        )

    with service.db.transaction() as connection:
        connection.execute("DROP INDEX IF EXISTS message_deliveries_reactivation_idx")
        connection.execute("ALTER TABLE message_deliveries DROP COLUMN reactivation_policy")
        connection.execute("UPDATE metadata SET value = '41' WHERE key = 'schema_version'")
        connection.execute("DELETE FROM schema_migrations WHERE version = 42")
        connection.execute("PRAGMA user_version = 41")

    migrated = Database(system["settings"])
    assert migrated.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    policies = {
        str(row["message_id"]): str(row["reactivation_policy"])
        for row in migrated.fetchall(
            "SELECT message_id, reactivation_policy FROM message_deliveries "
            "WHERE message_id IN (?, ?)",
            (assignment["id"], cancel["id"]),
        )
    }
    assert policies == {
        str(assignment["id"]): "retryable",
        str(cancel["id"]): "terminal",
    }


def test_runtime_registration_does_not_reactivate_terminal_attempt_delivery(system):
    from conftest import enroll_worker_runtime

    from cao_control_plane.models import WorkAssignment

    service = system["service"]
    assigned = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Superseded assignment",
            objective="Never replay this terminal attempt",
            acceptance=["Canceled attempts remain terminal"],
        ),
    )
    message = service.get_inbox(system["worker"])["items"][0]
    attempt = assigned["attempts"][-1]
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'canceled' WHERE id = ?",
            (attempt["id"],),
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5 WHERE message_id = ?",
            (message["id"],),
        )

    service.stop_runtime(system["cao"], system["runtime"]["id"])
    runtime, _, _ = enroll_worker_runtime(
        service,
        system["cao"],
        system["worker"]["id"],
        metadata={"command": ["/usr/bin/true"]},
    )

    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?",
        (message["id"],),
    )
    assert delivery["state"] == "dead"
    assert delivery["attempts"] == 5
    assert delivery["runtime_session_id"] != runtime["id"]
