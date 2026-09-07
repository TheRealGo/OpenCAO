from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.database import SCHEMA_VERSION, Database
from cao_control_plane.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    NotFoundError,
)
from cao_control_plane.goal_packets import (
    TASK_PACKET_FORMAT,
    build_task_packet,
    task_packet_digest,
)
from cao_control_plane.models import (
    DeleteWorkerThreadInput,
    InstructWorkerThreadInput,
    NewWorkerThreadInput,
    ResumeWorkerThreadInput,
    RuntimeHeartbeat,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.runtime_enrollment import ProcessIdentity
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane

_TASK_TABLES = (
    "work_items",
    "goal_revisions",
    "attempts",
    "messages",
    "message_deliveries",
)
_WORKER_TABLES = (
    "managed_worker_specs",
    "managed_worker_threads",
    "managed_worker_thread_epochs",
)


def _attached(
    service: ControlPlane,
    cao: dict[str, Any],
    *,
    suffix: str,
) -> dict[str, Any]:
    peer = ProcessIdentity(
        pid=1_600_000_000 + ord(suffix[0]),
        parent_pid=1,
        start_signature=f"worker-control-peer-{suffix}",
    )
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"worker-control-backend-{suffix}",
            project_digest=hashlib.sha256(suffix.encode()).hexdigest(),
        ),
        peer=peer,
    )
    return service.authenticate(str(attachment["context_token"]))


def _counts(service: ControlPlane, tables: tuple[str, ...]) -> dict[str, int]:
    return {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS n FROM {table}")["n"])
        for table in tables
    }


def _new(
    service: ControlPlane,
    actor: dict[str, Any],
    tmp_path: Path,
    *,
    suffix: str,
    idempotency_key: str | None = None,
) -> tuple[dict[str, Any], Path]:
    directory = tmp_path / f"private-worker-directory-{suffix}"
    directory.mkdir()
    result = service.new_worker_thread(
        actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            idempotency_key=idempotency_key or f"new-{suffix}",
        ),
    )
    return result, directory


def _thread_rows(
    service: ControlPlane, worker_thread_id: str
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    thread = service.db.fetchone(
        "SELECT * FROM managed_worker_threads WHERE id = ?", (worker_thread_id,)
    )
    assert thread is not None
    spec = service.db.fetchone(
        "SELECT spec.* FROM managed_worker_specs AS spec "
        "JOIN managed_worker_threads AS thread ON thread.managed_spec_id = spec.id "
        "WHERE thread.id = ?",
        (worker_thread_id,),
    )
    assert spec is not None
    epochs = service.db.fetchall(
        "SELECT * FROM managed_worker_thread_epochs WHERE thread_id = ? "
        "ORDER BY generation, connection_generation",
        (worker_thread_id,),
    )
    return dict(thread), dict(spec), [dict(row) for row in epochs]


def _instruct(
    service: ControlPlane,
    actor: dict[str, Any],
    worker_thread_id: str,
    *,
    key: str,
    objective: str = "Create the exact requested durable result.",
) -> dict[str, Any]:
    return service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=worker_thread_id,
            expected_generation=1,
            objective=objective,
            idempotency_key=key,
        ),
    )


def _enroll_current_runtime(service: ControlPlane, worker_thread_id: str) -> tuple[str, str]:
    _thread, spec, _epochs = _thread_rows(service, worker_thread_id)
    runtime_id = str(spec["runtime_session_id"])
    enrollment_id = str(spec["enrollment_id"])
    launch = service.issue_runtime_launch_ticket(runtime_id)
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        runtime_id,
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    # The public heartbeat path models an active Worker turn as busy.  This
    # suite needs the quiescent, nominally-ready state before selectively
    # invalidating one exact connection proof below.
    service.db.execute("UPDATE runtime_sessions SET state = 'ready' WHERE id = ?", (runtime_id,))
    return runtime_id, enrollment_id


def test_minimal_new_creates_only_empty_worker_lineage(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="a")
    before = _counts(service, _TASK_TABLES)

    created, directory = _new(service, actor, tmp_path, suffix="minimal")

    assert _counts(service, _TASK_TABLES) == before
    assert created == {
        "worker_thread_id": created["worker_thread_id"],
        "thread_state": "active",
        "thread_generation": 1,
        "name": "Codex Worker",
        "runner": "codex",
        "connection_state": "pending",
        "can_accept_instruction": True,
    }
    thread, spec, epochs = _thread_rows(service, str(created["worker_thread_id"]))
    assert (thread["state"], int(thread["generation"])) == ("active", 1)
    assert spec["worker_profile_id"] == "codex"
    assert spec["adapter"] == "codex-app-server"
    assert spec["requested_model"] == "gpt-5.6-terra"
    assert spec["requested_reasoning_effort"] == "medium"
    assert len(epochs) == 1
    assert (int(epochs[0]["generation"]), int(epochs[0]["connection_generation"])) == (
        1,
        1,
    )
    listed = service.list_managed_workers(actor)
    assert listed[0]["connection_state"] == "pending"
    assert listed[0]["can_accept_instruction"] is True
    assert listed[0]["instruction_queue"] == {
        "pending_count": 0,
        "head_state": "empty",
        "ordering": "durable_fifo",
    }
    with service.db.connection_scope() as connection:
        data_dump = "\n".join(
            line for line in connection.iterdump() if line.startswith("INSERT INTO")
        )
    assert str(directory) not in data_dump
    assert directory.name not in created["name"]


def test_minimal_instruction_commits_one_unset_v1_assignment(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="b")
    created, _directory = _new(service, actor, tmp_path, suffix="instruction")
    before = _counts(service, _TASK_TABLES)

    instructed = _instruct(
        service,
        actor,
        str(created["worker_thread_id"]),
        key="instruct-minimal",
    )

    after = _counts(service, _TASK_TABLES)
    assert {table: after[table] - before[table] for table in _TASK_TABLES} == {
        table: 1 for table in _TASK_TABLES
    }
    assert instructed["delivery_state"] == "queued"
    assert instructed["connection_state"] == "pending"
    assert instructed["can_accept_instruction"] is True
    queued_projection = service.list_managed_workers(actor)[0]["instruction_queue"]
    assert queued_projection == {
        "pending_count": 1,
        "head_state": "queued",
        "ordering": "durable_fifo",
    }
    work_id = str(instructed["task"]["work_item_id"])
    goal = service.db.fetchone(
        "SELECT * FROM goal_revisions WHERE work_item_id = ? AND version = 1",
        (work_id,),
    )
    attempt = service.db.fetchone(
        "SELECT * FROM attempts WHERE work_item_id = ? AND attempt_number = 1",
        (work_id,),
    )
    delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.work_item_id = ? AND message.kind = 'assignment'",
        (work_id,),
    )
    assert goal is not None and attempt is not None and delivery is not None
    assert goal["title"] == "Worker instruction"
    assert goal["maturity"] == "unset"
    assert delivery["state"] == "queued"
    assert delivery["runtime_session_id"] == attempt["runtime_session_id"]
    goal_packet = json.loads(str(goal["packet_json"]))
    packet = build_task_packet(
        goal_packet_digest_value=str(attempt["goal_packet_digest"]),
        work_item_id=work_id,
        goal_version=1,
        attempt_id=str(attempt["id"]),
        attempt_number=1,
        worker_id=str(attempt["worker_id"]),
        runtime_session_id=str(attempt["runtime_session_id"]),
        supervisor_attachment=goal_packet["supervisor_attachment"],
    )
    assert packet["format"] == TASK_PACKET_FORMAT == "cao-task-packet/v1"
    assert task_packet_digest(packet) == attempt["task_packet_digest"]


@pytest.mark.parametrize(
    "disconnect_kind",
    (
        "credential_absent",
        "credential_expired",
        "credential_revoked",
        "enrollment_failed",
        "enrollment_stale",
        "runtime_lease_expired",
    ),
)
def test_disconnected_ready_route_advances_only_connection_epoch_and_claims_once(
    system: dict[str, Any],
    tmp_path: Path,
    disconnect_kind: str,
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix=f"c-{disconnect_kind}")
    created, _directory = _new(service, actor, tmp_path, suffix=f"offline-{disconnect_kind}")
    thread_id = str(created["worker_thread_id"])
    old_runtime_id, old_enrollment_id = _enroll_current_runtime(service, thread_id)
    if disconnect_kind == "credential_absent":
        service.db.execute(
            "DELETE FROM runtime_credentials WHERE enrollment_id = ?",
            (old_enrollment_id,),
        )
    elif disconnect_kind == "credential_expired":
        service.db.execute(
            "UPDATE runtime_credentials SET expires_at = '1970-01-01T00:00:00Z' "
            "WHERE enrollment_id = ?",
            (old_enrollment_id,),
        )
    elif disconnect_kind == "credential_revoked":
        service.db.execute(
            "UPDATE runtime_credentials SET state = 'revoked', "
            "revoked_at = '1970-01-01T00:00:00Z' WHERE enrollment_id = ?",
            (old_enrollment_id,),
        )
    elif disconnect_kind in {"enrollment_failed", "enrollment_stale"}:
        service.db.execute(
            "UPDATE worker_enrollments SET state = ? WHERE id = ?",
            (disconnect_kind.removeprefix("enrollment_"), old_enrollment_id),
        )
    else:
        service.db.execute(
            "UPDATE runtime_sessions SET lease_expires_at = '1970-01-01T00:00:00Z' WHERE id = ?",
            (old_runtime_id,),
        )

    instructed = _instruct(
        service,
        actor,
        thread_id,
        key=f"instruct-offline-{disconnect_kind}",
    )

    thread, spec, epochs = _thread_rows(service, thread_id)
    assert int(thread["generation"]) == 1
    assert [(int(row["generation"]), int(row["connection_generation"])) for row in epochs] == [
        (1, 1),
        (1, 2),
    ]
    assert epochs[0]["retired_at"] is not None
    assert epochs[1]["retired_at"] is None
    assert spec["runtime_session_id"] != old_runtime_id
    assert instructed["thread_generation"] == 1
    assert instructed["connection_state"] == "pending"
    assert instructed["delivery_state"] == "queued"
    attempt = service.db.fetchone(
        "SELECT * FROM attempts WHERE work_item_id = ?",
        (instructed["task"]["work_item_id"],),
    )
    assert attempt is not None
    assert attempt["runtime_session_id"] == spec["runtime_session_id"]

    _enroll_current_runtime(service, thread_id)
    ready = Barrier(2)
    dispatchers = (Dispatcher(service, service.settings), Dispatcher(service, service.settings))

    def claim(dispatcher: Dispatcher) -> dict[str, Any] | None:
        ready.wait(timeout=5)
        return dispatcher._claim_delivery()

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, dispatchers))
    assert sum(item is not None for item in claims) == 1
    assert (
        service.db.fetchone("SELECT COUNT(*) AS n FROM message_deliveries WHERE state = 'leased'")[
            "n"
        ]
        == 1
    )


def test_dispatched_unknown_does_not_block_new_instruction_or_duplicate_old_delivery(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="d")
    created, _directory = _new(service, actor, tmp_path, suffix="unknown")
    thread_id = str(created["worker_thread_id"])
    _instruct(service, actor, thread_id, key="unknown-first")
    dispatcher = Dispatcher(service, service.settings)
    leased = dispatcher._claim_delivery()
    assert leased is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched' "
        "WHERE message_id = ? AND recipient_id = ? AND state = 'leased'",
        (leased["message_id"], leased["recipient_id"]),
    )
    before_tasks = _counts(service, _TASK_TABLES)
    _thread, _spec, before_epochs = _thread_rows(service, thread_id)

    second = _instruct(service, actor, thread_id, key="unknown-second")

    after_tasks = _counts(service, _TASK_TABLES)
    assert all(after_tasks[name] == before_tasks[name] + 1 for name in _TASK_TABLES)
    _thread, _spec, after_epochs = _thread_rows(service, thread_id)
    assert after_epochs == before_epochs
    assert second["delivery_state"] == "queued"
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM message_deliveries WHERE state = 'dispatched'"
        )["n"]
        == 1
    )


def test_pure_resume_advances_lifecycle_without_creating_work(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="e")
    created, _directory = _new(service, actor, tmp_path, suffix="resume")
    thread_id = str(created["worker_thread_id"])
    finished = service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=1,
            idempotency_key="finish-empty",
        ),
    )
    assert finished["thread_generation"] == 2
    before = _counts(service, _TASK_TABLES)

    resumed = service.resume_worker_thread(
        actor,
        ResumeWorkerThreadInput(
            worker_thread_id=thread_id,
            expected_generation=2,
            idempotency_key="resume-empty",
        ),
    )

    assert resumed == {
        "worker_thread_id": thread_id,
        "thread_state": "active",
        "thread_generation": 3,
        "connection_state": "pending",
        "can_accept_instruction": True,
    }
    assert _counts(service, _TASK_TABLES) == before
    thread, _spec, epochs = _thread_rows(service, thread_id)
    assert (thread["state"], int(thread["generation"])) == ("active", 3)
    assert [(int(row["generation"]), int(row["connection_generation"])) for row in epochs] == [
        (1, 1),
        (3, 1),
    ]


def test_foreign_project_is_hidden_while_busy_worker_accepts_later_work(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="g")
    created, directory = _new(service, actor, tmp_path, suffix="fences")
    thread_id = str(created["worker_thread_id"])
    before_foreign = _counts(service, (*_WORKER_TABLES, *_TASK_TABLES))
    foreign = _attached(service, system["cao"], suffix="h")
    with pytest.raises(NotFoundError):
        _instruct(service, foreign, thread_id, key="foreign-instruction")
    assert _counts(service, tuple(before_foreign)) == before_foreign

    first = _instruct(service, actor, thread_id, key="busy-first")
    assert first["delivery_state"] == "queued"
    before_busy = _counts(service, (*_WORKER_TABLES, *_TASK_TABLES))
    second = _instruct(service, actor, thread_id, key="busy-second")
    assert second["delivery_state"] == "queued"
    after_busy = _counts(service, (*_WORKER_TABLES, *_TASK_TABLES))
    assert all(after_busy[name] == before_busy[name] for name in _WORKER_TABLES)
    assert all(after_busy[name] == before_busy[name] + 1 for name in _TASK_TABLES)

    registry = service.settings.owner_private_workspace_registry_path
    registry_before = registry.read_bytes()
    renewed = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="worker-control-backend-g",
            project_digest=hashlib.sha256(b"g").hexdigest(),
        ),
        peer=ProcessIdentity(
            pid=1_600_000_000 + ord("g"),
            parent_pid=1,
            start_signature="worker-control-peer-g",
        ),
    )
    renewed_actor = service.authenticate(str(renewed["context_token"]))
    stale_directory = tmp_path / "stale-directory"
    stale_directory.mkdir()
    before_stale = _counts(service, (*_WORKER_TABLES, *_TASK_TABLES))
    with pytest.raises((AuthenticationError, AuthorizationError)):
        service.new_worker_thread(
            actor,
            NewWorkerThreadInput(
                working_directory=str(stale_directory),
                idempotency_key="stale-new",
            ),
        )
    assert _counts(service, tuple(before_stale)) == before_stale
    assert registry.read_bytes() == registry_before

    exact_retry = service.new_worker_thread(
        renewed_actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            idempotency_key="new-fences",
        ),
    )
    assert exact_retry["worker_thread_id"] == thread_id
    assert registry.read_bytes() == registry_before
    conflicting_directory = tmp_path / "same-key-conflicting-directory"
    conflicting_directory.mkdir()
    with pytest.raises(ConflictError):
        service.new_worker_thread(
            renewed_actor,
            NewWorkerThreadInput(
                working_directory=str(conflicting_directory),
                idempotency_key="new-fences",
            ),
        )
    assert registry.read_bytes() == registry_before
    assert str(directory) not in "\n".join(str(item) for item in first.values())


def test_same_project_conversation_lists_and_instructs_existing_worker_without_generation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    source = _attached(service, system["cao"], suffix="q")
    created, _directory = _new(service, source, tmp_path, suffix="shared-project")
    thread_id = str(created["worker_thread_id"])
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="worker-control-backend-q-fork",
            project_digest=str(source["_cao_project_digest"]),
        ),
        peer=ProcessIdentity(
            pid=1_600_000_777,
            parent_pid=1,
            start_signature="worker-control-peer-q-fork",
        ),
    )
    current = service.authenticate(str(attachment["context_token"]))

    assert thread_id in {
        str(worker["worker_thread_id"]) for worker in service.list_managed_workers(current)
    }
    instructed = service.instruct_worker_thread(
        current,
        InstructWorkerThreadInput(
            worker_thread_id=thread_id,
            objective="Continue through the current CAO conversation.",
            idempotency_key="same-project-fork-instruction",
        ),
    )

    work = service.get_work(str(instructed["task"]["work_item_id"]))
    assert work["managed_worker_thread_id"] == thread_id
    assert work["supervisor_attachment_id"] == current["_cao_attachment_id"]
    projection = verify_projection(service.db)
    assert projection.healthy is True, projection.violations


def test_same_project_conversation_can_finish_resume_and_delete_without_generation_or_ack(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    source = _attached(service, system["cao"], suffix="r")
    created, _directory = _new(service, source, tmp_path, suffix="shared-lifecycle")
    thread_id = str(created["worker_thread_id"])
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="worker-control-backend-r-fork",
            project_digest=str(source["_cao_project_digest"]),
        ),
        peer=ProcessIdentity(
            pid=1_600_000_778,
            parent_pid=1,
            start_signature="worker-control-peer-r-fork",
        ),
    )
    current = service.authenticate(str(attachment["context_token"]))

    finished = service.finish_worker_thread(
        current,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            idempotency_key="same-project-finish",
        ),
    )
    assert finished["thread_state"] == "archived"
    resumed = service.resume_worker_thread(
        current,
        ResumeWorkerThreadInput(
            worker_thread_id=thread_id,
            idempotency_key="same-project-resume",
        ),
    )
    assert resumed["thread_state"] == "active"
    deleted = service.delete_worker_thread(
        current,
        DeleteWorkerThreadInput(
            worker_thread_id=thread_id,
            idempotency_key="same-project-delete",
        ),
    )
    assert deleted["thread_state"] == "deleted"
    assert thread_id not in {
        str(worker["worker_thread_id"]) for worker in service.list_managed_workers(current)
    }


def test_new_creates_multiple_workers_in_same_directory_when_cao_requests_each_one(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="s")
    directory = tmp_path / "shared-worker-directory"
    directory.mkdir()

    first = service.new_worker_thread(
        actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            name="Parallel Worker",
            idempotency_key="parallel-worker-one",
        ),
    )
    second = service.new_worker_thread(
        actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            name="Parallel Worker",
            idempotency_key="parallel-worker-two",
        ),
    )

    assert first["worker_thread_id"] != second["worker_thread_id"]
    listed = [
        worker
        for worker in service.list_managed_workers(actor)
        if worker["operator_label"] == "Parallel Worker"
    ]
    assert len(listed) == 2


def test_stale_new_does_not_create_a_pristine_owner_private_registry(
    system: dict[str, Any], tmp_path: Path
) -> None:
    settings = replace(
        system["settings"],
        state_dir=tmp_path / "pristine-owner-state",
        runtime_launch_dir=tmp_path / "pristine-owner-state" / "runtime-launches",
        owner_private_policy_file=None,
    )
    settings.ensure_directories()
    service = ControlPlane(system["service"].db, settings)
    stale_actor = _attached(service, system["cao"], suffix="stale-pristine")
    renewed = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="worker-control-backend-stale-pristine",
            project_digest=hashlib.sha256(b"stale-pristine").hexdigest(),
        ),
        peer=ProcessIdentity(
            pid=1_600_000_000 + ord("s"),
            parent_pid=1,
            start_signature="worker-control-peer-stale-pristine",
        ),
    )
    service.authenticate(str(renewed["context_token"]))
    directory = tmp_path / "stale-pristine-directory"
    directory.mkdir()
    before = _counts(service, (*_WORKER_TABLES, *_TASK_TABLES))
    registry = settings.owner_private_workspace_registry_path
    policy = settings.owner_private_dynamic_policy_path
    assert not registry.exists()
    assert not policy.exists()

    with pytest.raises((AuthenticationError, AuthorizationError)):
        service.new_worker_thread(
            stale_actor,
            NewWorkerThreadInput(
                working_directory=str(directory),
                idempotency_key="stale-pristine-new",
            ),
        )

    assert _counts(service, tuple(before)) == before
    assert not registry.exists()
    assert not policy.exists()


def test_v32_epoch_migration_preserves_existing_v1_packets_byte_for_byte(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, system["cao"], suffix="i")
    created, _directory = _new(service, actor, tmp_path, suffix="migration")
    instructed = _instruct(
        service,
        actor,
        str(created["worker_thread_id"]),
        key="migration-instruction",
    )
    work_id = str(instructed["task"]["work_item_id"])
    packets_before = dict(
        service.db.fetchone(
            "SELECT goal.packet_json, goal.packet_digest, attempt.task_packet_digest "
            "FROM goal_revisions AS goal JOIN attempts AS attempt "
            "ON attempt.work_item_id = goal.work_item_id AND attempt.goal_version = goal.version "
            "WHERE goal.work_item_id = ?",
            (work_id,),
        )
    )
    with service.db.transaction() as connection:
        connection.execute("DROP INDEX managed_worker_thread_epochs_one_current")
        connection.execute(
            "ALTER TABLE managed_worker_thread_epochs RENAME TO managed_worker_thread_epochs_v33"
        )
        connection.execute(
            """
            CREATE TABLE managed_worker_thread_epochs (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL REFERENCES managed_worker_threads(id) ON DELETE CASCADE,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                runtime_session_id TEXT NOT NULL UNIQUE
                    REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
                enrollment_id TEXT NOT NULL UNIQUE
                    REFERENCES worker_enrollments(id) ON DELETE RESTRICT,
                created_at TEXT NOT NULL,
                retired_at TEXT,
                UNIQUE(thread_id, generation)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO managed_worker_thread_epochs(
                id, thread_id, generation, runtime_session_id,
                enrollment_id, created_at, retired_at
            )
            SELECT id, thread_id, generation, runtime_session_id,
                   enrollment_id, created_at, retired_at
            FROM managed_worker_thread_epochs_v33
            """
        )
        connection.execute("DROP TABLE managed_worker_thread_epochs_v33")
        connection.execute(
            "CREATE UNIQUE INDEX managed_worker_thread_epochs_one_current "
            "ON managed_worker_thread_epochs(thread_id) WHERE retired_at IS NULL"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 33")
        connection.execute("UPDATE metadata SET value = '32' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 32")

    Database(system["settings"])

    packets_after = dict(
        service.db.fetchone(
            "SELECT goal.packet_json, goal.packet_digest, attempt.task_packet_digest "
            "FROM goal_revisions AS goal JOIN attempts AS attempt "
            "ON attempt.work_item_id = goal.work_item_id AND attempt.goal_version = goal.version "
            "WHERE goal.work_item_id = ?",
            (work_id,),
        )
    )
    assert packets_after == packets_before
    epoch = service.db.fetchone(
        "SELECT generation, connection_generation FROM managed_worker_thread_epochs "
        "WHERE thread_id = ?",
        (created["worker_thread_id"],),
    )
    assert epoch is not None
    assert (int(epoch["generation"]), int(epoch["connection_generation"])) == (1, 1)
    assert service.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
