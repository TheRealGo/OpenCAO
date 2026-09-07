from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest

from cao_control_plane.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    StaleGenerationError,
)
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    AckInput,
    PrincipalCreate,
    ReportInput,
    ReportKind,
    RuntimeHeartbeat,
    RuntimeRegistration,
    RuntimeState,
    WorkAssignment,
)
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    receive_enrollment_capability,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS


def _reset_default_worker(system: dict[str, Any]) -> dict[str, Any]:
    system["service"].stop_runtime(system["cao"], system["runtime"]["id"])
    return system["service"].authenticate(system["worker_principal_token"])


def _register_managed_runtime(
    system: dict[str, Any],
    worker_id: str,
) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    service = system["service"]
    runtime = service.register_runtime(
        system["cao"], worker_id, RuntimeRegistration(adapter="claude")
    )
    ticket = service.issue_runtime_launch_ticket(runtime["id"])["ticket"]
    exchange = service.exchange_runtime_launch_ticket(ticket)
    credential = exchange["token"]
    return runtime, ticket, credential, service.authenticate(credential)


def _complete_handshake(
    system: dict[str, Any], runtime: dict[str, Any], actor: dict[str, Any]
) -> dict[str, Any]:
    service = system["service"]
    generation = actor["_enrollment_generation"]
    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    return service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(expected_enrollment_generation=generation, sequence=1),
    )


def _assign(system: dict[str, Any], worker_id: str, *, key: str) -> dict[str, Any]:
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=worker_id,
            title=f"Enrollment task {key}",
            objective="Prove the managed Worker lifecycle.",
            acceptance=["A managed runtime is bound"],
            non_goals=["Do not use principal tokens"],
            idempotency_key=key,
        ),
    )


def _assignment_message(service: Any, actor: dict[str, Any]) -> dict[str, Any]:
    return next(item for item in service.get_inbox(actor)["items"] if item["kind"] == "assignment")


def _progress(work: dict[str, Any]) -> ReportInput:
    attempt = work["current_attempt"]
    return ReportInput(
        kind=ReportKind.PROGRESS,
        expected_goal_version=work["goal_version"],
        expected_generation=work["generation"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        summary="Managed Worker is progressing.",
    )


def test_terminal_managed_worker_recovery_creates_one_boundary_without_retrying_assignment(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    _reset_default_worker(system)
    runtime, _, _, actor = _register_managed_runtime(system, system["worker"]["id"])
    _complete_handshake(system, runtime, actor)
    work = _assign(system, system["worker"]["id"], key="terminal-recovery")
    assignment = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (work["current_attempt"]["id"],),
    )
    assert assignment is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched' WHERE message_id = ? AND recipient_id = ?",
        (assignment["message_id"], assignment["recipient_id"]),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?", (runtime["id"],)
    )

    boundary = service.recover_terminal_worker_attempt(
        runtime["id"], reason="worker_inactive_timeout"
    )
    assert boundary is not None and boundary["kind"] == "failure"
    assert boundary["generation"] == work["generation"] + 1
    assert (
        service.recover_terminal_worker_attempt(runtime["id"], reason="worker_inactive_timeout")[
            "id"
        ]
        == boundary["id"]
    )
    recovered = service.get_work(work["id"])
    assert recovered["state"] == "waiting_supervisor"
    assert recovered["attention_owner"] == "cao"
    assert recovered["current_attempt"]["state"] == "waiting_supervisor"
    unchanged_assignment = service.db.fetchone(
        "SELECT state, generation, attempts FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (assignment["message_id"], assignment["recipient_id"]),
    )
    assert unchanged_assignment is not None
    assert dict(unchanged_assignment) == {
        "state": "dispatched",
        "generation": assignment["generation"],
        "attempts": assignment["attempts"],
    }
    recovery_delivery = service.db.fetchone(
        """
        SELECT delivery.state FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE json_extract(message.payload_json, '$.boundary_id') = ?
          AND delivery.recipient_id = ?
        """,
        (boundary["id"], system["cao"]["id"]),
    )
    assert recovery_delivery is not None and recovery_delivery["state"] == "queued"


def test_terminal_runtime_fences_every_historical_unsettled_work_fail_closed(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    _reset_default_worker(system)
    runtime, _, _, actor = _register_managed_runtime(system, system["worker"]["id"])
    _complete_handshake(system, runtime, actor)
    works = [
        _assign(system, system["worker"]["id"], key="historical-multi-one"),
        _assign(system, system["worker"]["id"], key="historical-multi-two"),
    ]
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?", (runtime["id"],)
    )

    boundary = service.recover_terminal_worker_attempt(
        runtime["id"], reason="worker_inactive_timeout"
    )

    assert boundary is not None
    for work in works:
        recovered = service.get_work(work["id"])
        assert recovered["state"] == "waiting_supervisor"
        assert recovered["attention_owner"] == "cao"
        assert recovered["current_attempt"]["stage"] == "system_reconciliation"
        assert recovered["current_attempt"]["next_boundary"] == ("system_reconciliation")
        assert len(recovered["open_boundaries"]) == 1
        assert recovered["open_boundaries"][0]["recovery_action"] == ("system_reconciliation")
        delivery = service.db.fetchone(
            "SELECT delivery.state, delivery.last_error "
            "FROM message_deliveries AS delivery "
            "JOIN messages AS message ON message.id = delivery.message_id "
            "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
            (work["current_attempt"]["id"],),
        )
        assert delivery is not None
        assert dict(delivery) == {
            "state": "dead",
            "last_error": "system_reconciliation_required",
        }
    repeated = service.recover_terminal_worker_attempt(
        runtime["id"], reason="worker_inactive_timeout"
    )
    assert repeated is not None and repeated["id"] == boundary["id"]
    count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM boundaries WHERE attempt_id IN (?, ?)",
        tuple(work["current_attempt"]["id"] for work in works),
    )
    assert count is not None and count["count"] == 2


def test_worker_is_awaiting_and_principal_token_cannot_use_worker_paths_or_assignment(system):
    service = system["service"]
    principal_actor = _reset_default_worker(system)
    runtime = service.register_runtime(
        system["cao"],
        system["worker"]["id"],
        RuntimeRegistration(adapter="claude"),
    )

    assert runtime["enrollment"]["state"] == "awaiting_handshake"
    with pytest.raises(AuthorizationError, match="managed MCP runtime credential"):
        service.get_inbox(principal_actor)
    with pytest.raises(ConflictError, match="not assignment-ready"):
        _assign(system, system["worker"]["id"], key="awaiting-assignment")


def test_ticket_discovery_and_ordered_heartbeat_bind_assignment_packet_and_delivery(system):
    service = system["service"]
    _reset_default_worker(system)
    runtime, _, _, actor = _register_managed_runtime(system, system["worker"]["id"])

    with pytest.raises(ConflictError, match="fresh handshake"):
        service.get_inbox(actor)
    with pytest.raises(ConflictError, match="not assignment-ready"):
        _assign(system, actor["id"], key="before-discovery")

    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    ready = service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=actor["_enrollment_generation"], sequence=1
        ),
    )
    assert ready["enrollment"]["state"] == "ready"
    assert ready["enrollment"]["heartbeat_sequence"] == 1

    work = _assign(system, actor["id"], key="ready-assignment")
    attempt = work["current_attempt"]
    message = _assignment_message(service, actor)
    assert attempt["runtime_session_id"] == runtime["id"]
    assert message["payload"]["attempt_id"] == attempt["id"]
    assert message["payload"]["goal_packet_digest"] == attempt["goal_packet_digest"]
    assert message["payload"]["task_packet_digest"] == attempt["task_packet_digest"]

    delivery = service.db.fetchone(
        "SELECT runtime_session_id FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (message["id"], actor["id"]),
    )
    assert delivery is not None
    assert delivery["runtime_session_id"] == runtime["id"]


def test_ticket_replay_wrong_or_expired_ticket_and_bad_heartbeat_order_fail_closed(system):
    service = system["service"]
    _reset_default_worker(system)
    runtime, ticket, _, first_actor = _register_managed_runtime(system, system["worker"]["id"])

    with pytest.raises(AuthenticationError):
        service.exchange_runtime_launch_ticket(ticket)
    with pytest.raises(AuthenticationError):
        service.exchange_runtime_launch_ticket("cao.ent_missing.wrong")

    # A live launch epoch owns the sole active credential.  A later delivery
    # must never rotate it underneath the connected Worker.
    with pytest.raises(ConflictError, match=r"active launch epoch|cannot issue"):
        service.issue_runtime_launch_ticket(runtime["id"])
    service.db.execute(
        "UPDATE runtime_credentials SET state = 'revoked', revoked_at = updated_at WHERE id = ?",
        (first_actor["_runtime_credential_id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?",
        (runtime["id"],),
    )

    replacement = service.issue_runtime_launch_ticket(runtime["id"])["ticket"]
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE runtime_enrollment_tickets SET expires_at = '2000-01-01T00:00:00Z' "
            "WHERE ticket_hash = (SELECT ticket_hash FROM runtime_enrollment_tickets "
            "WHERE id = ?)",
            (replacement.split(".", 2)[1],),
        )
    with pytest.raises(AuthenticationError):
        service.exchange_runtime_launch_ticket(replacement)
    service.revoke_runtime_launch_ticket(replacement.split(".", 2)[1])

    next_ticket = service.issue_runtime_launch_ticket(runtime["id"])["ticket"]
    next_exchange = service.exchange_runtime_launch_ticket(next_ticket)
    actor = service.authenticate(next_exchange["token"])
    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    with pytest.raises(StaleGenerationError):
        service.heartbeat_runtime(
            actor,
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=first_actor["_enrollment_generation"], sequence=1
            ),
        )
    with pytest.raises(ConflictError, match="stale or out of order"):
        service.heartbeat_runtime(
            actor,
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=actor["_enrollment_generation"], sequence=2
            ),
        )
    _complete_handshake(system, runtime, actor)
    before_enrollment = dict(
        service.db.fetchone(
            "SELECT state, heartbeat_sequence, heartbeat_at, lease_expires_at, updated_at "
            "FROM worker_enrollments WHERE runtime_session_id = ?",
            (runtime["id"],),
        )
    )
    before_runtime = dict(
        service.db.fetchone(
            "SELECT state, heartbeat_at, lease_expires_at, updated_at "
            "FROM runtime_sessions WHERE id = ?",
            (runtime["id"],),
        )
    )
    before_events = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events WHERE aggregate_id = ?", (runtime["id"],)
    )["count"]
    duplicate = service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(
            state=RuntimeState.BUSY,
            lease_seconds=86_400,
            expected_enrollment_generation=actor["_enrollment_generation"],
            sequence=1,
        ),
    )
    assert duplicate["enrollment"]["heartbeat_sequence"] == 1
    assert (
        dict(
            service.db.fetchone(
                "SELECT state, heartbeat_sequence, heartbeat_at, lease_expires_at, updated_at "
                "FROM worker_enrollments WHERE runtime_session_id = ?",
                (runtime["id"],),
            )
        )
        == before_enrollment
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT state, heartbeat_at, lease_expires_at, updated_at "
                "FROM runtime_sessions WHERE id = ?",
                (runtime["id"],),
            )
        )
        == before_runtime
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE aggregate_id = ?", (runtime["id"],)
        )["count"]
        == before_events
    )
    with pytest.raises(ConflictError, match="stale or out of order"):
        service.heartbeat_runtime(
            actor,
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=actor["_enrollment_generation"], sequence=0
            ),
        )
    with pytest.raises(ConflictError, match="stale or out of order"):
        service.heartbeat_runtime(
            actor,
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=actor["_enrollment_generation"], sequence=3
            ),
        )


def test_same_root_mcp_child_reconnect_converges_after_periodic_heartbeat(system):
    """A recreated stdio child gets one credential and converges before seq3."""

    service = system["service"]
    _reset_default_worker(system)
    runtime = service.register_runtime(
        system["cao"], system["worker"]["id"], RuntimeRegistration(adapter="claude")
    )
    issued = service.issue_runtime_launch_ticket(runtime["id"])
    exchanges: list[str] = []

    async def exercise() -> None:
        broker = EnrollmentCapabilityBroker(
            configured_root=system["settings"].runtime_launch_dir,
            ticket_id=str(issued["ticket_id"]),
            raw_ticket=str(issued["ticket"]),
            exchange=lambda ticket: (
                exchanges.append(ticket) or service.exchange_runtime_launch_ticket(ticket)
            ),
            delivery_failed=lambda reason: pytest.fail(reason),
        )
        await broker.start()
        broker.bind_runner_pid(os.getpid())
        try:
            first_child = await receive_enrollment_capability(broker.path, timeout_seconds=2)
            actor = service.authenticate(str(first_child["token"]))
            generation = int(actor["_enrollment_generation"])
            service.record_mcp_tool_discovery(
                actor,
                protocol_version="2025-06-18",
                tool_names=WORKER_MCP_REQUIRED_TOOLS,
            )
            service.heartbeat_runtime(
                actor,
                runtime["id"],
                RuntimeHeartbeat(expected_enrollment_generation=generation, sequence=1),
            )
            service.heartbeat_runtime(
                actor,
                runtime["id"],
                RuntimeHeartbeat(expected_enrollment_generation=generation, sequence=2),
            )
            before_enrollment = dict(
                service.db.fetchone(
                    "SELECT state, heartbeat_sequence, heartbeat_at, lease_expires_at, updated_at "
                    "FROM worker_enrollments WHERE runtime_session_id = ?",
                    (runtime["id"],),
                )
            )
            before_runtime = dict(
                service.db.fetchone(
                    "SELECT state, heartbeat_at, lease_expires_at, updated_at "
                    "FROM runtime_sessions WHERE id = ?",
                    (runtime["id"],),
                )
            )
            before_events = int(
                service.db.fetchone(
                    "SELECT COUNT(*) AS count FROM events WHERE aggregate_id = ?",
                    (runtime["id"],),
                )["count"]
            )

            recreated_child = await receive_enrollment_capability(broker.path, timeout_seconds=2)
            assert recreated_child == first_child
            assert exchanges == [str(issued["ticket"])]

            converged = service.heartbeat_runtime(
                actor,
                runtime["id"],
                RuntimeHeartbeat(
                    state=RuntimeState.BUSY,
                    lease_seconds=86_400,
                    expected_enrollment_generation=generation,
                    sequence=1,
                ),
            )
            assert converged["enrollment"]["heartbeat_sequence"] == 2
            assert (
                dict(
                    service.db.fetchone(
                        "SELECT state, heartbeat_sequence, heartbeat_at, lease_expires_at, updated_at "
                        "FROM worker_enrollments WHERE runtime_session_id = ?",
                        (runtime["id"],),
                    )
                )
                == before_enrollment
            )
            assert (
                dict(
                    service.db.fetchone(
                        "SELECT state, heartbeat_at, lease_expires_at, updated_at "
                        "FROM runtime_sessions WHERE id = ?",
                        (runtime["id"],),
                    )
                )
                == before_runtime
            )
            assert (
                int(
                    service.db.fetchone(
                        "SELECT COUNT(*) AS count FROM events WHERE aggregate_id = ?",
                        (runtime["id"],),
                    )["count"]
                )
                == before_events
            )

            advanced = service.heartbeat_runtime(
                actor,
                runtime["id"],
                RuntimeHeartbeat(expected_enrollment_generation=generation, sequence=3),
            )
            assert advanced["enrollment"]["heartbeat_sequence"] == 3
        finally:
            await broker.close()

    asyncio.run(exercise())


def test_runtime_credentials_isolate_worker_context_inbox_acknowledgement_and_report(system):
    service = system["service"]
    _reset_default_worker(system)
    other = service.create_principal(
        system["cao"], PrincipalCreate(name="isolated-worker", role="worker")
    )
    runtime_one, _, _, actor_one = _register_managed_runtime(system, system["worker"]["id"])
    runtime_two, _, _, actor_two = _register_managed_runtime(system, other["principal"]["id"])
    _complete_handshake(system, runtime_one, actor_one)
    _complete_handshake(system, runtime_two, actor_two)

    work_one = _assign(system, actor_one["id"], key="isolation-one")
    _assign(system, actor_two["id"], key="isolation-two")
    message_one = _assignment_message(service, actor_one)
    message_two = _assignment_message(service, actor_two)

    assert (
        service.get_worker_context(actor_one, work_one["current_attempt"]["id"])["attempt"]["id"]
        == work_one["current_attempt"]["id"]
    )
    assert {item["id"] for item in service.get_inbox(actor_one)["items"]}.isdisjoint(
        {message_two["id"]}
    )
    with pytest.raises(AuthorizationError):
        service.get_worker_context(actor_two, work_one["current_attempt"]["id"])
    with pytest.raises(AuthorizationError):
        service.acknowledge(actor_two, AckInput(message_ids=[message_one["id"]]))
    with pytest.raises(AuthorizationError):
        service.report(actor_two, work_one["current_attempt"]["id"], _progress(work_one))

    service.acknowledge(actor_one, AckInput(message_ids=[message_one["id"]]))
    service.acknowledge(actor_two, AckInput(message_ids=[message_two["id"]]))
    progressed = service.report(actor_one, work_one["current_attempt"]["id"], _progress(work_one))
    assert progressed["state"] == "active"
    assert progressed["current_attempt"]["state"] == "working"
    completed = service.report(
        actor_one,
        work_one["current_attempt"]["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work_one["goal_version"],
            expected_generation=work_one["generation"],
            expected_goal_packet_digest=work_one["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work_one["current_attempt"]["task_packet_digest"],
            summary="Managed Worker completion is durable.",
            evidence=[{"command": "pytest", "result": "pass"}],
        ),
    )
    assert completed["state"] == "waiting_supervisor"
    assert completed["current_attempt"]["completion_claim"]["summary"] == (
        "Managed Worker completion is durable."
    )


def test_stopping_revokes_prior_runtime_then_allows_fresh_enrollment_and_no_plaintext_secret_persists(
    system, tmp_path: Path
):
    service = system["service"]
    _reset_default_worker(system)
    runtime, ticket, credential, actor = _register_managed_runtime(system, system["worker"]["id"])
    _complete_handshake(system, runtime, actor)
    work = _assign(system, actor["id"], key="before-stop")
    message = _assignment_message(service, actor)

    stopped = service.stop_runtime(system["cao"], runtime["id"])
    assert stopped["enrollment"]["state"] == "revoked"
    with pytest.raises(AuthenticationError):
        service.authenticate(credential)
    with pytest.raises(AuthorizationError, match="stale or revoked"):
        service.acknowledge(actor, AckInput(message_ids=[message["id"]]))
    with pytest.raises(ConflictError, match="not assignment-ready"):
        _assign(system, actor["id"], key="after-stop")

    fresh_runtime, fresh_ticket, fresh_credential, fresh_actor = _register_managed_runtime(
        system, system["worker"]["id"]
    )
    _complete_handshake(system, fresh_runtime, fresh_actor)
    fresh_work = _assign(system, fresh_actor["id"], key="after-fresh-enrollment")
    assert fresh_work["current_attempt"]["runtime_session_id"] == fresh_runtime["id"]
    assert work["current_attempt"]["runtime_session_id"] == runtime["id"]

    backup = tmp_path / "enrollment-backup.sqlite3"
    service.db.backup(backup)
    snapshots = [
        service.db.path,
        service.db.path.with_name(f"{service.db.path.name}-wal"),
        service.db.path.with_name(f"{service.db.path.name}-shm"),
        backup,
    ]
    for path in snapshots:
        if not path.exists():
            continue
        contents = path.read_bytes()
        for secret in (ticket, credential, fresh_ticket, fresh_credential):
            assert secret.encode() not in contents, path
    rendered_metadata = json.dumps(service.get_runtime(fresh_runtime["id"])["metadata"])
    rendered_events = json.dumps(service.list_events(limit=1000)["items"])
    for secret in (ticket, credential, fresh_ticket, fresh_credential):
        assert secret not in rendered_metadata
        assert secret not in rendered_events


def test_unbound_runtime_revision_keeps_raw_bounded_error_without_mcp_rewrite(
    system,
) -> None:
    service = system["service"]
    _reset_default_worker(system)
    runtime, _, _, actor = _register_managed_runtime(system, system["worker"]["id"])
    _complete_handshake(system, runtime, actor)
    work = _assign(system, actor["id"], key="revise-after-runtime-stop")
    service.stop_runtime(system["cao"], runtime["id"])
    before_work = dict(
        service.db.fetchone(
            "SELECT state, generation, goal_version FROM work_items WHERE id = ?",
            (work["id"],),
        )
    )
    before_revision_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM goal_revisions WHERE work_item_id = ?",
        (work["id"],),
    )["count"]
    before_message_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM messages WHERE work_item_id = ?",
        (work["id"],),
    )["count"]

    with pytest.raises(ControlPlaneError) as caught:
        MCPServer(service).call_tool(
            system["cao"],
            "cao_revise_goal",
            {
                "work_item_id": work["id"],
                "expected_version": 1,
                "objective": "Inspect the remaining reusable categories.",
                "maturity": "defined",
                "acceptance": ["The classification is independently reviewable."],
                "reason": "Extend the requested inspection.",
                "idempotency_key": "revise-after-runtime-stop",
            },
        )

    assert caught.value.as_dict() == {
        "code": "conflict",
        "message": "current managed Worker is not assignment-ready",
        "details": {
            "reason_code": "worker_runtime_not_connected",
            "retryable": False,
        },
    }
    assert work["id"] not in json.dumps(caught.value.as_dict(), sort_keys=True)
    assert (
        dict(
            service.db.fetchone(
                "SELECT state, generation, goal_version FROM work_items WHERE id = ?",
                (work["id"],),
            )
        )
        == before_work
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM goal_revisions WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == before_revision_count
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM messages WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == before_message_count
    )
