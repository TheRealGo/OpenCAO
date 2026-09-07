from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from collections.abc import Callable
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from pydantic import ValidationError as PydanticValidationError

import cao_control_plane.database as database_module
import cao_control_plane.service as service_module
from cao_control_plane.dashboard import DashboardReadModel, build_operator_view
from cao_control_plane.database import (
    Database,
    _backfill_managed_worker_recovery_actions,
    utc_after,
    utc_now,
)
from cao_control_plane.errors import AuthorizationError, ConflictError, NotFoundError
from cao_control_plane.goal_packets import build_task_packet, task_packet_digest
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    BoundaryInput,
    CloseCAOConversationInput,
    InstructWorkerThreadInput,
    MessageKind,
    ResumeWorkerThreadInput,
    RuntimeDispatchResult,
    RuntimeHeartbeat,
    StatusRequestInput,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.security import hash_token
from cao_control_plane.service import (
    WORKER_MCP_REQUIRED_TOOLS,
    ControlPlane,
    _digest,
    worker_mcp_tool_contract_digest,
)


def _attached(system: dict[str, Any], *, suffix: str = "a") -> dict[str, Any]:
    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id=f"worker-thread-lifecycle-{suffix}",
            project_digest=suffix[0] * 64,
        ),
    )
    return system["service"].authenticate(attachment["context_token"])


def _revoke_and_reattach_cao(
    system: dict[str, Any],
    actor: dict[str, Any],
    *,
    suffix: str,
) -> dict[str, Any]:
    service = system["service"]
    source = service.db.fetchone(
        "SELECT runtime_session_id FROM cao_session_attachments WHERE id = ?",
        (actor["_cao_attachment_id"],),
    )
    assert source is not None
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE cao_session_attachments SET state = 'failed', updated_at = ? WHERE id = ?",
            (now, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE runtime_sessions SET state = 'failed', updated_at = ? WHERE id = ?",
            (now, source["runtime_session_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials SET state = 'revoked', "
            "revoked_at = ?, updated_at = ? WHERE attachment_id = ?",
            (now, now, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_runtime_credentials SET state = 'revoked', "
            "revoked_at = ?, updated_at = ? WHERE attachment_id = ?",
            (now, now, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_runtime_tickets SET state = 'revoked', updated_at = ? "
            "WHERE attachment_id = ? AND state = 'pending'",
            (now, actor["_cao_attachment_id"]),
        )
    renewed = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"worker-thread-lifecycle-{suffix}",
            project_digest=suffix[0] * 64,
        ),
    )
    renewed_actor = service.authenticate(renewed["context_token"])
    assert renewed_actor["_cao_attachment_id"] == actor["_cao_attachment_id"]
    assert renewed_actor["_cao_attachment_generation"] == actor["_cao_attachment_generation"]
    old_csc = actor.get("_cao_conversation_credential_id")
    if old_csc:
        assert renewed_actor["_cao_conversation_credential_id"] != old_csc
    return renewed_actor


def _run_after_role_check_gate(
    monkeypatch: Any,
    *,
    service: Any,
    actor: dict[str, Any],
    operation: Callable[[], Any],
    linearize_first: Callable[[], Any],
) -> dict[str, Any]:
    role_checked = threading.Event()
    continue_operation = threading.Event()
    original_require_role = service._require_role
    credential_field = (
        "_cao_conversation_credential_id"
        if actor.get("_cao_conversation_credential_id")
        else "_cao_runtime_credential_id"
    )
    credential_id = actor.get(credential_field)
    assert credential_id

    def gated_require_role(candidate: dict[str, Any], *roles: Any) -> None:
        original_require_role(candidate, *roles)
        if candidate.get(credential_field) == credential_id:
            role_checked.set()
            if not continue_operation.wait(timeout=10):
                raise AssertionError("CAO role-check gate timed out")

    monkeypatch.setattr(service, "_require_role", gated_require_role)
    outcome: dict[str, Any] = {}

    def run_operation() -> None:
        try:
            outcome["result"] = operation()
        except BaseException as error:
            outcome["error"] = error

    thread = threading.Thread(target=run_operation, daemon=True)
    thread.start()
    assert role_checked.wait(timeout=10)
    try:
        linearize_first()
    finally:
        continue_operation.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    return outcome


def _seed_managed_thread(
    system: dict[str, Any],
    actor: dict[str, Any],
    *,
    ordinal: int,
    with_thread: bool = True,
    spec_state: str = "enabled",
    runtime_state: str = "starting",
    enrollment_state: str = "awaiting_handshake",
    native_session_id: str = "",
    operator_scope: str = "production",
) -> dict[str, str]:
    suffix = f"{ordinal:032x}"
    ids = {
        "spec_id": f"mws_{suffix}",
        "principal_id": f"prn_{suffix}",
        "runtime_id": f"run_{suffix}",
        "enrollment_id": f"enr_{suffix}",
        "thread_id": f"mwt_{suffix}",
        "epoch_id": f"mwe_{suffix}",
    }
    now = utc_now()
    digest = hashlib.sha256(suffix.encode()).hexdigest()
    operator_label = (
        f"Managed lifecycle Worker {ordinal}"
        if operator_scope in {"production", "acceptance-test"}
        else ""
    )
    policy_binding_digest = _digest(
        {
            "attachment_id": actor["_cao_attachment_id"],
            "attachment_generation": actor["_cao_attachment_generation"],
            "profile": "test-profile",
            "adapter": "codex-app-server",
            "workspace_ref": f"workspace-{ordinal}",
            "model": "test-model",
            "reasoning_effort": "high",
            "operator_scope": operator_scope,
            "operator_label": operator_label,
        }
    )
    with system["service"].db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO principals(
                id, name, role, token_hash, enabled, operator_scope,
                operator_label, metadata_json, created_at, updated_at
            ) VALUES(?, ?, 'worker', ?, 1, ?, ?, '{}', ?, ?)
            """,
            (
                ids["principal_id"],
                f"managed-lifecycle-{ordinal}",
                hash_token(f"discarded-{ordinal}"),
                operator_scope,
                operator_label,
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO runtime_sessions(
                id, principal_id, adapter, endpoint, native_session_id, state,
                lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
            ) VALUES(?, ?, 'codex-app-server', '', ?, ?, ?, ?, '{}', ?, ?)
            """,
            (
                ids["runtime_id"],
                ids["principal_id"],
                native_session_id,
                runtime_state,
                utc_after(86400),
                now,
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO worker_enrollments(
                id, principal_id, runtime_session_id, state, generation, managed,
                required_tools_digest, discovered_tools_digest, protocol_version,
                heartbeat_sequence, discovered_at, heartbeat_at, lease_expires_at,
                revoked_at, created_at, updated_at
            ) VALUES(?, ?, ?, ?, 0, 1, ?, '', '', 0,
                     NULL, NULL, ?, NULL, ?, ?)
            """,
            (
                ids["enrollment_id"],
                ids["principal_id"],
                ids["runtime_id"],
                enrollment_state,
                worker_mcp_tool_contract_digest(),
                utc_after(86400),
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_specs(
                id, attachment_id, attachment_generation, principal_id,
                runtime_session_id, enrollment_id, worker_profile_id, adapter,
                workspace_ref, requested_model, effective_model,
                requested_reasoning_effort, effective_reasoning_effort,
                provider_scope_digest, catalog_target_id, state,
                policy_binding_digest, input_digest, idempotency_key,
                created_at, updated_at, stopped_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'test-profile', 'codex-app-server', ?,
                     'test-model', 'test-model', 'high', 'high', '', '', ?,
                     ?, ?, ?, ?, ?, ?)
            """,
            (
                ids["spec_id"],
                actor["_cao_attachment_id"],
                actor["_cao_attachment_generation"],
                ids["principal_id"],
                ids["runtime_id"],
                ids["enrollment_id"],
                f"workspace-{ordinal}",
                spec_state,
                policy_binding_digest,
                digest,
                f"seed-{ordinal}",
                now,
                now,
                now if spec_state != "enabled" else None,
            ),
        )
        if with_thread:
            connection.execute(
                """
                INSERT INTO managed_worker_threads(
                    id, managed_spec_id, state, generation,
                    created_at, updated_at, archived_at
                ) VALUES(?, ?, 'active', 1, ?, ?, NULL)
                """,
                (ids["thread_id"], ids["spec_id"], now, now),
            )
            connection.execute(
                """
                INSERT INTO managed_worker_thread_epochs(
                    id, thread_id, generation, runtime_session_id,
                    enrollment_id, created_at, retired_at
                ) VALUES(?, ?, 1, ?, ?, ?, NULL)
                """,
                (
                    ids["epoch_id"],
                    ids["thread_id"],
                    ids["runtime_id"],
                    ids["enrollment_id"],
                    now,
                ),
            )
    return ids


def _seeded_managed_work_assignment(ids: dict[str, str], **fields: Any) -> WorkAssignment:
    """Build an assignment for the exact generation-1 thread seeded above."""

    return WorkAssignment(
        worker_id=ids["principal_id"],
        runtime_session_id=ids["runtime_id"],
        managed_worker_thread_id=ids["thread_id"],
        managed_worker_thread_generation=1,
        **fields,
    )


def _connect_seeded_managed_runtime(service: Any, ids: dict[str, str]) -> dict[str, Any]:
    """Establish the real credential and handshake required by status requests."""

    launch = service.issue_runtime_launch_ticket(ids["runtime_id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'ready' WHERE id = ?",
        (ids["runtime_id"],),
    )
    return worker


def _revoke_seeded_runtime_credential(service: Any, ids: dict[str, str]) -> None:
    """Model a connection lost before the queued Assignment was claimed."""

    now = utc_now()
    with service.db.transaction() as connection:
        revoked = connection.execute(
            "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, "
            "updated_at = ? WHERE enrollment_id = ? AND state = 'active'",
            (now, now, ids["enrollment_id"]),
        ).rowcount
    assert revoked == 1


def _seed_historical_disconnected_status_request(
    service: Any,
    actor: dict[str, Any],
    work: dict[str, Any],
    *,
    summary: str,
    idempotency_key: str,
    response_due_seconds: int = 30,
) -> dict[str, Any]:
    """Recreate a persisted pre-contract status row without runtime authority."""

    attempt = work["current_attempt"]
    response_due_at = utc_after(response_due_seconds)
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=actor["id"],
            recipient_id=str(attempt["worker_id"]),
            kind=MessageKind.STATUS_REQUEST,
            payload={
                "action": "report_status",
                "summary": summary,
                "generation": work["generation"],
                "response_due_at": response_due_at,
            },
            work_item_id=work["id"],
            attempt_id=str(attempt["id"]),
            goal_version=int(work["goal_version"]),
            idempotency_key=f"status-request:{idempotency_key}",
        )
        service._event(
            connection,
            "work.status_requested",
            "work_item",
            work["id"],
            actor["id"],
            {
                "message_id": message["id"],
                "attempt_id": attempt["id"],
                "response_due_at": response_due_at,
            },
            causation_id=str(message["id"]),
        )
    return message


def _command(ids: dict[str, str], generation: int, key: str) -> WorkerThreadLifecycleInput:
    return WorkerThreadLifecycleInput(
        worker_thread_id=ids["thread_id"],
        expected_generation=generation,
        idempotency_key=key,
    )


def _resume_command(ids: dict[str, str], generation: int, key: str) -> ResumeWorkerThreadInput:
    return ResumeWorkerThreadInput(
        worker_thread_id=ids["thread_id"],
        expected_generation=generation,
        idempotency_key=key,
    )


def _prepare_runtime_recovery(
    system: dict[str, Any],
    actor: dict[str, Any],
    *,
    ordinal: int,
    reason: str,
    delivery_error: str = "",
    unsafe_kind: str = "",
    acquire_turn: bool = True,
) -> tuple[
    dict[str, str],
    dict[str, Any],
    dict[str, Any],
    BoundaryDispositionInput | None,
]:
    service = system["service"]
    ids = _seed_managed_thread(system, actor, ordinal=ordinal)
    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            title=f"Managed runtime recovery {ordinal}",
            objective="Recover only a proven pre-MCP Worker launch failure.",
            acceptance=["The same logical Worker advances to one fresh epoch."],
            idempotency_key=f"assign-runtime-recovery-{ordinal}",
        ),
    )
    work = service.get_work(str(instructed["task"]["work_item_id"]))
    attempt = work["current_attempt"]
    if reason == "runtime_dispatch_failed":
        launch = service.issue_runtime_launch_ticket(ids["runtime_id"])
        if unsafe_kind == "credential":
            service.exchange_runtime_launch_ticket(str(launch["ticket"]))
        elif delivery_error:
            with service.db.transaction() as connection:
                connection.execute(
                    """
                    UPDATE message_deliveries
                    SET state = 'dispatched', last_error = ?
                    WHERE recipient_id = ? AND runtime_session_id = ?
                      AND message_id IN (
                        SELECT id FROM messages
                        WHERE attempt_id = ? AND kind = 'assignment'
                      )
                    """,
                    (
                        delivery_error,
                        ids["principal_id"],
                        ids["runtime_id"],
                        attempt["id"],
                    ),
                )
    if unsafe_kind == "native":
        service.db.execute(
            "UPDATE runtime_sessions SET native_session_id = ? WHERE id = ?",
            ("provider-session-established", ids["runtime_id"]),
        )
    elif unsafe_kind == "progress":
        with service.db.transaction() as connection:
            service._message(
                connection,
                sender_id=ids["principal_id"],
                recipient_id=actor["id"],
                kind=MessageKind.PROGRESS,
                payload={"summary": "durable Worker progress"},
                work_item_id=work["id"],
                attempt_id=attempt["id"],
                goal_version=work["goal_version"],
                idempotency_key=f"unsafe-progress-{ordinal}",
            )
    service.fail_runtime_enrollment(ids["runtime_id"], reason=reason)
    recovery_boundary = service.recover_terminal_worker_attempt(ids["runtime_id"], reason=reason)
    assert recovery_boundary is not None
    failed = service.get_work(work["id"])
    boundary = next(
        item
        for item in failed["open_boundaries"]
        if item["metadata"].get("runtime_recovery") is True
    )
    request = None
    if acquire_turn:
        turn = service.acquire_reasoner_turn(
            actor,
            work["id"],
            boundary_id=boundary["id"],
            expected_generation=failed["generation"],
            idempotency_key=f"turn-runtime-recovery-{ordinal}",
        )
        request = BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=failed["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue the exact sealed task after a proven pre-MCP failure.",
            instruction="Continue the exact task on the fresh managed epoch.",
        )
    return ids, failed, boundary, request


def test_terminal_recovery_ignores_historical_work_on_the_same_runtime(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=726)
    service = system["service"]
    historical = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Historical terminal task",
            objective="Leave one terminal Work on the reusable managed runtime.",
            acceptance=["The historical Work is no longer execution-owned."],
            idempotency_key="assign-historical-terminal-task",
        ),
    )
    service.cancel_work(
        actor,
        historical["id"],
        "The historical fixture is terminal.",
        idempotency_key="cancel-historical-terminal-task",
    )
    current = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Current recoverable task",
            objective="Recover the only currently execution-owned Work.",
            acceptance=["Historical Work does not create false ownership ambiguity."],
            idempotency_key="assign-current-recoverable-task",
        ),
    )

    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_unavailable")
    recovered = service.recover_terminal_worker_attempt(
        ids["runtime_id"], reason="runtime_unavailable"
    )

    assert recovered is not None
    assert recovered["work_item_id"] == current["id"]
    assert recovered["metadata"].get("ambiguous_runtime_ownership") is not True
    assert recovered["recovery_action"] == "dispose_continue_or_correct"
    assert service.get_work(historical["id"])["state"] == "canceled"


def test_pre_submit_failure_is_recoverable_after_mcp_bootstrap(system) -> None:
    actor = _attached(system, suffix="b")
    native_thread_id = "native-pre-submit-recovery"
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=727,
        native_session_id=native_thread_id,
    )
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Recover a provably unsubmitted Assignment",
            objective="Keep MCP bootstrap evidence separate from Assignment submission.",
            acceptance=["The exact Work is safe to continue on its native thread."],
            idempotency_key="assign-pre-submit-recovery",
        ),
    )
    attempt = work["current_attempt"]
    launch = service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=attempt["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (attempt["id"],),
    )
    assert delivery is not None
    dispatch_summary = {
        "success": False,
        "state": "failed",
        "diagnostics": {
            "failure_code": "mcp_startup_timeout",
            "mcp_startup_failure_code": "mcp_startup_timeout",
            "delivery_acceptance": "not_submitted",
            "dispatch_phase": "mcp_startup",
        },
    }
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', "
            "last_error = 'runtime_dispatch_pre_submit_failed' "
            "WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], delivery["recipient_id"]),
        )
        connection.execute(
            "UPDATE runtime_sessions SET metadata_json = json_set(metadata_json, "
            "'$.last_dispatch_message_id', ?, '$.last_dispatch', json(?)) WHERE id = ?",
            (
                delivery["message_id"],
                json.dumps(dispatch_summary, separators=(",", ":")),
                ids["runtime_id"],
            ),
        )
        service._event(
            connection,
            "runtime.message_not_submitted",
            "message",
            str(delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "mcp_startup_timeout",
                "dispatch_phase": "mcp_startup",
                "binding_version": 2,
                "runtime_session_id": ids["runtime_id"],
                "delivery_generation": int(delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(ids["runtime_id"], reason="mcp_startup_timeout")
    assert (
        service.recover_terminal_worker_attempt(ids["runtime_id"], reason="runtime_dispatch_failed")
        is not None
    )

    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    evidence = waiting["current_attempt"]["assignment_delivery"]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"
    assert evidence["mcp_authority_boundary"] == "crossed"
    assert evidence["assignment_delivery_boundary"] == "not_submitted"
    assert evidence["safe_to_redeliver"] is True
    assert evidence["safety_reason"] == "assignment_not_submitted"

    actor = _revoke_and_reattach_cao(system, actor, suffix="b")
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="turn-pre-submit-recovery",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="The exact Assignment was never submitted to App Server.",
            instruction="Continue the sealed task on the same native Worker thread.",
        ),
    )
    recovered = service.get_work(work["id"])
    assert recovered["current_attempt"]["attempt_number"] == 2
    assert (
        service.get_runtime(recovered["current_attempt"]["runtime_session_id"])["native_session_id"]
        == native_thread_id
    )
    goal_packet = recovered["current_goal_revision"]["packet"]
    sealed_attachment = goal_packet["supervisor_attachment"]
    canonical_retry_packet = build_task_packet(
        goal_packet_digest_value=recovered["current_attempt"]["goal_packet_digest"],
        work_item_id=work["id"],
        goal_version=recovered["current_attempt"]["goal_version"],
        attempt_id=recovered["current_attempt"]["id"],
        attempt_number=recovered["current_attempt"]["attempt_number"],
        worker_id=recovered["current_attempt"]["worker_id"],
        runtime_session_id=recovered["current_attempt"]["runtime_session_id"],
        supervisor_attachment=sealed_attachment,
        dependencies=goal_packet.get("dependencies", []),
    )
    assert recovered["current_attempt"]["task_packet_digest"] == task_packet_digest(
        canonical_retry_packet
    )

    # A later connection epoch may also fail before App Server accepts its
    # Assignment (for example, while resuming a very long native transcript).
    # Prior recovery history must not turn that newly proven no-submit outcome
    # into ambiguous system reconciliation.
    second_attempt = recovered["current_attempt"]
    second_runtime_id = str(second_attempt["runtime_session_id"])
    with service.db.connection_scope() as connection:
        live_attachment = service._attachment_packet_tx(
            connection,
            str(recovered["supervisor_attachment_id"]),
        )
    assert live_attachment != sealed_attachment
    buggy_retry_packet = build_task_packet(
        goal_packet_digest_value=second_attempt["goal_packet_digest"],
        work_item_id=work["id"],
        goal_version=second_attempt["goal_version"],
        attempt_id=second_attempt["id"],
        attempt_number=second_attempt["attempt_number"],
        worker_id=second_attempt["worker_id"],
        runtime_session_id=second_runtime_id,
        supervisor_attachment=live_attachment,
        dependencies=goal_packet.get("dependencies", []),
    )
    buggy_task_digest = task_packet_digest(buggy_retry_packet)
    assert buggy_task_digest != second_attempt["task_packet_digest"]
    with service.db.transaction() as connection:
        assignment = connection.execute(
            "SELECT * FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
            (second_attempt["id"],),
        ).fetchone()
        assert assignment is not None
        assignment_payload = json.loads(str(assignment["payload_json"]))
        assignment_payload["task_packet_digest"] = buggy_task_digest
        assignment_message_digest = _digest(
            {
                "sender_id": str(assignment["sender_id"]),
                "recipient_ids": [ids["principal_id"]],
                "kind": str(assignment["kind"]),
                "payload": assignment_payload,
                "work_item_id": str(assignment["work_item_id"]),
                "attempt_id": str(assignment["attempt_id"]),
                "correlation_id": (
                    ""
                    if str(assignment["correlation_id"]) == str(assignment["id"])
                    else str(assignment["correlation_id"])
                ),
                "causation_id": str(assignment["causation_id"]),
                "goal_version": int(assignment["goal_version"]),
                "goal_packet_digest": str(assignment["goal_packet_digest"]),
                "task_packet_digest": buggy_task_digest,
                "runtime_session_id": None,
            }
        )
        connection.execute(
            "UPDATE attempts SET task_packet_digest = ? WHERE id = ?",
            (buggy_task_digest, second_attempt["id"]),
        )
        connection.execute(
            "UPDATE messages SET task_packet_digest = ?, payload_json = ?, "
            "payload_digest = ?, message_digest = ? WHERE id = ?",
            (
                buggy_task_digest,
                json.dumps(assignment_payload, sort_keys=True, separators=(",", ":")),
                _digest(assignment_payload),
                assignment_message_digest,
                assignment["id"],
            ),
        )
    second_launch = service.issue_runtime_launch_ticket(
        second_runtime_id,
        attempt_id=second_attempt["id"],
    )
    # A resumed native thread binds through App Server before the managed
    # Worker exchanges its ticket.  Reproduce that exact production ordering:
    # the thread-binding failure leaves one issued, unconsumed ticket.
    assert second_launch["ticket"]
    second_delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (second_attempt["id"],),
    )
    assert second_delivery is not None
    second_dispatch_summary = {
        "success": False,
        "state": "failed",
        "diagnostics": {
            "failure_code": "runtime_dispatch_failed",
            "delivery_acceptance": "not_submitted",
            "dispatch_phase": "thread_binding",
        },
    }
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', "
            "last_error = 'runtime_dispatch_pre_submit_failed' "
            "WHERE message_id = ? AND recipient_id = ?",
            (second_delivery["message_id"], second_delivery["recipient_id"]),
        )
        connection.execute(
            "UPDATE runtime_sessions SET metadata_json = json_set(metadata_json, "
            "'$.last_dispatch_message_id', ?, '$.last_dispatch', json(?)) WHERE id = ?",
            (
                second_delivery["message_id"],
                json.dumps(second_dispatch_summary, separators=(",", ":")),
                second_runtime_id,
            ),
        )
        service._event(
            connection,
            "runtime.message_not_submitted",
            "message",
            str(second_delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "runtime_dispatch_failed",
                "dispatch_phase": "thread_binding",
                "binding_version": 2,
                "runtime_session_id": second_runtime_id,
                "delivery_generation": int(second_delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(
        second_runtime_id,
        reason="runtime_dispatch_failed",
    )
    assert (
        service.recover_terminal_worker_attempt(
            second_runtime_id,
            reason="runtime_dispatch_failed",
        )
        is not None
    )
    Database(system["settings"])

    waiting_again = service.get_work(work["id"])
    second_boundary = waiting_again["open_boundaries"][0]
    second_evidence = waiting_again["current_attempt"]["assignment_delivery"]
    assert second_boundary["recovery_action"] == "dispose_continue_or_correct"
    assert second_evidence["assignment_delivery_boundary"] == "not_submitted"
    assert second_evidence["safe_to_redeliver"] is True
    assert buggy_task_digest != waiting_again["current_attempt"]["task_packet_digest"]
    assert (
        second_attempt["task_packet_digest"]
        == waiting_again["current_attempt"]["task_packet_digest"]
    )
    repair_event = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'attempt.task_packet_binding_repaired' "
        "AND aggregate_id = ?",
        (second_attempt["id"],),
    )
    assert repair_event is not None
    second_turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=second_boundary["id"],
        expected_generation=waiting_again["generation"],
        idempotency_key="turn-repeated-pre-submit-recovery",
    )
    service.dispose_boundary(
        actor,
        second_boundary["id"],
        BoundaryDispositionInput(
            turn_id=second_turn["id"],
            lease_token=second_turn["lease_token"],
            expected_generation=waiting_again["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="The repeated exact Assignment was also never submitted.",
            instruction="Continue the sealed task on the same native Worker thread.",
        ),
    )
    recovered_again = service.get_work(work["id"])
    assert recovered_again["current_attempt"]["attempt_number"] == 3
    assert (
        service.get_runtime(recovered_again["current_attempt"]["runtime_session_id"])[
            "native_session_id"
        ]
        == native_thread_id
    )


def test_get_work_exposes_crossed_mcp_boundary_as_unsafe_to_redeliver(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=727)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Unknown post-MCP delivery",
            objective="Expose the exact recovery safety facts to the supervisor.",
            acceptance=["The supervisor cannot mistake crossed MCP for pre-MCP failure."],
            idempotency_key="assign-unknown-post-mcp-delivery",
        ),
    )
    attempt = work["current_attempt"]
    launch = service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=attempt["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (attempt["id"],),
    )
    assert delivery is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dispatched', "
            "last_error = 'runtime_dispatch_failed' "
            "WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], delivery["recipient_id"]),
        )
        service._event(
            connection,
            "runtime.message_delivery_unknown",
            "message",
            str(delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "runtime_dispatch_failed",
                "binding_version": 2,
                "runtime_session_id": ids["runtime_id"],
                "delivery_generation": int(delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_dispatch_failed")
    assert (
        service.recover_terminal_worker_attempt(ids["runtime_id"], reason="runtime_dispatch_failed")
        is not None
    )

    evidence = service.get_work(work["id"])["current_attempt"]["assignment_delivery"]
    assert evidence == {
        "state": "dispatched",
        "outcome": "unknown",
        "mcp_authority_boundary": "crossed",
        "launch_ticket_consumed": True,
        "credential_issued": True,
        "mcp_tools_discovered": True,
        "heartbeat_observed": True,
        "worker_report_observed": False,
        "assignment_delivery_boundary": "not_observed",
        "safe_to_redeliver": False,
        "safe_to_reconcile": False,
        "recovery_action": "system_reconciliation",
        "safety_reason": "unknown_after_mcp_authority",
    }
    projected = MCPServer(service).call_tool(
        actor,
        "cao_get_work",
        {"work_item_id": work["id"]},
    )
    assert projected["current_attempt"]["assignment_delivery"] == evidence


def test_post_mcp_unknown_delivery_reconciles_on_same_work_and_native_thread(system) -> None:
    actor = _attached(system, suffix="c")
    native_thread_id = "native-post-mcp-reconciliation"
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=728,
        native_session_id=native_thread_id,
    )
    service = system["service"]
    historical = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Previously completed Work awaiting requester acceptance",
            objective="Preserve a settled historical result without blocking later Work.",
            acceptance=["The completed result remains available for requester acceptance."],
            idempotency_key="assign-historical-waiting-user",
        ),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'completed' WHERE id = ?",
            (historical["current_attempt"]["id"],),
        )
        connection.execute(
            "UPDATE work_items SET state = 'waiting_user', attention_owner = 'user' WHERE id = ?",
            (historical["id"],),
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', "
            "last_error = 'historical_result_already_settled' "
            "WHERE message_id IN (SELECT id FROM messages WHERE attempt_id = ?)",
            (historical["current_attempt"]["id"],),
        )
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Recover an unknown post-MCP Assignment",
            objective="Reconcile the current sealed task without replacing its logical Worker.",
            acceptance=["The same Work and native Worker thread receive one fenced continuation."],
            idempotency_key="assign-post-mcp-reconciliation",
        ),
    )
    source_attempt = work["current_attempt"]
    launch = service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=source_attempt["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (source_attempt["id"],),
    )
    assert delivery is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dispatched', "
            "last_error = 'runtime_dispatch_failed' "
            "WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], delivery["recipient_id"]),
        )
        service._event(
            connection,
            "runtime.message_delivery_unknown",
            "message",
            str(delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "runtime_dispatch_failed",
                "binding_version": 2,
                "runtime_session_id": ids["runtime_id"],
                "delivery_generation": int(delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_dispatch_failed")
    assert (
        service.recover_terminal_worker_attempt(ids["runtime_id"], reason="runtime_dispatch_failed")
        is not None
    )

    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    assert boundary["recovery_action"] == "reconcile_continue_same_thread"
    original_notification = service.db.fetchone(
        "SELECT message.id FROM messages AS message "
        "JOIN message_deliveries AS delivery ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'system' "
        "AND delivery.recipient_id = ? ORDER BY message.sequence LIMIT 1",
        (source_attempt["id"], actor["id"]),
    )
    assert original_notification is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE boundaries SET recovery_action = 'system_reconciliation', "
            "metadata_json = json_set(metadata_json, "
            "'$.ambiguous_runtime_ownership', json('true')) WHERE id = ?",
            (boundary["id"],),
        )
        connection.execute(
            "UPDATE attempts SET stage = 'system_reconciliation', "
            "next_boundary = 'system_reconciliation' WHERE id = ?",
            (source_attempt["id"],),
        )
    service.acknowledge(actor, AckInput(message_ids=[original_notification["id"]]))
    with pytest.raises(ConflictError, match="must be disposed"):
        service.mark_message_handled(
            actor,
            str(original_notification["id"]),
            evidence="Observed the prior non-executable system-reconciliation state.",
        )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', handled_at = updated_at "
            "WHERE message_id = ? AND recipient_id = ?",
            (original_notification["id"], actor["id"]),
        )
    service.db.initialize()
    restarted = ControlPlane(service.db, system["settings"])
    restarted = ControlPlane(service.db, system["settings"])
    ready_notifications = service.db.fetchall(
        "SELECT message.id, delivery.state FROM messages AS message "
        "JOIN message_deliveries AS delivery ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'system' "
        "AND json_extract(message.payload_json, '$.action') = "
        "'reconcile_continue_same_thread'",
        (source_attempt["id"],),
    )
    assert len(ready_notifications) == 1
    assert ready_notifications[0]["state"] == "queued"
    service = restarted
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="turn-post-mcp-reconciliation",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Reconcile the exact unknown Assignment on its original logical thread.",
            instruction=(
                "Inspect the current task and workspace state first. Report completion if it "
                "already happened; otherwise continue only the unmet acceptance conditions."
            ),
        ),
    )

    recovered = service.get_work(work["id"])
    successor = recovered["current_attempt"]
    assert recovered["id"] == work["id"]
    assert recovered["managed_worker_thread_id"] == ids["thread_id"]
    assert service.get_work(historical["id"])["state"] == "waiting_user"
    assert successor["attempt_number"] == 2
    assert successor["id"] != source_attempt["id"]
    assert service.get_runtime(successor["runtime_session_id"])["native_session_id"] == (
        native_thread_id
    )
    epochs = service.db.fetchall(
        "SELECT connection_generation FROM managed_worker_thread_epochs "
        "WHERE thread_id = ? ORDER BY connection_generation",
        (ids["thread_id"],),
    )
    assert [int(epoch["connection_generation"]) for epoch in epochs] == [1, 2]
    source_delivery = service.db.fetchone(
        "SELECT state, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], delivery["recipient_id"]),
    )
    assert source_delivery is not None
    assert source_delivery["state"] == "dead"
    assert source_delivery["last_error"] == "runtime_reconciliation_superseded"
    successor_assignment = service.db.fetchone(
        "SELECT message.payload_json, delivery.state "
        "FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (successor["id"],),
    )
    assert successor_assignment is not None
    successor_payload = json.loads(str(successor_assignment["payload_json"]))
    assert successor_payload["command"]["action"] == "reconcile_continue"
    assert successor_assignment["state"] == "queued"


def _assert_system_reconciliation_acquire_only_leases_decision(
    system: dict[str, Any],
    actor: dict[str, Any],
    *,
    ids: dict[str, str],
    failed: dict[str, Any],
    boundary: dict[str, Any],
    idempotency_key: str,
) -> None:
    service = system["service"]

    def counts() -> tuple[int, int, int, int, int]:
        work = service.db.fetchone(
            "SELECT generation FROM work_items WHERE id = ?", (failed["id"],)
        )
        assert work is not None
        return (
            int(
                service.db.fetchone(
                    "SELECT COUNT(*) AS count FROM reasoner_turns WHERE work_item_id = ?",
                    (failed["id"],),
                )["count"]
            ),
            int(
                service.db.fetchone(
                    "SELECT COUNT(*) AS count FROM boundary_dispositions WHERE boundary_id = ?",
                    (boundary["id"],),
                )["count"]
            ),
            int(
                service.db.fetchone(
                    "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs "
                    "WHERE thread_id = ?",
                    (ids["thread_id"],),
                )["count"]
            ),
            int(
                service.db.fetchone(
                    "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                    (failed["id"],),
                )["count"]
            ),
            int(work["generation"]),
        )

    before = counts()
    turn = service.acquire_reasoner_turn(
        actor,
        failed["id"],
        boundary_id=boundary["id"],
        expected_generation=failed["generation"],
        idempotency_key=idempotency_key,
    )
    after = counts()
    assert turn["boundary_id"] == boundary["id"]
    assert after == (before[0] + 1, *before[1:])


def _rewrite_watchdog_boundary_as_v30_dead(
    system: dict[str, Any],
    *,
    boundary_id: str,
    attempt_id: str,
    worker_id: str,
) -> dict[str, str]:
    service = system["service"]
    with service.db.transaction() as connection:
        boundary = connection.execute(
            "SELECT metadata_json, input_digest FROM boundaries WHERE id = ?",
            (boundary_id,),
        ).fetchone()
        boundary_event = connection.execute(
            """
            SELECT * FROM events
            WHERE event_type = 'boundary.recorded'
              AND json_extract(data_json, '$.boundary_id') = ?
            """,
            (boundary_id,),
        ).fetchone()
        recovery_message = connection.execute(
            """
            SELECT payload_json, payload_digest FROM messages
            WHERE attempt_id = ? AND kind = 'system'
              AND json_extract(payload_json, '$.boundary_id') = ?
            """,
            (attempt_id, boundary_id),
        ).fetchone()
        assert boundary is not None and boundary_event is not None
        assert recovery_message is not None
        sealed = {
            "metadata_json": str(boundary["metadata_json"]),
            "input_digest": str(boundary["input_digest"]),
            "payload_json": str(recovery_message["payload_json"]),
            "payload_digest": str(recovery_message["payload_digest"]),
        }
        connection.execute(
            "DELETE FROM events WHERE event_type = ? AND aggregate_id = ?",
            ("runtime.pre_dispatch_timeout_proven", boundary_id),
        )
        connection.execute("DELETE FROM events WHERE sequence = ?", (boundary_event["sequence"],))
        deliveries = connection.execute(
            """
            SELECT delivery.message_id, delivery.recipient_id
            FROM message_deliveries AS delivery
            JOIN messages AS message ON message.id = delivery.message_id
            WHERE message.attempt_id = ? AND delivery.recipient_id = ?
            """,
            (attempt_id, worker_id),
        ).fetchall()
        for delivery in deliveries:
            connection.execute(
                """
                UPDATE message_deliveries
                SET state = 'dead', lease_until = NULL, owner_token = '',
                    last_error = 'runtime_dispatch_failed'
                WHERE message_id = ? AND recipient_id = ?
                """,
                (delivery["message_id"], delivery["recipient_id"]),
            )
            service._event(
                connection,
                "message.delivery_superseded",
                "message",
                str(delivery["message_id"]),
                "",
                {"reason_code": "runtime_dispatch_failed"},
                causation_id=str(delivery["message_id"]),
            )
        service._event(
            connection,
            "boundary.recorded",
            "work_item",
            str(boundary_event["aggregate_id"]),
            str(boundary_event["actor_id"]),
            json.loads(str(boundary_event["data_json"])),
            correlation_id=str(boundary_event["correlation_id"] or ""),
            causation_id=str(boundary_event["causation_id"] or ""),
        )
    return sealed


def test_schema_31_backfills_by_spec_state_only_and_is_idempotent(system) -> None:
    actor = _attached(system, suffix="b")
    active = _seed_managed_thread(
        system,
        actor,
        ordinal=101,
        with_thread=False,
        runtime_state="missing",
        enrollment_state="failed",
    )
    stopped = _seed_managed_thread(
        system,
        actor,
        ordinal=102,
        with_thread=False,
        spec_state="stopped",
        runtime_state="ready",
        enrollment_state="ready",
    )
    preserved_tables = (
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "artifacts",
        "work_close_receipts",
        "effect_operations",
        "events",
    )
    before = {
        table: int(system["service"].db.fetchone(f"SELECT COUNT(*) AS n FROM {table}")["n"])
        for table in preserved_tables
    }
    with system["service"].db.transaction() as connection:
        connection.execute("DELETE FROM schema_migrations WHERE version = 31")
        connection.execute("UPDATE metadata SET value = '30' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 30")

    Database(system["settings"])
    rows = system["service"].db.fetchall(
        """
        SELECT spec.id AS spec_id, thread.state, thread.generation,
               epoch.runtime_session_id, epoch.enrollment_id, epoch.retired_at
        FROM managed_worker_specs AS spec
        JOIN managed_worker_threads AS thread ON thread.managed_spec_id = spec.id
        JOIN managed_worker_thread_epochs AS epoch ON epoch.thread_id = thread.id
        WHERE spec.id IN (?, ?) ORDER BY spec.id
        """,
        (active["spec_id"], stopped["spec_id"]),
    )
    by_spec = {str(row["spec_id"]): row for row in rows}
    assert by_spec[active["spec_id"]]["state"] == "active"
    assert by_spec[active["spec_id"]]["retired_at"] is None
    assert by_spec[stopped["spec_id"]]["state"] == "legacy_stopped"
    assert by_spec[stopped["spec_id"]]["retired_at"] is not None
    assert all(int(row["generation"]) == 1 for row in rows)
    assert by_spec[active["spec_id"]]["runtime_session_id"] == active["runtime_id"]
    assert by_spec[active["spec_id"]]["enrollment_id"] == active["enrollment_id"]
    after = {
        table: int(system["service"].db.fetchone(f"SELECT COUNT(*) AS n FROM {table}")["n"])
        for table in preserved_tables
    }
    assert after == {**before, "events": before["events"] + 1}
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS n FROM events WHERE event_type = 'dashboard.resync_requested'"
        )["n"]
        == 1
    )

    Database(system["settings"])
    assert (
        int(
            system["service"].db.fetchone(
                "SELECT COUNT(*) AS n FROM managed_worker_threads WHERE managed_spec_id IN (?, ?)",
                (active["spec_id"], stopped["spec_id"]),
            )["n"]
        )
        == 2
    )
    assert (
        int(
            system["service"].db.fetchone(
                "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs "
                "WHERE thread_id IN (SELECT id FROM managed_worker_threads "
                "WHERE managed_spec_id IN (?, ?))",
                (active["spec_id"], stopped["spec_id"]),
            )["n"]
        )
        == 2
    )


def test_finish_resume_preserves_native_identity_with_fresh_epoch(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=201,
        native_session_id="provider-native-session",
    )
    service = system["service"]

    finished = service.finish_worker_thread(actor, _command(ids, 1, "finish-201"))
    assert finished == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "archived",
        "thread_generation": 2,
    }
    assert (
        service.db.fetchone(
            "SELECT native_session_id FROM runtime_sessions WHERE id = ?",
            (ids["runtime_id"],),
        )["native_session_id"]
        == "provider-native-session"
    )
    archived = next(
        worker
        for worker in service.list_managed_workers(actor)
        if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert archived["thread_state"] == "archived"
    assert archived["thread_generation"] == 2

    request = _resume_command(ids, 2, "resume-201")
    resumed = service.resume_worker_thread(actor, request)
    assert {
        key: resumed[key] for key in ("worker_thread_id", "thread_state", "thread_generation")
    } == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "active",
        "thread_generation": 3,
    }
    assert "task" not in resumed
    assert service.resume_worker_thread(actor, request) == resumed
    spec = service.db.fetchone("SELECT * FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],))
    assert spec is not None
    assert spec["principal_id"] == ids["principal_id"]
    assert spec["runtime_session_id"] != ids["runtime_id"]
    assert spec["enrollment_id"] != ids["enrollment_id"]
    assert spec["state"] == "enabled"
    fresh_runtime = service.db.fetchone(
        "SELECT * FROM runtime_sessions WHERE id = ?", (spec["runtime_session_id"],)
    )
    fresh_enrollment = service.db.fetchone(
        "SELECT * FROM worker_enrollments WHERE id = ?", (spec["enrollment_id"],)
    )
    old_enrollment = service.db.fetchone(
        "SELECT * FROM worker_enrollments WHERE id = ?", (ids["enrollment_id"],)
    )
    assert fresh_runtime is not None and fresh_enrollment is not None
    assert fresh_runtime["native_session_id"] == "provider-native-session"
    assert fresh_runtime["state"] == "starting"
    assert fresh_enrollment["state"] == "awaiting_handshake"
    assert old_enrollment is not None and old_enrollment["state"] == "revoked"
    assert (
        service.db.fetchone(
            "SELECT 1 FROM runtime_enrollment_tickets WHERE enrollment_id = ?",
            (spec["enrollment_id"],),
        )
        is None
    )
    epochs = service.db.fetchall(
        "SELECT * FROM managed_worker_thread_epochs WHERE thread_id = ? ORDER BY generation",
        (ids["thread_id"],),
    )
    assert [int(epoch["generation"]) for epoch in epochs] == [1, 3]
    assert epochs[0]["retired_at"] is not None
    assert epochs[1]["retired_at"] is None
    with service.db.transaction() as connection:
        assert (
            service._bind_ready_worker_runtime_tx(
                connection,
                worker_id=ids["principal_id"],
                runtime_session_id=str(spec["runtime_session_id"]),
            )
            == spec["runtime_session_id"]
        )
    with pytest.raises(ConflictError):
        service.issue_runtime_launch_ticket(ids["runtime_id"])


def test_finish_archives_attention_worker_and_emits_dashboard_refresh_event(system) -> None:
    actor = _attached(system, suffix="finish-dashboard-refresh")
    target = _seed_managed_thread(
        system,
        actor,
        ordinal=909,
        runtime_state="failed",
        enrollment_state="failed",
    )
    independent = _seed_managed_thread(
        system,
        actor,
        ordinal=913,
        runtime_state="failed",
        enrollment_state="failed",
    )
    service = system["service"]
    dashboard = DashboardReadModel(service)
    before = dashboard.snapshot()
    assert before["operator"]["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 2,
        "inactive_workers": 0,
        "current_work_items": 0,
    }
    assert {worker["worker_label"] for worker in before["operator"]["ready"]} == {
        "Managed lifecycle Worker 909",
        "Managed lifecycle Worker 913",
    }

    finished = service.finish_worker_thread(
        actor,
        _command(target, 1, "finish-dashboard-refresh"),
    )
    assert finished == {
        "worker_thread_id": target["thread_id"],
        "thread_state": "archived",
        "thread_generation": 2,
    }

    history = dashboard.history(after=str(before["cursor"]), limit=100)
    assert isinstance(history, dict)
    assert [item["event"]["type"] for item in history["items"]] == [
        "managed_worker_thread.finished"
    ]

    async def receive_refresh_event() -> dict[str, Any]:
        stream = dashboard.stream(
            after=str(before["cursor"]),
            max_events=1,
            heartbeat_seconds=0.01,
        )
        return await anext(stream)

    refresh = asyncio.run(receive_refresh_event())
    assert refresh["event"]["event"]["type"] == "managed_worker_thread.finished"

    archived = dashboard.snapshot()["operator"]
    assert [worker["worker_label"] for worker in archived["ready"]] == [
        "Managed lifecycle Worker 913"
    ]
    assert independent["principal_id"] != target["principal_id"]
    for category in ("needs_attention", "working", "inactive_workers"):
        assert archived[category] == []
    assert all(
        worker["worker_label"] != "Managed lifecycle Worker 909"
        for category in ("needs_attention", "working", "ready", "inactive_workers")
        for worker in archived[category]
    )
    assert archived["work_items"] == []
    assert archived["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 0,
    }


@pytest.mark.parametrize(
    "operator_scope",
    ["acceptance-test", "system", "unclassified"],
)
def test_finish_dashboard_refresh_event_is_hidden_outside_production(
    system,
    operator_scope: str,
) -> None:
    actor = _attached(system, suffix=f"finish-hidden-{operator_scope}")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal={
            "acceptance-test": 910,
            "system": 912,
            "unclassified": 914,
        }[operator_scope],
        runtime_state="failed",
        enrollment_state="failed",
        operator_scope=operator_scope,
    )
    service = system["service"]
    dashboard = DashboardReadModel(service)
    cursor = str(dashboard.snapshot()["cursor"])

    service.finish_worker_thread(
        actor,
        _command(ids, 1, f"finish-hidden-{operator_scope}"),
    )

    history = dashboard.history(after=cursor, limit=100)
    assert isinstance(history, dict)
    assert history["items"] == []


def test_revoked_csc_cannot_replay_a_cached_finish_result(system) -> None:
    actor = _attached(system, suffix="finish-replay-fence")
    ids = _seed_managed_thread(system, actor, ordinal=199)
    service = system["service"]
    request = _command(ids, 1, "finish-replay-fence")

    finished = service.finish_worker_thread(actor, request)
    assert finished["thread_state"] == "archived"
    credential_id = str(actor["_cao_conversation_credential_id"])
    now = utc_now()
    service.db.execute(
        "UPDATE cao_conversation_credentials SET state = 'revoked', "
        "revoked_at = ?, updated_at = ? WHERE id = ?",
        (now, now, credential_id),
    )

    with pytest.raises(AuthorizationError):
        service.finish_worker_thread(actor, request)
    thread = service.db.fetchone(
        "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
        (ids["thread_id"],),
    )
    assert thread is not None
    assert (thread["state"], int(thread["generation"])) == ("archived", 2)


def test_resume_never_reactivates_a_retired_epoch_delivery(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=202)
    service = system["service"]
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=actor["id"],
            recipient_id=ids["principal_id"],
            kind=MessageKind.CANCEL,
            payload={"reason": "terminal historical cancellation"},
            runtime_session_id=ids["runtime_id"],
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5 "
            "WHERE message_id = ? AND recipient_id = ?",
            (message["id"], ids["principal_id"]),
        )

    service.finish_worker_thread(actor, _command(ids, 1, "finish-202"))
    service.resume_worker_thread(actor, _resume_command(ids, 2, "resume-202"))
    spec = service.db.fetchone(
        "SELECT runtime_session_id FROM managed_worker_specs WHERE id = ?",
        (ids["spec_id"],),
    )
    assert spec is not None
    with service.db.transaction() as connection:
        assert (
            service._reactivate_deliveries_tx(
                connection,
                ids["principal_id"],
                str(spec["runtime_session_id"]),
            )
            == 0
        )
    delivery = service.db.fetchone(
        "SELECT state, attempts, runtime_session_id FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (message["id"], ids["principal_id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "dead"
    assert int(delivery["attempts"]) == 5
    assert delivery["runtime_session_id"] == ids["runtime_id"]


def test_resume_does_not_rearm_legacy_null_runtime_cancel_delivery(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=220)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Legacy cancel delivery",
            objective="Keep a retired epoch cancel out of the resumed lane.",
            acceptance=["The historical cancel remains dead."],
            idempotency_key="assign-legacy-null-cancel",
        ),
    )
    canceled = service.cancel_work(
        actor,
        work["id"],
        "Settle the historical Work.",
        idempotency_key="cancel-legacy-null-cancel",
    )
    cancel_delivery = service.db.fetchone(
        """
        SELECT delivery.message_id, delivery.recipient_id
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'cancel'
        """,
        (canceled["current_attempt"]["id"],),
    )
    assert cancel_delivery is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dead', runtime_session_id = NULL "
        "WHERE message_id = ? AND recipient_id = ?",
        (cancel_delivery["message_id"], cancel_delivery["recipient_id"]),
    )
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dead' WHERE message_id IN "
        "(SELECT id FROM messages WHERE attempt_id = ?)",
        (canceled["current_attempt"]["id"],),
    )

    service.finish_worker_thread(actor, _command(ids, 1, "finish-legacy-null-cancel"))
    service.resume_worker_thread(
        actor,
        _resume_command(ids, 2, "resume-legacy-null-cancel"),
    )
    spec = service.db.fetchone(
        "SELECT runtime_session_id FROM managed_worker_specs WHERE id = ?",
        (ids["spec_id"],),
    )
    assert spec is not None
    with service.db.transaction() as connection:
        assert (
            service._reactivate_deliveries_tx(
                connection,
                ids["principal_id"],
                str(spec["runtime_session_id"]),
            )
            == 0
        )
    preserved = service.db.fetchone(
        "SELECT state, runtime_session_id FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (cancel_delivery["message_id"], cancel_delivery["recipient_id"]),
    )
    assert preserved is not None
    assert preserved["state"] == "dead"
    assert preserved["runtime_session_id"] is None


def test_finish_archives_an_active_thread_with_revoked_spec_metadata(system) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=203,
        spec_state="revoked",
    )
    service = system["service"]

    result = service.finish_worker_thread(actor, _command(ids, 1, "finish-203"))

    assert result["thread_state"] == "archived"
    assert result["thread_generation"] == 2
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],)
        )["state"]
        == "stopped"
    )
    anomaly = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
        "AND aggregate_id = ?",
        (ids["thread_id"],),
    )
    assert anomaly is not None


def test_terminal_transport_does_not_replace_logical_worker_thread(
    system,
) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=204,
        runtime_state="failed",
        enrollment_state="failed",
    )
    service = system["service"]
    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            objective="Advance only the failed transport and keep this Worker.",
            idempotency_key="terminal-transport-new-work",
        ),
    )
    assert instructed["worker_thread_id"] == ids["thread_id"]
    assert instructed["task"]["work_item_id"]
    rows = service.db.fetchall(
        "SELECT thread.id, thread.state FROM managed_worker_threads AS thread WHERE thread.id = ?",
        (ids["thread_id"],),
    )
    assert [(row["id"], row["state"]) for row in rows] == [
        (ids["thread_id"], "active"),
    ]
    listed = service.list_managed_workers(actor)
    assert sum(item.get("thread_state") == "active" for item in listed) == 1
    route = service.db.fetchone(
        "SELECT connection_generation FROM managed_worker_thread_epochs "
        "WHERE thread_id = ? AND retired_at IS NULL",
        (ids["thread_id"],),
    )
    assert route is not None and route["connection_generation"] == 2


def test_conversation_close_preserves_project_worker_until_explicit_finish_or_delete(
    system,
) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=205)
    service = system["service"]
    sibling_attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="worker-thread-lifecycle-c-sibling",
            project_digest="c" * 64,
        ),
    )
    sibling = service.authenticate(sibling_attachment["context_token"])

    result = service.close_cao_conversation(
        actor,
        CloseCAOConversationInput(idempotency_key="close-thread-conversation"),
    )
    assert result == {
        "status": "closed",
        "scope": "conversation",
        "work_items_canceled": 0,
        "managed_workers_stopped": 0,
    }
    thread = service.db.fetchone(
        "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
        (ids["thread_id"],),
    )
    assert thread is not None
    assert (thread["state"], int(thread["generation"])) == ("active", 1)
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],)
        )["state"]
        == "enabled"
    )
    close_event = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'cao.conversation_closed'"
    )
    assert close_event is not None
    event_data = json.loads(close_event["data_json"])
    assert event_data["managed_workers_stopped"] == 0
    listed = service.list_managed_workers(sibling)
    assert [item["worker_thread_id"] for item in listed] == [ids["thread_id"]]
    assert listed[0]["thread_state"] == "active"
    view = build_operator_view(build_projection(service.db).snapshot)
    assert any(item["worker_label"] == "Managed lifecycle Worker 205" for item in view["ready"])


def test_conversation_close_without_active_worker_thread_needs_no_resume_loss_ack(
    system,
) -> None:
    actor = _attached(system, suffix="e")
    result = system["service"].close_cao_conversation(
        actor,
        CloseCAOConversationInput(idempotency_key="close-without-worker-thread"),
    )
    assert result == {
        "status": "closed",
        "scope": "conversation",
        "work_items_canceled": 0,
        "managed_workers_stopped": 0,
    }


def test_finish_closes_busy_runtime_and_dead_letters_unclaimed_epoch_delivery(system) -> None:
    actor = _attached(system, suffix="d")
    busy = _seed_managed_thread(system, actor, ordinal=301)
    service = system["service"]
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE runtime_sessions SET state = 'busy' WHERE id = ?",
            (busy["runtime_id"],),
        )
    assert service.finish_worker_thread(actor, _command(busy, 1, "finish-busy")) == {
        "worker_thread_id": busy["thread_id"],
        "thread_state": "archived",
        "thread_generation": 2,
    }
    assert dict(
        service.db.fetchone(
            "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
            (busy["thread_id"],),
        )
    ) == {"state": "archived", "generation": 2}
    assert (
        service.db.fetchone(
            "SELECT state FROM worker_enrollments WHERE id = ?",
            (busy["enrollment_id"],),
        )["state"]
        == "revoked"
    )

    queued_thread = _seed_managed_thread(system, actor, ordinal=302)
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO messages(
                id, work_item_id, attempt_id, sender_id, kind, payload_json,
                payload_digest, message_digest, correlation_id, causation_id,
                idempotency_key, goal_version, goal_packet_digest,
                task_packet_digest, created_at
            ) VALUES('msg_worker_thread_queued', NULL, NULL, ?, 'system', '{}',
                     ?, ?, '', '', '', NULL, '', '', ?)
            """,
            (actor["id"], "1" * 64, "2" * 64, now),
        )
        connection.execute(
            """
            INSERT INTO message_deliveries(
                message_id, recipient_id, state, generation, attempts,
                next_attempt_at, runtime_session_id, lease_until, owner_token,
                delivered_at, acknowledged_at, handled_at, last_error,
                created_at, updated_at
            ) VALUES('msg_worker_thread_queued', ?, 'queued', 1, 0, ?, ?, NULL,
                     '', NULL, NULL, NULL, '', ?, ?)
            """,
            (
                queued_thread["principal_id"],
                now,
                queued_thread["runtime_id"],
                now,
                now,
            ),
        )
    assert service.finish_worker_thread(actor, _command(queued_thread, 1, "finish-delivery")) == {
        "worker_thread_id": queued_thread["thread_id"],
        "thread_state": "archived",
        "thread_generation": 2,
    }
    assert dict(
        service.db.fetchone(
            "SELECT state, last_error FROM message_deliveries WHERE message_id = ?",
            ("msg_worker_thread_queued",),
        )
    ) == {"state": "dead", "last_error": "worker_thread_finished"}
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_specs WHERE id = ?",
            (queued_thread["spec_id"],),
        )["state"]
        == "stopped"
    )


def test_delete_removes_only_logical_ledger_and_replays_exactly(system, monkeypatch) -> None:
    actor = _attached(system, suffix="e")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=401,
        native_session_id="native-delete-me-logically",
    )
    service = system["service"]
    monkeypatch.setattr(
        service,
        "_cleanup_runtime_launch_artifacts",
        lambda _ticket_ids: pytest.fail("logical lifecycle called filesystem cleanup"),
    )
    service.finish_worker_thread(actor, _command(ids, 1, "finish-401"))
    request = _command(ids, 2, "delete-401")
    deleted = service.delete_worker_thread(actor, request)
    assert deleted == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 3,
    }
    assert service.delete_worker_thread(actor, request) == deleted
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (ids["thread_id"],),
        )
        is None
    )
    assert all(
        worker.get("worker_thread_id") != ids["thread_id"]
        for worker in service.list_managed_workers(actor)
    )
    spec = service.db.fetchone("SELECT * FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],))
    principal = service.db.fetchone("SELECT * FROM principals WHERE id = ?", (ids["principal_id"],))
    runtime = service.db.fetchone(
        "SELECT * FROM runtime_sessions WHERE id = ?", (ids["runtime_id"],)
    )
    enrollment = service.db.fetchone(
        "SELECT * FROM worker_enrollments WHERE id = ?", (ids["enrollment_id"],)
    )
    assert spec is not None and spec["state"] == "revoked"
    assert principal is not None and not bool(principal["enabled"])
    assert principal["operator_scope"] == "unclassified"
    assert runtime is not None and runtime["state"] == "stopped"
    assert runtime["native_session_id"] == ""
    assert enrollment is not None and enrollment["state"] == "revoked"
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events WHERE event_type = 'managed_worker_thread.deleted' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )
        is not None
    )
    with pytest.raises(NotFoundError):
        service.resume_worker_thread(actor, _resume_command(ids, 3, "resume-deleted"))


def test_attached_mcp_runs_complete_worker_thread_lifecycle(system) -> None:
    actor = _attached(system, suffix="b")
    ids = _seed_managed_thread(system, actor, ordinal=599)
    service = system["service"]
    mcp = MCPServer(service)

    archived = mcp.call_tool(
        actor,
        "cao_finish_worker_thread",
        {
            "worker_thread_id": ids["thread_id"],
            "expected_generation": 1,
            "idempotency_key": "mcp-finish-thread",
        },
    )
    assert archived == {
        "worker_thread_id": ids["thread_id"],
        "state": "archived",
        "generation": 2,
    }
    listed = mcp.call_tool(actor, "cao_list_managed_workers", {})["workers"]
    assert (
        next(worker for worker in listed if worker.get("worker_thread_id") == ids["thread_id"])[
            "state"
        ]
        == "archived"
    )

    resumed = mcp.call_tool(
        actor,
        "cao_resume_worker_thread",
        {
            "worker_thread_id": ids["thread_id"],
            "expected_generation": 2,
            "idempotency_key": "mcp-resume-thread",
        },
    )
    assert resumed["state"] == "active"
    assert resumed["generation"] == 3
    assert "task" not in resumed
    instructed = mcp.call_tool(
        actor,
        "cao_instruct_worker_thread",
        {
            "worker_thread_id": ids["thread_id"],
            "expected_generation": 3,
            "idempotency_key": "mcp-instruct-resumed-thread",
            "title": "MCP resumed Work",
            "objective": "Prove a later instruction uses the resumed Worker.",
            "maturity": "defined",
            "acceptance": ["The fresh epoch receives one queued Assignment."],
        },
    )
    assert instructed["task"]["status"] == "active"
    service.cancel_work(
        actor,
        instructed["task"]["work_item_id"],
        "Settle the resumed Work before the next Finish.",
        idempotency_key="cancel-mcp-resumed-work",
    )
    current_spec = service.db.fetchone(
        "SELECT runtime_session_id FROM managed_worker_specs WHERE id = ?",
        (ids["spec_id"],),
    )
    assert current_spec is not None
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?",
        (current_spec["runtime_session_id"],),
    )
    assert service.reconcile_terminal_headless_delivery_lanes() >= 1
    archived_again = mcp.call_tool(
        actor,
        "cao_finish_worker_thread",
        {
            "worker_thread_id": ids["thread_id"],
            "expected_generation": 3,
            "idempotency_key": "mcp-finish-thread-again",
        },
    )
    assert archived_again["generation"] == 4
    request = {
        "worker_thread_id": ids["thread_id"],
        "expected_generation": 4,
        "idempotency_key": "mcp-delete-thread",
    }
    deleted = mcp.call_tool(actor, "cao_delete_worker_thread", request)
    assert deleted == {
        "worker_thread_id": ids["thread_id"],
        "state": "deleted",
        "generation": 5,
    }
    assert mcp.call_tool(actor, "cao_delete_worker_thread", request) == deleted
    assert all(
        worker.get("worker_thread_id") != ids["thread_id"]
        for worker in mcp.call_tool(actor, "cao_list_managed_workers", {})["workers"]
    )


def test_lifecycle_accepts_same_attachment_lineage_without_resealing_launch_policy(
    system,
) -> None:
    actor = _attached(system, suffix="f")
    ids = _seed_managed_thread(system, actor, ordinal=501)
    service = system["service"]
    foreign_project_actor = _attached(system, suffix="e")
    with pytest.raises(NotFoundError):
        service.finish_worker_thread(
            foreign_project_actor, _command(ids, 1, "finish-cross-project")
        )

    current_actor = _revoke_and_reattach_cao(system, actor, suffix="f")
    service.finish_worker_thread(current_actor, _command(ids, 1, "finish-reattached"))
    # Finish/Resume/Delete authorize against the monotonic attachment lineage;
    # they do not mutate the immutable launch-policy seal. A later catalog
    # ensure may atomically adopt both generation and policy digest together.
    assert int(
        service.db.fetchone(
            "SELECT attachment_generation FROM managed_worker_specs WHERE id = ?",
            (ids["spec_id"],),
        )["attachment_generation"]
    ) == int(actor["_cao_attachment_generation"])


def test_worker_thread_id_rejects_locator_like_input() -> None:
    for invalid in ("/tmp/private", "mwt_../../private", "mwt_bad\nvalue"):
        with pytest.raises(PydanticValidationError):
            WorkerThreadLifecycleInput(
                worker_thread_id=invalid,
                expected_generation=1,
                idempotency_key="invalid-thread-id",
            )


def test_runtime_recovery_atomically_advances_same_active_thread(system) -> None:
    actor = _attached(system, suffix="a")
    ids, failed, boundary, request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=701,
        reason="runtime_unavailable",
    )
    service = system["service"]

    disposition = service.dispose_boundary(actor, boundary["id"], request)
    assert service.dispose_boundary(actor, boundary["id"], request) == disposition
    current = service.get_work(failed["id"])
    spec = service.db.fetchone("SELECT * FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],))
    assert spec is not None
    assert spec["principal_id"] == ids["principal_id"]
    assert spec["runtime_session_id"] != ids["runtime_id"]
    assert spec["enrollment_id"] != ids["enrollment_id"]
    assert current["current_attempt"]["attempt_number"] == 2
    assert current["current_attempt"]["runtime_session_id"] == spec["runtime_session_id"]
    assert current["current_attempt"]["worker_id"] == ids["principal_id"]
    assert current["state"] == "active"

    epochs = service.db.fetchall(
        "SELECT * FROM managed_worker_thread_epochs WHERE thread_id = ? "
        "ORDER BY generation, connection_generation",
        (ids["thread_id"],),
    )
    assert [int(epoch["generation"]) for epoch in epochs] == [1, 1]
    assert [int(epoch["connection_generation"]) for epoch in epochs] == [1, 2]
    assert epochs[0]["retired_at"] is not None
    assert epochs[1]["retired_at"] is None
    thread = service.db.fetchone(
        "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
        (ids["thread_id"],),
    )
    assert thread is not None
    assert (thread["state"], int(thread["generation"])) == ("active", 1)
    fresh_runtime = service.get_runtime(str(spec["runtime_session_id"]))
    assert fresh_runtime["state"] == "starting"
    assert fresh_runtime["native_session_id"] == ""
    assert fresh_runtime["enrollment"]["state"] == "awaiting_handshake"
    assert service.get_runtime(ids["runtime_id"])["state"] == "failed"
    assert service.get_runtime(ids["runtime_id"])["enrollment"]["state"] == "failed"
    assignment = service.db.fetchone(
        """
        SELECT delivery.state, delivery.runtime_session_id
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (current["current_attempt"]["id"],),
    )
    assert assignment is not None
    assert assignment["state"] == "queued"
    assert assignment["runtime_session_id"] == spec["runtime_session_id"]
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 2
    )
    service.issue_runtime_launch_ticket(
        str(spec["runtime_session_id"]),
        attempt_id=str(current["current_attempt"]["id"]),
    )
    with pytest.raises(ConflictError):
        service.issue_runtime_launch_ticket(ids["runtime_id"])


def test_overdue_connected_but_unclaimed_managed_thread_requires_reconciliation(
    system, monkeypatch
) -> None:
    actor = _attached(system, suffix="e")
    ids = _seed_managed_thread(system, actor, ordinal=704)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Overdue managed launch",
            objective="Recover one unconsumed managed launch.",
            acceptance=["A fresh epoch receives the same sealed task."],
            idempotency_key="overdue-managed-launch",
        ),
    )
    attempt = work["current_attempt"]
    _connect_seeded_managed_runtime(service, ids)
    service.request_status(
        actor,
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Confirm launch progress.",
            response_due_seconds=30,
            idempotency_key="overdue-managed-launch-status",
        ),
    )
    _revoke_seeded_runtime_credential(service, ids)
    service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=attempt["id"])
    with monkeypatch.context() as clock:
        clock.setattr(service_module, "utc_now", lambda: utc_after(31))
        assert service.recover_overdue_unstarted_attempts() == 1
    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    assert boundary["recovery_action"] == "system_reconciliation"
    assert service.get_runtime(ids["runtime_id"])["state"] == "failed"
    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=waiting,
        boundary=boundary,
        idempotency_key="overdue-managed-launch-system-owned",
    )


@pytest.mark.parametrize("tamper", ["boundary_input_digest", "status_payload_digest"])
def test_watchdog_recovery_tampered_sealed_digest_fails_closed(
    system, monkeypatch, tamper: str
) -> None:
    actor = _attached(system, suffix="e")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=709 if tamper == "boundary_input_digest" else 710,
    )
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Tampered watchdog proof",
            objective="Reject a corrupted sealed watchdog proof.",
            acceptance=["No reasoner lease or fresh epoch is created."],
            idempotency_key=f"tampered-watchdog-{tamper}",
        ),
    )
    attempt = work["current_attempt"]
    _seed_historical_disconnected_status_request(
        service,
        actor,
        work,
        summary="Confirm the current Assignment.",
        idempotency_key=f"tampered-watchdog-status-{tamper}",
    )
    with monkeypatch.context() as clock:
        clock.setattr(service_module, "utc_now", lambda: utc_after(31))
        assert service.recover_overdue_unstarted_attempts() == 1
    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"
    if tamper == "boundary_input_digest":
        service.db.execute(
            "UPDATE boundaries SET input_digest = ? WHERE id = ?",
            ("0" * 64, boundary["id"]),
        )
    else:
        service.db.execute(
            """
            UPDATE messages SET payload_digest = ?
            WHERE attempt_id = ? AND kind = 'status_request'
            """,
            ("0" * 64, attempt["id"]),
        )
    service.db.initialize()
    failed = service.get_work(work["id"])
    failed_boundary = failed["open_boundaries"][0]
    assert failed_boundary["recovery_action"] == "system_reconciliation"
    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=failed_boundary,
        idempotency_key=f"tampered-watchdog-no-lease-{tamper}",
    )


def test_legacy_terminal_stale_watchdog_repair_is_idempotent_and_continues(
    system, monkeypatch
) -> None:
    actor = _attached(system, suffix="7")
    ids = _seed_managed_thread(system, actor, ordinal=705)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Legacy terminal stale watchdog",
            objective="Repair one exact schema-30 watchdog residual.",
            acceptance=["A fresh epoch receives the unchanged sealed task."],
            idempotency_key="legacy-terminal-stale-watchdog",
        ),
    )
    attempt = work["current_attempt"]
    _seed_historical_disconnected_status_request(
        service,
        actor,
        work,
        summary="Confirm legacy launch progress.",
        idempotency_key="legacy-terminal-stale-watchdog-status",
    )
    service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=attempt["id"])
    status = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? "
        "AND kind = 'status_request' ORDER BY sequence DESC LIMIT 1",
        (attempt["id"],),
    )
    assert status is not None

    observed_at = utc_after(31)
    with monkeypatch.context() as clock:
        clock.setattr(service_module, "utc_now", lambda: observed_at)
        clock.setattr(database_module, "utc_now", lambda: observed_at)
        with service.db.transaction() as connection:
            deliveries = connection.execute(
                """
                SELECT delivery.message_id, delivery.recipient_id
                FROM messages AS message
                JOIN message_deliveries AS delivery
                  ON delivery.message_id = message.id
                WHERE message.attempt_id = ? AND delivery.recipient_id = ?
                ORDER BY message.sequence
                """,
                (attempt["id"], ids["principal_id"]),
            ).fetchall()
            assert len(deliveries) == 2
            for delivery in deliveries:
                connection.execute(
                    """
                    UPDATE message_deliveries
                    SET state = 'dead', next_attempt_at = ?, lease_until = NULL,
                        owner_token = '', last_error = 'runtime_dispatch_failed',
                        updated_at = ?
                    WHERE message_id = ? AND recipient_id = ? AND state = 'queued'
                    """,
                    (
                        observed_at,
                        observed_at,
                        delivery["message_id"],
                        delivery["recipient_id"],
                    ),
                )
                service._event(
                    connection,
                    "message.delivery_superseded",
                    "message",
                    str(delivery["message_id"]),
                    "",
                    {"reason_code": "runtime_dispatch_failed"},
                    causation_id=str(delivery["message_id"]),
                )
            connection.execute(
                "UPDATE runtime_sessions SET state = 'failed', updated_at = ? WHERE id = ?",
                (observed_at, ids["runtime_id"]),
            )
            connection.execute(
                "UPDATE worker_enrollments SET state = 'stale', generation = 0, "
                "updated_at = ? WHERE id = ?",
                (observed_at, ids["enrollment_id"]),
            )
            connection.execute(
                "UPDATE runtime_enrollment_tickets SET state = 'revoked', "
                "updated_at = ? WHERE enrollment_id = ? AND state = 'pending'",
                (observed_at, ids["enrollment_id"]),
            )
            connection.execute(
                "UPDATE work_items SET state = 'waiting_supervisor', "
                "attention_owner = 'cao', generation = 2, updated_at = ? "
                "WHERE id = ? AND generation = 1",
                (observed_at, work["id"]),
            )
            connection.execute(
                "UPDATE attempts SET state = 'waiting_supervisor', "
                "trajectory = 'stalled', stage = 'runtime_recovery', "
                "next_boundary = 'cao_disposition', updated_at = ? WHERE id = ?",
                (observed_at, attempt["id"]),
            )
            repaired = service._record_boundary_tx(
                connection,
                actor={"id": ids["principal_id"], "role": "worker"},
                request=BoundaryInput(
                    source_event_id=(f"unstarted-status:{status['id']}:{attempt['id']}"),
                    work_item_id=work["id"],
                    attempt_id=attempt["id"],
                    expected_goal_version=work["goal_version"],
                    expected_goal_packet_digest=attempt["goal_packet_digest"],
                    expected_task_packet_digest=attempt["task_packet_digest"],
                    expected_generation=2,
                    kind="failure",
                    summary=(
                        "Managed Worker assignment remained unclaimed past its status deadline."
                    ),
                    runtime_state="failed",
                    metadata={
                        "reason": "runtime_dispatch_failed",
                        "runtime_recovery": True,
                        "pre_dispatch_timeout": True,
                    },
                ),
            )

        assert repaired["recovery_action"] == "dispose_continue_or_correct"
        enrollment = service.db.fetchone(
            "SELECT state, generation FROM worker_enrollments WHERE id = ?",
            (ids["enrollment_id"],),
        )
        assert enrollment is not None
        assert (enrollment["state"], int(enrollment["generation"])) == ("failed", 1)
        proof_count = service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'runtime.pre_dispatch_timeout_proven' "
            "AND aggregate_id = ?",
            (repaired["id"],),
        )
        assert proof_count is not None and int(proof_count["count"]) == 1

        Database(system["settings"])
        Database(system["settings"])
        repeated = service.db.fetchone(
            "SELECT state, generation FROM worker_enrollments WHERE id = ?",
            (ids["enrollment_id"],),
        )
        repeated_proof = service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'runtime.pre_dispatch_timeout_proven' "
            "AND aggregate_id = ?",
            (repaired["id"],),
        )
        assert repeated is not None
        assert (repeated["state"], int(repeated["generation"])) == ("failed", 1)
        assert repeated_proof is not None and int(repeated_proof["count"]) == 1

        waiting = service.get_work(work["id"])
        turn = service.acquire_reasoner_turn(
            actor,
            work["id"],
            boundary_id=repaired["id"],
            expected_generation=waiting["generation"],
            idempotency_key="legacy-terminal-stale-watchdog-turn",
        )
        service.dispose_boundary(
            actor,
            repaired["id"],
            BoundaryDispositionInput(
                turn_id=turn["id"],
                lease_token=turn["lease_token"],
                expected_generation=waiting["generation"],
                kind=BoundaryDispositionKind.CONTINUE,
                reason="Continue after exact legacy watchdog repair.",
                instruction="Continue the unchanged sealed task.",
            ),
        )
        continued = service.get_work(work["id"])
        assert continued["current_attempt"]["attempt_number"] == 2
        assert continued["current_attempt"]["runtime_session_id"] != ids["runtime_id"]
        epochs = service.db.fetchall(
            "SELECT generation, connection_generation, retired_at "
            "FROM managed_worker_thread_epochs WHERE thread_id = ? "
            "ORDER BY generation, connection_generation",
            (ids["thread_id"],),
        )
        assert [int(epoch["generation"]) for epoch in epochs] == [1, 1]
        assert [int(epoch["connection_generation"]) for epoch in epochs] == [1, 2]
        assert epochs[0]["retired_at"] is not None
        assert epochs[1]["retired_at"] is None


@pytest.mark.parametrize(
    ("enrollment_state", "enrollment_generation"),
    [("failed", 1), ("stale", 0)],
)
def test_v30_dead_watchdog_startup_repairs_terminal_epoch_and_continues(
    system, monkeypatch, enrollment_state: str, enrollment_generation: int
) -> None:
    actor = _attached(system, suffix="e")
    ids = _seed_managed_thread(system, actor, ordinal=706 + enrollment_generation)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Legacy watchdog recovery",
            objective="Repair one exact v30 unclaimed Assignment.",
            acceptance=["The same logical Worker advances once."],
            idempotency_key=f"legacy-watchdog-{enrollment_state}",
        ),
    )
    attempt = work["current_attempt"]
    _seed_historical_disconnected_status_request(
        service,
        actor,
        work,
        summary="Confirm the legacy Assignment launch.",
        idempotency_key=f"legacy-watchdog-status-{enrollment_state}",
    )
    service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=attempt["id"])
    with monkeypatch.context() as clock:
        clock.setattr(service_module, "utc_now", lambda: utc_after(31))
        assert service.recover_overdue_unstarted_attempts() == 1
    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    sealed = _rewrite_watchdog_boundary_as_v30_dead(
        system,
        boundary_id=str(boundary["id"]),
        attempt_id=str(attempt["id"]),
        worker_id=ids["principal_id"],
    )
    if enrollment_state == "stale":
        service.db.execute(
            """
            UPDATE worker_enrollments
            SET state = 'stale', generation = ?, revoked_at = NULL
            WHERE id = ?
            """,
            (enrollment_generation, ids["enrollment_id"]),
        )

    service.db.initialize()
    service.db.initialize()

    repaired = service.get_work(work["id"])
    repaired_boundary = repaired["open_boundaries"][0]
    assert repaired_boundary["recovery_action"] == "dispose_continue_or_correct"
    lifecycle = service.db.fetchone(
        """
        SELECT runtime.state AS runtime_state, enrollment.state AS enrollment_state,
               enrollment.generation AS enrollment_generation
        FROM runtime_sessions AS runtime
        JOIN worker_enrollments AS enrollment
          ON enrollment.runtime_session_id = runtime.id
        WHERE runtime.id = ?
        """,
        (ids["runtime_id"],),
    )
    assert lifecycle is not None
    assert lifecycle["runtime_state"] == "failed"
    assert lifecycle["enrollment_state"] == "failed"
    assert int(lifecycle["enrollment_generation"]) == 1
    proof_count = service.db.fetchone(
        """
        SELECT COUNT(*) AS count FROM events
        WHERE event_type = 'runtime.pre_dispatch_timeout_proven'
          AND aggregate_id = ?
        """,
        (boundary["id"],),
    )
    assert proof_count is not None and int(proof_count["count"]) == 1
    persisted_boundary = service.db.fetchone(
        "SELECT metadata_json, input_digest FROM boundaries WHERE id = ?",
        (boundary["id"],),
    )
    persisted_message = service.db.fetchone(
        """
        SELECT payload_json, payload_digest FROM messages
        WHERE attempt_id = ? AND kind = 'system'
          AND json_extract(payload_json, '$.boundary_id') = ?
        """,
        (attempt["id"], boundary["id"]),
    )
    assert persisted_boundary is not None and persisted_message is not None
    assert str(persisted_boundary["metadata_json"]) == sealed["metadata_json"]
    assert str(persisted_boundary["input_digest"]) == sealed["input_digest"]
    assert str(persisted_message["payload_json"]) == sealed["payload_json"]
    assert str(persisted_message["payload_digest"]) == sealed["payload_digest"]

    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=repaired["generation"],
        idempotency_key=f"legacy-watchdog-turn-{enrollment_state}",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=repaired["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue after exact legacy watchdog repair.",
            instruction="Continue the same sealed task.",
        ),
    )
    assert service.get_work(work["id"])["current_attempt"]["attempt_number"] == 2


@pytest.mark.parametrize("legacy_active", [False, True])
def test_overdue_assignment_after_prior_handshake_preserves_native_thread(
    system, monkeypatch, legacy_active: bool
) -> None:
    actor = _attached(system, suffix="e")
    ids = _seed_managed_thread(system, actor, ordinal=705)
    service = system["service"]
    prior = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Prior connected task",
            objective="Establish historical Worker authority on an earlier task.",
            acceptance=["The prior task is terminal before the next Assignment."],
            idempotency_key="prior-connected-task",
        ),
    )
    prior_attempt = prior["current_attempt"]
    launch = service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=prior_attempt["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    service.cancel_work(
        actor,
        prior["id"],
        "The historical authority fixture is complete.",
        idempotency_key="cancel-prior-connected-task",
    )
    native_locator = "native-prior-connected-thread"
    now = utc_now()
    with service.db.transaction() as connection:
        if not legacy_active:
            connection.execute(
                """
                UPDATE runtime_credentials
                SET state = 'revoked', revoked_at = ?, updated_at = ?
                WHERE enrollment_id = ? AND state = 'active'
                """,
                (now, now, ids["enrollment_id"]),
            )
        connection.execute(
            """
            UPDATE runtime_sessions
            SET state = 'waiting', native_session_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (native_locator, now, ids["runtime_id"]),
        )
        if legacy_active:
            connection.execute(
                "UPDATE runtime_sessions SET state = 'ready' WHERE id = ?",
                (ids["runtime_id"],),
            )

    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Current unclaimed task",
            objective="Recover only the current queued Assignment.",
            acceptance=["The prior native thread is resumed on a fresh epoch."],
            idempotency_key="current-unclaimed-after-history",
        ),
    )
    attempt = work["current_attempt"]
    if legacy_active:
        service.request_status(
            actor,
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Confirm the current Assignment was claimed.",
                response_due_seconds=30,
                idempotency_key="current-unclaimed-after-history-status",
            ),
        )
    else:
        _seed_historical_disconnected_status_request(
            service,
            actor,
            work,
            summary="Confirm the current Assignment was claimed.",
            idempotency_key="current-unclaimed-after-history-status",
        )
    with monkeypatch.context() as clock:
        clock.setattr(service_module, "utc_now", lambda: utc_after(31))
        assert service.recover_overdue_unstarted_attempts() == 1

    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    if legacy_active:
        assert boundary["recovery_action"] == "system_reconciliation"
        sealed = _rewrite_watchdog_boundary_as_v30_dead(
            system,
            boundary_id=str(boundary["id"]),
            attempt_id=str(attempt["id"]),
            worker_id=ids["principal_id"],
        )
        service.db.initialize()
        service.db.initialize()
        waiting = service.get_work(work["id"])
        boundary = waiting["open_boundaries"][0]
        persisted = service.db.fetchone(
            "SELECT metadata_json, input_digest FROM boundaries WHERE id = ?",
            (boundary["id"],),
        )
        assert persisted is not None
        assert str(persisted["metadata_json"]) == sealed["metadata_json"]
        assert str(persisted["input_digest"]) == sealed["input_digest"]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key=f"continue-current-unclaimed-after-history-{legacy_active}",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue the exact unclaimed task on a fresh epoch.",
            instruction="Resume the same provider-native Worker thread.",
        ),
    )
    current = service.get_work(work["id"])
    fresh_runtime_id = str(current["current_attempt"]["runtime_session_id"])
    assert current["current_attempt"]["attempt_number"] == 2
    assert fresh_runtime_id != ids["runtime_id"]
    assert service.get_runtime(ids["runtime_id"])["state"] == "failed"
    assert service.get_runtime(fresh_runtime_id)["native_session_id"] == native_locator
    epochs = service.db.fetchall(
        "SELECT generation, connection_generation "
        "FROM managed_worker_thread_epochs WHERE thread_id = ? "
        "ORDER BY generation, connection_generation",
        (ids["thread_id"],),
    )
    assert [int(epoch["generation"]) for epoch in epochs] == [1, 1]
    assert [int(epoch["connection_generation"]) for epoch in epochs] == [1, 2]
    assert attempt["attempt_number"] == 1


def test_multiple_open_recovery_boundaries_keep_only_latest_canonical(system) -> None:
    actor = _attached(system, suffix="f")
    _ids, failed, first, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=703,
        reason="runtime_unavailable",
    )
    service = system["service"]
    attempt = failed["current_attempt"]
    with service.db.transaction() as connection:
        second = service._record_boundary_tx(
            connection,
            actor={"id": attempt["worker_id"], "role": "worker"},
            request=BoundaryInput(
                source_event_id=f"second-recovery:{attempt['id']}",
                work_item_id=failed["id"],
                attempt_id=attempt["id"],
                expected_goal_version=failed["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=failed["generation"],
                kind="failure",
                summary="A later exact runtime recovery fact.",
                runtime_state="failed",
                metadata={
                    "reason": "worker_inactive_timeout",
                    "runtime_recovery": True,
                },
            ),
        )
    current = service.get_work(failed["id"])
    assert [item["id"] for item in current["open_boundaries"]] == [second["id"]]
    supersession = service.db.fetchone(
        "SELECT reason FROM boundary_supersessions WHERE boundary_id = ?",
        (first["id"],),
    )
    assert supersession is not None
    assert supersession["reason"] == "recovery_boundary_replaced"
    stale_delivery = service.db.fetchone(
        """
        SELECT delivery.state FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE json_extract(message.payload_json, '$.boundary_id') = ?
        """,
        (first["id"],),
    )
    assert stale_delivery is not None and stale_delivery["state"] == "dead"
    assert "boundary.supersession_invalid" not in {
        violation.code for violation in build_projection(service.db).violations
    }
    stale_turn = service.db.fetchone(
        "SELECT state FROM reasoner_turns WHERE boundary_id = ?",
        (first["id"],),
    )
    assert stale_turn is not None and stale_turn["state"] == "abandoned"
    turn = service.acquire_reasoner_turn(
        actor,
        failed["id"],
        boundary_id=second["id"],
        expected_generation=failed["generation"],
        idempotency_key="multi-recovery-terminal-decision",
    )
    assert turn["boundary_id"] == second["id"]


def test_schema31_startup_retires_delivery_for_existing_recovery_supersession(
    system,
) -> None:
    actor = _attached(system, suffix="9")
    _ids, failed, first, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=704,
        reason="runtime_unavailable",
    )
    service = system["service"]
    attempt = failed["current_attempt"]
    with service.db.transaction() as connection:
        service._record_boundary_tx(
            connection,
            actor={"id": attempt["worker_id"], "role": "worker"},
            request=BoundaryInput(
                source_event_id=f"schema31-second-recovery:{attempt['id']}",
                work_item_id=failed["id"],
                attempt_id=attempt["id"],
                expected_goal_version=failed["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=failed["generation"],
                kind="failure",
                summary="A later exact runtime recovery fact.",
                runtime_state="failed",
                metadata={
                    "reason": "worker_inactive_timeout",
                    "runtime_recovery": True,
                },
            ),
        )
        message = connection.execute(
            """
            SELECT message.id FROM messages AS message
            WHERE json_extract(message.payload_json, '$.boundary_id') = ?
            """,
            (first["id"],),
        ).fetchone()
        assert message is not None
        # Recreate the midpoint schema-31 state: supersession committed, but
        # its already-queued instruction and cleanup event were left behind.
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'queued', lease_until = NULL, owner_token = '',
                last_error = '', updated_at = created_at
            WHERE message_id = ? AND recipient_id = ?
            """,
            (message["id"], actor["id"]),
        )
        connection.execute(
            """
            DELETE FROM events
            WHERE event_type = 'message.delivery_superseded'
              AND aggregate_id = ? AND causation_id = ?
            """,
            (message["id"], first["id"]),
        )
        schema = connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        assert schema is not None
        assert schema["value"] == str(database_module.SCHEMA_VERSION)

    service.db.initialize()
    service.db.initialize()

    repaired = service.db.fetchone(
        """
        SELECT delivery.state FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE json_extract(message.payload_json, '$.boundary_id') = ?
        """,
        (first["id"],),
    )
    assert repaired is not None and repaired["state"] == "dead"
    events = service.db.fetchone(
        """
        SELECT COUNT(*) AS count FROM events AS event
        JOIN messages AS message ON message.id = event.aggregate_id
        WHERE event.event_type = 'message.delivery_superseded'
          AND event.causation_id = ?
          AND json_extract(message.payload_json, '$.boundary_id') = ?
        """,
        (first["id"], first["id"]),
    )
    assert events is not None and events["count"] == 1


def test_dispatcher_mcp_startup_timeout_advances_one_fresh_thread_epoch(system) -> None:
    actor = _attached(system, suffix="a")
    ids = _seed_managed_thread(system, actor, ordinal=705)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Dispatcher pre-MCP recovery",
            objective="Recover a fixed-code startup failure without replay.",
            acceptance=["The exact logical Worker advances once."],
            idempotency_key="assign-dispatcher-pre-mcp-recovery",
        ),
    )
    old_attempt_id = str(work["current_attempt"]["id"])
    raw_error = (
        "managed Codex MCP server readiness timed out: cao_control_plane; "
        "last status: raw-worker-thread-startup-sentinel"
    )

    class FailedAdapter:
        async def dispatch(self, _runtime, _message):
            return RuntimeDispatchResult(
                success=False,
                state="failed",
                output="raw-worker-thread-output-sentinel",
                error=raw_error,
                metadata={"raw": "raw-worker-thread-metadata-sentinel"},
            )

    class Registry:
        def get(self, _name):
            return FailedAdapter()

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 1
    failed = service.get_work(work["id"])
    boundary = next(
        item
        for item in failed["open_boundaries"]
        if item["metadata"].get("runtime_recovery") is True
    )
    old_delivery = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (old_attempt_id,),
    )
    assert old_delivery is not None
    assert old_delivery["state"] == "dispatched"
    assert old_delivery["last_error"] == "mcp_startup_timeout"
    exact_unknown = service.db.fetchone(
        """
        SELECT COUNT(*) AS count FROM events
        WHERE event_type = 'runtime.message_delivery_unknown'
          AND aggregate_type = 'message' AND aggregate_id = ?
          AND json_extract(data_json, '$.failure_code') = 'mcp_startup_timeout'
        """,
        (old_delivery["message_id"],),
    )
    assert exact_unknown is not None and int(exact_unknown["count"]) == 1

    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=failed["generation"],
        idempotency_key="turn-dispatcher-pre-mcp-recovery",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=failed["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue after the proven startup failure.",
            instruction="Continue the same sealed task.",
        ),
    )
    recovered = service.get_work(work["id"])
    fresh_attempt = recovered["current_attempt"]
    spec = service.db.fetchone(
        "SELECT runtime_session_id, enrollment_id FROM managed_worker_specs WHERE id = ?",
        (ids["spec_id"],),
    )
    assert spec is not None
    assert fresh_attempt["id"] != old_attempt_id
    assert fresh_attempt["runtime_session_id"] == spec["runtime_session_id"]
    assert service.get_runtime(str(spec["runtime_session_id"]))["state"] == "starting"
    assert (
        service.db.fetchone(
            "SELECT state FROM worker_enrollments WHERE id = ?",
            (spec["enrollment_id"],),
        )["state"]
        == "awaiting_handshake"
    )
    epochs = service.db.fetchall(
        "SELECT generation, connection_generation, retired_at "
        "FROM managed_worker_thread_epochs WHERE thread_id = ? "
        "ORDER BY generation, connection_generation",
        (ids["thread_id"],),
    )
    assert [int(epoch["generation"]) for epoch in epochs] == [1, 1]
    assert [int(epoch["connection_generation"]) for epoch in epochs] == [1, 2]
    assert epochs[0]["retired_at"] is not None and epochs[1]["retired_at"] is None
    assert (
        service.db.fetchone(
            "SELECT state, last_error FROM message_deliveries "
            "WHERE message_id = ? AND recipient_id = ?",
            (old_delivery["message_id"], old_delivery["recipient_id"]),
        )["state"]
        == "dead"
    )
    fresh_assignment = service.db.fetchone(
        """
        SELECT delivery.state FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (fresh_attempt["id"],),
    )
    assert fresh_assignment is not None and fresh_assignment["state"] == "queued"
    for sentinel in (
        "raw-worker-thread-startup-sentinel",
        "raw-worker-thread-output-sentinel",
        "raw-worker-thread-metadata-sentinel",
    ):
        for table, column in (
            ("runtime_sessions", "metadata_json"),
            ("events", "data_json"),
            ("message_deliveries", "last_error"),
        ):
            assert (
                int(
                    service.db.fetchone(
                        f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
                        (f"%{sentinel}%",),
                    )["count"]
                )
                == 0
            )


@pytest.mark.parametrize(
    ("delivery_error", "unknown_event_count"),
    [
        ("mcp_startup_timeout", 0),
        ("runtime_provider_rate_limited", 1),
        ("mcp_startup_timeout", 2),
    ],
)
def test_runtime_recovery_rejects_missing_nonclosed_or_duplicate_unknown_event(
    system, delivery_error: str, unknown_event_count: int
) -> None:
    actor = _attached(system, suffix="a")
    ordinal = 706 + unknown_event_count
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=ordinal,
        reason="runtime_dispatch_failed",
        delivery_error=delivery_error,
        acquire_turn=False,
    )
    service = system["service"]
    delivery = service.db.fetchone(
        """
        SELECT delivery.message_id FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (failed["current_attempt"]["id"],),
    )
    assert delivery is not None
    if unknown_event_count:
        with service.db.transaction() as connection:
            for _ in range(unknown_event_count):
                service._event(
                    connection,
                    "runtime.message_delivery_unknown",
                    "message",
                    str(delivery["message_id"]),
                    "",
                    {
                        "recipient_id": ids["principal_id"],
                        "failure_code": delivery_error,
                    },
                )

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key=f"unsafe-unknown-event-{ordinal}",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 1
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                (failed["id"],),
            )["count"]
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_runtime_recovery_does_not_treat_dead_as_proven_non_delivery(system) -> None:
    actor = _attached(system, suffix="a")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=715,
        reason="runtime_dispatch_failed",
        delivery_error="mcp_startup_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    service.db.execute(
        """
        UPDATE message_deliveries SET state = 'dead'
        WHERE message_id IN (
            SELECT id FROM messages
            WHERE attempt_id = ? AND kind = 'assignment'
        ) AND recipient_id = ?
        """,
        (failed["current_attempt"]["id"], ids["principal_id"]),
    )

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key="unsafe-dead-delivery",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_runtime_recovery_requires_one_exact_assignment_delivery(system) -> None:
    actor = _attached(system, suffix="a")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=721,
        reason="runtime_unavailable",
        acquire_turn=False,
    )
    service = system["service"]
    service.db.execute(
        """
        DELETE FROM message_deliveries
        WHERE message_id IN (
            SELECT id FROM messages
            WHERE attempt_id = ? AND kind = 'assignment'
        ) AND recipient_id = ?
        """,
        (failed["current_attempt"]["id"], ids["principal_id"]),
    )

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key="unsafe-missing-assignment-delivery",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 1
    )


@pytest.mark.parametrize("mismatch", ["recipient", "runtime", "generation"])
def test_runtime_recovery_unknown_event_is_bound_to_exact_delivery_epoch(
    system, mismatch: str
) -> None:
    actor = _attached(system, suffix="a")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal={"recipient": 716, "runtime": 717, "generation": 718}[mismatch],
        reason="runtime_dispatch_failed",
        delivery_error="mcp_startup_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    delivery = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (failed["current_attempt"]["id"],),
    )
    assert delivery is not None
    event_data = {
        "recipient_id": ids["principal_id"],
        "failure_code": "mcp_startup_timeout",
        "binding_version": 2,
        "runtime_session_id": ids["runtime_id"],
        "delivery_generation": int(delivery["generation"]),
    }
    if mismatch == "recipient":
        event_data["recipient_id"] = "prn_wrong_delivery_recipient"
    elif mismatch == "runtime":
        event_data["runtime_session_id"] = "run_wrong_delivery_epoch"
    else:
        event_data["delivery_generation"] = int(delivery["generation"]) + 1
    with service.db.transaction() as connection:
        service._event(
            connection,
            "runtime.message_delivery_unknown",
            "message",
            str(delivery["message_id"]),
            "",
            event_data,
        )

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key=f"unsafe-delivery-binding-{mismatch}",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 1
    )


@pytest.mark.parametrize("unsafe_kind", ["native", "credential", "progress"])
def test_runtime_recovery_unknown_outcome_is_atomic(system, unsafe_kind: str) -> None:
    actor = _attached(system, suffix="b")
    ordinal = {"native": 711, "credential": 712, "progress": 713}[unsafe_kind]
    reason = "runtime_dispatch_failed" if unsafe_kind == "credential" else "runtime_unavailable"
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=ordinal,
        reason=reason,
        unsafe_kind=unsafe_kind,
        acquire_turn=False,
    )
    service = system["service"]

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key=f"unsafe-runtime-evidence-{unsafe_kind}",
    )
    assert (
        service.db.fetchone(
            "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
            (ids["thread_id"],),
        )["generation"]
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT runtime_session_id, enrollment_id FROM managed_worker_specs WHERE id = ?",
            (ids["spec_id"],),
        )["runtime_session_id"]
        == ids["runtime_id"]
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                (failed["id"],),
            )["count"]
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_runtime_recovery_rejects_a_sibling_unsettled_work_atomically(system) -> None:
    actor = _attached(system, suffix="b")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=714,
        reason="runtime_unavailable",
        acquire_turn=False,
    )
    service = system["service"]
    # Schema 31 admission prevents this shape. Preserve a fail-closed recovery
    # fence for an upgraded/corrupted historical database that already has it.
    with service.db.transaction() as connection:
        source = connection.execute(
            "SELECT * FROM work_items WHERE id = ?", (failed["id"],)
        ).fetchone()
        assert source is not None
        values = dict(source)
        values["id"] = "wrk_historical_sibling_unsettled"
        values["title"] = "Historical sibling Work"
        columns = list(values)
        connection.execute(
            f"INSERT INTO work_items({', '.join(columns)}) "
            f"VALUES({', '.join('?' for _ in columns)})",
            tuple(values[column] for column in columns),
        )
        source_goal = connection.execute(
            "SELECT * FROM goal_revisions WHERE work_item_id = ? AND version = ?",
            (failed["id"], failed["goal_version"]),
        ).fetchone()
        assert source_goal is not None
        goal_values = dict(source_goal)
        goal_values["work_item_id"] = values["id"]
        goal_columns = list(goal_values)
        connection.execute(
            f"INSERT INTO goal_revisions({', '.join(goal_columns)}) "
            f"VALUES({', '.join('?' for _ in goal_columns)})",
            tuple(goal_values[column] for column in goal_columns),
        )

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key="unsafe-sibling-work",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 1
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                (failed["id"],),
            )["count"]
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_managed_thread_admission_queues_a_second_unsettled_work(system) -> None:
    actor = _attached(system, suffix="b")
    ids = _seed_managed_thread(system, actor, ordinal=719)
    service = system["service"]
    first = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="First managed Work",
            objective="Remain the first queued Work on this logical thread.",
            acceptance=["A later Work is durably recorded without replacing this one."],
            idempotency_key="single-work-admission-first",
        ),
    )
    before_receipts = int(
        service.db.fetchone("SELECT COUNT(*) AS count FROM source_receipts")["count"]
    )

    second = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Second managed Work",
            objective="Remain durable behind the earlier Work on this Worker.",
            acceptance=["Both Work records remain independently reviewable."],
            idempotency_key="single-work-admission-second",
        ),
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM work_items WHERE assigned_worker_id = ?",
                (ids["principal_id"],),
            )["count"]
        )
        == 2
    )
    assert service.get_work(first["id"])["state"] == "active"
    assert service.get_work(second["id"])["state"] == "active"
    assert (
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM source_receipts")["count"])
        == before_receipts + 1
    )


def test_runtime_recovery_rolls_back_epoch_rotation_if_retry_creation_fails(
    system, monkeypatch
) -> None:
    actor = _attached(system, suffix="b")
    ids, failed, boundary, request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=715,
        reason="runtime_unavailable",
    )
    service = system["service"]
    before_runtimes = int(
        service.db.fetchone("SELECT COUNT(*) AS count FROM runtime_sessions")["count"]
    )

    def fail_retry(*_args, **_kwargs):
        raise RuntimeError("injected retry creation failure")

    monkeypatch.setattr(service, "_retry_attempt_tx", fail_retry)
    with pytest.raises(RuntimeError, match="injected retry creation failure"):
        service.dispose_boundary(actor, boundary["id"], request)

    thread = service.db.fetchone(
        "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
        (ids["thread_id"],),
    )
    spec = service.db.fetchone(
        "SELECT runtime_session_id, enrollment_id FROM managed_worker_specs WHERE id = ?",
        (ids["spec_id"],),
    )
    assert thread is not None and (thread["state"], int(thread["generation"])) == (
        "active",
        1,
    )
    assert spec is not None
    assert spec["runtime_session_id"] == ids["runtime_id"]
    assert spec["enrollment_id"] == ids["enrollment_id"]
    assert (
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM runtime_sessions")["count"])
        == before_runtimes
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                (failed["id"],),
            )["count"]
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events "
            "WHERE event_type = 'managed_worker_thread.recovery_epoch_advanced' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )
        is None
    )


def test_runtime_recovery_disposition_cannot_transfer_system_fault(system) -> None:
    actor = _attached(system, suffix="c")
    ids, _failed, boundary, request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=721,
        reason="runtime_unavailable",
    )
    service = system["service"]
    invalid = request.model_copy(update={"kind": BoundaryDispositionKind.ACCEPT})

    with pytest.raises(ConflictError) as owned:
        service.dispose_boundary(actor, boundary["id"], invalid)
    assert owned.value.details == {"reason_code": "system_recovery_owned"}
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT generation FROM managed_worker_threads WHERE id = ?",
                (ids["thread_id"],),
            )["generation"]
        )
        == 1
    )


@pytest.mark.parametrize(
    "reason",
    ["worker_inactive_timeout", "runtime_provider_rate_limited"],
)
def test_non_pre_mcp_runtime_boundaries_cannot_requeue_failed_runtime(system, reason: str) -> None:
    actor = _attached(system, suffix="c")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=722 if reason == "worker_inactive_timeout" else 723,
        reason=reason,
        acquire_turn=False,
    )
    service = system["service"]

    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=failed,
        boundary=boundary,
        idempotency_key=f"unsafe-non-pre-mcp-{reason}",
    )
    current = service.get_work(failed["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["current_attempt"]["id"] == failed["current_attempt"]["id"]
    assert current["current_attempt"]["stage"] == "system_reconciliation"
    assert current["current_attempt"]["next_boundary"] == "system_reconciliation"
    inbox = MCPServer(service).call_tool(
        actor,
        "cao_get_inbox",
        {"include_acknowledged": True},
    )
    recovery_message = next(
        item for item in inbox["items"] if item["payload"].get("boundary_id") == boundary["id"]
    )
    assert recovery_message["recovery_action"] == "system_reconciliation"
    assert "recovery_action" not in recovery_message["payload"]
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT generation FROM managed_worker_threads WHERE id = ?",
                (ids["thread_id"],),
            )["generation"]
        )
        == 1
    )


def test_system_reconciliation_requires_terminal_disposition_before_handling(
    system,
) -> None:
    actor = _attached(system, suffix="c")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=728,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    inbox = service.get_inbox(actor, include_acknowledged=True)["items"]
    recovery_message = next(
        item for item in inbox if item["payload"].get("boundary_id") == boundary["id"]
    )
    trailing_message = service.send_message(
        actor,
        [actor["id"]],
        kind=MessageKind.SYSTEM,
        payload={"action": "observe_after_system_reconciliation"},
        work_item_id=failed["id"],
        attempt_id=failed["current_attempt"]["id"],
        goal_version=failed["goal_version"],
        idempotency_key="system-reconciliation-trailing-notification",
    )
    before = service.get_work(failed["id"])
    server = MCPServer(service)
    mark_handled_tool = next(
        tool for tool in server.tools_for(actor) if tool["name"] == "cao_mark_handled"
    )
    assert "cannot be handled" in mark_handled_tool["description"]
    assert "disposition or supersession" in mark_handled_tool["description"]

    server.call_tool(
        actor,
        "cao_ack",
        {"message_ids": [recovery_message["id"]]},
    )
    # Inbox visibility is independent of an older notification's semantic
    # disposition; the unresolved Boundary itself remains protected below.
    assert trailing_message["id"] in {
        item["id"] for item in server.call_tool(actor, "cao_get_inbox", {})["items"]
    }
    with pytest.raises(ConflictError, match="must be disposed"):
        server.call_tool(
            actor,
            "cao_mark_handled",
            {
                "message_id": recovery_message["id"],
                "evidence": "The exact system-owned state was read.",
            },
        )

    turn = service.acquire_reasoner_turn(
        actor,
        failed["id"],
        boundary_id=boundary["id"],
        expected_generation=failed["generation"],
        idempotency_key="terminal-system-reconciliation",
    )
    disposition = service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=failed["generation"],
            kind=BoundaryDispositionKind.FAIL,
            reason="No authoritative recovery path exists for this system failure",
        ),
    )
    assert disposition["kind"] == "fail"
    after = service.get_work(failed["id"])
    assert after["generation"] == before["generation"] + 1
    assert after["state"] == "failed"
    assert after["current_attempt"]["state"] == "failed"
    assert after["open_boundaries"] == []
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    handled = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (recovery_message["id"], actor["id"]),
    )
    assert handled is not None and handled["state"] == "handled"
    events = service.db.fetchall(
        "SELECT data_json FROM events WHERE event_type = 'boundary.disposed' AND aggregate_id = ?",
        (failed["id"],),
    )
    assert len(events) == 1
    assert json.loads(str(events[0]["data_json"])) == {
        "boundary_id": boundary["id"],
        "disposition_id": disposition["id"],
        "generation": failed["generation"],
        "kind": "fail",
    }
    assert recovery_message["id"] not in {item["id"] for item in service.get_inbox(actor)["items"]}
    handled_history = {
        item["id"]: item["delivery_state"]
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
    }
    assert handled_history[recovery_message["id"]] == "handled"
    assert trailing_message["id"] in {
        item["id"] for item in server.call_tool(actor, "cao_get_inbox", {})["items"]
    }


def test_unresolved_cao_boundary_with_legacy_handled_wake_is_rearmed_once(system) -> None:
    actor = _attached(system, suffix="d")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=729,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    original = next(
        item
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    with service.db.transaction() as connection:
        service._message(
            connection,
            sender_id=system["worker"]["id"],
            recipient_id=actor["id"],
            kind=MessageKind.SYSTEM,
            payload={
                "action": "malformed-foreign-boundary-reference",
                "boundary_id": boundary["id"],
            },
            idempotency_key="foreign-boundary-reference-must-not-fence-supervision",
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', "
            "acknowledged_at = updated_at, handled_at = updated_at "
            "WHERE message_id = ? AND recipient_id = ?",
            (original["id"], actor["id"]),
        )

    assert service.reconcile_cao_supervision_obligations() == 1
    assert service.reconcile_cao_supervision_obligations() == 0
    wake = service.db.fetchone(
        """
        SELECT message.id, delivery.state, delivery.recipient_attachment_id,
               json_extract(message.payload_json, '$.action') AS action
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.action') = 'rearm_supervision_boundary'
        """,
        (failed["id"],),
    )
    assert wake is not None
    assert dict(wake) == {
        "id": str(wake["id"]),
        "state": "queued",
        "recipient_attachment_id": actor["_cao_attachment_id"],
        "action": "rearm_supervision_boundary",
    }
    event = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'cao.supervision_wake_rearmed' AND aggregate_id = ?",
        (failed["id"],),
    )
    assert event is not None and event["count"] == 1


def test_new_authentic_connection_preserves_provider_accepted_cao_wake(system) -> None:
    actor = _attached(system, suffix="e")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=730,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    wake = next(
        item
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    before = service.db.fetchone(
        "SELECT generation, runtime_session_id FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (wake["id"], actor["id"]),
    )
    assert before is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered', delivered_at = updated_at "
        "WHERE message_id = ? AND recipient_id = ?",
        (wake["id"], actor["id"]),
    )

    renewed_actor = _attached(system, suffix="e")
    assert renewed_actor["_cao_attachment_id"] == actor["_cao_attachment_id"]
    after = service.db.fetchone(
        "SELECT state, generation, runtime_session_id, updated_at FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (wake["id"], actor["id"]),
    )
    assert after is not None
    assert after["state"] == "delivered"
    assert int(after["generation"]) == int(before["generation"])
    assert after["runtime_session_id"] == before["runtime_session_id"]
    event = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'cao.supervision_delivery_reactivated' "
        "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
        (failed["id"],),
    )
    assert event is None
    candidates = service.pending_cao_supervision_activations(updated_before=utc_after(1))
    assert candidates == [
        {
            "message_id": wake["id"],
            "message_sequence": int(wake["sequence"]),
            "delivery_generation": int(after["generation"]),
            "attachment_id": actor["_cao_attachment_id"],
            "runtime_id": before["runtime_session_id"],
            "native_thread_id": service.get_cao_attachment(actor["_cao_attachment_id"])[
                "native_thread_id"
            ],
            "updated_at": str(after["updated_at"]),
        }
    ]
    assert service.reconcile_cao_supervision_obligations() == 0


def test_system_reconciliation_notification_rejects_reattach_after_role_precheck(
    system,
    monkeypatch,
) -> None:
    actor = _attached(system, suffix="c")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=736,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    recovery_message = next(
        item
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    MCPServer(service).call_tool(
        actor,
        "cao_ack",
        {"message_ids": [recovery_message["id"]]},
    )
    before_delivery = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (recovery_message["id"], actor["id"]),
        )
    )

    outcome = _run_after_role_check_gate(
        monkeypatch,
        service=service,
        actor=actor,
        operation=lambda: service.mark_message_handled(
            actor,
            recovery_message["id"],
            evidence="The stale conversation must not handle this notification.",
        ),
        linearize_first=lambda: _revoke_and_reattach_cao(
            system,
            actor,
            suffix="c",
        ),
    )

    assert "result" not in outcome
    assert isinstance(outcome.get("error"), AuthorizationError)
    after_delivery = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (recovery_message["id"], actor["id"]),
        )
    )
    for field in (
        "message_id",
        "recipient_id",
        "recipient_attachment_id",
        "delivered_at",
        "acknowledged_at",
        "handled_at",
        "created_at",
    ):
        assert after_delivery[field] == before_delivery[field]
    assert after_delivery["state"] == "acknowledged"
    assert int(after_delivery["generation"]) == int(before_delivery["generation"])
    assert after_delivery["runtime_session_id"] == before_delivery["runtime_session_id"]
    assert after_delivery["attempts"] == before_delivery["attempts"]
    assert after_delivery["lease_until"] == before_delivery["lease_until"]
    assert after_delivery["owner_token"] == before_delivery["owner_token"]
    assert after_delivery["last_error"] == before_delivery["last_error"]
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events WHERE event_type = 'message.handled' AND aggregate_id = ?",
            (recovery_message["id"],),
        )
        is None
    )
    current = service.get_work(failed["id"])
    assert current["state"] == "waiting_supervisor"
    assert [item["id"] for item in current["open_boundaries"]] == [boundary["id"]]
    current_attachment = service.get_cao_attachment(actor["_cao_attachment_id"])
    assert current_attachment["runtime"]["id"] != before_delivery["runtime_session_id"]
    candidates = service.pending_cao_supervision_activations(updated_before=utc_after(1))
    assert candidates == [
        {
            "message_id": recovery_message["id"],
            "message_sequence": int(recovery_message["sequence"]),
            "delivery_generation": int(after_delivery["generation"]),
            "attachment_id": actor["_cao_attachment_id"],
            "runtime_id": current_attachment["runtime"]["id"],
            "native_thread_id": current_attachment["native_thread_id"],
            "updated_at": str(after_delivery["updated_at"]),
        }
    ]


@pytest.mark.parametrize("credential_kind", ["conversation", "runtime"])
def test_system_reconciliation_ack_rejects_reattach_after_role_precheck(
    system,
    monkeypatch,
    credential_kind: str,
) -> None:
    conversation_actor = _attached(system, suffix="c")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        conversation_actor,
        ordinal=740 if credential_kind == "conversation" else 741,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    actor = conversation_actor
    if credential_kind == "runtime":
        issued = service.issue_cao_runtime_launch_ticket(
            str(conversation_actor["_runtime_session_id"])
        )
        exchanged = service.exchange_cao_runtime_launch_ticket(issued["ticket"])
        actor = service.authenticate(exchanged["token"])
        assert actor.get("_cao_runtime_credential_id")
    recovery_message = next(
        item
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    before_delivery = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (recovery_message["id"], actor["id"]),
        )
    )

    outcome = _run_after_role_check_gate(
        monkeypatch,
        service=service,
        actor=actor,
        operation=lambda: service.acknowledge(
            actor,
            AckInput(message_ids=[recovery_message["id"]]),
        ),
        linearize_first=lambda: _revoke_and_reattach_cao(
            system,
            actor,
            suffix="c",
        ),
    )

    assert "result" not in outcome
    assert isinstance(outcome.get("error"), AuthorizationError)
    after_delivery = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (recovery_message["id"], actor["id"]),
        )
    )
    for field in (
        "message_id",
        "recipient_id",
        "recipient_attachment_id",
        "state",
        "delivered_at",
        "acknowledged_at",
        "handled_at",
        "created_at",
    ):
        assert after_delivery[field] == before_delivery[field]
    assert after_delivery["state"] == "queued"
    assert int(after_delivery["generation"]) == int(before_delivery["generation"]) + 1
    assert after_delivery["runtime_session_id"] != before_delivery["runtime_session_id"]
    assert after_delivery["attempts"] == 0
    assert after_delivery["lease_until"] is None
    assert after_delivery["owner_token"] == ""
    assert after_delivery["last_error"] == ""
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events WHERE event_type = 'message.acknowledged' AND aggregate_id = ?",
            (recovery_message["id"],),
        )
        is None
    )
    current = service.get_work(failed["id"])
    assert current["state"] == "waiting_supervisor"
    assert [item["id"] for item in current["open_boundaries"]] == [boundary["id"]]


@pytest.mark.parametrize(
    ("recovery_action", "ordinal"),
    [
        ("dispose_continue_or_correct", 729),
        ("reconcile_continue_same_thread", 731),
    ],
)
def test_executable_recovery_notification_still_requires_its_domain_action(
    system, recovery_action: str, ordinal: int
) -> None:
    actor = _attached(system, suffix="c")
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=ordinal,
        reason="runtime_unavailable",
        acquire_turn=False,
    )
    service = system["service"]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"
    if recovery_action != boundary["recovery_action"]:
        with service.db.transaction() as connection:
            connection.execute(
                "UPDATE boundaries SET recovery_action = ? WHERE id = ?",
                (recovery_action, boundary["id"]),
            )
    inbox = service.get_inbox(actor, include_acknowledged=True)["items"]
    recovery_message = next(
        item for item in inbox if item["payload"].get("boundary_id") == boundary["id"]
    )
    assert recovery_message["recovery_action"] == recovery_action
    service.acknowledge(actor, AckInput(message_ids=[recovery_message["id"]]))

    with pytest.raises(
        ConflictError,
        match="supervisor boundary must be disposed before its message is handled",
    ):
        service.mark_message_handled(
            actor,
            recovery_message["id"],
            evidence="No recovery transaction has committed.",
        )

    delivery = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (recovery_message["id"], actor["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "acknowledged"
    assert delivery["handled_at"] is None
    current = service.get_work(failed["id"])
    assert [item["id"] for item in current["open_boundaries"]] == [boundary["id"]]
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT generation FROM managed_worker_threads WHERE id = ?",
                (ids["thread_id"],),
            )["generation"]
        )
        == 1
    )


@pytest.mark.parametrize(
    ("copied_action", "case"),
    [
        ("recover_terminal_worker_attempt", "copied"),
        (["recover_terminal_worker_attempt"], "malformed"),
    ],
)
def test_copied_system_reconciliation_boundary_id_cannot_bypass_domain_action(
    system, copied_action: Any, case: str
) -> None:
    actor = _attached(system, suffix="c")
    _ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=730,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    original = next(
        item
        for item in service.get_inbox(actor, include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    service.acknowledge(actor, AckInput(message_ids=[original["id"]]))
    with pytest.raises(ConflictError, match="must be disposed"):
        service.mark_message_handled(
            actor,
            original["id"],
            evidence="The exact system-owned state was read and surfaced.",
        )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', handled_at = updated_at "
            "WHERE message_id = ? AND recipient_id = ?",
            (original["id"], actor["id"]),
        )
    copied = service.send_message(
        actor,
        [actor["id"]],
        kind=MessageKind.SYSTEM,
        payload={
            "action": copied_action,
            "boundary_id": boundary["id"],
            "boundary_kind": boundary["kind"],
            "generation": boundary["generation"],
        },
        work_item_id=failed["id"],
        attempt_id=failed["current_attempt"]["id"],
        goal_version=failed["goal_version"],
        idempotency_key=f"{case}-system-reconciliation-boundary-id",
    )
    service.acknowledge(actor, AckInput(message_ids=[copied["id"]]))

    with pytest.raises(
        ConflictError,
        match="supervisor boundary must be disposed before its message is handled",
    ):
        service.mark_message_handled(
            actor,
            copied["id"],
            evidence="A copied boundary identifier is not recovery authority.",
        )

    delivery = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (copied["id"], actor["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "acknowledged"
    assert delivery["handled_at"] is None
    current = service.get_work(failed["id"])
    assert [item["id"] for item in current["open_boundaries"]] == [boundary["id"]]
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_dynamic_worker_rate_limit_with_exact_pre_mcp_proof_rotates_same_thread(
    system,
) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=724)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Dynamic provider recovery",
            objective="Continue the exact task on the same logical thread.",
            acceptance=["A fresh fenced epoch is appended exactly once."],
            idempotency_key="assign-dynamic-provider-recovery",
        ),
    )
    attempt = work["current_attempt"]
    service.issue_runtime_launch_ticket(ids["runtime_id"])
    delivery = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (attempt["id"],),
    )
    assert delivery is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dispatched', "
            "last_error = 'runtime_provider_rate_limited' "
            "WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], delivery["recipient_id"]),
        )
        service._event(
            connection,
            "runtime.message_delivery_unknown",
            "message",
            str(delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "runtime_provider_rate_limited",
                "binding_version": 2,
                "runtime_session_id": ids["runtime_id"],
                "delivery_generation": int(delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_provider_rate_limited")
    recovered_boundary = service.recover_terminal_worker_attempt(
        ids["runtime_id"], reason="runtime_provider_rate_limited"
    )
    assert recovered_boundary is not None
    failed = service.get_work(work["id"])
    assert failed["current_attempt"]["stage"] == "runtime_recovery"
    assert failed["current_attempt"]["next_boundary"] == "cao_disposition"
    boundary = next(
        item
        for item in failed["open_boundaries"]
        if item["metadata"].get("runtime_recovery") is True
    )
    assert boundary["metadata"]["dynamic_thread_recovery"] is True
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=failed["generation"],
        idempotency_key="turn-dynamic-provider-recovery",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=failed["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue after exact pre-MCP provider failure.",
            instruction="Continue the same sealed task on the fresh epoch.",
        ),
    )
    current = service.get_work(work["id"])
    assert current["current_attempt"]["attempt_number"] == 2
    assert (
        int(
            service.db.fetchone(
                "SELECT generation FROM managed_worker_threads WHERE id = ?",
                (ids["thread_id"],),
            )["generation"]
        )
        == 1
    )
    epochs = service.db.fetchall(
        "SELECT generation, connection_generation FROM managed_worker_thread_epochs "
        "WHERE thread_id = ? ORDER BY generation, connection_generation",
        (ids["thread_id"],),
    )
    assert [
        (int(epoch["generation"]), int(epoch["connection_generation"])) for epoch in epochs
    ] == [(1, 1), (1, 2)]


def test_v30_dynamic_rate_limit_backfill_uses_exact_delivery_proof_without_circuit(
    system,
) -> None:
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=725)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Pre-v31 dynamic provider recovery",
            objective="Recover the exact pre-MCP task without catalog circuit state.",
            acceptance=["The migration advertises one executable same-thread action."],
            idempotency_key="assign-v30-dynamic-provider-recovery",
        ),
    )
    attempt = work["current_attempt"]
    service.issue_runtime_launch_ticket(ids["runtime_id"])
    delivery = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (attempt["id"],),
    )
    assert delivery is not None
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'dispatched', "
            "last_error = 'runtime_provider_rate_limited' "
            "WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], delivery["recipient_id"]),
        )
        service._event(
            connection,
            "runtime.message_delivery_unknown",
            "message",
            str(delivery["message_id"]),
            "",
            {
                "recipient_id": ids["principal_id"],
                "failure_code": "runtime_provider_rate_limited",
                "binding_version": 2,
                "runtime_session_id": ids["runtime_id"],
                "delivery_generation": int(delivery["generation"]),
            },
        )
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_provider_rate_limited")
    recovered = service.recover_terminal_worker_attempt(
        ids["runtime_id"], reason="runtime_provider_rate_limited"
    )
    assert recovered is not None

    boundary = service.db.fetchone("SELECT * FROM boundaries WHERE id = ?", (recovered["id"],))
    message = service.db.fetchone(
        "SELECT * FROM messages WHERE attempt_id = ? AND kind = 'system' "
        "AND json_extract(payload_json, '$.boundary_id') = ?",
        (attempt["id"], recovered["id"]),
    )
    assert boundary is not None and message is not None
    assert (
        service.db.fetchone(
            "SELECT 1 FROM provider_runtime_circuits WHERE source_boundary_id = ?",
            (recovered["id"],),
        )
        is None
    )

    # Reconstruct the exact v30 sealed rows: dynamic recovery metadata and the
    # message recovery_action field did not exist yet, and v30 deliberately
    # did not create provider circuit rows for non-catalog Workers.
    v30_metadata = {
        "reason": "runtime_provider_rate_limited",
        "runtime_recovery": True,
    }
    v30_metadata_json = json.dumps(
        v30_metadata, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    v30_boundary_digest = _digest(
        BoundaryInput(
            source_event_id=str(boundary["source_event_id"]),
            work_item_id=str(boundary["work_item_id"]),
            attempt_id=str(boundary["attempt_id"]),
            expected_goal_version=int(boundary["goal_version"]),
            expected_goal_packet_digest=str(boundary["goal_packet_digest"]),
            expected_task_packet_digest=str(boundary["task_packet_digest"]),
            expected_generation=int(boundary["generation"]),
            kind=str(boundary["kind"]),
            summary=str(boundary["summary"]),
            runtime_state=str(boundary["runtime_state"]),
            metadata=v30_metadata,
        ).model_dump(mode="json")
    )
    v30_payload = json.loads(str(message["payload_json"]))
    v30_payload.pop("recovery_action", None)
    v30_payload_json = json.dumps(
        v30_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    v30_payload_digest = _digest(v30_payload)
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE boundaries SET metadata_json = ?, input_digest = ?, "
            "recovery_action = '' WHERE id = ?",
            (v30_metadata_json, v30_boundary_digest, recovered["id"]),
        )
        connection.execute(
            "UPDATE messages SET payload_json = ?, payload_digest = ? WHERE id = ?",
            (v30_payload_json, v30_payload_digest, message["id"]),
        )
        _backfill_managed_worker_recovery_actions(connection, service.settings)

    migrated_boundary = service.db.fetchone(
        "SELECT * FROM boundaries WHERE id = ?", (recovered["id"],)
    )
    migrated_message = service.db.fetchone("SELECT * FROM messages WHERE id = ?", (message["id"],))
    migrated_attempt = service.db.fetchone(
        "SELECT stage, next_boundary FROM attempts WHERE id = ?", (attempt["id"],)
    )
    assert migrated_boundary is not None
    assert migrated_message is not None
    assert migrated_attempt is not None
    assert migrated_boundary["recovery_action"] == "dispose_continue_or_correct"
    assert migrated_attempt["stage"] == "runtime_recovery"
    assert migrated_attempt["next_boundary"] == "cao_disposition"
    assert migrated_boundary["metadata_json"] == v30_metadata_json
    assert migrated_boundary["input_digest"] == v30_boundary_digest
    assert migrated_message["payload_json"] == v30_payload_json
    assert migrated_message["payload_digest"] == v30_payload_digest
    viewed_message = next(
        item
        for item in service.get_inbox(
            actor,
            after=0,
            limit=100,
            include_acknowledged=True,
        )["items"]
        if item["id"] == message["id"]
    )
    assert viewed_message["recovery_action"] == "dispose_continue_or_correct"
    assert "recovery_action" not in viewed_message["payload"]
    assert _digest(viewed_message["payload"]) == viewed_message["payload_digest"]
    public_message = next(
        item
        for item in MCPServer(service).call_tool(
            actor,
            "cao_get_inbox",
            {"include_acknowledged": True},
        )["items"]
        if item["id"] == message["id"]
    )
    assert public_message["recovery_action"] == "dispose_continue_or_correct"
    assert "recovery_action" not in public_message["payload"]

    current = service.get_work(work["id"])
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=str(recovered["id"]),
        expected_generation=int(current["generation"]),
        idempotency_key="turn-v30-dynamic-provider-recovery",
    )
    service.dispose_boundary(
        actor,
        str(recovered["id"]),
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=int(current["generation"]),
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue after exact pre-MCP provider failure.",
            instruction="Continue the same sealed task on the fresh epoch.",
        ),
    )
    assert service.get_work(work["id"])["current_attempt"]["attempt_number"] == 2


def test_runtime_recovery_allows_only_one_fresh_epoch_per_work(system) -> None:
    actor = _attached(system, suffix="d")
    ids, failed, boundary, request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=731,
        reason="runtime_unavailable",
    )
    service = system["service"]
    service.dispose_boundary(actor, boundary["id"], request)
    first_recovery = service.get_work(failed["id"])
    fresh_runtime_id = str(first_recovery["current_attempt"]["runtime_session_id"])
    service.fail_runtime_enrollment(fresh_runtime_id, reason="runtime_unavailable")
    second_boundary = service.recover_terminal_worker_attempt(
        fresh_runtime_id, reason="runtime_unavailable"
    )
    assert second_boundary is not None
    waiting = service.get_work(failed["id"])
    assert waiting["current_attempt"]["stage"] == "system_reconciliation"
    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=waiting,
        boundary=second_boundary,
        idempotency_key="turn-runtime-recovery-exhausted",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 2
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM attempts WHERE work_item_id = ?",
                (failed["id"],),
            )["count"]
        )
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (second_boundary["id"],),
        )
        is None
    )


def test_recovery_backfill_requires_the_exact_persisted_work_thread_binding(system) -> None:
    actor = _attached(system, suffix="d")
    ids, failed, boundary, _ = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=733,
        reason="runtime_unavailable",
        acquire_turn=False,
    )
    service = system["service"]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"

    sealed_before = {
        "goal": tuple(
            service.db.fetchone(
                "SELECT packet_json, packet_digest FROM goal_revisions "
                "WHERE work_item_id = ? AND version = ?",
                (failed["id"], failed["goal_version"]),
            )
        ),
        "attempt": tuple(
            service.db.fetchone(
                "SELECT goal_packet_digest, task_packet_digest, completion_claim_json "
                "FROM attempts WHERE id = ?",
                (failed["current_attempt"]["id"],),
            )
        ),
        "boundary": tuple(
            service.db.fetchone(
                "SELECT metadata_json, input_digest, goal_packet_digest, "
                "task_packet_digest FROM boundaries WHERE id = ?",
                (boundary["id"],),
            )
        ),
        "messages": [
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, payload_json, payload_digest FROM messages "
                "WHERE attempt_id = ? ORDER BY sequence, id",
                (failed["current_attempt"]["id"],),
            )
        ],
    }

    with service.db.transaction() as connection:
        connection.execute("DROP TRIGGER work_items_managed_thread_exact_update")
        connection.execute("DROP TRIGGER work_items_managed_thread_rebind_update")
        connection.execute(
            "UPDATE work_items SET managed_worker_thread_id = NULL, "
            "managed_worker_thread_generation = NULL WHERE id = ?",
            (failed["id"],),
        )
        _backfill_managed_worker_recovery_actions(connection, service.settings)

    reconciled = service.get_work(failed["id"])
    reconciled_boundary = next(
        item for item in reconciled["open_boundaries"] if item["id"] == boundary["id"]
    )
    assert reconciled_boundary["recovery_action"] == "system_reconciliation"
    assert (
        reconciled["current_attempt"]["stage"],
        reconciled["current_attempt"]["next_boundary"],
    ) == ("system_reconciliation", "system_reconciliation")
    assert {
        "goal": tuple(
            service.db.fetchone(
                "SELECT packet_json, packet_digest FROM goal_revisions "
                "WHERE work_item_id = ? AND version = ?",
                (failed["id"], failed["goal_version"]),
            )
        ),
        "attempt": tuple(
            service.db.fetchone(
                "SELECT goal_packet_digest, task_packet_digest, completion_claim_json "
                "FROM attempts WHERE id = ?",
                (failed["current_attempt"]["id"],),
            )
        ),
        "boundary": tuple(
            service.db.fetchone(
                "SELECT metadata_json, input_digest, goal_packet_digest, "
                "task_packet_digest FROM boundaries WHERE id = ?",
                (boundary["id"],),
            )
        ),
        "messages": [
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, payload_json, payload_digest FROM messages "
                "WHERE attempt_id = ? ORDER BY sequence, id",
                (failed["current_attempt"]["id"],),
            )
        ],
    } == sealed_before
    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=reconciled,
        boundary=reconciled_boundary,
        idempotency_key="unresolved-work-thread-binding-system-owned",
    )


def test_second_failed_epoch_with_worker_activity_requires_reconciliation(system) -> None:
    actor = _attached(system, suffix="d")
    ids, failed, boundary, request = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=732,
        reason="runtime_unavailable",
    )
    service = system["service"]
    service.dispose_boundary(actor, boundary["id"], request)
    recovered = service.get_work(failed["id"])
    fresh_attempt = recovered["current_attempt"]
    fresh_runtime_id = str(fresh_attempt["runtime_session_id"])
    with service.db.transaction() as connection:
        service._message(
            connection,
            sender_id=ids["principal_id"],
            recipient_id=actor["id"],
            kind=MessageKind.PROGRESS,
            payload={"summary": "activity from the second epoch"},
            work_item_id=failed["id"],
            attempt_id=fresh_attempt["id"],
            goal_version=recovered["goal_version"],
            idempotency_key="second-epoch-progress",
        )
    service.fail_runtime_enrollment(fresh_runtime_id, reason="runtime_unavailable")
    second_boundary = service.recover_terminal_worker_attempt(
        fresh_runtime_id, reason="runtime_unavailable"
    )
    assert second_boundary is not None
    waiting = service.get_work(failed["id"])
    assert waiting["current_attempt"]["stage"] == "system_reconciliation"
    _assert_system_reconciliation_acquire_only_leases_decision(
        system,
        actor,
        ids=ids,
        failed=waiting,
        boundary=second_boundary,
        idempotency_key="turn-second-epoch-activity",
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM managed_worker_thread_epochs WHERE thread_id = ?",
                (ids["thread_id"],),
            )["count"]
        )
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (second_boundary["id"],),
        )
        is None
    )
