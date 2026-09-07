from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.errors import (
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    StaleGoalError,
)
from cao_control_plane.mcp import MCPServer, _conversation_work_projection
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    BoundaryInput,
    BoundaryKind,
    DeleteWorkerThreadInput,
    GoalMaturity,
    GoalRevision,
    InstructWorkerThreadInput,
    NewWorkerThreadInput,
    ReportInput,
    ReportKind,
    ResumeWorkerThreadInput,
    ReviewInput,
    ReviewVerdict,
    RuntimeHeartbeat,
    RuntimeState,
    StatusRequestInput,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.runtime import Dispatcher, render_message
from cao_control_plane.runtime_enrollment import ProcessIdentity
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane, _digest


def _attached(service: ControlPlane, *, suffix: str) -> dict[str, Any]:
    peer = ProcessIdentity(
        pid=1_710_000_000 + ord(suffix[0]),
        parent_pid=1,
        start_signature=f"managed-command-admission-{suffix}",
    )
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"managed-command-admission-{suffix}",
            project_digest=hashlib.sha256(suffix.encode()).hexdigest(),
        ),
        peer=peer,
    )
    return service.authenticate(str(attachment["context_token"]))


def _second_attached_connection(service: ControlPlane, *, suffix: str) -> dict[str, Any]:
    peer = ProcessIdentity(
        pid=1_710_100_000 + ord(suffix[0]),
        parent_pid=1,
        start_signature=f"managed-command-admission-{suffix}-second",
    )
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"managed-command-admission-{suffix}",
            project_digest=hashlib.sha256(suffix.encode()).hexdigest(),
        ),
        peer=peer,
    )
    return service.authenticate(str(attachment["context_token"]))


def _new_thread(
    service: ControlPlane,
    actor: dict[str, Any],
    tmp_path: Path,
    *,
    suffix: str,
) -> str:
    directory = tmp_path / f"managed-command-worker-{suffix}"
    directory.mkdir()
    created = service.new_worker_thread(
        actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            idempotency_key=f"new-{suffix}",
        ),
    )
    return str(created["worker_thread_id"])


def _thread_route(service: ControlPlane, thread_id: str) -> dict[str, Any]:
    row = service.db.fetchone(
        """
        SELECT thread.generation AS thread_generation,
               spec.principal_id, spec.runtime_session_id, spec.enrollment_id,
               epoch.connection_generation
        FROM managed_worker_threads AS thread
        JOIN managed_worker_specs AS spec ON spec.id = thread.managed_spec_id
        JOIN managed_worker_thread_epochs AS epoch
          ON epoch.thread_id = thread.id
         AND epoch.generation = thread.generation
         AND epoch.retired_at IS NULL
        WHERE thread.id = ?
        """,
        (thread_id,),
    )
    assert row is not None
    return dict(row)


def _enroll_current_runtime(service: ControlPlane, thread_id: str) -> tuple[dict[str, Any], str]:
    route = _thread_route(service, thread_id)
    launch = service.issue_runtime_launch_ticket(str(route["runtime_session_id"]))
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        str(route["runtime_session_id"]),
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'ready' WHERE id = ?",
        (route["runtime_session_id"],),
    )
    return worker, str(route["runtime_session_id"])


def _instruct(
    service: ControlPlane,
    actor: dict[str, Any],
    thread_id: str,
    *,
    suffix: str,
) -> dict[str, Any]:
    return service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=thread_id,
            expected_generation=1,
            objective="Produce the exact durable result.",
            idempotency_key=f"instruct-{suffix}",
        ),
    )


def _question(
    service: ControlPlane,
    worker: dict[str, Any],
    work_id: str,
    *,
    suffix: str,
    incorporated_message_id: str | None = None,
) -> dict[str, Any]:
    work = service.get_work(work_id)
    attempt = work["current_attempt"]
    return service.report(
        worker,
        str(attempt["id"]),
        ReportInput(
            kind=ReportKind.QUESTION,
            expected_goal_version=int(work["goal_version"]),
            expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
            expected_task_packet_digest=str(attempt["task_packet_digest"]),
            expected_generation=int(work["generation"]),
            summary="Which exact option should be used?",
            incorporated_message_ids=(
                [incorporated_message_id] if incorporated_message_id is not None else []
            ),
            idempotency_key=f"question-{suffix}",
        ),
    )


def _disconnect_with_native(
    service: ControlPlane, runtime_id: str, *, native_session_id: str
) -> None:
    service.db.execute(
        "UPDATE runtime_sessions SET native_session_id = ?, state = 'missing' WHERE id = ?",
        (native_session_id, runtime_id),
    )


def _wait_user(
    service: ControlPlane,
    actor: dict[str, Any],
    waiting: dict[str, Any],
    *,
    suffix: str,
) -> dict[str, Any]:
    boundary = waiting["open_boundaries"][-1]
    turn = service.acquire_reasoner_turn(
        actor,
        str(waiting["id"]),
        boundary_id=str(boundary["id"]),
        expected_generation=int(waiting["generation"]),
        idempotency_key=f"turn-wait-{suffix}",
    )
    service.dispose_boundary(
        actor,
        str(boundary["id"]),
        BoundaryDispositionInput(
            turn_id=str(turn["id"]),
            lease_token=str(turn["lease_token"]),
            expected_generation=int(waiting["generation"]),
            kind=BoundaryDispositionKind.WAIT_USER,
            reason="The requester owns this exact choice.",
            instruction="Choose option A or option B.",
            resume_condition="The requester supplies one exact option.",
        ),
    )
    return service.get_work(str(waiting["id"]))


def _record_retry_residue(
    service: ControlPlane,
    actor: dict[str, Any],
    work_id: str,
    *,
    idempotency_key: str,
    reason: str,
    legacy: bool = False,
) -> dict[str, Any]:
    work = service.get_work(work_id)
    attempt = work["current_attempt"]
    return service.record_boundary(
        actor,
        BoundaryInput(
            source_event_id=f"retry:{idempotency_key}",
            work_item_id=work_id,
            attempt_id=str(attempt["id"]),
            expected_goal_version=int(work["goal_version"]),
            expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
            expected_task_packet_digest=str(attempt["task_packet_digest"]),
            expected_generation=int(work["generation"]),
            kind=BoundaryKind.RETRY_REQUEST,
            summary=reason,
            runtime_state=RuntimeState.WAITING,
            metadata=(
                {
                    "worker_id": str(work["assigned_worker_id"]),
                    "runtime_session_id": attempt["runtime_session_id"],
                }
                if legacy
                else {
                    "worker_id": str(work["assigned_worker_id"]),
                    "runtime_session_id": None,
                    "managed_worker_thread_id": None,
                    "managed_worker_thread_generation": None,
                }
            ),
        ),
    )


def _offline_revised_work(
    system: dict[str, Any], tmp_path: Path, *, suffix: str
) -> tuple[ControlPlane, dict[str, Any], str, dict[str, Any], str]:
    service = system["service"]
    actor = _attached(service, suffix=suffix)
    thread_id = _new_thread(service, actor, tmp_path, suffix=suffix)
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=suffix)
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=suffix)
    boundary = waiting["open_boundaries"][-1]
    notification = next(
        item
        for item in service.get_inbox(actor, attempt_id=waiting["current_attempt"]["id"])["items"]
        if item["kind"] == ReportKind.QUESTION.value
        and item["payload"].get("boundary_id") == boundary["id"]
    )
    service.acknowledge(actor, AckInput(message_ids=[notification["id"]]))
    _wait_user(service, actor, waiting, suffix=suffix)
    native_session_id = f"native-session-{suffix}"
    _disconnect_with_native(service, runtime_id, native_session_id=native_session_id)
    MCPServer(service).call_tool(
        actor,
        "cao_revise_goal",
        {
            "work_item_id": work_id,
            "expected_version": 1,
            "objective": "Produce the revised exact durable result.",
            "maturity": GoalMaturity.DEFINED.value,
            "acceptance": ["The revised result is independently reviewable."],
            "reason": "The requester refined the exact result.",
            "idempotency_key": f"revise-{suffix}",
        },
    )
    revised = service.get_work(work_id)
    return service, actor, thread_id, revised, native_session_id


def _attempt_messages(service: ControlPlane, attempt_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in service.db.fetchall(
            "SELECT message.*, delivery.state AS delivery_state "
            "FROM messages AS message "
            "JOIN message_deliveries AS delivery ON delivery.message_id = message.id "
            "WHERE message.attempt_id = ? ORDER BY message.sequence",
            (attempt_id,),
        )
    ]


def _ledger_snapshot(
    service: ControlPlane, tables: tuple[str, ...]
) -> dict[str, list[dict[str, Any]]]:
    return {
        table: [dict(row) for row in service.db.fetchall(f"SELECT * FROM {table} ORDER BY rowid")]
        for table in tables
    }


def _seed_legacy_reply_context_and_turn(
    service: ControlPlane,
    actor: dict[str, Any],
    work_id: str,
    boundary_id: str,
    *,
    message_text: str,
    key: str,
) -> tuple[dict[str, Any], str]:
    work = service.get_work(work_id)
    request_digest = _digest(
        {
            "work_item_id": work_id,
            "message": message_text,
            "in_reply_to": None,
        }
    )
    with service.db.transaction() as connection:
        service._idempotent_put(
            connection,
            str(actor["id"]),
            "reply_context",
            f"provided:{key}",
            request_digest,
            {
                "work_item_id": work_id,
                "boundary_id": boundary_id,
                "generation": int(work["generation"]),
            },
        )
    turn = service.acquire_reasoner_turn(
        actor,
        work_id,
        boundary_id=boundary_id,
        expected_generation=int(work["generation"]),
        idempotency_key=f"reply:{key}",
    )
    return turn, request_digest


def test_offline_revision_keeps_logical_binding_and_dispatches_once_after_handshake(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service, actor, thread_id, revised, native_session_id = _offline_revised_work(
        system, tmp_path, suffix="revision"
    )
    work_id = str(revised["id"])
    current = revised["current_attempt"]
    route = _thread_route(service, thread_id)

    assert revised["managed_worker_thread_id"] == thread_id
    assert revised["managed_worker_thread_generation"] == 1
    assert (revised["goal_version"], revised["generation"]) == (2, 2)
    assert int(current["attempt_number"]) == 2
    assert str(current["runtime_session_id"]) == str(route["runtime_session_id"])
    epochs = service.db.fetchall(
        "SELECT connection_generation, retired_at FROM managed_worker_thread_epochs "
        "WHERE thread_id = ? ORDER BY connection_generation",
        (thread_id,),
    )
    assert [(int(row["connection_generation"]), row["retired_at"] is None) for row in epochs] == [
        (1, False),
        (2, True),
    ]
    runtime = service.db.fetchone(
        "SELECT native_session_id FROM runtime_sessions WHERE id = ?",
        (route["runtime_session_id"],),
    )
    assert runtime is not None and runtime["native_session_id"] == native_session_id
    replacement_messages = _attempt_messages(service, str(current["id"]))
    assert [(row["kind"], row["delivery_state"]) for row in replacement_messages] == [
        ("assignment", "queued")
    ]

    projection = MCPServer(service).call_tool(actor, "cao_get_work", {"work_item_id": work_id})
    assert projection["worker_thread_binding_state"] == "exact"
    assert projection["worker_thread_id"] == thread_id
    assert projection["worker_thread_generation"] == 1
    projected_json = json.dumps(projection, sort_keys=True)
    for private_value in (
        str(route["runtime_session_id"]),
        str(route["enrollment_id"]),
        native_session_id,
    ):
        assert private_value not in projected_json

    _enroll_current_runtime(service, thread_id)
    dispatcher = Dispatcher(service, service.settings)
    claimed = dispatcher._claim_delivery()
    assert claimed is not None and claimed["message_id"] == replacement_messages[0]["id"]
    assert dispatcher._claim_delivery() is None


@pytest.mark.parametrize(
    "tamper",
    (
        "work_pair",
        "lifecycle_generation",
        "untracked_delivery_runtime",
        "attempt_owner_mismatch",
        "disabled_spec",
        "foreign_spec_attachment",
        "spec_route_mismatch",
    ),
)
def test_dispatcher_rejects_tampered_work_binding_or_old_lifecycle_generation(
    system: dict[str, Any], tmp_path: Path, tamper: str
) -> None:
    service, _actor, thread_id, revised, _native = _offline_revised_work(
        system, tmp_path, suffix=f"tamper-{tamper}"
    )
    current = revised["current_attempt"]
    _enroll_current_runtime(service, thread_id)
    if tamper == "work_pair":
        with service.db.transaction() as connection:
            connection.execute("DROP TRIGGER work_items_managed_thread_exact_update")
            connection.execute("DROP TRIGGER work_items_managed_thread_rebind_update")
            connection.execute(
                "UPDATE work_items SET managed_worker_thread_id = 'mwt_tampered' WHERE id = ?",
                (revised["id"],),
            )
    elif tamper == "lifecycle_generation":
        with service.db.transaction() as connection:
            connection.execute("DROP TRIGGER managed_worker_threads_nonterminal_work_update")
            connection.execute(
                "UPDATE managed_worker_threads SET generation = generation + 1 WHERE id = ?",
                (thread_id,),
            )
    elif tamper == "untracked_delivery_runtime":
        service.db.execute(
            "UPDATE message_deliveries SET runtime_session_id = ? "
            "WHERE message_id IN (SELECT id FROM messages WHERE attempt_id = ?)",
            (system["runtime"]["id"], current["id"]),
        )
    elif tamper == "attempt_owner_mismatch":
        with service.db.transaction() as connection:
            connection.execute("DROP TRIGGER attempts_managed_thread_identity_immutable")
            connection.execute(
                "UPDATE attempts SET worker_id = ? WHERE id = ?",
                (system["worker"]["id"], current["id"]),
            )
    elif tamper == "disabled_spec":
        service.db.execute(
            "UPDATE managed_worker_specs SET state = 'revoked' "
            "WHERE id = (SELECT managed_spec_id FROM managed_worker_threads WHERE id = ?)",
            (thread_id,),
        )
    elif tamper == "foreign_spec_attachment":
        foreign_actor = _attached(service, suffix="foreign-dispatch-attachment")
        service.db.execute(
            "UPDATE managed_worker_specs SET attachment_id = ?, attachment_generation = ? "
            "WHERE id = (SELECT managed_spec_id FROM managed_worker_threads WHERE id = ?)",
            (
                foreign_actor["_cao_attachment_id"],
                foreign_actor["_cao_attachment_generation"],
                thread_id,
            ),
        )
    else:
        retired = service.db.fetchone(
            "SELECT runtime_session_id FROM managed_worker_thread_epochs "
            "WHERE thread_id = ? AND retired_at IS NOT NULL "
            "ORDER BY connection_generation DESC LIMIT 1",
            (thread_id,),
        )
        assert retired is not None
        service.db.execute(
            "UPDATE managed_worker_specs SET runtime_session_id = ? "
            "WHERE id = (SELECT managed_spec_id FROM managed_worker_threads WHERE id = ?)",
            (retired["runtime_session_id"], thread_id),
        )
    assert Dispatcher(service, service.settings)._claim_delivery() is None
    delivery = service.db.fetchone(
        "SELECT delivery.state FROM message_deliveries AS delivery "
        "JOIN messages AS message ON message.id = delivery.message_id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (current["id"],),
    )
    assert delivery is not None and delivery["state"] == "queued"


@pytest.mark.parametrize(
    "kind",
    (
        BoundaryDispositionKind.CONTINUE,
        BoundaryDispositionKind.CORRECT,
        BoundaryDispositionKind.RETRY,
    ),
)
def test_disconnected_disposition_uses_one_replacement_assignment(
    system: dict[str, Any], tmp_path: Path, kind: BoundaryDispositionKind
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"dispose-{kind.value}")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"dispose-{kind.value}")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"dispose-{kind.value}")
    waiting = _question(
        service,
        worker,
        str(instructed["task"]["work_item_id"]),
        suffix=f"dispose-{kind.value}",
    )
    boundary = waiting["open_boundaries"][-1]
    turn = service.acquire_reasoner_turn(
        actor,
        str(waiting["id"]),
        boundary_id=str(boundary["id"]),
        expected_generation=int(waiting["generation"]),
        idempotency_key=f"turn-dispose-{kind.value}",
    )
    _disconnect_with_native(service, runtime_id, native_session_id=f"native-dispose-{kind.value}")
    instruction = "Continue with the exact corrected option."
    service.dispose_boundary(
        actor,
        str(boundary["id"]),
        BoundaryDispositionInput(
            turn_id=str(turn["id"]),
            lease_token=str(turn["lease_token"]),
            expected_generation=int(waiting["generation"]),
            kind=kind,
            reason="The exact next action is now decided.",
            instruction=(instruction if kind != BoundaryDispositionKind.RETRY else ""),
        ),
    )
    current = service.get_work(str(waiting["id"]))["current_attempt"]
    assert int(current["attempt_number"]) == 2
    messages = _attempt_messages(service, str(current["id"]))
    assert [row["kind"] for row in messages] == ["assignment"]
    payload = json.loads(str(messages[0]["payload_json"]))
    if kind == BoundaryDispositionKind.RETRY:
        assert "command" not in payload
    else:
        assert payload["command"] == {
            "action": kind.value,
            "instruction": instruction,
            "reason": "The exact next action is now decided.",
            "source_boundary_id": boundary["id"],
        }
        rendered = render_message({"kind": "assignment", "payload": payload})
        assert f"- Action: {kind.value}" in rendered
        assert "- Reason: The exact next action is now decided." in rendered
        assert f"- Instruction: {instruction}" in rendered
    fresh_worker, _fresh_runtime = _enroll_current_runtime(service, thread_id)
    context = service.get_worker_context(fresh_worker, str(current["id"]))
    assignment_context = next(item for item in context["inbox"] if item["kind"] == "assignment")
    if kind == BoundaryDispositionKind.RETRY:
        assert "command" not in assignment_context["payload"]
    else:
        assert assignment_context["payload"]["command"] == {
            "action": kind.value,
            "reason": "The exact next action is now decided.",
            "instruction": instruction,
        }
    work = service.get_work(str(waiting["id"]))
    service.report(
        fresh_worker,
        str(current["id"]),
        ReportInput(
            kind=ReportKind.PROGRESS,
            expected_goal_version=int(work["goal_version"]),
            expected_goal_packet_digest=str(current["goal_packet_digest"]),
            expected_task_packet_digest=str(current["task_packet_digest"]),
            expected_generation=int(work["generation"]),
            summary="The replacement Assignment started exactly once.",
            idempotency_key=f"replacement-start-{kind.value}",
        ),
    )
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ?",
        (messages[0]["id"],),
    )
    assert delivery is not None and delivery["state"] == "handled"


def test_disconnected_user_needed_reply_uses_one_replacement_assignment(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply")
    waiting = _question(service, worker, str(instructed["task"]["work_item_id"]), suffix="reply")
    user_needed = _wait_user(service, actor, waiting, suffix="reply")
    _disconnect_with_native(service, runtime_id, native_session_id="native-reply")

    resumed = MCPServer(service).call_tool(
        actor,
        "cao_reply",
        {
            "work_item_id": str(user_needed["id"]),
            "message": "Use option B exactly.",
            "idempotency_key": "reply-user-needed-offline",
        },
    )

    current = resumed["current_attempt"]
    projected = json.dumps(resumed, sort_keys=True)
    assert runtime_id not in projected
    assert "native-reply" not in projected
    work_projection = MCPServer(service).call_tool(
        actor, "cao_get_work", {"work_item_id": str(user_needed["id"])}
    )
    assert work_projection["worker_thread_binding_state"] == "exact"
    assert work_projection["worker_thread_id"] == thread_id
    assert work_projection["worker_thread_generation"] == 1
    assert int(current["attempt_number"]) == 2
    messages = _attempt_messages(service, str(current["id"]))
    assert [row["kind"] for row in messages] == ["assignment"]
    command = json.loads(str(messages[0]["payload_json"]))["command"]
    assert command["action"] == "correct"
    assert command["instruction"] == "Use option B exactly."


def test_disconnected_wait_user_reply_rolls_back_continuation_and_epoch_on_final_receipt_failure(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-wait-rollback")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-wait-rollback")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-wait-rollback")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-wait-rollback")
    wait = _wait_user(service, actor, waiting, suffix="reply-wait-rollback")
    boundary_id = str(wait["user_needed"]["boundary_id"])
    _disconnect_with_native(service, runtime_id, native_session_id="native-reply-wait-rollback")
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "boundary_continuations",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)
    original_put = service._idempotent_put

    def fail_final_reply_receipt(
        connection: sqlite3.Connection,
        actor_id: str,
        operation: str,
        key: str,
        request_digest: str,
        result: dict[str, Any],
    ) -> None:
        if operation == "reply":
            raise RuntimeError("injected final reply receipt failure")
        original_put(connection, actor_id, operation, key, request_digest, result)

    monkeypatch.setattr(service, "_idempotent_put", fail_final_reply_receipt)
    with pytest.raises(RuntimeError, match="injected final reply receipt failure"):
        service.reply(
            actor,
            work_id,
            "Persist this requester decision atomically.",
            idempotency_key="reply-wait-final-receipt-rollback",
        )

    assert _ledger_snapshot(service, snapshot_tables) == before
    monkeypatch.setattr(service, "_idempotent_put", original_put)
    resumed = service.reply(
        actor,
        work_id,
        "Persist this requester decision atomically.",
        idempotency_key="reply-wait-final-receipt-rollback",
    )

    assert resumed["state"] == "active"
    assert int(resumed["current_attempt"]["attempt_number"]) == 2
    continuation = service.db.fetchall(
        "SELECT * FROM boundary_continuations WHERE work_item_id = ?", (work_id,)
    )
    assert len(continuation) == 1
    assert str(continuation[0]["boundary_id"]) == boundary_id
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 2
    )
    assert [
        row["kind"] for row in _attempt_messages(service, str(resumed["current_attempt"]["id"]))
    ] == ["assignment"]


def test_disconnected_open_boundary_reply_uses_one_replacement_assignment(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="open-reply")
    thread_id = _new_thread(service, actor, tmp_path, suffix="open-reply")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="open-reply")
    work_id = str(instructed["task"]["work_item_id"])
    _question(service, worker, work_id, suffix="open-reply")
    _disconnect_with_native(service, runtime_id, native_session_id="native-open-reply")

    resumed = service.reply(
        actor,
        work_id,
        "Use the exact supervisor correction.",
        idempotency_key="open-boundary-offline-reply",
    )

    current = resumed["current_attempt"]
    assert int(current["attempt_number"]) == 2
    messages = _attempt_messages(service, str(current["id"]))
    assert [row["kind"] for row in messages] == ["assignment"]
    command = json.loads(str(messages[0]["payload_json"]))["command"]
    assert command["action"] == "correct"
    assert command["instruction"] == "Use the exact supervisor correction."


def test_disposed_boundary_replay_reauthorizes_the_exact_conversation_before_returning(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    owner = _attached(service, suffix="disposed-replay-owner")
    thread_id = _new_thread(service, owner, tmp_path, suffix="disposed-replay-owner")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, owner, thread_id, suffix="disposed-replay-owner")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-replay-owner")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    turn = service.acquire_reasoner_turn(
        owner,
        work_id,
        boundary_id=boundary_id,
        expected_generation=int(waiting["generation"]),
        idempotency_key="disposed-replay-owner-turn",
    )
    request = BoundaryDispositionInput(
        turn_id=str(turn["id"]),
        lease_token=str(turn["lease_token"]),
        expected_generation=int(waiting["generation"]),
        kind=BoundaryDispositionKind.CORRECT,
        reason="Apply the exact owning-conversation correction.",
        instruction="Continue only on the owning conversation lane.",
    )
    disposed = service.dispose_boundary(owner, boundary_id, request)
    assert disposed["boundary_id"] == boundary_id

    foreign = _attached(service, suffix="disposed-replay-foreign")
    snapshot_tables = (
        "cao_session_attachments",
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError) as caught:
        service.dispose_boundary(foreign, boundary_id, request)

    assert str(caught.value) == "work item belongs to another CAO conversation"
    mcp_arguments = request.model_dump(mode="json", exclude_none=True)
    mcp_arguments.update({"work_item_id": work_id, "boundary_id": boundary_id})
    with pytest.raises(AuthorizationError) as mcp_caught:
        MCPServer(service).call_tool(foreign, "cao_dispose_boundary", mcp_arguments)
    assert str(mcp_caught.value) == "work item belongs to another CAO conversation"
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize("ingress", ("acquire_turn", "revise_goal", "request_status"))
def test_foreign_attachment_cannot_replay_cached_work_authority(
    system: dict[str, Any], tmp_path: Path, ingress: str
) -> None:
    service = system["service"]
    owner = _attached(service, suffix=f"cache-owner-{ingress}")
    thread_id = _new_thread(service, owner, tmp_path, suffix=f"cache-owner-{ingress}")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, owner, thread_id, suffix=f"cache-owner-{ingress}")
    work_id = str(instructed["task"]["work_item_id"])
    invoke: Any

    if ingress == "acquire_turn":
        waiting = _question(service, worker, work_id, suffix="cache-acquire-turn")
        boundary_id = str(waiting["open_boundaries"][-1]["id"])

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.acquire_reasoner_turn(
                actor,
                work_id,
                boundary_id=boundary_id,
                expected_generation=int(waiting["generation"]),
                lease_seconds=180,
                idempotency_key="foreign-cache-acquire-turn",
            )

    elif ingress == "revise_goal":
        waiting = _question(service, worker, work_id, suffix="cache-revise-goal")
        _wait_user(service, owner, waiting, suffix="cache-revise-goal")
        revision = GoalRevision(
            expected_version=1,
            objective="Persist the exact owner-authorized revised objective.",
            maturity=GoalMaturity.DEFINED,
            acceptance=["Only the owning conversation receives this revision bundle."],
            reason="Exercise cached replay authorization.",
            idempotency_key="foreign-cache-revise-goal",
        )

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.revise_goal(actor, work_id, revision)

    else:
        status_request = StatusRequestInput(
            expected_generation=1,
            summary="Report exact owner-scoped status.",
            idempotency_key="foreign-cache-request-status",
        )

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.request_status(actor, work_id, status_request)

    owner_result = invoke(owner)
    assert isinstance(owner_result, dict)
    if ingress == "acquire_turn":
        assert str(owner_result["lease_token"])
    foreign = _attached(service, suffix=f"cache-foreign-{ingress}")
    snapshot_tables = (
        "cao_session_attachments",
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "boundary_continuations",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError) as caught:
        invoke(foreign)

    assert str(caught.value) == "work item belongs to another CAO conversation"
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_foreign_attachment_cannot_replay_cached_assignment_result(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    owner = _attached(service, suffix="cache-owner-assign")
    thread_id = _new_thread(service, owner, tmp_path, suffix="cache-owner-assign")
    route = _thread_route(service, thread_id)
    request = WorkAssignment(
        worker_id=str(route["principal_id"]),
        managed_worker_thread_id=thread_id,
        managed_worker_thread_generation=1,
        title="Owner-scoped direct assignment",
        objective="Persist one exact owner-scoped assignment.",
        maturity=GoalMaturity.DEFINED,
        acceptance=["The assigned Work remains bound to its owning conversation."],
        idempotency_key="foreign-cache-assign-work",
    )
    assigned = service.assign_work(owner, request)
    work_id = str(assigned["id"])
    foreign = _attached(service, suffix="cache-foreign-assign")
    snapshot_tables = (
        "cao_session_attachments",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError) as caught:
        service.assign_work(foreign, request)

    assert str(caught.value) == "work item belongs to another CAO conversation"
    assert service.get_work(work_id)["supervisor_attachment_id"] == owner["_cao_attachment_id"]
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize("ingress", ("reply", "create_attempt", "cancel"))
def test_foreign_attachment_cannot_replay_cached_work_mutator_result(
    system: dict[str, Any], tmp_path: Path, ingress: str
) -> None:
    service = system["service"]
    owner = _attached(service, suffix=f"cache-owner-{ingress}")
    thread_id = _new_thread(service, owner, tmp_path, suffix=f"cache-owner-{ingress}")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, owner, thread_id, suffix=f"cache-owner-{ingress}")
    work_id = str(instructed["task"]["work_item_id"])
    invoke: Any

    if ingress == "reply":
        waiting = _question(service, worker, work_id, suffix="cache-reply")
        _wait_user(service, owner, waiting, suffix="cache-reply")

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.reply(
                actor,
                work_id,
                "Use the exact owner-scoped requester decision.",
                idempotency_key="foreign-cache-reply",
            )

    elif ingress == "create_attempt":
        waiting = _question(service, worker, work_id, suffix="cache-create-attempt")
        _wait_user(service, owner, waiting, suffix="cache-create-attempt")

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.create_attempt(
                actor,
                work_id,
                reason="Exercise exact cached retry authorization.",
                idempotency_key="foreign-cache-create-attempt",
                managed_worker_thread_id=thread_id,
                managed_worker_thread_generation=1,
            )

    else:

        def invoke(actor: dict[str, Any]) -> dict[str, Any]:
            return service.cancel_work(
                actor,
                work_id,
                "Exercise exact cached cancellation authorization.",
                idempotency_key="foreign-cache-cancel",
            )

    owner_result = invoke(owner)
    assert isinstance(owner_result, dict)
    foreign = _attached(service, suffix=f"cache-foreign-{ingress}")
    snapshot_tables = (
        "cao_session_attachments",
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "boundary_continuations",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError) as caught:
        invoke(foreign)

    assert str(caught.value) == "work item belongs to another CAO conversation"
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_new_runtime_credential_cannot_replay_cached_report_from_prior_attempt_route(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="cache-report-route")
    thread_id = _new_thread(service, actor, tmp_path, suffix="cache-report-route")
    first_worker, first_runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="cache-report-route")
    work_id = str(instructed["task"]["work_item_id"])
    work = service.get_work(work_id)
    first_attempt = work["current_attempt"]
    report = ReportInput(
        kind=ReportKind.PROGRESS,
        expected_goal_version=int(work["goal_version"]),
        expected_goal_packet_digest=str(first_attempt["goal_packet_digest"]),
        expected_task_packet_digest=str(first_attempt["task_packet_digest"]),
        expected_generation=int(work["generation"]),
        summary="Cache this report only for its exact runtime route.",
        idempotency_key="cached-report-prior-runtime",
    )
    first_result = service.report(first_worker, str(first_attempt["id"]), report)
    assert first_result["current_attempt"]["id"] == first_attempt["id"]
    waiting = _question(service, first_worker, work_id, suffix="cache-report-route")
    _wait_user(service, actor, waiting, suffix="cache-report-route")
    _disconnect_with_native(
        service,
        first_runtime_id,
        native_session_id="native-cache-report-prior-runtime",
    )
    resumed = service.reply(
        actor,
        work_id,
        "Resume on a fresh exact runtime route.",
        idempotency_key="cache-report-route-resume",
    )
    assert int(resumed["current_attempt"]["attempt_number"]) == 2
    second_worker, second_runtime_id = _enroll_current_runtime(service, thread_id)
    assert second_worker["id"] == first_worker["id"]
    assert second_runtime_id != first_runtime_id
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "boundary_continuations",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError):
        service.report(second_worker, str(first_attempt["id"]), report)

    assert _ledger_snapshot(service, snapshot_tables) == before


def test_foreign_attachment_cannot_replay_cached_review_bundle(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    owner = _attached(service, suffix="cache-owner-review")
    thread_id = _new_thread(service, owner, tmp_path, suffix="cache-owner-review")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, owner, thread_id, suffix="cache-owner-review")
    work_id = str(instructed["task"]["work_item_id"])
    work = service.get_work(work_id)
    attempt = work["current_attempt"]
    service.report(
        worker,
        str(attempt["id"]),
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=int(work["goal_version"]),
            expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
            expected_task_packet_digest=str(attempt["task_packet_digest"]),
            expected_generation=int(work["generation"]),
            summary="Submit the exact result for owner-scoped review.",
            idempotency_key="cache-review-completion",
        ),
    )
    request = ReviewInput(
        attempt_id=str(attempt["id"]),
        verdict=ReviewVerdict.NEEDS_WORK,
        summary="The owning conversation requests one exact correction.",
        idempotency_key="foreign-cache-review",
    )
    owner_result = service.review(owner, request)
    assert owner_result["id"] == work_id
    foreign = _attached(service, suffix="cache-foreign-review")
    snapshot_tables = (
        "cao_session_attachments",
        "work_items",
        "attempts",
        "boundaries",
        "reviews",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(AuthorizationError) as caught:
        service.review(foreign, request)

    assert str(caught.value) == "work item belongs to another CAO conversation"
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_disconnected_open_boundary_reply_different_key_loser_is_atomic(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-race")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-race")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-race")
    work_id = str(instructed["task"]["work_item_id"])
    _question(service, worker, work_id, suffix="reply-race")
    _disconnect_with_native(service, runtime_id, native_session_id="native-reply-race")
    ready = Barrier(2)

    def reply_once(ordinal: int) -> dict[str, Any] | ControlPlaneError:
        ready.wait(timeout=5)
        try:
            return service.reply(
                actor,
                work_id,
                f"Use exact option {ordinal}.",
                idempotency_key=f"offline-reply-race-{ordinal}",
            )
        except ControlPlaneError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reply_once, range(2)))

    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, ControlPlaneError) for result in results) == 1
    current = service.get_work(work_id)["current_attempt"]
    assert int(current["attempt_number"]) == 2
    assert [row["kind"] for row in _attempt_messages(service, str(current["id"]))] == ["assignment"]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM idempotency_results "
            "WHERE actor_id = ? AND operation = 'reply_context'",
            (actor["id"],),
        )["n"]
        == 0
    )
    turns = service.db.fetchall(
        "SELECT state FROM reasoner_turns WHERE work_item_id = ?",
        (work_id,),
    )
    assert [str(row["state"]) for row in turns] == ["completed"]


def test_atomic_open_boundary_reply_rolls_back_every_row_on_late_admission_blocker(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-atomic-rollback")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-atomic-rollback")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-atomic-rollback")
    work_id = str(instructed["task"]["work_item_id"])
    _question(service, worker, work_id, suffix="reply-atomic-rollback")
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "boundary_supersessions",
        "reasoner_turns",
        "directives",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    def late_blocker(*_args: Any, **_kwargs: Any) -> None:
        raise ConflictError(
            "injected execution-owned blocker",
            reason_code="worker_instruction_outcome_unknown",
            retryable=False,
        )

    monkeypatch.setattr(service, "_admit_managed_worker_command_tx", late_blocker)
    with pytest.raises(ConflictError) as caught:
        service.reply(
            actor,
            work_id,
            "Apply the exact atomic correction.",
            idempotency_key="reply-atomic-rollback",
        )

    assert caught.value.details == {
        "reason_code": "worker_instruction_outcome_unknown",
        "retryable": False,
    }
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_exact_legacy_leased_reply_turn_resumes_through_shared_disposition_core(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-legacy-leased")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-legacy-leased")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-legacy-leased")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-legacy-leased")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    message_text = "Resume the exact pre-upgrade leased reply turn."
    key = "reply-legacy-leased"
    turn, _request_digest = _seed_legacy_reply_context_and_turn(
        service,
        actor,
        work_id,
        boundary_id,
        message_text=message_text,
        key=key,
    )

    result = service.reply(actor, work_id, message_text, idempotency_key=key)

    assert result["id"] == work_id
    turns = service.db.fetchall(
        "SELECT id, state FROM reasoner_turns WHERE boundary_id = ? ORDER BY created_at, id",
        (boundary_id,),
    )
    assert [(str(row["id"]), str(row["state"])) for row in turns] == [
        (str(turn["id"]), "completed")
    ]
    disposition = service.db.fetchone(
        "SELECT reasoner_turn_id, kind, instruction FROM boundary_dispositions "
        "WHERE boundary_id = ?",
        (boundary_id,),
    )
    assert disposition is not None
    assert dict(disposition) == {
        "reasoner_turn_id": turn["id"],
        "kind": "correct",
        "instruction": message_text,
    }


def test_exact_expired_legacy_reply_turn_is_replaced_atomically(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-legacy-expired")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-legacy-expired")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-legacy-expired")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-legacy-expired")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    message_text = "Replace the exact expired pre-upgrade reply turn."
    key = "reply-legacy-expired"
    turn, _request_digest = _seed_legacy_reply_context_and_turn(
        service,
        actor,
        work_id,
        boundary_id,
        message_text=message_text,
        key=key,
    )
    receipt_row = service.db.fetchone(
        "SELECT result_json FROM idempotency_results WHERE actor_id = ? "
        "AND operation = 'acquire_reasoner_turn' AND idempotency_key = ?",
        (actor["id"], f"reply:{key}"),
    )
    assert receipt_row is not None
    receipt = json.loads(str(receipt_row["result_json"]))
    receipt["lease_expires_at"] = "2000-01-01T00:00:00Z"
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            (receipt["lease_expires_at"], turn["id"]),
        )
        connection.execute(
            "UPDATE idempotency_results SET result_json = ? WHERE actor_id = ? "
            "AND operation = 'acquire_reasoner_turn' AND idempotency_key = ?",
            (
                json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                actor["id"],
                f"reply:{key}",
            ),
        )

    service.reply(actor, work_id, message_text, idempotency_key=key)

    turns = service.db.fetchall(
        "SELECT id, state FROM reasoner_turns WHERE boundary_id = ? ORDER BY created_at, id",
        (boundary_id,),
    )
    assert len(turns) == 2
    assert {str(row["state"]) for row in turns} == {"abandoned", "completed"}
    disposition = service.db.fetchone(
        "SELECT reasoner_turn_id FROM boundary_dispositions WHERE boundary_id = ?",
        (boundary_id,),
    )
    assert disposition is not None
    assert str(disposition["reasoner_turn_id"]) != str(turn["id"])


@pytest.mark.parametrize("corruption", ["token", "foreign_boundary"])
def test_corrupt_legacy_reply_turn_receipt_fails_closed_without_mutation(
    system: dict[str, Any], tmp_path: Path, corruption: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"reply-legacy-corrupt-{corruption}")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"reply-legacy-corrupt-{corruption}")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"reply-legacy-corrupt-{corruption}")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=f"reply-legacy-corrupt-{corruption}")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    message_text = "Reject the corrupt pre-upgrade reply turn."
    key = f"reply-legacy-corrupt-{corruption}"
    _turn, _request_digest = _seed_legacy_reply_context_and_turn(
        service,
        actor,
        work_id,
        boundary_id,
        message_text=message_text,
        key=key,
    )
    receipt_row = service.db.fetchone(
        "SELECT result_json FROM idempotency_results WHERE actor_id = ? "
        "AND operation = 'acquire_reasoner_turn' AND idempotency_key = ?",
        (actor["id"], f"reply:{key}"),
    )
    assert receipt_row is not None
    receipt = json.loads(str(receipt_row["result_json"]))
    if corruption == "token":
        receipt["lease_token"] = "corrupt-token"
    else:
        receipt["boundary_id"] = "bnd_foreign"
    service.db.execute(
        "UPDATE idempotency_results SET result_json = ? WHERE actor_id = ? "
        "AND operation = 'acquire_reasoner_turn' AND idempotency_key = ?",
        (
            json.dumps(receipt, sort_keys=True, separators=(",", ":")),
            actor["id"],
            f"reply:{key}",
        ),
    )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.reply(actor, work_id, message_text, idempotency_key=key)

    assert caught.value.details == {
        "reason_code": "reply_turn_provenance_conflict",
        "retryable": False,
    }
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_exact_legacy_disposed_reply_residue_replays_without_redisposition(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-legacy-disposed")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-legacy-disposed")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-legacy-disposed")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-legacy-disposed")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    message_text = "Replay the exact pre-upgrade disposed reply."
    key = "reply-legacy-disposed"
    turn, _request_digest = _seed_legacy_reply_context_and_turn(
        service,
        actor,
        work_id,
        boundary_id,
        message_text=message_text,
        key=key,
    )
    service.dispose_boundary(
        actor,
        boundary_id,
        BoundaryDispositionInput(
            turn_id=str(turn["id"]),
            lease_token=str(turn["lease_token"]),
            expected_generation=int(waiting["generation"]),
            kind=BoundaryDispositionKind.CORRECT,
            reason="Supervisor response to a recorded boundary",
            instruction=message_text,
        ),
        expected_work_item_id=work_id,
    )
    domain_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
    )
    before = _ledger_snapshot(service, domain_tables)

    replay = service.reply(actor, work_id, message_text, idempotency_key=key)

    assert replay["id"] == work_id
    assert _ledger_snapshot(service, domain_tables) == before
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM idempotency_results WHERE actor_id = ? "
            "AND operation = 'reply' AND idempotency_key = ?",
            (actor["id"], key),
        )["n"]
        == 1
    )


def test_explicit_reply_to_disposed_wait_user_boundary_uses_requester_continuation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-wait-user-explicit")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-wait-user-explicit")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-wait-user-explicit")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-wait-user-explicit")
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    _wait_user(service, actor, waiting, suffix="reply-wait-user-explicit")
    question_message = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND attempt_id = ? "
        "AND kind = 'question' AND json_extract(payload_json, '$.boundary_id') = ?",
        (work_id, waiting["current_attempt"]["id"], boundary_id),
    )
    assert question_message is not None

    resumed = service.reply(
        actor,
        work_id,
        "Use the exact requester-selected option.",
        in_reply_to=str(question_message["id"]),
        idempotency_key="reply-wait-user-explicit",
    )

    assert resumed["state"] == "active"
    assert resumed["attention_owner"] == "worker"
    assert int(resumed["current_attempt"]["attempt_number"]) == 1
    assert [
        row["kind"] for row in _attempt_messages(service, str(resumed["current_attempt"]["id"]))
    ][-1] == "instruction"


def test_same_attempt_can_ask_wait_and_consume_two_exact_requester_decisions(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="two-waits")
    thread_id = _new_thread(service, actor, tmp_path, suffix="two-waits")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="two-waits")
    work_id = str(instructed["task"]["work_item_id"])

    first_question = _question(service, worker, work_id, suffix="two-waits-first")
    first_boundary_id = str(first_question["open_boundaries"][-1]["id"])
    first_wait = _wait_user(service, actor, first_question, suffix="two-waits-first")
    assert first_wait["user_needed"]["boundary_id"] == first_boundary_id
    first_projection = _conversation_work_projection(first_wait)
    assert first_projection["user_needed"]["boundary_id"] == first_boundary_id
    first_reply = service.reply(
        actor,
        work_id,
        "Use requester option A for the first decision.",
        idempotency_key="two-waits-first-reply",
    )

    first_instruction = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND attempt_id = ? "
        "AND kind = 'instruction' ORDER BY sequence DESC LIMIT 1",
        (work_id, first_reply["current_attempt"]["id"]),
    )
    assert first_instruction is not None
    service.acknowledge(worker, AckInput(message_ids=[str(first_instruction["id"])]))
    second_question = _question(
        service,
        worker,
        work_id,
        suffix="two-waits-second",
        incorporated_message_id=str(first_instruction["id"]),
    )
    second_boundary_id = str(second_question["open_boundaries"][-1]["id"])
    assert second_boundary_id != first_boundary_id
    second_wait = _wait_user(service, actor, second_question, suffix="two-waits-second")
    assert second_wait["user_needed"]["boundary_id"] == second_boundary_id
    second_projection = _conversation_work_projection(second_wait)
    assert second_projection["user_needed"]["boundary_id"] == second_boundary_id
    second_reply = service.reply(
        actor,
        work_id,
        "Use requester option B for the second decision.",
        idempotency_key="two-waits-second-reply",
    )

    assert first_reply["current_attempt"]["id"] == second_reply["current_attempt"]["id"]
    assert first_reply["generation"] == second_reply["generation"] == 1
    assert "user_needed_boundary_id" not in second_reply
    stored_work = service.db.fetchone(
        "SELECT user_needed_boundary_id FROM work_items WHERE id = ?", (work_id,)
    )
    assert stored_work is not None and stored_work["user_needed_boundary_id"] is None
    continuations = service.db.fetchall(
        """
        SELECT continuation.*, message.kind, message.attempt_id AS message_attempt_id,
               delivery.recipient_id, delivery.state AS delivery_state
        FROM boundary_continuations AS continuation
        JOIN messages AS message ON message.id = continuation.message_id
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE continuation.work_item_id = ?
        ORDER BY continuation.created_at, continuation.boundary_id
        """,
        (work_id,),
    )
    assert {str(row["boundary_id"]) for row in continuations} == {
        first_boundary_id,
        second_boundary_id,
    }
    assert len(continuations) == 2
    assert all(
        str(row["source_attempt_id"]) == str(first_reply["current_attempt"]["id"])
        and str(row["successor_attempt_id"]) == str(first_reply["current_attempt"]["id"])
        and int(row["source_generation"]) == 1
        and int(row["successor_generation"]) == 1
        and str(row["kind"]) == "instruction"
        and str(row["message_attempt_id"]) == str(first_reply["current_attempt"]["id"])
        and str(row["recipient_id"]) == str(_thread_route(service, thread_id)["principal_id"])
        and str(row["delivery_state"]) in {"handled", "queued"}
        for row in continuations
    )
    delivery_by_boundary = {
        str(row["boundary_id"]): str(row["delivery_state"]) for row in continuations
    }
    assert delivery_by_boundary == {
        first_boundary_id: "handled",
        second_boundary_id: "queued",
    }
    with pytest.raises(sqlite3.IntegrityError):
        service.db.execute(
            "UPDATE boundary_continuations SET successor_generation = 2 WHERE boundary_id = ?",
            (first_boundary_id,),
        )


@pytest.mark.parametrize("selection_order", ("concurrent", "both_selected", "winner_committed"))
@pytest.mark.parametrize("same_key", (True, False))
def test_concurrent_wait_user_replies_consume_one_exact_boundary(
    system: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    same_key: bool,
    selection_order: str,
) -> None:
    service = system["service"]
    suffix = f"wait-reply-race-{'same' if same_key else 'different'}"
    actor_one = _attached(service, suffix=suffix)
    actor_two = _second_attached_connection(service, suffix=suffix)
    thread_id = _new_thread(service, actor_one, tmp_path, suffix=suffix)
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor_one, thread_id, suffix=suffix)
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=suffix)
    wait = _wait_user(service, actor_one, waiting, suffix=suffix)
    boundary_id = str(wait["user_needed"]["boundary_id"])
    ready = Barrier(2)
    winner_committed = Event()
    if selection_order == "both_selected":
        selections_ready = Barrier(2)
        original_reply_user_needed = service._reply_user_needed

        def reply_after_both_selections(*args: Any, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["boundary_id"] == boundary_id
            selections_ready.wait(timeout=5)
            return original_reply_user_needed(*args, **kwargs)

        monkeypatch.setattr(service, "_reply_user_needed", reply_after_both_selections)

    def answer(ordinal_actor: tuple[int, dict[str, Any]]) -> dict[str, Any] | ControlPlaneError:
        ordinal, actor = ordinal_actor
        ready.wait(timeout=5)
        if selection_order == "winner_committed" and ordinal == 1:
            assert winner_committed.wait(timeout=5)
        try:
            result = service.reply(
                actor,
                work_id,
                "Use the one exact concurrent requester decision.",
                idempotency_key=(
                    "concurrent-wait-user-same"
                    if same_key
                    else f"concurrent-wait-user-different-{ordinal}"
                ),
            )
        except ControlPlaneError as error:
            return error
        if selection_order == "winner_committed" and ordinal == 0:
            winner_committed.set()
        return result

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(answer, enumerate((actor_one, actor_two))))

    successes = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    failures = [outcome for outcome in outcomes if isinstance(outcome, ControlPlaneError)]
    if same_key:
        assert len(successes) == 2 and not failures
        assert successes[0] == successes[1]
    else:
        assert len(successes) == 1 and len(failures) == 1
        assert failures[0].details == {
            "reason_code": "worker_command_generation_conflict",
            "retryable": False,
        }
        if selection_order == "winner_committed":
            assert failures[0].message == "work item has no unresolved boundary to answer"
    continuation = service.db.fetchall(
        "SELECT * FROM boundary_continuations WHERE work_item_id = ?", (work_id,)
    )
    assert len(continuation) == 1
    assert str(continuation[0]["boundary_id"]) == boundary_id
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM messages WHERE work_item_id = ? AND kind = 'instruction'",
            (work_id,),
        )["n"]
        == 1
    )


@pytest.mark.parametrize("lifecycle", ("finish", "delete"))
def test_terminal_worker_lifecycle_clears_current_wait_user_pointer_without_consuming_it(
    system: dict[str, Any], tmp_path: Path, lifecycle: str
) -> None:
    service = system["service"]
    suffix = f"wait-terminal-{lifecycle}"
    actor = _attached(service, suffix=suffix)
    thread_id = _new_thread(service, actor, tmp_path, suffix=suffix)
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=suffix)
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=suffix)
    wait = _wait_user(service, actor, waiting, suffix=suffix)
    boundary_id = str(wait["user_needed"]["boundary_id"])

    if lifecycle == "finish":
        service.finish_worker_thread(
            actor,
            WorkerThreadLifecycleInput(
                worker_thread_id=thread_id,
                expected_generation=1,
                idempotency_key="finish-current-wait-user",
            ),
        )
    else:
        service.delete_worker_thread(
            actor,
            DeleteWorkerThreadInput(
                worker_thread_id=thread_id,
                expected_generation=1,
                idempotency_key="delete-current-wait-user",
            ),
        )

    stored_work = service.db.fetchone(
        "SELECT state, attention_owner, generation, user_needed_boundary_id "
        "FROM work_items WHERE id = ?",
        (work_id,),
    )
    assert stored_work is not None
    assert dict(stored_work) == {
        "state": "canceled",
        "attention_owner": "none",
        "generation": 2,
        "user_needed_boundary_id": None,
    }
    disposition = service.db.fetchone(
        "SELECT kind FROM boundary_dispositions WHERE boundary_id = ?", (boundary_id,)
    )
    assert disposition is not None and disposition["kind"] == "wait_user"
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM boundary_continuations WHERE boundary_id = ?",
            (boundary_id,),
        )["n"]
        == 0
    )


def test_wait_user_reply_rechecks_no_new_unresolved_boundary_after_selection(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="reply-wait-user-race")
    thread_id = _new_thread(service, actor, tmp_path, suffix="reply-wait-user-race")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="reply-wait-user-race")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="reply-wait-user-race")
    _wait_user(service, actor, waiting, suffix="reply-wait-user-race")
    original = service._reply_user_needed
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    after_race: dict[str, list[dict[str, Any]]] | None = None

    def inject_unresolved_boundary(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal after_race
        current = service.get_work(work_id)
        attempt = current["current_attempt"]
        service.record_boundary(
            actor,
            BoundaryInput(
                source_event_id="injected-after-wait-user-selection",
                work_item_id=work_id,
                attempt_id=str(attempt["id"]),
                expected_goal_version=int(current["goal_version"]),
                expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
                expected_task_packet_digest=str(attempt["task_packet_digest"]),
                expected_generation=int(current["generation"]),
                kind=BoundaryKind.BLOCKER,
                summary="Concurrent unresolved boundary",
                runtime_state=RuntimeState.WAITING,
                metadata={"source": "race-regression"},
            ),
        )
        after_race = _ledger_snapshot(service, snapshot_tables)
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "_reply_user_needed", inject_unresolved_boundary)
    with pytest.raises(ConflictError) as caught:
        service.reply(
            actor,
            work_id,
            "Do not cross the newly unresolved boundary.",
            idempotency_key="reply-wait-user-race",
        )

    assert caught.value.details == {
        "reason_code": "worker_command_binding_conflict",
        "retryable": False,
    }
    assert after_race is not None
    assert _ledger_snapshot(service, snapshot_tables) == after_race


def test_resume_then_instruction_reuses_the_fresh_pending_epoch(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="resume")
    thread_id = _new_thread(service, actor, tmp_path, suffix="resume")
    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=1,
            idempotency_key="finish-before-task-resume",
        ),
    )
    resumed = service.resume_worker_thread(
        actor,
        ResumeWorkerThreadInput(
            worker_thread_id=thread_id,
            expected_generation=2,
            idempotency_key="resume-with-task-one-epoch",
        ),
    )

    assert resumed["thread_generation"] == 3
    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=thread_id,
            expected_generation=3,
            title="Resumed exact task",
            objective="Produce the exact resumed result.",
            maturity=GoalMaturity.DEFINED,
            acceptance=["The resumed result is reviewable."],
            idempotency_key="instruction-after-resume-one-epoch",
        ),
    )
    current_epochs = service.db.fetchall(
        "SELECT generation, connection_generation FROM managed_worker_thread_epochs "
        "WHERE thread_id = ? AND retired_at IS NULL",
        (thread_id,),
    )
    assert [
        (int(row["generation"]), int(row["connection_generation"])) for row in current_epochs
    ] == [(3, 1)]
    work_id = str(instructed["task"]["work_item_id"])
    attempt = service.get_work(work_id)["current_attempt"]
    assert [
        (row["kind"], row["delivery_state"])
        for row in _attempt_messages(service, str(attempt["id"]))
    ] == [("assignment", "queued")]


def test_generic_managed_assignment_cannot_guess_the_only_thread(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="ambiguous-create")
    thread_id = _new_thread(service, actor, tmp_path, suffix="ambiguous-create")
    route = _thread_route(service, thread_id)
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "source_receipts",
        "submitted_intents",
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.assign_work(
            actor,
            WorkAssignment(
                worker_id=str(route["principal_id"]),
                title="Ambiguous generic managed assignment",
                objective="Do not guess a logical Worker thread.",
                acceptance=["No logical Worker authority is guessed."],
                idempotency_key="ambiguous-generic-managed-assignment",
            ),
        )

    assert caught.value.details is not None
    assert caught.value.details["reason_code"] == "worker_command_binding_ambiguous"
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_full_assignment_model_requires_and_persists_the_exact_thread_pair(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="explicit-managed-assign")
    thread_id = _new_thread(service, actor, tmp_path, suffix="explicit-managed-assign")
    route = _thread_route(service, thread_id)

    assigned = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=str(route["principal_id"]),
            runtime_session_id=str(route["runtime_session_id"]),
            managed_worker_thread_id=thread_id,
            managed_worker_thread_generation=1,
            title="Explicit managed assignment",
            objective="Persist the exact logical Worker authority.",
            acceptance=["The Work pair is exact before its Attempt is inserted."],
            idempotency_key="explicit-managed-assignment",
        ),
    )

    assert assigned["managed_worker_thread_id"] == thread_id
    assert assigned["managed_worker_thread_generation"] == 1
    assert assigned["current_attempt"]["runtime_session_id"] == route["runtime_session_id"]
    assert int(_thread_route(service, thread_id)["connection_generation"]) == 1
    full_assign = next(
        tool for tool in MCPServer(service).tools_for(system["cao"]) if tool["name"] == "cao_assign"
    )
    properties = full_assign["inputSchema"]["properties"]
    assert "managed_worker_thread_id" in properties
    assert "managed_worker_thread_generation" in properties


def test_managed_assignment_rejects_a_noncanonical_runtime_without_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="assign-runtime-mismatch")
    thread_id = _new_thread(service, actor, tmp_path, suffix="assign-runtime-mismatch")
    route = _thread_route(service, thread_id)
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "source_receipts",
        "submitted_intents",
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ControlPlaneError) as caught:
        service.assign_work(
            actor,
            WorkAssignment(
                worker_id=str(route["principal_id"]),
                runtime_session_id=str(system["runtime"]["id"]),
                managed_worker_thread_id=thread_id,
                managed_worker_thread_generation=1,
                title="Reject a mismatched managed runtime",
                objective="Use only the route selected by exact logical authority.",
                acceptance=["No request provenance seals a different runtime."],
                idempotency_key="managed-assignment-runtime-mismatch",
            ),
        )

    assert caught.value.details == {"reason_code": "managed_runtime_selection_conflict"}
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_disconnected_status_is_bounded_and_has_zero_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="status")
    thread_id = _new_thread(service, actor, tmp_path, suffix="status")
    _worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="status")
    work_id = str(instructed["task"]["work_item_id"])
    work = service.get_work(work_id)
    _disconnect_with_native(service, runtime_id, native_session_id="private-native-status")
    snapshot_tables = (
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.request_status(
            actor,
            work_id,
            StatusRequestInput(
                expected_generation=int(work["generation"]),
                idempotency_key="disconnected-status-zero-mutation",
            ),
        )

    assert caught.value.details == {
        "reason_code": "worker_status_runtime_not_connected",
        "retryable": False,
    }
    assert "private-native-status" not in json.dumps(caught.value.as_dict(), sort_keys=True)
    assert runtime_id not in json.dumps(caught.value.as_dict(), sort_keys=True)
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_pending_unhandshaken_status_is_bounded_and_has_zero_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="pending-status")
    thread_id = _new_thread(service, actor, tmp_path, suffix="pending-status")
    instructed = _instruct(service, actor, thread_id, suffix="pending-status")
    work_id = str(instructed["task"]["work_item_id"])
    work = service.get_work(work_id)
    route = _thread_route(service, thread_id)
    assert int(route["connection_generation"]) == 1
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 1
    )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.request_status(
            actor,
            work_id,
            StatusRequestInput(
                expected_generation=int(work["generation"]),
                idempotency_key="pending-status-zero-mutation",
            ),
        )

    assert caught.value.details == {
        "reason_code": "worker_status_runtime_not_connected",
        "retryable": False,
    }
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize(
    ("blocker", "expected_kind"),
    (
        ("active_attempt", "work_not_settled"),
        ("claimed_delivery", "delivery_outcome_unsettled"),
        ("dispatched_delivery", "delivery_outcome_unsettled"),
        ("unknown_effect", "effect_outcome_unsettled"),
    ),
)
def test_unsettled_execution_blockers_leave_every_command_row_unchanged(
    system: dict[str, Any],
    tmp_path: Path,
    blocker: str,
    expected_kind: str,
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"blocker-{blocker}")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"blocker-{blocker}")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"blocker-{blocker}")
    work_id = str(instructed["task"]["work_item_id"])
    if blocker != "active_attempt":
        _question(service, worker, work_id, suffix=f"blocker-{blocker}")
        assignment = service.db.fetchone(
            "SELECT message.id FROM messages AS message "
            "WHERE message.work_item_id = ? AND message.kind = 'assignment'",
            (work_id,),
        )
        assert assignment is not None
        if blocker == "claimed_delivery":
            service.db.execute(
                "UPDATE message_deliveries SET state = 'leased', "
                "owner_token = 'active-command-claim', "
                "lease_until = '2999-01-01T00:00:00Z', handled_at = NULL "
                "WHERE message_id = ?",
                (assignment["id"],),
            )
        elif blocker == "dispatched_delivery":
            service.db.execute(
                "UPDATE message_deliveries SET state = 'dispatched', handled_at = NULL "
                "WHERE message_id = ?",
                (assignment["id"],),
            )
        else:
            route = _thread_route(service, thread_id)
            service.db.execute(
                """
                INSERT INTO effect_operations(
                    id, principal_id, kind, target, action, status,
                    created_at, updated_at
                ) VALUES(?, ?, 'external', 'opaque-target', 'opaque-action',
                         'unknown', '2026-08-21T00:00:00Z', '2026-08-21T00:00:00Z')
                """,
                (f"eff_{blocker}", route["principal_id"]),
            )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "work_items",
        "goal_revisions",
        "attempts",
        "messages",
        "message_deliveries",
        "managed_worker_thread_epochs",
        "effect_operations",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.revise_goal(
            actor,
            work_id,
            GoalRevision(
                expected_version=1,
                objective="This revision must not cross an unsettled outcome.",
                maturity=GoalMaturity.DEFINED,
                acceptance=["The prior outcome is settled first."],
                reason="Exercise the command admission blocker.",
                idempotency_key=f"blocked-revision-{blocker}",
            ),
        )

    assert caught.value.details is not None
    assert caught.value.details["reason_code"] == "worker_command_outcome_unsettled"
    assert caught.value.details["blocker_kind"] == expected_kind
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_concurrent_offline_revisions_advance_once_and_stale_loser_writes_nothing(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="revise-race")
    thread_id = _new_thread(service, actor, tmp_path, suffix="revise-race")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="revise-race")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="revise-race")
    _wait_user(service, actor, waiting, suffix="revise-race")
    _disconnect_with_native(service, runtime_id, native_session_id="native-revise-race")
    ready = Barrier(2)

    def revise(ordinal: int) -> dict[str, Any] | StaleGoalError:
        ready.wait(timeout=5)
        try:
            return service.revise_goal(
                actor,
                work_id,
                GoalRevision(
                    expected_version=1,
                    objective=f"Persist exactly one concurrent revision {ordinal}.",
                    maturity=GoalMaturity.DEFINED,
                    acceptance=["Only one revision advances the logical Work."],
                    reason="Exercise different-key revision serialization.",
                    idempotency_key=f"offline-revise-race-{ordinal}",
                ),
            )
        except StaleGoalError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(revise, range(2)))

    winners = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    losers = [outcome for outcome in outcomes if isinstance(outcome, StaleGoalError)]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0].details == {"expected": 1, "current": 2}
    assert int(winners[0]["goal_version"]) == 2
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM goal_revisions WHERE work_item_id = ?",
            (work_id,),
        )["n"]
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM attempts WHERE work_item_id = ?",
            (work_id,),
        )["n"]
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM messages WHERE work_item_id = ? AND kind = 'assignment'",
            (work_id,),
        )["n"]
        == 2
    )
    stored_keys = {
        str(row["idempotency_key"])
        for row in service.db.fetchall(
            "SELECT idempotency_key FROM idempotency_results "
            "WHERE actor_id = ? AND operation = 'revise_goal'",
            (actor["id"],),
        )
    }
    assert len(stored_keys & {"offline-revise-race-0", "offline-revise-race-1"}) == 1


def test_two_authentic_connections_replay_same_offline_revision_once(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor_one = _attached(service, suffix="revise-same-key")
    actor_two = _second_attached_connection(service, suffix="revise-same-key")
    assert actor_one["_cao_attachment_id"] == actor_two["_cao_attachment_id"]
    assert actor_one["_cao_connection_id"] != actor_two["_cao_connection_id"]
    thread_id = _new_thread(service, actor_one, tmp_path, suffix="revise-same-key")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor_one, thread_id, suffix="revise-same-key")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="revise-same-key")
    _wait_user(service, actor_one, waiting, suffix="revise-same-key")
    _disconnect_with_native(service, runtime_id, native_session_id="native-revise-same-key")
    request = GoalRevision(
        expected_version=1,
        objective="Persist this identical revision exactly once.",
        maturity=GoalMaturity.DEFINED,
        acceptance=["Both authentic connections observe the same durable result."],
        reason="Exercise same-key connection linearization.",
        idempotency_key="offline-revise-two-connections",
    )
    ready = Barrier(2)

    def revise(actor: dict[str, Any]) -> dict[str, Any]:
        ready.wait(timeout=5)
        return service.revise_goal(actor, work_id, request)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(revise, (actor_one, actor_two)))

    assert results[0] == results[1]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 2
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM attempts WHERE work_item_id = ?",
            (work_id,),
        )["n"]
        == 2
    )
    current = results[0]["current_attempt"]
    assert [
        (row["kind"], row["delivery_state"])
        for row in _attempt_messages(service, str(current["id"]))
    ] == [("assignment", "queued")]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM idempotency_results "
            "WHERE actor_id = ? AND operation = 'revise_goal' "
            "AND idempotency_key = 'offline-revise-two-connections'",
            (actor_one["id"],),
        )["n"]
        == 1
    )


def test_retry_cannot_stage_a_boundary_to_erase_a_working_attempt_blocker(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="retry-working")
    thread_id = _new_thread(service, actor, tmp_path, suffix="retry-working")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="retry-working")
    work_id = str(instructed["task"]["work_item_id"])
    work = service.get_work(work_id)
    attempt = work["current_attempt"]
    service.report(
        worker,
        str(attempt["id"]),
        ReportInput(
            kind=ReportKind.PROGRESS,
            expected_goal_version=int(work["goal_version"]),
            expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
            expected_task_packet_digest=str(attempt["task_packet_digest"]),
            expected_generation=int(work["generation"]),
            summary="The exact Attempt is still running.",
            idempotency_key="retry-working-progress",
        ),
    )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "managed_worker_thread_epochs",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.create_attempt(
            actor,
            work_id,
            reason="Do not retry an execution-owned Attempt.",
            idempotency_key="retry-working-blocked",
        )

    assert caught.value.details is not None
    assert caught.value.details["reason_code"] == "worker_command_outcome_unsettled"
    assert caught.value.details["blocker_kind"] == "work_not_settled"
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize(
    ("ingress", "delivery_state"),
    (("reply", "leased"), ("create_attempt", "dispatched")),
)
def test_claimed_delivery_blocks_reply_and_retry_before_facade_mutation(
    system: dict[str, Any], tmp_path: Path, ingress: str, delivery_state: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"{ingress}-delivery-block")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"{ingress}-delivery-block")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"{ingress}-delivery-block")
    work_id = str(instructed["task"]["work_item_id"])
    _question(service, worker, work_id, suffix=f"{ingress}-delivery-block")
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = 'assignment'",
        (work_id,),
    )
    assert assignment is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = ?, owner_token = ?, lease_until = ?, "
        "handled_at = NULL WHERE message_id = ?",
        (
            delivery_state,
            "active-ingress-claim",
            "2999-01-01T00:00:00Z",
            assignment["id"],
        ),
    )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        if ingress == "reply":
            service.reply(
                actor,
                work_id,
                "Do not cross the active delivery claim.",
                idempotency_key="blocked-reply-delivery",
            )
        else:
            service.create_attempt(
                actor,
                work_id,
                reason="Do not retry across a dispatched Delivery.",
                idempotency_key="blocked-retry-delivery",
            )

    assert caught.value.details is not None
    assert caught.value.details["reason_code"] == "worker_command_outcome_unsettled"
    assert caught.value.details["blocker_kind"] == "delivery_outcome_unsettled"
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_atomic_retry_same_key_converges_on_one_attempt_and_one_connection(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="retry-concurrent")
    thread_id = _new_thread(service, actor, tmp_path, suffix="retry-concurrent")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="retry-concurrent")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="retry-concurrent")
    _wait_user(service, actor, waiting, suffix="retry-concurrent")
    _disconnect_with_native(service, runtime_id, native_session_id="native-retry-concurrent")
    ready = Barrier(2)

    def retry_once() -> dict[str, Any]:
        ready.wait(timeout=5)
        return service.create_attempt(
            actor,
            work_id,
            reason="Retry this exact safe Attempt once.",
            idempotency_key="atomic-retry-same-key",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _ordinal: retry_once(), range(2)))

    assert results[0] == results[1]
    assert len(results[0]["attempts"]) == 2
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 2
    )
    latest = results[0]["current_attempt"]
    assert [row["kind"] for row in _attempt_messages(service, str(latest["id"]))] == ["assignment"]
    with pytest.raises(ConflictError, match="idempotency key"):
        service.create_attempt(
            actor,
            work_id,
            reason="A different request cannot reuse the same key.",
            idempotency_key="atomic-retry-same-key",
        )


def test_atomic_retry_rolls_back_boundary_epoch_attempt_and_delivery_on_injected_failure(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="retry-rollback")
    thread_id = _new_thread(service, actor, tmp_path, suffix="retry-rollback")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="retry-rollback")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="retry-rollback")
    _wait_user(service, actor, waiting, suffix="retry-rollback")
    _disconnect_with_native(service, runtime_id, native_session_id="native-retry-rollback")
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)
    original_message = service._message

    def fail_replacement_assignment(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if kwargs.get("kind") == "assignment":
            raise RuntimeError("injected replacement Assignment failure")
        return original_message(*args, **kwargs)

    monkeypatch.setattr(service, "_message", fail_replacement_assignment)
    with pytest.raises(RuntimeError, match="injected replacement Assignment failure"):
        service.create_attempt(
            actor,
            work_id,
            reason="This atomic retry is deliberately interrupted.",
            idempotency_key="atomic-retry-rollback",
        )

    assert _ledger_snapshot(service, snapshot_tables) == before


def test_matching_legacy_retry_boundary_residue_resumes_atomically(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="matching-retry-residue")
    thread_id = _new_thread(service, actor, tmp_path, suffix="matching-retry-residue")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="matching-retry-residue")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="matching-retry-residue")
    _wait_user(service, actor, waiting, suffix="matching-retry-residue")
    reason = "Resume the exact matching historical retry receipt."
    boundary = _record_retry_residue(
        service,
        actor,
        work_id,
        idempotency_key="matching-retry-residue",
        reason=reason,
        legacy=True,
    )

    result = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key="matching-retry-residue",
    )

    assert int(result["current_attempt"]["attempt_number"]) == 2
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )["n"]
        == 1
    )
    assert [
        row["kind"] for row in _attempt_messages(service, str(result["current_attempt"]["id"]))
    ] == ["assignment"]


def test_successful_disposed_retry_without_receipt_replays_proven_successor_once(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="disposed-retry-replay")
    thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-replay")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="disposed-retry-replay")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-retry-replay")
    _wait_user(service, actor, waiting, suffix="disposed-retry-replay")
    reason = "Replay only the proven successful retry transition."
    key = "disposed-retry-success-replay"
    first = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key=key,
    )
    service.db.execute(
        "DELETE FROM idempotency_results WHERE actor_id = ? "
        "AND operation = 'create_attempt' AND idempotency_key = ?",
        (actor["id"], key),
    )
    domain_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
    )
    before = _ledger_snapshot(service, domain_tables)

    replay = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key=key,
    )

    assert replay == first
    assert _ledger_snapshot(service, domain_tables) == before
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM idempotency_results WHERE actor_id = ? "
            "AND operation = 'create_attempt' AND idempotency_key = ?",
            (actor["id"], key),
        )["n"]
        == 1
    )


def test_disposed_retry_replay_survives_later_legitimate_work_evolution(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="disposed-retry-evolved")
    thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-evolved")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="disposed-retry-evolved")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-retry-evolved-first")
    _wait_user(service, actor, waiting, suffix="disposed-retry-evolved-first")
    reason = "Preserve the immutable retry even after later Work evolution."
    key = "disposed-retry-evolved"
    retried = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key=key,
    )
    waiting_again = _question(service, worker, work_id, suffix="disposed-retry-evolved-second")
    _wait_user(service, actor, waiting_again, suffix="disposed-retry-evolved-second")
    evolved = service.revise_goal(
        actor,
        work_id,
        GoalRevision(
            expected_version=1,
            objective="Produce the later, independently revised result.",
            maturity=GoalMaturity.DEFINED,
            acceptance=["The retry replay returns this current canonical Work."],
            reason="Legitimately evolve Work after the historical retry.",
            idempotency_key="evolve-after-retry",
        ),
    )
    assert evolved["current_attempt"]["id"] != retried["current_attempt"]["id"]
    service.db.execute(
        "DELETE FROM idempotency_results WHERE actor_id = ? "
        "AND operation = 'create_attempt' AND idempotency_key = ?",
        (actor["id"], key),
    )
    domain_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
    )
    before = _ledger_snapshot(service, domain_tables)

    replay = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key=key,
    )

    assert replay["current_attempt"]["id"] == evolved["current_attempt"]["id"]
    assert replay["goal_version"] == 2
    assert _ledger_snapshot(service, domain_tables) == before


def test_legacy_disposed_retry_to_different_managed_worker_replays_exact_runtime(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="disposed-retry-legacy-target")
    source_thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-legacy-source")
    target_thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-legacy-target")
    worker, _runtime_id = _enroll_current_runtime(service, source_thread_id)
    instructed = _instruct(service, actor, source_thread_id, suffix="disposed-retry-legacy-target")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-retry-legacy-target")
    _wait_user(service, actor, waiting, suffix="disposed-retry-legacy-target")
    target_route = _thread_route(service, target_thread_id)
    reason = "Replay the exact historical cross-Worker retry transition."
    key = "disposed-retry-legacy-target"
    retried = service.create_attempt(
        actor,
        work_id,
        worker_id=str(target_route["principal_id"]),
        reason=reason,
        idempotency_key=key,
        managed_worker_thread_id=target_thread_id,
        managed_worker_thread_generation=int(target_route["thread_generation"]),
    )
    successor = retried["current_attempt"]
    successor_runtime = str(successor["runtime_session_id"])
    boundary = service.db.fetchone(
        "SELECT * FROM boundaries WHERE source_principal_id = ? AND source_event_id = ?",
        (actor["id"], f"retry:{key}"),
    )
    assert boundary is not None
    disposition = service.db.fetchone(
        "SELECT * FROM boundary_dispositions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert disposition is not None
    prior = service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (boundary["attempt_id"],))
    assert prior is not None
    legacy_metadata = {
        "worker_id": str(target_route["principal_id"]),
        "runtime_session_id": successor_runtime,
    }
    legacy_boundary_digest = _digest(
        BoundaryInput(
            source_event_id=f"retry:{key}",
            work_item_id=work_id,
            attempt_id=str(prior["id"]),
            expected_goal_version=int(boundary["goal_version"]),
            expected_goal_packet_digest=str(prior["goal_packet_digest"]),
            expected_task_packet_digest=str(prior["task_packet_digest"]),
            expected_generation=int(boundary["generation"]),
            kind=BoundaryKind.RETRY_REQUEST,
            summary=reason,
            runtime_state=RuntimeState.WAITING,
            metadata=legacy_metadata,
        ).model_dump(mode="json")
    )
    legacy_disposition_digest = _digest(
        {
            "boundary_id": str(boundary["id"]),
            "generation": int(boundary["generation"]),
            "kind": BoundaryDispositionKind.RETRY.value,
            "reason": reason,
            "worker_id": str(target_route["principal_id"]),
            "runtime_session_id": successor_runtime,
        }
    )
    legacy_turn_input_digest = _digest(
        {
            "boundary_id": str(boundary["id"]),
            "boundary_input_digest": legacy_boundary_digest,
            "goal_packet_digest": str(boundary["goal_packet_digest"]),
            "task_packet_digest": str(boundary["task_packet_digest"]),
            "generation": int(boundary["generation"]),
        }
    )
    legacy_turn_result_digest = _digest(
        {
            "boundary_id": str(boundary["id"]),
            "disposition_id": str(disposition["id"]),
            "request_digest": legacy_disposition_digest,
            "goal_packet_digest": str(boundary["goal_packet_digest"]),
            "task_packet_digest": str(boundary["task_packet_digest"]),
        }
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE boundaries SET metadata_json = ?, input_digest = ? WHERE id = ?",
            (
                json.dumps(legacy_metadata, sort_keys=True, separators=(",", ":")),
                legacy_boundary_digest,
                boundary["id"],
            ),
        )
        connection.execute(
            "UPDATE boundary_dispositions SET request_digest = ? WHERE id = ?",
            (legacy_disposition_digest, disposition["id"]),
        )
        connection.execute(
            "UPDATE reasoner_turns SET input_digest = ?, result_digest = ? WHERE id = ?",
            (
                legacy_turn_input_digest,
                legacy_turn_result_digest,
                disposition["reasoner_turn_id"],
            ),
        )
        connection.execute(
            "DELETE FROM idempotency_results WHERE actor_id = ? "
            "AND operation = 'create_attempt' AND idempotency_key = ?",
            (actor["id"], key),
        )
    domain_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
    )
    before = _ledger_snapshot(service, domain_tables)

    replay = service.create_attempt(
        actor,
        work_id,
        worker_id=str(target_route["principal_id"]),
        runtime_session_id=successor_runtime,
        reason=reason,
        idempotency_key=key,
    )

    assert replay["id"] == retried["id"]
    assert replay["current_attempt"] == retried["current_attempt"]
    assert replay["managed_worker_thread_id"] == target_thread_id
    assert _ledger_snapshot(service, domain_tables) == before


def test_disposed_retry_without_a_successor_is_bounded_and_zero_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="disposed-retry-incomplete")
    thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-incomplete")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="disposed-retry-incomplete")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-retry-incomplete")
    _wait_user(service, actor, waiting, suffix="disposed-retry-incomplete")
    reason = "Do not accept a disposition without its exact successor."
    key = "disposed-retry-incomplete"
    boundary = _record_retry_residue(
        service,
        actor,
        work_id,
        idempotency_key=key,
        reason=reason,
    )
    work = service.get_work(work_id)
    attempt = work["current_attempt"]
    turn_id = "turn_disposed_retry_incomplete"
    disposition_id = "disp_disposed_retry_incomplete"
    disposition_digest = _digest(
        {
            "boundary_id": boundary["id"],
            "generation": work["generation"],
            "kind": BoundaryDispositionKind.RETRY.value,
            "reason": reason,
            "worker_id": work["assigned_worker_id"],
            "runtime_session_id": None,
            "managed_worker_thread_id": None,
            "managed_worker_thread_generation": None,
        }
    )
    turn_input_digest = _digest(
        {
            "boundary_id": boundary["id"],
            "boundary_input_digest": boundary["input_digest"],
            "goal_packet_digest": attempt["goal_packet_digest"],
            "task_packet_digest": attempt["task_packet_digest"],
            "generation": work["generation"],
        }
    )
    turn_result_digest = _digest(
        {
            "boundary_id": boundary["id"],
            "disposition_id": disposition_id,
            "request_digest": disposition_digest,
            "goal_packet_digest": attempt["goal_packet_digest"],
            "task_packet_digest": attempt["task_packet_digest"],
        }
    )
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO reasoner_turns(
                id, supervisor_id, work_item_id, boundary_id, generation,
                goal_version, goal_packet_digest, task_packet_digest,
                input_digest, result_digest, state, lease_token_digest,
                lease_expires_at, idempotency_key, completed_at,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', 'historical-lease',
                     '2026-08-21T00:00:00Z', '', '2026-08-21T00:00:00Z',
                     '2026-08-21T00:00:00Z', '2026-08-21T00:00:00Z')
            """,
            (
                turn_id,
                actor["id"],
                work_id,
                boundary["id"],
                work["generation"],
                work["goal_version"],
                attempt["goal_packet_digest"],
                attempt["task_packet_digest"],
                turn_input_digest,
                turn_result_digest,
            ),
        )
        connection.execute(
            """
            INSERT INTO boundary_dispositions(
                id, boundary_id, reasoner_turn_id, decided_by, generation,
                kind, reason, instruction, resume_condition,
                request_digest, created_at
            ) VALUES(?, ?, ?, ?, ?, 'retry', ?, '', '', ?,
                     '2026-08-21T00:00:00Z')
            """,
            (
                disposition_id,
                boundary["id"],
                turn_id,
                actor["id"],
                work["generation"],
                reason,
                disposition_digest,
            ),
        )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.create_attempt(
            actor,
            work_id,
            reason=reason,
            idempotency_key=key,
        )

    assert caught.value.details == {
        "reason_code": "retry_boundary_transition_incomplete",
        "retryable": False,
    }
    assert _ledger_snapshot(service, snapshot_tables) == before


def test_disposed_retry_successor_assignment_is_immutable_and_zero_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="disposed-retry-tampered")
    thread_id = _new_thread(service, actor, tmp_path, suffix="disposed-retry-tampered")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="disposed-retry-tampered")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="disposed-retry-tampered")
    _wait_user(service, actor, waiting, suffix="disposed-retry-tampered")
    reason = "Reject a retry whose successor provenance was altered."
    key = "disposed-retry-tampered"
    retried = service.create_attempt(
        actor,
        work_id,
        reason=reason,
        idempotency_key=key,
    )
    latest = retried["current_attempt"]
    assignment = service.db.fetchone(
        "SELECT id, payload_json FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (latest["id"],),
    )
    assert assignment is not None
    payload = json.loads(str(assignment["payload_json"]))
    payload["task_packet_digest"] = "0" * 64
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(
        sqlite3.IntegrityError,
        match="boundary continuation message is immutable",
    ):
        service.db.execute(
            "UPDATE messages SET payload_json = ? WHERE id = ?",
            (
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                assignment["id"],
            ),
        )

    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize("residue_state", ("unresolved", "disposed"))
def test_cross_work_retry_boundary_residue_is_bounded_and_zero_mutation(
    system: dict[str, Any], tmp_path: Path, residue_state: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"cross-retry-{residue_state}")

    def safe_work(suffix: str) -> str:
        thread_id = _new_thread(service, actor, tmp_path, suffix=suffix)
        worker, _runtime_id = _enroll_current_runtime(service, thread_id)
        instructed = _instruct(service, actor, thread_id, suffix=suffix)
        work_id = str(instructed["task"]["work_item_id"])
        waiting = _question(service, worker, work_id, suffix=suffix)
        _wait_user(service, actor, waiting, suffix=suffix)
        return work_id

    source_work_id = safe_work(f"cross-source-{residue_state}")
    target_work_id = safe_work(f"cross-target-{residue_state}")
    key = f"cross-work-retry-{residue_state}"
    reason = "Retry only the exact Work bound to this request."
    if residue_state == "unresolved":
        _record_retry_residue(
            service,
            actor,
            source_work_id,
            idempotency_key=key,
            reason=reason,
        )
    else:
        service.create_attempt(
            actor,
            source_work_id,
            reason=reason,
            idempotency_key=key,
        )
        service.db.execute(
            "DELETE FROM idempotency_results WHERE actor_id = ? "
            "AND operation = 'create_attempt' AND idempotency_key = ?",
            (actor["id"], key),
        )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        service.create_attempt(
            actor,
            target_work_id,
            reason=reason,
            idempotency_key=key,
        )

    assert caught.value.details == {
        "reason_code": "retry_boundary_provenance_conflict",
        "retryable": False,
    }
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize(
    "invalid_binding",
    ("missing", "incomplete", "boolean_generation", "stale_generation", "foreign"),
)
def test_retry_successor_rejects_nonexact_managed_pair_without_any_mutation(
    system: dict[str, Any], tmp_path: Path, invalid_binding: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"successor-invalid-{invalid_binding}")
    source_thread = _new_thread(
        service, actor, tmp_path, suffix=f"successor-source-{invalid_binding}"
    )
    source_worker, _source_runtime = _enroll_current_runtime(service, source_thread)
    instructed = _instruct(
        service, actor, source_thread, suffix=f"successor-source-{invalid_binding}"
    )
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(
        service, source_worker, work_id, suffix=f"successor-source-{invalid_binding}"
    )
    _wait_user(service, actor, waiting, suffix=f"successor-source-{invalid_binding}")
    successor_actor = actor
    if invalid_binding == "foreign":
        successor_actor = _attached(service, suffix="foreign-successor")
    successor_thread = _new_thread(
        service,
        successor_actor,
        tmp_path,
        suffix=f"successor-target-{invalid_binding}",
    )
    successor_route = _thread_route(service, successor_thread)
    thread_id: str | None = successor_thread
    generation: int | bool | None = 1
    if invalid_binding == "missing":
        thread_id = None
        generation = None
    elif invalid_binding == "incomplete":
        generation = None
    elif invalid_binding == "boolean_generation":
        generation = True
    elif invalid_binding == "stale_generation":
        generation = 2
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ControlPlaneError):
        service.create_attempt(
            actor,
            work_id,
            worker_id=str(successor_route["principal_id"]),
            reason="Reject a successor without exact logical authority.",
            idempotency_key=f"invalid-successor-{invalid_binding}",
            managed_worker_thread_id=thread_id,
            managed_worker_thread_generation=generation,  # type: ignore[arg-type]
        )

    assert _ledger_snapshot(service, snapshot_tables) == before


def test_full_mcp_retry_rebinds_to_exact_managed_successor_atomically(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="successor-valid")
    source_thread = _new_thread(service, actor, tmp_path, suffix="successor-valid-source")
    source_worker, _source_runtime = _enroll_current_runtime(service, source_thread)
    instructed = _instruct(service, actor, source_thread, suffix="successor-valid-source")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, source_worker, work_id, suffix="successor-valid-source")
    _wait_user(service, actor, waiting, suffix="successor-valid-source")
    successor_thread = _new_thread(service, actor, tmp_path, suffix="successor-valid-target")
    successor_route = _thread_route(service, successor_thread)

    result = MCPServer(service).call_tool(
        system["cao"],
        "cao_create_attempt",
        {
            "work_item_id": work_id,
            "worker_id": successor_route["principal_id"],
            "managed_worker_thread_id": successor_thread,
            "managed_worker_thread_generation": 1,
            "reason": "Move the exact Work to its explicit managed successor.",
            "idempotency_key": "exact-managed-successor",
        },
    )

    assert result["assigned_worker_id"] == successor_route["principal_id"]
    assert result["managed_worker_thread_id"] == successor_thread
    assert result["managed_worker_thread_generation"] == 1
    current = result["current_attempt"]
    assert current["runtime_session_id"] == successor_route["runtime_session_id"]
    assert int(current["attempt_number"]) == 2
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (successor_thread,),
        )["n"]
        == 1
    )
    assert [
        (row["kind"], row["delivery_state"])
        for row in _attempt_messages(service, str(current["id"]))
    ] == [("assignment", "queued")]
    retry_boundary = service.db.fetchone(
        "SELECT metadata_json FROM boundaries WHERE work_item_id = ? AND kind = 'retry_request'",
        (work_id,),
    )
    assert retry_boundary is not None
    metadata = json.loads(str(retry_boundary["metadata_json"]))
    assert metadata["managed_worker_thread_id"] == successor_thread
    assert metadata["managed_worker_thread_generation"] == 1


def test_managed_retry_rejects_even_a_current_runtime_override_without_mutation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="retry-runtime-selector")
    thread_id = _new_thread(service, actor, tmp_path, suffix="retry-runtime-selector")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix="retry-runtime-selector")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix="retry-runtime-selector")
    _wait_user(service, actor, waiting, suffix="retry-runtime-selector")
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "runtime_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ControlPlaneError) as caught:
        service.create_attempt(
            actor,
            work_id,
            runtime_session_id=runtime_id,
            reason="A managed retry cannot select its execution route.",
            idempotency_key="managed-retry-runtime-selector",
        )

    assert caught.value.details == {"reason_code": "managed_runtime_selection_forbidden"}
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize("command", ("revise", "reply", "retry"))
def test_legacy_null_work_binding_never_guesses_its_unique_managed_thread(
    system: dict[str, Any], tmp_path: Path, command: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"legacy-null-{command}")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"legacy-null-{command}")
    worker, _runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"legacy-null-{command}")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=f"legacy-null-{command}")
    _wait_user(service, actor, waiting, suffix=f"legacy-null-{command}")
    with service.db.transaction() as connection:
        connection.execute("DROP TRIGGER work_items_managed_thread_exact_update")
        connection.execute("DROP TRIGGER work_items_managed_thread_rebind_update")
        connection.execute(
            "UPDATE work_items SET managed_worker_thread_id = NULL, "
            "managed_worker_thread_generation = NULL WHERE id = ?",
            (work_id,),
        )
    snapshot_tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "work_items",
        "goal_revisions",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "messages",
        "message_deliveries",
        "managed_worker_thread_epochs",
        "events",
        "idempotency_results",
    )
    before = _ledger_snapshot(service, snapshot_tables)

    with pytest.raises(ConflictError) as caught:
        if command == "revise":
            service.revise_goal(
                actor,
                work_id,
                GoalRevision(
                    expected_version=1,
                    objective="Do not infer legacy logical authority.",
                    maturity=GoalMaturity.DEFINED,
                    acceptance=["The unresolved binding remains unchanged."],
                    reason="Exercise legacy binding admission.",
                    idempotency_key="legacy-null-revise",
                ),
            )
        elif command == "reply":
            service.reply(
                actor,
                work_id,
                "Use option A.",
                idempotency_key="legacy-null-reply",
            )
        else:
            service.create_attempt(
                actor,
                work_id,
                reason="Do not infer a retry lane.",
                idempotency_key="legacy-null-retry",
            )

    assert caught.value.details is not None
    assert caught.value.details["reason_code"] == "worker_command_binding_ambiguous"
    assert _ledger_snapshot(service, snapshot_tables) == before


@pytest.mark.parametrize("command", ("revise", "reply", "continue"))
def test_connected_commands_do_not_rotate_the_execution_connection(
    system: dict[str, Any], tmp_path: Path, command: str
) -> None:
    service = system["service"]
    actor = _attached(service, suffix=f"connected-{command}")
    thread_id = _new_thread(service, actor, tmp_path, suffix=f"connected-{command}")
    worker, runtime_id = _enroll_current_runtime(service, thread_id)
    instructed = _instruct(service, actor, thread_id, suffix=f"connected-{command}")
    work_id = str(instructed["task"]["work_item_id"])
    waiting = _question(service, worker, work_id, suffix=f"connected-{command}")
    if command in {"revise", "reply"}:
        _wait_user(service, actor, waiting, suffix=f"connected-{command}")
        if command == "revise":
            service.revise_goal(
                actor,
                work_id,
                GoalRevision(
                    expected_version=1,
                    objective="Revise without rotating the live connection.",
                    maturity=GoalMaturity.DEFINED,
                    acceptance=["The live connection generation remains one."],
                    reason="Exercise connected revision admission.",
                    idempotency_key="connected-revise",
                ),
            )
        else:
            service.reply(
                actor,
                work_id,
                "Use option A on the current connection.",
                idempotency_key="connected-reply",
            )
    else:
        boundary = waiting["open_boundaries"][-1]
        turn = service.acquire_reasoner_turn(
            actor,
            work_id,
            boundary_id=str(boundary["id"]),
            expected_generation=int(waiting["generation"]),
            idempotency_key="connected-continue-turn",
        )
        service.dispose_boundary(
            actor,
            str(boundary["id"]),
            BoundaryDispositionInput(
                turn_id=str(turn["id"]),
                lease_token=str(turn["lease_token"]),
                expected_generation=int(waiting["generation"]),
                kind=BoundaryDispositionKind.CONTINUE,
                reason="Continue on the exact connected Attempt.",
                instruction="Continue with option A.",
            ),
        )
    route = _thread_route(service, thread_id)
    assert route["runtime_session_id"] == runtime_id
    assert int(route["connection_generation"]) == 1
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread_id,),
        )["n"]
        == 1
    )


def test_partial_handshake_state_is_not_reused_as_a_pristine_pending_route(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    actor = _attached(service, suffix="partial-handshake")
    thread_id = _new_thread(service, actor, tmp_path, suffix="partial-handshake")
    original = _thread_route(service, thread_id)
    service.db.execute(
        "UPDATE worker_enrollments SET discovered_tools_digest = 'partial-proof' WHERE id = ?",
        (original["enrollment_id"],),
    )

    _instruct(service, actor, thread_id, suffix="partial-handshake")

    current = _thread_route(service, thread_id)
    assert current["runtime_session_id"] != original["runtime_session_id"]
    assert int(current["connection_generation"]) == 2


def test_projection_rejects_boolean_thread_generation() -> None:
    projection = _conversation_work_projection(
        {
            "managed_worker_thread_id": "mwt_boolean",
            "managed_worker_thread_generation": True,
        }
    )
    assert projection["worker_thread_binding_state"] == "historical_unresolved"
    assert "worker_thread_id" not in projection
    assert "worker_thread_generation" not in projection


def test_legacy_assignment_idempotency_digest_without_new_pair_fields_replays(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    request = WorkAssignment(
        worker_id=str(system["worker"]["id"]),
        title="Legacy assignment replay",
        objective="Replay the pre-thread-binding request digest.",
        acceptance=["No second Work or Attempt is created."],
        idempotency_key="legacy-assignment-pair-digest",
    )
    first = service.assign_work(system["cao"], request)
    legacy_fields = request.model_dump(mode="json")
    legacy_fields.pop("managed_worker_thread_id", None)
    legacy_fields.pop("managed_worker_thread_generation", None)
    legacy_fields.pop("completion_contract", None)
    service.db.execute(
        "UPDATE idempotency_results SET request_digest = ? "
        "WHERE actor_id = ? AND operation = 'assign_work' AND idempotency_key = ?",
        (
            _digest(legacy_fields),
            system["cao"]["id"],
            request.idempotency_key,
        ),
    )
    before = _ledger_snapshot(
        service,
        ("work_items", "attempts", "messages", "message_deliveries", "events"),
    )

    replay = service.assign_work(system["cao"], request)

    assert replay == first
    assert (
        _ledger_snapshot(
            service,
            ("work_items", "attempts", "messages", "message_deliveries", "events"),
        )
        == before
    )
