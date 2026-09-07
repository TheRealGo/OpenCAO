from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import sqlite3
import struct
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ValidationError,
)
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    GoalRevision,
    ReportInput,
    ReportKind,
    RuntimeDispatchResult,
    RuntimeState,
    WorkAssignment,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    DesktopCAOTerminalTurnEvidence,
    Dispatcher,
    RuntimeAdapterError,
    _codex_delivery_client_user_message_id,
    _JsonRpcDesktopSocket,
    _managed_codex_mcp_server_name,
    render_message,
)
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    receive_enrollment_capability,
)

_PROJECT_DIGEST = "a" * 64
_INVALID_TURN_CHRONOLOGIES = [
    (100, None),
    (None, 101),
    (True, 101),
    (100, True),
    (100.0, 101),
    (100, 101.0),
    ("100", 101),
    (100, "101"),
    (0, 101),
    (-1, 101),
    (102, 101),
    (100, 2**62),
]


def _attach(system: dict[str, Any], *, thread_id: str = "cao-thread") -> dict[str, Any]:
    return cast(
        dict[str, Any],
        attach_cao_session_with_peer(
            system["service"],
            current_cao_session_attachment(
                native_thread_id=thread_id,
                project_digest=_PROJECT_DIGEST,
                model="gpt-5.6-terra",
                sandbox="workspace-write",
            ),
        ),
    )


def _worker_boundary(
    system: dict[str, Any],
    attachment: dict[str, Any] | None = None,
    *,
    actor: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    work = service.assign_work(
        actor or system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Worker boundary",
            objective="Wake the attached CAO thread after a durable Worker report",
            acceptance=["CAO receives one durable boundary"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=(attachment or {}).get("id"),
            supervisor_project_digest=(attachment or {}).get("project_digest"),
        ),
    )
    attempt = work["current_attempt"]
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Worker has reached a durable review boundary",
            idempotency_key=f"cao-wake-boundary:{work['id']}",
        ),
    )
    return work, reported


def _terminal_cao_proof(
    attachment: Mapping[str, Any], delivery: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "native_thread_id": str(attachment["native_thread_id"]),
        "client_user_message_id": _codex_delivery_client_user_message_id(
            {"id": delivery["message_id"]}
        ),
        "native_turn_id": "provider-terminal-wake-turn",
        "terminal_status": "completed",
        "started_at": 100,
        "completed_at": 101,
    }


class _CAOBrokerAdapter:
    name = "codex-app-server"

    def __init__(
        self,
        service: Any,
        *,
        fail_before_exchange: bool = False,
        fail_after_start: bool = False,
        consume_delivery: bool = False,
        acquire_without_disposition: bool = False,
    ) -> None:
        self.service = service
        self.fail_before_exchange = fail_before_exchange
        self.fail_after_start = fail_after_start
        self.consume_delivery = consume_delivery
        self.acquire_without_disposition = acquire_without_disposition
        self.calls: list[dict[str, Any]] = []

    async def dispatch(
        self, runtime: Mapping[str, Any], message: Mapping[str, Any]
    ) -> RuntimeDispatchResult:
        broker = runtime.get("_enrollment_capability_broker")
        assert isinstance(broker, EnrollmentCapabilityBroker)
        broker.bind_runner_pid(os.getpid())
        capability_path = runtime.get("cao_runtime_capability_socket")
        assert capability_path == broker.path
        if self.fail_before_exchange:
            self.calls.append({"runtime": dict(runtime), "message": dict(message), "actor": None})
            return RuntimeDispatchResult(
                success=False,
                state=RuntimeState.FAILED,
                error="app-server exited before capability exchange",
            )
        exchange = await receive_enrollment_capability(broker.path, timeout_seconds=2)
        actor = self.service.authenticate(str(exchange["token"]))
        assert actor["_runtime_session_id"] == runtime["id"]
        assert actor["_native_thread_id"] == runtime["native_session_id"]
        tools = {tool["name"] for tool in MCPServer(self.service).tools_for(actor)}
        assert "cao_receive_intent" not in tools
        assert "cao_dispose_boundary" in tools
        self.calls.append({"runtime": dict(runtime), "message": dict(message), "actor": actor})
        if self.acquire_without_disposition:
            self.service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
            payload = message["payload"]
            self.service.acquire_reasoner_turn(
                actor,
                str(message["work_item_id"]),
                boundary_id=str(payload["boundary_id"]),
                expected_generation=int(
                    self.service.get_work(str(message["work_item_id"]))["generation"]
                ),
                idempotency_key=f"incomplete:{message['id']}",
            )
        if self.consume_delivery:
            self.service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
            boundary_id = str(message["payload"].get("boundary_id") or "")
            if boundary_id:
                work = self.service.get_work(str(message["work_item_id"]))
                turn = self.service.acquire_reasoner_turn(
                    actor,
                    str(message["work_item_id"]),
                    boundary_id=boundary_id,
                    expected_generation=int(work["generation"]),
                    idempotency_key=f"consume:{message['id']}",
                )
                self.service.dispose_boundary(
                    actor,
                    boundary_id,
                    BoundaryDispositionInput(
                        turn_id=turn["id"],
                        lease_token=turn["lease_token"],
                        expected_generation=int(work["generation"]),
                        kind=(
                            BoundaryDispositionKind.ACCEPT
                            if message["kind"] == ReportKind.COMPLETION_CLAIM.value
                            else BoundaryDispositionKind.CONTINUE
                        ),
                        reason="the resumed CAO turn incorporated the Worker boundary",
                    ),
                )
            self.service.mark_message_handled(
                actor,
                str(message["id"]),
                evidence="the resumed CAO turn committed its boundary disposition",
            )
        if self.fail_after_start:
            return RuntimeDispatchResult(
                success=False,
                state=RuntimeState.FAILED,
                error="app-server connection lost after turn/start",
            )
        return RuntimeDispatchResult(
            success=True,
            native_session_id=str(runtime["native_session_id"]),
            state=RuntimeState.READY,
            output="local app-server double accepted the resumed turn",
        )


class _Registry:
    def __init__(self, adapter: _CAOBrokerAdapter) -> None:
        self.adapter = adapter

    def get(self, name: str) -> _CAOBrokerAdapter:
        assert name == "codex-app-server"
        return self.adapter


def _boundary_delivery(system: dict[str, Any], work_id: str) -> Mapping[str, Any]:
    row = system["service"].db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE d.recipient_id = ? AND m.work_item_id = ?
          AND json_extract(m.payload_json, '$.boundary_id') IS NOT NULL
        """,
        (system["cao"]["id"], work_id),
    )
    assert row is not None
    return cast(Mapping[str, Any], row)


def test_live_completion_wake_renders_as_supervisor_boundary(
    system: dict[str, Any],
) -> None:
    """The real CAO inbox shape must preserve normal completion semantics."""

    attachment = _attach(system, thread_id="normal-completion-render")
    work, _ = _worker_boundary(system, attachment)
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    items = system["service"].get_inbox(conversation_actor)["items"]
    message = next(item for item in items if item["work_item_id"] == work["id"])

    assert message["kind"] == ReportKind.COMPLETION_CLAIM.value
    assert message.get("recovery_action") is None
    rendered = render_message(message)
    assert "Supervisor boundary ready for disposition" in rendered
    assert "dispose this boundary exactly once" in rendered
    assert "system-owned recovery Boundary" not in rendered
    assert f"Delivery message ID: {message['id']}" in rendered
    assert "owns only the Delivery message ID above" in rendered
    assert "do not drain" in rendered.lower()


@pytest.mark.parametrize(
    ("lease_until", "owner_token"),
    [(None, ""), ("2000-01-01T00:00:00Z", "stopped-dispatcher")],
)
def test_settled_unknown_cao_wake_does_not_block_a_later_work_completion(
    system: dict[str, Any], lease_until: str | None, owner_token: str
) -> None:
    attachment = _attach(system, thread_id="unknown-before-later-completion")
    first_work, _ = _worker_boundary(system, attachment)
    first_delivery = _boundary_delivery(system, first_work["id"])
    system["service"].db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dispatched', attempts = 1, lease_until = ?,
            owner_token = ?, last_error = 'runtime_dispatch_failed'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (lease_until, owner_token, first_delivery["message_id"], system["cao"]["id"]),
    )
    unknown_before = dict(
        system["service"].db.fetchone(
            """
            SELECT state, generation, attempts, lease_until, owner_token,
                   last_error, updated_at
            FROM message_deliveries
            WHERE message_id = ? AND recipient_id = ?
            """,
            (first_delivery["message_id"], system["cao"]["id"]),
        )
    )

    second_work, _ = _worker_boundary(system, attachment)
    second_delivery = _boundary_delivery(system, second_work["id"])
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    visible_ids = {item["id"] for item in system["service"].get_inbox(conversation_actor)["items"]}
    # Transport uncertainty is evidence, not an inbox visibility fence. It is
    # still unavailable for acknowledgement and cannot be blindly replayed.
    assert first_delivery["message_id"] in visible_ids
    assert second_delivery["message_id"] in visible_ids
    with pytest.raises(ConflictError, match="unknown CAO handoff"):
        system["service"].acknowledge(
            conversation_actor,
            AckInput(message_ids=[str(first_delivery["message_id"])]),
        )
    adapter = _CAOBrokerAdapter(system["service"], consume_delivery=True)
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert [call["message"]["id"] for call in adapter.calls] == [second_delivery["message_id"]]
    assert (
        system["service"].db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (second_delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "handled"
    )
    unknown_after = dict(
        system["service"].db.fetchone(
            """
            SELECT state, generation, attempts, lease_until, owner_token,
                   last_error, updated_at
            FROM message_deliveries
            WHERE message_id = ? AND recipient_id = ?
            """,
            (first_delivery["message_id"], system["cao"]["id"]),
        )
    )
    assert unknown_after == unknown_before


def test_terminal_acknowledged_cao_wake_does_not_block_a_later_work_completion(
    system: dict[str, Any],
) -> None:
    """A disposed Work must not leave its attachment lane permanently fenced."""

    attachment = _attach(system, thread_id="terminal-ack-before-later-completion")
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    first_work, _ = _worker_boundary(system, attachment)
    first_delivery = _boundary_delivery(system, first_work["id"])
    first_message = next(
        item
        for item in system["service"].get_inbox(conversation_actor)["items"]
        if item["id"] == first_delivery["message_id"]
    )
    system["service"].acknowledge(
        conversation_actor,
        AckInput(message_ids=[str(first_message["id"])]),
    )
    system["service"].db.execute(
        "UPDATE runtime_sessions SET state = 'busy' WHERE id = ?",
        (system["runtime"]["id"],),
    )
    system["service"].cancel_work(
        conversation_actor,
        str(first_work["id"]),
        "This Work is no longer required.",
        idempotency_key="cancel-first-terminal-wake",
    )
    assert system["service"].get_work(str(first_work["id"]))["state"] == "canceled"
    assert (
        system["service"].db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (first_delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "acknowledged"
    )

    second_work, _ = _worker_boundary(system, attachment)
    second_delivery = _boundary_delivery(system, second_work["id"])
    adapter = _CAOBrokerAdapter(system["service"], consume_delivery=True)
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert [call["message"]["id"] for call in adapter.calls] == [second_delivery["message_id"]]
    first_after = system["service"].db.fetchone(
        "SELECT state, handled_at, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (first_delivery["message_id"], system["cao"]["id"]),
    )
    assert first_after is not None
    assert dict(first_after) == {
        "state": "handled",
        "handled_at": first_after["handled_at"],
        "last_error": "",
    }
    assert first_after["handled_at"]
    audit = system["service"].db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'message.handled' "
        "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
        (first_delivery["message_id"],),
    )
    assert audit is not None
    assert json.loads(str(audit["data_json"]))["reason_code"] == ("terminal_work_cao_wake_closed")
    assert asyncio.run(dispatcher.run_once()) == 0
    assert [call["message"]["id"] for call in adapter.calls] == [second_delivery["message_id"]]


@pytest.mark.parametrize(
    ("delivery_state", "expected_state"),
    [
        ("queued", "dead"),
        ("leased", "dead"),
        ("delivered", "dead"),
        ("dispatched", "dispatched"),
    ],
)
def test_terminal_cao_wake_reconciliation_preserves_only_unknown_dispatch(
    system: dict[str, Any], delivery_state: str, expected_state: str
) -> None:
    attachment = _attach(system, thread_id=f"terminal-cao-wake-{delivery_state}")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = ?, owner_token = ?, lease_until = ? "
        "WHERE message_id = ? AND recipient_id = ?",
        (
            delivery_state,
            "active-dispatcher" if delivery_state in {"leased", "dispatched"} else "",
            "2099-01-01T00:00:00Z" if delivery_state in {"leased", "dispatched"} else None,
            delivery["message_id"],
            system["cao"]["id"],
        ),
    )
    system["service"].db.execute(
        "UPDATE work_items SET state = 'failed', attention_owner = 'none' WHERE id = ?",
        (work["id"],),
    )

    expected_settled = 0 if delivery_state == "dispatched" else 1
    assert system["service"].reconcile_terminal_cao_wake_delivery_lanes() == expected_settled
    persisted = system["service"].db.fetchone(
        "SELECT state, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted is not None and persisted["state"] == expected_state
    if expected_state == "dead":
        assert persisted["last_error"] == "terminal_work_cao_wake_closed"
    else:
        assert persisted["last_error"] == ""
    assert system["service"].reconcile_terminal_cao_wake_delivery_lanes() == 0
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM messages WHERE id = ?",
            (delivery["message_id"],),
        )["count"]
        == 1
    )


def test_nonterminal_acknowledged_cao_wake_remains_actionable(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="nonterminal-acknowledged-cao-wake")
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].acknowledge(
        conversation_actor,
        AckInput(message_ids=[str(delivery["message_id"])]),
    )

    assert system["service"].get_work(str(work["id"]))["state"] == "waiting_supervisor"
    assert system["service"].reconcile_terminal_cao_wake_delivery_lanes() == 0
    assert (
        system["service"].db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "acknowledged"
    )


def test_completion_boundary_coalesces_older_queued_progress_wake(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="progress-before-completion")
    work = system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Progress then completion",
            objective="Reach one durable completion boundary",
            acceptance=["CAO receives the completion boundary"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Implementation reached the final verification stage",
            idempotency_key="progress-before-completion",
        ),
    )
    progress_delivery = system["service"].db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.work_item_id = ? AND m.kind = 'progress'
        """,
        (work["id"],),
    )
    assert progress_delivery is not None and progress_delivery["state"] == "queued"
    system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The requested outcome and verification are complete",
            idempotency_key="completion-after-progress",
        ),
    )
    completion_delivery = _boundary_delivery(system, work["id"])
    adapter = _CAOBrokerAdapter(system["service"], consume_delivery=True)
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert [call["message"]["id"] for call in adapter.calls] == [completion_delivery["message_id"]]
    coalesced = system["service"].db.fetchone(
        """
        SELECT state, last_error FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (progress_delivery["message_id"], system["cao"]["id"]),
    )
    assert coalesced is not None
    assert dict(coalesced) == {
        "state": "dead",
        "last_error": "cao_progress_wake_superseded_by_boundary",
    }


def test_completion_boundary_releases_older_acknowledged_progress_wake(
    system: dict[str, Any],
) -> None:
    """A crash after Ack must not wedge every later wake on the attachment."""

    attachment = _attach(system, thread_id="acknowledged-progress-before-completion")
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    work = system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Acknowledged progress then completion",
            objective="Reach one durable completion boundary after an interrupted progress turn",
            acceptance=["CAO receives the completion boundary"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The interrupted CAO turn incorporated this progress update",
            idempotency_key="acknowledged-progress-before-completion",
        ),
    )
    progress_delivery = system["service"].db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.work_item_id = ? AND m.kind = 'progress'
        """,
        (work["id"],),
    )
    assert progress_delivery is not None and progress_delivery["state"] == "queued"
    system["service"].acknowledge(
        conversation_actor,
        AckInput(message_ids=[str(progress_delivery["message_id"])]),
    )
    assert (
        system["service"].db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (progress_delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "acknowledged"
    )

    system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The requested outcome and verification are complete",
            idempotency_key="completion-after-acknowledged-progress",
        ),
    )
    completion_delivery = _boundary_delivery(system, work["id"])
    adapter = _CAOBrokerAdapter(system["service"], consume_delivery=True)
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert [call["message"]["id"] for call in adapter.calls] == [completion_delivery["message_id"]]
    coalesced = system["service"].db.fetchone(
        """
        SELECT state, last_error FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (progress_delivery["message_id"], system["cao"]["id"]),
    )
    assert coalesced is not None
    assert dict(coalesced) == {
        "state": "dead",
        "last_error": "cao_progress_wake_superseded_by_boundary",
    }


def test_worker_boundary_resumes_the_exact_attached_cao_thread(system: dict[str, Any]) -> None:
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    assert delivery["runtime_session_id"] == attachment["runtime_session_id"]

    adapter = _CAOBrokerAdapter(system["service"], acquire_without_disposition=True)
    assert (
        asyncio.run(
            Dispatcher(
                system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
            ).run_once()
        )
        == 1
    )

    assert adapter.calls[0]["runtime"]["native_session_id"] == "cao-thread"
    persisted = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted["state"] == "acknowledged"
    recovery = system["service"].db.fetchone(
        """
        SELECT d.state FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.work_item_id = ?
          AND json_extract(m.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        """,
        (work["id"],),
    )
    assert recovery is None
    turn = system["service"].db.fetchone(
        "SELECT state FROM reasoner_turns WHERE work_item_id = ?", (work["id"],)
    )
    assert turn is not None and turn["state"] == "leased"
    assert system["service"].get_runtime(attachment["runtime_session_id"])["state"] == "waiting"
    dumped = system["service"].db.fetchall("SELECT * FROM cao_runtime_credentials")
    assert dumped and dumped[0]["state"] == "revoked"
    assert "cao.crc_" not in str(system["service"].list_runtimes())


def test_each_incomplete_recovery_turn_schedules_one_successor_wake(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="state-change-only-wake")
    work, _ = _worker_boundary(system, attachment)
    adapter = _CAOBrokerAdapter(system["service"])
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert asyncio.run(dispatcher.run_once()) == 1

    recoveries = system["service"].db.fetchall(
        """
        SELECT m.id, d.state FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.work_item_id = ?
          AND json_extract(m.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        ORDER BY m.sequence
        """,
        (work["id"],),
    )
    assert [row["state"] for row in recoveries] == ["dead", "queued"]
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'reasoner.incomplete_turn_recovery_scheduled'"
        )["count"]
        == 2
    )
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'reasoner.incomplete_turn_recovery_exhausted'"
        )["count"]
        == 0
    )
    turns = system["service"].db.fetchall(
        "SELECT state FROM reasoner_turns WHERE work_item_id = ? ORDER BY created_at",
        (work["id"],),
    )
    assert turns == []
    assert system["service"].reconcile_cao_supervision_obligations() == 0


def test_expired_then_incomplete_turn_each_schedule_one_successor_wake(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="cross-mode-recovery-budget")
    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    work, _ = _worker_boundary(system, attachment)
    current = system["service"].get_work(work["id"])
    boundary_id = str(current["open_boundaries"][0]["id"])
    first_turn = system["service"].acquire_reasoner_turn(
        conversation_actor,
        work["id"],
        boundary_id=boundary_id,
        expected_generation=work["generation"],
        idempotency_key="cross-mode-expiring-turn",
    )
    with system["service"].db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", first_turn["id"]),
        )
    assert system["service"].recover_expired_reasoner_turns() == 1

    adapter = _CAOBrokerAdapter(system["service"])
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )
    assert asyncio.run(dispatcher.run_once()) == 1

    recoveries = system["service"].db.fetchall(
        """
        SELECT json_extract(m.payload_json, '$.action') AS action, d.state
        FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.work_item_id = ?
          AND json_extract(m.payload_json, '$.action') IN (
              'recover_expired_reasoner_turn',
              'recover_incomplete_reasoner_turn'
          )
        ORDER BY m.sequence
        """,
        (work["id"],),
    )
    assert [(row["action"], row["state"]) for row in recoveries] == [
        ("recover_expired_reasoner_turn", "dead"),
        ("recover_incomplete_reasoner_turn", "queued"),
    ]
    assert system["service"].reconcile_cao_supervision_obligations() == 0


def test_cao_can_ack_and_handle_its_boundary_during_the_resumed_turn(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    adapter = _CAOBrokerAdapter(system["service"], consume_delivery=True)

    assert (
        asyncio.run(
            Dispatcher(
                system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
            ).run_once()
        )
        == 1
    )

    persisted = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted["state"] == "handled"
    assert system["service"].get_runtime(attachment["runtime_session_id"])["state"] == "waiting"
    credential = system["service"].db.fetchone(
        "SELECT state FROM cao_runtime_credentials ORDER BY created_at DESC LIMIT 1"
    )
    assert credential["state"] == "revoked"


def test_no_attachment_means_worker_report_does_not_autonomously_dispatch_cao(
    system: dict[str, Any],
) -> None:
    work, _ = _worker_boundary(system)
    delivery = _boundary_delivery(system, work["id"])
    assert delivery["runtime_session_id"] is None
    system["service"].db.execute(
        """
        UPDATE message_deliveries SET state = 'handled'
        WHERE recipient_id = ? AND state = 'queued' AND message_id <> ?
        """,
        (system["worker"]["id"], delivery["message_id"]),
    )
    assert asyncio.run(Dispatcher(system["service"], system["settings"]).run_once()) == 0
    assert delivery["state"] == "queued"


def test_progress_burst_gives_each_delivery_its_own_exact_cao_wake(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="progress-burst-wake")
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Milestone reporting",
            objective="Wake the exact CAO conversation for meaningful progress",
            acceptance=["Every progress Delivery owns exactly one queued CAO wake"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    common = {
        "expected_goal_version": work["goal_version"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
        "expected_generation": work["generation"],
    }

    for index, summary in enumerate(("First milestone", "Second milestone"), start=1):
        service.report(
            system["worker"],
            attempt["id"],
            ReportInput(
                kind=ReportKind.PROGRESS,
                summary=summary,
                idempotency_key=f"progress-{index}",
                **common,
            ),
        )
    progress = service.db.fetchall(
        """
        SELECT m.id, m.payload_json, d.state, d.runtime_session_id
        FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.attempt_id = ? AND m.kind = 'progress'
        ORDER BY m.sequence
        """,
        (attempt["id"],),
    )
    assert [json.loads(row["payload_json"])["summary"] for row in progress] == [
        "First milestone",
        "Second milestone",
    ]
    assert [row["state"] for row in progress] == ["queued", "queued"]
    assert {row["runtime_session_id"] for row in progress} == {attachment["runtime_session_id"]}
    assert verify_projection(service.db).healthy

    adapter = _CAOBrokerAdapter(service, consume_delivery=True)
    dispatcher = Dispatcher(service, system["settings"], registry=cast(Any, _Registry(adapter)))
    assert asyncio.run(dispatcher.run_once()) == 1
    assert len(adapter.calls) == 1
    assert adapter.calls[0]["runtime"]["native_session_id"] == "progress-burst-wake"
    first_message_id = adapter.calls[0]["message"]["id"]
    assert [
        row["state"]
        for row in service.db.fetchall(
            """
            SELECT d.state FROM messages AS m
            JOIN message_deliveries AS d ON d.message_id = m.id
            WHERE m.attempt_id = ? AND m.kind = 'progress'
            ORDER BY m.sequence
            """,
            (attempt["id"],),
        )
    ] == ["handled", "queued"]

    assert asyncio.run(dispatcher.run_once()) == 1
    assert len(adapter.calls) == 2
    assert adapter.calls[1]["runtime"]["native_session_id"] == "progress-burst-wake"
    assert adapter.calls[1]["message"]["id"] != first_message_id
    assert [
        row["state"]
        for row in service.db.fetchall(
            """
            SELECT d.state FROM messages AS m
            JOIN message_deliveries AS d ON d.message_id = m.id
            WHERE m.attempt_id = ? AND m.kind = 'progress'
            ORDER BY m.sequence
            """,
            (attempt["id"],),
        )
    ] == ["handled", "handled"]
    assert asyncio.run(dispatcher.run_once()) == 0


def test_artifact_only_report_remains_durable_without_cao_wake(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="artifact-history-only")
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Artifact history",
            objective="Preserve an artifact report without waking CAO",
            acceptance=["Artifact-only report has no CAO delivery"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.ARTIFACT,
            summary="Artifact recorded",
            artifacts=[
                ArtifactInput(
                    name="result.json",
                    uri="https://example.invalid/result.json",
                    media_type="application/json",
                    digest="b" * 64,
                )
            ],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
        ),
    )
    artifact = service.db.fetchone(
        """
        SELECT m.id, COUNT(d.message_id) AS delivery_count
        FROM messages AS m
        LEFT JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.attempt_id = ? AND m.kind = 'artifact'
        GROUP BY m.id
        """,
        (attempt["id"],),
    )
    assert artifact is not None and artifact["delivery_count"] == 0
    assert verify_projection(service.db).healthy


def test_mixed_progress_and_blocker_burst_coalesces_progress_and_disposes_boundary(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="mixed-report-burst")
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Mixed report burst",
            objective="Supervise progress and a later blocker in one CAO turn",
            acceptance=["The blocker is disposed before its delivery is handled"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    common = {
        "expected_goal_version": work["goal_version"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
        "expected_generation": work["generation"],
    }
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            summary="Reached the transfer step",
            idempotency_key="mixed-progress",
            **common,
        ),
    )
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.BLOCKER,
            summary="Transfer needs a CAO decision",
            idempotency_key="mixed-blocker",
            **common,
        ),
    )
    boundary_id = str(reported["open_boundaries"][0]["id"])

    adapter = _CAOBrokerAdapter(service, consume_delivery=True)
    dispatcher = Dispatcher(service, system["settings"], registry=cast(Any, _Registry(adapter)))
    assert asyncio.run(dispatcher.run_once()) == 1
    assert len(adapter.calls) == 1
    disposition = service.db.fetchone(
        "SELECT id FROM boundary_dispositions WHERE boundary_id = ?",
        (boundary_id,),
    )
    assert disposition is not None
    states = service.db.fetchall(
        """
        SELECT message.kind, delivery.state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind IN ('progress', 'blocker')
        ORDER BY message.sequence
        """,
        (attempt["id"],),
    )
    assert [(row["kind"], row["state"]) for row in states] == [
        ("progress", "dead"),
        ("blocker", "handled"),
    ]


@pytest.mark.parametrize("kind", [ReportKind.QUESTION, ReportKind.BLOCKER])
def test_supervisor_report_boundaries_queue_the_exact_cao_conversation(
    system: dict[str, Any], kind: ReportKind
) -> None:
    attachment = _attach(system, thread_id=f"{kind.value}-wake")
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title=f"{kind.value} wake",
            objective="Wake the exact delegating CAO conversation",
            acceptance=["The report has one exact-thread delivery"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=kind,
            summary=f"Worker submitted a {kind.value}",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
        ),
    )
    delivery = service.db.fetchone(
        """
        SELECT d.state, d.runtime_session_id
        FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.attempt_id = ? AND m.kind = ?
        """,
        (attempt["id"], kind.value),
    )
    assert delivery is not None
    assert delivery["state"] == "queued"
    assert delivery["runtime_session_id"] == attachment["runtime_session_id"]


def test_stale_or_replayed_cao_ticket_is_rejected(system: dict[str, Any]) -> None:
    attachment = _attach(system)
    renewed = _attach(system)
    assert renewed["id"] == attachment["id"]
    issued = system["service"].issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    system["service"].fail_cao_session_attachment(
        attachment["runtime_session_id"], reason="attachment changed before launch"
    )
    with pytest.raises(AuthenticationError):
        system["service"].exchange_cao_runtime_launch_ticket(issued["ticket"])


def test_unconsumed_cao_wake_retries_the_exact_conversation(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="retry-unstarted-wake")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    failing = _CAOBrokerAdapter(system["service"], fail_before_exchange=True)
    first_dispatcher = Dispatcher(
        system["service"],
        system["settings"],
        registry=cast(Any, _Registry(failing)),
    )

    assert asyncio.run(first_dispatcher.run_once()) == 1
    retried = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, next_attempt_at, runtime_session_id
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert retried is not None
    assert retried["state"] == "queued"
    assert retried["generation"] == int(delivery["generation"]) + 1
    assert retried["attempts"] == 1
    assert retried["runtime_session_id"] == attachment["runtime_session_id"]
    ticket = system["service"].db.fetchone(
        """
        SELECT state, consumed_at FROM cao_runtime_tickets
        WHERE attachment_id = ? ORDER BY created_at DESC LIMIT 1
        """,
        (attachment["id"],),
    )
    assert ticket is not None
    assert ticket["state"] == "revoked"
    assert ticket["consumed_at"] is None
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["generation"] == attachment["generation"]
    assert current_attachment["runtime"]["state"] == "waiting"
    retry_event = system["service"].db.fetchone(
        """
        SELECT data_json FROM events
        WHERE event_type = 'runtime.message_retry_scheduled'
        ORDER BY sequence DESC LIMIT 1
        """
    )
    assert retry_event is not None
    assert json.loads(retry_event["data_json"])["start_proof"] == ("cao_runtime_ticket_unconsumed")

    system["service"].db.execute(
        """
        UPDATE message_deliveries SET next_attempt_at = '2000-01-01T00:00:00Z'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    succeeding = _CAOBrokerAdapter(system["service"], consume_delivery=True)
    second_dispatcher = Dispatcher(
        system["service"],
        system["settings"],
        registry=cast(Any, _Registry(succeeding)),
    )
    assert asyncio.run(second_dispatcher.run_once()) == 1
    final = system["service"].db.fetchone(
        """
        SELECT state FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert final is not None and final["state"] == "handled"
    assert succeeding.calls[0]["runtime"]["native_session_id"] == ("retry-unstarted-wake")


def test_desktop_pre_turn_wake_failure_retries_without_revoking_the_conversation(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unavailable Desktop socket is retryable, not an attachment failure."""

    attachment = _attach(system, thread_id="desktop-pre-turn-retry")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    unavailable_socket = Path("/tmp") / f"cao-unavailable-{secrets.token_hex(8)}.sock"
    monkeypatch.setattr(
        "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", unavailable_socket
    )

    assert asyncio.run(Dispatcher(system["service"], system["settings"]).run_once()) == 1
    retried = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, last_error
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert retried is not None
    assert retried["state"] == "queued"
    assert retried["generation"] == int(delivery["generation"]) + 1
    assert retried["attempts"] == 1
    assert retried["last_error"] == "desktop_wake_pre_start_unavailable"
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["generation"] == attachment["generation"]
    assert current_attachment["runtime"]["state"] == "waiting"
    assert (
        system["service"].db.fetchone("SELECT COUNT(*) AS count FROM cao_runtime_tickets")["count"]
        == 0
    )
    assert (
        system["service"].db.fetchone("SELECT COUNT(*) AS count FROM cao_runtime_credentials")[
            "count"
        ]
        == 0
    )
    retry_event = system["service"].db.fetchone(
        """
        SELECT data_json FROM events WHERE event_type = 'runtime.message_retry_scheduled'
        ORDER BY sequence DESC LIMIT 1
        """
    )
    assert retry_event is not None
    assert json.loads(retry_event["data_json"])["start_proof"] == "desktop_host_pre_turn"


def test_consumed_cao_ticket_cannot_requeue_a_dispatched_delivery(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="consumed-ticket-fence")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    dispatcher = Dispatcher(system["service"], system["settings"])
    issued = system["service"].issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    system["service"].exchange_cao_runtime_launch_ticket(issued["ticket"])
    system["service"].db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dispatched', owner_token = ?
        WHERE message_id = ? AND recipient_id = ? AND generation = ?
        """,
        (
            dispatcher.owner_token,
            delivery["message_id"],
            system["cao"]["id"],
            delivery["generation"],
        ),
    )

    assert not dispatcher._retry_unstarted_cao_delivery(
        delivery,
        ticket_id=issued["ticket_id"],
        error_code="runtime_dispatch_failed",
    )
    persisted = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, owner_token
        FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted is not None
    assert persisted["state"] == "dispatched"
    assert persisted["generation"] == delivery["generation"]
    assert persisted["attempts"] == delivery["attempts"]
    assert persisted["owner_token"] == dispatcher.owner_token
    ticket = system["service"].db.fetchone(
        "SELECT state, consumed_at FROM cao_runtime_tickets WHERE id = ?",
        (issued["ticket_id"],),
    )
    assert ticket is not None
    assert ticket["state"] == "consumed"
    assert ticket["consumed_at"] is not None


def test_post_turn_start_failure_is_unknown_and_is_not_retried(system: dict[str, Any]) -> None:
    attachment = _attach(system)
    conversation_token = str(attachment["context_token"])
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    adapter = _CAOBrokerAdapter(system["service"], fail_after_start=True)
    dispatcher = Dispatcher(
        system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
    )
    assert asyncio.run(dispatcher.run_once()) == 1
    persisted = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted["state"] == "dispatched"
    assert persisted["generation"] == delivery["generation"]
    assert persisted["attempts"] == int(delivery["attempts"]) + 1
    consumed_ticket = system["service"].db.fetchone(
        """
        SELECT state, consumed_at FROM cao_runtime_tickets
        WHERE attachment_id = ? ORDER BY created_at DESC LIMIT 1
        """,
        (attachment["id"],),
    )
    assert consumed_ticket is not None
    assert consumed_ticket["state"] == "consumed"
    assert consumed_ticket["consumed_at"] is not None
    assert asyncio.run(dispatcher.run_once()) == 0
    assert system["service"].get_runtime(adapter.calls[0]["runtime"]["id"])["state"] == "waiting"
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["generation"] == attachment["generation"]
    conversation_actor = system["service"].authenticate(conversation_token)
    assert conversation_actor["_cao_attachment_id"] == attachment["id"]
    assert "cao_list_managed_workers" in {
        tool["name"] for tool in MCPServer(system["service"]).tools_for(conversation_actor)
    }
    wake_credential = system["service"].db.fetchone(
        "SELECT state FROM cao_runtime_credentials ORDER BY created_at DESC LIMIT 1"
    )
    assert wake_credential is not None and wake_credential["state"] == "revoked"
    wake_failure = system["service"].db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'cao.runtime_wake_failed' "
        "ORDER BY sequence DESC LIMIT 1"
    )
    assert wake_failure is not None
    assert json.loads(wake_failure["data_json"])["reason_code"] == "runtime_dispatch_failed"


def test_unknown_wake_for_one_cao_conversation_does_not_block_another(
    system: dict[str, Any],
) -> None:
    """Delivery ordering is per pinned conversation runtime, not CAO principal."""

    first = _attach(system, thread_id="thread-a")
    second = _attach(system, thread_id="thread-b")
    first_work, _ = _worker_boundary(system, first)
    second_work, _ = _worker_boundary(system, second)
    first_delivery = _boundary_delivery(system, first_work["id"])
    second_delivery = _boundary_delivery(system, second_work["id"])
    system["service"].db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dispatched'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (first_delivery["message_id"], system["cao"]["id"]),
    )

    adapter = _CAOBrokerAdapter(system["service"])
    assert (
        asyncio.run(
            Dispatcher(
                system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
            ).run_once()
        )
        == 1
    )

    assert [call["runtime"]["id"] for call in adapter.calls] == [second["runtime_session_id"]]
    persisted_first = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (first_delivery["message_id"], system["cao"]["id"]),
    )
    persisted_second = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (second_delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted_first is not None and persisted_first["state"] == "dispatched"
    assert persisted_second is not None and persisted_second["state"] == "dead"
    second_recovery = system["service"].db.fetchone(
        """
        SELECT d.state FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.work_item_id = ?
          AND json_extract(m.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        """,
        (second_work["id"],),
    )
    assert second_recovery is not None and second_recovery["state"] == "queued"


def test_active_cao_handoff_still_blocks_later_work_for_the_same_conversation(
    system: dict[str, Any],
) -> None:
    """An active handoff serializes transport while all Work results stay readable."""

    attachment = _attach(system)
    first_work, _ = _worker_boundary(system, attachment)
    second_work, _ = _worker_boundary(system, attachment)
    first_delivery = _boundary_delivery(system, first_work["id"])
    second_delivery = _boundary_delivery(system, second_work["id"])
    system["service"].db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dispatched', lease_until = '2099-01-01T00:00:00Z',
            owner_token = 'active-cao-handoff'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (first_delivery["message_id"], system["cao"]["id"]),
    )

    conversation_actor = system["service"].authenticate(str(attachment["context_token"]))
    visible_ids = {item["id"] for item in system["service"].get_inbox(conversation_actor)["items"]}
    assert first_delivery["message_id"] in visible_ids
    assert second_delivery["message_id"] in visible_ids

    adapter = _CAOBrokerAdapter(system["service"])
    assert (
        asyncio.run(
            Dispatcher(
                system["service"], system["settings"], registry=cast(Any, _Registry(adapter))
            ).run_once()
        )
        == 0
    )

    assert adapter.calls == []
    persisted_second = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (second_delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted_second is not None and persisted_second["state"] == "queued"


async def _read_websocket_client_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    first, second = await reader.readexactly(2)
    assert first == 0x81
    assert second & 0x80
    size = second & 0x7F
    if size == 126:
        size = struct.unpack("!H", await reader.readexactly(2))[0]
    elif size == 127:
        size = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4)
    payload = await reader.readexactly(size)
    return cast(
        dict[str, Any],
        json.loads(bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))),
    )


async def _accept_websocket_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    request = await reader.readuntil(b"\r\n\r\n")
    headers = request.decode("ascii").split("\r\n")
    key = next(
        line.split(":", 1)[1].strip()
        for line in headers
        if line.lower().startswith("sec-websocket-key:")
    )
    accept = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")
    writer.write(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode("ascii")
    )
    await writer.drain()


async def _write_websocket_server_message(
    writer: asyncio.StreamWriter, payload: Mapping[str, Any]
) -> None:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    frame = bytearray((0x81,))
    if len(encoded) < 126:
        frame.append(len(encoded))
    elif len(encoded) <= 0xFFFF:
        frame.append(126)
        frame.extend(struct.pack("!H", len(encoded)))
    else:
        frame.append(127)
        frame.extend(struct.pack("!Q", len(encoded)))
    frame.extend(encoded)
    writer.write(bytes(frame))
    await writer.drain()


async def _expect_desktop_thread_resume(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    thread_id: str,
    observed: list[str],
) -> None:
    resume = await _read_websocket_client_message(reader)
    observed.append(str(resume["method"]))
    assert resume["method"] == "thread/resume"
    assert resume["params"] == {
        "threadId": thread_id,
        "excludeTurns": True,
    }
    await _write_websocket_server_message(
        writer,
        {
            "id": resume["id"],
            "result": {"thread": {"id": thread_id}},
        },
    )


async def _expect_desktop_queue_list(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    thread_id: str,
    observed: list[str],
    data: list[dict[str, Any]],
) -> None:
    queue = await _read_websocket_client_message(reader)
    observed.append(str(queue["method"]))
    assert queue["method"] == "thread/queue/list"
    assert queue["params"] == {"threadId": thread_id, "limit": 100}
    await _write_websocket_server_message(
        writer,
        {
            "id": queue["id"],
            "result": {"data": data, "nextCursor": None},
        },
    )


def test_desktop_durable_queue_admission_does_not_require_local_thread_resume(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A GUI/TUI writer may live elsewhere; the shared queue is admitted first."""

    attachment = _attach(system, thread_id="desktop-active-writer-queue")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    socket_path = Path("/tmp") / f"cao-active-writer-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")

            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            queued = await _read_websocket_client_message(reader)
            observed.append(str(queued["method"]))
            assert queued["method"] == "thread/queue/add"
            assert queued["params"]["threadId"] == attachment["native_thread_id"]
            client_id = str(queued["params"]["clientUserMessageId"])
            assert client_id.startswith("cao-delivery-")
            await _write_websocket_server_message(
                writer,
                {
                    "id": queued["id"],
                    "result": {
                        "queuedSubmission": {
                            "id": "queued-cao-wake",
                            "clientUserMessageId": client_id,
                            "input": queued["params"]["input"],
                        }
                    },
                },
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            assert (
                await asyncio.wait_for(
                    Dispatcher(system["service"], settings).run_once(), timeout=3
                )
                == 1
            )
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert observed == [
        "connected",
        "initialize",
        "initialized",
        "thread/queue/add",
    ]
    assert asyncio.run(Dispatcher(system["service"], settings).run_once()) == 0
    delivered = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, last_error, delivered_at
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert delivered is not None
    assert delivered["state"] == "delivered", dict(delivered)
    assert delivered["generation"] == delivery["generation"]
    assert delivered["attempts"] == 0
    assert delivered["last_error"] == ""
    assert delivered["delivered_at"]
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["runtime"]["state"] == "waiting"


@pytest.mark.parametrize(
    "activation_error",
    [
        "resume is not supported",
        "another host owns the writer",
        "truncated_payload",
        "truncated_extended_length",
        "write_oserror",
    ],
)
def test_desktop_queue_acceptance_survives_unavailable_local_activation(
    system: dict[str, Any],
    settings: Any,
    monkeypatch: pytest.MonkeyPatch,
    activation_error: str,
) -> None:
    """Neither unsupported resume nor another writer can revoke a queue ACK."""

    attachment = _attach(system, thread_id="desktop-cold-resume-retry")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    socket_path = Path("/tmp") / f"cao-cold-resume-{secrets.token_hex(8)}.sock"
    observed: list[str] = []
    dispatcher = Dispatcher(system["service"], settings)

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")
            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            queue = await _read_websocket_client_message(reader)
            observed.append(str(queue["method"]))
            assert queue["method"] == "thread/queue/add"
            await _write_websocket_server_message(
                writer,
                {
                    "id": queue["id"],
                    "result": {
                        "queuedSubmission": {
                            "id": "queued-before-independent-activation",
                            "clientUserMessageId": queue["params"]["clientUserMessageId"],
                        }
                    },
                },
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            assert await asyncio.wait_for(dispatcher.run_once(), timeout=3) == 1
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert observed == ["connected", "initialize", "initialized", "thread/queue/add"]
    adapter = dispatcher.registry.get("codex-app-server")
    assert isinstance(adapter, CodexAppServerAdapter)

    async def fail_activation(runtime: Mapping[str, Any], *, message_id: str) -> str:
        assert runtime["native_session_id"] == attachment["native_thread_id"]
        assert message_id == delivery["message_id"]
        if activation_error.startswith("truncated_"):
            reader = asyncio.StreamReader()
            reader.feed_data(
                b"\x81\x05{" if activation_error == "truncated_payload" else b"\x81\x7e\x00"
            )
            reader.feed_eof()
            rpc = _JsonRpcDesktopSocket(reader, cast(Any, object()))
            await rpc.read_line(1)
            pytest.fail("truncated transport unexpectedly succeeded")
        if activation_error == "write_oserror":

            class BrokenWriter:
                def write(self, payload: bytes) -> None:
                    raise OSError("private transport detail")

            rpc = _JsonRpcDesktopSocket(asyncio.StreamReader(), cast(Any, BrokenWriter()))
            await rpc.send({"id": 1, "method": "initialize", "params": {}})
            pytest.fail("broken transport unexpectedly succeeded")
        raise RuntimeAdapterError(activation_error)

    monkeypatch.setattr(adapter, "activate_desktop_cao_thread", fail_activation)
    system["service"].db.execute(
        "UPDATE message_deliveries SET delivered_at = ?, updated_at = ? "
        "WHERE message_id = ? AND recipient_id = ?",
        (
            "2000-01-01T00:00:00Z",
            "2000-01-01T00:00:00Z",
            delivery["message_id"],
            system["cao"]["id"],
        ),
    )
    candidates = dispatcher._cao_supervision_activation_candidates()
    assert len(candidates) == 1
    expected_error = (
        "closed its local wake socket"
        if activation_error.startswith("truncated_")
        else "wake socket write failed"
        if activation_error == "write_oserror"
        else activation_error
    )
    with pytest.raises(RuntimeAdapterError, match=expected_error):
        asyncio.run(dispatcher._activate_cao_supervision_threads(candidates))
    independent_attachment = _attach(system, thread_id="independent-desktop-queue-owner")
    independent_work, _ = _worker_boundary(system, independent_attachment)
    independent_delivery = _boundary_delivery(system, independent_work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (independent_delivery["message_id"],),
    )
    dispatched: list[str] = []

    async def accept_independent(
        runtime: Mapping[str, Any], message: Mapping[str, Any]
    ) -> RuntimeDispatchResult:
        assert runtime["native_session_id"] == independent_attachment["native_thread_id"]
        assert message["id"] == independent_delivery["message_id"]
        dispatched.append(str(message["id"]))
        return RuntimeDispatchResult(
            success=True,
            native_session_id=str(runtime["native_session_id"]),
            state=RuntimeState.READY,
            metadata={"delivery_method": "thread_queue", "delivery_acceptance": "queued"},
        )

    monkeypatch.setattr(adapter, "dispatch", accept_independent)
    assert asyncio.run(dispatcher.run_once()) == 1
    assert dispatched == [independent_delivery["message_id"]]
    assert dispatcher.last_error == "runtime_dispatch_failed"
    assert (
        system["service"].db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (independent_delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "delivered"
    )
    assert dispatcher._claim_delivery() is None
    accepted = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, last_error
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert accepted is not None
    assert dict(accepted) == {
        "state": "delivered",
        "generation": int(delivery["generation"]),
        "attempts": 0,
        "last_error": "",
    }
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["runtime"]["state"] == "waiting"


def test_dispatcher_cold_activates_provider_accepted_wake_without_resubmission(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A delivered wake survives provider unload and resumes its same queued turn."""

    attachment = _attach(system, thread_id="desktop-provider-accepted-cold-thread")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered', delivered_at = ?, updated_at = ? "
        "WHERE message_id = ? AND recipient_id = ?",
        (
            "2000-01-01T00:00:00Z",
            "2000-01-01T00:00:00Z",
            delivery["message_id"],
            system["cao"]["id"],
        ),
    )
    socket_path = Path("/tmp") / f"cao-cold-activation-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")
            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            read = await _read_websocket_client_message(reader)
            observed.append(str(read["method"]))
            assert read["params"] == {
                "threadId": attachment["native_thread_id"],
                "includeTurns": False,
            }
            await _write_websocket_server_message(
                writer,
                {
                    "id": read["id"],
                    "result": {
                        "thread": {
                            "id": attachment["native_thread_id"],
                            "status": {"type": "notLoaded"},
                        }
                    },
                },
            )
            await _expect_desktop_queue_list(
                reader,
                writer,
                thread_id=str(attachment["native_thread_id"]),
                observed=observed,
                data=[
                    {
                        "id": "queued-provider-wake",
                        "input": [],
                        "clientUserMessageId": _codex_delivery_client_user_message_id(
                            {"id": delivery["message_id"]}
                        ),
                    }
                ],
            )
            await _expect_desktop_thread_resume(
                reader,
                writer,
                thread_id=str(attachment["native_thread_id"]),
                observed=observed,
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            assert (
                await asyncio.wait_for(
                    Dispatcher(system["service"], settings).run_once(), timeout=3
                )
                == 1
            )
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert observed == [
        "connected",
        "initialize",
        "initialized",
        "thread/read",
        "thread/queue/list",
        "thread/resume",
    ]
    preserved = system["service"].db.fetchone(
        "SELECT state, generation, attempts, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert preserved is not None
    assert dict(preserved) == {
        "state": "delivered",
        "generation": delivery["generation"],
        "attempts": 0,
        "last_error": "",
    }


def test_provider_consumed_wake_without_mcp_gets_one_durable_successor(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exact persisted terminal turn is corroborated, then advanced once."""

    attachment = _attach(system, thread_id="desktop-provider-consumed-before-mcp")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered', delivered_at = ?, updated_at = ? "
        "WHERE message_id = ? AND recipient_id = ?",
        (
            "2000-01-01T00:00:00Z",
            "2000-01-01T00:00:00Z",
            delivery["message_id"],
            system["cao"]["id"],
        ),
    )
    socket_path = Path("/tmp") / f"cao-consumed-wake-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")
            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer,
                {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}},
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            read = await _read_websocket_client_message(reader)
            observed.append(str(read["method"]))
            assert read["params"] == {
                "threadId": attachment["native_thread_id"],
                "includeTurns": False,
            }
            await _write_websocket_server_message(
                writer,
                {
                    "id": read["id"],
                    "result": {
                        "thread": {
                            "id": attachment["native_thread_id"],
                            "status": {"type": "idle"},
                        }
                    },
                },
            )
            await _expect_desktop_queue_list(
                reader,
                writer,
                thread_id=str(attachment["native_thread_id"]),
                observed=observed,
                data=[],
            )
            history = await _read_websocket_client_message(reader)
            observed.append(str(history["method"]))
            assert history["method"] == "thread/turns/list"
            assert history["params"] == {
                "threadId": attachment["native_thread_id"],
                "itemsView": "summary",
                "limit": 20,
                "sortDirection": "desc",
            }
            await _write_websocket_server_message(
                writer,
                {
                    "id": history["id"],
                    "result": {
                        "data": [
                            {
                                "id": "provider-terminal-wake-turn",
                                "status": "completed",
                                "startedAt": 100,
                                "completedAt": 101,
                                "items": [
                                    {
                                        "type": "userMessage",
                                        "id": "provider-user-item",
                                        "clientId": _terminal_cao_proof(attachment, delivery)[
                                            "client_user_message_id"
                                        ],
                                    }
                                ],
                            }
                        ],
                        "nextCursor": None,
                    },
                },
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            dispatcher = Dispatcher(system["service"], settings)
            first_candidates = dispatcher._cao_supervision_activation_candidates()
            assert [item["message_id"] for item in first_candidates] == [delivery["message_id"]]
            assert await dispatcher._activate_cao_supervision_threads(first_candidates) == 1

            first_persisted = system["service"].db.fetchone(
                "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                (delivery["message_id"], system["cao"]["id"]),
            )
            assert first_persisted is not None and first_persisted["state"] == "delivered"
            assert dispatcher._cao_supervision_activation_candidates() == []
            assert (
                await dispatcher._activate_cao_supervision_threads(
                    system["service"].pending_cao_supervision_activations(
                        updated_before="2099-01-01T00:00:00Z"
                    )
                )
                == 1
            )
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert (
        observed
        == [
            "connected",
            "initialize",
            "initialized",
            "thread/read",
            "thread/queue/list",
            "thread/turns/list",
        ]
        * 2
    )
    persisted = system["service"].db.fetchone(
        "SELECT state, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted is not None
    assert persisted["state"] == "dead"
    assert str(persisted["last_error"]).startswith("superseded;evidence_digest=")
    recoveries = system["service"].db.fetchall(
        """
        SELECT message.id, delivery.state, message.payload_json
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        ORDER BY message.sequence
        """,
        (work["id"],),
    )
    assert len(recoveries) == 1
    assert recoveries[0]["state"] == "queued"
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'cao.supervision_provider_turn_incomplete_observed' "
            "AND aggregate_id = ?",
            (delivery["message_id"],),
        )["count"]
        == 1
    )
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'reasoner.incomplete_turn_recovery_scheduled' "
            "AND causation_id = ?",
            (delivery["message_id"],),
        )["count"]
        == 1
    )
    assert (
        system["service"].reconcile_incomplete_cao_provider_turn(
            message_id=str(delivery["message_id"]),
            attachment_id=str(attachment["id"]),
            delivery_generation=int(delivery["generation"]),
            **_terminal_cao_proof(attachment, delivery),
        )
        == "not_required"
    )
    assert (
        len(
            system["service"].db.fetchall(
                """
            SELECT message.id FROM messages AS message
            WHERE message.work_item_id = ?
              AND json_extract(message.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
            """,
                (work["id"],),
            )
        )
        == 1
    )


@pytest.mark.parametrize("local_status", ["idle", "notLoaded"])
@pytest.mark.parametrize(
    "history_case",
    [
        "missing",
        "unrelated",
        "assistant_spoof",
        "tool_spoof",
        "running",
        "unsupported",
        "terminal_without_completion",
    ],
)
def test_queue_absence_and_local_host_state_cannot_authorize_cao_replay(
    system: dict[str, Any],
    settings: Any,
    monkeypatch: pytest.MonkeyPatch,
    local_status: str,
    history_case: str,
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="cross-host-cao-thread")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    proof = _terminal_cao_proof(attachment, delivery)
    events_before = int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"])
    messages_before = int(service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"])
    methods: list[str] = []

    class ReadOnlyRPC:
        async def request(
            self, method: str, params: dict[str, Any], **_: Any
        ) -> tuple[int, dict[str, Any]]:
            methods.append(method)
            if method == "initialize":
                return 1, {}
            if method == "thread/read":
                return 1, {
                    "thread": {
                        "id": attachment["native_thread_id"],
                        "status": {"type": local_status},
                    }
                }
            if method == "thread/queue/list":
                return 1, {"data": [], "nextCursor": None}
            assert method == "thread/turns/list"
            assert params == {
                "threadId": attachment["native_thread_id"],
                "itemsView": "summary",
                "limit": 20,
                "sortDirection": "desc",
            }
            if history_case == "unsupported":
                raise RuntimeAdapterError("private provider diagnostic")
            if history_case == "missing":
                return 1, {"data": [], "nextCursor": None}
            item_type = {"assistant_spoof": "agentMessage", "tool_spoof": "mcpToolCall"}.get(
                history_case, "userMessage"
            )
            return 1, {
                "data": [
                    {
                        "id": "other-host-turn",
                        "status": "inProgress"
                        if history_case == "running"
                        else "interrupted"
                        if history_case == "terminal_without_completion"
                        else "completed",
                        "startedAt": 100,
                        "completedAt": None,
                        "items": [
                            {
                                "type": item_type,
                                "clientId": "different-client"
                                if history_case == "unrelated"
                                else proof["client_user_message_id"],
                            }
                        ],
                    }
                ],
                "nextCursor": None,
            }

        async def send(self, message: dict[str, Any]) -> None:
            assert message == {"method": "initialized", "params": {}}

        async def close(self) -> None:
            pass

    async def connect(*_: Any, **__: Any) -> Any:
        return ReadOnlyRPC()

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", connect)
    dispatcher = Dispatcher(service, settings)
    candidates = [
        {
            "runtime_id": attachment["runtime_session_id"],
            "native_thread_id": attachment["native_thread_id"],
            "attachment_id": attachment["id"],
            "message_id": delivery["message_id"],
            "delivery_generation": delivery["generation"],
        }
    ]
    for _ in range(2):
        if history_case == "unsupported":
            with pytest.raises(
                RuntimeAdapterError, match=r"^cao_provider_turn_evidence_unavailable$"
            ):
                asyncio.run(dispatcher._activate_cao_supervision_threads(candidates))
        else:
            assert asyncio.run(dispatcher._activate_cao_supervision_threads(candidates)) == 0
    assert methods == ["initialize", "thread/read", "thread/queue/list", "thread/turns/list"] * 2
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"] == events_before
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"] == messages_before
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ?", (delivery["message_id"],)
        )["state"]
        == "delivered"
    )


@pytest.mark.parametrize("terminal_status", ["completed", "failed", "interrupted"])
def test_terminal_cao_turn_evidence_uses_exact_persisted_summary_identity(
    settings: Any,
    terminal_status: str,
) -> None:
    adapter = CodexAppServerAdapter(settings)
    client_id = _codex_delivery_client_user_message_id({"id": "exact-delivery"})
    calls: list[dict[str, Any]] = []

    class SummaryRPC:
        async def request(
            self, method: str, params: dict[str, Any], **_: Any
        ) -> tuple[int, dict[str, Any]]:
            assert method == "thread/turns/list"
            calls.append(params)
            if len(calls) == 1:
                return 1, {"data": [], "nextCursor": "second-page"}
            return 1, {
                "data": [
                    {
                        "id": "exact-provider-turn",
                        "status": terminal_status,
                        "startedAt": 100,
                        "completedAt": 101,
                        "items": [{"type": "userMessage", "clientId": client_id}],
                    }
                ],
                "nextCursor": None,
            }

    evidence = asyncio.run(
        adapter._desktop_cao_terminal_turn_evidence(
            cast(Any, SummaryRPC()),
            native_thread_id="exact-native-thread",
            client_user_message_id=client_id,
            remaining=lambda: 1.0,
        )
    )
    assert evidence == DesktopCAOTerminalTurnEvidence(
        native_thread_id="exact-native-thread",
        client_user_message_id=client_id,
        native_turn_id="exact-provider-turn",
        terminal_status=cast(Any, terminal_status),
        started_at=100,
        completed_at=101,
    )
    assert calls == [
        {
            "threadId": "exact-native-thread",
            "itemsView": "summary",
            "limit": 20,
            "sortDirection": "desc",
        },
        {
            "threadId": "exact-native-thread",
            "itemsView": "summary",
            "limit": 20,
            "sortDirection": "desc",
            "cursor": "second-page",
        },
    ]


@pytest.mark.parametrize("terminal_status", ["completed", "failed", "interrupted"])
@pytest.mark.parametrize(("started_at", "completed_at"), _INVALID_TURN_CHRONOLOGIES)
def test_terminal_status_without_valid_completion_chronology_remains_pending(
    settings: Any,
    terminal_status: str,
    started_at: Any,
    completed_at: Any,
) -> None:
    client_id = _codex_delivery_client_user_message_id({"id": "exact-delivery"})

    class SummaryRPC:
        async def request(
            self, method: str, params: dict[str, Any], **_: Any
        ) -> tuple[int, dict[str, Any]]:
            assert method == "thread/turns/list"
            return 1, {
                "data": [
                    {
                        "id": "current-provider-turn",
                        "status": terminal_status,
                        "startedAt": started_at,
                        "completedAt": completed_at,
                        "items": [{"type": "userMessage", "clientId": client_id}],
                    }
                ],
                "nextCursor": None,
            }

    assert (
        asyncio.run(
            CodexAppServerAdapter(settings)._desktop_cao_terminal_turn_evidence(
                cast(Any, SummaryRPC()),
                native_thread_id="exact-native-thread",
                client_user_message_id=client_id,
                remaining=lambda: 1.0,
            )
        )
        == "pending"
    )


@pytest.mark.parametrize(("started_at", "completed_at"), _INVALID_TURN_CHRONOLOGIES)
def test_existing_terminal_observation_cannot_bypass_invalid_new_chronology(
    system: dict[str, Any],
    started_at: Any,
    completed_at: Any,
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "attachment_id": attachment["id"],
        "delivery_generation": delivery["generation"],
        **_terminal_cao_proof(attachment, delivery),
    }
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
    before = service.db.commit_generation()
    arguments.update(started_at=started_at, completed_at=completed_at)
    with pytest.raises(ValidationError, match="exact terminal provider turn evidence"):
        service.reconcile_incomplete_cao_provider_turn(**arguments)
    assert service.db.commit_generation() == before
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ?", (delivery["message_id"],)
        )["state"]
        == "delivered"
    )


@pytest.mark.parametrize("terminal_status", ["completed", "failed", "interrupted"])
def test_identical_completed_chronology_admits_one_terminal_recovery(
    system: dict[str, Any],
    terminal_status: str,
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "attachment_id": attachment["id"],
        "delivery_generation": delivery["generation"],
        **_terminal_cao_proof(attachment, delivery),
        "terminal_status": terminal_status,
    }
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "scheduled"
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"


@pytest.mark.parametrize(
    "history_case",
    ["page_cap", "repeated_cursor", "malformed", "oversize", "ambiguous", "invalid_status"],
)
def test_terminal_cao_summary_read_is_bounded_and_fails_closed(
    settings: Any,
    history_case: str,
) -> None:
    adapter = CodexAppServerAdapter(settings)
    client_id = _codex_delivery_client_user_message_id({"id": "exact-delivery"})
    calls = 0

    class SummaryRPC:
        async def request(
            self, method: str, params: dict[str, Any], **_: Any
        ) -> tuple[int, dict[str, Any]]:
            nonlocal calls
            calls += 1
            assert method == "thread/turns/list"
            assert params["limit"] == 20 and params["itemsView"] == "summary"
            turn = {
                "id": f"provider-turn-{calls}",
                "status": "completed",
                "startedAt": 100,
                "completedAt": 101,
                "items": [{"type": "userMessage", "clientId": client_id}],
            }
            if history_case == "malformed":
                return 1, {"data": {}}
            if history_case == "oversize":
                return 1, {"data": [turn] * 21}
            if history_case == "invalid_status":
                return 1, {"data": [{**turn, "status": {"type": "completed"}}], "nextCursor": None}
            if history_case == "ambiguous":
                return 1, {"data": [turn], "nextCursor": "second-page" if calls == 1 else None}
            return 1, {
                "data": [],
                "nextCursor": "same-page" if history_case == "repeated_cursor" else f"page-{calls}",
            }

    async def read() -> Any:
        return await adapter._desktop_cao_terminal_turn_evidence(
            cast(Any, SummaryRPC()),
            native_thread_id="exact-native-thread",
            client_user_message_id=client_id,
            remaining=lambda: 1.0,
        )

    if history_case in {"page_cap", "invalid_status"}:
        assert asyncio.run(read()) == "pending"
        assert calls == (5 if history_case == "page_cap" else 1)
    else:
        with pytest.raises(RuntimeAdapterError, match=r"^cao_provider_turn_evidence_unavailable$"):
            asyncio.run(read())
        assert calls <= 2


@pytest.mark.parametrize(
    "changed_field",
    [
        "native_thread_id",
        "client_user_message_id",
        "native_turn_id",
        "terminal_status",
        "delivery_generation",
        "attachment_generation",
        "runtime_failed",
        "runtime_missing",
        "runtime_stopped",
        "runtime_adapter",
        "empty_turn",
        "nonterminal_status",
        "malformed_status",
        "unicode_client",
        "started_at",
        "completed_at",
    ],
)
def test_cao_terminal_proof_cannot_be_corroborated_by_changed_authority(
    system: dict[str, Any],
    changed_field: str,
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="proof-bound-native-thread")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "attachment_id": attachment["id"],
        "delivery_generation": delivery["generation"],
        **_terminal_cao_proof(attachment, delivery),
    }
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
    observed = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'cao.supervision_provider_turn_incomplete_observed' AND aggregate_id = ?",
        (delivery["message_id"],),
    )
    serialized = str(observed["data_json"])
    proof = json.loads(serialized)["provider_turn_proof"]
    assert set(proof) == {
        "source",
        "native_thread_digest",
        "native_turn_digest",
        "client_message_digest",
        "terminal_status",
        "completion_evidence",
        "started_at",
        "completed_at",
        "attachment_generation",
        "delivery_generation",
    }
    for field in ("native_thread_id", "native_turn_id", "client_user_message_id"):
        assert arguments[field] not in serialized
    if changed_field == "attachment_generation":
        service.db.execute(
            "UPDATE cao_session_attachments SET generation = generation + 1 WHERE id = ?",
            (attachment["id"],),
        )
    elif changed_field.startswith("runtime_"):
        if changed_field == "runtime_adapter":
            service.db.execute(
                "UPDATE runtime_sessions SET adapter = 'claude-code' WHERE id = ?",
                (attachment["runtime_session_id"],),
            )
        else:
            service.db.execute(
                "UPDATE runtime_sessions SET state = ? WHERE id = ?",
                (changed_field.removeprefix("runtime_"), attachment["runtime_session_id"]),
            )
    elif changed_field == "delivery_generation":
        arguments[changed_field] += 1
    elif changed_field == "terminal_status":
        arguments[changed_field] = "interrupted"
    elif changed_field == "started_at":
        arguments[changed_field] = 99
    elif changed_field == "completed_at":
        arguments[changed_field] = 102
    elif changed_field == "empty_turn":
        arguments["native_turn_id"] = ""
    elif changed_field == "nonterminal_status":
        arguments["terminal_status"] = "inProgress"
    elif changed_field == "malformed_status":
        arguments["terminal_status"] = {"type": "completed"}
    elif changed_field == "unicode_client":
        arguments["client_user_message_id"] = "別の通知"
    else:
        arguments[changed_field] = "different-proof-identity"
    if changed_field in {
        "client_user_message_id",
        "empty_turn",
        "nonterminal_status",
        "malformed_status",
        "unicode_client",
    }:
        with pytest.raises(ValidationError, match="exact terminal provider turn evidence"):
            service.reconcile_incomplete_cao_provider_turn(**arguments)
    else:
        assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ?", (delivery["message_id"],)
        )["state"]
        == "delivered"
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = 'reasoner.incomplete_turn_recovery_scheduled' AND causation_id = ?",
            (delivery["message_id"],),
        )["count"]
        == 0
    )


@pytest.mark.parametrize("legacy_kind", ["empty_queue", "status_only", "invalid_chronology"])
def test_legacy_empty_queue_observation_cannot_substitute_for_terminal_proof(
    system: dict[str, Any],
    legacy_kind: str,
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="legacy-observation-native-thread")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "attachment_id": attachment["id"],
        "delivery_generation": delivery["generation"],
        **_terminal_cao_proof(attachment, delivery),
    }
    legacy_data = {"attachment_id": attachment["id"], "delivery_generation": delivery["generation"]}
    if legacy_kind != "empty_queue":
        legacy_data["provider_turn_proof"] = {
            "source": "app_server_persisted_turn_summary",
            "native_thread_digest": hashlib.sha256(
                arguments["native_thread_id"].encode()
            ).hexdigest(),
            "native_turn_digest": hashlib.sha256(arguments["native_turn_id"].encode()).hexdigest(),
            "client_message_digest": hashlib.sha256(
                arguments["client_user_message_id"].encode()
            ).hexdigest(),
            "terminal_status": arguments["terminal_status"],
            "attachment_generation": attachment["generation"],
            "delivery_generation": delivery["generation"],
        }
        if legacy_kind == "invalid_chronology":
            legacy_data["provider_turn_proof"].update(
                {
                    "completion_evidence": "persisted_turn_completed_at",
                    "started_at": 100,
                    "completed_at": None,
                }
            )
    with service.db.transaction() as connection:
        service._event(
            connection,
            "cao.supervision_provider_turn_incomplete_observed",
            "message",
            delivery["message_id"],
            system["cao"]["id"],
            legacy_data,
        )
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ?", (delivery["message_id"],)
        )["state"]
        == "delivered"
    )
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "scheduled"
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"


@pytest.mark.parametrize(
    "fence",
    [
        "queued",
        "leased",
        "handled",
        "dead",
        "work_generation",
        "work_attention",
        "attachment_revoked",
        "attachment_expired",
        "attachment_mismatch",
        "packet_mismatch",
        "boundary_mismatch",
        "delivery_generation",
        "wrong_recipient",
    ],
)
def test_common_incomplete_recovery_rejects_noncurrent_source_without_writes(
    system: dict[str, Any],
    fence: str,
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    boundary_id = service.get_work(work["id"])["open_boundaries"][0]["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "recipient_id": system["cao"]["id"],
        "delivery_generation": delivery["generation"],
        "boundary_id": boundary_id,
    }
    if fence in {"queued", "leased", "handled", "dead"}:
        service.db.execute(
            "UPDATE message_deliveries SET state = ? WHERE message_id = ?",
            (fence, delivery["message_id"]),
        )
    elif fence == "work_generation":
        service.db.execute(
            "UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],)
        )
    elif fence == "work_attention":
        service.db.execute(
            "UPDATE work_items SET attention_owner = 'worker' WHERE id = ?", (work["id"],)
        )
    elif fence == "attachment_revoked":
        service.db.execute(
            "UPDATE cao_session_attachments SET state = 'revoked' WHERE id = ?", (attachment["id"],)
        )
    elif fence == "attachment_expired":
        service.db.execute(
            "UPDATE cao_session_attachments SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", attachment["id"]),
        )
    elif fence == "attachment_mismatch":
        service.db.execute(
            "UPDATE work_items SET supervisor_attachment_id = NULL WHERE id = ?",
            (work["id"],),
        )
    elif fence == "packet_mismatch":
        service.db.execute(
            "UPDATE messages SET task_packet_digest = ? WHERE id = ?",
            ("0" * 64, delivery["message_id"]),
        )
    elif fence == "boundary_mismatch":
        service.db.execute(
            "UPDATE messages SET payload_json = '{}' WHERE id = ?", (delivery["message_id"],)
        )
    elif fence == "delivery_generation":
        arguments["delivery_generation"] += 1
    else:
        arguments["recipient_id"] = system["worker"]["id"]
    before_events = service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]
    before_messages = service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"]
    before_delivery = dict(
        service.db.fetchone(
            "SELECT state,generation,last_error FROM message_deliveries WHERE message_id = ?",
            (delivery["message_id"],),
        )
    )
    with service.db.transaction() as connection:
        assert (
            service._recover_incomplete_reasoner_delivery_tx(connection, **arguments)
            == "not_required"
        )
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"] == before_events
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"] == before_messages
    assert (
        dict(
            service.db.fetchone(
                "SELECT state,generation,last_error FROM message_deliveries WHERE message_id = ?",
                (delivery["message_id"],),
            )
        )
        == before_delivery
    )


@pytest.mark.parametrize("live_lease", [False, True])
@pytest.mark.parametrize("source_reappeared", [False, True])
def test_common_incomplete_recovery_never_replays_a_predecessor_over_newer_owner(
    system: dict[str, Any],
    live_lease: bool,
    source_reappeared: bool,
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    boundary_id = service.get_work(work["id"])["open_boundaries"][0]["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    arguments = {
        "message_id": delivery["message_id"],
        "recipient_id": system["cao"]["id"],
        "delivery_generation": delivery["generation"],
        "boundary_id": boundary_id,
    }
    with service.db.transaction() as connection:
        assert (
            service._recover_incomplete_reasoner_delivery_tx(connection, **arguments) == "scheduled"
        )
    if live_lease:
        actor = service.authenticate(str(attachment["context_token"]))
        service.acquire_reasoner_turn(
            actor,
            work["id"],
            boundary_id=boundary_id,
            expected_generation=work["generation"],
            idempotency_key="current-owner-lease",
        )
    if source_reappeared:
        service.db.execute(
            "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
            (delivery["message_id"],),
        )
    before = {
        "deliveries": [
            dict(row)
            for row in service.db.fetchall(
                "SELECT message_id,state,generation,last_error FROM message_deliveries ORDER BY message_id"
            )
        ],
        "turns": [
            dict(row)
            for row in service.db.fetchall(
                "SELECT id,state,lease_expires_at FROM reasoner_turns ORDER BY id"
            )
        ],
        "events": service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"],
        "messages": service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"],
    }
    for _ in range(2):
        with service.db.transaction() as connection:
            assert (
                service._recover_incomplete_reasoner_delivery_tx(connection, **arguments)
                == "not_required"
            )
    assert [
        dict(row)
        for row in service.db.fetchall(
            "SELECT message_id,state,generation,last_error FROM message_deliveries ORDER BY message_id"
        )
    ] == before["deliveries"]
    assert [
        dict(row)
        for row in service.db.fetchall(
            "SELECT id,state,lease_expires_at FROM reasoner_turns ORDER BY id"
        )
    ] == before["turns"]
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"] == before["events"]
    assert (
        service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"] == before["messages"]
    )


def test_common_incomplete_recovery_preserves_live_lease_until_proven_expiry(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    boundary_id = service.get_work(work["id"])["open_boundaries"][0]["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    actor = service.authenticate(str(attachment["context_token"]))
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary_id,
        expected_generation=work["generation"],
        idempotency_key="live-owner-lease",
    )
    arguments = {
        "message_id": delivery["message_id"],
        "recipient_id": system["cao"]["id"],
        "delivery_generation": delivery["generation"],
        "boundary_id": boundary_id,
    }
    with service.db.transaction() as connection:
        assert (
            service._recover_incomplete_reasoner_delivery_tx(connection, **arguments)
            == "not_required"
        )
    assert (
        service.db.fetchone("SELECT state FROM reasoner_turns WHERE id = ?", (turn["id"],))["state"]
        == "leased"
    )
    service.db.execute(
        "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", turn["id"]),
    )
    with service.db.transaction() as connection:
        assert (
            service._recover_incomplete_reasoner_delivery_tx(connection, **arguments) == "scheduled"
        )
    assert (
        service.db.fetchone("SELECT state FROM reasoner_turns WHERE id = ?", (turn["id"],))["state"]
        == "abandoned"
    )


def test_reconnect_never_infers_cao_retry_authority_from_dead_state(
    system: dict[str, Any],
) -> None:
    """A CAO connection replacement also requires the typed retry policy."""

    attachment = _attach(system, thread_id="desktop-terminal-wake-history")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' "
        "WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'dead', "
        "last_error = 'semantic_terminal_history', reactivation_policy = 'terminal' "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )

    renewed = _attach(system, thread_id="desktop-terminal-wake-history")
    assert renewed["id"] == attachment["id"]
    preserved = system["service"].db.fetchone(
        "SELECT state, generation, reactivation_policy FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert preserved is not None
    assert dict(preserved) == {
        "state": "dead",
        "generation": delivery["generation"],
        "reactivation_policy": "terminal",
    }
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'cao.supervision_delivery_reactivated' "
            "AND aggregate_id = ?",
            (work["id"],),
        )["count"]
        == 0
    )


def test_reconnect_reactivates_only_unsuperseded_failed_successor(system: dict[str, Any]) -> None:
    """A dead predecessor remains history while its failed successor retries."""

    attachment = _attach(system, thread_id="desktop-superseded-wake-history")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered' "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    reconcile = system["service"].reconcile_incomplete_cao_provider_turn
    arguments = {
        "message_id": str(delivery["message_id"]),
        "attachment_id": str(attachment["id"]),
        "delivery_generation": int(delivery["generation"]),
        **_terminal_cao_proof(attachment, delivery),
    }
    assert reconcile(**arguments) == "observed"
    assert reconcile(**arguments) == "scheduled"
    successor = system["service"].db.fetchone(
        """
        SELECT message.id, delivery.generation
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        """,
        (work["id"],),
    )
    assert successor is not None
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'dead', last_error = ?, "
        "reactivation_policy = 'retryable' "
        "WHERE message_id = ? AND recipient_id = ?",
        (
            "desktop_wake_pre_start_unavailable",
            successor["id"],
            system["cao"]["id"],
        ),
    )

    renewed = _attach(system, thread_id="desktop-superseded-wake-history")
    assert renewed["id"] == attachment["id"]
    rows = system["service"].db.fetchall(
        """
        SELECT message.id, delivery.state, delivery.generation
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.boundary_id') IS NOT NULL
        ORDER BY message.sequence
        """,
        (work["id"],),
    )
    assert [row["id"] for row in rows] == [delivery["message_id"], successor["id"]]
    assert rows[0]["state"] == "dead"
    assert int(rows[0]["generation"]) == int(delivery["generation"])
    assert rows[1]["state"] == "queued"
    assert int(rows[1]["generation"]) == int(successor["generation"]) + 1
    reactivation = system["service"].db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'cao.supervision_delivery_reactivated' "
        "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
        (work["id"],),
    )
    assert reactivation is not None
    assert json.loads(str(reactivation["data_json"]))["message_id"] == successor["id"]


def test_reconnect_never_reactivates_superseded_wake_beside_accepted_successor(
    system: dict[str, Any],
) -> None:
    """A provider-accepted successor blocks its dead predecessor on reconnect."""

    attachment = _attach(system, thread_id="desktop-accepted-successor")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered' "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    reconcile = system["service"].reconcile_incomplete_cao_provider_turn
    arguments = {
        "message_id": str(delivery["message_id"]),
        "attachment_id": str(attachment["id"]),
        "delivery_generation": int(delivery["generation"]),
        **_terminal_cao_proof(attachment, delivery),
    }
    assert reconcile(**arguments) == "observed"
    assert reconcile(**arguments) == "scheduled"
    successor = system["service"].db.fetchone(
        """
        SELECT message.id, delivery.generation
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.action') = 'recover_incomplete_reasoner_turn'
        """,
        (work["id"],),
    )
    assert successor is not None
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered' "
        "WHERE message_id = ? AND recipient_id = ?",
        (successor["id"], system["cao"]["id"]),
    )

    renewed = _attach(system, thread_id="desktop-accepted-successor")
    assert renewed["id"] == attachment["id"]
    rows = system["service"].db.fetchall(
        """
        SELECT message.id, delivery.state, delivery.generation
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND json_extract(message.payload_json, '$.boundary_id') IS NOT NULL
        ORDER BY message.sequence
        """,
        (work["id"],),
    )
    assert [row["state"] for row in rows] == ["dead", "delivered"]
    assert int(rows[0]["generation"]) == int(delivery["generation"])
    assert int(rows[1]["generation"]) == int(successor["generation"])
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'cao.supervision_delivery_reactivated' "
            "AND aggregate_id = ?",
            (work["id"],),
        )["count"]
        == 0
    )


def test_one_cold_cao_thread_failure_does_not_starve_later_attachments(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    dispatcher = Dispatcher(system["service"], settings)
    adapter = dispatcher.registry.get("codex-app-server")
    assert isinstance(adapter, CodexAppServerAdapter)
    observed: list[str] = []

    def get_runtime(runtime_id: str) -> dict[str, Any]:
        return {
            "id": runtime_id,
            "adapter": "codex-app-server",
            "native_session_id": runtime_id,
        }

    async def activate(runtime: Mapping[str, Any], *, message_id: str) -> str:
        runtime_id = str(runtime["id"])
        observed.append(f"{runtime_id}:{message_id}")
        if runtime_id == "cold-thread-unavailable":
            raise RuntimeError("one cold thread is temporarily unavailable")
        return "resumed"

    monkeypatch.setattr(system["service"], "get_runtime", get_runtime)
    monkeypatch.setattr(adapter, "activate_desktop_cao_thread", activate)
    candidates = [
        {
            "message_id": "msg-cold-thread-unavailable",
            "attachment_id": "att-cold-thread-unavailable",
            "delivery_generation": 1,
            "runtime_id": "cold-thread-unavailable",
            "native_thread_id": "cold-thread-unavailable",
        },
        {
            "message_id": "msg-independent-cold-thread",
            "attachment_id": "att-independent-cold-thread",
            "delivery_generation": 1,
            "runtime_id": "independent-cold-thread",
            "native_thread_id": "independent-cold-thread",
        },
    ]

    with pytest.raises(RuntimeError, match="one cold thread is temporarily unavailable"):
        asyncio.run(dispatcher._activate_cao_supervision_threads(candidates))
    assert observed == [
        "cold-thread-unavailable:msg-cold-thread-unavailable",
        "independent-cold-thread:msg-independent-cold-thread",
    ]


def test_desktop_queue_unavailable_never_falls_back_and_remains_retryable(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unsupported canonical queue cannot invoke an older delivery route."""

    attachment = _attach(system, thread_id="desktop-active-writer-retry")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    socket_path = Path("/tmp") / f"cao-active-writer-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")

            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            queue = await _read_websocket_client_message(reader)
            observed.append(str(queue["method"]))
            await _write_websocket_server_message(
                writer,
                {
                    "id": queue["id"],
                    "error": {"code": -32601, "message": "Method not found"},
                },
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            assert (
                await asyncio.wait_for(
                    Dispatcher(system["service"], settings).run_once(), timeout=3
                )
                == 1
            )
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert observed == [
        "connected",
        "initialize",
        "initialized",
        "thread/queue/add",
    ]
    retried = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, last_error
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert retried is not None
    assert dict(retried) == {
        "state": "queued",
        "generation": int(delivery["generation"]) + 1,
        "attempts": 1,
        "last_error": "desktop_wake_pre_start_unavailable",
    }
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"
    assert current_attachment["runtime"]["state"] == "waiting"


def test_desktop_queue_transport_loss_after_submit_is_unknown_not_retried(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost queue ACK cannot manufacture either rejection or a retry."""

    attachment = _attach(system, thread_id="desktop-queue-unknown")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    socket_path = Path("/tmp") / f"cao-queue-unknown-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")

            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            queue = await _read_websocket_client_message(reader)
            observed.append(str(queue["method"]))
            assert queue["method"] == "thread/queue/add"
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            assert (
                await asyncio.wait_for(
                    Dispatcher(system["service"], settings).run_once(), timeout=3
                )
                == 1
            )
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())

    assert observed == [
        "connected",
        "initialize",
        "initialized",
        "thread/queue/add",
    ]
    assert asyncio.run(Dispatcher(system["service"], settings).run_once()) == 0
    unknown = system["service"].db.fetchone(
        """
        SELECT state, generation, attempts, last_error, owner_token
        FROM message_deliveries WHERE message_id = ? AND recipient_id = ?
        """,
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert unknown is not None
    assert unknown["state"] == "dispatched"
    assert unknown["generation"] == delivery["generation"]
    assert unknown["attempts"] == 1
    assert unknown["last_error"] == "runtime_dispatch_failed"
    assert unknown["owner_token"] == ""
    current_attachment = system["service"].get_cao_attachment(attachment["id"])
    assert current_attachment["state"] == "active"


def test_desktop_wake_uses_existing_host_and_completes_review_without_a_ticket(
    system: dict[str, Any], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Completion uses the Desktop owner, never a second app-server writer."""

    attachment = _attach(system, thread_id="desktop-owned-review-thread")
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    attached_runtime = system["service"].get_runtime(attachment["runtime_session_id"])
    assert attached_runtime["adapter"] == "codex-app-server"
    assert attached_runtime["native_session_id"] == attachment["native_thread_id"]
    assert attached_runtime["cao_attachment"] is not None
    # The Worker assignment is a separate earlier lane; this test isolates the
    # completion-to-CAO wake rather than simulating Worker execution again.
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id <> ? AND state = 'queued'",
        (delivery["message_id"],),
    )
    socket_path = Path("/tmp") / f"cao-wake-{secrets.token_hex(8)}.sock"
    observed: list[str] = []

    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _accept_websocket_client(reader, writer)
            observed.append("connected")

            initialize = await _read_websocket_client_message(reader)
            observed.append(str(initialize["method"]))
            await _write_websocket_server_message(
                writer, {"id": initialize["id"], "result": {"serverInfo": {"name": "Desktop"}}}
            )
            initialized = await _read_websocket_client_message(reader)
            observed.append(str(initialized["method"]))
            queue = await _read_websocket_client_message(reader)
            observed.append(str(queue["method"]))
            assert queue["method"] == "thread/queue/add"
            await _write_websocket_server_message(
                writer,
                {
                    "id": queue["id"],
                    "result": {
                        "queuedSubmission": {
                            "clientUserMessageId": queue["params"]["clientUserMessageId"],
                        }
                    },
                },
            )

            # This represents the already-attached Desktop conversation using
            # its existing CSC after the host receives the wake prompt.
            actor = system["service"].authenticate(str(attachment["context_token"]))
            message = system["service"]._message_view(
                system["service"].db.fetchone(
                    "SELECT * FROM messages WHERE id = ?", (delivery["message_id"],)
                )
            )
            system["service"].acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
            boundary_id = str(message["payload"]["boundary_id"])
            current = system["service"].get_work(str(message["work_item_id"]))
            reasoner = system["service"].acquire_reasoner_turn(
                actor,
                str(message["work_item_id"]),
                boundary_id=boundary_id,
                expected_generation=int(current["generation"]),
                idempotency_key="desktop-host-review",
            )
            system["service"].dispose_boundary(
                actor,
                boundary_id,
                BoundaryDispositionInput(
                    turn_id=reasoner["id"],
                    lease_token=reasoner["lease_token"],
                    expected_generation=int(current["generation"]),
                    kind=BoundaryDispositionKind.ACCEPT,
                    reason="Desktop CAO reviewed the completion after its event wake.",
                ),
            )
            system["service"].mark_message_handled(
                actor, str(message["id"]), evidence="Desktop CAO review committed"
            )
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )

            async def no_subprocess(*_args: object, **_kwargs: object) -> None:
                raise AssertionError("Desktop wake must not spawn another app-server writer")

            monkeypatch.setattr(asyncio, "create_subprocess_exec", no_subprocess)
            try:
                dispatched = await asyncio.wait_for(
                    Dispatcher(system["service"], settings).run_once(), timeout=3
                )
            except TimeoutError as error:
                raise AssertionError(f"Desktop wake timed out after {observed!r}") from error
            assert dispatched == 1
        finally:
            server.close()
            await server.wait_closed()
            with suppress(FileNotFoundError):
                socket_path.unlink()

    asyncio.run(scenario())
    assert observed == [
        "connected",
        "initialize",
        "initialized",
        "thread/queue/add",
    ]
    state = system["service"].db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert state is not None and state["state"] == "handled"
    assert system["service"].get_runtime(attachment["runtime_session_id"])["state"] == "waiting"
    assert (
        system["service"].db.fetchone("SELECT COUNT(*) AS count FROM cao_runtime_tickets")["count"]
        == 0
    )
    assert (
        system["service"].db.fetchone("SELECT COUNT(*) AS count FROM cao_runtime_credentials")[
            "count"
        ]
        == 0
    )
    assert verify_projection(system["service"].db).healthy


def test_attaching_a_cao_session_does_not_ingest_ordinary_conversation(
    system: dict[str, Any],
) -> None:
    _attach(system)
    assert (
        system["service"].db.fetchone("SELECT COUNT(*) AS count FROM source_receipts")["count"] == 0
    )


def test_two_work_items_remain_bound_to_their_delegating_cao_threads(
    system: dict[str, Any],
) -> None:
    first = _attach(system, thread_id="thread-a")
    second = _attach(system, thread_id="thread-b")
    work_a, _ = _worker_boundary(system, first)
    work_b, _ = _worker_boundary(system, second)

    assert (
        _boundary_delivery(system, work_a["id"])["runtime_session_id"]
        == first["runtime_session_id"]
    )
    assert (
        _boundary_delivery(system, work_b["id"])["runtime_session_id"]
        == second["runtime_session_id"]
    )


def test_binding_validates_owner_and_exact_project(system: dict[str, Any]) -> None:
    first = _attach(system, thread_id="thread-a")
    first_actor = system["service"].authenticate(first["context_token"])
    with pytest.raises(AuthorizationError):
        _worker_boundary(
            system,
            {**first, "project_digest": "b" * 64},
            actor=first_actor,
        )

    foreign = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id="thread-other",
            project_digest=_PROJECT_DIGEST,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    with pytest.raises(AuthorizationError):
        _worker_boundary(system, foreign, actor=first_actor)

    bound, _report = _worker_boundary(system, actor=first_actor)
    assert bound["supervisor_attachment_id"] == first["id"]
    sealed_attachment = bound["current_goal_revision"]["packet"]["supervisor_attachment"]
    assert sealed_attachment["attachment_id"] == first["id"]
    assert sealed_attachment["project_digest"] == first["project_digest"]


def test_attachment_binding_is_digested_and_survives_goal_revision(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    initial = work["current_goal_revision"]["packet"]
    assert initial["supervisor_attachment"]["attachment_id"] == attachment["id"]
    assert work["current_attempt"]

    revised = system["service"].revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=1,
            objective="Revised exact-thread objective",
            maturity="defined",
            acceptance=["same thread remains bound"],
            reason="explicit delegation revision",
        ),
    )
    current = revised["current_goal_revision"]["packet"]
    assert current["supervisor_attachment"] == initial["supervisor_attachment"]
    assert (
        revised["current_attempt"]["task_packet_digest"]
        != work["current_attempt"]["task_packet_digest"]
    )


def test_unbound_work_is_queued_in_compatibility_and_rejected_in_strict_mode(
    system: dict[str, Any],
) -> None:
    work, _ = _worker_boundary(system)
    assert _boundary_delivery(system, work["id"])["runtime_session_id"] is None
    system["service"].settings = replace(
        system["service"].settings, require_cao_attachment_for_work=True
    )
    with pytest.raises(AuthorizationError):
        _worker_boundary(system)


def test_expired_attachment_lease_can_open_a_new_transport_connection(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system)
    service.db.execute(
        "UPDATE cao_session_attachments SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (attachment["id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?",
        (attachment["runtime_session_id"],),
    )
    wake_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
        )
    )
    work_before = tuple(tuple(row) for row in service.db.fetchall("SELECT * FROM work_items"))
    worker_before = tuple(
        tuple(row) for row in service.db.fetchall("SELECT * FROM managed_worker_threads")
    )
    old_connections = tuple(
        tuple(row)
        for row in service.db.fetchall(
            "SELECT id, state, revoked_at, lease_expires_at "
            "FROM cao_attachment_connections WHERE attachment_id = ? ORDER BY id",
            (attachment["id"],),
        )
    )
    old_credentials = tuple(
        tuple(row)
        for row in service.db.fetchall(
            "SELECT id, connection_id, state, revoked_at, expires_at "
            "FROM cao_conversation_credentials WHERE attachment_id = ? ORDER BY id",
            (attachment["id"],),
        )
    )
    prior_connection_generation = int(attachment["connection_generation"])

    connected = _attach(system)

    assert connected["id"] == attachment["id"]
    assert connected["generation"] == attachment["generation"]
    assert connected["runtime_session_id"] != attachment["runtime_session_id"]
    assert connected["connection_generation"] == prior_connection_generation + 1
    assert connected["state"] == "active"
    assert connected["runtime"]["state"] == "ready"
    assert connected["runtime"]["native_session_id"] == attachment["native_thread_id"]
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
            )
        )
        == wake_before
    )
    assert (
        tuple(tuple(row) for row in service.db.fetchall("SELECT * FROM work_items")) == work_before
    )
    assert (
        tuple(tuple(row) for row in service.db.fetchall("SELECT * FROM managed_worker_threads"))
        == worker_before
    )
    assert (
        tuple(
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, state, revoked_at, lease_expires_at "
                "FROM cao_attachment_connections WHERE attachment_id = ? AND id <> ? ORDER BY id",
                (attachment["id"], connected["connection_id"]),
            )
        )
        == old_connections
    )
    assert (
        tuple(
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, connection_id, state, revoked_at, expires_at "
                "FROM cao_conversation_credentials WHERE attachment_id = ? AND connection_id <> ? "
                "ORDER BY id",
                (attachment["id"], connected["connection_id"]),
            )
        )
        == old_credentials
    )
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM submitted_intents")["count"] == 0


def test_disconnected_wake_runtime_does_not_hide_attachment_inbox_from_new_connections(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="durable-attachment-inbox")
    first_actor = service.authenticate(attachment["context_token"])
    service.db.execute(
        "UPDATE cao_session_attachments SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (attachment["id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?",
        (attachment["runtime_session_id"],),
    )

    reconnected = _attach(system, thread_id="durable-attachment-inbox")
    second_actor = service.authenticate(reconnected["context_token"])
    work = service.assign_work(
        second_actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Durable attachment inbox",
            objective="Keep Worker progress visible after the old wake runtime disappears",
            acceptance=["Every authentic connection sees the same attachment-bound report"],
            runtime_session_id=system["runtime"]["id"],
        ),
    )
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            summary="The report is durable even though the wake route is unavailable",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            idempotency_key="disconnected-wake-progress",
        ),
    )

    delivery = service.db.fetchone(
        """
        SELECT delivery.*
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'progress'
        """,
        (attempt["id"],),
    )
    assert delivery is not None
    assert delivery["recipient_attachment_id"] == attachment["id"]
    assert delivery["runtime_session_id"] == reconnected["runtime_session_id"]
    first_inbox = service.get_inbox(first_actor, attempt_id=attempt["id"])
    second_inbox = service.get_inbox(second_actor, attempt_id=attempt["id"])
    assert [item["id"] for item in first_inbox["items"]] == [delivery["message_id"]]
    assert [item["id"] for item in second_inbox["items"]] == [delivery["message_id"]]

    foreign = _attach(system, thread_id="foreign-attachment-inbox")
    foreign_actor = service.authenticate(foreign["context_token"])
    assert service.get_inbox(foreign_actor, attempt_id=attempt["id"])["items"] == []
    with pytest.raises(AuthorizationError):
        service.acknowledge(foreign_actor, AckInput(message_ids=[delivery["message_id"]]))

    service.db.execute(
        "UPDATE cao_attachment_connections SET state = 'revoked', revoked_at = updated_at "
        "WHERE id = ?",
        (first_actor["_cao_connection_id"],),
    )
    with pytest.raises((AuthenticationError, AuthorizationError)):
        service.get_inbox(first_actor, attempt_id=attempt["id"])

    service.acknowledge(second_actor, AckInput(message_ids=[delivery["message_id"]]))
    service.mark_message_handled(
        second_actor,
        delivery["message_id"],
        evidence="the authentic attachment incorporated the Worker progress",
    )
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "handled"
    )
    assert asyncio.run(Dispatcher(service, system["settings"]).run_once()) == 0


def test_missing_wake_runtime_is_replaced_before_completion_dispatch(
    system: dict[str, Any],
) -> None:
    """An active attachment must not strand completion behind its old wake route."""

    attachment = _attach(system, thread_id="recovered-completion-wake")
    service = system["service"]
    old_runtime_id = str(attachment["runtime_session_id"])
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing', "
        "lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (old_runtime_id,),
    )

    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    assert delivery["state"] == "queued"
    assert delivery["runtime_session_id"] is None

    restarted_service = type(service)(service.db, service.settings)
    adapter = _CAOBrokerAdapter(restarted_service, consume_delivery=True)
    dispatcher = Dispatcher(
        restarted_service,
        system["settings"],
        registry=cast(Any, _Registry(adapter)),
    )
    assert asyncio.run(dispatcher.run_once()) == 1

    current_attachment = restarted_service.get_cao_attachment(attachment["id"])
    new_runtime_id = str(current_attachment["runtime_session_id"])
    assert current_attachment["generation"] == attachment["generation"]
    assert new_runtime_id != old_runtime_id
    assert current_attachment["runtime"]["state"] == "waiting"
    assert adapter.calls[0]["runtime"]["native_session_id"] == ("recovered-completion-wake")
    persisted = restarted_service.db.fetchone(
        "SELECT state, generation, runtime_session_id FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (delivery["message_id"], system["cao"]["id"]),
    )
    assert persisted is not None
    assert persisted["state"] == "handled"
    assert int(persisted["generation"]) == int(delivery["generation"]) + 1
    assert persisted["runtime_session_id"] == new_runtime_id
    assert restarted_service.get_runtime(old_runtime_id)["state"] == "missing"


def test_wake_route_replacement_never_rebinds_unknown_delivery(
    system: dict[str, Any],
) -> None:
    attachment = _attach(system, thread_id="unknown-wake-fence")
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Preserve unknown CAO wake",
            objective="Keep a post-dispatch wake outcome fenced",
            acceptance=["Only an unstarted later report may adopt a replacement route"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
        ),
    )
    attempt = work["current_attempt"]
    common = {
        "expected_goal_version": work["goal_version"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
        "expected_generation": work["generation"],
    }
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            summary="First report entered the wake boundary",
            idempotency_key="unknown-wake-first",
            **common,
        ),
    )
    first = service.db.fetchone(
        "SELECT d.* FROM messages AS m JOIN message_deliveries AS d "
        "ON d.message_id = m.id WHERE m.attempt_id = ? AND m.kind = 'progress'",
        (attempt["id"],),
    )
    assert first is not None
    old_runtime_id = str(first["runtime_session_id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched' "
        "WHERE message_id = ? AND recipient_id = ?",
        (first["message_id"], system["cao"]["id"]),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing', "
        "lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (old_runtime_id,),
    )
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.PROGRESS,
            summary="Second report has not crossed the wake boundary",
            idempotency_key="unknown-wake-second",
            **common,
        ),
    )

    assert service.reconcile_cao_wake_routes() == 1
    rows = service.db.fetchall(
        "SELECT m.sequence, d.state, d.generation, d.runtime_session_id "
        "FROM messages AS m JOIN message_deliveries AS d ON d.message_id = m.id "
        "WHERE m.attempt_id = ? AND m.kind = 'progress' ORDER BY m.sequence",
        (attempt["id"],),
    )
    assert len(rows) == 2
    assert rows[0]["state"] == "dispatched"
    assert rows[0]["runtime_session_id"] == old_runtime_id
    assert rows[1]["state"] == "queued"
    assert rows[1]["runtime_session_id"] != old_runtime_id


def test_schema37_backfills_the_exact_cao_delivery_attachment_once(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="delivery-attachment-migration")
    work, _reported = _worker_boundary(system, attachment)
    unresolved_work, _unresolved_report = _worker_boundary(system)
    stable_before = tuple(
        tuple(row)
        for row in service.db.fetchall(
            """
            SELECT message.id, message.payload_json, message.payload_digest,
                   message.message_digest, delivery.recipient_id, delivery.state,
                   delivery.generation, delivery.runtime_session_id,
                   delivery.created_at, delivery.updated_at
            FROM messages AS message
            JOIN message_deliveries AS delivery ON delivery.message_id = message.id
            ORDER BY message.sequence, delivery.recipient_id
            """
        )
    )
    with service.db.transaction() as connection:
        for trigger in (
            "message_deliveries_attachment_exact_insert",
            "message_deliveries_identity_immutable_update",
            "message_deliveries_attachment_immutable_update",
        ):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute("DROP INDEX IF EXISTS message_deliveries_attachment_state_idx")
        connection.execute("ALTER TABLE message_deliveries DROP COLUMN recipient_attachment_id")
        connection.execute("UPDATE metadata SET value = '36' WHERE key = 'schema_version'")
        connection.execute("DELETE FROM schema_migrations WHERE version = 37")
        connection.execute("PRAGMA user_version = 36")

    service.db.initialize()
    migrated = service.db.fetchone(
        """
        SELECT delivery.recipient_attachment_id
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ? AND delivery.recipient_id = ?
        """,
        (work["id"], system["cao"]["id"]),
    )
    assert migrated is not None
    assert migrated["recipient_attachment_id"] == attachment["id"]
    unresolved = service.db.fetchone(
        """
        SELECT delivery.recipient_attachment_id
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ? AND delivery.recipient_id = ?
        """,
        (unresolved_work["id"], system["cao"]["id"]),
    )
    assert unresolved is not None and unresolved["recipient_attachment_id"] is None
    foreign = _attach(system, thread_id="delivery-attachment-migration-foreign")
    with pytest.raises(sqlite3.IntegrityError, match="attachment binding is immutable"):
        service.db.execute(
            "UPDATE message_deliveries SET recipient_attachment_id = ? "
            "WHERE recipient_id = ? AND message_id IN "
            "(SELECT id FROM messages WHERE work_item_id = ?)",
            (foreign["id"], system["cao"]["id"], work["id"]),
        )
    assert (
        tuple(
            tuple(row)
            for row in service.db.fetchall(
                """
                SELECT message.id, message.payload_json, message.payload_digest,
                       message.message_digest, delivery.recipient_id, delivery.state,
                       delivery.generation, delivery.runtime_session_id,
                       delivery.created_at, delivery.updated_at
                FROM messages AS message
                JOIN message_deliveries AS delivery ON delivery.message_id = message.id
                ORDER BY message.sequence, delivery.recipient_id
                """
            )
        )
        == stable_before
    )
    service.db.initialize()
    assert (
        service.db.fetchone(
            "SELECT recipient_attachment_id FROM message_deliveries "
            "WHERE recipient_id = ? AND message_id IN "
            "(SELECT id FROM messages WHERE work_item_id = ?)",
            (system["cao"]["id"], work["id"]),
        )["recipient_attachment_id"]
        == attachment["id"]
    )
    assert verify_projection(service.db).healthy is True


def test_schema37_rejects_a_conflicting_prepopulated_delivery_attachment_atomically(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system, thread_id="delivery-attachment-conflict")
    work, _reported = _worker_boundary(system, attachment)
    foreign = _attach(system, thread_id="delivery-attachment-conflict-foreign")
    with service.db.transaction() as connection:
        connection.execute("DROP TRIGGER IF EXISTS message_deliveries_attachment_immutable_update")
        connection.execute(
            "UPDATE message_deliveries SET recipient_attachment_id = ? "
            "WHERE recipient_id = ? AND message_id IN "
            "(SELECT id FROM messages WHERE work_item_id = ?)",
            (foreign["id"], system["cao"]["id"], work["id"]),
        )
    before = {
        str(table["name"]): tuple(
            tuple(row)
            for row in service.db.fetchall(f'SELECT * FROM "{table["name"]}" ORDER BY rowid')
        )
        for table in service.db.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
    }

    with pytest.raises(RuntimeError, match="conflicting CAO delivery attachment binding"):
        service.db.initialize()

    after = {
        table: tuple(
            tuple(row) for row in service.db.fetchall(f'SELECT * FROM "{table}" ORDER BY rowid')
        )
        for table in before
    }
    assert after == before


class _Stdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, value: bytes) -> None:
        self.writes.append(value)

    async def drain(self) -> None:
        return None


class _LocalAppServerDouble:
    """Deterministic local Codex app-server protocol double (not a helper review)."""

    def __init__(self, server_name: str) -> None:
        self.pid = os.getpid()
        self.stdin = _Stdin()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode = 0
        for payload in (
            {"id": 1, "result": {}},
            {"id": 2, "result": {"thread": {"id": "attached-thread"}}},
            {
                "method": "mcpServer/startupStatus/updated",
                "params": {
                    "threadId": "attached-thread",
                    "name": server_name,
                    "status": "ready",
                },
            },
            {"id": 3, "result": {"turn": {"id": "turn-1"}}},
            {
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            },
        ):
            self.stdout.feed_data((json.dumps(payload) + "\n").encode())
        self.stdout.feed_eof()


def test_codex_adapter_resumes_attached_thread_with_cao_broker_config(
    monkeypatch: pytest.MonkeyPatch, settings: Any, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}
    runtime = {
        "id": "run-attached-cao-test",
        "endpoint": "http://127.0.0.1:8768/mcp",
        "native_session_id": "attached-thread",
        "cao_runtime_capability_socket": str(tmp_path / "cao-capability.sock"),
        "metadata": {},
    }
    server_name = _managed_codex_mcp_server_name(runtime)

    async def spawn(*argv: str, **kwargs: Any) -> _LocalAppServerDouble:
        captured["argv"] = list(argv)
        captured["env"] = kwargs["env"]
        process = _LocalAppServerDouble(server_name)
        captured["process"] = process
        return process

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_codex_app_server_command",
        lambda: ["codex", "app-server", "--stdio"],
    )
    result = asyncio.run(
        CodexAppServerAdapter(settings).dispatch(
            runtime,
            {"kind": "system", "payload": {"boundary_id": "bnd_1"}},
        )
    )
    assert result.success
    args = captured["argv"]
    assert args == ["codex", "app-server", "--stdio"]
    writes = [json.loads(value) for value in captured["process"].stdin.writes]
    methods = [value.get("method") for value in writes]
    assert methods[:3] == ["initialize", "initialized", "thread/resume"]
    assert methods[-1] == "turn/start"
    assert all(value.get("method") != "thread/start" for value in writes)
    resume = next(value for value in writes if value.get("method") == "thread/resume")
    assert (
        resume["params"]["config"][f"mcp_servers.{server_name}"]["args"][-2]
        == "--cao-runtime-broker-socket"
    )
