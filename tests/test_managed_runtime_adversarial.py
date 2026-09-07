"""Adversarial regressions for the managed Worker runtime enrollment boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import EnrollmentHandshakeAdapter, EnrollmentHandshakeRegistry

from cao_control_plane.database import utc_now
from cao_control_plane.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ValidationError,
)
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    MessageKind,
    ReportInput,
    ReportKind,
    RuntimeDispatchResult,
    RuntimeHeartbeat,
    RuntimeRegistration,
    WorkAssignment,
)
from cao_control_plane.runtime import ClaudeAdapter, Dispatcher, RuntimeAdapterError
from cao_control_plane.security import (
    contains_control_plane_secret,
    redact_control_plane_secrets,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS


def _reset_worker(system: dict[str, Any]) -> dict[str, Any]:
    system["service"].stop_runtime(system["cao"], system["runtime"]["id"])
    return system["service"].authenticate(system["worker_principal_token"])


def _register_awaiting_runtime(system: dict[str, Any]) -> dict[str, Any]:
    return system["service"].register_runtime(
        system["cao"],
        system["worker"]["id"],
        RuntimeRegistration(adapter="claude"),
    )


def test_first_pinned_assignment_establishes_handshake_without_bootstrap_model_turn(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="First managed delivery",
            objective="Establish MCP and receive the first exact task packet.",
            acceptance=["The assignment launch completes the managed handshake."],
            runtime_session_id=runtime["id"],
        ),
    )
    adapter = EnrollmentHandshakeAdapter(service)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    quiesced = service.get_runtime(runtime["id"])
    assert quiesced["state"] == "waiting"
    assert quiesced["enrollment"]["state"] == "ready"
    with pytest.raises(AuthenticationError):
        service.authenticate(adapter.credentials[-1])
    delivery = service.db.fetchone(
        """
        SELECT d.state
        FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.attempt_id = ? AND d.recipient_id = ?
        """,
        (work["current_attempt"]["id"], system["worker"]["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "delivered"
    bootstrap_count = service.db.fetchone(
        """
        SELECT COUNT(*) AS count
        FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE d.recipient_id = ? AND m.kind = 'system'
          AND json_extract(m.payload_json, '$.action') = 'establish_mcp_enrollment'
        """,
        (system["worker"]["id"],),
    )
    assert bootstrap_count is not None
    assert bootstrap_count["count"] == 0


def test_missing_default_claude_executable_fails_before_dispatch_unknown(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Pre-dispatch executable boundary",
            objective="Do not claim an unknown Worker outcome when no process can start.",
            acceptance=["The Delivery remains safely retryable before dispatch."],
            runtime_session_id=runtime["id"],
        ),
    )

    def unavailable() -> list[str]:
        raise RuntimeAdapterError("private path must not become durable")

    async def must_not_spawn(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("runtime process was started after executable discovery failed")

    monkeypatch.setattr("cao_control_plane.runtime._default_claude_command", unavailable)
    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", must_not_spawn)

    assert asyncio.run(Dispatcher(service, system["settings"]).run_once()) == 1

    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (work["current_attempt"]["id"],),
    )
    assert assignment is not None
    delivery = service.db.fetchone(
        "SELECT state, attempts, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (assignment["id"], system["worker"]["id"]),
    )
    assert delivery is not None
    assert dict(delivery) == {
        "state": "queued",
        "attempts": 1,
        "last_error": "runtime_unavailable",
    }
    tickets = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_enrollment_tickets "
        "WHERE enrollment_id = (SELECT id FROM worker_enrollments "
        "WHERE runtime_session_id = ?)",
        (runtime["id"],),
    )
    assert tickets is not None
    assert tickets["count"] == 0
    unknown_events = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'runtime.message_delivery_unknown' AND aggregate_id = ?",
        (assignment["id"],),
    )
    assert unknown_events is not None
    assert unknown_events["count"] == 0
    assert "private path" not in str(dict(delivery))


def test_terminal_prelaunch_unavailability_creates_exact_supervisor_boundary(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="terminal-prelaunch-supervisor",
            project_digest="b" * 64,
        ),
    )
    attached_cao = service.authenticate(str(attachment["context_token"]))
    work = service.assign_work(
        attached_cao,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Terminal prelaunch recovery",
            objective="Return terminal runtime unavailability to the exact supervisor.",
            acceptance=["One failure boundary is pinned to this CAO attachment."],
            runtime_session_id=runtime["id"],
        ),
    )

    def unavailable() -> list[str]:
        raise RuntimeAdapterError("owner-private executable is unavailable")

    async def must_not_spawn(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("runtime process started after terminal prelaunch failure")

    monkeypatch.setattr("cao_control_plane.runtime._default_claude_command", unavailable)
    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", must_not_spawn)
    terminal_settings = replace(system["settings"], max_dispatch_attempts=1)

    assert asyncio.run(Dispatcher(service, terminal_settings).run_once()) == 1

    assignment = service.db.fetchone(
        "SELECT message.id, delivery.state, delivery.attempts, delivery.last_error "
        "FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (work["current_attempt"]["id"],),
    )
    assert assignment is not None
    assert assignment["state"] == "dead"
    assert assignment["attempts"] == 1
    assert assignment["last_error"] == "runtime_unavailable"

    failed_runtime = service.get_runtime(runtime["id"])
    assert failed_runtime["state"] == "failed"
    assert failed_runtime["enrollment"]["state"] == "failed"
    recovered = service.get_work(work["id"])
    assert recovered["state"] == "waiting_supervisor"
    assert recovered["attention_owner"] == "cao"
    assert recovered["current_attempt"]["state"] == "waiting_supervisor"
    assert recovered["current_attempt"]["stage"] == "system_reconciliation"
    assert recovered["current_attempt"]["next_boundary"] == "system_reconciliation"

    boundary = service.db.fetchone(
        "SELECT id, kind, runtime_state, metadata_json FROM boundaries WHERE work_item_id = ?",
        (work["id"],),
    )
    assert boundary is not None
    assert boundary["kind"] == "failure"
    assert boundary["runtime_state"] == "failed"
    assert '"reason":"runtime_unavailable"' in boundary["metadata_json"]
    recovery = service.db.fetchone(
        "SELECT message.payload_json, delivery.recipient_id, "
        "delivery.runtime_session_id, delivery.state "
        "FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id "
        "WHERE message.work_item_id = ? AND message.kind = 'system' "
        "AND json_extract(message.payload_json, '$.action') = "
        "'recover_terminal_worker_attempt'",
        (work["id"],),
    )
    assert recovery is not None
    assert recovery["recipient_id"] == attached_cao["id"]
    assert recovery["runtime_session_id"] == attachment["runtime_session_id"]
    assert recovery["state"] == "queued"
    assert f'"boundary_id":"{boundary["id"]}"' in recovery["payload_json"]
    assert '"reason":"runtime_unavailable"' in recovery["payload_json"]


def test_resolved_default_claude_command_is_not_persisted_after_success(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Ephemeral executable resolution",
            objective="Keep the owner-private host locator out of durable runtime state.",
            acceptance=["Only the fixed dispatch summary is persisted."],
            runtime_session_id=runtime["id"],
        ),
    )
    private_locator = "/owner-private/native/claude"
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_claude_command",
        lambda: [private_locator],
    )
    handshake = EnrollmentHandshakeAdapter(service)

    class Registry:
        def prepare_launch(self, name: str, launch: Mapping[str, Any]) -> Mapping[str, Any]:
            assert name == "claude"
            return ClaudeAdapter(system["settings"]).prepare_launch(launch)

        def get(self, name: str) -> EnrollmentHandshakeAdapter:
            assert name == "claude"
            return handshake

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 1

    persisted = service.db.fetchone(
        "SELECT metadata_json FROM runtime_sessions WHERE id = ?", (runtime["id"],)
    )
    assert persisted is not None
    assert private_locator not in persisted["metadata_json"]
    assert "command" not in persisted["metadata_json"]
    assert "last_dispatch" in persisted["metadata_json"]


def test_raw_worker_principal_token_cannot_mark_a_delivery_handled(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Credential fence",
            objective="Only the managed runtime credential may consume Worker messages.",
            acceptance=["Principal credentials are rejected."],
        ),
    )
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (work["current_attempt"]["id"],),
    )
    assert assignment is not None
    principal_actor = service.authenticate(system["worker_principal_token"])

    with pytest.raises(AuthorizationError, match="managed MCP runtime credential"):
        service.mark_message_handled(
            principal_actor,
            assignment["id"],
            evidence="a raw principal token must not consume a Worker delivery",
        )


def test_inbox_acknowledgement_is_head_ordered_and_cannot_downgrade_handled(system):
    service = system["service"]
    worker = system["worker"]
    first = service.send_message(
        system["cao"],
        [worker["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"message": "first ordered instruction"},
    )
    second = service.send_message(
        system["cao"],
        [worker["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"message": "second ordered instruction"},
    )

    inbox = service.get_inbox(worker)
    assert [item["id"] for item in inbox["items"]] == [first["id"]]
    with pytest.raises(ConflictError, match="unhandled predecessor"):
        service.acknowledge(worker, AckInput(message_ids=[second["id"]]))

    service.acknowledge(worker, AckInput(message_ids=[first["id"]]))
    handled = service.mark_message_handled(
        worker, first["id"], evidence="first ordered instruction applied"
    )
    assert handled["state"] == "handled"
    replay = service.acknowledge(worker, AckInput(message_ids=[first["id"]]))
    assert replay["acknowledged"] == [first["id"]]
    state = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (first["id"], worker["id"]),
    )
    assert state is not None
    assert state["state"] == "handled"
    assert [item["id"] for item in service.get_inbox(worker)["items"]] == [second["id"]]


def test_stop_during_dispatcher_handshake_cannot_resurrect_or_leave_runtime_busy(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Stop-race first delivery",
            objective="Keep a stop authoritative during the initial managed launch.",
            acceptance=["The stopped runtime cannot be resurrected."],
            runtime_session_id=runtime["id"],
        ),
    )

    def stop_after_handshake(_: Mapping[str, Any], __: Mapping[str, Any]) -> None:
        service.stop_runtime(system["cao"], runtime["id"])

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=stop_after_handshake)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    stopped = service.get_runtime(runtime["id"])
    assert stopped["state"] == "stopped"
    assert stopped["enrollment"]["state"] == "revoked"
    assert stopped["state"] != "busy"
    active = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_credentials WHERE state = 'active'"
    )
    assert active is not None
    assert active["count"] == 0


def test_later_delivery_during_busy_runtime_epoch_cannot_rotate_current_credential(system):
    service = system["service"]
    runtime = system["runtime"]
    credential = system["worker_token"]
    generation = runtime["enrollment"]["generation"]
    service.send_message(
        system["cao"],
        [system["worker"]["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"message": "a later delivery while this runtime is busy"},
    )
    service.db.execute("UPDATE runtime_sessions SET state = 'busy' WHERE id = ?", (runtime["id"],))

    class BusyEpochAdapter:
        async def dispatch(
            self, launch: Mapping[str, Any], _: Mapping[str, Any]
        ) -> RuntimeDispatchResult:
            assert "enrollment_capability_socket" not in launch
            return RuntimeDispatchResult(success=True, state="ready")

    class Registry:
        def get(self, name: str) -> BusyEpochAdapter:
            assert name == "claude"
            return BusyEpochAdapter()

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 0
    assert service.authenticate(credential)["id"] == system["worker"]["id"]
    inbox = service.get_inbox(system["worker"])
    assert len(inbox["items"]) == 1
    service.acknowledge(system["worker"], AckInput(message_ids=[inbox["items"][0]["id"]]))
    service.mark_message_handled(
        system["worker"],
        inbox["items"][0]["id"],
        evidence="live runtime consumed the durable inbox without relaunch",
    )
    enrollment = service.get_runtime(runtime["id"])["enrollment"]
    assert enrollment["generation"] == generation


def test_expired_runtime_credential_has_a_terminal_fresh_registration_path(system):
    service = system["service"]
    runtime = system["runtime"]
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (runtime["id"],),
    )

    assert service.expire_runtime_leases() == 1
    expired = service.get_runtime(runtime["id"])
    assert expired["state"] == "missing"
    assert expired["enrollment"]["state"] in {"revoked", "failed"}
    with pytest.raises(AuthenticationError):
        service.authenticate(system["worker_token"])

    principal = service.authenticate(system["worker_principal_token"])
    fresh = service.register_runtime(
        system["cao"],
        principal["id"],
        RuntimeRegistration(adapter="claude"),
    )
    assert fresh["id"] != runtime["id"]
    assert fresh["enrollment"]["state"] == "awaiting_handshake"


def test_worker_report_atomically_handles_assignment_and_unblocks_successor(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Report ordering",
            objective="Make the next review instruction visible after reporting.",
            acceptance=["The assignment predecessor is handled atomically."],
            runtime_session_id=system["runtime"]["id"],
        ),
    )
    assignment = service.get_inbox(system["worker"])["items"][0]
    service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    attempt = work["current_attempt"]

    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The task packet has been completed.",
        ),
    )
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (assignment["id"], system["worker"]["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "handled"

    successor = service.send_message(
        system["cao"],
        [system["worker"]["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"message": "Apply the review correction."},
    )
    assert [item["id"] for item in service.get_inbox(system["worker"])["items"]] == [
        successor["id"]
    ]


def test_heartbeat_never_reactivates_semantically_closed_cancel_delivery(system):
    """A terminal Cancel cannot re-enter the FIFO lane on reconnect or replay."""

    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Terminal cancel predecessor",
            objective="Keep semantic history out of the live command lane.",
            acceptance=["Heartbeat does not resurrect the closed Delivery."],
            runtime_session_id=system["runtime"]["id"],
        ),
    )
    attempt = work["current_attempt"]
    with service.db.transaction() as connection:
        cancel = service._message(
            connection,
            sender_id=system["cao"]["id"],
            recipient_id=system["worker"]["id"],
            kind=MessageKind.CANCEL,
            payload={"reason": "terminal semantic history"},
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'dead', attempts = 5, "
            "last_error = 'terminal_work_headless_epoch_closed', "
            "reactivation_policy = 'terminal' WHERE message_id = ?",
            (cancel["id"],),
        )

    before = dict(
        service.db.fetchone(
            "SELECT state, generation, attempts, reactivation_policy "
            "FROM message_deliveries WHERE message_id = ?",
            (cancel["id"],),
        )
    )
    runtime = service.get_runtime(system["runtime"]["id"])
    for offset in (1, 2):
        service.heartbeat_runtime(
            system["worker"],
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=runtime["enrollment"]["generation"],
                sequence=runtime["enrollment"]["heartbeat_sequence"] + offset,
            ),
        )

    after = dict(
        service.db.fetchone(
            "SELECT state, generation, attempts, reactivation_policy "
            "FROM message_deliveries WHERE message_id = ?",
            (cancel["id"],),
        )
    )
    assert (
        after
        == before
        == {
            "state": "dead",
            "generation": 1,
            "attempts": 5,
            "reactivation_policy": "terminal",
        }
    )


def test_managed_registration_rejects_control_plane_environment_reinjection(system):
    non_secret_value = "must-not-reinject-the-control-plane-environment"
    with pytest.raises(ValidationError, match="reserved variable"):
        system["service"].register_runtime(
            system["cao"],
            system["worker"]["id"],
            RuntimeRegistration(
                adapter="claude",
                metadata={"environment": {"CAO_A2A_TOKEN": non_secret_value}},
            ),
        )
    persisted = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_sessions WHERE metadata_json LIKE ?",
        (f"%{non_secret_value}%",),
    )
    assert persisted is not None
    assert persisted["count"] == 0


def test_worker_heartbeat_cannot_persist_secrets_or_rewrite_launch_metadata(system):
    service = system["service"]
    runtime = service.get_runtime(system["runtime"]["id"])
    original_metadata = runtime["metadata"]
    secret = system["worker_token"]

    with pytest.raises(ValidationError, match="metadata is not mutable"):
        service.heartbeat_runtime(
            system["worker"],
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=runtime["enrollment"]["generation"],
                sequence=runtime["enrollment"]["heartbeat_sequence"] + 1,
                metadata={
                    "environment": {"UNRELATED_ALIAS": secret},
                    "command": ["/tmp/worker-controlled-launch"],
                },
            ),
        )

    persisted = service.db.fetchone(
        "SELECT metadata_json FROM runtime_sessions WHERE id = ?", (runtime["id"],)
    )
    assert persisted is not None
    assert secret not in persisted["metadata_json"]
    assert "/tmp/worker-controlled-launch" not in persisted["metadata_json"]
    assert service.get_runtime(runtime["id"])["metadata"] == original_metadata


def test_run_once_cannot_launch_or_reauthenticate_an_expired_first_assignment(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Expired first delivery",
            objective="Never launch after the pinned enrollment expires.",
            acceptance=["No runtime credential is issued."],
            runtime_session_id=runtime["id"],
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (runtime["id"],),
    )
    service.db.execute(
        "UPDATE worker_enrollments SET lease_expires_at = '2000-01-01T00:00:00Z' "
        "WHERE runtime_session_id = ?",
        (runtime["id"],),
    )
    with pytest.raises(ConflictError, match="lease has expired"):
        service.issue_runtime_launch_ticket(runtime["id"])

    adapter = EnrollmentHandshakeAdapter(service)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )
    assert asyncio.run(dispatcher.run_once()) == 0
    expired = service.get_runtime(runtime["id"])
    assert expired["state"] == "missing"
    assert expired["enrollment"]["state"] == "failed"
    assert adapter.credentials == []
    tickets = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_enrollment_tickets WHERE enrollment_id = ?",
        (expired["enrollment"]["id"],),
    )
    assert tickets is not None
    assert tickets["count"] == 0


def test_expired_initial_credential_cannot_renew_without_dispatcher(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    launch = service.issue_runtime_launch_ticket(runtime["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    actor = service.authenticate(str(exchange["token"]))
    expired_at = "2000-01-01T00:00:00Z"
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = ? WHERE id = ?",
        (expired_at, runtime["id"]),
    )
    service.db.execute(
        "UPDATE worker_enrollments SET lease_expires_at = ? WHERE runtime_session_id = ?",
        (expired_at, runtime["id"]),
    )

    with pytest.raises(AuthorizationError, match="stale or revoked"):
        service.record_mcp_tool_discovery(
            actor,
            protocol_version="2026-07-28",
            tool_names=WORKER_MCP_REQUIRED_TOOLS,
        )
    with pytest.raises(AuthorizationError, match="stale or revoked"):
        service.heartbeat_runtime(
            actor,
            runtime["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=actor["_enrollment_generation"],
                sequence=1,
            ),
        )

    persisted = service.get_runtime(runtime["id"])
    assert persisted["state"] == "busy"
    assert persisted["lease_expires_at"] == expired_at
    assert persisted["enrollment"]["state"] == "awaiting_handshake"
    assert persisted["enrollment"]["lease_expires_at"] == expired_at
    assert persisted["enrollment"]["heartbeat_sequence"] == 0


def test_ticket_issue_rejects_lease_expiring_in_the_current_second(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    expires_at = utc_now()
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = ? WHERE id = ?",
        (expires_at, runtime["id"]),
    )
    service.db.execute(
        "UPDATE worker_enrollments SET lease_expires_at = ? WHERE runtime_session_id = ?",
        (expires_at, runtime["id"]),
    )

    with pytest.raises(ConflictError, match="lease has expired"):
        service.issue_runtime_launch_ticket(runtime["id"])


def test_ticket_exchange_rejects_an_expired_runtime_epoch(system):
    service = system["service"]
    _reset_worker(system)
    runtime = _register_awaiting_runtime(system)
    launch = service.issue_runtime_launch_ticket(runtime["id"])
    expired_at = "2000-01-01T00:00:00Z"
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = ? WHERE id = ?",
        (expired_at, runtime["id"]),
    )

    with pytest.raises(AuthenticationError, match="invalid or expired"):
        service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    ticket = service.db.fetchone(
        "SELECT state FROM runtime_enrollment_tickets WHERE id = ?",
        (launch["ticket_id"],),
    )
    assert ticket is not None
    assert ticket["state"] == "pending"
    credentials = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_credentials "
        "WHERE enrollment_id = ("
        "SELECT id FROM worker_enrollments WHERE runtime_session_id = ?"
        ") AND state = 'active'",
        (runtime["id"],),
    )
    assert credentials is not None
    assert credentials["count"] == 0


def test_worker_report_rejects_runtime_credentials_before_any_durable_sink(system):
    service = system["service"]
    secret = system["worker_token"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Report secret fence",
            objective="Reject credentials before report persistence.",
            acceptance=["No report sink contains the runtime credential."],
            runtime_session_id=system["runtime"]["id"],
        ),
    )
    attempt = work["current_attempt"]
    request = ReportInput(
        kind=ReportKind.COMPLETION_CLAIM,
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        summary=f"completed with {secret}",
        evidence=[{"runtime_credential": secret}],
        artifacts=[
            ArtifactInput(
                name="credential-bearing-artifact",
                uri=f"memory://{secret}",
                metadata={secret: {"nested": secret}},
            )
        ],
        idempotency_key="reject-worker-report-secret",
    )
    assert contains_control_plane_secret(request.model_dump(mode="json"))

    with pytest.raises(ValidationError, match="credential-like secret") as error:
        service.report(system["worker"], attempt["id"], request)
    assert secret not in str(error.value)

    sinks = (
        ("attempts", "completion_claim_json"),
        ("boundaries", "summary"),
        ("boundaries", "metadata_json"),
        ("messages", "payload_json"),
        ("events", "data_json"),
        ("artifacts", "name"),
        ("artifacts", "uri"),
        ("artifacts", "metadata_json"),
        ("idempotency_results", "result_json"),
    )
    for table, column in sinks:
        row = service.db.fetchone(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
            (f"%{secret}%",),
        )
        assert row is not None
        assert row["count"] == 0, (table, column)


@pytest.mark.parametrize(
    ("secret", "metadata"),
    [
        (
            "cao.prn_parent.parent-control-plane-secret",
            {
                "environment": {
                    "UNRELATED_ENVIRONMENT_KEY": "cao.prn_parent.parent-control-plane-secret"
                }
            },
        ),
        (
            "cao.rtc_runtime.runtime-credential-secret",
            {
                "launch": {
                    "command": ["worker", "--cwd", "cao.rtc_runtime.runtime-credential-secret"]
                }
            },
        ),
        (
            "cao.ent_ticket.enrollment-ticket-secret",
            {"launch": {"ticket": "cao.ent_ticket.enrollment-ticket-secret"}},
        ),
        (
            "cao.prn_mapping_key.mapping-key-secret",
            {"nested": {"cao.prn_mapping_key.mapping-key-secret": "opaque"}},
        ),
    ],
)
def test_managed_registration_rejects_nested_control_plane_secrets_before_persistence(
    system, secret, metadata
):
    assert contains_control_plane_secret(metadata)
    redacted = redact_control_plane_secrets(metadata)
    assert secret not in str(redacted)
    assert "[control-plane-credential-redacted]" in str(redacted)

    with pytest.raises(ValidationError, match="credential-like secret") as error:
        system["service"].register_runtime(
            system["cao"],
            system["worker"]["id"],
            RuntimeRegistration(adapter="claude", metadata=metadata),
        )

    assert secret not in str(error.value)
    persisted = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_sessions WHERE metadata_json LIKE ?",
        (f"%{secret}%",),
    )
    assert persisted is not None
    assert persisted["count"] == 0
