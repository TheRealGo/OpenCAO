from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.artifact_preservation_edge import ARTIFACT_CONTENT_MAX_BYTES
from cao_control_plane.database import Database, utc_after, utc_now
from cao_control_plane.errors import AuthorizationError, ConflictError, NotFoundError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    ArtifactContentReadInput,
    ArtifactInput,
    BoundaryDispositionInput,
    CompletionContract,
    MessageKind,
    ReportInput,
    ReviewInput,
    RuntimeHeartbeat,
    StatusRequestInput,
    WorkAssignment,
    WorkerOutputReadInput,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane
from cao_control_plane.worker_output import WorkerOutputEvent


def _capture_fixture(
    system: dict[str, Any],
    *,
    completion_contract: CompletionContract = CompletionContract.NO_ARTIFACT_EXPECTED,
    consume_launch: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="output-supervisor",
            project_digest="a" * 64,
        ),
    )
    actor = service.authenticate(str(attachment["context_token"]))
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Provider-owned output",
            objective="Return a bounded answer for independent supervisor review.",
            acceptance=["The answer remains reviewable without a reporting tool call."],
            completion_contract=completion_contract,
            idempotency_key="assign:provider-output",
        ),
    )
    source = service.db.fetchone(
        "SELECT d.* FROM message_deliveries d JOIN messages m ON m.id = d.message_id "
        "WHERE m.attempt_id = ? AND m.kind = 'assignment' AND d.recipient_id = ?",
        (work["current_attempt"]["id"], system["worker"]["id"]),
    )
    assert source is not None
    launch = service.issue_runtime_launch_ticket(
        system["runtime"]["id"], attempt_id=work["current_attempt"]["id"]
    )
    ticket = service.db.fetchone(
        "SELECT generation FROM runtime_enrollment_tickets WHERE id = ?", (launch["ticket_id"],)
    )
    assert ticket is not None
    # This fixture stops at the dispatch reservation boundary. It does not
    # fabricate a model report, a model ACK, or a provider completion receipt.
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched', attempts = 1, "
        "owner_token = ?, lease_until = ? WHERE message_id = ? AND recipient_id = ?",
        (
            "test-output-dispatch-owner",
            utc_after(3600),
            source["message_id"],
            source["recipient_id"],
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'busy', native_session_id = ? WHERE id = ?",
        ("output-provider-thread", system["runtime"]["id"]),
    )
    binding = {
        "runtime_id": system["runtime"]["id"],
        "attempt_id": work["current_attempt"]["id"],
        "delivery_message_id": source["message_id"],
        "delivery_generation": int(source["generation"]),
        "enrollment_generation": int(ticket["generation"]),
        "owner_token": "test-output-dispatch-owner",
    }
    service.begin_worker_output_capture(**binding)
    if consume_launch:
        exchanged = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
        worker = service.authenticate(str(exchanged["token"]))
        service.record_mcp_tool_discovery(
            worker, protocol_version="2025-06-18", tool_names=WORKER_MCP_REQUIRED_TOOLS
        )
        service.heartbeat_runtime(
            worker,
            system["runtime"]["id"],
            RuntimeHeartbeat(
                expected_enrollment_generation=binding["enrollment_generation"], sequence=1
            ),
        )
        system["worker"] = worker
        system["worker_token"] = str(exchanged["token"])
    return actor, work, binding


def _event(**overrides: Any) -> WorkerOutputEvent:
    values = {
        "native_thread_id": "output-provider-thread",
        "turn_id": "output-provider-turn",
        "item_id": "output-item-1",
        "kind": "message",
        "text": "The bounded answer is forty-two.",
        "phase": "final",
    }
    values.update(overrides)
    return WorkerOutputEvent(**values)


def _finalize_successful_capture(
    service: ControlPlane, binding: dict[str, Any], *, publish: bool = True
) -> None:
    """Model the Dispatcher's authenticated, quiescent success transaction."""

    with service.db.transaction() as connection:
        enrollment = connection.execute(
            "SELECT * FROM worker_enrollments WHERE runtime_session_id = ?",
            (binding["runtime_id"],),
        ).fetchone()
        assert enrollment["state"] == "ready"
        assert enrollment["generation"] == binding["enrollment_generation"]
        runtime = connection.execute(
            "SELECT * FROM runtime_sessions WHERE id = ?", (binding["runtime_id"],)
        ).fetchone()
        summary = {
            "success": True,
            "state": "waiting",
            "diagnostics": {"turn_status": "completed", "delivery_acceptance": "completed"},
        }
        metadata = json.loads(runtime["metadata_json"])
        metadata.update(
            {"last_dispatch": summary, "last_dispatch_message_id": binding["delivery_message_id"]}
        )
        connection.execute(
            "UPDATE runtime_sessions SET state = 'waiting', metadata_json = ? WHERE id = ?",
            (json.dumps(metadata), binding["runtime_id"]),
        )
        connection.execute(
            "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, updated_at = ? WHERE enrollment_id = ? AND generation = ? AND state = 'active'",
            (utc_now(), utc_now(), enrollment["id"], binding["enrollment_generation"]),
        )
        prior = connection.execute(
            "SELECT 1 FROM events WHERE event_type = 'runtime.message_delivered' AND aggregate_id = ? AND causation_id = ?",
            (binding["runtime_id"], binding["delivery_message_id"]),
        ).fetchone()
        if prior is None:
            service._event(
                connection,
                "runtime.message_delivered",
                "runtime",
                binding["runtime_id"],
                "",
                {
                    "message_id": binding["delivery_message_id"],
                    "adapter": runtime["adapter"],
                    "result": summary,
                },
                causation_id=binding["delivery_message_id"],
            )
        if publish:
            service._finalize_worker_output_capture_tx(
                connection,
                runtime_id=binding["runtime_id"],
                attempt_id=binding["attempt_id"],
                delivery_message_id=binding["delivery_message_id"],
            )


def _recover_failed_capture(service: ControlPlane, binding: dict[str, Any]) -> None:
    service.fail_runtime_enrollment(binding["runtime_id"], reason="runtime_dispatch_failed")
    service.recover_terminal_worker_attempt(binding["runtime_id"], reason="runtime_dispatch_failed")
    service.reconcile_abandoned_worker_output_captures()


def _terminal(
    service: ControlPlane, binding: dict[str, Any], *, settle: bool = True, **overrides: Any
) -> dict[str, Any]:
    values = {"kind": "turn_end", "item_id": "", "text": "", "status": "completed"}
    values.update(overrides)
    result = service.observe_worker_output(**binding, event=_event(**values))
    if settle:
        if values["status"] == "completed":
            _finalize_successful_capture(service, binding)
        else:
            _recover_failed_capture(service, binding)
    receipt = service.db.fetchone(
        "SELECT work_item_id FROM worker_output_receipts WHERE id = ?", (result["id"],)
    )
    return next(
        item
        for item in service.get_work(str(receipt["work_item_id"]))["worker_outputs"]
        if item["id"] == result["id"]
    )


def _read_request(
    work: dict[str, Any], output: dict[str, Any], **overrides: Any
) -> WorkerOutputReadInput:
    values = {
        "work_item_id": work["id"],
        "attempt_id": work["current_attempt"]["id"],
        "output_id": output["id"],
        "expected_digest": output["digest"],
        "byte_offset": 0,
        "max_bytes": 65_536,
    }
    values.update(overrides)
    values.setdefault(
        "idempotency_key", f"read:{output['id']}:{values['byte_offset']}:{values['max_bytes']}"
    )
    return WorkerOutputReadInput(**values)


def _review(
    service: ControlPlane, actor: dict[str, Any], work: dict[str, Any], *, verdict: str = "ok"
) -> dict[str, Any]:
    return service.review(
        actor,
        ReviewInput(
            attempt_id=work["current_attempt"]["id"],
            verdict=verdict,
            summary="Independent supervisor judgment of the exact retained result.",
            evidence=[{"check": "retained-output", "result": verdict}],
            idempotency_key=f"review:output:{verdict}",
        ),
    )


def _dispose(
    service: ControlPlane, actor: dict[str, Any], work: dict[str, Any], *, kind: str
) -> dict[str, Any]:
    current = service.get_work(work["id"])
    boundary = current["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=current["generation"],
        idempotency_key=f"turn:output:{kind}",
    )
    return service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=current["generation"],
            kind=kind,
            reason="Apply the explicit supervisor judgment.",
            instruction="Inspect the retained result and correct the missing condition."
            if kind in {"correct", "continue"}
            else "",
        ),
    )


def _row_count(service: ControlPlane, table: str) -> int:
    assert table in {
        "worker_output_receipts",
        "worker_output_streams",
        "reviews",
        "artifacts",
        "events",
        "messages",
        "boundaries",
    }
    row = service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")
    assert row is not None
    return int(row["count"])


def _queued_successor(
    system: dict[str, Any],
    actor: dict[str, Any],
    work: dict[str, Any],
    *,
    kind: str,
    through_status_api: bool = True,
) -> str:
    service = system["service"]
    if kind == "status_request" and through_status_api:
        service.request_status(
            actor,
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Return the current bounded execution status.",
                idempotency_key="status:pending-output-successor",
            ),
        )
        message = service.db.fetchone(
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'status_request' ORDER BY sequence DESC LIMIT 1",
            (work["current_attempt"]["id"],),
        )
        assert message is not None
        return str(message["id"])
    # Seed through the shared durable-message edge to represent an admitted
    # instruction or a historical pending input, not a model-authored ACK.
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=actor["id"],
            recipient_id=system["worker"]["id"],
            kind=MessageKind(kind),
            payload={
                "action": "continue",
                "instruction": "Inspect the additional bounded condition.",
                "generation": work["generation"],
            },
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            goal_version=work["goal_version"],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key=f"input:pending-output-successor:{kind}",
        )
    return str(message["id"])


def _begin_successor_capture(
    service: ControlPlane, binding: dict[str, Any], message_id: str
) -> dict[str, Any]:
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?", (binding["runtime_id"],)
    )
    service.db.execute(
        "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, updated_at = ? WHERE enrollment_id IN (SELECT id FROM worker_enrollments WHERE runtime_session_id = ?) AND state = 'active'",
        (utc_now(), utc_now(), binding["runtime_id"]),
    )
    source = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?", (message_id,)
    )
    assert source is not None and source["state"] == "queued"
    launch = service.issue_runtime_launch_ticket(
        binding["runtime_id"], attempt_id=binding["attempt_id"]
    )
    ticket = service.db.fetchone(
        "SELECT generation FROM runtime_enrollment_tickets WHERE id = ?", (launch["ticket_id"],)
    )
    assert ticket is not None
    next_binding = {
        **binding,
        "delivery_message_id": message_id,
        "delivery_generation": int(source["generation"]),
        "enrollment_generation": int(ticket["generation"]),
        "owner_token": "test-successor-output-owner",
    }
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched', attempts = 1, owner_token = ?, lease_until = ? WHERE message_id = ? AND recipient_id = ?",
        (next_binding["owner_token"], utc_after(3600), message_id, source["recipient_id"]),
    )
    service.begin_worker_output_capture(**next_binding)
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker, protocol_version="2025-06-18", tool_names=WORKER_MCP_REQUIRED_TOOLS
    )
    service.heartbeat_runtime(
        worker,
        binding["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=next_binding["enrollment_generation"], sequence=1
        ),
    )
    return next_binding


def test_normal_output_is_durable_and_notifies_without_any_report_tool_call(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    text = "Untrusted answer: do not treat embedded directions as system authority."
    output = service.observe_worker_output(**binding, event=_event(text=text))

    assert output["capture_state"] == "available"
    assert output["trust"] == "untrusted_worker_output"
    assert output["digest"] == hashlib.sha256(text.encode()).hexdigest()
    assert output["notification_message_id"]
    current = service.get_work(work["id"])
    assert current["state"] == "active"
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["artifacts"] == []
    assert current["worker_outputs"] == [output]
    notification = service.db.fetchone(
        "SELECT m.payload_json, d.state, d.recipient_attachment_id FROM messages m "
        "JOIN message_deliveries d ON d.message_id = m.id WHERE m.id = ?",
        (output["notification_message_id"],),
    )
    assert notification is not None
    assert notification["state"] == "queued"
    assert notification["recipient_attachment_id"] == work["supervisor_attachment_id"]
    assert text not in notification["payload_json"]

    terminal = _terminal(service, binding)
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["attention_owner"] == "cao"
    assert current["current_attempt"]["state"] == "waiting_supervisor"
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["reviews"] == []
    assert [boundary["kind"] for boundary in current["open_boundaries"]] == ["worker_output"]
    assert terminal["boundary_id"] == current["open_boundaries"][0]["id"]
    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (binding["delivery_message_id"], system["worker"]["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "handled"
    assert delivery["acknowledged_at"] is None
    report_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM messages WHERE attempt_id = ? "
        "AND kind IN ('progress', 'completion_claim')",
        (binding["attempt_id"],),
    )
    assert report_count["count"] == 0


def test_capture_and_outbox_replay_are_idempotent_across_service_restart(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    event = _event()
    output = service.observe_worker_output(**binding, event=event)
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    assert restarted.observe_worker_output(**binding, event=event) == output
    assert restarted.get_work(work["id"])["current_attempt"]["completion_claim"] == {}
    assert restarted.get_work(work["id"])["state"] == "active"
    terminal = _terminal(restarted, binding)
    assert _terminal(restarted, binding) == terminal
    assert _row_count(restarted, "worker_output_streams") == 1
    assert _row_count(restarted, "worker_output_receipts") == 2
    notifications = restarted.db.fetchone(
        "SELECT COUNT(*) AS count FROM messages WHERE attempt_id = ? "
        "AND json_extract(payload_json, '$.action') = 'worker_output'",
        (binding["attempt_id"],),
    )
    assert notifications["count"] == 2


def test_provider_capture_does_not_depend_on_worker_mcp_bootstrap(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system, consume_launch=False)
    output = service.observe_worker_output(**binding, event=_event())
    terminal = _terminal(service, binding, settle=False)
    pending = service.db.fetchone(
        "SELECT state FROM runtime_enrollment_tickets WHERE attempt_id = ? AND generation = ?",
        (binding["attempt_id"], binding["enrollment_generation"]),
    )
    assert pending["state"] == "pending"
    assert output["capture_state"] == "available"
    assert terminal["boundary_id"] == ""
    assert service.get_work(work["id"])["state"] == "active"
    assert (
        service.read_worker_output(actor, _read_request(work, output))["content"] == _event().text
    )
    assert service.get_work(work["id"])["current_attempt"]["completion_claim"] == {}
    _recover_failed_capture(service, binding)
    failed = service.get_work(work["id"])
    assert failed["attention_owner"] == "cao"
    assert [item["kind"] for item in failed["open_boundaries"]] == ["failure"]
    assert failed["worker_outputs"][-1]["boundary_id"] == failed["open_boundaries"][0]["id"]
    with pytest.raises(ConflictError):
        _review(service, actor, work)


def test_provider_terminal_cannot_steal_work_before_dispatcher_success_settlement(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    terminal = _terminal(service, binding, settle=False)
    assert terminal["boundary_id"] == ""
    assert service.get_runtime(binding["runtime_id"])["state"] == "busy"
    current = service.get_work(work["id"])
    assert current["state"] == "active"
    assert current["attention_owner"] == "worker"
    assert current["open_boundaries"] == []
    service.read_worker_output(actor, _read_request(work, output))
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    with service.db.transaction() as connection:
        service._finalize_worker_output_capture_tx(
            connection,
            runtime_id=binding["runtime_id"],
            attempt_id=binding["attempt_id"],
            delivery_message_id=binding["delivery_message_id"],
        )
    assert service.get_work(work["id"])["open_boundaries"] == []
    _finalize_successful_capture(service, binding)
    assert [item["kind"] for item in service.get_work(work["id"])["open_boundaries"]] == [
        "worker_output"
    ]


def test_partial_stream_without_terminal_is_not_reviewable_after_restart(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    restarted.read_worker_output(actor, _read_request(work, output))
    assert restarted.reconcile_abandoned_worker_output_captures() == 0
    with pytest.raises(ConflictError):
        _review(restarted, actor, work)
    assert restarted.get_work(work["id"])["state"] == "active"
    assert _row_count(restarted, "worker_output_receipts") == 1


def test_runtime_expiry_retains_partial_capture_and_settles_it_once(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    expired = utc_after(-60)
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = ? WHERE id = ?",
        (expired, binding["runtime_id"]),
    )
    service.db.execute(
        "UPDATE worker_enrollments SET lease_expires_at = ? WHERE runtime_session_id = ?",
        (expired, binding["runtime_id"]),
    )
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    restarted.expire_runtime_leases()
    assert restarted.reconcile_abandoned_worker_output_captures() == 1
    assert restarted.reconcile_abandoned_worker_output_captures() == 0
    current = restarted.get_work(work["id"])
    terminal = current["worker_outputs"][-1]
    assert terminal["kind"] == "turn_end"
    assert terminal["turn_status"] == "interrupted"
    assert terminal["capture_state"] == "unavailable"
    assert terminal["complete"] is False
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["attention_owner"] == "cao"
    assert terminal["boundary_id"] in {item["id"] for item in current["open_boundaries"]}
    assert (
        restarted.read_worker_output(actor, _read_request(work, output))["content"] == _event().text
    )
    with pytest.raises(ConflictError):
        _review(restarted, actor, work)


def test_receipt_boundary_and_notification_commit_atomically(system, monkeypatch) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding, settle=False)
    before = service.get_work(work["id"])

    def fail_outbox(*_: Any, **__: Any) -> dict[str, Any]:
        raise OSError("injected durable outbox failure")

    with monkeypatch.context() as patch:
        patch.setattr(service, "_message", fail_outbox)
        with pytest.raises(OSError, match="injected durable outbox failure"):
            _finalize_successful_capture(service, binding)
    assert service.get_work(work["id"]) == before
    assert _row_count(service, "worker_output_receipts") == 2
    assert _terminal(service, binding)["boundary_id"]


def test_first_output_receipt_and_availability_outbox_commit_atomically(
    system, monkeypatch
) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    before = service.get_work(work["id"])

    def fail_outbox(*_: Any, **__: Any) -> dict[str, Any]:
        raise OSError("injected output availability failure")

    with monkeypatch.context() as patch:
        patch.setattr(service, "_message", fail_outbox)
        with pytest.raises(OSError, match="injected output availability failure"):
            service.observe_worker_output(**binding, event=_event())
    assert _row_count(service, "worker_output_receipts") == 0
    assert service.get_work(work["id"]) == before
    assert service.observe_worker_output(**binding, event=_event())["notification_message_id"]


def test_reused_output_identity_with_different_content_is_rejected_without_mutation(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    before = service.get_work(work["id"])
    with pytest.raises(ConflictError, match="identity"):
        service.observe_worker_output(**binding, event=_event(text="A changed answer."))
    assert service.get_work(work["id"]) == before
    assert _row_count(service, "worker_output_receipts") == 1


@pytest.mark.parametrize(
    "field", ["owner_token", "runtime_id", "attempt_id", "delivery_message_id"]
)
def test_capture_requires_exact_dispatch_reservation(system, field: str) -> None:
    service = system["service"]
    _, _, binding = _capture_fixture(system)
    invalid = {**binding, field: "a-different-binding"}
    with pytest.raises(ConflictError):
        service.observe_worker_output(**invalid, event=_event())
    assert _row_count(service, "worker_output_receipts") == 0


@pytest.mark.parametrize("target", ["work", "enrollment", "delivery"])
def test_callback_cannot_cross_a_retired_generation(system, target: str) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    if target == "work":
        service.db.execute(
            "UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],)
        )
    elif target == "enrollment":
        service.db.execute(
            "UPDATE worker_enrollments SET generation = generation + 1 WHERE runtime_session_id = ?",
            (binding["runtime_id"],),
        )
    else:
        service.db.execute(
            "UPDATE message_deliveries SET generation = generation + 1, state = 'dead', owner_token = '' WHERE message_id = ?",
            (binding["delivery_message_id"],),
        )
    with pytest.raises(ConflictError):
        service.observe_worker_output(**binding, event=_event())
    assert _row_count(service, "worker_output_receipts") == 0


def test_initial_output_must_match_the_known_native_worker_thread(system) -> None:
    service = system["service"]
    _, _, binding = _capture_fixture(system)
    with pytest.raises(ConflictError):
        service.observe_worker_output(
            **binding, event=_event(native_thread_id="another-provider-thread")
        )
    assert _row_count(service, "worker_output_receipts") == 0


def test_first_observed_native_worker_thread_is_preserved_after_provider_failure(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.db.execute(
        "UPDATE runtime_sessions SET native_session_id = '' WHERE id = ?", (binding["runtime_id"],)
    )
    output = service.observe_worker_output(**binding, event=_event())
    assert (
        service.get_runtime(binding["runtime_id"])["native_session_id"] == _event().native_thread_id
    )
    terminal = _terminal(service, binding, status="failed", complete=False)
    assert (
        service.get_runtime(binding["runtime_id"])["native_session_id"] == _event().native_thread_id
    )
    current = service.get_work(work["id"])
    assert [item["kind"] for item in current["open_boundaries"]] == ["failure"]
    assert terminal["boundary_id"] == current["open_boundaries"][0]["id"]
    assert current["worker_outputs"][0]["id"] == output["id"]
    assert _event().native_thread_id not in json.dumps(
        [dict(row) for row in service.db.fetchall("SELECT * FROM events")]
    )


@pytest.mark.parametrize("target", ["work", "enrollment", "delivery"])
def test_finalization_cannot_publish_success_for_a_retired_capture_generation(
    system, target: str
) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding, settle=False)
    _finalize_successful_capture(service, binding, publish=False)
    if target == "work":
        service.db.execute(
            "UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],)
        )
    elif target == "enrollment":
        service.db.execute(
            "UPDATE worker_enrollments SET generation = generation + 1 WHERE runtime_session_id = ?",
            (binding["runtime_id"],),
        )
    else:
        service.db.execute(
            "UPDATE message_deliveries SET generation = generation + 1 WHERE message_id = ?",
            (binding["delivery_message_id"],),
        )
    with service.db.transaction() as connection:
        service._finalize_worker_output_capture_tx(
            connection,
            runtime_id=binding["runtime_id"],
            attempt_id=binding["attempt_id"],
            delivery_message_id=binding["delivery_message_id"],
        )
    current = service.get_work(work["id"])
    assert "worker_output" not in {item["kind"] for item in current["open_boundaries"]}
    assert current["current_attempt"]["completion_claim"] == {}


@pytest.mark.parametrize("field", ["native_thread_id", "turn_id"])
def test_later_provider_event_cannot_switch_thread_or_turn(system, field: str) -> None:
    service = system["service"]
    _, _, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    with pytest.raises(ConflictError, match=r"another provider (thread|turn)"):
        service.observe_worker_output(
            **binding, event=_event(item_id="another-item", **{field: "another-source"})
        )
    assert _row_count(service, "worker_output_receipts") == 1


def test_late_new_item_cannot_reopen_a_terminal_output_stream(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    terminal = _terminal(service, binding)
    before = service.get_work(work["id"])
    with pytest.raises(ConflictError, match="terminal"):
        service.observe_worker_output(**binding, event=_event(item_id="late-item"))
    assert _terminal(service, binding) == terminal
    assert service.get_work(work["id"]) == before


def test_output_is_private_but_readable_through_exact_attachment_scoped_chunks(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    content = "ab日語cd" + str(system["settings"].state_dir / "private-result")
    output = service.observe_worker_output(**binding, event=_event(text=content))
    request = _read_request(work, output, max_bytes=5)
    first = service.read_worker_output(actor, request)
    assert first["content"] == "ab日"
    assert first["next_byte_offset"] == 5
    assert first["complete"] is False
    assert service.read_worker_output(actor, request) == first
    with pytest.raises(ConflictError):
        service.read_worker_output(actor, request.model_copy(update={"max_bytes": 6}))
    with pytest.raises(ConflictError):
        service.read_worker_output(actor, _read_request(work, output, expected_digest="0" * 64))
    with pytest.raises(AuthorizationError):
        service.read_worker_output(system["cao"], request)
    with pytest.raises(AuthorizationError):
        service.read_worker_output(system["worker"], request)
    other = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="other-output-supervisor", project_digest="a" * 64
        ),
    )
    other_actor = service.authenticate(str(other["context_token"]))
    with pytest.raises(NotFoundError):
        service.read_worker_output(other_actor, request)
    with pytest.raises(NotFoundError):
        service.read_worker_output(actor, _read_request(work, output, attempt_id="att_another"))
    for query in (
        "SELECT * FROM worker_output_receipts",
        "SELECT * FROM worker_output_streams",
        "SELECT * FROM messages",
        "SELECT * FROM events",
    ):
        rows = [dict(row) for row in service.db.fetchall(query)]
        assert content not in json.dumps(rows)
        assert "output-provider-thread" not in json.dumps(rows)
        assert "output-provider-turn" not in json.dumps(rows)
    assert content not in json.dumps(service.get_work(work["id"]))
    assert _row_count(service, "artifacts") == 0


def test_public_output_read_tool_is_only_available_to_current_attached_cao(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    server = MCPServer(service)
    assert "cao_read_worker_output" in {tool["name"] for tool in server.tools_for(actor)}
    for other_actor in (system["cao"], system["worker"], system["user"]):
        assert "cao_read_worker_output" not in {
            tool["name"] for tool in server.tools_for(other_actor)
        }
    arguments = _read_request(work, output).model_dump(mode="json")
    read = server.call_tool(actor, "cao_read_worker_output", arguments)
    assert read["content"] == _event().text
    assert read["complete"] is True
    assert read["trust"] == "untrusted_worker_output"
    public_work = server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
    assert public_work["worker_outputs"][0]["id"] == output["id"]
    assert _event().text not in json.dumps(public_work)


def test_old_attachment_generation_read_does_not_authorize_current_review(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, output))
    next_generation = int(actor["_cao_attachment_generation"]) + 1
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE cao_session_attachments SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_attachment_connections SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_connection_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_conversation_credential_id"]),
        )
    with pytest.raises(AuthorizationError):
        service.read_worker_output(actor, _read_request(work, output))
    current_actor = {**actor, "_cao_attachment_generation": next_generation}
    with pytest.raises(ConflictError):
        _review(service, current_actor, work)
    service.read_worker_output(
        current_actor,
        _read_request(work, output, idempotency_key="read:current-attachment-generation"),
    )
    assert _review(service, current_actor, work)["reviews"][-1]["verdict"] == "ok"


def test_review_requires_full_audited_output_read_then_accept_remains_separate(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event(text="0123456789ab"))
    terminal = _terminal(service, binding)
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    assert _row_count(service, "reviews") == 0
    assert service.get_work(work["id"])["current_attempt"]["completion_claim"] == {}
    with pytest.raises(ConflictError):
        _dispose(service, actor, work, kind="accept")
    service.read_worker_output(actor, _read_request(work, output, byte_offset=0, max_bytes=4))
    service.read_worker_output(actor, _read_request(work, output, byte_offset=8, max_bytes=4))
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    service.read_worker_output(actor, _read_request(work, output, byte_offset=4, max_bytes=4))
    reviewed = _review(service, actor, work)
    assert reviewed["state"] == "waiting_supervisor"
    claim = reviewed["current_attempt"]["completion_claim"]
    assert claim["source"] == "cao_reviewed_worker_output"
    assert claim["output_id"] == terminal["id"]
    assert claim["artifacts"] == []
    assert reviewed["reviews"][-1]["reviewer_role"] == "cao"
    _dispose(service, actor, work, kind="accept")
    accepted = service.get_work(work["id"])
    assert accepted["state"] == "waiting_user"
    assert accepted["attention_owner"] == "user"
    assert accepted["current_attempt"]["state"] == "completed"
    assert accepted["current_attempt"]["evidence_confidence"] == "verified"
    assert accepted["open_boundaries"] == []


def test_review_requires_every_distinct_substantive_output_item(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    first = service.observe_worker_output(
        **binding, event=_event(text="First result part: the bounded caveat.")
    )
    second = service.observe_worker_output(
        **binding,
        event=_event(item_id="output-item-2", text="Second result part: the bounded conclusion."),
    )
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, second))
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    assert _row_count(service, "reviews") == 0
    service.read_worker_output(actor, _read_request(work, first))
    assert _review(service, actor, work)["reviews"][-1]["verdict"] == "ok"


def test_exact_repeated_final_content_requires_only_one_complete_read(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding, text=_event().text)
    service.read_worker_output(actor, _read_request(work, output))
    assert _review(service, actor, work)["reviews"][-1]["verdict"] == "ok"


def test_identical_content_read_in_an_earlier_stream_does_not_satisfy_review(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    original = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, original))
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?", (binding["runtime_id"],)
    )
    service.db.execute(
        "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, updated_at = ? WHERE enrollment_id IN (SELECT id FROM worker_enrollments WHERE runtime_session_id = ?) AND state = 'active'",
        (utc_now(), utc_now(), binding["runtime_id"]),
    )
    _dispose(service, actor, work, kind="continue")
    source = service.db.fetchone(
        "SELECT d.* FROM message_deliveries d JOIN messages m ON m.id = d.message_id "
        "WHERE m.attempt_id = ? AND m.kind = 'instruction' ORDER BY m.sequence DESC LIMIT 1",
        (binding["attempt_id"],),
    )
    assert source is not None
    next_binding = _begin_successor_capture(service, binding, str(source["message_id"]))
    fresh = service.observe_worker_output(
        **next_binding, event=_event(turn_id="second-provider-turn")
    )
    _terminal(service, next_binding, turn_id="second-provider-turn")
    assert fresh["digest"] == original["digest"]
    assert fresh["id"] != original["id"]
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    service.read_worker_output(actor, _read_request(work, fresh))
    assert _review(service, actor, work)["reviews"][-1]["verdict"] == "ok"


def test_review_rehashes_retained_output_after_an_earlier_successful_read(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, output))
    digest = output["digest"]
    archive = system["settings"].state_dir / "artifact-archive-v1" / digest[:2] / digest
    archive.write_bytes(b"X" * output["byte_count"])
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    assert _row_count(service, "reviews") == 0
    assert service.get_work(work["id"])["current_attempt"]["completion_claim"] == {}


@pytest.mark.parametrize(
    ("text", "complete", "status", "phase"),
    [
        ("", True, "completed", "final"),
        ("Unfinished output", False, "completed", "final"),
        ("Observed failure", True, "failed", "final"),
        ("Observed interruption", False, "interrupted", "final"),
        ("Intermediate commentary", True, "completed", "commentary"),
    ],
)
def test_empty_partial_failed_or_commentary_only_output_never_becomes_completion(
    system, text: str, complete: bool, status: str, phase: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    if text:
        output = service.observe_worker_output(
            **binding, event=_event(text=text, complete=complete, phase=phase)
        )
        if output["digest"]:
            service.read_worker_output(actor, _read_request(work, output))
    _terminal(service, binding, status=status, complete=complete)
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    expected_kind = "worker_output" if status == "completed" else "failure"
    assert [item["kind"] for item in current["open_boundaries"]] == [expected_kind]
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    assert service.get_work(work["id"])["current_attempt"]["completion_claim"] == {}
    assert _row_count(service, "reviews") == 0


def test_oversized_unicode_output_is_explicitly_partial_not_silently_complete(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(
        **binding, event=_event(text="日" * (ARTIFACT_CONTENT_MAX_BYTES // 3 + 1))
    )
    assert output["capture_state"] == "partial"
    assert output["complete"] is False
    assert output["byte_count"] <= ARTIFACT_CONTENT_MAX_BYTES
    assert output["byte_count"] % 3 == 0
    first = service.read_worker_output(actor, _read_request(work, output))
    assert first["capture_complete"] is False
    assert set(first["content"]) == {"日"}
    _terminal(service, binding)
    with pytest.raises(ConflictError):
        _review(service, actor, work)


@pytest.mark.parametrize("credential_kind", ["control_plane", "generic"])
def test_credential_bearing_output_is_withheld_before_private_archive_write(
    system, monkeypatch, credential_kind: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    credential = (
        "cao.prn_fixture." + "x" * 48
        if credential_kind == "control_plane"
        else "Authorization: Bearer " + "x" * 48
    )
    staged: list[dict[str, Any]] = []
    monkeypatch.setattr(
        service.owner_private_artifact_preservation,
        "stage_bytes",
        lambda **kwargs: staged.append(kwargs),
    )
    output = service.observe_worker_output(
        **binding, event=_event(text=f"Unexpected credential: {credential}")
    )
    assert output["capture_state"] == "withheld"
    assert output["complete"] is False
    assert output["digest"] == ""
    assert staged == []
    _terminal(service, binding)
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    assert credential not in json.dumps(
        [dict(row) for row in service.db.fetchall("SELECT * FROM worker_output_receipts")]
    )


def test_archive_failure_still_delivers_explicit_unavailable_terminal_evidence(
    system, monkeypatch
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)

    def fail_archive(**_: Any) -> str:
        raise OSError("unavailable fixture archive")

    monkeypatch.setattr(service.owner_private_artifact_preservation, "stage_bytes", fail_archive)
    output = service.observe_worker_output(**binding, event=_event())
    assert output["capture_state"] == "unavailable"
    assert output["digest"] == ""
    terminal = _terminal(service, binding)
    assert terminal["notification_message_id"]
    assert terminal["boundary_id"]
    with pytest.raises(ConflictError):
        _review(service, actor, work)


def test_artifact_required_work_cannot_be_satisfied_by_normal_answer_alone(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(
        system, completion_contract=CompletionContract.COMPLETION_REQUIRED
    )
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, output))
    with pytest.raises(ConflictError):
        _review(service, actor, work)
    current = service.get_work(work["id"])
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["artifacts"] == []
    assert current["reviews"] == []


def test_registered_artifact_and_automatic_answer_share_a_frozen_review_manifest(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(
        system, completion_contract=CompletionContract.COMPLETION_REQUIRED
    )
    content = b"A bounded generated artifact."
    digest = hashlib.sha256(content).hexdigest()
    attempt = work["current_attempt"]
    registered = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Register the exact generated artifact without claiming completion.",
            artifacts=[
                ArtifactInput(
                    name="result",
                    uri="data:application/octet-stream;base64,"
                    + base64.b64encode(content).decode("ascii"),
                    media_type="text/plain",
                    digest=digest,
                )
            ],
            idempotency_key="report:artifact-registration",
        ),
    )
    artifact = registered["artifacts"][0]
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, output))
    read_artifact = service.read_artifact_content(
        actor,
        ArtifactContentReadInput(
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            artifact_id=artifact["id"],
            expected_digest=digest,
            expected_media_type="text/plain",
            idempotency_key="read:artifact-before-output-review",
        ),
    )
    assert read_artifact["content"] == content.decode()
    reviewed = _review(service, actor, work)
    assert len(reviewed["artifacts"]) == 1
    claim = reviewed["current_attempt"]["completion_claim"]
    assert claim["source"] == "cao_reviewed_worker_output"
    assert [item["id"] for item in claim["artifacts"]] == [artifact["id"]]
    assert output["digest"] != digest
    _dispose(service, actor, work, kind="accept")
    assert service.get_work(work["id"])["state"] == "waiting_user"


def test_optional_structured_report_keeps_its_boundary_and_claim(system) -> None:
    service = system["service"]
    _, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    attempt = work["current_attempt"]
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The optional structured result is ready for review.",
            trajectory="complete",
            idempotency_key="report:optional",
        ),
    )
    original_claim = reported["current_attempt"]["completion_claim"]
    original_boundary = reported["open_boundaries"][0]
    terminal = _terminal(service, binding)
    current = service.get_work(work["id"])
    assert terminal["boundary_id"] == original_boundary["id"]
    assert current["current_attempt"]["completion_claim"] == original_claim
    assert [item["id"] for item in current["open_boundaries"]] == [original_boundary["id"]]
    assert current["open_boundaries"][0]["kind"] == "completion"


def test_corrected_output_continues_same_attempt_and_releases_next_input_lane(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    # Model the adapter's quiescent post-turn boundary without claiming that
    # the model itself acknowledged or reported the Assignment.
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?", (binding["runtime_id"],)
    )
    service.db.execute(
        "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, updated_at = ? WHERE enrollment_id IN (SELECT id FROM worker_enrollments WHERE runtime_session_id = ?) AND state = 'active'",
        (utc_now(), utc_now(), binding["runtime_id"]),
    )
    _review(service, actor, work, verdict="needs_work")
    _dispose(service, actor, work, kind="correct")
    current = service.get_work(work["id"])
    assert current["state"] == "active"
    assert current["current_attempt"]["id"] == binding["attempt_id"]
    assert current["current_attempt"]["runtime_session_id"] == binding["runtime_id"]
    assert current["current_attempt"]["completion_claim"] == {}
    command = service.db.fetchone(
        "SELECT m.id, d.state, d.runtime_session_id FROM messages m JOIN message_deliveries d "
        "ON d.message_id = m.id WHERE m.attempt_id = ? AND m.kind = 'instruction' ORDER BY m.sequence DESC LIMIT 1",
        (binding["attempt_id"],),
    )
    assert command is not None
    assert command["state"] == "queued"
    assert command["runtime_session_id"] == binding["runtime_id"]
    with pytest.raises(ConflictError):
        service.observe_worker_output(
            **binding, event=replace(_event(), item_id="stale-after-correction")
        )


@pytest.mark.parametrize("kind", ["status_request", "instruction"])
def test_terminal_output_preserves_pending_same_attempt_input_until_its_own_turn(
    system, kind: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    successor_id = _queued_successor(system, actor, work, kind=kind)
    first_terminal = _terminal(service, binding)
    current = service.get_work(work["id"])
    assert first_terminal["notification_message_id"]
    assert first_terminal["boundary_id"] == ""
    assert current["state"] == "active"
    assert current["attention_owner"] == "worker"
    assert current["open_boundaries"] == []
    assert current["current_attempt"]["completion_claim"] == {}
    source = service.db.fetchone(
        "SELECT state, acknowledged_at FROM message_deliveries WHERE message_id = ?",
        (binding["delivery_message_id"],),
    )
    successor = service.db.fetchone(
        "SELECT state, acknowledged_at, handled_at FROM message_deliveries WHERE message_id = ?",
        (successor_id,),
    )
    assert source["state"] == "handled"
    assert source["acknowledged_at"] is None
    assert dict(successor) == {"state": "queued", "acknowledged_at": None, "handled_at": None}
    with pytest.raises(ConflictError):
        _review(service, actor, work)

    next_binding = _begin_successor_capture(service, binding, successor_id)
    service.observe_worker_output(
        **next_binding,
        event=_event(turn_id="successor-provider-turn", text="The successor's own result."),
    )
    final_terminal = _terminal(service, next_binding, turn_id="successor-provider-turn")
    settled = service.get_work(work["id"])
    assert final_terminal["boundary_id"]
    assert settled["state"] == "waiting_supervisor"
    assert settled["current_attempt"]["id"] == binding["attempt_id"]
    assert [item["id"] for item in settled["open_boundaries"]] == [final_terminal["boundary_id"]]
    successor = service.db.fetchone(
        "SELECT state, acknowledged_at FROM message_deliveries WHERE message_id = ?",
        (successor_id,),
    )
    assert dict(successor) == {"state": "handled", "acknowledged_at": None}


@pytest.mark.parametrize("kind", ["status_request", "instruction"])
def test_previously_reviewed_output_cannot_accept_over_unsettled_successor(
    system, kind: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding)
    service.read_worker_output(actor, _read_request(work, output))
    _review(service, actor, work)
    successor_id = _queued_successor(system, actor, work, kind=kind, through_status_api=False)
    with pytest.raises(ConflictError):
        _dispose(service, actor, work, kind="accept")
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["current_attempt"]["state"] == "waiting_supervisor"
    assert current["open_boundaries"]
    successor = service.db.fetchone(
        "SELECT state, acknowledged_at, handled_at FROM message_deliveries WHERE message_id = ?",
        (successor_id,),
    )
    assert dict(successor) == {"state": "queued", "acknowledged_at": None, "handled_at": None}


@pytest.mark.parametrize("status", ["failed", "interrupted", "completed"])
def test_restart_recovers_failure_committed_before_its_output_boundary(system, status: str) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    terminal = _terminal(
        service, binding, settle=False, status=status, complete=status == "completed"
    )
    service.fail_runtime_enrollment(binding["runtime_id"], reason="runtime_dispatch_failed")
    # Crash before recover_terminal_worker_attempt commits. A provider terminal
    # alone must neither claim success nor suppress the durable recovery path.
    unfinished = service.get_work(work["id"])
    assert unfinished["state"] == "active"
    assert unfinished["attention_owner"] == "worker"
    assert unfinished["open_boundaries"] == []
    delivery_before = service.db.fetchone(
        "SELECT state, generation, attempts, acknowledged_at, handled_at FROM message_deliveries WHERE message_id = ?",
        (binding["delivery_message_id"],),
    )
    assert delivery_before["state"] == ("handled" if status == "completed" else "dispatched")
    assert delivery_before["acknowledged_at"] is None

    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    restarted.expire_runtime_leases()
    restarted.reconcile_abandoned_worker_output_captures()
    current = restarted.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["attention_owner"] == "cao"
    assert current["current_attempt"]["completion_claim"] == {}
    assert len(current["attempts"]) == 1
    assert [item["kind"] for item in current["open_boundaries"]] == ["failure"]
    boundary = current["open_boundaries"][0]
    assert boundary["recovery_action"] in {
        "dispose_continue_or_correct",
        "reconcile_continue_same_thread",
        "system_reconciliation",
    }
    retained = next(item for item in current["worker_outputs"] if item["id"] == terminal["id"])
    assert retained["turn_status"] == status
    assert retained["boundary_id"] == boundary["id"]
    assert retained["notification_message_id"]
    notification = restarted.db.fetchone(
        "SELECT d.state, d.recipient_attachment_id, m.payload_json FROM message_deliveries d JOIN messages m ON m.id = d.message_id WHERE m.id = ?",
        (retained["notification_message_id"],),
    )
    assert notification["state"] == "queued"
    assert notification["recipient_attachment_id"] == work["supervisor_attachment_id"]
    assert json.loads(notification["payload_json"])["boundary_id"] == boundary["id"]
    delivery_after = restarted.db.fetchone(
        "SELECT state, generation, attempts, acknowledged_at, handled_at FROM message_deliveries WHERE message_id = ?",
        (binding["delivery_message_id"],),
    )
    assert dict(delivery_after) == dict(delivery_before)
    assert (
        restarted.read_worker_output(actor, _read_request(work, output))["content"] == _event().text
    )
    with pytest.raises(ConflictError):
        _review(restarted, actor, work)
    sizes = {
        table: _row_count(restarted, table)
        for table in ("events", "messages", "boundaries", "worker_output_receipts")
    }
    restarted.expire_runtime_leases()
    restarted.reconcile_abandoned_worker_output_captures()
    assert {table: _row_count(restarted, table) for table in sizes} == sizes
    assert [item["id"] for item in restarted.get_work(work["id"])["open_boundaries"]] == [
        boundary["id"]
    ]


def test_restart_recovers_partial_capture_after_runtime_failure_without_terminal(system) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(
        **binding, event=_event(text="Retained incomplete answer.", complete=False)
    )
    service.fail_runtime_enrollment(binding["runtime_id"], reason="runtime_dispatch_failed")
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    restarted.expire_runtime_leases()
    restarted.reconcile_abandoned_worker_output_captures()
    current = restarted.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["attention_owner"] == "cao"
    assert [item["kind"] for item in current["open_boundaries"]] == ["failure"]
    terminal = current["worker_outputs"][-1]
    assert terminal["kind"] == "turn_end"
    assert terminal["turn_status"] == "interrupted"
    assert terminal["capture_state"] == "unavailable"
    assert terminal["complete"] is False
    assert terminal["boundary_id"] == current["open_boundaries"][0]["id"]
    assert terminal["notification_message_id"]
    assert current["current_attempt"]["completion_claim"] == {}
    assert (
        restarted.read_worker_output(actor, _read_request(work, output))["content"]
        == "Retained incomplete answer."
    )
    sizes = {
        table: _row_count(restarted, table)
        for table in ("events", "messages", "boundaries", "worker_output_receipts")
    }
    restarted.reconcile_abandoned_worker_output_captures()
    assert {table: _row_count(restarted, table) for table in sizes} == sizes


@pytest.mark.parametrize("status", ["failed", "interrupted"])
def test_restart_does_not_resurrect_disposed_failure_for_unsettled_output(
    system, status: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event())
    _terminal(service, binding, settle=False, status=status, complete=False)
    service.fail_runtime_enrollment(binding["runtime_id"], reason="runtime_dispatch_failed")
    service.recover_terminal_worker_attempt(binding["runtime_id"], reason="runtime_dispatch_failed")
    _dispose(service, actor, work, kind="fail")
    assert service.get_work(work["id"])["state"] == "failed"
    before_messages = _row_count(service, "messages")
    before_boundaries = _row_count(service, "boundaries")
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    restarted.expire_runtime_leases()
    restarted.reconcile_abandoned_worker_output_captures()
    current = restarted.get_work(work["id"])
    assert current["state"] == "failed"
    assert current["open_boundaries"] == []
    assert current["current_attempt"]["completion_claim"] == {}
    assert _row_count(restarted, "messages") == before_messages
    assert _row_count(restarted, "boundaries") == before_boundaries
    sizes = {
        table: _row_count(restarted, table)
        for table in ("events", "messages", "boundaries", "worker_output_receipts")
    }
    restarted.reconcile_abandoned_worker_output_captures()
    assert {table: _row_count(restarted, table) for table in sizes} == sizes
