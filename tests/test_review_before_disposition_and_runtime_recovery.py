from __future__ import annotations

import json
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

import cao_control_plane.service as service_module
from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    CompletionContract,
    ReportInput,
    RuntimeHeartbeat,
    WorkAssignment,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane


def _attached_cao(
    system: dict[str, Any], *, thread: str, service: ControlPlane | None = None
) -> dict[str, Any]:
    service = service or system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=thread,
            project_digest="a" * 64,
        ),
    )
    return service.authenticate(str(attachment["context_token"]))


def _completion_boundary(
    system: dict[str, Any], *, key: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    service = system["service"]
    cao = _attached_cao(system, thread=f"review-before-disposition-{key}")
    work = service.assign_work(
        cao,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            title=f"Review before disposition {key}",
            objective="Record the independent CAO review before disposing completion.",
            acceptance=["The review and disposition remain separate durable facts."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key=f"assign:{key}",
        ),
    )
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
            summary="The declared result is ready for independent CAO review.",
            trajectory="complete",
            evidence=[{"check": "completion", "result": "pass"}],
            idempotency_key=f"report:{key}",
        ),
    )
    boundary = next(item for item in reported["open_boundaries"] if item["kind"] == "completion")
    return cao, reported, boundary


def _review_open_boundary(
    system: dict[str, Any],
    *,
    cao: dict[str, Any],
    work: dict[str, Any],
    boundary: dict[str, Any],
    verdict: str,
    key: str,
) -> tuple[MCPServer, dict[str, Any], dict[str, Any]]:
    service = system["service"]
    mcp = MCPServer(service)
    reviewed = mcp.call_tool(
        cao,
        "cao_review",
        {
            "attempt_id": work["current_attempt"]["id"],
            "verdict": verdict,
            "summary": f"Independent review verdict: {verdict}.",
            "evidence": [{"boundary_id": boundary["id"], "result": verdict}],
            "idempotency_key": f"review:{key}",
        },
    )
    review = reviewed["reviews"][-1]
    assert reviewed["state"] == "waiting_supervisor"
    assert reviewed["current_attempt"]["state"] == "waiting_supervisor"
    assert {item["id"] for item in reviewed["open_boundaries"]} == {boundary["id"]}
    assert review["verdict"] == verdict
    event = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'work.reviewed' "
        "AND json_extract(data_json, '$.review_id') = ?",
        (review["id"],),
    )
    assert event is not None
    assert json.loads(str(event["data_json"])) == {
        "awaiting_disposition": True,
        "boundary_id": boundary["id"],
        "review_id": review["id"],
        "verdict": verdict,
    }
    turn = mcp.call_tool(
        cao,
        "cao_acquire_reasoner_turn",
        {
            "work_item_id": work["id"],
            "boundary_id": boundary["id"],
            "expected_generation": work["generation"],
            "idempotency_key": f"turn:{key}",
        },
    )
    return mcp, review, turn


@pytest.mark.parametrize(
    ("verdict", "disposition", "expected_work_state", "expected_attempt_state"),
    [
        ("ok", "accept", "waiting_user", "completed"),
        ("needs_work", "correct", "active", "working"),
    ],
)
def test_open_completion_boundary_review_precedes_matching_disposition(
    system: dict[str, Any],
    verdict: str,
    disposition: str,
    expected_work_state: str,
    expected_attempt_state: str,
) -> None:
    key = f"matching:{verdict}"
    cao, work, boundary = _completion_boundary(system, key=key)
    mcp, review, turn = _review_open_boundary(
        system,
        cao=cao,
        work=work,
        boundary=boundary,
        verdict=verdict,
        key=key,
    )
    instruction = "Correct the evidence and submit a new completion claim."
    mcp.call_tool(
        cao,
        "cao_dispose_boundary",
        {
            "work_item_id": work["id"],
            "boundary_id": boundary["id"],
            "turn_id": turn["id"],
            "lease_token": turn["lease_token"],
            "expected_generation": work["generation"],
            "kind": disposition,
            "reason": f"Apply the durable {verdict} review.",
            "instruction": instruction if disposition == "correct" else "",
        },
    )

    current = system["service"].get_work(work["id"])
    assert current["state"] == expected_work_state
    assert current["current_attempt"]["state"] == expected_attempt_state
    assert current["open_boundaries"] == []
    assert current["reviews"][-1]["id"] == review["id"]
    assert current["reviews"][-1]["verdict"] == verdict
    if verdict == "ok":
        assert current["current_attempt"]["evidence_confidence"] == "verified"
        assert current["attention_owner"] == "user"
    else:
        instruction_message = system["service"].db.fetchone(
            "SELECT message.payload_json, delivery.runtime_session_id "
            "FROM messages AS message JOIN message_deliveries AS delivery "
            "ON delivery.message_id = message.id "
            "WHERE message.attempt_id = ? AND message.kind = 'instruction' "
            "ORDER BY message.sequence DESC LIMIT 1",
            (work["current_attempt"]["id"],),
        )
        assert instruction_message is not None
        payload = json.loads(str(instruction_message["payload_json"]))
        assert payload["action"] == "correct"
        assert payload["instruction"] == instruction
        assert instruction_message["runtime_session_id"] == system["runtime"]["id"]


@pytest.mark.parametrize(
    ("verdict", "disposition"),
    [
        ("ok", "correct"),
        ("needs_work", "accept"),
        ("needs_work", "continue"),
    ],
)
def test_open_completion_boundary_rejects_review_disposition_mismatch(
    system: dict[str, Any], verdict: str, disposition: str
) -> None:
    key = f"mismatch:{verdict}:{disposition}"
    cao, work, boundary = _completion_boundary(system, key=key)
    mcp, review, turn = _review_open_boundary(
        system,
        cao=cao,
        work=work,
        boundary=boundary,
        verdict=verdict,
        key=key,
    )

    with pytest.raises(ConflictError):
        mcp.call_tool(
            cao,
            "cao_dispose_boundary",
            {
                "work_item_id": work["id"],
                "boundary_id": boundary["id"],
                "turn_id": turn["id"],
                "lease_token": turn["lease_token"],
                "expected_generation": work["generation"],
                "kind": disposition,
                "reason": "Reject a disposition that contradicts the durable review.",
                "instruction": "This instruction must never be delivered.",
            },
        )

    current = system["service"].get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert {item["id"] for item in current["open_boundaries"]} == {boundary["id"]}
    assert current["reviews"][-1]["id"] == review["id"]
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )["count"]
        == 0
    )
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM messages WHERE attempt_id = ? AND kind = 'instruction'",
            (work["current_attempt"]["id"],),
        )["count"]
        == 0
    )


def test_repeated_completion_reviews_bind_to_exact_boundary_at_same_timestamp(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    cao, first_work, first_boundary = _completion_boundary(system, key="same-timestamp-cycles")
    fixed_time = first_boundary["created_at"]
    monkeypatch.setattr(service_module, "utc_now", lambda: fixed_time)
    mcp, first_review, first_turn = _review_open_boundary(
        system,
        cao=cao,
        work=first_work,
        boundary=first_boundary,
        verdict="needs_work",
        key="same-timestamp-cycle-1",
    )
    mcp.call_tool(
        cao,
        "cao_dispose_boundary",
        {
            "work_item_id": first_work["id"],
            "boundary_id": first_boundary["id"],
            "turn_id": first_turn["id"],
            "lease_token": first_turn["lease_token"],
            "expected_generation": first_work["generation"],
            "kind": "correct",
            "reason": "Apply the first exact-boundary review.",
            "instruction": "Correct the result and submit it again.",
        },
    )

    current = system["service"].get_work(first_work["id"])
    attempt = current["current_attempt"]
    second_work = system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=current["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=current["generation"],
            summary="The corrected result is ready for another exact review.",
            trajectory="complete",
            idempotency_key="report:same-timestamp-cycle-2",
        ),
    )
    second_boundary = second_work["open_boundaries"][0]
    _, second_review, _ = _review_open_boundary(
        system,
        cao=cao,
        work=second_work,
        boundary=second_boundary,
        verdict="needs_work",
        key="same-timestamp-cycle-2",
    )

    assert first_review["created_at"] == second_review["created_at"] == fixed_time
    events = system["service"].db.fetchall(
        "SELECT data_json FROM events WHERE event_type = 'work.reviewed' "
        "AND aggregate_id = ? ORDER BY sequence",
        (first_work["id"],),
    )
    bindings: dict[str, str] = {}
    for event in events:
        data = json.loads(str(event["data_json"]))
        bindings[str(data["review_id"])] = str(data["boundary_id"])
    assert bindings[first_review["id"]] == first_boundary["id"]
    assert bindings[second_review["id"]] == second_boundary["id"]


def _activate_runtime(service: ControlPlane, runtime_id: str) -> dict[str, Any]:
    runtime = service.get_runtime(runtime_id)
    launch = service.issue_runtime_launch_ticket(runtime["id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    actor = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=actor["_enrollment_generation"],
            sequence=1,
        ),
    )
    service.db.execute("UPDATE runtime_sessions SET state = 'ready' WHERE id = ?", (runtime["id"],))
    return service.get_runtime(runtime["id"])
