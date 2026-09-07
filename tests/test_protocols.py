from __future__ import annotations

import asyncio
import json
import re

import pytest

from cao_control_plane.a2a import (
    ROLE_USER,
    TASK_STATE_INPUT_REQUIRED,
    TASK_STATE_SUBMITTED,
    A2AServer,
)
from cao_control_plane.errors import AuthorizationError
from cao_control_plane.goal_packets import build_task_packet, task_packet_digest
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    MCPServer,
    conversation_proxy_tools,
)
from cao_control_plane.models import ReportInput, WorkAssignment


def _meta() -> dict:
    return {
        PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
        CLIENT_CAPABILITIES_META_KEY: {},
        CLIENT_INFO_META_KEY: {"name": "pytest", "version": "1"},
    }


def _modern(method: str, params: dict | None = None, request_id: int = 1) -> dict:
    values = dict(params or {})
    values["_meta"] = _meta()
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": values}


def test_mcp_role_specific_tools(system):
    server = MCPServer(system["service"])
    worker_tools = {item["name"] for item in server.tools_for(system["worker"])}
    cao_tools = {item["name"] for item in server.tools_for(system["cao"])}
    assert "cao_report" in worker_tools
    assert "cao_record_boundary" not in worker_tools
    assert "cao_observe_runtime" not in worker_tools
    assert "cao_assign" not in worker_tools
    assert "cao_assign" in cao_tools
    assert "cao_report" not in cao_tools
    assert "cao_receive_intent" not in cao_tools
    assert "cao_classify_intent" not in cao_tools
    assert "cao_acquire_reasoner_turn" in cao_tools
    assert "cao_dispose_boundary" in cao_tools
    assert "cao_get_work" in cao_tools
    assert "cao_mark_handled" in cao_tools
    assert "cao_record_boundary" not in cao_tools
    assert "cao_observe_runtime" not in cao_tools
    assert "cao_receive_intent" not in {item["name"] for item in server.tools_for(system["user"])}
    report = next(
        item for item in server.tools_for(system["worker"]) if item["name"] == "cao_report"
    )
    assert {"expected_goal_packet_digest", "expected_task_packet_digest"} <= set(
        report["inputSchema"]["required"]
    )
    worker_definitions = {item["name"]: item for item in server.tools_for(system["worker"])}
    assert worker_definitions["cao_get_context"]["annotations"] == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    assert report["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    }
    assert all(
        item["annotations"]["openWorldHint"] is False
        and item["annotations"]["destructiveHint"] is False
        for item in worker_definitions.values()
    )


def test_conversation_catalog_never_relies_on_missing_effect_annotations():
    definitions = {item["name"]: item for item in conversation_proxy_tools()}
    annotation_keys = {
        "readOnlyHint",
        "destructiveHint",
        "idempotentHint",
        "openWorldHint",
    }
    assert all(set(item.get("annotations", {})) == annotation_keys for item in definitions.values())
    assert definitions["cao_get_work"]["annotations"] == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    assert definitions["cao_acquire_reasoner_turn"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    assert definitions["cao_record_requester_decision"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    for name in ("cao_reply", "cao_review", "cao_revise_goal"):
        assert definitions[name]["annotations"] == {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        }
    assert definitions["cao_cancel"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False,
    }


def test_mcp_assignment_and_worker_context(system):
    server = MCPServer(system["service"])
    tools = server.handle_modern(system["cao"], _modern("tools/list"))
    assignment_tool = next(
        item for item in tools["result"]["tools"] if item["name"] == "cao_assign"
    )
    dependencies_schema = assignment_tool["inputSchema"]["properties"]["dependencies"]
    assert dependencies_schema["type"] == "array"
    assert dependencies_schema["items"]["enum"] == ["docker_api_ping"]
    assert "dependencies" not in assignment_tool["inputSchema"]["required"]
    assigned = server.handle_modern(
        system["cao"],
        _modern(
            "tools/call",
            {
                "name": "cao_assign",
                "arguments": {
                    "worker_id": system["worker"]["id"],
                    "title": "MCP",
                    "objective": "Use MCP",
                    "acceptance": ["Context available"],
                    "dependencies": ["docker_api_ping"],
                },
            },
        ),
    )
    work = assigned["result"]["structuredContent"]
    assert work["goal_history"][0]["packet"]["dependencies"] == ["docker_api_ping"]
    attempt = work["current_attempt"]
    expected_task_packet = build_task_packet(
        goal_packet_digest_value=attempt["goal_packet_digest"],
        work_item_id=work["id"],
        goal_version=work["goal_version"],
        attempt_id=attempt["id"],
        attempt_number=attempt["attempt_number"],
        worker_id=system["worker"]["id"],
        runtime_session_id=system["runtime"]["id"],
        dependencies=["docker_api_ping"],
    )
    assert expected_task_packet["dependencies"] == ["docker_api_ping"]
    assert task_packet_digest(expected_task_packet) == attempt["task_packet_digest"]
    message = system["service"].db.fetchone(
        "SELECT payload_json FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (attempt["id"],),
    )
    assert message is not None
    assert json.loads(message["payload_json"])["dependencies"] == ["docker_api_ping"]
    context = server.handle_modern(
        system["worker"],
        _modern(
            "tools/call",
            {
                "name": "cao_get_context",
                "arguments": {"attempt_id": work["current_attempt"]["id"]},
            },
            2,
        ),
    )
    assert context["result"]["structuredContent"]["work"]["id"] == work["id"]


def test_mcp_assignment_dependency_omission_keeps_legacy_packets(system):
    server = MCPServer(system["service"])
    response = server.handle_modern(
        system["cao"],
        _modern(
            "tools/call",
            {
                "name": "cao_assign",
                "arguments": {
                    "worker_id": system["worker"]["id"],
                    "title": "MCP compatibility",
                    "objective": "Keep the pre-dependency packet representation.",
                    "acceptance": ["Dependency key remains absent"],
                    "idempotency_key": "mcp-legacy-dependency-omission",
                },
            },
        ),
    )
    work = response["result"]["structuredContent"]
    attempt = work["current_attempt"]
    assert "dependencies" not in work["goal_history"][0]["packet"]
    expected_task_packet = build_task_packet(
        goal_packet_digest_value=attempt["goal_packet_digest"],
        work_item_id=work["id"],
        goal_version=work["goal_version"],
        attempt_id=attempt["id"],
        attempt_number=attempt["attempt_number"],
        worker_id=system["worker"]["id"],
        runtime_session_id=system["runtime"]["id"],
    )
    assert "dependencies" not in expected_task_packet
    assert task_packet_digest(expected_task_packet) == attempt["task_packet_digest"]
    message = system["service"].db.fetchone(
        "SELECT payload_json FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (attempt["id"],),
    )
    assert message is not None
    assert "dependencies" not in json.loads(message["payload_json"])
    receipt = system["service"].db.fetchone(
        "SELECT payload_json FROM source_receipts WHERE source_id = ?",
        ("assign_work:mcp-legacy-dependency-omission",),
    )
    assert receipt is not None
    assert "dependencies" not in json.loads(receipt["payload_json"])["assignment"]


@pytest.mark.parametrize("actor_key", ["worker", "cao", "user"])
def test_mcp_discovery_instructions_name_only_role_available_tools(system, actor_key: str) -> None:
    server = MCPServer(system["service"])
    actor = system[actor_key]
    instructions = server.discover(actor)["instructions"]
    available = {tool["name"] for tool in server.tools_for(actor)}
    named_tools = set(re.findall(r"\bcao_[a-z0-9_]+\b", instructions))

    assert named_tools <= available
    if actor_key == "worker":
        assert {"cao_get_context", "cao_report"} <= named_tools
    else:
        assert "cao_get_context" not in instructions


def test_a2a_task_id_is_stable_across_attempts(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="A2A",
            objective="Stable ID",
            acceptance=["Stable"],
        ),
    )
    a2a = A2AServer(service)
    first = a2a.get_task(work["a2a"]["task_id"], actor=system["cao"])
    service.create_attempt(system["cao"], work["id"], reason="retry")
    second = a2a.get_task(work["a2a"]["task_id"], actor=system["cao"])
    assert first["id"] == second["id"]
    assert second["metadata"]["attemptNumber"] == 2


def _send_params(system, message_id: str = "client-1") -> dict:
    return {
        "message": {
            "messageId": message_id,
            "role": ROLE_USER,
            "parts": [{"text": "Build the feature"}],
            "metadata": {
                "workerId": system["worker"]["id"],
                "acceptance": ["Feature works"],
            },
        },
        "configuration": {"returnImmediately": True},
    }


def test_a2a_send_is_idempotent_and_uses_1_0_shapes(system):
    a2a = A2AServer(system["service"])
    task = a2a.send_message(system["cao"], _send_params(system))
    assert task["status"]["state"] == TASK_STATE_SUBMITTED
    assert "history" not in task
    again = a2a.send_message(system["cao"], _send_params(system))
    assert again["id"] == task["id"]

    get_response = a2a.handle_jsonrpc(
        system["cao"],
        {"jsonrpc": "2.0", "id": 1, "method": "GetTask", "params": {"id": task["id"]}},
    )
    assert get_response["result"]["id"] == task["id"]
    listed = a2a.list_tasks(system["cao"], {"pageSize": 1})
    assert listed["tasks"][0]["id"] == task["id"]
    assert "artifacts" not in listed["tasks"][0]


def test_a2a_blocking_boundary_stops_on_worker_question(system):
    a2a = A2AServer(system["service"])
    task = a2a.send_message(system["cao"], _send_params(system, "blocking-1"))
    work = system["service"].get_work(task["metadata"]["workItemId"])

    async def scenario() -> dict:
        waiter = asyncio.create_task(
            a2a.wait_for_settled(task["id"], actor=system["cao"], timeout_seconds=1.0)
        )
        await asyncio.sleep(0.05)
        system["service"].report(
            system["worker"],
            work["current_attempt"]["id"],
            ReportInput(
                kind="question",
                expected_goal_version=1,
                expected_generation=work["generation"],
                expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
                expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
                summary="Need a decision",
            ),
        )
        return await waiter

    settled = asyncio.run(scenario())
    assert settled["status"]["state"] == TASK_STATE_INPUT_REQUIRED


def test_a2a_task_visibility_and_status_filter(system):
    a2a = A2AServer(system["service"])
    task = a2a.send_message(system["cao"], _send_params(system, "visible-1"))
    with pytest.raises(AuthorizationError):
        a2a.get_task(task["id"], actor=system["user"])
    submitted = a2a.list_tasks(system["cao"], {"status": TASK_STATE_SUBMITTED, "pageSize": 50})
    assert [item["id"] for item in submitted["tasks"]] == [task["id"]]
    working = a2a.list_tasks(system["cao"], {"status": "TASK_STATE_WORKING", "pageSize": 50})
    assert working["tasks"] == []


def test_a2a_push_config_is_canonical_and_delete_is_idempotent(system):
    a2a = A2AServer(system["service"])
    task = a2a.send_message(system["cao"], _send_params(system, "push-1"))
    created = a2a.set_push_config(
        system["cao"],
        {
            "taskId": task["id"],
            "taskPushNotificationConfig": {
                "url": "http://127.0.0.1:9876/callback",
                "authentication": {"scheme": "Bearer", "credentials": "secret"},
            },
        },
    )
    assert created["url"].startswith("http://127.0.0.1")
    assert "credentials" not in created["authentication"]
    params = {"taskId": task["id"], "configId": created["id"]}
    a2a.delete_push_config(system["cao"], params)
    assert a2a.delete_push_config(system["cao"], params) == {"id": created["id"]}


def test_agent_card_declares_only_1_0_interfaces(system):
    card = A2AServer(system["service"]).agent_card()
    assert card["capabilities"]["streaming"] is True
    assert {item["protocolBinding"] for item in card["supportedInterfaces"]} == {
        "JSONRPC",
        "HTTP+JSON",
    }
    assert all(item["protocolVersion"] == "1.0" for item in card["supportedInterfaces"])


def test_a2a_does_not_fabricate_acceptance_conditions(system):
    a2a = A2AServer(system["service"])
    params = _send_params(system, "exploring-1")
    params["message"]["metadata"].pop("acceptance")
    task = a2a.send_message(system["cao"], params)
    work = system["service"].get_work(task["metadata"]["workItemId"], system["cao"])
    assert work["maturity"] == "exploring"
    assert work["acceptance"] == []


def test_user_mcp_surface_cannot_direct_workers(system):
    server = MCPServer(system["service"])
    tools = {item["name"] for item in server.tools_for(system["user"])}
    # Requesters have no direct acceptance tool or Worker-control capability.
    assert {"cao_query", "cao_cancel"} <= tools
    assert "cao_user_acceptance" not in tools
    assert {"cao_record_requester_decision", "cao_close_work"}.isdisjoint(tools)
    assert {"cao_assign", "cao_reply", "cao_revise_goal"}.isdisjoint(tools)

    # The replacement surface belongs only to the CAO that owns an attached
    # conversation; it records the requester decision rather than accepting a
    # raw requester MCP ingress.
    attached_cao = {
        **system["cao"],
        "_cao_conversation_credential_id": "test-conversation-credential",
        "_cao_attachment_id": "test-attachment",
        "_cao_attachment_generation": 1,
    }
    attached_tools = {item["name"] for item in server.tools_for(attached_cao)}
    assert {
        "cao_record_requester_decision",
        "cao_close_conversation",
        "cao_finish_worker_thread",
    } <= attached_tools
    assert {
        "cao_close_work",
        "cao_prepare_work_close",
        "cao_execute_prepared_cleanup",
        "cao_stop_work_runtime",
    }.isdisjoint(attached_tools)
    assert "cao_user_acceptance" not in attached_tools
    resources = {item["uri"] for item in server.resources_for(system["user"])}
    assert "cao://work" in resources
    assert "cao://runtimes" not in resources
    assert "cao://events" not in resources


def test_a2a_projection_exposes_non_worker_attention_as_input_required(system):
    from cao_control_plane.a2a import TASK_STATE_INPUT_REQUIRED, A2AServer
    from cao_control_plane.models import (
        BoundaryDispositionInput,
        ReportInput,
        ReviewInput,
        WorkAssignment,
    )

    service = system["service"]
    a2a = A2AServer(service)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Review boundary",
            objective="Expose supervision boundaries through A2A",
            acceptance=["CAO and user input are visible"],
        ),
    )
    task_id = service.task_for_work(work["id"])["task_id"]
    attempt_id = work["current_attempt"]["id"]

    claimed = service.report(
        system["worker"],
        attempt_id,
        ReportInput(
            kind="completion_claim",
            expected_goal_version=1,
            expected_generation=work["generation"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            summary="Ready for CAO review",
        ),
    )
    assert (
        a2a.get_task(task_id, actor=system["cao"])["status"]["state"] == TASK_STATE_INPUT_REQUIRED
    )

    boundary = claimed["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        expected_generation=claimed["generation"],
        idempotency_key="protocol-completion-review",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=claimed["generation"],
            kind="accept",
            reason="The completion claim is ready for review",
        ),
    )

    service.review(
        system["cao"],
        ReviewInput(attempt_id=attempt_id, verdict="ok", summary="Verified"),
    )
    assert (
        a2a.get_task(task_id, actor=system["cao"])["status"]["state"] == TASK_STATE_INPUT_REQUIRED
    )
