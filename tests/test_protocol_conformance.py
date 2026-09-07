from __future__ import annotations

import asyncio
import base64
import io
import json
from dataclasses import replace

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from fastapi.testclient import TestClient
from pydantic import ValidationError as PydanticValidationError
from test_worker_thread_lifecycle_core import _attached, _seed_managed_thread

from cao_control_plane.a2a import (
    PUSH_NOT_SUPPORTED,
    ROLE_USER,
    VERSION_NOT_SUPPORTED,
    A2AServer,
)
from cao_control_plane.api import create_app
from cao_control_plane.errors import ConflictError, ValidationError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    MCPServer,
    _pending_bridge_response,
    modern_http_headers,
    serve_stdio_proxy,
)
from cao_control_plane.models import PrincipalCreate, PrincipalRole, ReportInput, WorkAssignment


def _a2a_params(system, message: dict[str, object]) -> dict[str, object]:
    return {
        "message": {
            "role": ROLE_USER,
            "metadata": {
                "workerId": system["worker"]["id"],
                "acceptance": ["Verified"],
            },
            **message,
        },
        "configuration": {"returnImmediately": True},
    }


def _modern_request(
    method: str, params: dict[str, object], *, request_id: int | None = 1
) -> dict[str, object]:
    payload = {
        **params,
        "_meta": {
            PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
            CLIENT_CAPABILITIES_META_KEY: {},
            CLIENT_INFO_META_KEY: {"name": "conformance", "version": "1"},
        },
    }
    request: dict[str, object] = {"jsonrpc": "2.0", "method": method, "params": payload}
    if request_id is not None:
        request["id"] = request_id
    return request


def _mcp_headers(token: str, method: str, name: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "MCP-Protocol-Version": MCP_LATEST_VERSION,
        "Mcp-Method": method,
        "Mcp-Name": name,
    }


def _sse_messages(content: bytes) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    data: list[str] = []
    for line in content.decode("utf-8").splitlines():
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif not line and data:
            messages.append(json.loads("\n".join(data)))
            data = []
    if data:
        messages.append(json.loads("\n".join(data)))
    return messages


def test_a2a_requires_1_0_and_uses_google_rpc_status_errors(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/a2a/http/message:send",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/a2a+json",
            },
            json={},
        )
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/a2a+json")
    detail = response.json()["details"][0]
    assert detail["@type"] == "type.googleapis.com/google.rpc.ErrorInfo"
    assert detail["reason"] == "VERSION_NOT_SUPPORTED"
    assert detail["metadata"]["requestedVersion"] == "0.3"


def test_a2a_http_errors_redact_credential_shaped_headers(settings):
    app = create_app(settings)
    secret = app.state.bootstrap["tokens"]["cao"]["token"]
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/a2a/http/message:send",
            headers={
                "Authorization": f"Bearer {secret}",
                "Content-Type": "application/a2a+json",
                "A2A-Version": secret,
            },
            json={},
        )
    assert response.status_code == 400
    assert secret not in response.text
    assert "[control-plane-credential-redacted]" in response.text


def test_a2a_jsonrpc_pre_dispatch_uses_standard_version_error(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/a2a",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"jsonrpc": "2.0", "id": 7, "method": "ListTasks", "params": {}},
        )
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == VERSION_NOT_SUPPORTED
    assert error["data"][0]["reason"] == "VERSION_NOT_SUPPORTED"


def test_a2a_message_validation_covers_message_id_parts_media_and_context(system):
    a2a = A2AServer(system["service"])
    invalid_messages = [
        {"parts": [{"text": "missing ID"}]},
        {"messageId": "oneof", "parts": [{"text": "x", "raw": "eA=="}]},
        {"messageId": "raw", "parts": [{"raw": "not base64!"}]},
        {"messageId": "media", "parts": [{"text": "x", "mediaType": 1}]},
    ]
    for message in invalid_messages:
        with pytest.raises(ValidationError):
            a2a.send_message(system["cao"], _a2a_params(system, message))

    created = a2a.send_message(
        system["cao"],
        _a2a_params(
            system,
            {
                "messageId": "valid-raw",
                "contextId": "context-a",
                "parts": [{"raw": base64.b64encode(b"data").decode("ascii")}],
            },
        ),
    )
    with pytest.raises(ValidationError, match="contextId"):
        a2a.send_message(
            system["cao"],
            _a2a_params(
                system,
                {
                    "messageId": "wrong-context",
                    "taskId": created["id"],
                    "contextId": "context-b",
                    "parts": [{"text": "continue"}],
                },
            ),
        )


def test_a2a_managed_assignment_uses_the_exact_logical_thread_pair(system):
    actor = _attached(system, suffix="a2a-exact")
    ids = _seed_managed_thread(system, actor, ordinal=1950)
    task = A2AServer(system["service"]).send_message(
        actor,
        {
            "message": {
                "messageId": "managed-exact-pair",
                "role": ROLE_USER,
                "parts": [{"text": "Run one exact managed task."}],
                "metadata": {
                    "workerId": ids["principal_id"],
                    "runtimeSessionId": ids["runtime_id"],
                    "managedWorkerThreadId": ids["thread_id"],
                    "managedWorkerThreadGeneration": 1,
                    "acceptance": ["The generation-1 Assignment stays queued."],
                },
            },
            "configuration": {"returnImmediately": True},
        },
    )
    work = system["service"].get_work(str(task["metadata"]["workItemId"]))
    exact = system["service"].db.fetchone(
        """
        SELECT work.managed_worker_thread_id,
               work.managed_worker_thread_generation,
               attempt.worker_id, attempt.runtime_session_id,
               delivery.state AS delivery_state,
               delivery.runtime_session_id AS delivery_runtime_session_id,
               epoch.connection_generation
        FROM work_items AS work
        JOIN attempts AS attempt ON attempt.id = ? AND attempt.work_item_id = work.id
        JOIN messages AS message
          ON message.attempt_id = attempt.id AND message.kind = 'assignment'
        JOIN message_deliveries AS delivery
          ON delivery.message_id = message.id
         AND delivery.recipient_id = attempt.worker_id
        JOIN managed_worker_thread_epochs AS epoch
          ON epoch.thread_id = work.managed_worker_thread_id
         AND epoch.generation = work.managed_worker_thread_generation
         AND epoch.runtime_session_id = attempt.runtime_session_id
         AND epoch.retired_at IS NULL
        WHERE work.id = ?
        """,
        (work["current_attempt"]["id"], work["id"]),
    )
    assert exact is not None
    assert tuple(exact) == (
        ids["thread_id"],
        1,
        ids["principal_id"],
        ids["runtime_id"],
        "queued",
        ids["runtime_id"],
        1,
    )


@pytest.mark.parametrize(
    "binding",
    [
        {"managedWorkerThreadId": "mwt_partial"},
        {"managedWorkerThreadGeneration": 1},
        {
            "managedWorkerThreadId": "mwt_boolean",
            "managedWorkerThreadGeneration": True,
        },
        {
            "managedWorkerThreadId": "current",
            "managedWorkerThreadGeneration": 2,
        },
    ],
    ids=("missing-generation", "missing-thread", "boolean-generation", "stale-generation"),
)
def test_a2a_invalid_managed_thread_pair_has_zero_mutation(system, binding):
    actor = _attached(system, suffix="a2a-invalid")
    ids = _seed_managed_thread(system, actor, ordinal=1951)
    metadata = {
        "workerId": ids["principal_id"],
        "runtimeSessionId": ids["runtime_id"],
        "acceptance": ["Invalid binding is rejected before mutation."],
        **binding,
    }
    if metadata.get("managedWorkerThreadId") == "current":
        metadata["managedWorkerThreadId"] = ids["thread_id"]

    tables = (
        "runtime_sessions",
        "worker_enrollments",
        "managed_worker_specs",
        "managed_worker_threads",
        "managed_worker_thread_epochs",
        "source_receipts",
        "submitted_intents",
        "intent_dispositions",
        "directives",
        "work_items",
        "attempts",
        "messages",
        "message_deliveries",
        "events",
        "idempotency_results",
    )

    def snapshot() -> dict[str, tuple[tuple[object, ...], ...]]:
        with system["service"].db.connect() as connection:
            return {
                table: tuple(
                    tuple(row)
                    for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
                )
                for table in tables
            }

    before = snapshot()
    with pytest.raises((PydanticValidationError, ConflictError)):
        A2AServer(system["service"]).send_message(
            actor,
            {
                "message": {
                    "messageId": "managed-invalid-pair",
                    "role": ROLE_USER,
                    "parts": [{"text": "Reject the invalid managed binding."}],
                    "metadata": metadata,
                },
                "configuration": {"returnImmediately": True},
            },
        )
    assert snapshot() == before


def test_a2a_unmanaged_assignment_remains_supported_without_thread_metadata(system):
    task = A2AServer(system["service"]).send_message(
        system["cao"],
        _a2a_params(
            system,
            {
                "messageId": "unmanaged-without-thread-pair",
                "parts": [{"text": "Keep the legacy unmanaged A2A path working."}],
            },
        ),
    )
    work = system["service"].get_work(str(task["metadata"]["workItemId"]))
    assert work["managed_worker_thread_id"] is None
    assert work["managed_worker_thread_generation"] is None
    delivery = system["service"].db.fetchone(
        """
        SELECT delivery.state, delivery.runtime_session_id
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (work["current_attempt"]["id"],),
    )
    assert delivery is not None
    assert tuple(delivery) == ("queued", system["runtime"]["id"])


def test_a2a_stream_closes_at_input_required(system):
    a2a = A2AServer(system["service"])
    task = a2a.send_message(
        system["cao"],
        _a2a_params(
            system,
            {"messageId": "stream-close", "parts": [{"text": "work"}]},
        ),
    )
    work = system["service"].get_work(task["metadata"]["workItemId"])
    system["service"].report(
        system["worker"],
        work["current_attempt"]["id"],
        ReportInput(
            kind="question",
            expected_goal_version=1,
            expected_generation=work["generation"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            summary="Need input",
        ),
    )

    async def collect() -> list[dict[str, object]]:
        return [item async for item in a2a.task_event_stream(task["id"], actor=system["cao"])]

    items = asyncio.run(collect())
    assert len(items) == 1
    assert items[0]["task"]["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"


def test_push_disabled_uses_standard_a2a_error(settings):
    disabled = replace(settings, enable_a2a_push=False)
    app = create_app(disabled)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "CreateTaskPushNotificationConfig",
        "params": {"taskId": "unknown", "taskPushNotificationConfig": {}},
    }
    with TestClient(app, base_url="http://localhost") as client:
        rpc = client.post(
            "/a2a",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "A2A-Version": "1.0",
            },
            json=request,
        )
        rest = client.post(
            "/a2a/http/tasks/unknown/pushNotificationConfigs",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/a2a+json",
                "A2A-Version": "1.0",
            },
            json={},
        )
    assert rpc.json()["error"]["code"] == PUSH_NOT_SUPPORTED
    assert rpc.json()["error"]["data"][0]["reason"] == "PUSH_NOTIFICATION_NOT_SUPPORTED"
    assert rest.headers["content-type"].startswith("application/a2a+json")
    assert rest.json()["details"][0]["reason"] == "PUSH_NOTIFICATION_NOT_SUPPORTED"


def test_mcp_notifications_do_not_execute_tools_and_return_202(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "tools/call",
        {
            "name": "cao_create_principal",
            "arguments": {"name": "notification-worker", "role": "worker", "metadata": {}},
        },
        request_id=None,
    )
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "tools/call", "cao_create_principal"),
            json=request,
        )
        discover = client.post(
            "/mcp",
            headers=_mcp_headers(token, "server/discover", ""),
            json=_modern_request("server/discover", {}, request_id=None),
        )
    assert response.status_code == 202
    assert response.content == b""
    assert discover.status_code == 202
    assert (
        app.state.service.db.fetchone(
            "SELECT id FROM principals WHERE name = ?", ("notification-worker",)
        )
        is None
    )


def test_unknown_modern_version_is_rejected_before_legacy_session_routing(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request("server/discover", {}, request_id=71)
    request["params"]["_meta"][PROTOCOL_VERSION_META_KEY] = "2099-01-01"
    headers = _mcp_headers(token, "server/discover", "")
    headers["MCP-Protocol-Version"] = "2099-01-01"

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post("/mcp", headers=headers, json=request)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32022
    assert response.json()["error"]["data"]["supported"] == [MCP_LATEST_VERSION]


def test_unknown_modern_version_is_rejected_on_direct_and_pending_stdio(system):
    request = _modern_request("server/discover", {}, request_id=79)
    request["params"]["_meta"][PROTOCOL_VERSION_META_KEY] = "2099-01-01"

    direct = MCPServer(system["service"]).handle_modern(system["cao"], request)
    pending = _pending_bridge_response(request)

    assert direct is not None
    assert direct["error"]["code"] == -32022
    assert pending is not None
    assert pending["error"]["code"] == -32022


def test_missing_required_modern_metadata_is_invalid_params(system):
    request = _modern_request("server/discover", {}, request_id=80)
    del request["params"]["_meta"][CLIENT_CAPABILITIES_META_KEY]

    response = MCPServer(system["service"]).handle_modern(system["cao"], request)

    assert response is not None
    assert response["error"]["code"] == -32602


def test_initialize_with_modern_envelope_never_creates_a_legacy_session(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "initialize",
        {
            "protocolVersion": MCP_LATEST_VERSION,
            "clientInfo": {"name": "conformance", "version": "1"},
            "capabilities": {},
        },
        request_id=72,
    )

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "initialize", ""),
            json=request,
        )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == -32601
    assert "mcp-session-id" not in response.headers


@pytest.mark.parametrize("invalid_request_id", [None, True, 1.5, [], {}])
def test_modern_request_ids_reject_null_boolean_float_and_containers(
    system, invalid_request_id: object
):
    server = MCPServer(system["service"])
    request = _modern_request("server/discover", {}, request_id=73)
    request["id"] = invalid_request_id

    response = server.validate_modern_request(
        request,
        protocol_header=MCP_LATEST_VERSION,
        method_header="server/discover",
        name_header=None,
    )

    assert response is not None
    assert response["error"]["code"] == -32600
    assert "id" not in response


def test_pending_modern_catalog_has_cache_hints_and_no_change_capability():
    discover = _pending_bridge_response(_modern_request("server/discover", {}, request_id=74))
    tools = _pending_bridge_response(_modern_request("tools/list", {}, request_id=75))

    assert discover is not None
    assert discover["result"]["capabilities"]["tools"]["listChanged"] is False
    assert discover["result"]["cacheScope"] == "private"
    assert discover["result"]["ttlMs"] >= 0
    assert tools is not None
    assert tools["result"]["cacheScope"] == "private"
    assert tools["result"]["ttlMs"] >= 0


def test_tool_protocol_errors_are_distinct_from_actionable_execution_errors(system):
    server = MCPServer(system["service"])

    unknown = server.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {"name": "cao_missing_tool", "arguments": {}},
            request_id=76,
        ),
    )
    invalid_arguments = server.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {"name": "cao_ack", "arguments": {}},
            request_id=77,
        ),
    )
    missing_work = server.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {"name": "cao_get_work", "arguments": {"work_item_id": "wrk_missing"}},
            request_id=78,
        ),
    )

    assert unknown is not None
    assert unknown["error"]["code"] == -32602
    assert invalid_arguments is not None
    assert invalid_arguments["result"]["isError"] is True
    assert invalid_arguments["result"]["structuredContent"]["error"]["code"] == (
        "invalid_tool_arguments"
    )
    assert missing_work is not None
    assert missing_work["result"]["isError"] is True
    assert missing_work["result"]["structuredContent"]["error"]["code"] == "not_found"


def test_tool_input_schema_rejects_renamed_and_extra_fields_before_execution(system):
    server = MCPServer(system["service"])

    renamed_required_fields = server.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {
                "name": "cao_mark_handled",
                "arguments": {"message_ids": ["msg_not_a_real_message"]},
            },
            request_id=791,
        ),
    )
    ignored_extra_field = server.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {
                "name": "cao_get_inbox",
                "arguments": {"unsupported_filter": "must-not-be-ignored"},
            },
            request_id=792,
        ),
    )

    for response in (renamed_required_fields, ignored_extra_field):
        assert response is not None
        assert "error" not in response
        assert response["result"]["isError"] is True
        error = response["result"]["structuredContent"]["error"]
        assert error["code"] == "invalid_tool_arguments"
        rendered = json.dumps(response, ensure_ascii=False)
        assert "must-not-be-ignored" not in rendered
        assert "unsupported_filter" not in rendered
        assert "message_ids" not in rendered

    renamed_details = renamed_required_fields["result"]["structuredContent"]["error"][
        "details"
    ]
    assert renamed_details["allowed_fields"] == ["evidence", "message_id"]
    assert renamed_details["required_fields"] == ["evidence", "message_id"]


def test_streamable_http_returns_model_actionable_tool_schema_errors(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "tools/call",
        {
            "name": "cao_mark_handled",
            "arguments": {"message_ids": ["msg_not_a_real_message"]},
        },
        request_id=793,
    )

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "tools/call", "cao_mark_handled"),
            json=request,
        )

    assert response.status_code == 200
    body = response.json()
    assert "error" not in body
    assert body["result"]["isError"] is True
    error = body["result"]["structuredContent"]["error"]
    assert error == {
        "code": "invalid_tool_arguments",
        "message": "Tool input validation failed",
        "details": {
            "allowed_fields": ["evidence", "message_id"],
            "required_fields": ["evidence", "message_id"],
            "violations": [
                {"rule": "additionalProperties"},
                {"rule": "required"},
                {"rule": "required"},
            ],
            "missing_required_fields": ["evidence", "message_id"],
        },
    }


def test_streamable_http_maps_protocol_invalid_params_to_http_400(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "tools/call",
        {"name": "cao_missing_tool", "arguments": {}},
        request_id=81,
    )

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "tools/call", "cao_missing_tool"),
            json=request,
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32602


def test_streamable_http_rejects_duplicate_routing_headers(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request("server/discover", {}, request_id=90)
    headers = list(_mcp_headers(token, "server/discover", "").items())
    headers.append(("Mcp-Method", "server/discover"))

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post("/mcp", headers=headers, json=request)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32020


@pytest.mark.parametrize("invalid_progress_token", [None, True, 1.5, [], {}])
def test_modern_progress_token_requires_string_or_integer(system, invalid_progress_token: object):
    request = _modern_request("tools/list", {}, request_id=82)
    request["params"]["_meta"]["progressToken"] = invalid_progress_token

    response = MCPServer(system["service"]).handle_modern(system["cao"], request)

    assert response is not None
    assert response["error"]["code"] == -32602


def test_active_progress_tokens_are_unique_per_authenticated_client(system):
    server = MCPServer(system["service"])
    request = _modern_request(
        "tools/call",
        {"name": "cao_get_inbox", "arguments": {}},
        request_id=820,
    )
    request["params"]["_meta"]["progressToken"] = "shared-active-token"

    async def exercise() -> tuple[dict[str, object], dict[str, object]]:
        first = server.progress_messages(system["cao"], request)
        await anext(first)
        duplicate = [message async for message in server.progress_messages(system["cao"], request)]
        await first.aclose()
        retried = [message async for message in server.progress_messages(system["cao"], request)]
        return duplicate[0], retried[-1]

    duplicate, retried = asyncio.run(exercise())
    assert duplicate["error"]["code"] == -32602
    assert duplicate["error"]["message"] == "progressToken is already active"
    assert retried["id"] == 820


@pytest.mark.parametrize("method", ["tools/list", "resources/list"])
@pytest.mark.parametrize("cursor", ["", "cursor-never-issued"])
def test_modern_list_methods_reject_unissued_cursors(system, method: str, cursor: str):
    response = MCPServer(system["service"]).handle_modern(
        system["cao"],
        _modern_request(method, {"cursor": cursor}, request_id=821),
    )

    assert response is not None
    assert response["error"]["code"] == -32602
    assert response["error"]["message"] == "Invalid cursor"


def test_pending_catalog_rejects_unissued_cursor():
    response = _pending_bridge_response(
        _modern_request("tools/list", {"cursor": "cursor-never-issued"}, request_id=822)
    )

    assert response is not None
    assert response["error"]["code"] == -32602
    assert response["error"]["message"] == "Invalid cursor"


def test_streamable_http_progress_notifications_precede_the_final_tool_result(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "tools/call",
        {"name": "cao_get_inbox", "arguments": {}},
        request_id=83,
    )
    request["params"]["_meta"]["progressToken"] = "progress-83"

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "tools/call", "cao_get_inbox"),
            json=request,
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    messages = _sse_messages(response.content)
    assert [message.get("method") for message in messages[:-1]] == [
        "notifications/progress",
        "notifications/progress",
    ]
    assert [message["params"]["progress"] for message in messages[:-1]] == [0, 1]
    assert all(message["params"]["progressToken"] == "progress-83" for message in messages[:-1])
    assert messages[-1]["id"] == 83
    assert messages[-1]["result"]["isError"] is False


def test_subscription_stream_acknowledges_only_authorized_dynamic_resources_and_notifies_changes(
    system,
):
    server = MCPServer(system["service"])
    discover = server.discover(system["cao"])
    request = _modern_request(
        "subscriptions/listen",
        {
            "notifications": {
                "toolsListChanged": True,
                "resourceSubscriptions": ["cao://self", "cao://events"],
            }
        },
        request_id=84,
    )

    assert discover["capabilities"]["resources"] == {
        "subscribe": True,
        "listChanged": False,
    }

    async def exercise() -> tuple[dict[str, object], dict[str, object]]:
        stream = server.subscription_messages(
            system["cao"],
            request,
            heartbeat_seconds=0.2,
            poll_seconds=0.01,
        )
        acknowledged = await anext(stream)
        system["service"].create_principal(
            system["cao"],
            PrincipalCreate(
                name="subscription-change",
                role=PrincipalRole.EXTERNAL,
                metadata={},
            ),
        )
        changed = await asyncio.wait_for(anext(stream), timeout=1)
        await stream.aclose()
        assert acknowledged is not None
        assert changed is not None
        return acknowledged, changed

    acknowledged, changed = asyncio.run(exercise())
    assert acknowledged["method"] == "notifications/subscriptions/acknowledged"
    assert acknowledged["params"]["notifications"] == {"resourceSubscriptions": ["cao://events"]}
    assert acknowledged["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == 84
    assert changed["method"] == "notifications/resources/updated"
    assert changed["params"]["uri"] == "cao://events"


def test_streamable_http_subscription_with_no_honored_filter_closes_gracefully(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "subscriptions/listen",
        {"notifications": {"resourceSubscriptions": ["cao://self"]}},
        request_id=85,
    )

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "subscriptions/listen", ""),
            json=request,
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    messages = _sse_messages(response.content)
    assert messages[0]["method"] == "notifications/subscriptions/acknowledged"
    assert messages[0]["params"]["notifications"] == {}
    assert messages[1]["id"] == 85
    assert messages[1]["result"]["resultType"] == "complete"
    assert messages[1]["result"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == 85


def test_streamable_http_server_teardown_cancels_an_acknowledged_subscription(
    settings, monkeypatch
):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    request = _modern_request(
        "subscriptions/listen",
        {"notifications": {"resourceSubscriptions": ["cao://events"]}},
        request_id=850,
    )

    def fail_after_ack(*_args, **_kwargs):
        raise RuntimeError("private backend detail")

    monkeypatch.setattr(app.state.service.db, "wait_for_commit", fail_after_ack)
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/mcp",
            headers=_mcp_headers(token, "subscriptions/listen", ""),
            json=request,
        )

    assert response.status_code == 200
    messages = _sse_messages(response.content)
    assert messages[0]["method"] == "notifications/subscriptions/acknowledged"
    assert messages[1] == {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 850, "reason": "Subscription stream failed"},
    }


def test_stdio_proxy_keeps_reading_until_a_long_lived_subscription_is_cancelled(
    system, monkeypatch
):
    subscription_request = _modern_request(
        "subscriptions/listen",
        {"notifications": {"resourceSubscriptions": ["cao://events"]}},
        request_id=89,
    )
    cancellation = {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 89, "reason": "client closed the subscription"},
    }

    class Input:
        buffer = io.BytesIO(
            json.dumps(subscription_request).encode("utf-8")
            + b"\n"
            + json.dumps(cancellation).encode("utf-8")
            + b"\n"
        )

    class Response:
        status_code = 200

        def __init__(self):
            self.headers = {"content-type": "text/event-stream"}

        async def aiter_lines(self):
            acknowledged = {
                "jsonrpc": "2.0",
                "method": "notifications/subscriptions/acknowledged",
                "params": {
                    "_meta": {"io.modelcontextprotocol/subscriptionId": 89},
                    "notifications": {"resourceSubscriptions": ["cao://events"]},
                },
            }
            yield "event: message"
            yield "data: " + json.dumps(acknowledged)
            yield ""
            await asyncio.Event().wait()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *args, **kwargs):
            del args, kwargs
            return Stream()

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                token=system["cao_token"],
            )
        )
        == 0
    )
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(messages) == 1
    assert messages[0]["method"] == "notifications/subscriptions/acknowledged"


@pytest.mark.parametrize("standard_stdio", [False, True])
def test_stdio_proxy_progress_request_can_be_cancelled_without_blocking_input(
    system, monkeypatch, standard_stdio
):
    if standard_stdio:
        request = {
            "jsonrpc": "2.0",
            "id": 891,
            "method": "tools/call",
            "params": {
                "name": "cao_get_inbox",
                "arguments": {},
                "_meta": {},
            },
        }
    else:
        request = _modern_request(
            "tools/call",
            {"name": "cao_get_inbox", "arguments": {}},
            request_id=891,
        )
    request["params"]["_meta"]["progressToken"] = "proxy-cancel-891"
    cancellation = {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 891, "reason": "no longer needed"},
    }

    class Input:
        buffer = io.BytesIO(
            json.dumps(request).encode("utf-8")
            + b"\n"
            + json.dumps(cancellation).encode("utf-8")
            + b"\n"
        )

    class Response:
        status_code = 200

        def __init__(self):
            self.headers = {"content-type": "text/event-stream"}

        async def aiter_lines(self):
            progress = {
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": {"progressToken": "proxy-cancel-891", "progress": 0},
            }
            yield "event: message"
            yield "data: " + json.dumps(progress)
            yield ""
            await asyncio.Event().wait()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    stream_calls = []

    class Client:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *args, **kwargs):
            stream_calls.append((args, kwargs))
            return Stream()

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    result = asyncio.run(
        asyncio.wait_for(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                token=system["cao_token"],
            ),
            timeout=1,
        )
    )

    assert result == 0
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert messages == [
        {
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {"progressToken": "proxy-cancel-891", "progress": 0},
        }
    ]
    assert len(stream_calls) == 1
    posted_meta = stream_calls[0][1]["json"]["params"]["_meta"]
    assert posted_meta[PROTOCOL_VERSION_META_KEY] == MCP_LATEST_VERSION
    assert posted_meta[CLIENT_CAPABILITIES_META_KEY] == {}
    assert isinstance(posted_meta[CLIENT_INFO_META_KEY], dict)


def test_http_mcp_cached_reasoner_turn_replay_reauthorizes_foreign_conversation(
    system,
):
    app = create_app(system["settings"])
    service = app.state.service
    owner_attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="protocol-http-replay-owner",
            project_digest="c" * 64,
        ),
    )
    foreign_attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="protocol-http-replay-foreign",
            project_digest="d" * 64,
        ),
    )
    owner_token = str(owner_attachment["context_token"])
    foreign_token = str(foreign_attachment["context_token"])
    owner = service.authenticate(owner_token)
    work = service.assign_work(
        owner,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="HTTP MCP cached replay authority",
            objective="Keep the reasoner-turn lease within its exact CAO conversation.",
            acceptance=["A foreign conversation cannot replay the cached lease."],
            idempotency_key="protocol-http-replay-assign",
        ),
    )
    waiting = service.report(
        system["worker"],
        str(work["current_attempt"]["id"]),
        ReportInput(
            kind="question",
            expected_goal_version=int(work["goal_version"]),
            expected_generation=int(work["generation"]),
            expected_goal_packet_digest=str(work["current_attempt"]["goal_packet_digest"]),
            expected_task_packet_digest=str(work["current_attempt"]["task_packet_digest"]),
            summary="Which exact option should the Worker use?",
            idempotency_key="protocol-http-replay-question",
        ),
    )
    boundary_id = str(waiting["open_boundaries"][-1]["id"])
    arguments = {
        "work_item_id": str(work["id"]),
        "boundary_id": boundary_id,
        "expected_generation": int(waiting["generation"]),
        "idempotency_key": "protocol-http-replay-acquire",
    }

    owner_request = _modern_request(
        "tools/call",
        {"name": "cao_acquire_reasoner_turn", "arguments": arguments},
        request_id=41,
    )
    client = TestClient(app, base_url="http://localhost")
    try:
        acquired_response = client.post(
            "/mcp",
            headers=_mcp_headers(owner_token, "tools/call", "cao_acquire_reasoner_turn"),
            json=owner_request,
        )
        assert acquired_response.status_code == 200, acquired_response.text
        acquired_payload = acquired_response.json()
        assert "error" not in acquired_payload
        acquired = acquired_payload["result"]["structuredContent"]

        def ledger_snapshot() -> dict[str, tuple[tuple[object, ...], ...]]:
            with service.db.connect() as connection:
                table_names = tuple(
                    str(row["name"])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                    )
                )
                return {
                    table: tuple(
                        tuple(row)
                        for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
                    )
                    for table in table_names
                }

        before = ledger_snapshot()
        denied_response = client.post(
            "/mcp",
            headers=_mcp_headers(foreign_token, "tools/call", "cao_acquire_reasoner_turn"),
            json=_modern_request(
                "tools/call",
                {"name": "cao_acquire_reasoner_turn", "arguments": arguments},
                request_id=42,
            ),
        )
        assert denied_response.status_code == 200, denied_response.text
        denied = denied_response.json()
        assert "error" not in denied
        assert denied["result"]["isError"] is True
        assert denied["result"]["structuredContent"]["error"]["code"] == "forbidden"
        rendered = json.dumps(denied, sort_keys=True)
        assert "work item belongs to another CAO conversation" in rendered
        for private_value in (
            acquired["id"],
            acquired["lease_token"],
            owner_token,
            foreign_token,
            owner_attachment["id"],
            foreign_attachment["id"],
        ):
            assert str(private_value) not in rendered
        assert ledger_snapshot() == before
    finally:
        client.close()


def test_mcp_encodes_whitespace_name_headers(system):
    server = MCPServer(system["service"])
    uri = "cao://resource with space"
    request = _modern_request("resources/read", {"uri": uri})
    headers = modern_http_headers(request)
    assert headers["Mcp-Name"].startswith("=?base64?")
    assert (
        server.validate_modern_request(
            request,
            protocol_header=MCP_LATEST_VERSION,
            method_header="resources/read",
            name_header=headers["Mcp-Name"],
        )
        is None
    )


def test_mcp_encodes_literal_base64_sentinel_name_headers(system):
    server = MCPServer(system["service"])
    literal = "=?base64?YWJj?="
    request = _modern_request("resources/read", {"uri": literal})
    headers = modern_http_headers(request)

    assert headers["Mcp-Name"].startswith("=?base64?")
    assert headers["Mcp-Name"] != literal
    assert (
        server.validate_modern_request(
            request,
            protocol_header=MCP_LATEST_VERSION,
            method_header="resources/read",
            name_header=headers["Mcp-Name"],
        )
        is None
    )


def test_mcp_and_a2a_validation_errors_never_reflect_credentials_or_input(system):
    secret = system["worker_token"]
    mcp = MCPServer(system["service"])
    mcp_response = mcp.handle_modern(
        system["cao"],
        _modern_request(
            "tools/call",
            {
                "name": "cao_assign",
                "arguments": {
                    "worker_id": system["worker"]["id"],
                    "title": "validation reflection",
                    "objective": "Reject an attacker-controlled extra key.",
                    "acceptance": ["The credential is not reflected."],
                    secret: "invalid extra field",
                },
            },
        ),
    )
    rendered_mcp = json.dumps(mcp_response, ensure_ascii=False)
    assert mcp_response["result"]["isError"] is True
    assert mcp_response["result"]["structuredContent"]["error"]["code"] == (
        "invalid_tool_arguments"
    )
    assert secret not in rendered_mcp
    assert '"input"' not in rendered_mcp

    a2a = A2AServer(system["service"])
    params = _a2a_params(
        system,
        {"messageId": "validation-reflection", "parts": [{"text": "validate"}]},
    )
    params["message"]["metadata"]["runtimeSessionId"] = {secret: "invalid"}
    a2a_response = a2a.handle_jsonrpc(
        system["cao"],
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "SendMessage",
            "params": params,
        },
    )
    rendered_a2a = json.dumps(a2a_response, ensure_ascii=False)
    assert a2a_response["error"]["code"] == -32602
    assert secret not in rendered_a2a
    assert '"input"' not in rendered_a2a


def test_agent_card_omits_nonstandard_extended_card_capability(system):
    card = A2AServer(system["service"]).agent_card()
    assert "extendedAgentCard" not in card["capabilities"]
    assert "localBearer" in card["securitySchemes"]
