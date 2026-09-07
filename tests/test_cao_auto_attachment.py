from __future__ import annotations

import asyncio
import io
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from conftest import (
    CURRENT_CAO_CATALOG_DIGEST,
    CURRENT_CAO_PROXY_ABI_VERSION,
    current_cao_session_attachment,
)
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.cli import build_parser, run
from cao_control_plane.config import Settings
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.dashboard_access import DashboardAccessResult
from cao_control_plane.errors import (
    AuthenticationError,
    AuthorizationError,
    ValidationError,
)
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    AttachedCAOConversation,
    CAOConversationContext,
    CAOStartVerification,
    _attached_start_catalog_request,
    _attached_start_probe_request,
    _can_preserve_degraded_connection,
    _verified_connection_marker,
    conversation_proxy_tools,
    conversation_server_tools,
    current_cao_conversation_context,
    serve_stdio,
    serve_stdio_proxy,
)
from cao_control_plane.models import (
    ReportInput,
    ReportKind,
    WorkAssignment,
)
from cao_control_plane.release_identity import catalog_digest
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityError,
    ProcessIdentity,
    _process_identity,
)


def _start_verification_payload(request: dict[str, Any]) -> dict[str, Any] | None:
    """Return the daemon half of the exact cao_start readiness contract."""

    if request.get("method") == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"tools": conversation_server_tools()},
        }
    params = request.get("params", {})
    if (
        request.get("method") == "tools/call"
        and isinstance(params, dict)
        and params.get("name") == "cao_list_managed_workers"
    ):
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "content": [],
                "structuredContent": {"workers": []},
                "isError": False,
            },
        }
    return None


def test_start_readiness_probes_do_not_reuse_the_call_progress_token() -> None:
    request = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "tools/call",
        "params": {
            "name": "cao_start",
            "arguments": {"native_thread_id": "thread-progress"},
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "codex", "version": "1"},
                "progressToken": "start-progress-17",
            },
        },
    }

    catalog_request = _attached_start_catalog_request(request)
    worker_request = _attached_start_probe_request(request)

    assert "progressToken" not in catalog_request["params"]["_meta"]
    assert "progressToken" not in worker_request["params"]["_meta"]
    assert request["params"]["_meta"]["progressToken"] == "start-progress-17"


def _bootstrap(service: Any, thread: str, project: str) -> dict[str, Any]:
    identity = _process_identity(os.getpid())
    token = service.issue_owner_local_attachment_bootstrap(
        identity,
        thread,
        project * 64,
        CURRENT_CAO_CATALOG_DIGEST,
        CURRENT_CAO_PROXY_ABI_VERSION,
    )
    return service.authenticate(token)


def _attach(service: Any, thread: str, project: str) -> dict[str, Any]:
    return service.attach_cao_session(
        _bootstrap(service, thread, project),
        current_cao_session_attachment(native_thread_id=thread, project_digest=project * 64),
    )


def _assign_and_report(
    system: dict[str, Any], actor: dict[str, Any], key: str
) -> tuple[dict[str, Any], str]:
    service = system["service"]
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title=f"Conversation {key}",
            objective=f"Produce a durable boundary for conversation {key}",
            acceptance=["The exact originating conversation receives the boundary"],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key=f"conversation-{key}",
        ),
    )
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
            summary=f"Boundary {key}",
            idempotency_key=f"boundary-{key}",
        ),
    )
    row = service.db.fetchone(
        """
        SELECT m.id FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.work_item_id = ? AND d.recipient_id = ?
          AND json_extract(m.payload_json, '$.boundary_id') IS NOT NULL
        """,
        (work["id"], actor["id"]),
    )
    assert row is not None
    return work, str(row["id"])


def test_current_conversation_context_is_thread_bound_without_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    context = current_cao_conversation_context({"CODEX_THREAD_ID": "thread-one"})
    assert context is not None
    assert context.native_thread_id == "thread-one"
    assert len(context.project_digest) == 64
    assert str(tmp_path) not in context.project_digest
    assert current_cao_conversation_context({}) is None


def test_cli_without_thread_id_uses_pending_bridge_not_admin_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def bridge(*args: Any, **kwargs: Any) -> int:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return 0

    def no_bootstrap(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        raise AssertionError("normal mcp-stdio must not load an admin token")

    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.setattr("cao_control_plane.cli.serve_stdio", bridge)
    monkeypatch.setattr("cao_control_plane.cli._token_from_bootstrap", no_bootstrap)

    assert run(build_parser().parse_args(["mcp-stdio"])) == 0
    assert captured["args"][1] is None
    assert captured["kwargs"]["pending_conversation_bridge"] is True
    assert captured["kwargs"]["cao_attachment_issuer_socket"] is not None


def test_cli_wires_independent_dashboard_access_into_pending_bridge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    state = tmp_path / "state"
    bootstrap = state / "dashboard-bootstrap"
    sessions = state / "dashboard-sessions"
    bootstrap.mkdir(parents=True, mode=0o700)
    sessions.mkdir(mode=0o700)
    settings = Settings(
        state_dir=state,
        runtime_launch_dir=state / "runtime-launches",
        enable_dashboard=True,
        enable_dashboard_access_probe=True,
        dashboard_credentials_file=state / "dashboard-credentials.json",
        dashboard_bootstrap_record_dir=bootstrap,
        dashboard_session_record_dir=sessions,
    )

    async def bridge(*args: Any, **kwargs: Any) -> int:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return 0

    monkeypatch.setattr("cao_control_plane.cli._settings", lambda _args: settings)
    monkeypatch.setattr("cao_control_plane.cli.serve_stdio", bridge)

    assert run(build_parser().parse_args(["mcp-stdio"])) == 0
    coordinator = captured["kwargs"]["dashboard_access"]
    assert coordinator is not None
    assert captured["kwargs"]["pending_conversation_bridge"] is True


@pytest.mark.asyncio
async def test_stdio_wrapper_propagates_dashboard_access_to_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Marker:
        url = "https://dashboard.example.test/dashboard/"

        def inspect(self) -> object:
            return object()

    marker = Marker()
    captured: dict[str, Any] = {}

    async def proxy(*args: Any, **kwargs: Any) -> int:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return 0

    monkeypatch.setattr("cao_control_plane.mcp.serve_stdio_proxy", proxy)

    assert (
        await serve_stdio(
            "http://127.0.0.1:8768/mcp",
            None,
            cao_attachment_issuer_socket="issuer.sock",
            pending_conversation_bridge=True,
            dashboard_access=marker,
        )
        == 0
    )
    assert captured["kwargs"]["dashboard_access"] is marker


def test_cao_runtime_wake_never_observes_dashboard_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Access:
        @property
        def url(self) -> str:
            raise AssertionError("Worker wake must not read Dashboard configuration")

        def inspect(self) -> DashboardAccessResult:
            raise AssertionError("Worker wake must not probe Dashboard access")

    async def receive(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        return {"token": "cao.runtime-token", "runtime_id": "runtime-1", "generation": 1}

    class Input:
        buffer = io.BytesIO(b"")

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.receive_enrollment_capability", receive)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_runtime_broker_socket="runtime-broker.sock",
                dashboard_access=Access(),
            )
        )
        == 0
    )
    assert output.getvalue() == ""


def test_proxy_obtains_one_use_bootstrap_before_attaching_conversation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = CAOConversationContext(
        native_thread_id="issuer-proxy-thread", project_digest="a" * 64
    )
    calls: list[dict[str, Any]] = []

    class Response:
        status_code = 200

        def json(self) -> dict[str, str]:
            return {
                "id": "attachment-1",
                "native_thread_id": context.native_thread_id,
                "project_digest": context.project_digest,
                "context_token": "cao.csc_scoped-conversation-token",
            }

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            calls.append({"url": url, **kwargs})
            return Response()

    async def issue(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return "cao.cab_one-use-attachment-token"

    class EmptyInput:
        buffer = io.BytesIO()

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", EmptyInput())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    result = asyncio.run(
        serve_stdio_proxy(
            "http://127.0.0.1:8768/mcp",
            cao_attachment_issuer_socket="/owner/private/issuer.sock",
            cao_conversation_context=context,
        )
    )
    assert result == 0
    assert len(calls) == 1
    assert calls[0]["url"].endswith("/api/v1/cao-session-attachments")
    assert calls[0]["headers"]["Authorization"] == "Bearer cao.cab_one-use-attachment-token"
    assert calls[0]["json"] == {
        "native_thread_id": context.native_thread_id,
        "project_digest": context.project_digest,
        "proxy_catalog_digest": catalog_digest(conversation_proxy_tools()),
        "proxy_abi_version": CAO_CONVERSATION_PROXY_ABI_VERSION,
    }
    assert output.getvalue() == ""


def test_initial_dashboard_failure_keeps_attachment_and_immediate_control_plane_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    issued = 0

    class Visibility:
        url = "https://dashboard.example.test/dashboard/"

        def __init__(self) -> None:
            self.calls = 0

        def inspect(self) -> DashboardAccessResult:
            self.calls += 1
            return DashboardAccessResult(
                "unavailable",
                "ready",
                "ready",
                "unavailable",
                self.url,
                "dashboard_public_access_unavailable",
            )

    visibility = Visibility()

    async def issue(*args: Any, **kwargs: Any) -> str:
        nonlocal issued
        del args, kwargs
        issued += 1
        return "cao.cab_initial-dashboard-failure"

    class Response:
        def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
            self.status_code = status_code
            self._payload = payload
            self.headers: dict[str, str] = {}
            self.content = b"{}"

        def json(self) -> dict[str, Any]:
            return self._payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                body = kwargs["json"]
                return Response(
                    200,
                    {
                        "id": "initial-dashboard-failure-attachment",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": "cao.csc_initial-dashboard-failure",
                    },
                )
            request = kwargs["json"]
            verification_payload = _start_verification_payload(request)
            if verification_payload is not None:
                return Response(200, verification_payload)
            return Response(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"structuredContent": {"items": []}},
                },
            )

    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {
                    "native_thread_id": "dashboard-failed-thread",
                },
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "cao_list_managed_workers", "arguments": {}},
        },
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "cao_show_dashboard", "arguments": {}},
        },
    ]

    class Input:
        buffer = io.BytesIO(
            b"".join((json.dumps(request).encode() + b"\n") for request in requests)
        )

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
                dashboard_access=visibility,
            )
        )
        == 0
    )

    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    started = next(value for value in responses if value.get("id") == 1)
    started_content = started["result"]["structuredContent"]
    assert started_content["status"] == "ready"
    assert started_content["attachment_verification"] == "verified"
    assert started_content["catalog_verification"]["status"] == "verified"
    assert started_content["dashboard"] == {
        "service": "independent",
        "public_access": "not-checked",
        "presentation": "client-controlled",
        "url": "https://dashboard.example.test/dashboard/",
        "view_scope": "all_production_conversations",
        "control_scope": "current_conversation_only",
        "routing": {
            "visible_card_absent_from_scoped_tools": "belongs_to_another_conversation",
            "prohibited_inference": "stale_or_equivalent_work",
        },
    }
    shown = next(value for value in responses if value.get("id") == 3)["result"]
    assert shown["isError"] is True
    assert shown["structuredContent"]["error"]["reason_code"] == (
        "dashboard_public_access_unavailable"
    )
    listed = next(value for value in responses if value.get("id") == 2)
    assert "error" not in listed
    assert issued == 1
    assert [call["url"].rsplit("/", 1)[-1] for call in calls] == [
        "cao-session-attachments",
        "mcp",
        "mcp",
        "mcp",
    ]
    assert calls[0]["headers"]["Authorization"] == ("Bearer cao.cab_initial-dashboard-failure")
    assert all(
        call["headers"]["Authorization"] == ("Bearer cao.csc_initial-dashboard-failure")
        for call in calls[1:]
    )
    assert visibility.calls == 1  # only the explicit Dashboard access tool probes
    assert "private presenter detail" not in output.getvalue()
    assert "cao.cab_initial-dashboard-failure" not in output.getvalue()
    assert "cao.csc_initial-dashboard-failure" not in output.getvalue()


@pytest.mark.parametrize(
    ("status_code", "failure_payload", "reason_code"),
    (
        (
            503,
            {"detail": "private attachment locator must not escape"},
            "attachment_peer_unavailable",
        ),
        (
            409,
            {
                "error": {
                    "code": "conflict",
                    "message": "private attachment conflict detail must not escape",
                    "details": {
                        "reason_code": "unrecognized_internal_conflict",
                        "retryable": False,
                        "private_peer_pid": 4242,
                    },
                }
            },
            "attachment_context_invalid",
        ),
    ),
)
def test_initial_attachment_failure_is_bounded_and_does_not_retry_or_present(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    failure_payload: dict[str, Any],
    reason_code: str,
) -> None:
    calls: list[dict[str, Any]] = []
    issued = 0

    class Visibility:
        url = "https://dashboard.example.test/dashboard/"
        calls = 0

        def inspect(self) -> DashboardAccessResult:
            self.calls += 1
            raise AssertionError("attachment failure must not probe Dashboard access")

    visibility = Visibility()

    async def issue(*args: Any, **kwargs: Any) -> str:
        nonlocal issued
        del args, kwargs
        issued += 1
        return "cao.cab_attachment-failure"

    class Response:
        content = b"{}"

        def __init__(self) -> None:
            self.status_code = status_code

        def json(self) -> dict[str, Any]:
            return failure_payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            calls.append({"url": url, **kwargs})
            return Response()

    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "cao_start",
            "arguments": {
                "native_thread_id": "attachment-failed-thread",
            },
        },
    }

    class Input:
        buffer = io.BytesIO(json.dumps(request).encode() + b"\n")

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
                dashboard_access=visibility,
            )
        )
        == 0
    )

    response = json.loads(output.getvalue().splitlines()[0])
    assert response["result"]["isError"] is False
    assert response["result"]["structuredContent"] == {
        "status": "stopped",
        "reason_code": reason_code,
        "retryable": False,
    }
    assert issued == 1
    assert len(calls) == 1
    assert calls[0]["url"].endswith("/api/v1/cao-session-attachments")
    assert calls[0]["headers"]["Authorization"] == "Bearer cao.cab_attachment-failure"
    assert visibility.calls == 0
    assert "private attachment locator" not in output.getvalue()
    assert "private prior peer identity" not in output.getvalue()
    assert "4242" not in output.getvalue()
    assert "cao.cab_attachment-failure" not in output.getvalue()


def test_pending_bridge_returns_to_unattached_state_after_csc_401_without_retry_or_admin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class Response:
        def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
            self.status_code = status_code
            self._payload = payload
            self.headers: dict[str, str] = {}
            self.content = b"{}"

        def json(self) -> dict[str, Any]:
            return self._payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                body = kwargs["json"]
                return Response(
                    200,
                    {
                        "id": "attachment-401",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": "cao.csc_expiring-conversation-token",
                    },
                )
            verification_payload = _start_verification_payload(kwargs["json"])
            if verification_payload is not None:
                return Response(200, verification_payload)
            return Response(401, {"detail": "Unauthorized"})

    async def issue(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return "cao.cab_fresh-one-use-token"

    class Visibility:
        url = "https://dashboard.example.test/dashboard/"
        calls = 0

        def inspect(self) -> DashboardAccessResult:
            self.calls += 1
            raise AssertionError("attachment expiry must not probe Dashboard access")

    visibility = Visibility()

    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "cao_start", "arguments": {"native_thread_id": "thread-401"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "cao_assign",
                "arguments": {"idempotency_key": "expired-attachment-assignment"},
            },
        },
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
    ]

    class Input:
        buffer = io.BytesIO(
            b"".join((json.dumps(request).encode("utf-8") + b"\n") for request in requests)
        )

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
                dashboard_access=visibility,
            )
        )
        == 0
    )

    # An attached operation forwards only once and its 401 returns the bridge
    # to pending; Dashboard access is never consulted on this authority path.
    assert [call["url"].rsplit("/", 1)[-1] for call in calls] == [
        "cao-session-attachments",
        "mcp",
        "mcp",
        "mcp",
    ]
    assert calls[-1]["headers"]["Authorization"] == ("Bearer cao.csc_expiring-conversation-token")
    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    expired = next(value for value in responses if value.get("id") == 2)
    assert expired["error"]["message"] == (
        "CAO conversation attachment expired; call cao_start again."
    )
    listed = next(value for value in responses if value.get("id") == 3)
    assert {tool["name"] for tool in listed["result"]["tools"]} == {
        tool["name"] for tool in conversation_proxy_tools()
    }
    assert not any(value.get("method") == "notifications/tools/list_changed" for value in responses)
    assert visibility.calls == 0
    assert "presenter repair failed" not in output.getvalue()
    assert "cao.cab_fresh-one-use-token" not in output.getvalue()
    assert "cao.csc_expiring-conversation-token" not in output.getvalue()


@pytest.mark.parametrize("modern", [False, True], ids=["standard-stdio", "modern"])
def test_attached_cao_start_renews_an_expired_csc_in_one_call(
    monkeypatch: pytest.MonkeyPatch,
    modern: bool,
) -> None:
    calls: list[dict[str, Any]] = []
    attachment_count = 0

    class Response:
        def __init__(
            self,
            status_code: int,
            payload: dict[str, Any],
            *,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.status_code = status_code
            self._payload = payload
            self.headers = headers or {}
            self.content = json.dumps(payload).encode()

        def json(self) -> dict[str, Any]:
            return self._payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            nonlocal attachment_count
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                attachment_count += 1
                body = kwargs["json"]
                return Response(
                    200,
                    {
                        "id": f"attachment-{attachment_count}",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": f"cao.csc_context-{attachment_count}",
                    },
                )

            body = kwargs["json"]
            headers = kwargs.get("headers") or {}
            if body.get("id") == 3 and headers.get("Authorization") == "Bearer cao.csc_context-1":
                return Response(401, {"detail": "Unauthorized"})
            verification_payload = _start_verification_payload(body)
            if (
                verification_payload is not None
                and headers.get("MCP-Protocol-Version") == MCP_LATEST_VERSION
            ):
                return Response(200, verification_payload)
            if headers.get("MCP-Protocol-Version") == MCP_LATEST_VERSION:
                return Response(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": {
                            "structuredContent": {"workers": []},
                            "isError": False,
                        },
                    },
                )
            raise AssertionError("proxy must use the canonical stateless HTTP route")

    async def issue(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return f"cao.cab_bootstrap-{attachment_count + 1}"

    def start_request(request_id: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            "name": "cao_start",
            "arguments": {"native_thread_id": "thread-one"},
        }
        if modern:
            params["_meta"] = {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "pytest", "version": "1"},
            }
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": params,
        }

    requests = (
        []
        if modern
        else [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            }
        ]
    ) + [start_request(2), start_request(3)]

    class Input:
        buffer = io.BytesIO(
            b"".join((json.dumps(request).encode() + b"\n") for request in requests)
        )

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
            )
        )
        == 0
    )

    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    renewed = next(value for value in responses if value.get("id") == 3)
    assert renewed["result"]["structuredContent"]["status"] == "ready"
    assert attachment_count == 2
    forwarded_methods = [call["json"]["method"] for call in calls if call["url"].endswith("/mcp")]
    assert forwarded_methods == [
        "tools/list",
        "tools/call",
        "tools/list",
        "tools/list",
        "tools/call",
    ]


def test_verified_connection_stays_usable_after_one_transient_reprobe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    issued = 0

    class Response:
        def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
            self.status_code = status_code
            self._payload = payload
            self.headers: dict[str, str] = {}
            self.content = json.dumps(payload).encode()

        def json(self) -> dict[str, Any]:
            return self._payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                body = kwargs["json"]
                return Response(
                    200,
                    {
                        "id": "attachment-degraded-ready",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": "cao.csc_degraded-ready",
                        "generation": 4,
                        "connection_id": "cac_degraded-ready",
                        "connection_generation": 7,
                        "peer_binding_digest": "a" * 64,
                    },
                )
            request = kwargs["json"]
            if request["id"] == 2 and request["method"] == "tools/list":
                return Response(503, {"detail": "temporary probe transport failure"})
            verification_payload = _start_verification_payload(request)
            if request["id"] == 1 and verification_payload is not None:
                return Response(200, verification_payload)
            if request["method"] == "tools/list":
                return Response(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {"tools": conversation_server_tools()},
                    },
                )
            return Response(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {
                        "structuredContent": {"status": "accepted"},
                        "isError": False,
                    },
                },
            )

    async def issue(*args: Any, **kwargs: Any) -> str:
        nonlocal issued
        del args, kwargs
        issued += 1
        return "cao.cab_degraded-ready"

    meta = {
        PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
        CLIENT_CAPABILITIES_META_KEY: {},
        CLIENT_INFO_META_KEY: {"name": "degraded-ready-regression", "version": "1"},
    }

    def tool_call(request_id: int, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments, "_meta": meta},
        }

    requests = [
        tool_call(1, "cao_start", {"native_thread_id": "degraded-ready-thread"}),
        tool_call(2, "cao_start", {"native_thread_id": "degraded-ready-thread"}),
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {"_meta": meta}},
        tool_call(
            4,
            "cao_new_worker_thread",
            {"directory": "workspace", "idempotency_key": "degraded-new"},
        ),
        tool_call(
            5,
            "cao_finish_worker_thread",
            {
                "worker_thread_id": "mwt_exact",
                "expected_generation": 1,
                "idempotency_key": "degraded-close",
            },
        ),
    ]

    class Input:
        buffer = io.BytesIO(b"".join(json.dumps(request).encode() + b"\n" for request in requests))

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
            )
        )
        == 0
    )

    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    degraded = next(value for value in responses if value.get("id") == 2)["result"][
        "structuredContent"
    ]
    assert degraded == {
        "status": "ready",
        "attachment_verification": "previously_verified",
        "verification_status": "degraded",
        "reason_code": "catalog_verification_failed",
        "retryable": False,
        "catalog_verification": {
            "status": "degraded",
            "last_verified_mcp_catalog_digest": catalog_digest(conversation_proxy_tools()),
            "current_probe": "failed",
        },
        "attachment": {
            "id": "attachment-degraded-ready",
            "generation": 4,
            "peer_binding_digest": "a" * 64,
        },
        "connection": {
            "id": "cac_degraded-ready",
            "generation": 7,
            "peer_binding_digest": "a" * 64,
        },
    }
    for request_id in (3, 4, 5):
        assert "error" not in next(value for value in responses if value.get("id") == request_id)
    assert issued == 1
    assert (
        sum(
            call["json"]["id"] == 2 and call["json"]["method"] == "tools/list"
            for call in calls
            if call["url"].endswith("/mcp")
        )
        == 1
    )
    assert [
        call["json"].get("params", {}).get("name")
        for call in calls
        if call["url"].endswith("/mcp") and call["json"]["id"] in {4, 5}
    ] == ["cao_new_worker_thread", "cao_finish_worker_thread"]
    assert all(
        call["headers"]["Authorization"] == "Bearer cao.csc_degraded-ready"
        for call in calls
        if call["url"].endswith("/mcp") and call["json"]["id"] in {3, 4, 5}
    )


def test_degraded_readiness_rejects_unverified_or_mismatched_connection_markers() -> None:
    attachment = AttachedCAOConversation(
        attachment_id="cat_marker",
        native_thread_id="marker-thread",
        project_digest="a" * 64,
        context_bearer="cao.csc_marker",
        generation=3,
        connection_id="cac_marker",
        connection_generation=5,
    )
    verification = CAOStartVerification(catalog_digest=catalog_digest(conversation_proxy_tools()))
    marker = _verified_connection_marker(attachment, verification)

    assert marker is not None
    assert _can_preserve_degraded_connection(attachment, marker) is True
    assert _can_preserve_degraded_connection(attachment, None) is False
    assert (
        _can_preserve_degraded_connection(replace(attachment, connection_generation=6), marker)
        is False
    )
    assert (
        _can_preserve_degraded_connection(replace(attachment, connection_id="cac_other"), marker)
        is False
    )
    assert _can_preserve_degraded_connection(attachment, (marker[0], marker[1], "0" * 64)) is False


def test_successful_conversation_close_resets_proxy_and_same_thread_reattaches_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    attachment_count = 0

    class Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.status_code = 200
            self._payload = payload
            self.headers: dict[str, str] = {}
            self.content = b"{}"

        def json(self) -> dict[str, Any]:
            return self._payload

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, **kwargs: Any) -> Response:
            nonlocal attachment_count
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                attachment_count += 1
                body = kwargs["json"]
                return Response(
                    {
                        "id": "attachment-close-reuse",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": f"cao.csc_close-generation-{attachment_count}",
                    }
                )
            request = kwargs["json"]
            verification_payload = _start_verification_payload(request)
            if verification_payload is not None:
                return Response(verification_payload)
            if request.get("params", {}).get("name") == "cao_close_conversation":
                return Response(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {
                            "structuredContent": {
                                "status": "closed",
                                "scope": "conversation",
                            }
                        },
                    }
                )
            return Response(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"structuredContent": {"items": []}},
                }
            )

    issued = 0

    async def issue(*args: Any, **kwargs: Any) -> str:
        nonlocal issued
        del args, kwargs
        issued += 1
        return f"cao.cab_close-generation-{issued}"

    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {"native_thread_id": "close-reuse-thread"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "cao_close_conversation",
                "arguments": {"idempotency_key": "close-reuse"},
            },
        },
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {"native_thread_id": "close-reuse-thread"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "cao_list_managed_workers", "arguments": {}},
        },
    ]

    class Input:
        buffer = io.BytesIO(b"".join(json.dumps(request).encode() + b"\n" for request in requests))

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket="/owner/private/issuer.sock",
                pending_conversation_bridge=True,
            )
        )
        == 0
    )

    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    assert (
        next(value for value in responses if value.get("id") == 2)["result"]["structuredContent"][
            "status"
        ]
        == "closed"
    )
    pending_catalog = next(value for value in responses if value.get("id") == 3)
    pending_names = {tool["name"] for tool in pending_catalog["result"]["tools"]}
    assert pending_names == {tool["name"] for tool in conversation_proxy_tools()}
    assert (
        next(value for value in responses if value.get("id") == 4)["result"]["structuredContent"][
            "status"
        ]
        == "ready"
    )
    assert attachment_count == issued == 2
    assert [call["url"].rsplit("/", 1)[-1] for call in calls] == [
        "cao-session-attachments",
        "mcp",
        "mcp",
        "mcp",
        "cao-session-attachments",
        "mcp",
        "mcp",
        "mcp",
    ]
    assert calls[1]["headers"]["Authorization"] == ("Bearer cao.csc_close-generation-1")
    assert calls[7]["headers"]["Authorization"] == ("Bearer cao.csc_close-generation-2")


def test_bootstrap_binds_exact_context_and_renewal_rotates_csc(system) -> None:
    service = system["service"]
    bootstrap = _bootstrap(service, "thread-a", "a")
    with pytest.raises(AuthorizationError):
        service.attach_cao_session(
            bootstrap,
            current_cao_session_attachment(native_thread_id="thread-b", project_digest="b" * 64),
        )
    first = service.attach_cao_session(
        bootstrap,
        current_cao_session_attachment(native_thread_id="thread-a", project_digest="a" * 64),
    )
    with pytest.raises(AuthorizationError):
        service.attach_cao_session(
            bootstrap,
            current_cao_session_attachment(native_thread_id="thread-a", project_digest="a" * 64),
        )
    renewed = _attach(service, "thread-a", "a")
    assert renewed["id"] == first["id"]
    assert renewed["generation"] == first["generation"]
    assert renewed["peer_binding_digest"] == first["peer_binding_digest"]
    assert renewed["context_token"] != first["context_token"]
    with pytest.raises(AuthenticationError):
        service.authenticate(first["context_token"])
    renewed_actor = service.authenticate(renewed["context_token"])
    assert renewed_actor["_cao_attachment_id"] == first["id"]


@pytest.mark.parametrize("old_bridge_state", ["live", "unknown"])
def test_second_authentic_bridge_opens_an_independent_connection_without_takeover(
    system: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    old_bridge_state: str,
) -> None:
    service = system["service"]
    first_root = ProcessIdentity(pid=401, parent_pid=1, start_signature="first-root")
    first_bridge = ProcessIdentity(pid=411, parent_pid=401, start_signature="first-bridge")
    second_root = ProcessIdentity(pid=402, parent_pid=1, start_signature="second-root")
    second_bridge = ProcessIdentity(pid=412, parent_pid=402, start_signature="second-bridge")

    def initially_live(pid: int) -> ProcessIdentity:
        return {
            first_root.pid: first_root,
            first_bridge.pid: first_bridge,
            second_root.pid: second_root,
            second_bridge.pid: second_bridge,
        }[pid]

    monkeypatch.setattr("cao_control_plane.service._process_identity", initially_live)
    first_cab = service.authenticate(
        service.issue_owner_local_attachment_bootstrap(
            first_bridge,
            "rotating-thread",
            "a" * 64,
            CURRENT_CAO_CATALOG_DIGEST,
            CURRENT_CAO_PROXY_ABI_VERSION,
        )
    )
    first = service.attach_cao_session(
        first_cab,
        current_cao_session_attachment(native_thread_id="rotating-thread", project_digest="a" * 64),
    )
    second_cab = service.authenticate(
        service.issue_owner_local_attachment_bootstrap(
            second_bridge,
            "rotating-thread",
            "a" * 64,
            CURRENT_CAO_CATALOG_DIGEST,
            CURRENT_CAO_PROXY_ABI_VERSION,
        )
    )

    def current_identity(pid: int) -> ProcessIdentity:
        if pid == first_bridge.pid and old_bridge_state == "unknown":
            raise EnrollmentCapabilityError("process identity unavailable")
        return initially_live(pid)

    monkeypatch.setattr("cao_control_plane.service._process_identity", current_identity)

    def broad_process_probe(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("connection admission must not probe or kill another bridge")

    monkeypatch.setattr("cao_control_plane.service.os.kill", broad_process_probe)
    attachment_before = service.db.fetchone(
        "SELECT id, principal_id, runtime_session_id, native_thread_id, "
        "project_digest, state, generation FROM cao_session_attachments WHERE id = ?",
        (first["id"],),
    )
    assert attachment_before is not None
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
        )
    )
    first_credential_before = service.db.fetchone(
        "SELECT * FROM cao_conversation_credentials WHERE id = ?",
        (first["context_credential_id"],),
    )
    assert first_credential_before is not None

    second = service.attach_cao_session(
        second_cab,
        current_cao_session_attachment(native_thread_id="rotating-thread", project_digest="a" * 64),
    )

    assert second["id"] == first["id"]
    assert second["generation"] == first["generation"]
    assert second["connection_id"] != first["connection_id"]
    assert second["connection_generation"] == first["connection_generation"] + 1
    assert tuple(
        service.db.fetchone(
            "SELECT id, principal_id, runtime_session_id, native_thread_id, "
            "project_digest, state, generation FROM cao_session_attachments WHERE id = ?",
            (first["id"],),
        )
    ) == tuple(attachment_before)
    runtime_after = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
        )
    )
    assert str(runtime_after["lease_expires_at"]) >= str(runtime_before["lease_expires_at"])
    for volatile_field in ("lease_expires_at", "updated_at"):
        runtime_after.pop(volatile_field)
        runtime_before.pop(volatile_field)
    assert runtime_after == runtime_before
    assert dict(
        service.db.fetchone(
            "SELECT * FROM cao_conversation_credentials WHERE id = ?",
            (first["context_credential_id"],),
        )
    ) == dict(first_credential_before)
    assert (
        service.authenticate(first["context_token"])["_cao_connection_id"] == first["connection_id"]
    )
    assert (
        service.authenticate(second["context_token"])["_cao_connection_id"]
        == second["connection_id"]
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM cao_attachment_connections "
            "WHERE attachment_id = ? AND state = 'active'",
            (first["id"],),
        )["count"]
        == 2
    )


def test_catalog_rollover_fences_only_stale_connections_and_admits_a_current_bridge(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    first_root = ProcessIdentity(pid=451, parent_pid=1, start_signature="first-root")
    first_bridge = ProcessIdentity(pid=461, parent_pid=451, start_signature="first-bridge")
    second_root = ProcessIdentity(pid=452, parent_pid=1, start_signature="second-root")
    second_bridge = ProcessIdentity(pid=462, parent_pid=452, start_signature="second-bridge")
    identities = {
        identity.pid: identity
        for identity in (first_root, first_bridge, second_root, second_bridge)
    }
    monkeypatch.setattr("cao_control_plane.service._process_identity", lambda pid: identities[pid])

    catalog_before = catalog_digest(conversation_proxy_tools())
    catalog_after = "d" * 64
    service.reconcile_conversation_tool_catalog(catalog_before, "e" * 64)
    first = service.attach_cao_session(
        service.authenticate(
            service.issue_owner_local_attachment_bootstrap(
                first_bridge,
                "catalog-fenced-thread",
                "c" * 64,
                catalog_before,
                CAO_CONVERSATION_PROXY_ABI_VERSION,
            )
        ),
        current_cao_session_attachment(
            native_thread_id="catalog-fenced-thread",
            project_digest="c" * 64,
            proxy_catalog_digest=catalog_before,
            proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
        ),
    )
    logical_before = tuple(
        service.db.fetchone(
            "SELECT id, principal_id, runtime_session_id, native_thread_id, "
            "project_digest, state, generation FROM cao_session_attachments WHERE id = ?",
            (first["id"],),
        )
    )

    reconciled = service.reconcile_conversation_tool_catalog(catalog_after, "f" * 64)
    assert reconciled["revoked_credential_count"] == 1
    with pytest.raises(AuthenticationError):
        service.authenticate(first["context_token"])

    current_cab = service.authenticate(
        service.issue_owner_local_attachment_bootstrap(
            second_bridge,
            "catalog-fenced-thread",
            "c" * 64,
            catalog_after,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )
    )
    current = service.attach_cao_session(
        current_cab,
        current_cao_session_attachment(
            native_thread_id="catalog-fenced-thread",
            project_digest="c" * 64,
            proxy_catalog_digest=catalog_after,
            proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
        ),
    )
    assert current["id"] == first["id"]
    assert current["generation"] == first["generation"]
    assert current["connection_generation"] == first["connection_generation"] + 1
    assert current["peer_binding_digest"] != first["peer_binding_digest"]
    assert (
        tuple(
            service.db.fetchone(
                "SELECT id, principal_id, runtime_session_id, native_thread_id, "
                "project_digest, state, generation FROM cao_session_attachments WHERE id = ?",
                (first["id"],),
            )
        )
        == logical_before
    )
    states = {
        str(row["proxy_catalog_digest"]): str(row["state"])
        for row in service.db.fetchall(
            "SELECT proxy_catalog_digest, state FROM cao_attachment_connections "
            "WHERE attachment_id = ? ORDER BY connection_generation",
            (first["id"],),
        )
    }
    assert states == {catalog_before: "stale", catalog_after: "active"}
    assert (
        service.authenticate(current["context_token"])["_cao_connection_id"]
        == current["connection_id"]
    )


def test_bootstrap_capability_is_rejected_by_generic_transports(settings) -> None:
    app = create_app(settings)
    token = app.state.service.issue_owner_local_attachment_bootstrap(
        _process_identity(os.getpid()),
        "generic-rejection",
        "e" * 64,
        CURRENT_CAO_CATALOG_DIGEST,
        CURRENT_CAO_PROXY_ABI_VERSION,
    )
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(app, base_url="http://127.0.0.1:8768") as client:
        assert client.get("/api/v1/principals", headers=headers).status_code == 403
        assert client.get("/api/v1/runtimes", headers=headers).status_code == 403
        assert client.get("/api/v1/work", headers=headers).status_code == 403
        assert client.post("/api/v1/effects:check", headers=headers, json={}).status_code == 403
        assert (
            client.post(
                "/mcp",
                headers={
                    **headers,
                    "MCP-Protocol-Version": "2026-07-28",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            ).status_code
            == 403
        )
        assert (
            client.get(
                "/a2a/http/extendedAgentCard",
                headers={**headers, "A2A-Version": "1.0"},
            ).status_code
            == 403
        )

    with app.state.database.connection_scope() as connection:
        dump = "\n".join(connection.iterdump())
    assert token not in dump


def test_attachment_rejects_credential_shaped_thread_before_durable_write(system) -> None:
    leaked_value = "cao.cab_threadleak.durable-value"
    service = system["service"]
    with pytest.raises(AuthenticationError):
        service.issue_owner_local_attachment_bootstrap(
            _process_identity(os.getpid()),
            leaked_value,
            "a" * 64,
            CURRENT_CAO_CATALOG_DIGEST,
            CURRENT_CAO_PROXY_ABI_VERSION,
        )
    peer = _process_identity(os.getpid())
    valid_capability = service.issue_owner_local_attachment_bootstrap(
        peer,
        "credential-shape-validation",
        "a" * 64,
        CURRENT_CAO_CATALOG_DIGEST,
        CURRENT_CAO_PROXY_ABI_VERSION,
    )
    with pytest.raises(ValidationError):
        service.attach_cao_session(
            service.authenticate(valid_capability),
            current_cao_session_attachment(native_thread_id=leaked_value, project_digest="a" * 64),
        )
    with service.db.connection_scope() as connection:
        assert leaked_value not in "\n".join(connection.iterdump())


def test_direct_actor_is_revalidated_after_conversation_revocation(system) -> None:
    attachment = _attach(system["service"], "thread-stale", "c")
    actor = system["service"].authenticate(attachment["context_token"])
    work, _ = _assign_and_report(system, actor, "stale")
    system["service"].db.execute(
        "UPDATE cao_conversation_credentials SET state = 'revoked' WHERE id = ?",
        (actor["_cao_conversation_credential_id"],),
    )
    with pytest.raises(AuthorizationError):
        system["service"].get_work(work["id"], actor)


def test_no_cao_conversation_binding_comes_from_model_arguments(system) -> None:
    attachment = _attach(system["service"], "thread-sealed", "d")
    actor = system["service"].authenticate(attachment["context_token"])
    with pytest.raises(AuthorizationError):
        system["service"].assign_work(
            actor,
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Caller override",
                objective="Attempt to override the immutable conversation binding",
                acceptance=["The override is rejected"],
                runtime_session_id=system["runtime"]["id"],
                supervisor_attachment_id=attachment["id"],
                supervisor_project_digest=attachment["project_digest"],
            ),
        )
