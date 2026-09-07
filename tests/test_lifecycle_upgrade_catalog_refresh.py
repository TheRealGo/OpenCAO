from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import socket
import stat
import struct
import tempfile
import threading
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.dashboard_lifecycle import (
    DashboardLifecycleSettings,
    LifecycleUnknownOutcome,
    build_dashboard_lifecycle_plan,
    build_dashboard_upgrade_plan,
    reload_codex_mcp_server,
)
from cao_control_plane.database import APPLICATION_ID, SCHEMA_VERSION, SQLiteDatabaseIdentity
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.mcp import (
    CAO_SHOW_DASHBOARD_TOOL,
    CAO_START_TOOL,
    _merge_attached_conversation_tools,
    conversation_proxy_tools,
    conversation_server_tools,
    pending_conversation_tools,
    serve_stdio_proxy,
)
from cao_control_plane.models import WorkAssignment
from cao_control_plane.release_identity import (
    ReleaseIdentity,
    catalog_digest,
    current_release_identity,
)

_CATALOG_A = catalog_digest(conversation_proxy_tools())
_CATALOG_B = "b" * 64
_RELEASE_A = "1" * 64
_RELEASE_B = "2" * 64


def _read_exact(connection: socket.socket, size: int) -> bytes:
    value = bytearray()
    while len(value) < size:
        chunk = connection.recv(size - len(value))
        if not chunk:
            raise ConnectionError("WebSocket peer closed before the frame was complete")
        value.extend(chunk)
    return bytes(value)


def _read_websocket_json(connection: socket.socket) -> dict[str, Any]:
    first, second = _read_exact(connection, 2)
    assert first & 0x80
    assert first & 0x0F == 0x1
    assert second & 0x80
    size = second & 0x7F
    if size == 126:
        size = struct.unpack("!H", _read_exact(connection, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", _read_exact(connection, 8))[0]
    mask = _read_exact(connection, 4)
    encoded = _read_exact(connection, size)
    value = json.loads(bytes(byte ^ mask[index % 4] for index, byte in enumerate(encoded)))
    assert isinstance(value, dict)
    return value


def _write_websocket_json(connection: socket.socket, value: dict[str, Any]) -> None:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    header = bytearray((0x81,))
    if len(payload) < 126:
        header.append(len(payload))
    elif len(payload) <= 0xFFFF:
        header.append(126)
        header.extend(struct.pack("!H", len(payload)))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", len(payload)))
    connection.sendall(bytes(header) + payload)


class _OwnerAppServerDouble:
    def __init__(
        self,
        socket_path: Path,
        *,
        close_after_reload: bool = False,
        omit_reload_result: bool = False,
    ) -> None:
        self.socket_path = socket_path
        self.close_after_reload = close_after_reload
        self.omit_reload_result = omit_reload_result
        self.messages: list[dict[str, Any]] = []
        self.error: BaseException | None = None
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(socket_path))
        self.listener.listen(1)
        self.listener.settimeout(6.0)
        socket_path.chmod(0o600)
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self) -> _OwnerAppServerDouble:
        self.thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.thread.join(timeout=7.0)
        self.listener.close()
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error

    def _serve(self) -> None:
        try:
            connection, _address = self.listener.accept()
            with connection:
                connection.settimeout(6.0)
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    request.extend(connection.recv(4096))
                    if len(request) > 64 * 1024:
                        raise ValueError("WebSocket upgrade request exceeded its bound")
                headers = request.decode("ascii").split("\r\n")
                assert headers[0] == "GET / HTTP/1.1"
                values = {
                    name.strip().lower(): value.strip()
                    for line in headers[1:]
                    if ":" in line
                    for name, value in [line.split(":", 1)]
                }
                key = values["sec-websocket-key"]
                accept = base64.b64encode(
                    hashlib.sha1(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                    ).digest()
                ).decode("ascii")
                connection.sendall(
                    (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\n"
                        "Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                    ).encode("ascii")
                )
                initialize = _read_websocket_json(connection)
                self.messages.append(initialize)
                _write_websocket_json(
                    connection,
                    {
                        "id": initialize["id"],
                        "result": {
                            "userAgent": "codex-app-server-test/1",
                            "codexHome": "/owner/codex",
                            "platformFamily": "unix",
                            "platformOs": "macos",
                        },
                    },
                )
                self.messages.append(_read_websocket_json(connection))
                reload_request = _read_websocket_json(connection)
                self.messages.append(reload_request)
                if not self.close_after_reload:
                    _write_websocket_json(
                        connection,
                        (
                            {"id": reload_request["id"]}
                            if self.omit_reload_result
                            else {"id": reload_request["id"], "result": {}}
                        ),
                    )
        except BaseException as error:
            self.error = error


def _attached_work(system: dict[str, Any], *, suffix: str) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"lifecycle-upgrade-{suffix}",
            project_digest="d" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Lifecycle upgrade {suffix}",
            objective="Preserve durable conversation work across a release boundary",
            acceptance=["Only a stale conversation credential may be fenced"],
            idempotency_key=f"lifecycle-upgrade-{suffix}",
        ),
    )
    return attachment, work


def _durable_rows(
    system: dict[str, Any], attachment_id: str, work_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    attachment = service.db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?", (attachment_id,)
    )
    work = service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work_id,))
    assert attachment is not None
    assert work is not None
    return dict(attachment), dict(work)


def test_same_catalog_preserves_active_csc_and_durable_conversation_state(system) -> None:
    service = system["service"]
    attachment, work = _attached_work(system, suffix="same-catalog")
    actor = service.authenticate(attachment["context_token"])
    before = _durable_rows(system, attachment["id"], work["id"])

    result = service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_B)

    assert result == {
        "changed": False,
        "catalog_digest": _CATALOG_A,
        "revoked_credential_count": 0,
    }
    renewed_actor = service.authenticate(attachment["context_token"])
    assert (
        renewed_actor["_cao_conversation_credential_id"] == actor["_cao_conversation_credential_id"]
    )
    credential = service.db.fetchone(
        "SELECT state, revoked_at FROM cao_conversation_credentials WHERE id = ?",
        (actor["_cao_conversation_credential_id"],),
    )
    assert credential is not None
    assert dict(credential) == {"state": "active", "revoked_at": None}
    assert _durable_rows(system, attachment["id"], work["id"]) == before


def test_changed_catalog_revokes_only_csc_and_preserves_attachment_work_and_generation(
    system,
) -> None:
    service = system["service"]
    attachment, work = _attached_work(system, suffix="changed-catalog")
    actor = service.authenticate(attachment["context_token"])
    before = _durable_rows(system, attachment["id"], work["id"])

    result = service.reconcile_conversation_tool_catalog(_CATALOG_B, _RELEASE_B)

    assert result == {
        "changed": True,
        "catalog_digest": _CATALOG_B,
        "revoked_credential_count": 1,
    }
    with pytest.raises(AuthenticationError):
        service.authenticate(attachment["context_token"])
    credential = service.db.fetchone(
        "SELECT state, revoked_at FROM cao_conversation_credentials WHERE id = ?",
        (actor["_cao_conversation_credential_id"],),
    )
    assert credential is not None
    assert credential["state"] == "revoked"
    assert credential["revoked_at"] is not None
    after = _durable_rows(system, attachment["id"], work["id"])
    assert after == before
    assert after[0]["generation"] == attachment["generation"]
    assert after[1]["generation"] == work["generation"]


def test_release_identity_and_health_fields_are_exact_and_deterministic(settings) -> None:
    first = current_release_identity()
    second = current_release_identity()
    tools = conversation_proxy_tools()
    remote_names = {tool["name"] for tool in conversation_server_tools()}

    assert second == first
    assert [tool["name"] for tool in tools] == sorted(tool["name"] for tool in tools)
    assert pending_conversation_tools() == tools
    assert remote_names < {tool["name"] for tool in tools}
    assert {"cao_start", "cao_show_dashboard", "cao_close_conversation"} <= {
        tool["name"] for tool in tools
    }
    assert catalog_digest(tools) == first.mcp_catalog_digest
    assert len(first.release_id) == 64
    assert len(first.mcp_catalog_digest) == 64

    app = create_app(settings)
    with TestClient(app, base_url="http://127.0.0.1:8768") as client:
        health = client.get("/health")
        ready = client.get("/ready")

    expected = {
        "release_id": first.release_id,
        "schema_version": first.schema_version,
        "mcp_catalog_digest": first.mcp_catalog_digest,
    }
    assert health.status_code == 200
    assert {key: health.json()[key] for key in expected} == expected
    assert ready.status_code == 200
    assert {key: ready.json()[key] for key in expected} == expected
    assert app.state.release_identity == first


def test_stale_proxy_401_reenters_pending_and_reattaches_to_upgraded_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    attachment_count = 0
    upgraded_tools = conversation_server_tools()

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
            nonlocal attachment_count
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                attachment_count += 1
                body = kwargs["json"]
                return Response(
                    200,
                    {
                        "id": "stale-proxy-attachment",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": f"cao.csc_proxy-generation-{attachment_count}",
                    },
                )
            request = kwargs["json"]
            params = request.get("params", {})
            name = params.get("name") if isinstance(params, dict) else None
            if name == "cao_query" and attachment_count == 1:
                return Response(401, {"detail": "Unauthorized"})
            if request.get("method") == "tools/call":
                assert name == "cao_list_managed_workers"
                return Response(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {
                            "structuredContent": {"workers": []},
                            "isError": False,
                        },
                    },
                )
            return Response(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"tools": upgraded_tools},
                },
            )

    async def issue(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return f"cao.cab_generation-{attachment_count + 1}"

    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {"native_thread_id": "stale-proxy-thread"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "cao_query", "arguments": {"resource": "work"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "cao_list_managed_workers", "arguments": {}},
        },
        {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {"native_thread_id": "stale-proxy-thread"},
            },
        },
        {"jsonrpc": "2.0", "id": 6, "method": "tools/list", "params": {}},
    ]

    class Input:
        buffer = io.BytesIO(
            b"".join(json.dumps(request).encode("utf-8") + b"\n" for request in requests)
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

    # The stale CSC is not retried. The same live stdio bridge returns to
    # locally fenced pending dispatch while keeping the stable release catalog
    # visible, then reattaches once and uses the upgraded daemon's exact remote
    # schema plus only the two stable bridge-local tools.
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
    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    expired = next(value for value in responses if value.get("id") == 2)
    pending = next(value for value in responses if value.get("id") == 3)
    listed = next(value for value in responses if value.get("id") == 4)
    upgraded = next(value for value in responses if value.get("id") == 6)
    assert expired["error"]["message"] == (
        "CAO conversation attachment expired; call cao_start again."
    )
    assert pending["error"]["message"] == (
        "CAO conversation is not attached; call cao_start first."
    )
    assert {tool["name"] for tool in listed["result"]["tools"]} == {
        tool["name"] for tool in conversation_proxy_tools()
    }
    upgraded_by_name = {tool["name"]: tool for tool in upgraded["result"]["tools"]}
    assert set(upgraded_by_name) == {tool["name"] for tool in conversation_proxy_tools()}
    assert upgraded_by_name["cao_start"] == CAO_START_TOOL
    assert upgraded_by_name["cao_show_dashboard"] == CAO_SHOW_DASHBOARD_TOOL
    assert not any(
        response.get("method") == "notifications/tools/list_changed" for response in responses
    )
    assert calls[1]["headers"]["Authorization"] == "Bearer cao.csc_proxy-generation-1"
    assert calls[7]["headers"]["Authorization"] == "Bearer cao.csc_proxy-generation-2"
    assert "cao.cab_generation-" not in output.getvalue()
    assert "cao.csc_proxy-generation-" not in output.getvalue()


def test_pending_catalog_is_visible_but_mismatched_daemon_catalog_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    daemon_tools = [
        {
            "name": "cao_upgraded_daemon_tool",
            "description": "A schema supplied only by the upgraded daemon.",
            "inputSchema": {
                "type": "object",
                "properties": {"daemon_revision": {"type": "integer", "const": 2}},
                "required": ["daemon_revision"],
                "additionalProperties": False,
            },
        }
    ]

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
            calls.append({"url": url, **kwargs})
            if url.endswith("/api/v1/cao-session-attachments"):
                body = kwargs["json"]
                return Response(
                    {
                        "id": "stable-catalog-attachment",
                        "native_thread_id": body["native_thread_id"],
                        "project_digest": body["project_digest"],
                        "context_token": "cao.csc_stable-catalog-token",
                    }
                )
            request = kwargs["json"]
            return Response(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"tools": daemon_tools},
                }
            )

    async def issue(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return "cao.cab_stable-catalog-bootstrap"

    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "cao_close_conversation",
                "arguments": {"idempotency_key": "blocked-before-attachment"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "cao_start",
                "arguments": {"native_thread_id": "stable-catalog-thread"},
            },
        },
        {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
    ]

    class Input:
        buffer = io.BytesIO(
            b"".join(json.dumps(request).encode("utf-8") + b"\n" for request in requests)
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
    pending = next(value for value in responses if value.get("id") == 1)
    blocked = next(value for value in responses if value.get("id") == 2)
    stopped = next(value for value in responses if value.get("id") == 3)
    still_pending = next(value for value in responses if value.get("id") == 4)
    expected_visible_names = {tool["name"] for tool in conversation_proxy_tools()}
    assert {tool["name"] for tool in pending["result"]["tools"]} == (expected_visible_names)
    assert stopped["result"]["structuredContent"] == {
        "status": "stopped",
        "reason_code": "attachment_catalog_refresh_required",
        "retryable": False,
        "recovery_action": "refresh_mcp",
    }
    assert {tool["name"] for tool in still_pending["result"]["tools"]} == (expected_visible_names)
    assert blocked["error"]["message"] == (
        "CAO conversation is not attached; call cao_start first."
    )
    assert not any(
        response.get("method") == "notifications/tools/list_changed" for response in responses
    )
    assert [call["url"].rsplit("/", 1)[-1] for call in calls] == [
        "cao-session-attachments",
        "mcp",
    ]
    assert calls[-1]["headers"]["Authorization"] == ("Bearer cao.csc_stable-catalog-token")


@pytest.mark.parametrize(
    "remote_tools",
    (
        [CAO_START_TOOL],
        [CAO_SHOW_DASHBOARD_TOOL],
        [
            {"name": "cao_duplicate", "inputSchema": {"type": "object"}},
            {"name": "cao_duplicate", "inputSchema": {"type": "object"}},
        ],
        [{"description": "missing daemon tool name"}],
    ),
)
def test_attached_catalog_merge_rejects_collisions_and_duplicate_names(
    remote_tools: list[dict[str, Any]],
) -> None:
    response = {
        "jsonrpc": "2.0",
        "id": 91,
        "result": {"tools": remote_tools, "daemonField": "preserve-only-on-success"},
    }

    merged = _merge_attached_conversation_tools(response)

    assert merged == {
        "jsonrpc": "2.0",
        "id": 91,
        "error": {
            "code": -32603,
            "message": "Attached CAO tool catalog is invalid.",
        },
    }
    assert "preserve-only-on-success" not in json.dumps(merged)


def test_attached_catalog_merge_preserves_daemon_schema_authority() -> None:
    remote_tool = {
        "name": "cao_daemon_schema_probe",
        "description": "Exact daemon-owned schema sentinel.",
        "inputSchema": {
            "type": "object",
            "properties": {"daemon_revision": {"type": "integer", "const": 7}},
            "required": ["daemon_revision"],
            "additionalProperties": False,
        },
    }
    response = {
        "jsonrpc": "2.0",
        "id": 92,
        "result": {"tools": [remote_tool], "daemonField": "preserved"},
    }

    merged = _merge_attached_conversation_tools(response)

    assert merged["result"]["daemonField"] == "preserved"
    merged_by_name = {tool["name"]: tool for tool in merged["result"]["tools"]}
    assert merged_by_name["cao_daemon_schema_probe"] == remote_tool
    assert merged_by_name["cao_start"] == CAO_START_TOOL
    assert merged_by_name["cao_show_dashboard"] == CAO_SHOW_DASHBOARD_TOOL


def test_codex_reload_uses_exact_websocket_initialize_and_reload_contract() -> None:
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="cao-ws-", dir=temporary_root) as directory:
        owner_directory = Path(directory)
        owner_directory.chmod(0o700)
        socket_path = owner_directory / "control.sock"

        with _OwnerAppServerDouble(socket_path) as app_server:
            reload_codex_mcp_server(socket_path, _RELEASE_A)

    assert app_server.messages == [
        {
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "codex_app_server_daemon",
                    "version": _RELEASE_A,
                },
                "capabilities": {"experimentalApi": True},
            },
        },
        {"method": "initialized", "params": {}},
        {"id": 2, "method": "config/mcpServer/reload"},
    ]


def test_codex_reload_after_send_failure_has_unknown_outcome() -> None:
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="cao-ws-", dir=temporary_root) as directory:
        owner_directory = Path(directory)
        owner_directory.chmod(0o700)
        socket_path = owner_directory / "control.sock"

        with (
            _OwnerAppServerDouble(socket_path, close_after_reload=True) as app_server,
            pytest.raises(LifecycleUnknownOutcome, match="reload outcome is unknown"),
        ):
            reload_codex_mcp_server(socket_path, _RELEASE_A)

    assert app_server.messages[-1] == {
        "id": 2,
        "method": "config/mcpServer/reload",
    }


def test_codex_reload_rejects_a_matching_response_without_result() -> None:
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="cao-ws-", dir=temporary_root) as directory:
        owner_directory = Path(directory)
        owner_directory.chmod(0o700)
        socket_path = owner_directory / "control.sock"

        with (
            _OwnerAppServerDouble(socket_path, omit_reload_result=True) as app_server,
            pytest.raises(LifecycleUnknownOutcome, match="reload outcome is unknown"),
        ):
            reload_codex_mcp_server(socket_path, _RELEASE_A)

    assert app_server.messages[-1] == {
        "id": 2,
        "method": "config/mcpServer/reload",
    }


def test_codex_reload_requires_real_owner_only_socket_parent() -> None:
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="cao-ws-", dir=temporary_root) as directory:
        non_owner_only = Path(directory)
        non_owner_only.chmod(0o755)
        socket_path = non_owner_only / "control.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(socket_path))
            socket_path.chmod(0o600)
            assert stat.S_IMODE(non_owner_only.stat().st_mode) == 0o755
            with pytest.raises(ValueError, match="real owner-only 0700 directory"):
                reload_codex_mcp_server(socket_path, _RELEASE_A)
        finally:
            listener.close()


def test_upgrade_plan_records_global_next_turn_codex_reload_semantics(tmp_path: Path) -> None:
    target = ReleaseIdentity(
        release_id=_RELEASE_A,
        schema_version=SCHEMA_VERSION,
        mcp_catalog_digest=_CATALOG_A,
    )
    source = SQLiteDatabaseIdentity(
        application_id=APPLICATION_ID,
        user_version=SCHEMA_VERSION,
        schema_version=SCHEMA_VERSION,
        integrity=("ok",),
        foreign_key_error_count=0,
    )
    lifecycle = build_dashboard_lifecycle_plan(
        DashboardLifecycleSettings(
            application_support_dir=tmp_path / "dashboard",
            working_directory=tmp_path,
            control_plane_command=("cao-a2a",),
            credentials_file=tmp_path / "credentials.json",
            bootstrap_record_dir=tmp_path / "bootstrap",
            session_record_dir=tmp_path / "sessions",
        )
    )
    plan = build_dashboard_upgrade_plan(
        lifecycle,
        database_path=tmp_path / "control-plane.sqlite3",
        backup_destination=tmp_path / "backup.sqlite3",
        credentials_file=tmp_path / "credentials.json",
        source_identity=source,
        target_identity=target,
        codex_app_server_socket=tmp_path / "app-server-control" / "control.sock",
    )

    assert plan.codex_mcp_reload_scope == "all_loaded_codex_threads"
    assert plan.codex_mcp_reload_application == "next_active_turn"
