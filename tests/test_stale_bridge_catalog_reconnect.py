from __future__ import annotations

import asyncio
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest
from conftest import current_cao_session_attachment
from test_worker_thread_lifecycle_core import _prepare_runtime_recovery

from cao_control_plane.attachment_issuer import (
    AttachmentCapabilityIssuer,
)
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.dashboard_lifecycle import (
    build_codex_mcp_refresh_plan,
    refresh_codex_mcp,
)
from cao_control_plane.database import utc_after, utc_now
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    MCPServer,
    cao_conversation_context_for_thread,
    conversation_proxy_tools,
    serve_stdio_proxy,
)
from cao_control_plane.models import CAOSessionAttachment
from cao_control_plane.release_identity import current_release_identity
from cao_control_plane.service import _process_identity

_NATIVE_THREAD_ID = "stale-bridge-catalog-reconnect"
_CATALOG_BEFORE = "1" * 64
_RELEASE_BEFORE = "3" * 64


def _modern_request(
    request_id: int,
    method: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {
            **params,
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {
                    "name": "stale-bridge-catalog-reconnect-test",
                    "version": "1",
                },
            },
        },
    }


def _tool_call(request_id: int, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return _modern_request(
        request_id,
        "tools/call",
        {"name": name, "arguments": arguments},
    )


def _durable_authority_snapshot(system: dict[str, Any]) -> dict[str, tuple[tuple[Any, ...], ...]]:
    service = system["service"]
    tables = (
        "principals",
        "runtime_sessions",
        "cao_session_attachments",
        "cao_attachment_bootstrap_credentials",
        "cao_conversation_credentials",
        "managed_worker_specs",
        "managed_worker_threads",
        "work_items",
        "message_deliveries",
        "effect_operations",
        "events",
    )
    return {
        table: tuple(
            tuple(row) for row in service.db.fetchall(f'SELECT * FROM "{table}" ORDER BY rowid')
        )
        for table in tables
    }


def test_catalog_refresh_requires_same_conversation_start_before_delete(
    system: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = system["service"]
    mcp = MCPServer(service)
    context = cao_conversation_context_for_thread(_NATIVE_THREAD_ID)
    service.reconcile_conversation_tool_catalog(_CATALOG_BEFORE, _RELEASE_BEFORE)

    old_bridge = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        old_bridge_identity = _process_identity(old_bridge.pid)
        bootstrap = service.issue_owner_local_attachment_bootstrap(
            old_bridge_identity,
            context.native_thread_id,
            context.project_digest,
            _CATALOG_BEFORE,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )
        old_attachment = service.attach_cao_session(
            service.authenticate(bootstrap),
            current_cao_session_attachment(
                native_thread_id=context.native_thread_id,
                project_digest=context.project_digest,
                proxy_catalog_digest=_CATALOG_BEFORE,
            ),
        )
    finally:
        old_bridge.terminate()
        old_bridge.wait(timeout=10)

    old_actor = service.authenticate(old_attachment["context_token"])
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        old_actor,
        ordinal=2201,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    assert boundary["recovery_action"] == "system_reconciliation"
    assert service.get_work(failed["id"])["state"] == "waiting_supervisor"

    assignment = service.db.fetchone(
        """
        SELECT delivery.message_id, delivery.recipient_id
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (failed["current_attempt"]["id"],),
    )
    assert assignment is not None
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'dispatched', owner_token = 'durable-unknown-owner',
                lease_until = ?, last_error = 'outcome_unknown', updated_at = ?
            WHERE message_id = ? AND recipient_id = ?
            """,
            (
                utc_after(300),
                now,
                assignment["message_id"],
                assignment["recipient_id"],
            ),
        )
        connection.execute(
            """
            INSERT INTO effect_operations(
                id, principal_id, kind, target, action, status, evidence,
                cleanup_work_item_id, created_at, updated_at
            ) VALUES(
                'eff_stale_bridge_unknown', ?, 'external', 'opaque-target',
                'opaque-action', 'unknown', '', ?, ?, ?
            )
            """,
            (ids["principal_id"], failed["id"], now, now),
        )

    dispatched_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (assignment["message_id"], assignment["recipient_id"]),
        )
    )
    effect_before = dict(
        service.db.fetchone("SELECT * FROM effect_operations WHERE id = 'eff_stale_bridge_unknown'")
    )
    attachment_while_unreachable = service.db.fetchone(
        "SELECT state, generation, peer_pid, peer_start_signature "
        "FROM cao_attachment_connections WHERE id = ?",
        (old_attachment["connection_id"],),
    )
    assert attachment_while_unreachable is not None
    assert dict(attachment_while_unreachable) == {
        "state": "active",
        "generation": old_attachment["generation"],
        "peer_pid": old_bridge_identity.pid,
        "peer_start_signature": old_bridge_identity.start_signature,
    }
    assert old_bridge.poll() is not None

    target_identity = current_release_identity()
    reload_calls: list[tuple[Path, str]] = []
    with tempfile.TemporaryDirectory(
        prefix="cao-r-",
        dir=Path(tempfile.gettempdir()).resolve(),
    ) as directory:
        codex_control = Path(directory)
        codex_control.chmod(0o700)
        codex_socket = codex_control / "app.sock"
        socket_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        socket_server.bind(os.fspath(codex_socket))
        codex_socket.chmod(0o600)
        try:
            refresh_plan = build_codex_mcp_refresh_plan(
                codex_app_server_socket=codex_socket,
                target_identity=target_identity,
            )
            refresh_result = refresh_codex_mcp(
                refresh_plan,
                reload_codex_mcp=lambda path, release_id: reload_calls.append((path, release_id)),
                release_identity=lambda: target_identity,
                execute=True,
                owner_confirmed=True,
            )
        finally:
            socket_server.close()

        assert reload_calls == [(codex_socket, target_identity.release_id)]
    assert refresh_result.status == "submitted_for_next_active_turn"
    assert refresh_result.codex_mcp_reload == "submitted_for_next_active_turn"
    assert refresh_result.current_conversation_verification == "pending"
    assert refresh_result.status not in {"ready", "success", "verified"}

    refreshed = service.reconcile_conversation_tool_catalog(
        target_identity.mcp_catalog_digest,
        target_identity.release_id,
    )
    assert refreshed == {
        "changed": True,
        "catalog_digest": target_identity.mcp_catalog_digest,
        "revoked_credential_count": 1,
    }
    with pytest.raises(AuthenticationError):
        service.authenticate(old_attachment["context_token"])

    current_identity = _process_identity(os.getpid())
    fresh_bootstrap = service.issue_owner_local_attachment_bootstrap(
        current_identity,
        context.native_thread_id,
        context.project_digest,
        target_identity.mcp_catalog_digest,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    captured_attachment: dict[str, Any] = {}
    http_calls: list[tuple[str, str]] = []

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
            request = kwargs["json"]
            params = request.get("params", {}) if isinstance(request, dict) else {}
            name = params.get("name", "") if isinstance(params, dict) else ""
            http_calls.append((url.rsplit("/", 1)[-1], str(name)))
            authorization = str(kwargs["headers"]["Authorization"])
            bearer = authorization.removeprefix("Bearer ")
            try:
                actor = service.authenticate(bearer)
            except AuthenticationError:
                return Response(401, {"detail": "Unauthorized"})
            if url.endswith("/api/v1/cao-session-attachments"):
                value = service.attach_cao_session(
                    actor,
                    CAOSessionAttachment.model_validate(kwargs["json"]),
                )
                captured_attachment.update(value)
                return Response(200, value)
            response = mcp.handle_modern(actor, kwargs["json"])
            assert response is not None
            return Response(200, response)

    issuer_calls = 0

    async def issue_once(*args: Any, **kwargs: Any) -> str:
        nonlocal issuer_calls
        del args, kwargs
        issuer_calls += 1
        return fresh_bootstrap

    thread_row = service.db.fetchone(
        "SELECT generation FROM managed_worker_threads WHERE id = ?",
        (ids["thread_id"],),
    )
    assert thread_row is not None
    delete_arguments = {
        "worker_thread_id": ids["thread_id"],
        "expected_generation": int(thread_row["generation"]),
        "idempotency_key": "stale-bridge-explicit-delete",
    }
    requests = [
        _modern_request(1, "tools/list", {}),
        _tool_call(2, "cao_delete_worker_thread", delete_arguments),
        _tool_call(3, "cao_start", {"native_thread_id": context.native_thread_id}),
        _tool_call(4, "cao_list_managed_workers", {}),
        _tool_call(5, "cao_delete_worker_thread", delete_arguments),
    ]

    class Input:
        buffer = io.BytesIO(
            b"".join(json.dumps(request).encode("utf-8") + b"\n" for request in requests)
        )

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_attachment_bootstrap", issue_once)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket=tmp_path / "attachment-issuer.sock",
                pending_conversation_bridge=True,
            )
        )
        == 0
    )

    responses = {
        int(value["id"]): value
        for line in output.getvalue().splitlines()
        if (value := json.loads(line)).get("id") is not None
    }
    initial_names = {tool["name"] for tool in responses[1]["result"]["tools"]}
    assert initial_names == {tool["name"] for tool in conversation_proxy_tools()}
    assert {"cao_start", "cao_list_managed_workers", "cao_delete_worker_thread"} <= (initial_names)
    assert responses[2]["error"]["message"] == (
        "CAO conversation is not attached; call cao_start first."
    )
    started = responses[3]["result"]["structuredContent"]
    assert started["status"] == "ready"
    assert started["attachment_verification"] == "verified"
    assert started["catalog_verification"] == {
        "status": "verified",
        "mcp_catalog_digest": target_identity.mcp_catalog_digest,
    }

    listed = responses[4]["result"]["structuredContent"]
    listed_worker = next(
        worker for worker in listed["workers"] if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert listed_worker["state"] == "active"
    assert responses[5]["result"]["structuredContent"] == {
        "worker_thread_id": ids["thread_id"],
        "state": "deleted",
        "generation": int(thread_row["generation"]) + 2,
    }
    assert http_calls == [
        ("cao-session-attachments", ""),
        ("mcp", ""),
        ("mcp", "cao_list_managed_workers"),
        ("mcp", "cao_list_managed_workers"),
        ("mcp", "cao_delete_worker_thread"),
    ]
    assert issuer_calls == 1

    fresh_actor = service.authenticate(captured_attachment["context_token"])
    assert fresh_actor["_cao_attachment_id"] == old_attachment["id"]
    assert fresh_actor["_cao_attachment_generation"] == old_attachment["generation"]
    assert (
        fresh_actor["_cao_conversation_credential_id"]
        != old_actor["_cao_conversation_credential_id"]
    )
    current_attachment = service.db.fetchone(
        "SELECT state, generation FROM cao_session_attachments WHERE id = ?",
        (captured_attachment["id"],),
    )
    assert current_attachment is not None
    assert dict(current_attachment) == {
        "state": "active",
        "generation": old_attachment["generation"],
    }
    current_connection = service.db.fetchone(
        "SELECT state, generation, peer_pid, peer_start_signature "
        "FROM cao_attachment_connections WHERE id = ?",
        (captured_attachment["connection_id"],),
    )
    assert current_connection is not None
    assert dict(current_connection) == {
        "state": "active",
        "generation": old_attachment["generation"],
        "peer_pid": current_identity.pid,
        "peer_start_signature": current_identity.start_signature,
    }
    assert service.get_work(failed["id"])["state"] == "canceled"
    assert service.get_work(failed["id"])["open_boundaries"] == []
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                (assignment["message_id"], assignment["recipient_id"]),
            )
        )
        == dispatched_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM effect_operations WHERE id = 'eff_stale_bridge_unknown'"
            )
        )
        == effect_before
    )


def test_foreign_uid_attachment_rejection_returns_one_bounded_stopped_result(
    system: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _durable_authority_snapshot(system)

    def reject_issue(*_args: Any) -> str:
        raise AssertionError("a foreign-UID peer issued a capability")

    monkeypatch.setattr(
        "cao_control_plane.attachment_issuer._peer_uid", lambda _peer: os.geteuid() + 1
    )

    issuer = AttachmentCapabilityIssuer(
        tmp_path / "issuer-state",
        reject_issue,
    )

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("a rejected attachment reached loopback HTTP")

    request = _tool_call(
        1,
        "cao_start",
        {"native_thread_id": "fixed-peer-admission-rejection"},
    )

    class Input:
        buffer = io.BytesIO(json.dumps(request).encode("utf-8") + b"\n")

    output = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", Client)
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", Input())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", output)

    async def run_rejected_start() -> int:
        await issuer.start()
        try:
            return await serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                cao_attachment_issuer_socket=issuer.path,
                pending_conversation_bridge=True,
            )
        finally:
            await issuer.close()

    assert asyncio.run(run_rejected_start()) == 0

    responses = [
        value
        for line in output.getvalue().splitlines()
        if (value := json.loads(line)).get("id") is not None
    ]
    assert len(responses) == 1
    response = responses[0]
    assert "error" not in response
    assert response["result"]["isError"] is False
    assert response["result"]["structuredContent"] == {
        "status": "stopped",
        "reason_code": "attachment_peer_unavailable",
        "retryable": False,
    }
    assert "fixed-peer-admission-rejection" not in output.getvalue()
    assert _durable_authority_snapshot(system) == before
