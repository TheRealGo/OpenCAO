from __future__ import annotations

import asyncio
import io
import json
import threading
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from cao_control_plane import cli
from cao_control_plane.api import create_app
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    serve_stdio_proxy,
)
from cao_control_plane.models import PrincipalCreate, RuntimeRegistration


def _modern(method: str, *, request_id: int = 1) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "pytest", "version": "1"},
            }
        },
    }


def _modern_headers(token: str, method: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MCP_LATEST_VERSION,
        "Mcp-Method": method,
    }


def _managed_runtime(app) -> tuple[dict[str, Any], str]:
    service = app.state.service
    cao = service.authenticate(app.state.bootstrap["tokens"]["cao"]["token"])
    worker = service.create_principal(cao, PrincipalCreate(name="enrolled-worker", role="worker"))
    runtime = service.register_runtime(
        cao,
        worker["principal"]["id"],
        RuntimeRegistration(adapter="claude", endpoint="http://127.0.0.1:8768/mcp"),
    )
    ticket = service.issue_runtime_launch_ticket(runtime["id"])["ticket"]
    return runtime, ticket


def test_ticket_exchange_is_one_shot_and_me_hides_runtime_auth_fields(settings):
    app = create_app(settings)
    _, ticket = _managed_runtime(app)
    credential = app.state.service.exchange_runtime_launch_ticket(ticket)
    client = TestClient(app, base_url="http://localhost")
    try:
        assert credential["token"] != ticket

        me = client.get("/api/v1/me", headers={"Authorization": f"Bearer {credential['token']}"})
        assert me.status_code == 200
        assert me.json()["role"] == "worker"
        assert not {key for key in me.json() if key.startswith("_")}

        with pytest.raises(AuthenticationError):
            app.state.service.exchange_runtime_launch_ticket(ticket)
        with pytest.raises(AuthenticationError):
            app.state.service.exchange_runtime_launch_ticket("cao.ent_missing.wrong")
        removed = client.post("/api/v1/runtime-enrollment:exchange", json={"ticket": ticket})
        assert removed.status_code == 404
        assert ticket not in removed.text
    finally:
        client.close()


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"ticket": {"nested": ["cao.ent_nested.secret-value"]}},
    ],
)
def test_removed_http_ticket_exchange_never_reflects_input(settings, body):
    app = create_app(settings)
    client = TestClient(app, base_url="http://localhost")
    try:
        response = client.post("/api/v1/runtime-enrollment:exchange", json=body)
        assert response.status_code == 404
        assert "cao.ent_nested.secret-value" not in response.text
        assert '"input"' not in response.text
    finally:
        client.close()


def test_non_secret_request_validation_retains_schema_details_without_input_echo(settings):
    app = create_app(settings)
    client = TestClient(app, base_url="http://localhost")
    try:
        response = client.post(
            "/api/v1/principals",
            headers={"Authorization": f"Bearer {app.state.bootstrap['tokens']['cao']['token']}"},
            json={"name": ["cao.ent_nested.secret-value"], "role": "worker"},
        )
        assert response.status_code == 422
        details = response.json()["error"]["details"]
        assert details == [
            {
                "msg": "Input should be a valid string",
                "type": "string_type",
                "loc": ["body"],
            }
        ]
        assert "cao.ent_nested.secret-value" not in response.text
        assert '"input"' not in response.text
    finally:
        client.close()


def test_tools_list_records_worker_discovery_for_canonical_http(settings):
    app = create_app(settings)
    runtime, ticket = _managed_runtime(app)
    client = TestClient(app, base_url="http://localhost")
    try:
        credential = app.state.service.exchange_runtime_launch_ticket(ticket)
        token = credential["token"]
        modern = client.post(
            "/mcp",
            headers=_modern_headers(token, "tools/list"),
            json=_modern("tools/list"),
        )
        assert modern.status_code == 200
        heartbeat_schema = next(
            tool["inputSchema"]
            for tool in modern.json()["result"]["tools"]
            if tool["name"] == "cao_runtime_heartbeat"
        )
        assert {"expected_enrollment_generation", "sequence"} <= set(heartbeat_schema["properties"])
        enrollment = app.state.service.get_runtime(runtime["id"])["enrollment"]
        assert enrollment["protocol_version"] == MCP_LATEST_VERSION
        assert enrollment["discovered_tools_digest"]
    finally:
        client.close()


class _Response:
    def __init__(self, body: dict[str, Any], *, headers: dict[str, str] | None = None) -> None:
        self.status_code = 200
        self._body = body
        self.content = json.dumps(body).encode()
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._body


class _Client:
    calls: list[tuple[str, dict[str, str] | None, dict[str, Any]]]

    def __init__(self, **_: Any) -> None:
        self.calls = _Client.calls

    async def __aenter__(self) -> _Client:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def post(self, url: str, **kwargs: Any) -> _Response:
        headers = kwargs.get("headers")
        body = kwargs["json"]
        self.calls.append((url, headers, body))
        if body["method"] == "tools/list":
            return _Response({"jsonrpc": "2.0", "id": body["id"], "result": {"tools": []}})
        if (
            body["method"] == "tools/call"
            and body.get("params", {}).get("name") == "cao_runtime_heartbeat"
        ):
            sequence = body["params"]["arguments"]["sequence"]
            return _Response(
                {
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "result": {
                        "structuredContent": {"enrollment": {"heartbeat_sequence": sequence}}
                    },
                }
            )
        return _Response({"jsonrpc": "2.0", "id": body["id"], "result": {"ok": True}})


def test_stdio_proxy_receives_broker_credential_and_heartbeats_after_tools_list(
    monkeypatch, tmp_path: Path
):
    broker_path = tmp_path / "broker.sock"

    async def receive(*args: Any, **kwargs: Any) -> dict[str, Any]:
        assert args == (broker_path,)
        assert kwargs["timeout_seconds"] == 120.0
        return {
            "token": "runtime-bearer",
            "runtime_id": "run_1",
            "generation": 4,
            "heartbeat_lease_seconds": 3600,
        }

    _Client.calls = []
    stdin = io.BytesIO((json.dumps(_modern("tools/list")) + "\n").encode())
    stdout = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", type("In", (), {"buffer": stdin})())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", stdout)
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", _Client)
    monkeypatch.setattr("cao_control_plane.mcp.receive_enrollment_capability", receive)

    assert (
        asyncio.run(
            serve_stdio_proxy("http://127.0.0.1:8768/mcp", enrollment_broker_socket=broker_path)
        )
        == 0
    )
    assert "runtime-bearer" not in stdout.getvalue()
    assert [call[0] for call in _Client.calls] == [
        "http://127.0.0.1:8768/mcp",
        "http://127.0.0.1:8768/mcp",
    ]
    heartbeat = _Client.calls[-1][2]
    assert heartbeat["method"] == "tools/call"
    assert heartbeat["params"]["name"] == "cao_runtime_heartbeat"
    assert heartbeat["params"]["arguments"] == {
        "runtime_id": "run_1",
        "expected_enrollment_generation": 4,
        "lease_seconds": 3600,
        "sequence": 1,
    }


def test_stdio_proxy_handles_connection_control_locally_and_posts_once(monkeypatch):
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    ]
    stdin = io.BytesIO(("\n".join(json.dumps(value) for value in requests) + "\n").encode())
    stdout = io.StringIO()
    _Client.calls = []
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", type("In", (), {"buffer": stdin})())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", stdout)
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", _Client)

    assert asyncio.run(serve_stdio_proxy("http://127.0.0.1:8768/mcp", "cao-token")) == 0
    assert [call[2]["method"] for call in _Client.calls] == ["tools/list"]
    output = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [value["id"] for value in output] == [1, 2]


def test_stdio_proxy_stops_after_unrecoverable_legacy_session_loss(monkeypatch):
    class MissingSessionClient(_Client):
        async def post(self, url: str, **kwargs: Any) -> _Response:
            body = kwargs["json"]
            self.calls.append((url, kwargs.get("headers"), body))
            response = _Response({"detail": "MCP session is missing or expired"})
            response.status_code = 404
            return response

    request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    stdin = io.BytesIO((json.dumps(request) + "\n").encode())
    stdout = io.StringIO()
    _Client.calls = []
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", type("In", (), {"buffer": stdin})())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", stdout)
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", MissingSessionClient)

    assert asyncio.run(serve_stdio_proxy("http://127.0.0.1:8768/mcp", "cao-token")) == 1
    assert len(_Client.calls) == 1
    response = json.loads(stdout.getvalue())
    assert response["id"] == 1
    assert response["error"]["message"] == "MCP stdio proxy failure"
    assert "session" not in stdout.getvalue().lower()


@pytest.mark.parametrize(
    "failure_payload",
    [
        {"detail": "internal transport failure"},
        {"jsonrpc": "2.0", "id": 2, "error": None},
        {"jsonrpc": "2.0", "id": 2, "error": "internal transport failure"},
    ],
)
@pytest.mark.parametrize("modern", [False, True], ids=["legacy", "modern"])
def test_stdio_proxy_never_forwards_an_invalid_http_failure_to_its_host(
    monkeypatch: pytest.MonkeyPatch,
    failure_payload: dict[str, Any],
    modern: bool,
) -> None:
    class RawFailureClient(_Client):
        async def post(self, url: str, **kwargs: Any) -> _Response:
            body = kwargs["json"]
            self.calls.append((url, kwargs.get("headers"), body))
            if body["method"] == "initialize":
                return _Response(
                    {"jsonrpc": "2.0", "id": body["id"], "result": {}},
                    headers={"mcp-session-id": "legacy-1"},
                )
            response = _Response(failure_payload)
            response.status_code = 500
            return response

    requests = (
        [_modern("tools/list", request_id=2)]
        if modern
        else [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        ]
    )
    stdin = io.BytesIO(("\n".join(json.dumps(value) for value in requests) + "\n").encode())
    stdout = io.StringIO()
    _Client.calls = []
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", type("In", (), {"buffer": stdin})())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", stdout)
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", RawFailureClient)

    assert asyncio.run(serve_stdio_proxy("http://127.0.0.1:8768/mcp", "cao-token")) == 1
    output = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [value["id"] for value in output] == ([2] if modern else [1, 2])
    assert output[-1]["error"]["message"] == "MCP stdio proxy failure"
    assert "internal transport failure" not in stdout.getvalue()


def test_stdio_proxy_fails_closed_when_a_periodic_runtime_heartbeat_is_rejected(
    monkeypatch, tmp_path: Path
):
    failure_observed = threading.Event()

    class BlockingInput:
        def __init__(self) -> None:
            self.buffer = self
            self._first = True

        def readline(self) -> bytes:
            if self._first:
                self._first = False
                return (json.dumps(_modern("tools/list")) + "\n").encode()
            failure_observed.wait()
            return b""

    class RejectingPeriodicHeartbeatClient(_Client):
        heartbeat_calls = 0

        async def post(self, url: str, **kwargs: Any) -> _Response:
            response = await super().post(url, **kwargs)
            body = kwargs["json"]
            if url.endswith("/mcp") and body.get("method") == "tools/call":
                type(self).heartbeat_calls += 1
                if type(self).heartbeat_calls == 2:
                    response.status_code = 401
                    failure_observed.set()
            return response

    class OnePeriodicTick:
        async def __call__(self, stop_event: asyncio.Event, interval_seconds: float) -> bool:
            del stop_event, interval_seconds
            return True

    broker_path = tmp_path / "rejected.sock"

    async def receive(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "token": "runtime-bearer",
            "runtime_id": "run_1",
            "generation": 4,
            "heartbeat_lease_seconds": 3600,
        }

    _Client.calls = []
    RejectingPeriodicHeartbeatClient.heartbeat_calls = 0
    stdout = io.StringIO()
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdin", BlockingInput())
    monkeypatch.setattr("cao_control_plane.mcp.sys.stdout", stdout)
    monkeypatch.setattr("cao_control_plane.mcp.httpx.AsyncClient", RejectingPeriodicHeartbeatClient)
    monkeypatch.setattr("cao_control_plane.mcp.receive_enrollment_capability", receive)

    assert (
        asyncio.run(
            serve_stdio_proxy(
                "http://127.0.0.1:8768/mcp",
                enrollment_broker_socket=broker_path,
                heartbeat_tick_waiter=OnePeriodicTick(),
            )
        )
        == 1
    )
    assert "MCP stdio proxy failure" in stdout.getvalue()
    assert "runtime-bearer" not in stdout.getvalue()
    assert str(broker_path) not in stdout.getvalue()


def test_proxy_cli_does_not_construct_or_bootstrap_a_local_service(monkeypatch, tmp_path: Path):
    broker_path = tmp_path / "cli.sock"
    captured: dict[str, Any] = {}

    async def fake_stdio(*args: Any, **kwargs: Any) -> int:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return 0

    def forbidden_service(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("proxy mode must not create a local service")

    monkeypatch.setattr(cli, "serve_stdio", fake_stdio)
    monkeypatch.setattr(cli, "ControlPlane", forbidden_service)
    args = cli.build_parser().parse_args(
        [
            "mcp-stdio",
            "--url",
            "http://127.0.0.1:8768/mcp",
            "--enrollment-broker-socket",
            str(broker_path),
        ]
    )
    assert cli.run(args) == 0
    assert captured["args"][:2] == ("http://127.0.0.1:8768/mcp", None)
    assert captured["kwargs"]["enrollment_broker_socket"] == broker_path
