from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest
from enrollment_helpers import enroll_ready_worker_runtime
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)


def _client(app):
    return TestClient(app, base_url="http://localhost")


def _modern_body(method: str, params: dict | None = None, request_id: int = 1) -> dict:
    values = dict(params or {})
    values["_meta"] = {
        PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
        CLIENT_CAPABILITIES_META_KEY: {},
        CLIENT_INFO_META_KEY: {"name": "pytest", "version": "1"},
    }
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": values}


def _modern_headers(token: str, method: str, *, name: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MCP_LATEST_VERSION,
        "Mcp-Method": method,
    }
    if name is not None:
        headers["Mcp-Name"] = name
    return headers


def test_health_agent_card_and_docs_default(settings):
    app = create_app(settings)
    with _client(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["mcpProtocol"] == MCP_LATEST_VERSION
        card = client.get("/.well-known/agent-card.json").json()
        assert {item["protocolVersion"] for item in card["supportedInterfaces"]} == {"1.0"}
        assert client.get("/docs").status_code == 404


def test_authority_schema_is_canonical_only_and_ready(settings):
    app = create_app(settings)
    with pytest.raises(sqlite3.IntegrityError), app.state.database.transaction() as connection:
        connection.execute("UPDATE control_authority SET mode = 'shadow' WHERE singleton = 1")
    with _client(app) as client:
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json()["ready"] is True
        assert response.json()["authority"]["mode"] == "canonical"


@pytest.mark.parametrize(
    "safe_error", ["runtime_process_exited", "cao_provider_turn_evidence_unavailable"]
)
def test_readiness_fails_closed_on_dispatcher_error_or_stale_cycle(
    settings, monkeypatch, safe_error
):
    app = create_app(settings)
    healthy_shape = {
        "running": True,
        "authority_mode": "canonical",
        "owner_token": "must-not-cross-the-ready-edge",
        "queued_deliveries": 2,
        "leased_deliveries": 0,
        "unknown_delivery_outcomes": 1,
        "pending_push_deliveries": 0,
        "active_deliveries": 0,
    }
    with _client(app) as client:
        monkeypatch.setattr(
            app.state.dispatcher,
            "status",
            lambda: {
                **healthy_shape,
                "last_cycle_at": datetime.now(UTC).isoformat(),
                "last_error": safe_error,
            },
        )
        errored = client.get("/ready")
        assert errored.status_code == 503
        assert errored.json()["dispatcher"]["issues"] == ["dispatcher_cycle_error"]
        assert errored.json()["dispatcher"]["last_error"] == safe_error
        assert "owner_token" not in errored.json()["dispatcher"]

        monkeypatch.setattr(
            app.state.dispatcher,
            "status",
            lambda: {
                **healthy_shape,
                "last_cycle_at": datetime.now(UTC).isoformat(),
                "last_error": "PrivateSecret123",
            },
        )
        redacted = client.get("/ready")
        assert redacted.status_code == 503
        assert redacted.json()["dispatcher"]["last_error"] == "dispatcher_error"
        assert "PrivateSecret123" not in redacted.text

        monkeypatch.setattr(
            app.state.dispatcher,
            "status",
            lambda: {
                **healthy_shape,
                "last_cycle_at": "2000-01-01T00:00:00Z",
                "last_error": "",
            },
        )
        stale = client.get("/ready")
        assert stale.status_code == 503
        assert stale.json()["dispatcher"]["issues"] == ["dispatcher_cycle_stale"]
        assert stale.json()["dispatcher"]["unknown_delivery_outcomes"] == 1

        def unavailable_status():
            raise RuntimeError("private dispatcher diagnostic")

        monkeypatch.setattr(app.state.dispatcher, "status", unavailable_status)
        unavailable = client.get("/ready")
        assert unavailable.status_code == 503
        assert unavailable.json()["dispatcher"]["issues"] == ["dispatcher_status_unavailable"]
        assert "private dispatcher diagnostic" not in unavailable.text


def test_api_requires_authentication(settings):
    app = create_app(settings)
    with _client(app) as client:
        response = client.get("/api/v1/principals")
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"


def test_modern_unknown_method_is_http_404(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    body = _modern_body("custom/unknown")
    with _client(app) as client:
        response = client.post(
            "/mcp",
            headers=_modern_headers(token, "custom/unknown"),
            json=body,
        )
        assert response.status_code == 404
        assert response.json()["error"]["code"] == -32601


def test_admin_api_can_create_worker_and_assign(settings):
    app = create_app(settings)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    headers = {"Authorization": f"Bearer {token}"}
    with _client(app) as client:
        created = client.post(
            "/api/v1/principals",
            headers=headers,
            json={"name": "api-worker", "role": "worker", "metadata": {}},
        )
        assert created.status_code == 200
        worker = created.json()
    # Complete enrollment while the app's lifespan dispatcher is stopped: a
    # bootstrap delivery must not race the direct handshake used by this test.
    cao = app.state.service.authenticate(token)
    enroll_ready_worker_runtime(app.state.service, cao, worker["principal"]["id"])
    with _client(create_app(settings)) as client:
        work = client.post(
            "/api/v1/work",
            headers=headers,
            json={
                "worker_id": worker["principal"]["id"],
                "title": "API",
                "objective": "Assigned over REST",
                "maturity": "defined",
                "acceptance": ["Created"],
            },
        )
        assert work.status_code == 200
        assert work.json()["title"] == "API"


def test_production_api_excludes_runtime_observation_and_boundary_ingress(settings):
    app = create_app(settings)
    cao_token = app.state.bootstrap["tokens"]["cao"]["token"]
    cao_headers = {"Authorization": f"Bearer {cao_token}"}
    with _client(app) as client:
        paths = {route.path for route in app.routes}
        assert "/api/v1/runtimes/{runtime_id}:observe" not in paths
        assert "/api/v1/boundaries" not in paths
        assert (
            client.post(
                "/api/v1/runtimes/nonexistent:observe", headers=cao_headers, json={}
            ).status_code
            == 404
        )
        assert client.post("/api/v1/boundaries", headers=cao_headers, json={}).status_code == 404


def test_a2a_http_send_returns_immediately_when_requested(settings):
    app = create_app(settings)
    cao_token = app.state.bootstrap["tokens"]["cao"]["token"]
    with _client(app) as client:
        worker = client.post(
            "/api/v1/principals",
            headers={"Authorization": f"Bearer {cao_token}"},
            json={"name": "a2a-worker", "role": "worker", "metadata": {}},
        ).json()["principal"]
    cao = app.state.service.authenticate(cao_token)
    enroll_ready_worker_runtime(app.state.service, cao, worker["id"])
    with _client(create_app(settings)) as client:
        response = client.post(
            "/a2a/http/message:send",
            headers={
                "Authorization": f"Bearer {cao_token}",
                "Content-Type": "application/a2a+json",
                "A2A-Version": "1.0",
            },
            json={
                "message": {
                    "messageId": "http-message-1",
                    "role": "ROLE_USER",
                    "parts": [{"text": "Build the feature"}],
                    "metadata": {
                        "workerId": worker["id"],
                        "acceptance": ["Feature works"],
                    },
                },
                "configuration": {"returnImmediately": True},
            },
        )
        assert response.status_code == 200
        assert response.headers["a2a-version"] == "1.0"
        assert response.json()["task"]["status"]["state"] == "TASK_STATE_SUBMITTED"


def test_user_cannot_create_a2a_tasks_directly(settings):
    app = create_app(settings)
    cao_token = app.state.bootstrap["tokens"]["cao"]["token"]
    user_token = app.state.bootstrap["tokens"]["user"]["token"]
    with _client(app) as client:
        worker = client.post(
            "/api/v1/principals",
            headers={"Authorization": f"Bearer {cao_token}"},
            json={"name": "guarded-worker", "role": "worker", "metadata": {}},
        ).json()["principal"]
        response = client.post(
            "/a2a/http/message:send",
            headers={
                "Authorization": f"Bearer {user_token}",
                "Content-Type": "application/a2a+json",
                "A2A-Version": "1.0",
            },
            json={
                "message": {
                    "messageId": "forbidden-user-send",
                    "role": "ROLE_USER",
                    "parts": [{"text": "Bypass CAO"}],
                    "metadata": {
                        "workerId": worker["id"],
                        "acceptance": ["Should not run"],
                    },
                },
                "configuration": {"returnImmediately": True},
            },
        )
        assert response.status_code in {403, 404}
