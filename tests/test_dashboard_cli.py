from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastapi.testclient import TestClient

from cao_control_plane.dashboard_cli import load_dashboard_credentials, main
from cao_control_plane.dashboard_edge import (
    DashboardEdgeSettings,
    create_dashboard_edge,
    issue_dashboard_bootstrap_url,
)


def _private_directory(path: Path) -> Path:
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


def _credentials(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "upstream_base_url": "http://127.0.0.1:8768",
                "dashboard_bearer": "dashboard-bearer-credential-value",
                "public_origin": "https://dashboard.example.test",
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def test_issue_then_running_edge_consumes_dynamic_hash_record_once(tmp_path: Path) -> None:
    records = _private_directory(tmp_path / "bootstrap-records")

    def upstream(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "format": "cao-dashboard-read-model/v1",
                "authority": {"mode": "canonical", "generation": 1},
                "cursor": "cursor-1",
                "operator": {
                    "format": "cao-dashboard-operator/v1",
                    "work_items": [],
                    "runtime_delivery": {},
                },
                "snapshot_digest": "a" * 64,
            },
        )

    app = create_dashboard_edge(
        DashboardEdgeSettings(
            upstream_base_url="http://127.0.0.1:8768",
            dashboard_bearer="dashboard-bearer-credential-value",
            bootstrap_record_dir=records,
            public_origin="https://dashboard.example.test",
            transport=httpx.MockTransport(upstream),
        )
    )
    # The record is issued after the app has already been constructed; no
    # edge restart is needed for a newly issued browser session.
    url = issue_dashboard_bootstrap_url(records, "https://dashboard.example.test", ttl_seconds=60)
    parsed = urlparse(url)
    secret = parse_qs(parsed.fragment)["dashboard_bootstrap"][0]
    record = records / hashlib.sha256(secret.encode("utf-8")).hexdigest()
    assert record.exists()
    assert secret not in record.name and secret not in record.read_text(encoding="utf-8")
    with TestClient(app, base_url="https://dashboard.example.test") as client:
        accepted = client.post("/dashboard/session", json={"secret": secret})
        replay = client.post("/dashboard/session", json={"secret": secret})
        assert accepted.status_code == 204
        assert replay.status_code == 401
        assert client.get("/dashboard/api/snapshot").status_code == 200
    assert not record.exists()


def test_credentials_require_a_real_owner_only_file(tmp_path: Path) -> None:
    credentials = _credentials(tmp_path / "dashboard-credentials.json")
    loaded = load_dashboard_credentials(credentials)
    assert loaded.upstream_base_url == "http://127.0.0.1:8768"
    assert loaded.dashboard_bearer == "dashboard-bearer-credential-value"

    credentials.chmod(0o640)
    with pytest.raises(ValueError, match="0600"):
        load_dashboard_credentials(credentials)

    credentials.chmod(0o600)
    link = tmp_path / "dashboard-credentials-link.json"
    link.symlink_to(credentials)
    with pytest.raises(ValueError, match="regular file"):
        load_dashboard_credentials(link)


def test_credentials_load_exact_cloudflare_access_identity_as_one_unit(tmp_path: Path) -> None:
    credentials = tmp_path / "dashboard-credentials.json"
    credentials.write_text(
        json.dumps(
            {
                "upstream_base_url": "http://127.0.0.1:8768",
                "dashboard_bearer": "dashboard-bearer-credential-value",
                "public_origin": "https://cao.example.test",
                "cloudflare_access_team_domain": "cao-team.cloudflareaccess.com",
                "cloudflare_access_audience": "a" * 64,
                "cloudflare_access_allowed_email": "owner@example.test",
            }
        ),
        encoding="utf-8",
    )
    credentials.chmod(0o600)
    loaded = load_dashboard_credentials(credentials)
    assert loaded.cloudflare_access is not None
    assert loaded.cloudflare_access.team_domain == "cao-team.cloudflareaccess.com"
    assert loaded.cloudflare_access.application_audience == "a" * 64
    assert loaded.cloudflare_access.allowed_email == "owner@example.test"

    value = json.loads(credentials.read_text(encoding="utf-8"))
    value.pop("cloudflare_access_allowed_email")
    credentials.write_text(json.dumps(value), encoding="utf-8")
    credentials.chmod(0o600)
    with pytest.raises(ValueError, match="configured together"):
        load_dashboard_credentials(credentials)


def test_native_mcp_stdio_loads_only_owner_private_dashboard_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials = _credentials(tmp_path / "dashboard-credentials.json")
    captured: dict[str, str] = {}

    async def proxy(endpoint: str, token: str) -> int:
        captured["endpoint"] = endpoint
        captured["token"] = token
        return 0

    monkeypatch.setattr("cao_control_plane.dashboard_cli.serve_stdio_proxy", proxy)

    assert main(["mcp-stdio", "--credentials-file", str(credentials)]) == 0
    assert captured == {
        "endpoint": "http://127.0.0.1:8768/mcp",
        "token": "dashboard-bearer-credential-value",
    }


def test_edge_cli_does_not_wire_viewer_disconnect_to_runtime_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials = _credentials(tmp_path / "dashboard-credentials.json")
    bootstrap = _private_directory(tmp_path / "dashboard-bootstrap")
    sessions = _private_directory(tmp_path / "dashboard-sessions")
    captured: dict[str, object] = {}

    def create_edge(settings: object) -> object:
        captured["settings"] = settings
        return object()

    def run_edge(_app: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("cao_control_plane.dashboard_cli.create_dashboard_edge", create_edge)
    monkeypatch.setattr("cao_control_plane.dashboard_cli.uvicorn.run", run_edge)

    assert (
        main(
            [
                "serve",
                "--credentials-file",
                str(credentials),
                "--bootstrap-record-dir",
                str(bootstrap),
                "--session-record-dir",
                str(sessions),
            ]
        )
        == 0
    )
    assert captured["settings"].on_authenticated_stream_count is None
