from __future__ import annotations

import json
from pathlib import Path

import httpx

from cao_control_plane.dashboard_access import (
    DashboardAccessCoordinator,
    DashboardAccessResult,
)

_BEARER = "dashboard-bearer-credential-value"
_CP_ORIGIN = "http://127.0.0.1:18768"
_EDGE_ORIGIN = "http://127.0.0.1:18769"
_PUBLIC_ORIGIN = "https://dashboard.example.test"
_ACCESS_DOMAIN = "owner.cloudflareaccess.com"


def _credentials(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "upstream_base_url": _CP_ORIGIN,
                "dashboard_bearer": _BEARER,
                "public_origin": _PUBLIC_ORIGIN,
                "allowed_private_upstream_hosts": ["127.0.0.1"],
                "cloudflare_access_team_domain": _ACCESS_DOMAIN,
                "cloudflare_access_audience": "audience",
                "cloudflare_access_allowed_email": "owner@example.test",
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _transport(
    *,
    public_location: str = f"https://{_ACCESS_DOMAIN}/cdn-cgi/access/login/dashboard",
    cp_status: int = 200,
    edge_status: int = 200,
) -> tuple[httpx.MockTransport, list[str]]:
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        if request.url.host == "127.0.0.1" and request.url.port == 18768:
            assert request.url.path == "/api/v1/dashboard/v1/snapshot"
            assert request.headers["authorization"] == f"Bearer {_BEARER}"
            return httpx.Response(
                cp_status,
                headers={"content-type": "application/json"},
                json={
                    "format": "cao-dashboard-read-model/v1",
                    "cursor": "cursor",
                    "snapshot_digest": "d" * 64,
                },
            )
        if request.url.host == "127.0.0.1" and request.url.port == 18769:
            assert request.url.path == "/dashboard/"
            return httpx.Response(
                edge_status,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if request.url.host == "dashboard.example.test":
            assert request.url.path == "/dashboard/"
            return httpx.Response(302, headers={"location": public_location})
        raise AssertionError(f"unexpected request: {request.url}")

    return httpx.MockTransport(handler), requests


def test_access_probe_proves_control_plane_edge_and_cloudflare_boundary_once(
    tmp_path: Path,
) -> None:
    transport, requests = _transport()
    coordinator = DashboardAccessCoordinator(
        credentials_file=_credentials(tmp_path / "credentials.json"),
        edge_base_url=_EDGE_ORIGIN,
        timeout_seconds=1,
        http_transport=transport,
    )

    result = coordinator.inspect()

    assert result == DashboardAccessResult(
        service="ready",
        control_plane="ready",
        edge="ready",
        public_access="access-protected",
        url=_PUBLIC_ORIGIN + "/dashboard/",
        reason_code=None,
    )
    assert requests == [
        _CP_ORIGIN + "/api/v1/dashboard/v1/snapshot",
        _EDGE_ORIGIN + "/dashboard/",
        _PUBLIC_ORIGIN + "/dashboard/",
    ]


def test_wrong_public_redirect_is_typed_unavailable_without_retry_or_effect(
    tmp_path: Path,
) -> None:
    transport, requests = _transport(
        public_location="https://attacker.example.test/cdn-cgi/access/login"
    )
    coordinator = DashboardAccessCoordinator(
        credentials_file=_credentials(tmp_path / "credentials.json"),
        edge_base_url=_EDGE_ORIGIN,
        timeout_seconds=1,
        http_transport=transport,
    )

    result = coordinator.inspect()

    assert result.service == "unavailable"
    assert result.control_plane == "ready"
    assert result.edge == "ready"
    assert result.public_access == "unhealthy"
    assert result.reason_code == "dashboard_public_access_unhealthy"
    assert len(requests) == 3


def test_dashboard_access_runtime_has_no_gui_or_service_lifecycle_authority() -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            Path("src/cao_control_plane/dashboard_access.py"),
            Path("src/cao_control_plane/cli.py"),
            Path("src/cao_control_plane/dashboard_cli.py"),
            Path("src/cao_control_plane/mcp.py"),
        )
    ).lower()

    assert "osascript" not in sources
    assert "safaridashboardpresenter" not in sources
    assert "dashboarddisconnectrecovery" not in sources
    assert "launchctl\", \"kickstart" not in sources


def test_result_dict_separates_service_access_from_client_presentation() -> None:
    result = DashboardAccessResult(
        service="ready",
        control_plane="ready",
        edge="ready",
        public_access="access-protected",
        url=_PUBLIC_ORIGIN + "/dashboard/",
        reason_code=None,
    )

    assert result.as_dict() == {
        "service": "ready",
        "control_plane": "ready",
        "edge": "ready",
        "public_access": "access-protected",
        "url": _PUBLIC_ORIGIN + "/dashboard/",
        "presentation": "client-controlled",
    }
