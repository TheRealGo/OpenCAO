"""Read-only Dashboard access evidence, independent from CAO supervision.

This boundary is intentionally observational.  It cannot open a browser,
restart a process, mint a browser session, or change Control Plane state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx

from .dashboard_credentials import load_dashboard_credentials

_SNAPSHOT_PATH = "/api/v1/dashboard/v1/snapshot"
_DASHBOARD_PATH = "/dashboard/"
_DASHBOARD_FORMAT = "cao-dashboard-read-model/v1"
DashboardAccessState = Literal["ready", "unavailable"]
DashboardComponentState = Literal[
    "ready", "unavailable", "unhealthy", "access-protected", "not-configured"
]


@dataclass(frozen=True, slots=True)
class DashboardAccessResult:
    """Sanitized evidence for three independent Dashboard access boundaries."""

    service: DashboardAccessState
    control_plane: DashboardComponentState
    edge: DashboardComponentState
    public_access: DashboardComponentState
    url: str | None
    reason_code: str | None

    @property
    def ready(self) -> bool:
        return self.service == "ready"

    def as_dict(self) -> dict[str, str]:
        value = {
            "service": self.service,
            "control_plane": self.control_plane,
            "edge": self.edge,
            "public_access": self.public_access,
            "presentation": "client-controlled",
        }
        if self.url is not None:
            value["url"] = self.url
        if self.reason_code is not None:
            value["reason_code"] = self.reason_code
        return value


class DashboardAccessCoordinator:
    """Inspect each access boundary once without retries or effects."""

    def __init__(
        self,
        *,
        credentials_file: Path,
        edge_base_url: str,
        timeout_seconds: float,
        http_transport: httpx.BaseTransport | None = None,
    ) -> None:
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("Dashboard access timeout must be finite and positive")
        self._credentials_file = Path(credentials_file)
        self._edge_base_url = _loopback_http_origin(edge_base_url)
        self._timeout_seconds = float(timeout_seconds)
        self._http_transport = http_transport

    @property
    def url(self) -> str | None:
        try:
            credentials = load_dashboard_credentials(self._credentials_file)
        except (OSError, UnicodeError, ValueError):
            return None
        return _dashboard_url(credentials.public_origin)

    def inspect(self) -> DashboardAccessResult:
        try:
            credentials = load_dashboard_credentials(self._credentials_file)
        except (OSError, UnicodeError, ValueError):
            return DashboardAccessResult(
                "unavailable",
                "unavailable",
                "unavailable",
                "not-configured",
                None,
                "dashboard_credentials_unavailable",
            )
        dashboard_url = _dashboard_url(credentials.public_origin)
        if dashboard_url is None:
            return DashboardAccessResult(
                "unavailable",
                "unavailable",
                "unavailable",
                "not-configured",
                None,
                "dashboard_public_origin_unavailable",
            )
        with httpx.Client(
            timeout=self._timeout_seconds,
            follow_redirects=False,
            transport=self._http_transport,
        ) as client:
            control_plane = _probe_control_plane(
                client,
                credentials.upstream_base_url.rstrip("/") + _SNAPSHOT_PATH,
                credentials.dashboard_bearer,
            )
            edge = _probe_edge(client, self._edge_base_url + _DASHBOARD_PATH)
            public_access = _probe_public_access(
                client,
                dashboard_url,
                access_team_domain=(
                    credentials.cloudflare_access.team_domain
                    if credentials.cloudflare_access is not None
                    else None
                ),
            )
        reason_code = _reason_code(control_plane, edge, public_access)
        return DashboardAccessResult(
            service="ready" if reason_code is None else "unavailable",
            control_plane=control_plane,
            edge=edge,
            public_access=public_access,
            url=dashboard_url,
            reason_code=reason_code,
        )


def _probe_control_plane(
    client: httpx.Client, url: str, bearer: str
) -> DashboardComponentState:
    try:
        response = client.get(url, headers={"Authorization": f"Bearer {bearer}"})
    except httpx.HTTPError:
        return "unavailable"
    if response.status_code != 200 or not response.headers.get(
        "content-type", ""
    ).startswith("application/json"):
        return "unhealthy"
    try:
        value = response.json()
    except ValueError:
        return "unhealthy"
    if (
        not isinstance(value, dict)
        or value.get("format") != _DASHBOARD_FORMAT
        or not isinstance(value.get("cursor"), str)
        or not isinstance(value.get("snapshot_digest"), str)
        or len(value["snapshot_digest"]) != 64
    ):
        return "unhealthy"
    return "ready"


def _probe_edge(client: httpx.Client, url: str) -> DashboardComponentState:
    try:
        response = client.get(url)
    except httpx.HTTPError:
        return "unavailable"
    disposition = response.headers.get("content-disposition", "").lower()
    if (
        response.status_code != 200
        or not response.headers.get("content-type", "").startswith("text/html")
        or "attachment" in disposition
    ):
        return "unhealthy"
    return "ready"


def _probe_public_access(
    client: httpx.Client,
    url: str,
    *,
    access_team_domain: str | None,
) -> DashboardComponentState:
    try:
        response = client.get(url)
    except httpx.HTTPError:
        return "unavailable"
    if access_team_domain is None:
        disposition = response.headers.get("content-disposition", "").lower()
        return (
            "ready"
            if response.status_code == 200
            and response.headers.get("content-type", "").startswith("text/html")
            and "attachment" not in disposition
            else "unhealthy"
        )
    location = response.headers.get("location", "")
    try:
        target = httpx.URL(location)
    except (httpx.InvalidURL, ValueError):
        return "unhealthy"
    if (
        response.status_code in {302, 303, 307, 308}
        and target.scheme == "https"
        and target.host == access_team_domain
        and target.path.startswith("/cdn-cgi/access/")
    ):
        return "access-protected"
    return "unhealthy"


def _reason_code(
    control_plane: DashboardComponentState,
    edge: DashboardComponentState,
    public_access: DashboardComponentState,
) -> str | None:
    if control_plane != "ready":
        return f"dashboard_control_plane_{control_plane}"
    if edge != "ready":
        return f"dashboard_edge_{edge}"
    if public_access not in {"ready", "access-protected"}:
        return f"dashboard_public_access_{public_access}"
    return None


def _dashboard_url(public_origin: str | None) -> str | None:
    if public_origin is None:
        return None
    parsed = urlparse(public_origin)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return None
    return public_origin.rstrip("/") + _DASHBOARD_PATH


def _loopback_http_origin(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("dashboard edge must be a loopback HTTP origin")
    return value.rstrip("/")
