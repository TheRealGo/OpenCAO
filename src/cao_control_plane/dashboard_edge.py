"""Read-only Web and text edge for the versioned dashboard API.

The edge deliberately knows only the four ``/api/v1/dashboard/v1`` routes.
It owns the dashboard bearer credential on the server side, and converts a
short-lived, one-time bootstrap secret into a browser-only HttpOnly session.
It has no control-plane database, service, MCP, or generic REST dependency.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import stat
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import tzinfo
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import RequestResponseEndpoint

from .cloudflare_access import CloudflareAccessJWTValidator, CloudflareAccessSettings
from .dashboard_history import HISTORY_KINDS, HISTORY_REFERENCE, full_operator_text
from .dashboard_presentation import local_timestamp_display
from .projection import sanitize_operator_text
from .provider_models import model_identifier

_API_PREFIX = "/api/v1/dashboard/v1"
_DASHBOARD_FORMAT = "cao-dashboard-read-model/v1"
_OPERATOR_FORMAT = "cao-dashboard-operator/v1"
_MAX_CURSOR_BYTES = 4096
_MAX_JSON_BYTES = 16 * 1024 * 1024
_MAX_STREAM_CHUNK_BYTES = 64 * 1024
_SESSION_COOKIE = "cao_dashboard_session"
_CSP = (
    "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; "
    "img-src 'self'; object-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; manifest-src 'none'"
)
_DISPLAY_LABEL = re.compile(r"^(?:Work item|Worker) [1-9][0-9]*$")
_SAFE_EVENT = re.compile(r"^[a-z][a-z0-9_]{0,63}\.[a-z][a-z0-9_]{0,63}$")
_SAFE_LABEL = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_CLOSURE_STATES = frozenset({"open", "awaiting-explicit-close", "closed"})
_REQUESTER_DECISIONS = frozenset({"accepted", "rejected", "pending"})
_CAO_REVIEW_DECISIONS = frozenset({"ok", "needs-work", "pending"})
_ARTIFACT_PRESERVATION_STATES = frozenset({"preserved", "pending"})
_CLEANUP_STATES = frozenset({"verified", "pending", "unknown"})
_RUNNER_ADAPTERS = frozenset({"codex-app-server", "claude", "subprocess", "webhook"})
_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})
_RUNNER_AVAILABILITY = frozenset({"available", "unavailable"})
_RUNNER_SPEC_STATES = frozenset(
    {
        "enabled",
        "stopped",
        "revoked",
        "unsupported",
        "mismatched",
        "invalid",
        "unavailable",
    }
)
_RUNNER_CONNECTION_STATES = frozenset(
    {
        "enrolling",
        "connected-idle",
        "connected-busy",
        "enrolled-reopenable",
        "stopped",
        "failed",
        "missing",
        "unavailable",
    }
)
_OPERATOR_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_REPORT_KINDS = frozenset({"progress", "question", "blocker", "artifact", "completion_claim", "worker_output"})
_ATTEMPT_STATES = frozenset(
    {
        "assigned",
        "accepted",
        "working",
        "suspended",
        "waiting_supervisor",
        "input_required",
        "blocked",
        "submitted",
        "completed",
        "failed",
        "canceled",
    }
)
_ATTENTION_REASONS = frozenset(
    {
        "user-action-required",
        "cao-action-required",
        "cao-processing",
        "external-action-required",
        "progress-at-risk",
        "awaiting-explicit-close",
        "system-reconciliation",
        "runner-failed",
        "runner-missing",
        "runner-stopped",
    }
)
_AVAILABILITY_STATES = frozenset({"available", "unavailable"})
_STATUS_REQUEST_STATES = frozenset({"pending", "overdue", "responded"})
_COMPLETION_CONTRACTS = frozenset(
    {"completion_required", "no_artifact_expected", "legacy_unclassified"}
)
_DELIVERY_STATES = frozenset(
    {"pending", "not_required", "legacy_unclassified", "ready", "delivery_missing"}
)
_RECOVERY_ACTIONS = frozenset(
    {
        "dispose_continue_or_correct",
        "reconcile_continue_same_thread",
        "system_reconciliation",
    }
)
_RECOVERY_NOTIFICATION_STATES = frozenset(
    {"queued", "leased", "dispatched", "delivered", "acknowledged", "handled", "dead"}
)
_CAO_SUPERVISION_STATES = frozenset({"scheduled", "active", "unscheduled"})


class DashboardAccessTokenValidator(Protocol):
    async def authorized(self, token: str) -> bool: ...


_WORK_AVAILABILITY_KEYS = (
    "work_title",
    "objective_summary",
    "attempt_state",
    "progress_stage",
    "next_boundary_summary",
    "pending_supervisor_boundary",
    "recovery_action",
    "recovery_waiting_since",
    "recovery_notification_state",
    "latest_report_kind",
    "latest_report_summary",
    "latest_reported_at",
    "runtime_heartbeat_at",
    "last_worker_activity_at",
    "last_artifact_at",
    "status_request_state",
    "status_requested_at",
    "status_response_due_at",
    "status_responded_at",
    "completion_contract",
    "delivery_state",
    "next_observable_boundary",
    "latest_worker_report_summary",
    "runner_adapter",
    "runner_model",
    "runner_reasoning_effort",
    "runner_requested_model",
    "runner_effective_model",
    "runner_requested_reasoning_effort",
    "runner_effective_reasoning_effort",
    "runner_availability",
    "runner_state",
    "runner_connection_state",
    "latest_cao_review_decision",
    "requester_decision",
    "closure_summary",
)
_OPERATOR_COUNT_KEYS = (
    "needs_attention",
    "working",
    "ready",
    "inactive_workers",
    "current_work_items",
)


def _secret_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_all(descriptor: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        count = os.write(descriptor, payload[written:])
        if count <= 0:
            raise OSError("dashboard record write made no progress")
        written += count


def _canonical_origin(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("public_origin must be an origin without credentials or a path")
    host = parsed.hostname.lower().rstrip(".")
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port or default_port
    rendered = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{rendered}" + ("" if port == default_port else f":{port}")


def _validated_upstream_base_url(value: str, allowed_hosts: frozenset[str]) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("upstream_base_url must be a bare http(s) origin")
    host = parsed.hostname.lower().rstrip(".")
    is_private = host == "localhost"
    try:
        address = ipaddress.ip_address(host)
        is_private = address.is_loopback or address.is_private
    except ValueError:
        pass
    if not is_private and host not in allowed_hosts:
        raise ValueError(
            "upstream_base_url host must be loopback, private, or explicitly configured"
        )
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port or default_port
    rendered = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{rendered}" + ("" if port == default_port else f":{port}")


def dashboard_mcp_endpoint(
    upstream_base_url: str,
    *,
    allowed_private_upstream_hosts: tuple[str, ...] = (),
) -> str:
    """Return the only MCP origin a dashboard-native stdio proxy may contact."""

    allowed = frozenset(host.lower().rstrip(".") for host in allowed_private_upstream_hosts)
    return f"{_validated_upstream_base_url(upstream_base_url, allowed)}/mcp"


@dataclass(frozen=True, slots=True)
class DashboardEdgeSettings:
    """Configuration for a dedicated dashboard edge process.

    ``bootstrap_secrets`` supports in-process tests and is stored as hashes.
    Production uses ``bootstrap_record_dir``: the edge reads a hash-only,
    expiring, one-use record at consumption time, so links can be issued
    without restarting this process.
    ``allowed_private_upstream_hosts`` exists for a configured private DNS name;
    it is not populated from browser input.
    """

    upstream_base_url: str
    dashboard_bearer: str
    bootstrap_secrets: Mapping[str, float] = field(default_factory=dict)
    bootstrap_record_dir: Path | None = None
    session_record_dir: Path | None = None
    public_origin: str | None = None
    session_ttl_seconds: int = 8 * 60 * 60
    allowed_private_upstream_hosts: tuple[str, ...] = ()
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0
    max_history_limit: int = 100
    cloudflare_access: CloudflareAccessSettings | None = None
    cloudflare_access_validator: DashboardAccessTokenValidator | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    transport: httpx.AsyncBaseTransport | None = field(default=None, repr=False, compare=False)
    on_authenticated_stream_count: Callable[[int], None] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if len(self.dashboard_bearer) < 16:
            raise ValueError("dashboard_bearer must be a dedicated non-empty credential")
        if not self.bootstrap_secrets and self.bootstrap_record_dir is None:
            raise ValueError("a bootstrap secret or bootstrap record directory is required")
        if self.session_ttl_seconds <= 0 or self.session_ttl_seconds > 24 * 60 * 60:
            raise ValueError("session_ttl_seconds must be between one second and one day")
        if self.connect_timeout_seconds <= 0 or self.read_timeout_seconds <= 0:
            raise ValueError("edge timeouts must be positive")
        if not 1 <= self.max_history_limit <= 1000:
            raise ValueError("max_history_limit must be between 1 and 1000")
        for secret, expiry in self.bootstrap_secrets.items():
            if len(secret) < 32 or not isinstance(expiry, (int, float)):
                raise ValueError("bootstrap secrets must be high entropy with numeric expiries")
        if self.bootstrap_record_dir is not None:
            object.__setattr__(
                self, "bootstrap_record_dir", _owner_only_directory(self.bootstrap_record_dir)
            )
        if self.session_record_dir is not None:
            object.__setattr__(
                self, "session_record_dir", _owner_only_directory(self.session_record_dir)
            )
        allowed = frozenset(
            host.lower().rstrip(".") for host in self.allowed_private_upstream_hosts
        )
        object.__setattr__(
            self, "upstream_base_url", _validated_upstream_base_url(self.upstream_base_url, allowed)
        )
        if self.public_origin is not None:
            object.__setattr__(self, "public_origin", _canonical_origin(self.public_origin))
        if self.cloudflare_access_validator is not None and self.cloudflare_access is None:
            raise ValueError("Cloudflare Access settings are required for its validator")

    @property
    def secure_cookie(self) -> bool:
        return bool(self.public_origin and self.public_origin.startswith("https://"))


class _ExpiringSecretStore:
    def __init__(self, values: Mapping[str, float], clock: Callable[[], float]) -> None:
        self._values = {_secret_digest(secret): float(expiry) for secret, expiry in values.items()}
        self._clock = clock
        self._lock = threading.Lock()

    def consume(self, secret: str) -> bool:
        digest = _secret_digest(secret)
        with self._lock:
            expiry = self._values.pop(digest, None)
        return expiry is not None and expiry > self._clock()


def _owner_only_directory(value: Path) -> Path:
    """Require a local Dashboard record directory to be private and non-symlinked."""

    path = Path(value)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError("dashboard record directory must be a real directory")
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("dashboard record directory must be owner-only (0700)")
    return path.resolve()


class FileBootstrapSecretStore:
    """Consume hash-only, short-lived bootstrap records from a private directory."""

    def __init__(self, directory: Path, clock: Callable[[], float]) -> None:
        self._directory = _owner_only_directory(directory)
        self._clock = clock

    def consume(self, secret: str) -> bool:
        if len(secret) < 32 or len(secret) > 512:
            return False
        digest = _secret_digest(secret)
        record = self._directory / digest
        claimed = self._directory / f".{digest}.{secrets.token_hex(8)}.consuming"
        try:
            info = record.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                return False
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
                return False
            os.replace(record, claimed)
        except FileNotFoundError:
            return False
        try:
            info = claimed.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                return False
            raw = json.loads(claimed.read_text(encoding="utf-8"))
            expiry = raw.get("expires_at") if isinstance(raw, Mapping) else None
            return (
                isinstance(expiry, (int, float))
                and not isinstance(expiry, bool)
                and float(expiry) > self._clock()
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        finally:
            with suppress(FileNotFoundError):
                claimed.unlink()


def issue_dashboard_bootstrap_url(
    directory: Path,
    public_origin: str,
    *,
    ttl_seconds: int = 300,
    clock: Callable[[], float] = time.time,
) -> str:
    """Issue a one-use fragment URL while persisting only a secret hash."""

    if not 1 <= ttl_seconds <= 3600:
        raise ValueError("bootstrap TTL must be between one second and one hour")
    root = _owner_only_directory(directory)
    origin = _canonical_origin(public_origin)
    secret = secrets.token_urlsafe(32)
    record = root / _secret_digest(secret)
    payload = json.dumps({"expires_at": clock() + ttl_seconds}, separators=(",", ":"))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(record, flags, 0o600)
    except FileExistsError:
        # A digest collision is cryptographically implausible. Retrying never
        # risks overwriting an existing record and keeps the raw secret local.
        return issue_dashboard_bootstrap_url(
            directory, public_origin, ttl_seconds=ttl_seconds, clock=clock
        )
    try:
        os.write(descriptor, payload.encode("utf-8"))
    finally:
        os.close(descriptor)
    return f"{origin}/dashboard/#dashboard_bootstrap={secret}"


class _SessionStore:
    def __init__(self, ttl_seconds: int, clock: Callable[[], float]) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._values: dict[str, float] = {}
        self._lock = threading.Lock()

    def issue(self) -> str:
        raw = secrets.token_urlsafe(32)
        with self._lock:
            self._values[_secret_digest(raw)] = self._clock() + self._ttl_seconds
        return raw

    def valid(self, raw: str) -> bool:
        with self._lock:
            expiry = self._values.get(_secret_digest(raw))
            if expiry is None or expiry <= self._clock():
                self._values.pop(_secret_digest(raw), None)
                return False
            return True


class _ActiveStreamCounter:
    """Track only currently iterated, authenticated browser SSE responses."""

    def __init__(self, on_change: Callable[[int], None] | None = None) -> None:
        self._value = 0
        self._lock = threading.Lock()
        self._on_change = on_change

    def _notify(self, value: int) -> None:
        if self._on_change is None:
            return
        # Visibility recovery is advisory to the SSE transport. A broken
        # observer must never disconnect an authenticated Dashboard.
        with suppress(Exception):
            self._on_change(value)

    def acquire(self) -> None:
        with self._lock:
            self._value += 1
            value = self._value
        self._notify(value)

    def release(self) -> None:
        with self._lock:
            if self._value <= 0:
                raise RuntimeError("dashboard stream counter underflow")
            self._value -= 1
            value = self._value
        self._notify(value)

    def value(self) -> int:
        with self._lock:
            return self._value

    def notify_current(self) -> None:
        self._notify(self.value())


class _RenderedSnapshotTracker:
    """Keep the last cursor/digest a browser confirmed after DOM rendering."""

    def __init__(self) -> None:
        self._cursor = ""
        self._snapshot_digest = ""
        self._lock = threading.Lock()

    def observe(self, cursor: str, snapshot_digest: str) -> None:
        with self._lock:
            self._cursor = cursor
            self._snapshot_digest = snapshot_digest

    def clear(self) -> None:
        with self._lock:
            self._cursor = ""
            self._snapshot_digest = ""

    def value(self) -> tuple[str, str]:
        with self._lock:
            return self._cursor, self._snapshot_digest


async def _tracked_stream(
    counter: _ActiveStreamCounter,
    source: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    counter.acquire()
    try:
        async for chunk in source:
            yield chunk
    finally:
        counter.release()


class FileSessionStore:
    """Persist only session hashes so browser access survives an edge restart."""

    def __init__(
        self,
        directory: Path,
        ttl_seconds: int,
        clock: Callable[[], float],
    ) -> None:
        self._directory = _owner_only_directory(directory)
        self._ttl_seconds = ttl_seconds
        self._clock = clock

    def issue(self) -> str:
        raw = secrets.token_urlsafe(32)
        record = self._directory / _secret_digest(raw)
        payload = json.dumps(
            {"expires_at": self._clock() + self._ttl_seconds},
            separators=(",", ":"),
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(record, flags, 0o600)
        except FileExistsError:
            return self.issue()
        try:
            _write_all(descriptor, payload.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory_descriptor = os.open(self._directory, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return raw

    def valid(self, raw: str) -> bool:
        if len(raw) < 32 or len(raw) > 512:
            return False
        record = self._directory / _secret_digest(raw)
        try:
            info = record.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                return False
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(record, flags)
        except (FileNotFoundError, OSError):
            return False
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
                info.st_dev,
                info.st_ino,
            ):
                return False
            payload = os.read(descriptor, 1024)
            if os.read(descriptor, 1):
                return False
        finally:
            os.close(descriptor)
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        if not isinstance(value, Mapping):
            return False
        expiry = value.get("expires_at")
        if (
            set(value) != {"expires_at"}
            or not isinstance(expiry, (int, float))
            or isinstance(expiry, bool)
        ):
            return False
        if float(expiry) > self._clock():
            return True
        try:
            current = record.lstat()
            if (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino):
                record.unlink()
        except FileNotFoundError:
            pass
        return False


class DashboardTextClient:
    """CLI-friendly dashboard reader which uses the same official endpoints."""

    def __init__(
        self,
        upstream_base_url: str,
        dashboard_bearer: str,
        *,
        allowed_private_upstream_hosts: tuple[str, ...] = (),
        timeout_seconds: float = 15.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        allowed = frozenset(host.lower().rstrip(".") for host in allowed_private_upstream_hosts)
        self._base_url = _validated_upstream_base_url(upstream_base_url, allowed)
        if len(dashboard_bearer) < 16:
            raise ValueError("dashboard_bearer must be a dedicated non-empty credential")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._bearer = dashboard_bearer
        self._timeout = timeout_seconds
        self._transport = transport

    def snapshot(self) -> str:
        with self._client() as client:
            response = client.get(f"{_API_PREFIX}/snapshot")
        body = _dashboard_json(response)
        return render_dashboard_snapshot(body)

    def follow(self, *, after: str = "", limit: int | None = None) -> Iterator[str]:
        query: dict[str, str | int] = {}
        if after:
            query["after"] = _cursor(after)
        if limit is not None:
            if not 1 <= limit <= 1000:
                raise ValueError("limit must be between 1 and 1000")
            query["limit"] = limit
        with (
            self._client(read_timeout=30.0) as client,
            client.stream("GET", f"{_API_PREFIX}/stream", params=query) as response,
        ):
            if response.status_code != 200 or not response.headers.get(
                "content-type", ""
            ).startswith("text/event-stream"):
                raise RuntimeError("dashboard stream is unavailable")
            event_type = "message"
            data: list[str] = []
            for line in response.iter_lines():
                if not line:
                    if data:
                        yield render_dashboard_event(event_type, json.loads("\n".join(data)))
                    event_type, data = "message", []
                elif line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())

    def _client(self, *, read_timeout: float | None = None) -> httpx.Client:
        return httpx.Client(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {self._bearer}", "Accept": "application/json"},
            timeout=httpx.Timeout(self._timeout, read=read_timeout or self._timeout),
            follow_redirects=False,
            transport=self._transport,
        )


def render_dashboard_snapshot(body: Mapping[str, Any], *, timezone: tzinfo | None = None) -> str:
    authority = body.get("authority", {})
    operator = _operator_view(body)
    delivery = operator["runtime_delivery"]
    lines = [
        "CAO ダッシュボード",
        "現在の本番Worker",
    ]
    current_groups = (
        ("CAOが処理中", operator["cao_processing"]),
        ("あなたの確認待ち", operator["user_confirmation"]),
        ("停止・異常", operator["stopped_or_failed"]),
        ("Worker作業中", operator["working"]),
        ("待機・再開可能", operator["ready"]),
    )
    if not any(workers for _, workers in current_groups):
        lines.append("現在稼働中の本番Workerはありません")
    else:
        for heading, workers in current_groups:
            if not workers:
                continue
            lines.append(heading)
            for worker in workers:
                lines.extend(_render_worker_lines(worker, timezone=timezone))

    if operator["recently_completed"]:
        lines.append("最近完了")
        for item in operator["recently_completed"]:
            lines.append(
                f"- {_display(item['work_title'] or item['display_label'])}; "
                f"Worker {_display(item['worker_label'])}; "
                "完了 "
                f"{_display(local_timestamp_display(item['completed_at'], timezone=timezone))}"
            )

    if operator["inactive_workers"]:
        lines.append("停止済み・移行Worker")
        for worker in operator["inactive_workers"]:
            lines.extend(_render_worker_lines(worker, timezone=timezone))

    lines.extend(
        [
            "技術状態",
            f"Authority: {_field(authority, 'mode', 'unknown')} (generation {_field(authority, 'generation', '?')})",
            f"Runtimes: {delivery['runtime_count']}",
            f"Deliveries queued: {delivery['queued_deliveries']}; unknown: {delivery['unknown_delivery_outcomes']}; dead: {delivery['dead_deliveries']}",
            f"Effects: {_summary(delivery['effects_by_state'])}",
        ]
    )
    return "\n".join(lines)


def _render_worker_lines(worker: Mapping[str, Any], *, timezone: tzinfo | None = None) -> list[str]:
    lines = [f"- {_display(worker['worker_label'])}"]
    if worker["attention_reason"] is not None:
        lines.append(f"  attention reason {_display(worker['attention_reason'])}")
    for item in worker["current_work_items"]:
        lines.append(f"  - {item['display_label']}")
        lines.append(
            "    "
            f"title {_display(item['work_title'])}; objective {_display(item['objective_text'] or item['objective_summary'])}; "
            f"progress stage {_display(item['progress_stage'])}; "
            f"latest report {_display(item['latest_report_text'] or item['latest_report_summary'])}; "
            f"report kind {_display(item['latest_report_kind'])}; "
            "reported at "
            f"{_display(local_timestamp_display(item['latest_reported_at'], timezone=timezone))}; "
            "runtime heartbeat "
            f"{_display(local_timestamp_display(item['runtime_heartbeat_at'], timezone=timezone))}; "
            "last worker activity "
            f"{_display(local_timestamp_display(item['last_worker_activity_at'], timezone=timezone))}; "
            "last artifact "
            f"{_display(local_timestamp_display(item['last_artifact_at'], timezone=timezone))}; "
            f"status request {_display(item['status_request_state'])}; "
            f"completion contract {_display(item['completion_contract'])}; "
            f"delivery state {_display(item['delivery_state'])}; "
            "status due "
            f"{_display(local_timestamp_display(item['status_response_due_at'], timezone=timezone))}; "
            f"next boundary {_display(item['next_boundary_summary'])}; "
            f"attention {_display(item['attention_owner'])}; "
            f"pending supervisor boundary {'yes' if item['pending_supervisor_boundary'] else 'no'}; "
            f"recovery action {_display(item['recovery_action'])}; "
            "recovery waiting since "
            f"{_display(local_timestamp_display(item['recovery_waiting_since'], timezone=timezone))}; "
            f"recovery notification {_display(item['recovery_notification_state'])}; "
            f"CAO supervision {_display(item['cao_supervision_state'])}; "
            "CAO supervision updated "
            f"{_display(local_timestamp_display(item['cao_supervision_updated_at'], timezone=timezone))}; "
            f"state {_display(item['state'])}; attempt state {_display(item['attempt_state'])}; "
            f"trajectory {_display(item['trajectory'])}; "
            f"CAO review {_display(item['latest_cao_review_decision'])}; "
            f"requester decision {_display(item['requester_decision'])}; "
            f"closure {item['closure_state']}; "
            f"artifacts {_display(item['closure_summary']['artifact_preservation'])}; "
            f"cleanup {_display(item['closure_summary']['cleanup'])}; "
            f"unresolved deliveries {_display(item['closure_summary']['unresolved_deliveries'])}; "
            f"unresolved effects {_display(item['closure_summary']['unresolved_effects'])}; "
            f"active runtimes {_display(item['closure_summary']['active_runtimes'])}"
        )
    lines.append(
        "  "
        f"connection: worker state {_display(worker['worker_state'])}; "
        f"runner {_display(worker['runner_adapter'])}; "
        f"model {_display(worker['runner_model'])}; "
        f"reasoning effort {_display(worker['runner_reasoning_effort'])}; "
        f"requested model {_display(worker['runner_requested_model'])}; "
        f"effective model {_display(worker['runner_effective_model'])}; "
        f"requested reasoning effort {_display(worker['runner_requested_reasoning_effort'])}; "
        f"effective reasoning effort {_display(worker['runner_effective_reasoning_effort'])}; "
        f"runner availability {_display(worker['runner_availability'])}; "
        f"runner connection {_display(worker['runner_connection_state'])}"
    )
    return lines


def render_dashboard_event(
    event_type: str,
    body: Mapping[str, Any],
    *,
    timezone: tzinfo | None = None,
) -> str:
    if event_type == "resync-required" or body.get("status") == "resync-required":
        return "Resync required; fetch a new dashboard snapshot."
    if event_type == "dashboard-synced" or body.get("status") == "synced":
        return "Dashboard stream synchronized."
    event = body.get("event", {})
    if isinstance(event, Mapping):
        event_name = event.get("type")
        occurred_at = event.get("occurred_at")
        if (
            isinstance(event_name, str)
            and _SAFE_EVENT.fullmatch(event_name)
            and isinstance(occurred_at, str)
            and len(occurred_at) <= 64
        ):
            return (
                f"Update: {event_name} at "
                f"{_display(local_timestamp_display(occurred_at, timezone=timezone))}"
            )
    return "Dashboard update received."


def _field(value: object, key: str, default: object) -> object:
    return value.get(key, default) if isinstance(value, Mapping) else default


def _summary(value: object) -> str:
    if not isinstance(value, Mapping):
        return "none"
    return ", ".join(f"{key}: {item}" for key, item in sorted(value.items())) or "none"


def _display(value: object) -> str:
    return str(value) if isinstance(value, (str, int)) else "unavailable"


def _operator_view(body: Mapping[str, Any]) -> dict[str, Any]:
    """Allowlist the operator DTO for both direct text and browser rendering."""

    raw = body.get("operator", {})
    value = raw if isinstance(raw, Mapping) and raw.get("format") == _OPERATOR_FORMAT else {}
    categories = {
        key: _worker_items(value.get(key))
        for key in (
            "needs_attention",
            "cao_processing",
            "user_confirmation",
            "stopped_or_failed",
            "working",
            "ready",
            "inactive_workers",
        )
    }
    if (
        not categories["cao_processing"]
        and not categories["user_confirmation"]
        and not categories["stopped_or_failed"]
        and categories["needs_attention"]
    ):
        # Accept an older v1 upstream during one rolling restart, while still
        # presenting it under an explicit abnormal-state label.
        categories["stopped_or_failed"] = categories["needs_attention"]
    return {
        "format": _OPERATOR_FORMAT,
        "counts": {
            key: _count(_field(value.get("counts"), key, 0)) for key in _OPERATOR_COUNT_KEYS
        },
        **categories,
        # Compatibility only. Category membership remains exclusively owned by
        # the server and is never reconstructed from this flat alias.
        "work_items": _work_items(value.get("work_items")),
        "recently_completed": _work_items(value.get("recently_completed")),
        "runtime_delivery": _runtime_delivery(value.get("runtime_delivery")),
    }


def _worker_items(value: object) -> list[dict[str, Any]]:
    raw_workers = value if isinstance(value, list) else []
    workers: list[dict[str, Any]] = []
    for index, raw_worker in enumerate(raw_workers, 1):
        if not isinstance(raw_worker, Mapping):
            continue
        worker_label = sanitize_operator_text(raw_worker.get("worker_label"))
        workers.append(
            {
                "worker_label": worker_label or f"Worker {index}",
                "attention_reason": _safe_status(
                    raw_worker.get("attention_reason"), _ATTENTION_REASONS
                ),
                "worker_state": _safe_status(raw_worker.get("worker_state"), _RUNNER_SPEC_STATES),
                "runner_adapter": _safe_status(raw_worker.get("runner_adapter"), _RUNNER_ADAPTERS),
                "runner_model": _model_label(raw_worker.get("runner_model")),
                "runner_reasoning_effort": _safe_status(
                    raw_worker.get("runner_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_requested_model": _model_label(raw_worker.get("runner_requested_model")),
                "runner_effective_model": _model_label(raw_worker.get("runner_effective_model")),
                "runner_requested_reasoning_effort": _safe_status(
                    raw_worker.get("runner_requested_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_effective_reasoning_effort": _safe_status(
                    raw_worker.get("runner_effective_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_availability": _safe_status(
                    raw_worker.get("runner_availability"), _RUNNER_AVAILABILITY
                ),
                "runner_connection_state": _safe_status(
                    raw_worker.get("runner_connection_state"), _RUNNER_CONNECTION_STATES
                ),
                "current_work_items": _work_items(raw_worker.get("current_work_items")),
            }
        )
    return workers


def _work_items(value: object) -> list[dict[str, Any]]:
    raw_items = value if isinstance(value, list) else []
    items: list[dict[str, Any]] = []
    for index, raw_item in enumerate(raw_items, 1):
        if not isinstance(raw_item, Mapping):
            continue
        label = raw_item.get("display_label")
        closure = _closure_summary(raw_item.get("closure_summary"))
        attempt_state = _safe_status(
            raw_item.get("attempt_state"), _ATTEMPT_STATES
        ) or _safe_status(raw_item.get("stage"), _ATTEMPT_STATES)
        report_summary = sanitize_operator_text(
            raw_item.get("latest_report_summary")
        ) or sanitize_operator_text(raw_item.get("latest_worker_report_summary"))
        pending_boundary = raw_item.get("pending_supervisor_boundary") is True or (
            "pending_supervisor_boundary" not in raw_item
            and raw_item.get("next_observable_boundary") == "pending"
        )
        items.append(
            {
                "display_label": label
                if isinstance(label, str) and _DISPLAY_LABEL.fullmatch(label)
                else f"Work item {index}",
                "worker_label": sanitize_operator_text(raw_item.get("worker_label")),
                "history_reference": _safe_digest(raw_item.get("history_reference")),
                "work_title": sanitize_operator_text(raw_item.get("work_title")),
                "objective_summary": sanitize_operator_text(raw_item.get("objective_summary")),
                "objective_text": full_operator_text(raw_item.get("objective_text")),
                "state": _safe_enum(raw_item.get("state")),
                "attempt_state": attempt_state,
                "progress_stage": sanitize_operator_text(raw_item.get("progress_stage")),
                "trajectory": _safe_enum(raw_item.get("trajectory")),
                "attention_owner": _safe_enum(raw_item.get("attention_owner")),
                "next_boundary_summary": sanitize_operator_text(
                    raw_item.get("next_boundary_summary")
                ),
                "pending_supervisor_boundary": pending_boundary,
                "recovery_action": _safe_status(raw_item.get("recovery_action"), _RECOVERY_ACTIONS),
                "recovery_waiting_since": _safe_timestamp(raw_item.get("recovery_waiting_since")),
                "recovery_notification_state": _safe_status(
                    raw_item.get("recovery_notification_state"),
                    _RECOVERY_NOTIFICATION_STATES,
                ),
                "cao_supervision_state": _safe_status(
                    raw_item.get("cao_supervision_state"),
                    _CAO_SUPERVISION_STATES,
                ),
                "cao_supervision_updated_at": _safe_timestamp(
                    raw_item.get("cao_supervision_updated_at")
                ),
                "latest_report_kind": _safe_status(
                    raw_item.get("latest_report_kind"), _REPORT_KINDS
                ),
                "latest_report_summary": report_summary,
                "latest_report_text": full_operator_text(raw_item.get("latest_report_text")),
                "latest_reported_at": _safe_timestamp(raw_item.get("latest_reported_at")),
                "runtime_heartbeat_at": _safe_timestamp(raw_item.get("runtime_heartbeat_at")),
                "last_worker_activity_at": _safe_timestamp(raw_item.get("last_worker_activity_at")),
                "last_artifact_at": _safe_timestamp(raw_item.get("last_artifact_at")),
                "status_request_state": _safe_status(
                    raw_item.get("status_request_state"), _STATUS_REQUEST_STATES
                ),
                "status_requested_at": _safe_timestamp(raw_item.get("status_requested_at")),
                "status_response_due_at": _safe_timestamp(raw_item.get("status_response_due_at")),
                "status_responded_at": _safe_timestamp(raw_item.get("status_responded_at")),
                "completion_contract": _safe_status(
                    raw_item.get("completion_contract"), _COMPLETION_CONTRACTS
                ),
                "delivery_state": _safe_status(raw_item.get("delivery_state"), _DELIVERY_STATES),
                # Compatibility aliases for older v1 consumers.
                "stage": attempt_state,
                "next_observable_boundary": "pending" if pending_boundary else None,
                "latest_worker_report_summary": report_summary,
                "runner_adapter": _safe_status(raw_item.get("runner_adapter"), _RUNNER_ADAPTERS),
                "runner_model": _model_label(raw_item.get("runner_model")),
                "runner_reasoning_effort": _safe_status(
                    raw_item.get("runner_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_requested_model": _model_label(raw_item.get("runner_requested_model")),
                "runner_effective_model": _model_label(raw_item.get("runner_effective_model")),
                "runner_requested_reasoning_effort": _safe_status(
                    raw_item.get("runner_requested_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_effective_reasoning_effort": _safe_status(
                    raw_item.get("runner_effective_reasoning_effort"), _REASONING_EFFORTS
                ),
                "runner_availability": _safe_status(
                    raw_item.get("runner_availability"), _RUNNER_AVAILABILITY
                ),
                "runner_state": _safe_status(raw_item.get("runner_state"), _RUNNER_SPEC_STATES),
                "runner_connection_state": _safe_status(
                    raw_item.get("runner_connection_state"), _RUNNER_CONNECTION_STATES
                ),
                "latest_cao_review_decision": closure["cao_review"],
                "requester_decision": closure["requester_decision"],
                "closure_state": _closure_state(
                    raw_item.get("closure_state"), raw_item.get("state")
                ),
                "closure_summary": closure,
                "completed_at": _safe_timestamp(raw_item.get("completed_at")),
                "availability": _work_availability(raw_item.get("availability")),
            }
        )
    return items


def _work_availability(value: object) -> dict[str, str]:
    raw = value if isinstance(value, Mapping) else {}
    return {
        key: _safe_status(raw.get(key), _AVAILABILITY_STATES) or "unavailable"
        for key in _WORK_AVAILABILITY_KEYS
    }


def _safe_enum(value: object) -> str | None:
    return value if isinstance(value, str) and _SAFE_LABEL.fullmatch(value) else None


def _closure_state(value: object, work_state: object) -> str:
    if isinstance(value, str) and value in _CLOSURE_STATES:
        return value
    return "awaiting-explicit-close" if work_state == "completed" else "open"


def _closure_summary(value: object) -> dict[str, str | int | None]:
    raw = value if isinstance(value, Mapping) else {}
    return {
        "requester_decision": _safe_status(raw.get("requester_decision"), _REQUESTER_DECISIONS),
        "cao_review": _safe_status(raw.get("cao_review"), _CAO_REVIEW_DECISIONS),
        "artifact_preservation": _safe_status(
            raw.get("artifact_preservation"), _ARTIFACT_PRESERVATION_STATES
        ),
        "cleanup": _safe_status(raw.get("cleanup"), _CLEANUP_STATES),
        "unresolved_deliveries": _optional_count(raw.get("unresolved_deliveries")),
        "unresolved_effects": _optional_count(raw.get("unresolved_effects")),
        "active_runtimes": _optional_count(raw.get("active_runtimes")),
    }


def _safe_status(value: object, allowed: frozenset[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _model_label(value: object) -> str | None:
    """Retain only a canonical, bounded model identifier from the DTO."""

    return model_identifier(value)


def _safe_timestamp(value: object) -> str | None:
    """Retain only the canonical bounded timestamp from the DTO."""

    return value if isinstance(value, str) and _OPERATOR_TIMESTAMP.fullmatch(value) else None


def _runtime_delivery(value: object) -> dict[str, Any]:
    raw = value if isinstance(value, Mapping) else {}
    effects = raw.get("effects_by_state", {})
    return {
        "runtime_count": _count(raw.get("runtime_count")),
        "queued_deliveries": _count(raw.get("queued_deliveries")),
        "unknown_delivery_outcomes": _count(raw.get("unknown_delivery_outcomes")),
        "dead_deliveries": _count(raw.get("dead_deliveries")),
        "effects_by_state": {
            key: _count(item)
            for key, item in effects.items()
            if isinstance(effects, Mapping) and isinstance(key, str) and _SAFE_LABEL.fullmatch(key)
        }
        if isinstance(effects, Mapping)
        else {},
    }


def _count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _optional_count(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _cursor(value: str) -> str:
    if not value or len(value.encode("utf-8")) > _MAX_CURSOR_BYTES:
        raise ValueError("invalid dashboard cursor")
    return value


def _dashboard_json(response: httpx.Response) -> dict[str, Any]:
    if response.status_code != 200:
        raise RuntimeError("dashboard upstream is unavailable")
    try:
        body = response.json()
    except ValueError as error:
        raise RuntimeError("dashboard upstream returned an invalid response") from error
    if not isinstance(body, dict) or body.get("format") != "cao-dashboard-read-model/v1":
        raise RuntimeError("dashboard upstream returned an unexpected response")
    return body


def create_dashboard_edge(
    settings: DashboardEdgeSettings,
    *,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    """Create a standalone, read-only dashboard app.

    The factory intentionally accepts configuration rather than ``Settings``
    from the control plane, keeping the edge independent of its database and
    service internals.
    """

    bootstrap = (
        FileBootstrapSecretStore(settings.bootstrap_record_dir, clock)
        if settings.bootstrap_record_dir is not None
        else _ExpiringSecretStore(settings.bootstrap_secrets, clock)
    )
    sessions = (
        FileSessionStore(settings.session_record_dir, settings.session_ttl_seconds, clock)
        if settings.session_record_dir is not None
        else _SessionStore(settings.session_ttl_seconds, clock)
    )
    rendered_snapshot = _RenderedSnapshotTracker()

    def stream_count_changed(value: int) -> None:
        if value == 0:
            rendered_snapshot.clear()
        if settings.on_authenticated_stream_count is not None:
            settings.on_authenticated_stream_count(value)

    active_streams = _ActiveStreamCounter(stream_count_changed)
    access_validator = settings.cloudflare_access_validator
    if access_validator is None and settings.cloudflare_access is not None:
        access_validator = CloudflareAccessJWTValidator(settings.cloudflare_access)
    static_dir = Path(__file__).with_name("static")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        active_streams.notify_current()
        yield

    app = FastAPI(
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.dashboard_active_streams = active_streams
    app.state.dashboard_rendered_snapshot = rendered_snapshot
    app.mount("/dashboard/static", StaticFiles(directory=str(static_dir)), name="dashboard-static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Content-Security-Policy", _CSP)
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Permissions-Policy", "camera=(), microphone=(), geolocation=()"
        )
        return response

    @app.get("/dashboard", include_in_schema=False)
    @app.get("/dashboard/", include_in_schema=False)
    async def dashboard_page() -> FileResponse:
        return FileResponse(static_dir / "dashboard.html", media_type="text/html")

    @app.post("/dashboard/session", include_in_schema=False)
    async def create_session(request: Request) -> Response:
        if settings.public_origin:
            origin = request.headers.get("origin", "")
            if origin and origin != settings.public_origin:
                return _edge_error(403, "dashboard bootstrap was rejected")
        content_length = request.headers.get("content-length", "")
        if content_length:
            try:
                if int(content_length) > 512:
                    return _edge_error(413, "dashboard bootstrap was rejected")
            except ValueError:
                return _edge_error(400, "dashboard bootstrap was rejected")
        raw = await request.body()
        if len(raw) > 512:
            return _edge_error(413, "dashboard bootstrap was rejected")
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _edge_error(400, "dashboard bootstrap was rejected")
        secret = body.get("secret") if isinstance(body, dict) and set(body) == {"secret"} else None
        if not isinstance(secret, str) or not bootstrap.consume(secret):
            return _edge_error(401, "dashboard bootstrap was rejected")
        response = Response(status_code=204)
        response.set_cookie(
            _SESSION_COOKIE,
            sessions.issue(),
            max_age=settings.session_ttl_seconds,
            httponly=True,
            secure=settings.secure_cookie,
            samesite="strict",
            path="/dashboard",
        )
        return response

    async def require_session(request: Request) -> bool:
        if sessions.valid(request.cookies.get(_SESSION_COOKIE, "")):
            return True
        if access_validator is None:
            return False
        assertion = request.headers.get("cf-access-jwt-assertion", "")
        return await access_validator.authorized(assertion)

    @app.post("/dashboard/session/rendered", include_in_schema=False)
    async def rendered(request: Request) -> Response:
        if not await require_session(request):
            return _edge_error(401, "dashboard session is required")
        if settings.public_origin:
            origin = request.headers.get("origin", "")
            if origin and origin != settings.public_origin:
                return _edge_error(403, "dashboard render observation was rejected")
        content_length = request.headers.get("content-length", "")
        if content_length:
            try:
                if int(content_length) > 8192:
                    return _edge_error(413, "dashboard render observation was rejected")
            except ValueError:
                return _edge_error(400, "dashboard render observation was rejected")
        raw = await request.body()
        if len(raw) > 8192:
            return _edge_error(413, "dashboard render observation was rejected")
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _edge_error(400, "dashboard render observation was rejected")
        if not isinstance(body, dict) or set(body) != {"cursor", "snapshot_digest"}:
            return _edge_error(400, "dashboard render observation was rejected")
        cursor = body.get("cursor")
        digest = _safe_digest(body.get("snapshot_digest"))
        if not isinstance(cursor, str) or not digest:
            return _edge_error(400, "dashboard render observation was rejected")
        try:
            rendered_snapshot.observe(_cursor(cursor), digest)
        except ValueError:
            return _edge_error(400, "dashboard render observation was rejected")
        return Response(status_code=204)

    @app.get("/dashboard/internal/visibility", include_in_schema=False)
    async def visibility(request: Request) -> Response:
        expected = f"Bearer {settings.dashboard_bearer}"
        supplied = request.headers.get("authorization", "")
        if not hmac.compare_digest(supplied, expected):
            return _edge_error(401, "dashboard visibility credential is required")
        rendered_cursor, rendered_digest = rendered_snapshot.value()
        return JSONResponse(
            {
                "format": "cao-dashboard-visibility/v2",
                "active_streams": active_streams.value(),
                "rendered_cursor": rendered_cursor,
                "rendered_snapshot_digest": rendered_digest,
            }
        )

    async def upstream_json(path: str, params: Mapping[str, str | int]) -> Response:
        try:
            async with _async_client(settings, history=path.endswith("/work-history")) as client:
                response = await client.get(path, params=params)
                if response.status_code == 409:
                    return _bounded_json_response(response, 409, path=path)
                if response.status_code != 200:
                    return _edge_error(502, "dashboard upstream is unavailable")
                return _bounded_json_response(response, 200, path=path)
        except (httpx.HTTPError, ValueError):
            return _edge_error(502, "dashboard upstream is unavailable")

    @app.get("/dashboard/api/snapshot", include_in_schema=False)
    async def snapshot(request: Request) -> Response:
        if not await require_session(request):
            return _edge_error(401, "dashboard session is required")
        return await upstream_json(f"{_API_PREFIX}/snapshot", {})

    @app.get("/dashboard/api/history", include_in_schema=False)
    async def history(request: Request, after: str = "", limit: int = 100) -> Response:
        if not await require_session(request):
            return _edge_error(401, "dashboard session is required")
        if after:
            try:
                _cursor(after)
            except ValueError:
                return _edge_error(400, "invalid dashboard cursor")
        if not 1 <= limit <= settings.max_history_limit:
            return _edge_error(400, "invalid dashboard history limit")
        params: dict[str, str | int] = {"limit": limit}
        if after:
            params["after"] = after
        return await upstream_json(f"{_API_PREFIX}/history", params)

    @app.get("/dashboard/api/work-history", include_in_schema=False)
    async def work_history(request: Request, work: str = "", before: str = "", limit: int = 20) -> Response:
        if not await require_session(request):
            return _edge_error(401, "dashboard session is required")
        if not 1 <= limit <= 100 or any(
            value and not HISTORY_REFERENCE.fullmatch(value) for value in (work, before)
        ):
            return _edge_error(400, "invalid dashboard history request")
        return await upstream_json(f"{_API_PREFIX}/work-history", {"work": work, "before": before, "limit": limit})

    @app.get("/dashboard/api/stream", include_in_schema=False)
    async def stream(request: Request, after: str = "") -> Response:
        if not await require_session(request):
            return _edge_error(401, "dashboard session is required")
        last_event_id = request.headers.get("last-event-id", "").strip()
        # EventSource reconnects to its original URL and supplies the newest
        # server-issued id in Last-Event-ID.  Consequently ``after`` can be a
        # valid but older bootstrap cursor on a normal reconnect.  Validate
        # both inputs, then prefer the protocol-owned header.
        for candidate in (after, last_event_id):
            if not candidate:
                continue
            try:
                _cursor(candidate)
            except ValueError:
                return _edge_error(400, "invalid dashboard cursor")
        cursor = last_event_id or after

        async def relay() -> AsyncIterator[bytes]:
            # Keep one upstream stream for the life of this browser request.
            # EventSource reconnects with Last-Event-ID only after a transport
            # failure or an intentional upstream close.
            async with (
                _async_client(settings, stream=True) as client,
                client.stream(
                    "GET",
                    f"{_API_PREFIX}/stream",
                    headers={"Last-Event-ID": cursor} if cursor else {},
                ) as response,
            ):
                if response.status_code != 200 or not response.headers.get(
                    "content-type", ""
                ).startswith("text/event-stream"):
                    yield b'event: edge-error\ndata: {"error":"dashboard stream unavailable"}\n\n'
                    return
                if response.is_stream_consumed:
                    # Some in-process transports pre-buffer a finite response.
                    # A real upstream SSE response remains unconsumed here.
                    if response.content:
                        yield response.content
                    return
                async for chunk in _relay_sse_chunks(response):
                    yield chunk

        return StreamingResponse(
            _tracked_stream(active_streams, relay()),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    return app


async def _relay_sse_chunks(response: httpx.Response) -> AsyncIterator[bytes]:
    """Forward each upstream SSE transport chunk without aggregation.

    Passing a positive ``chunk_size`` to httpx's ``aiter_raw`` asks httpx to
    accumulate bytes until that size is reached.  Dashboard events are tiny
    and the stream is intentionally long lived, so such aggregation can hold
    an otherwise complete SSE frame indefinitely.  Consume the transport's
    natural chunks and only split an unexpectedly large chunk on output; this
    keeps the relay bounded without turning the edge into a polling or
    buffering boundary.
    """

    async for chunk in response.aiter_raw():
        if not chunk:
            continue
        for offset in range(0, len(chunk), _MAX_STREAM_CHUNK_BYTES):
            yield chunk[offset : offset + _MAX_STREAM_CHUNK_BYTES]


def _async_client(
    settings: DashboardEdgeSettings, *, stream: bool = False, history: bool = False
) -> httpx.AsyncClient:
    timeout = httpx.Timeout(
        connect=settings.connect_timeout_seconds,
        read=(
            settings.read_timeout_seconds
            if stream
            else min(settings.read_timeout_seconds, 30.0 if history else 15.0)
        ),
        write=5.0,
        pool=5.0,
    )
    return httpx.AsyncClient(
        base_url=settings.upstream_base_url,
        headers={
            "Authorization": f"Bearer {settings.dashboard_bearer}",
            "Accept": "text/event-stream" if stream else "application/json",
        },
        timeout=timeout,
        follow_redirects=False,
        transport=settings.transport,
    )


def _bounded_json_response(response: httpx.Response, status_code: int, *, path: str) -> Response:
    raw = response.content
    if len(raw) > _MAX_JSON_BYTES:
        return _edge_error(502, "dashboard upstream is unavailable")
    try:
        body = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _edge_error(502, "dashboard upstream is unavailable")
    if not isinstance(body, dict) or body.get("format") != _DASHBOARD_FORMAT:
        return _edge_error(502, "dashboard upstream is unavailable")
    if path.endswith("/snapshot") and status_code == 200:
        return JSONResponse(_edge_snapshot(body), status_code=status_code)
    if path.endswith("/work-history") and status_code == 200:
        return JSONResponse(_edge_work_history(body))
    if path.endswith("/history"):
        return JSONResponse(_edge_history(body, status_code=status_code), status_code=status_code)
    return _edge_error(502, "dashboard upstream is unavailable")


def _edge_work_history(body: Mapping[str, Any]) -> dict[str, Any]:
    """Independently allowlist the reader; never forward upstream payloads."""
    items = []
    for raw in body.get("items", []) if isinstance(body.get("items"), list) else []:
        if not isinstance(raw, Mapping) or not _safe_digest(raw.get("history_reference")):
            continue
        items.append(
            {
                "history_reference": _safe_digest(raw.get("history_reference")),
                "work_title": sanitize_operator_text(raw.get("work_title")),
                "worker_label": sanitize_operator_text(raw.get("worker_label")),
                "completed_at": _safe_timestamp(raw.get("completed_at")),
                "state": _safe_enum(raw.get("state")),
            }
        )
    entries = []
    for raw in body.get("entries", []) if isinstance(body.get("entries"), list) else []:
        if (
            not isinstance(raw, Mapping)
            or not isinstance(raw.get("kind"), str)
            or raw["kind"] not in HISTORY_KINDS
        ):
            continue
        reference = _safe_digest(raw.get("reference"))
        if not reference:
            continue
        entries.append(
            {
                "reference": reference,
                "kind": raw["kind"],
                "occurred_at": _safe_timestamp(raw.get("occurred_at")),
                "text": full_operator_text(raw.get("text")),
                "outcome": _safe_status(
                    raw.get("outcome"),
                    frozenset(
                        {
                            "ok",
                            "needs-work",
                            "accepted",
                            "rejected",
                            "accept",
                            "continue",
                            "correct",
                            "pause",
                            "wait_user",
                            "escalate",
                            "close",
                            "cancel",
                            "reconcile",
                        }
                    ),
                ),
            }
        )
    work = _work_items([body["work"]]) if isinstance(body.get("work"), Mapping) else []
    return {
        "format": _DASHBOARD_FORMAT,
        "items": items,
        "entries": entries,
        "work": work[0] if work else None,
        "has_more": body.get("has_more") is True,
        "next_before": _safe_digest(body.get("next_before")),
    }


def _edge_snapshot(body: Mapping[str, Any]) -> dict[str, Any]:
    authority = body.get("authority", {})
    return {
        "format": _DASHBOARD_FORMAT,
        "authority": _authority(authority),
        "cursor": _safe_cursor(body.get("cursor")),
        "operator": _operator_view(body),
        "snapshot_digest": _safe_digest(body.get("snapshot_digest")),
    }


def _edge_history(body: Mapping[str, Any], *, status_code: int) -> dict[str, Any]:
    if status_code == 409:
        return {
            "format": _DASHBOARD_FORMAT,
            "status": "resync-required",
            "reason": _safe_enum(body.get("reason")) or "unknown",
            "authority_generation": _count(body.get("authority_generation")),
            "cursor": _safe_cursor(body.get("cursor")),
        }
    raw_items = body.get("items", [])
    items = []
    if isinstance(raw_items, list):
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                continue
            event = raw.get("event", {})
            if not isinstance(event, Mapping):
                continue
            event_type = event.get("type")
            aggregate_type = event.get("aggregate_type")
            occurred_at = event.get("occurred_at")
            if not (
                isinstance(event_type, str)
                and _SAFE_EVENT.fullmatch(event_type)
                and isinstance(aggregate_type, str)
                and _SAFE_LABEL.fullmatch(aggregate_type)
                and isinstance(occurred_at, str)
                and len(occurred_at) <= 64
            ):
                continue
            items.append(
                {
                    "cursor": _safe_cursor(raw.get("cursor")),
                    "event": {
                        "type": event_type,
                        "aggregate_type": aggregate_type,
                        "occurred_at": occurred_at,
                    },
                }
            )
    return {
        "format": _DASHBOARD_FORMAT,
        "authority": _authority(body.get("authority")),
        "after": _safe_cursor(body.get("after")) if body.get("after") else None,
        "items": items,
        "next_cursor": _safe_cursor(body.get("next_cursor")),
        "has_more": body.get("has_more") is True,
    }


def _authority(value: object) -> dict[str, Any]:
    raw = value if isinstance(value, Mapping) else {}
    mode = raw.get("mode")
    return {
        "mode": mode if mode in {"canonical", "missing"} else "unknown",
        "generation": _count(raw.get("generation")),
    }


def _safe_cursor(value: object) -> str:
    return (
        value
        if isinstance(value, str) and 0 < len(value.encode("utf-8")) <= _MAX_CURSOR_BYTES
        else ""
    )


def _safe_digest(value: object) -> str:
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) else ""


def _edge_error(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": "dashboard_unavailable", "message": message}}, status_code=status_code
    )
