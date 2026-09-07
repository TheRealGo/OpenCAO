"""Fail-closed local lifecycle contract for the private dashboard deployment.

The module deliberately separates planning from effects.  ``plan`` and
``status`` only interpret injected command/HTTP observations.  ``apply``,
``upgrade``, ``remove``, and ``refresh-mcp`` require a matching local
authorization receipt or an explicit owner-confirmed execution boundary, and
never retry an operation with an unknown outcome.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import hashlib
import json
import os
import plistlib
import re
import secrets
import shlex
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Literal, Protocol

import httpx

from .canonical import canonical_sha256
from .config import Settings
from .dashboard_credentials import _read_owner_only_file, load_dashboard_credentials
from .database import (
    Database,
    SQLiteBackupResult,
    SQLiteDatabaseIdentity,
    backup_sqlite_database,
    inspect_sqlite_database,
)
from .projection import verify_projection
from .release_identity import ReleaseIdentity, current_release_identity

if TYPE_CHECKING:
    from .runtime import _JsonRpcDesktopSocket

_FORMAT = "cao-dashboard-lifecycle/v2"
_UPGRADE_FORMAT = "cao-dashboard-lifecycle-upgrade/v1"
_MCP_REFRESH_FORMAT = "cao-dashboard-lifecycle-refresh-mcp/v1"
_RECEIPT_FORMAT = "cao-dashboard-lifecycle-effect-receipt/v1"
_CP_LABEL = "dev.cao.dashboard.control-plane"
_EDGE_LABEL = "dev.cao.dashboard.edge"
_CLOUDFLARED_LABEL = "dev.cao.dashboard.cloudflare-tunnel"
_SHA256_LENGTH = 64
_PROCESS_EXIT_ATTEMPTS = 100
_PROCESS_EXIT_INTERVAL_SECONDS = 0.05
_DARWIN_PROC_PIDTBSDINFO = 3
_DARWIN_TASK_AUDIT_TOKEN = 15
_DARWIN_AUDIT_TOKEN_WORDS = 8
_CODEX_MCP_RELOAD_SCOPE = "all_loaded_codex_threads"
_CODEX_MCP_RELOAD_APPLICATION = "next_active_turn"
_CODEX_MCP_RELOAD_STATUS = "submitted_for_next_active_turn"
_CODEX_APP_SERVER_RESTART_STATUS = "not_performed"
_CODEX_APP_SERVER_RESTART_PERFORMED_STATUS = "performed"
_CODEX_CURRENT_CONVERSATION_VERIFICATION = "pending"
_OPENAI_TEAM_IDENTIFIER = "2DC432GLL2"
_CODEX_COMMAND_TIMEOUT_SECONDS = 20.0
_CODEX_HOST_RESTART_ATTEMPTS = 100
_CODEX_HOST_RESTART_INTERVAL_SECONDS = 0.05
_CODEX_DESKTOP_ARGUMENTS_AFTER_EXECUTABLE = (
    "-c",
    "features.code_mode_host=true",
    "app-server",
    "--listen",
    "unix://",
    "--analytics-default-enabled",
)
_CODEX_DESKTOP_LAUNCHAGENT_REQUIRED_KEYS = frozenset(
    {
        "KeepAlive",
        "Label",
        "ProcessType",
        "ProgramArguments",
        "RunAtLoad",
        "StandardErrorPath",
        "StandardOutPath",
        "ThrottleInterval",
    }
)
_CODEX_DESKTOP_LAUNCHAGENT_OPTIONAL_KEYS = frozenset(
    {
        "EnvironmentVariables",
        "HardResourceLimits",
        "SoftResourceLimits",
    }
)
_CODEX_LAUNCHAGENT_RESOURCE_LIMIT_KEYS = frozenset(
    {
        "Core",
        "CPU",
        "Data",
        "FileSize",
        "MemoryLock",
        "NumberOfFiles",
        "NumberOfProcesses",
        "ResidentSetSize",
        "Stack",
    }
)
_CODEX_USER_AGENT_PATTERN = re.compile(r"(?:^|[^a-z0-9])codex(?:[^a-z0-9]|$)", re.IGNORECASE)
_CODEX_APP_SERVER_PROBE_CLIENT_NAME = "codex_app_server_daemon"
_LEGACY_LIFECYCLE_USER_AGENT_PREFIX = "cao-dashboard-lifecycle/"
_HOSTNAME = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)


def _valid_hostname(value: str, *, suffix: str | None) -> bool:
    normalized = value.lower().rstrip(".")
    return _HOSTNAME.fullmatch(normalized) is not None and (
        suffix is None or normalized.endswith(suffix)
    )


class LifecycleUnknownOutcome(RuntimeError):
    """An effect may have happened; callers must inspect state before retrying."""


class CodexMCPRefreshPhaseError(RuntimeError):
    """A confirmed host restart stopped before the reload request was sent."""

    def __init__(self, message: str, *, reload_status: str) -> None:
        super().__init__(message)
        self.restart_status = _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS
        self.reload_status = reload_status

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "stopped",
            "reason_code": "codex_mcp_refresh_incomplete",
            "retryable": False,
            "codex_app_server_restart": self.restart_status,
            "codex_mcp_reload": self.reload_status,
            "current_conversation_verification": (_CODEX_CURRENT_CONVERSATION_VERIFICATION),
        }


class CodexMCPRefreshPhaseUnknown(
    CodexMCPRefreshPhaseError,
    LifecycleUnknownOutcome,
):
    """The restart is confirmed but the reload request outcome is unknown."""

    def to_dict(self) -> dict[str, object]:
        value = super().to_dict()
        value["reason_code"] = "codex_mcp_reload_outcome_unknown"
        return value


@dataclass(frozen=True, slots=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(self, argv: Sequence[str]) -> CommandResult: ...


class HttpProbe(Protocol):
    def get(self, url: str, headers: Mapping[str, str]) -> HttpResult: ...


@dataclass(frozen=True, slots=True)
class HttpResult:
    status_code: int
    headers: Mapping[str, str]
    body: Mapping[str, object] | None = None


class SubprocessRunner:
    """The production runner; planning tests should inject a fake instead."""

    def run(self, argv: Sequence[str]) -> CommandResult:
        completed = subprocess.run(list(argv), check=False, capture_output=True, text=True)
        return CommandResult(tuple(argv), completed.returncode, completed.stdout, completed.stderr)


@dataclass(frozen=True, slots=True)
class DashboardLifecycleSettings:
    """Non-secret deployment settings.

    The credential file path is allowed in a plist, but its bearer value is
    never read into, passed through, or logged by a LaunchAgent definition.
    """

    application_support_dir: Path
    working_directory: Path
    control_plane_command: tuple[str, ...]
    credentials_file: Path
    bootstrap_record_dir: Path
    session_record_dir: Path
    dashboard_command: tuple[str, ...] = ("cao-dashboard",)
    cp_port: int = 8768
    edge_port: int = 8769
    tailscale_binary: str = "tailscale"
    exposure_provider: Literal["tailscale", "cloudflare"] = "tailscale"
    cloudflared_binary: str = "cloudflared"
    cloudflare_token_file: Path | None = None
    cloudflare_hostname: str | None = None
    cloudflare_access_team_domain: str | None = None
    cloudflare_metrics_port: int = 8770
    launchagent_dir_override: Path | None = None
    log_dir_override: Path | None = None
    control_plane_log_name: str = f"{_CP_LABEL}.log"
    edge_log_name: str = f"{_EDGE_LABEL}.log"
    cloudflare_log_name: str = f"{_CLOUDFLARED_LABEL}.log"

    def __post_init__(self) -> None:
        if not self.control_plane_command:
            raise ValueError("control_plane_command is required")
        if not self.dashboard_command:
            raise ValueError("dashboard_command is required")
        if not 1 <= self.cp_port <= 65535 or not 1 <= self.edge_port <= 65535:
            raise ValueError("dashboard service ports must be valid TCP ports")
        if self.cp_port == self.edge_port:
            raise ValueError("control-plane and edge ports must differ")
        if (
            not 1 <= self.cloudflare_metrics_port <= 65535
            or self.cloudflare_metrics_port in {self.cp_port, self.edge_port}
        ):
            raise ValueError("cloudflared metrics port must be valid and distinct")
        if not self.working_directory.is_absolute():
            raise ValueError("working_directory must be absolute")
        _reject_secret_arguments(self.control_plane_command)
        _reject_secret_arguments(self.dashboard_command)
        if self.exposure_provider not in {"tailscale", "cloudflare"}:
            raise ValueError("Dashboard exposure provider is invalid")
        if self.launchagent_dir_override is not None:
            expected = Path.home() / "Library" / "LaunchAgents"
            if not self.launchagent_dir_override.is_absolute() or self.launchagent_dir_override != expected:
                raise ValueError("LaunchAgent override must be the owner's canonical directory")
        if self.log_dir_override is not None and not self.log_dir_override.is_absolute():
            raise ValueError("Dashboard log directory override must be absolute")
        for name in (
            self.control_plane_log_name,
            self.edge_log_name,
            self.cloudflare_log_name,
        ):
            if (
                not name
                or name in {".", ".."}
                or Path(name).name != name
                or any(character in name for character in ("\x00", "\n", "\r"))
            ):
                raise ValueError("Dashboard log filename is invalid")
        if self.exposure_provider == "cloudflare":
            hostname = (self.cloudflare_hostname or "").lower().rstrip(".")
            team_domain = (self.cloudflare_access_team_domain or "").lower().rstrip(".")
            if self.cloudflare_token_file is None or not self.cloudflare_token_file.is_absolute():
                raise ValueError("Cloudflare tunnel token file must be an absolute path")
            if not _valid_hostname(hostname, suffix=None):
                raise ValueError("Cloudflare Dashboard hostname is invalid")
            if not _valid_hostname(team_domain, suffix=".cloudflareaccess.com"):
                raise ValueError("Cloudflare Access team domain is invalid")
            if not self.cloudflared_binary or any(
                character in self.cloudflared_binary for character in ("\x00", "\n", "\r")
            ):
                raise ValueError("cloudflared binary is invalid")
            if not Path(self.cloudflared_binary).is_absolute():
                raise ValueError("cloudflared binary must be an absolute path")
            object.__setattr__(self, "cloudflare_hostname", hostname)
            object.__setattr__(self, "cloudflare_access_team_domain", team_domain)

    @property
    def launchagent_dir(self) -> Path:
        return self.launchagent_dir_override or self.application_support_dir / "launchagents"

    @property
    def log_dir(self) -> Path:
        return self.log_dir_override or self.application_support_dir / "logs"

    @property
    def control_plane_plist(self) -> Path:
        return self.launchagent_dir / f"{_CP_LABEL}.plist"

    @property
    def edge_plist(self) -> Path:
        return self.launchagent_dir / f"{_EDGE_LABEL}.plist"

    @property
    def edge_origin(self) -> str:
        return f"http://127.0.0.1:{self.edge_port}"

    @property
    def cp_dashboard_url(self) -> str:
        return f"http://127.0.0.1:{self.cp_port}/api/v1/dashboard/v1/snapshot"


def _reject_secret_arguments(argv: Sequence[str]) -> None:
    forbidden = ("--token", "--bearer", "--password", "--secret", "authorization=")
    if any(any(marker in value.lower() for marker in forbidden) for value in argv):
        raise ValueError("service commands must not embed credentials or secrets")


@dataclass(frozen=True, slots=True)
class LaunchAgentPlan:
    label: str
    plist_path: str
    plist_sha256: str
    program_arguments: tuple[str, ...]
    working_directory: str
    log_path: str


@dataclass(frozen=True, slots=True)
class TailscaleServePlan:
    version_command: tuple[str, ...]
    serve_status_command: tuple[str, ...]
    tailnet_status_command: tuple[str, ...]
    apply_command: tuple[str, ...]
    remove_command: tuple[str, ...]
    expected_edge_origin: str
    prohibited_control_plane_origin: str


@dataclass(frozen=True, slots=True)
class CloudflareTunnelPlan:
    launch_agent: LaunchAgentPlan
    version_command: tuple[str, ...]
    ready_command: tuple[str, ...]
    token_file: str
    metrics_origin: str
    hostname: str
    public_origin: str
    access_team_domain: str
    expected_edge_origin: str
    prohibited_control_plane_origin: str


@dataclass(frozen=True, slots=True)
class DashboardLifecyclePlan:
    format: str
    control_plane: LaunchAgentPlan
    edge: LaunchAgentPlan
    tailscale: TailscaleServePlan | None
    cloudflare: CloudflareTunnelPlan | None
    plan_digest: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @property
    def expected_edge_origin(self) -> str:
        exposure = self.tailscale or self.cloudflare
        if exposure is None:  # pragma: no cover - constructor invariant
            raise ValueError("Dashboard exposure is unavailable")
        return exposure.expected_edge_origin

    @property
    def prohibited_control_plane_origin(self) -> str:
        exposure = self.tailscale or self.cloudflare
        if exposure is None:  # pragma: no cover - constructor invariant
            raise ValueError("Dashboard exposure is unavailable")
        return exposure.prohibited_control_plane_origin


@dataclass(frozen=True, slots=True)
class UpgradeLaunchAgentBinding:
    """Exact existing owner plist that may be restarted during an upgrade."""

    label: str
    plist_path: str
    plist_sha256: str
    working_directory: str
    loopback_port: int


@dataclass(frozen=True, slots=True)
class CodexDesktopLaunchAgentBinding:
    """Exact owner LaunchAgent allowed to replace one stale Desktop host."""

    label: str
    plist_path: str
    plist_sha256: str
    program_arguments: tuple[str, ...]
    environment_sha256: str


@dataclass(frozen=True, slots=True)
class DashboardUpgradePlan:
    """Reviewed, deterministic inputs for one code/schema upgrade."""

    format: str
    lifecycle_plan_digest: str
    control_plane: UpgradeLaunchAgentBinding
    edge: UpgradeLaunchAgentBinding
    database_path: str
    backup_destination: str
    source_identity: SQLiteDatabaseIdentity
    target_identity: ReleaseIdentity
    control_plane_origin: str
    edge_origin: str
    credentials_file: str
    codex_app_server_executable: str
    codex_app_server_executable_identity: tuple[int, int, int, int, int] | None
    codex_app_server_socket: str
    codex_desktop_launchagent: CodexDesktopLaunchAgentBinding | None
    codex_mcp_reload_scope: str
    codex_mcp_reload_application: str
    readiness_attempts: int
    readiness_interval_seconds: float
    plan_digest: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DashboardUpgradePreflightResult:
    database_identity: SQLiteDatabaseIdentity
    migration_identity: SQLiteDatabaseIdentity
    migration_projection_healthy: bool
    control_plane_plist_sha256: str
    edge_plist_sha256: str


@dataclass(frozen=True, slots=True)
class DashboardUpgradeResult:
    status: str
    plan_digest: str
    backup_path: str
    backup_sha256: str
    source_identity: SQLiteDatabaseIdentity
    backup_identity: SQLiteDatabaseIdentity
    target_identity: ReleaseIdentity
    codex_app_server_restart: str
    codex_mcp_reload: str
    codex_mcp_reload_scope: str
    codex_mcp_reload_application: str
    current_conversation_verification: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CodexMCPRefreshPlan:
    """Exact owner-local host action used to reload MCP configuration."""

    format: str
    target_identity: ReleaseIdentity
    codex_app_server_executable: str
    codex_app_server_executable_identity: tuple[int, int, int, int, int] | None
    codex_app_server_socket: str
    codex_desktop_launchagent: CodexDesktopLaunchAgentBinding | None
    codex_mcp_reload_scope: str
    codex_mcp_reload_application: str
    plan_digest: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CodexMCPRefreshResult:
    """Host refresh accepted; task-side catalog verification remains pending."""

    status: str
    plan_digest: str
    target_identity: ReleaseIdentity
    codex_app_server_restart: str
    codex_mcp_reload: str
    codex_mcp_reload_scope: str
    codex_mcp_reload_application: str
    current_conversation_verification: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CodexAppServerMCPPreflight:
    """Read-only host classification bound to one canonical socket inode."""

    backend: str
    socket_device: int
    socket_inode: int
    bundled_executable_identity: tuple[int, int, int, int, int]
    desktop_peer: CodexAppServerPeerObservation | None = None
    desktop_process: DarwinAuditProcessIdentity | None = None
    desktop_bridges: tuple[DarwinAuditProcessIdentity, ...] = ()
    desktop_restart_required: bool = False

    @property
    def socket_identity(self) -> tuple[int, int]:
        return (self.socket_device, self.socket_inode)


@dataclass(frozen=True, slots=True)
class CodexAppServerPeerObservation:
    """Kernel-bound process generation serving the canonical Desktop socket."""

    pid: int
    uid: int
    start_signature: str
    executable_path: str | None
    argv: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class DarwinAuditProcessIdentity:
    """One macOS process execution bound by its kernel audit token."""

    pid: int
    pgid: int
    uid: int
    asid: int
    audit_token: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class DashboardUpgradeProcessGroup:
    """Stable process-group membership captured before a LaunchAgent bootout."""

    pgid: int
    members: tuple[DarwinAuditProcessIdentity, ...]


class DashboardUpgradeReadinessProbe(Protocol):
    def require_preflight_ready(
        self, plan: DashboardUpgradePlan, *, target_projection_verified: bool
    ) -> None: ...

    def wait_for_stopped(self, plan: DashboardUpgradePlan) -> None: ...

    def wait_for_control_plane(self, plan: DashboardUpgradePlan) -> None: ...

    def wait_for_edge(self, plan: DashboardUpgradePlan) -> None: ...


class DashboardUpgradeProcessController(Protocol):
    """Bind and retire the exact LaunchAgent process generations."""

    def capture(
        self,
        runner: CommandRunner,
        binding: UpgradeLaunchAgentBinding,
    ) -> DashboardUpgradeProcessGroup: ...

    def fence_after_bootout(
        self,
        groups: Sequence[DashboardUpgradeProcessGroup],
    ) -> None: ...


class DashboardUpgradePreflight(Protocol):
    def __call__(
        self,
        plan: DashboardUpgradePlan,
        runner: CommandRunner,
        backup_database: DashboardUpgradeBackup,
    ) -> DashboardUpgradePreflightResult: ...


class DashboardUpgradeBackup(Protocol):
    def __call__(
        self,
        source: Path,
        destination: Path,
        *,
        replace: bool = True,
    ) -> SQLiteBackupResult: ...


class DashboardUpgradeMigrationPreflight(Protocol):
    def __call__(
        self,
        plan: DashboardUpgradePlan,
        backup_database: DashboardUpgradeBackup,
    ) -> SQLiteDatabaseIdentity: ...


class CodexMCPReloader(Protocol):
    def __call__(self, socket_path: Path, release_id: str) -> str | None: ...


class BoundedCodexCommandRunner(Protocol):
    def __call__(self, argv: Sequence[str], timeout: float) -> CommandResult: ...


class CodexAppServerHostInspector(Protocol):
    def inspect(self, socket_path: Path) -> CodexAppServerPeerObservation: ...

    def capture_process(
        self,
        pid: int,
        *,
        expected_asid: int,
    ) -> DarwinAuditProcessIdentity: ...

    def executable_path(self, process: DarwinAuditProcessIdentity) -> Path: ...

    def capture_cao_bridges(
        self,
        root_pid: int,
        *,
        expected_asid: int,
    ) -> tuple[DarwinAuditProcessIdentity, ...]: ...

    def is_live(self, process: DarwinAuditProcessIdentity) -> bool: ...


def _plist_bytes(value: Mapping[str, object]) -> bytes:
    return plistlib.dumps(dict(value), fmt=plistlib.FMT_XML, sort_keys=True)


def _launchagent_plist_bytes(
    plan: LaunchAgentPlan,
    *,
    program_arguments: Sequence[str] | None = None,
    keep_alive: object = True,
) -> bytes:
    return _plist_bytes(
        {
            "Label": plan.label,
            "ProgramArguments": list(program_arguments or plan.program_arguments),
            "WorkingDirectory": plan.working_directory,
            "StandardOutPath": plan.log_path,
            "StandardErrorPath": plan.log_path,
            "RunAtLoad": True,
            "KeepAlive": keep_alive,
            "ProcessType": "Background",
        }
    )


def _launch_agent(
    settings: DashboardLifecycleSettings,
    *,
    label: str,
    argv: tuple[str, ...],
    log_name: str,
) -> LaunchAgentPlan:
    path = settings.launchagent_dir / f"{label}.plist"
    log_path = settings.log_dir / log_name
    plist = {
        "Label": label,
        "ProgramArguments": list(argv),
        "WorkingDirectory": str(settings.working_directory),
        "StandardOutPath": str(log_path),
        "StandardErrorPath": str(log_path),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
    }
    return LaunchAgentPlan(
        label=label,
        plist_path=str(path),
        plist_sha256=hashlib.sha256(_plist_bytes(plist)).hexdigest(),
        program_arguments=argv,
        working_directory=str(settings.working_directory),
        log_path=str(log_path),
    )


def build_dashboard_lifecycle_plan(settings: DashboardLifecycleSettings) -> DashboardLifecyclePlan:
    """Build a deterministic plan without reading state or running a command."""

    control_plane = _launch_agent(
        settings,
        label=_CP_LABEL,
        log_name=settings.control_plane_log_name,
        argv=(
            *settings.control_plane_command,
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(settings.cp_port),
        ),
    )
    edge = _launch_agent(
        settings,
        label=_EDGE_LABEL,
        log_name=settings.edge_log_name,
        argv=(
            *settings.dashboard_command,
            "serve",
            "--credentials-file",
            str(settings.credentials_file),
            "--bootstrap-record-dir",
            str(settings.bootstrap_record_dir),
            "--session-record-dir",
            str(settings.session_record_dir),
            "--host",
            "127.0.0.1",
            "--port",
            str(settings.edge_port),
        ),
    )
    tailscale: TailscaleServePlan | None = None
    cloudflare: CloudflareTunnelPlan | None = None
    if settings.exposure_provider == "tailscale":
        tailscale = TailscaleServePlan(
            version_command=(settings.tailscale_binary, "version"),
            serve_status_command=(settings.tailscale_binary, "serve", "status", "--json"),
            tailnet_status_command=(settings.tailscale_binary, "status", "--json"),
            apply_command=(
                settings.tailscale_binary,
                "serve",
                "--https=443",
                settings.edge_origin,
            ),
            remove_command=(settings.tailscale_binary, "serve", "--https=443", "off"),
            expected_edge_origin=settings.edge_origin,
            prohibited_control_plane_origin=f"http://127.0.0.1:{settings.cp_port}",
        )
    else:
        token_file = settings.cloudflare_token_file
        hostname = settings.cloudflare_hostname
        team_domain = settings.cloudflare_access_team_domain
        assert token_file is not None and hostname is not None and team_domain is not None
        tunnel_agent = _launch_agent(
            settings,
            label=_CLOUDFLARED_LABEL,
            log_name=settings.cloudflare_log_name,
            argv=(
                settings.cloudflared_binary,
                "tunnel",
                "--no-autoupdate",
                "--metrics",
                f"127.0.0.1:{settings.cloudflare_metrics_port}",
                "run",
                "--token-file",
                str(token_file),
            ),
        )
        cloudflare = CloudflareTunnelPlan(
            launch_agent=tunnel_agent,
            version_command=(settings.cloudflared_binary, "--version"),
            ready_command=(
                settings.cloudflared_binary,
                "tunnel",
                "--metrics",
                f"127.0.0.1:{settings.cloudflare_metrics_port}",
                "ready",
            ),
            token_file=str(token_file),
            metrics_origin=f"http://127.0.0.1:{settings.cloudflare_metrics_port}",
            hostname=hostname,
            public_origin=f"https://{hostname}",
            access_team_domain=team_domain,
            expected_edge_origin=settings.edge_origin,
            prohibited_control_plane_origin=f"http://127.0.0.1:{settings.cp_port}",
        )
    partial = {
        "format": _FORMAT,
        "control_plane": asdict(control_plane),
        "edge": asdict(edge),
        "tailscale": asdict(tailscale) if tailscale is not None else None,
        "cloudflare": asdict(cloudflare) if cloudflare is not None else None,
    }
    digest = canonical_sha256(partial)
    return DashboardLifecyclePlan(
        format=_FORMAT,
        control_plane=control_plane,
        edge=edge,
        tailscale=tailscale,
        cloudflare=cloudflare,
        plan_digest=digest,
    )


def _is_sha256(value: str) -> bool:
    return len(value) == _SHA256_LENGTH and all(
        character in "0123456789abcdef" for character in value
    )


def _absolute_path(path: Path, *, label: str) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        raise ValueError(f"{label} must be absolute")
    return expanded


def canonical_codex_app_server_socket() -> Path:
    """Return the one Codex Desktop control socket shared by project runtimes."""

    # Import lazily because the runtime module also owns the JSON-RPC client
    # reused by ``reload_codex_mcp_server``.
    from .runtime import codex_desktop_control_socket_path

    return codex_desktop_control_socket_path()


def canonical_codex_app_server_executable() -> Path:
    """Return the exact signed Codex executable bundled with Desktop."""
    from .runtime import bundled_codex_app_server_executable

    # A plan may describe an absent host, but execution still requires the
    # existing file identity, signature, and exact process/LaunchAgent binding.
    return bundled_codex_app_server_executable() or Path(
        "/Applications/Codex.app/Contents/Resources/codex"
    )


def _canonical_owner_launchagent_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def _expected_codex_desktop_program_arguments() -> tuple[str, ...]:
    return (
        os.fspath(canonical_codex_app_server_executable()),
        *_CODEX_DESKTOP_ARGUMENTS_AFTER_EXECUTABLE,
    )


def _codex_launchagent_environment_digest(value: object) -> str:
    if value is None:
        environment: dict[str, str] = {}
    elif isinstance(value, Mapping) and len(value) <= 128:
        environment = {}
        total_bytes = 0
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,255}", key) is None
                or not isinstance(item, str)
                or len(item) > 32_768
                or any(character in item for character in "\0\r\n")
            ):
                raise ValueError("Codex Desktop LaunchAgent environment is invalid")
            total_bytes += len(key.encode("utf-8")) + len(item.encode("utf-8"))
            if total_bytes > 1_048_576:
                raise ValueError("Codex Desktop LaunchAgent environment is invalid")
            environment[key] = item
    else:
        raise ValueError("Codex Desktop LaunchAgent environment is invalid")
    return canonical_sha256(environment)


def _codex_launchagent_resource_limits(value: object) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > len(_CODEX_LAUNCHAGENT_RESOURCE_LIMIT_KEYS):
        raise ValueError("Codex Desktop LaunchAgent resource limits are invalid")
    limits: dict[str, int] = {}
    for key, item in value.items():
        if (
            key not in _CODEX_LAUNCHAGENT_RESOURCE_LIMIT_KEYS
            or isinstance(item, bool)
            or not isinstance(item, int)
            or item < 0
            or item > 2**63 - 1
        ):
            raise ValueError("Codex Desktop LaunchAgent resource limits are invalid")
        limits[str(key)] = item
    return limits


def _codex_launchagent_fixed_semantics(value: Mapping[str, object]) -> bool:
    support_root = _canonical_owner_launchagent_dir().parent / "Application Support"

    def safe_log_path(raw: object) -> bool:
        if not isinstance(raw, str) or not raw or any(character in raw for character in "\0\r\n"):
            return False
        path = Path(raw)
        if not path.is_absolute() or ".." in path.parts or not path.is_relative_to(support_root):
            return False
        try:
            parent_info = path.parent.lstat()
            path_info = path.lstat()
            resolved_parent = path.parent.resolve(strict=True)
            resolved_path = path.resolve(strict=True)
        except OSError:
            return False
        return (
            resolved_parent == path.parent
            and resolved_path == path
            and not stat.S_ISLNK(parent_info.st_mode)
            and stat.S_ISDIR(parent_info.st_mode)
            and parent_info.st_uid == os.getuid()
            and stat.S_IMODE(parent_info.st_mode) == 0o700
            and not stat.S_ISLNK(path_info.st_mode)
            and stat.S_ISREG(path_info.st_mode)
            and path_info.st_uid == os.getuid()
            and not stat.S_IMODE(path_info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
        )

    keys = set(value)
    if (
        not _CODEX_DESKTOP_LAUNCHAGENT_REQUIRED_KEYS.issubset(keys)
        or not (keys - _CODEX_DESKTOP_LAUNCHAGENT_REQUIRED_KEYS).issubset(
            _CODEX_DESKTOP_LAUNCHAGENT_OPTIONAL_KEYS
        )
    ):
        return False
    try:
        _codex_launchagent_environment_digest(value.get("EnvironmentVariables"))
        hard_limits = _codex_launchagent_resource_limits(value.get("HardResourceLimits"))
        soft_limits = _codex_launchagent_resource_limits(value.get("SoftResourceLimits"))
    except ValueError:
        return False
    if any(
        key in hard_limits and item > hard_limits[key]
        for key, item in soft_limits.items()
    ):
        return False
    return (
        value.get("KeepAlive") is True
        and value.get("RunAtLoad") is True
        and value.get("ProcessType") == "Interactive"
        and value.get("ThrottleInterval") == 10
        and safe_log_path(value.get("StandardOutPath"))
        and safe_log_path(value.get("StandardErrorPath"))
    )


def _require_codex_desktop_launchagent_shape(
    binding: CodexDesktopLaunchAgentBinding | None,
) -> None:
    if binding is None:
        return
    path = Path(binding.plist_path)
    expected_parent = _canonical_owner_launchagent_dir()
    if (
        not binding.label
        or len(binding.label) > 255
        or re.fullmatch(r"[A-Za-z0-9._-]+", binding.label) is None
        or not path.is_absolute()
        or path.parent != expected_parent
        or path.name != f"{binding.label}.plist"
        or not _is_sha256(binding.plist_sha256)
        or not _is_sha256(binding.environment_sha256)
        or binding.program_arguments != _expected_codex_desktop_program_arguments()
    ):
        raise ValueError("Codex Desktop LaunchAgent binding is invalid")


def discover_codex_desktop_launchagent_binding() -> CodexDesktopLaunchAgentBinding | None:
    """Bind the unique canonical owner LaunchAgent for the bundled app-server."""

    directory = _canonical_owner_launchagent_dir()
    try:
        directory_info = directory.lstat()
        resolved_directory = directory.resolve(strict=True)
    except (FileNotFoundError, OSError):
        return None
    if (
        resolved_directory != directory
        or stat.S_ISLNK(directory_info.st_mode)
        or not stat.S_ISDIR(directory_info.st_mode)
        or directory_info.st_uid != os.getuid()
        or stat.S_IMODE(directory_info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise RuntimeError("canonical owner LaunchAgents directory is unsafe")
    expected_arguments = _expected_codex_desktop_program_arguments()
    matches: list[CodexDesktopLaunchAgentBinding] = []
    try:
        candidates = tuple(sorted(directory.iterdir(), key=lambda item: item.name))
    except OSError as error:
        raise RuntimeError("canonical owner LaunchAgents directory is unavailable") from error
    if len(candidates) > 4096:
        raise RuntimeError("canonical owner LaunchAgents directory is unbounded")
    for candidate in candidates:
        if candidate.suffix != ".plist":
            continue
        try:
            info = candidate.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
                or info.st_size > 1_048_576
            ):
                continue
            payload = candidate.read_bytes()
            parsed = plistlib.loads(payload)
            after = candidate.lstat()
        except (OSError, plistlib.InvalidFileException, ValueError):
            continue
        if _file_identity(after) != _file_identity(info) or not isinstance(parsed, Mapping):
            continue
        label = parsed.get("Label")
        arguments = parsed.get("ProgramArguments")
        if (
            not isinstance(label, str)
            or candidate.name != f"{label}.plist"
            or not isinstance(arguments, list)
            or tuple(arguments) != expected_arguments
            or not _codex_launchagent_fixed_semantics(parsed)
        ):
            continue
        try:
            environment_sha256 = _codex_launchagent_environment_digest(
                parsed.get("EnvironmentVariables")
            )
        except ValueError:
            continue
        binding = CodexDesktopLaunchAgentBinding(
            label=label,
            plist_path=os.fspath(candidate),
            plist_sha256=hashlib.sha256(payload).hexdigest(),
            program_arguments=expected_arguments,
            environment_sha256=environment_sha256,
        )
        _require_codex_desktop_launchagent_shape(binding)
        matches.append(binding)
    if len(matches) > 1:
        raise RuntimeError("multiple canonical Codex Desktop LaunchAgents matched")
    return matches[0] if matches else None


def _planned_codex_app_server_socket(explicit: Path | None) -> Path:
    candidate = canonical_codex_app_server_socket() if explicit is None else explicit
    return _absolute_path(candidate, label="Codex app-server socket")


def _planned_codex_executable_identity(
    executable: Path,
) -> tuple[int, int, int, int, int] | None:
    try:
        info = executable.lstat()
    except OSError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        return None
    return _file_identity(info)


def _valid_codex_executable_identity(value: object) -> bool:
    return value is None or (
        isinstance(value, tuple)
        and len(value) == 5
        and all(
            isinstance(item, int) and not isinstance(item, bool) and item >= 0 for item in value
        )
    )


def _require_current_codex_executable_plan_identity(
    executable_path: str,
    expected: tuple[int, int, int, int, int] | None,
) -> None:
    if expected is None:
        raise RuntimeError("Codex app-server executable identity was not plan-bound")
    try:
        current = Path(executable_path).lstat()
    except OSError as error:
        raise RuntimeError("Codex app-server executable changed after planning") from error
    if _file_identity(current) != expected:
        raise RuntimeError("Codex app-server executable changed after planning")


def _plan_digest(value: Mapping[str, object]) -> str:
    return canonical_sha256(value)


def _recomputed_plan_digest(
    plan: DashboardLifecyclePlan | DashboardUpgradePlan | CodexMCPRefreshPlan,
) -> str:
    payload = asdict(plan)
    payload.pop("plan_digest")
    return _plan_digest(payload)


def build_codex_mcp_refresh_plan(
    *,
    codex_app_server_socket: Path | None = None,
    codex_desktop_launchagent: CodexDesktopLaunchAgentBinding | None = None,
    target_identity: ReleaseIdentity | None = None,
) -> CodexMCPRefreshPlan:
    """Build the bounded host-reload action without touching the socket."""

    if codex_desktop_launchagent is None:
        codex_desktop_launchagent = discover_codex_desktop_launchagent_binding()
    target = target_identity or current_release_identity()
    if (
        not _is_sha256(target.release_id)
        or not _is_sha256(target.mcp_catalog_digest)
        or target.schema_version < 1
    ):
        raise ValueError("target release identity is invalid")
    codex_socket = _planned_codex_app_server_socket(codex_app_server_socket)
    codex_executable = canonical_codex_app_server_executable()
    codex_executable_identity = _planned_codex_executable_identity(codex_executable)
    _require_codex_desktop_launchagent_shape(codex_desktop_launchagent)
    partial: dict[str, object] = {
        "format": _MCP_REFRESH_FORMAT,
        "target_identity": asdict(target),
        "codex_app_server_executable": str(codex_executable),
        "codex_app_server_executable_identity": codex_executable_identity,
        "codex_app_server_socket": str(codex_socket),
        "codex_desktop_launchagent": (
            asdict(codex_desktop_launchagent) if codex_desktop_launchagent is not None else None
        ),
        "codex_mcp_reload_scope": _CODEX_MCP_RELOAD_SCOPE,
        "codex_mcp_reload_application": _CODEX_MCP_RELOAD_APPLICATION,
    }
    return CodexMCPRefreshPlan(
        format=_MCP_REFRESH_FORMAT,
        target_identity=target,
        codex_app_server_executable=str(codex_executable),
        codex_app_server_executable_identity=codex_executable_identity,
        codex_app_server_socket=str(codex_socket),
        codex_desktop_launchagent=codex_desktop_launchagent,
        codex_mcp_reload_scope=_CODEX_MCP_RELOAD_SCOPE,
        codex_mcp_reload_application=_CODEX_MCP_RELOAD_APPLICATION,
        plan_digest=_plan_digest(partial),
    )


def _require_codex_mcp_refresh_plan(plan: CodexMCPRefreshPlan) -> None:
    target = plan.target_identity
    _require_codex_desktop_launchagent_shape(plan.codex_desktop_launchagent)
    if (
        plan.format != _MCP_REFRESH_FORMAT
        or plan.codex_mcp_reload_scope != _CODEX_MCP_RELOAD_SCOPE
        or plan.codex_mcp_reload_application != _CODEX_MCP_RELOAD_APPLICATION
        or not _is_sha256(target.release_id)
        or not _is_sha256(target.mcp_catalog_digest)
        or target.schema_version < 1
        or plan.codex_app_server_executable != str(canonical_codex_app_server_executable())
        or not _valid_codex_executable_identity(plan.codex_app_server_executable_identity)
        or not isinstance(plan.codex_app_server_socket, str)
        or not Path(plan.codex_app_server_socket).expanduser().is_absolute()
        or _recomputed_plan_digest(plan) != plan.plan_digest
    ):
        raise ValueError("Codex MCP refresh plan no longer matches its reviewed digest")


def _require_dashboard_upgrade_plan(plan: DashboardUpgradePlan) -> None:
    _require_codex_desktop_launchagent_shape(plan.codex_desktop_launchagent)
    if (
        plan.format != _UPGRADE_FORMAT
        or plan.codex_mcp_reload_scope != _CODEX_MCP_RELOAD_SCOPE
        or plan.codex_mcp_reload_application != _CODEX_MCP_RELOAD_APPLICATION
        or plan.codex_app_server_executable != str(canonical_codex_app_server_executable())
        or not _valid_codex_executable_identity(plan.codex_app_server_executable_identity)
        or not isinstance(plan.codex_app_server_socket, str)
        or not Path(plan.codex_app_server_socket).expanduser().is_absolute()
        or _recomputed_plan_digest(plan) != plan.plan_digest
    ):
        raise ValueError("Dashboard upgrade plan no longer matches its reviewed digest")


def _upgrade_launchagent_binding(
    canonical: LaunchAgentPlan,
    *,
    explicit_path: Path | None,
    explicit_sha256: str | None,
    loopback_port: int,
) -> UpgradeLaunchAgentBinding:
    if (explicit_path is None) != (explicit_sha256 is None):
        raise ValueError("an explicit plist path and sha256 must be supplied together")
    path = (
        Path(canonical.plist_path)
        if explicit_path is None
        else _absolute_path(explicit_path, label=f"{canonical.label} plist path")
    )
    digest = canonical.plist_sha256 if explicit_sha256 is None else explicit_sha256
    if not _is_sha256(digest):
        raise ValueError(f"{canonical.label} plist sha256 must be 64 lowercase hex characters")
    return UpgradeLaunchAgentBinding(
        label=canonical.label,
        plist_path=str(path),
        plist_sha256=digest,
        working_directory=canonical.working_directory,
        loopback_port=loopback_port,
    )


def build_dashboard_upgrade_plan(
    lifecycle: DashboardLifecyclePlan,
    *,
    database_path: Path,
    backup_destination: Path,
    credentials_file: Path,
    source_identity: SQLiteDatabaseIdentity,
    control_plane_plist_path: Path | None = None,
    control_plane_plist_sha256: str | None = None,
    edge_plist_path: Path | None = None,
    edge_plist_sha256: str | None = None,
    codex_app_server_socket: Path | None = None,
    codex_desktop_launchagent: CodexDesktopLaunchAgentBinding | None = None,
    target_identity: ReleaseIdentity | None = None,
    readiness_attempts: int = 30,
    readiness_interval_seconds: float = 1.0,
) -> DashboardUpgradePlan:
    """Build a reviewed upgrade plan without writing files or running commands."""

    if codex_desktop_launchagent is None:
        codex_desktop_launchagent = discover_codex_desktop_launchagent_binding()
    database = _absolute_path(database_path, label="database path")
    backup = _absolute_path(backup_destination, label="backup destination")
    credentials = _absolute_path(credentials_file, label="credentials file")
    codex_socket = _planned_codex_app_server_socket(codex_app_server_socket)
    codex_executable = canonical_codex_app_server_executable()
    codex_executable_identity = _planned_codex_executable_identity(codex_executable)
    _require_codex_desktop_launchagent_shape(codex_desktop_launchagent)
    if database == backup:
        raise ValueError("backup destination must differ from the live database")
    if not source_identity.ok:
        raise ValueError("source database identity is not healthy")
    target = target_identity or current_release_identity()
    if (
        not _is_sha256(target.release_id)
        or not _is_sha256(target.mcp_catalog_digest)
        or target.schema_version < source_identity.schema_version
    ):
        raise ValueError("target release identity is invalid or cannot read the source schema")
    if readiness_attempts < 1 or readiness_interval_seconds < 0:
        raise ValueError("readiness policy must be bounded and non-negative")

    control_plane = _upgrade_launchagent_binding(
        lifecycle.control_plane,
        explicit_path=control_plane_plist_path,
        explicit_sha256=control_plane_plist_sha256,
        loopback_port=int(lifecycle.prohibited_control_plane_origin.rsplit(":", 1)[1]),
    )
    edge = _upgrade_launchagent_binding(
        lifecycle.edge,
        explicit_path=edge_plist_path,
        explicit_sha256=edge_plist_sha256,
        loopback_port=int(lifecycle.expected_edge_origin.rsplit(":", 1)[1]),
    )
    if control_plane.plist_path == edge.plist_path:
        raise ValueError("Control Plane and Edge must use different plist files")
    partial = {
        "format": _UPGRADE_FORMAT,
        "lifecycle_plan_digest": lifecycle.plan_digest,
        "control_plane": asdict(control_plane),
        "edge": asdict(edge),
        "database_path": str(database),
        "backup_destination": str(backup),
        "source_identity": asdict(source_identity),
        "target_identity": asdict(target),
        "control_plane_origin": lifecycle.prohibited_control_plane_origin,
        "edge_origin": lifecycle.expected_edge_origin,
        "credentials_file": str(credentials),
        "codex_app_server_executable": str(codex_executable),
        "codex_app_server_executable_identity": codex_executable_identity,
        "codex_app_server_socket": str(codex_socket),
        "codex_desktop_launchagent": (
            asdict(codex_desktop_launchagent) if codex_desktop_launchagent is not None else None
        ),
        "codex_mcp_reload_scope": _CODEX_MCP_RELOAD_SCOPE,
        "codex_mcp_reload_application": _CODEX_MCP_RELOAD_APPLICATION,
        "readiness_attempts": readiness_attempts,
        "readiness_interval_seconds": readiness_interval_seconds,
    }
    digest = _plan_digest(partial)
    return DashboardUpgradePlan(
        format=_UPGRADE_FORMAT,
        lifecycle_plan_digest=lifecycle.plan_digest,
        control_plane=control_plane,
        edge=edge,
        database_path=str(database),
        backup_destination=str(backup),
        source_identity=source_identity,
        target_identity=target,
        control_plane_origin=lifecycle.prohibited_control_plane_origin,
        edge_origin=lifecycle.expected_edge_origin,
        credentials_file=str(credentials),
        codex_app_server_executable=str(codex_executable),
        codex_app_server_executable_identity=codex_executable_identity,
        codex_app_server_socket=str(codex_socket),
        codex_desktop_launchagent=codex_desktop_launchagent,
        codex_mcp_reload_scope=_CODEX_MCP_RELOAD_SCOPE,
        codex_mcp_reload_application=_CODEX_MCP_RELOAD_APPLICATION,
        readiness_attempts=readiness_attempts,
        readiness_interval_seconds=readiness_interval_seconds,
        plan_digest=digest,
    )


def canonical_launchagent_plist(plan: LaunchAgentPlan) -> bytes:
    return _launchagent_plist_bytes(plan)


def _previous_canonical_launchagent_plist(plan: LaunchAgentPlan) -> bytes | None:
    """Return only the exact plist emitted by the immediately prior release."""

    arguments: Sequence[str]
    if plan.label == _CP_LABEL:
        arguments = plan.program_arguments
    elif plan.label == _EDGE_LABEL:
        edge_arguments = list(plan.program_arguments)
        if edge_arguments.count("--session-record-dir") != 1:
            return None
        index = edge_arguments.index("--session-record-dir")
        if index + 1 >= len(edge_arguments):
            return None
        del edge_arguments[index : index + 2]
        arguments = edge_arguments
    else:
        return None
    return _launchagent_plist_bytes(
        plan,
        program_arguments=arguments,
        keep_alive={"Crashed": True},
    )


def _plist_state(plan: LaunchAgentPlan) -> str:
    path = Path(plan.plist_path)
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            return "identity-conflict"
        # macOS launchd rejects user LaunchAgent plists that are owner-only.
        # The plist contains paths but no credential value, so use the
        # platform-required non-writable 0644 mode while keeping every secret
        # in its separate 0600 credential file.
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o644:
            return "identity-conflict"
        actual = path.read_bytes()
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "unknown"
    if hashlib.sha256(actual).hexdigest() == plan.plist_sha256:
        return "ready"
    try:
        actual_value = plistlib.loads(actual)
        canonical_value = plistlib.loads(canonical_launchagent_plist(plan))
    except (plistlib.InvalidFileException, ValueError, TypeError):
        return "content-conflict"
    # plist key order and XML whitespace are not launchd semantics. Accept an
    # exact decoded value so a previously hand-rendered but otherwise exact
    # owner plist is not restarted or overwritten merely for serialization.
    return "ready" if actual_value == canonical_value else "content-conflict"


def _json(result: CommandResult) -> Mapping[str, object] | None:
    if result.returncode != 0:
        return None
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, Mapping) else None


def _proxy_targets(value: object) -> set[str]:
    if isinstance(value, Mapping):
        values: set[str] = set()
        for key, item in value.items():
            if key == "Proxy" and isinstance(item, str):
                values.add(item.rstrip("/"))
            values.update(_proxy_targets(item))
        return values
    if isinstance(value, list):
        return set().union(*(_proxy_targets(item) for item in value)) if value else set()
    return set()


def _tailnet_url(status: Mapping[str, object] | None) -> str | None:
    if status is None:
        return None
    self_value = status.get("Self")
    dns_name = self_value.get("DNSName") if isinstance(self_value, Mapping) else None
    if not isinstance(dns_name, str) or (not dns_name.endswith(".") and "." not in dns_name):
        return None
    return f"https://{dns_name.rstrip('.')}"


def _serve_state(plan: TailscaleServePlan, result: CommandResult) -> tuple[str, str | None]:
    payload = _json(result)
    if payload is None:
        return "unknown", None
    targets = _proxy_targets(payload)
    if plan.prohibited_control_plane_origin in targets:
        return "conflict-control-plane-exposed", None
    if not targets:
        return "missing", None
    if targets == {plan.expected_edge_origin.rstrip("/")}:
        return "ready", None
    return "conflict", None


@dataclass(frozen=True, slots=True)
class DashboardLifecycleStatus:
    exposure_provider: str
    control_plane_plist: str
    edge_plist: str
    control_plane_launchd: str
    edge_launchd: str
    tailscale_version: str
    tailscale_serve: str
    tailnet_url: str | None
    cloudflare_plist: str
    cloudflare_launchd: str
    cloudflared_version: str
    cloudflare_tunnel: str
    cloudflare_url: str | None
    health: Mapping[str, str]

    @property
    def ready(self) -> bool:
        shared = (
            self.control_plane_plist,
            self.edge_plist,
            self.control_plane_launchd,
            self.edge_launchd,
            self.health.get("control_plane"),
            self.health.get("edge"),
        )
        if not all(value == "ready" for value in shared):
            return False
        if self.exposure_provider == "tailscale":
            return (
                self.tailscale_version == "ready"
                and self.tailscale_serve == "ready"
                and self.tailnet_url is not None
            )
        return (
            self.exposure_provider == "cloudflare"
            and self.cloudflare_plist == "ready"
            and self.cloudflare_launchd == "ready"
            and self.cloudflared_version == "ready"
            and self.cloudflare_tunnel == "ready"
            and self.cloudflare_url is not None
            and self.health.get("public") == "access-protected"
        )


class HttpxProbe:
    """Short, redirect-free loopback probe used by the explicit status command."""

    def get(self, url: str, headers: Mapping[str, str]) -> HttpResult:
        response = httpx.get(url, headers=dict(headers), timeout=5.0, follow_redirects=False)
        try:
            value = response.json()
        except ValueError:
            value = None
        body = value if isinstance(value, Mapping) else None
        return HttpResult(response.status_code, dict(response.headers), body)


def dashboard_lifecycle_status(
    plan: DashboardLifecyclePlan,
    runner: CommandRunner,
    *,
    probe: HttpProbe | None = None,
    credentials_file: Path | None = None,
) -> DashboardLifecycleStatus:
    """Observe service state once; invalid/unknown observations stay fail-closed."""

    cp_loaded = runner.run(("launchctl", "print", f"gui/{os.getuid()}/{plan.control_plane.label}"))
    edge_loaded = runner.run(("launchctl", "print", f"gui/{os.getuid()}/{plan.edge.label}"))
    health: dict[str, str] = {"control_plane": "not-checked", "edge": "not-checked"}
    if probe is not None and credentials_file is not None:
        credentials = load_dashboard_credentials(credentials_file)
        cp = probe.get(
            plan.prohibited_control_plane_origin + "/api/v1/dashboard/v1/snapshot",
            {"Authorization": f"Bearer {credentials.dashboard_bearer}"},
        )
        edge = probe.get(plan.expected_edge_origin + "/dashboard/", {})
        health["control_plane"] = (
            "ready"
            if cp.status_code == 200
            and cp.headers.get("content-type", "").startswith("application/json")
            else "unhealthy"
        )
        disposition = edge.headers.get("content-disposition", "").lower()
        health["edge"] = (
            "ready"
            if edge.status_code == 200
            and edge.headers.get("content-type", "").startswith("text/html")
            and "attachment" not in disposition
            else "safari-content-type-regression"
        )
    if plan.tailscale is not None:
        version = runner.run(plan.tailscale.version_command)
        serve = runner.run(plan.tailscale.serve_status_command)
        tailnet = runner.run(plan.tailscale.tailnet_status_command)
        serve_state, _ = _serve_state(plan.tailscale, serve)
        return DashboardLifecycleStatus(
            "tailscale",
            _plist_state(plan.control_plane),
            _plist_state(plan.edge),
            "ready" if cp_loaded.returncode == 0 else "missing",
            "ready" if edge_loaded.returncode == 0 else "missing",
            "ready" if version.returncode == 0 and version.stdout.strip() else "unknown",
            serve_state,
            _tailnet_url(_json(tailnet)),
            "not-configured",
            "not-configured",
            "not-configured",
            "not-configured",
            None,
            health,
        )
    cloudflare = plan.cloudflare
    if cloudflare is None:  # pragma: no cover - constructor invariant
        raise ValueError("Dashboard exposure is unavailable")
    tunnel_loaded = runner.run(
        ("launchctl", "print", f"gui/{os.getuid()}/{cloudflare.launch_agent.label}")
    )
    version = runner.run(cloudflare.version_command)
    connector_ready = runner.run(cloudflare.ready_command)
    public_state = "not-checked"
    if probe is not None:
        public = probe.get(cloudflare.public_origin + "/dashboard/", {})
        location = public.headers.get("location", "")
        try:
            parsed_location = httpx.URL(location) if location else None
        except httpx.InvalidURL:
            parsed_location = None
        public_state = (
            "access-protected"
            if public.status_code in {302, 303, 307, 308}
            and parsed_location is not None
            and parsed_location.scheme == "https"
            and parsed_location.host == cloudflare.access_team_domain
            and parsed_location.path.startswith("/cdn-cgi/access/")
            else "unhealthy"
        )
        health["public"] = public_state
    return DashboardLifecycleStatus(
        "cloudflare",
        _plist_state(plan.control_plane),
        _plist_state(plan.edge),
        "ready" if cp_loaded.returncode == 0 else "missing",
        "ready" if edge_loaded.returncode == 0 else "missing",
        "not-configured",
        "not-configured",
        None,
        _plist_state(cloudflare.launch_agent),
        "ready" if tunnel_loaded.returncode == 0 else "missing",
        "ready" if version.returncode == 0 and version.stdout.strip() else "unknown",
        "ready"
        if tunnel_loaded.returncode == 0
        and connector_ready.returncode == 0
        and public_state == "access-protected"
        else "down",
        cloudflare.public_origin,
        health,
    )


def _plist_argument(arguments: Sequence[str], name: str) -> str | None:
    if arguments.count(name) != 1:
        return None
    index = arguments.index(name)
    return arguments[index + 1] if index + 1 < len(arguments) else None


def _upgrade_plist_state(binding: UpgradeLaunchAgentBinding) -> str:
    """Verify the exact reviewed plist and its loopback deployment semantics."""

    path = Path(binding.plist_path)
    try:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return "identity-conflict"
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o644:
            return "identity-conflict"
        payload = path.read_bytes()
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "unknown"
    if hashlib.sha256(payload).hexdigest() != binding.plist_sha256:
        return "content-conflict"
    try:
        plist = plistlib.loads(payload)
    except (plistlib.InvalidFileException, ValueError):
        return "semantic-conflict"
    if not isinstance(plist, Mapping):
        return "semantic-conflict"
    arguments = plist.get("ProgramArguments")
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        return "semantic-conflict"
    if any(not item or any(character in item for character in "\r\n\0") for item in arguments):
        return "semantic-conflict"
    if (
        plist.get("Label") != binding.label
        or plist.get("WorkingDirectory") != binding.working_directory
        or _plist_argument(arguments, "--host") != "127.0.0.1"
        or _plist_argument(arguments, "--port") != str(binding.loopback_port)
    ):
        return "semantic-conflict"
    return "ready"


def _require_upgrade_plist(binding: UpgradeLaunchAgentBinding) -> None:
    state = _upgrade_plist_state(binding)
    if state != "ready":
        raise RuntimeError(f"refusing to restart {binding.label}: {state}")


def _bound_upgrade_plist(binding: UpgradeLaunchAgentBinding) -> Mapping[str, object]:
    """Re-read the exact reviewed plist for launch/config identity checks."""

    _require_upgrade_plist(binding)
    payload = Path(binding.plist_path).read_bytes()
    if hashlib.sha256(payload).hexdigest() != binding.plist_sha256:
        raise RuntimeError(f"refusing to restart {binding.label}: content-conflict")
    try:
        value = plistlib.loads(payload)
    except (plistlib.InvalidFileException, ValueError) as error:
        raise RuntimeError(f"refusing to restart {binding.label}: semantic-conflict") from error
    if not isinstance(value, Mapping):
        raise RuntimeError(f"refusing to restart {binding.label}: semantic-conflict")
    return value


def _launchctl_arguments(output: str) -> tuple[str, ...] | None:
    lines = output.splitlines()
    starts = [index for index, line in enumerate(lines) if line.strip() == "arguments = {"]
    if len(starts) != 1:
        return None
    values: list[str] = []
    for line in lines[starts[0] + 1 :]:
        rendered = line.strip()
        if rendered == "}":
            return tuple(values)
        if not rendered:
            return None
        values.append(rendered)
    return None


def _launchctl_scalar(output: str, name: str) -> str | None:
    prefix = f"{name} = "
    matches = [
        (len(line) - len(line.lstrip()), line.strip()[len(prefix) :])
        for line in output.splitlines()
        if line.strip().startswith(prefix) and line != line.lstrip()
    ]
    if not matches:
        return None
    root_depth = min(depth for depth, _value in matches)
    root_values = [value for depth, value in matches if depth == root_depth]
    return root_values[0] if len(root_values) == 1 else None


def _require_loaded_launchagent_binding(
    result: CommandResult,
    binding: UpgradeLaunchAgentBinding,
    plist: Mapping[str, object],
) -> None:
    arguments = plist.get("ProgramArguments")
    expected_arguments = (
        tuple(arguments)
        if isinstance(arguments, list) and all(isinstance(item, str) for item in arguments)
        else ()
    )
    if (
        result.returncode != 0
        or _launchctl_scalar(result.stdout, "path") != binding.plist_path
        or _launchctl_scalar(result.stdout, "state") != "running"
        or _launchctl_scalar(result.stdout, "working directory") != binding.working_directory
        or _launchctl_arguments(result.stdout) != expected_arguments
    ):
        raise RuntimeError(f"loaded LaunchAgent {binding.label} differs from its reviewed plist")


class _DarwinAuditToken(ctypes.Structure):
    _fields_ = [("val", ctypes.c_uint32 * _DARWIN_AUDIT_TOKEN_WORDS)]


class _DarwinProcBSDInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


class _DarwinDashboardProcessAPI(Protocol):
    def capture(self, pid: int, *, expected_asid: int) -> DarwinAuditProcessIdentity: ...

    def list_process_group(self, pgid: int) -> tuple[int, ...]: ...

    def executable_path(self, identity: DarwinAuditProcessIdentity) -> Path: ...

    def is_live(self, identity: DarwinAuditProcessIdentity) -> bool: ...

    def signal(self, identity: DarwinAuditProcessIdentity, value: int) -> bool: ...


class DarwinDashboardProcessAPI:
    """Narrow libSystem/libproc adapter for PID-reuse-safe process fencing."""

    def __init__(self) -> None:
        if sys.platform != "darwin":
            raise RuntimeError("Dashboard LaunchAgent process fencing requires macOS")
        self._system: ctypes.CDLL = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        self._proc: ctypes.CDLL = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        self._system.mach_task_self.argtypes = []
        self._system.mach_task_self.restype = ctypes.c_uint32
        self._system.task_name_for_pid.argtypes = [
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        self._system.task_name_for_pid.restype = ctypes.c_int
        self._system.task_info.argtypes = [
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        self._system.task_info.restype = ctypes.c_int
        self._system.mach_port_deallocate.argtypes = [ctypes.c_uint32, ctypes.c_uint32]
        self._system.mach_port_deallocate.restype = ctypes.c_int
        self._proc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        self._proc.proc_pidinfo.restype = ctypes.c_int
        self._proc.proc_listpgrppids.argtypes = [
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        self._proc.proc_listpgrppids.restype = ctypes.c_int
        self._proc.proc_pidpath_audittoken.argtypes = [
            ctypes.POINTER(_DarwinAuditToken),
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        self._proc.proc_pidpath_audittoken.restype = ctypes.c_int
        self._proc.proc_signal_with_audittoken.argtypes = [
            ctypes.POINTER(_DarwinAuditToken),
            ctypes.c_int,
        ]
        self._proc.proc_signal_with_audittoken.restype = ctypes.c_int

    @staticmethod
    def _token(value: tuple[int, ...]) -> _DarwinAuditToken:
        if len(value) != _DARWIN_AUDIT_TOKEN_WORDS:
            raise LifecycleUnknownOutcome("captured process audit token is invalid")
        token = _DarwinAuditToken()
        token.val[:] = value
        return token

    def _bsd_info(self, pid: int) -> _DarwinProcBSDInfo:
        value = _DarwinProcBSDInfo()
        size = ctypes.sizeof(value)
        if (
            self._proc.proc_pidinfo(
                pid,
                _DARWIN_PROC_PIDTBSDINFO,
                0,
                ctypes.byref(value),
                size,
            )
            != size
            or int(value.pbi_pid) != pid
        ):
            raise RuntimeError("LaunchAgent process metadata is unavailable")
        return value

    def capture(self, pid: int, *, expected_asid: int) -> DarwinAuditProcessIdentity:
        if pid <= 1 or expected_asid <= 0:
            raise RuntimeError("LaunchAgent process identity is unsafe")
        task_self = int(self._system.mach_task_self())
        name_port = ctypes.c_uint32()
        if self._system.task_name_for_pid(task_self, pid, ctypes.byref(name_port)) != 0:
            raise RuntimeError("LaunchAgent audit token is unavailable")
        token = _DarwinAuditToken()
        count = ctypes.c_uint32(_DARWIN_AUDIT_TOKEN_WORDS)
        try:
            if (
                self._system.task_info(
                    name_port.value,
                    _DARWIN_TASK_AUDIT_TOKEN,
                    token.val,
                    ctypes.byref(count),
                )
                != 0
                or count.value != _DARWIN_AUDIT_TOKEN_WORDS
            ):
                raise RuntimeError("LaunchAgent audit token is unavailable")
        finally:
            if self._system.mach_port_deallocate(task_self, name_port.value) != 0:
                raise RuntimeError("LaunchAgent audit token port could not be released")
        raw = tuple(int(item) for item in token.val)
        info = self._bsd_info(pid)
        if (
            raw[5] != pid
            or raw[6] != expected_asid
            or raw[7] <= 0
            or raw[1] != os.geteuid()
            or raw[3] != os.getuid()
            or int(info.pbi_uid) != os.geteuid()
            or int(info.pbi_ruid) != os.getuid()
        ):
            raise RuntimeError("LaunchAgent audit token does not match its owner process")
        return DarwinAuditProcessIdentity(
            pid=pid,
            pgid=int(info.pbi_pgid),
            uid=int(info.pbi_uid),
            asid=raw[6],
            audit_token=raw,
        )

    def list_process_group(self, pgid: int) -> tuple[int, ...]:
        if pgid <= 1:
            raise RuntimeError("LaunchAgent process group is unsafe")
        ctypes.set_errno(0)
        capacity = self._proc.proc_listpgrppids(pgid, None, 0)
        capacity_errno = ctypes.get_errno()
        if capacity < 0 or capacity > 1_000_000 or (capacity == 0 and capacity_errno != 0):
            raise RuntimeError("LaunchAgent process group membership is unavailable")
        buffer_size = max(capacity + 20, 32)
        for _attempt in range(3):
            values = (ctypes.c_int * buffer_size)()
            ctypes.set_errno(0)
            count = self._proc.proc_listpgrppids(pgid, values, ctypes.sizeof(values))
            count_errno = ctypes.get_errno()
            if count < 0 or count > buffer_size or (count == 0 and count_errno != 0):
                raise RuntimeError("LaunchAgent process group membership is unavailable")
            if count < buffer_size:
                return tuple(
                    sorted({int(values[index]) for index in range(count) if values[index] > 1})
                )
            buffer_size *= 2
        raise RuntimeError("LaunchAgent process group membership did not stabilize")

    def is_live(self, identity: DarwinAuditProcessIdentity) -> bool:
        token = self._token(identity.audit_token)
        path = ctypes.create_string_buffer(4096)
        ctypes.set_errno(0)
        result = self._proc.proc_pidpath_audittoken(
            ctypes.byref(token),
            path,
            len(path),
        )
        if result > 0:
            return True
        if ctypes.get_errno() == errno.ESRCH:
            return False
        raise LifecycleUnknownOutcome("booted-out LaunchAgent process state is unknown")

    def executable_path(self, identity: DarwinAuditProcessIdentity) -> Path:
        """Resolve the executable for one exact audit-token generation."""

        token = self._token(identity.audit_token)
        path = ctypes.create_string_buffer(4096)
        ctypes.set_errno(0)
        result = self._proc.proc_pidpath_audittoken(
            ctypes.byref(token),
            path,
            len(path),
        )
        if result <= 0 or not path.value:
            if ctypes.get_errno() == errno.ESRCH:
                raise RuntimeError("Codex app-server process generation exited")
            raise RuntimeError("Codex app-server mapped executable is unavailable")
        try:
            return Path(os.fsdecode(path.value)).resolve(strict=True)
        except (OSError, UnicodeError) as error:
            raise RuntimeError("Codex app-server mapped executable is unavailable") from error

    def signal(self, identity: DarwinAuditProcessIdentity, value: int) -> bool:
        token = self._token(identity.audit_token)
        ctypes.set_errno(0)
        result = self._proc.proc_signal_with_audittoken(ctypes.byref(token), value)
        if result == 0:
            return True
        if result == errno.ESRCH or ctypes.get_errno() == errno.ESRCH:
            return False
        raise LifecycleUnknownOutcome("exact booted-out LaunchAgent process could not be stopped")


class ExactDashboardUpgradeProcessController:
    """Retire every exact execution in each captured LaunchAgent process group."""

    def __init__(
        self,
        *,
        process_api: _DarwinDashboardProcessAPI | None = None,
        sleeper: Callable[[float], None] = sleep,
        attempts: int = _PROCESS_EXIT_ATTEMPTS,
        interval_seconds: float = _PROCESS_EXIT_INTERVAL_SECONDS,
    ) -> None:
        if attempts < 1 or interval_seconds < 0:
            raise ValueError("process exit polling settings are invalid")
        self._process_api: _DarwinDashboardProcessAPI = (
            process_api or DarwinDashboardProcessAPI()
        )
        self._sleeper = sleeper
        self._attempts = attempts
        self._interval_seconds = interval_seconds

    def capture(
        self,
        runner: CommandRunner,
        binding: UpgradeLaunchAgentBinding,
    ) -> DashboardUpgradeProcessGroup:
        plist = _bound_upgrade_plist(binding)
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        loaded = runner.run(command)
        _require_loaded_launchagent_binding(loaded, binding, plist)
        raw_pid = _launchctl_scalar(loaded.stdout, "pid")
        raw_asid = _launchctl_scalar(loaded.stdout, "asid")
        if (
            raw_pid is None
            or raw_asid is None
            or not raw_pid.isascii()
            or not raw_pid.isdecimal()
            or not raw_asid.isascii()
            or not raw_asid.isdecimal()
        ):
            raise RuntimeError(f"loaded LaunchAgent {binding.label} has no exact process identity")
        pid = int(raw_pid)
        asid = int(raw_asid)
        root = self._process_api.capture(pid, expected_asid=asid)
        if root.pgid != root.pid:
            raise RuntimeError(f"loaded LaunchAgent {binding.label} has a shared process group")
        first = self._process_api.list_process_group(root.pgid)
        if root.pid not in first:
            raise RuntimeError(f"loaded LaunchAgent {binding.label} process group is incomplete")
        members = tuple(
            root
            if member_pid == root.pid
            else self._process_api.capture(member_pid, expected_asid=asid)
            for member_pid in first
        )
        second = self._process_api.list_process_group(root.pgid)
        if first != second or any(
            member.pgid != root.pgid
            or member.uid != os.geteuid()
            or member.asid != asid
            or not self._process_api.is_live(member)
            for member in members
        ):
            raise RuntimeError(f"loaded LaunchAgent {binding.label} process group changed")
        return DashboardUpgradeProcessGroup(root.pgid, members)

    def _live_members(
        self,
        group: DashboardUpgradeProcessGroup,
    ) -> tuple[tuple[DarwinAuditProcessIdentity, ...], bool]:
        live = tuple(member for member in group.members if self._process_api.is_live(member))
        try:
            current = self._process_api.list_process_group(group.pgid)
        except RuntimeError as error:
            raise LifecycleUnknownOutcome(
                "booted-out LaunchAgent process group state is unknown"
            ) from error
        current_ids = set(current)
        captured_ids = {member.pid for member in group.members}
        live_ids = {member.pid for member in live}
        if current_ids - captured_ids:
            raise LifecycleUnknownOutcome(
                "booted-out LaunchAgent process group gained an unknown member"
            )
        return live, bool(current_ids or live_ids)

    def _wait(
        self,
        groups: Sequence[DashboardUpgradeProcessGroup],
    ) -> tuple[DashboardUpgradeProcessGroup, ...]:
        remaining = tuple(groups)
        for attempt in range(self._attempts):
            remaining = tuple(group for group in remaining if self._live_members(group)[1])
            if not remaining:
                return ()
            if attempt + 1 < self._attempts:
                self._sleeper(self._interval_seconds)
        return remaining

    def _signal_exact(
        self,
        groups: Sequence[DashboardUpgradeProcessGroup],
        value: int,
    ) -> None:
        for group in groups:
            live, _group_nonempty = self._live_members(group)
            for member in live:
                self._process_api.signal(member, value)

    def fence_after_bootout(
        self,
        groups: Sequence[DashboardUpgradeProcessGroup],
    ) -> None:
        """Wait, repeat TERM, then KILL using exact macOS audit tokens."""

        remaining = self._wait(groups)
        if not remaining:
            return
        self._signal_exact(remaining, signal.SIGTERM)
        remaining = self._wait(remaining)
        if not remaining:
            return
        self._signal_exact(remaining, signal.SIGKILL)
        remaining = self._wait(remaining)
        if remaining:
            raise LifecycleUnknownOutcome(
                "exact booted-out LaunchAgent process remained alive after escalation"
            )


def _configured_control_plane_database(
    binding: UpgradeLaunchAgentBinding,
    plist: Mapping[str, object],
    loaded_launchagent: str,
    launchd_state_dir: str,
) -> Path:
    arguments = plist.get("ProgramArguments")
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        raise RuntimeError("Control Plane plist arguments are invalid")
    job_environment = plist.get("EnvironmentVariables", {})
    if not isinstance(job_environment, Mapping):
        raise RuntimeError("Control Plane plist environment is invalid")
    job_state_dir = job_environment.get("CAO_A2A_STATE_DIR")
    inherited_state_dir = launchd_state_dir.rstrip("\r\n")
    if any(character in inherited_state_dir for character in "\r\n\0"):
        raise RuntimeError("launchd state directory environment is invalid")
    loaded_state_dir = any(
        line.strip().startswith("CAO_A2A_STATE_DIR =>") for line in loaded_launchagent.splitlines()
    )
    if job_state_dir is not None or inherited_state_dir or loaded_state_dir:
        raise RuntimeError(
            "upgrade requires Control Plane state_dir to come from its exact config file"
        )

    configured_state_dir: object | None = None
    config_argument = _plist_argument(arguments, "--config")
    if config_argument is None:
        raise RuntimeError("upgrade requires exactly one explicit Control Plane --config argument")
    # Match ``Settings.load(args.config)`` exactly: an explicit argparse Path
    # is not tilde-expanded, and a relative value is resolved by the launched
    # process from its reviewed WorkingDirectory.
    config_path = Path(config_argument)
    if not config_path.is_absolute():
        config_path = Path(binding.working_directory) / config_path
    try:
        raw = tomllib.loads(_read_owner_only_file(config_path))
    except (OSError, ValueError) as error:
        raise RuntimeError("Control Plane config identity is invalid") from error
    for section in (
        "server",
        "security",
        "protocols",
        "runtime",
        "dashboard",
        "retention",
    ):
        section_value = raw.get(section, {})
        if isinstance(section_value, Mapping) and "state_dir" in section_value:
            configured_state_dir = section_value["state_dir"]

    if not isinstance(configured_state_dir, (str, Path)):
        raise RuntimeError(
            "upgrade requires an explicit absolute state_dir in the Control Plane config"
        )
    state_dir = Path(configured_state_dir)
    if not state_dir.is_absolute():
        raise RuntimeError(
            "upgrade requires an explicit absolute state_dir in the Control Plane config"
        )
    return (state_dir / "control-plane.sqlite3").resolve(strict=False)


def _checked_codex_socket(path: Path) -> os.stat_result:
    parent = path.parent
    try:
        parent_info = parent.lstat()
        resolved_parent = parent.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ValueError("Codex app-server socket parent is unavailable") from error
    if (
        resolved_parent != parent
        or stat.S_ISLNK(parent_info.st_mode)
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.getuid()
        or stat.S_IMODE(parent_info.st_mode) != 0o700
    ):
        raise ValueError("Codex app-server socket parent must be a real owner-only 0700 directory")
    try:
        info = path.lstat()
    except FileNotFoundError as error:
        raise ValueError("Codex app-server socket does not exist") from error
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISSOCK(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ValueError("Codex app-server socket must be an owner-only 0600 Unix socket")
    return info


def preflight_dashboard_upgrade(
    plan: DashboardUpgradePlan,
    runner: CommandRunner,
    backup_database: DashboardUpgradeBackup,
    *,
    inspect_database: Callable[[Path], SQLiteDatabaseIdentity] = inspect_sqlite_database,
    release_identity: Callable[[], ReleaseIdentity] = current_release_identity,
    migration_preflight: DashboardUpgradeMigrationPreflight | None = None,
) -> DashboardUpgradePreflightResult:
    """Re-read every plan-bound local identity before any backup or restart."""

    bound_plists: dict[str, Mapping[str, object]] = {}
    loaded_launchagents: dict[str, str] = {}
    for binding in (plan.control_plane, plan.edge):
        plist = _bound_upgrade_plist(binding)
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        loaded = runner.run(command)
        _require_loaded_launchagent_binding(loaded, binding, plist)
        bound_plists[binding.label] = plist
        loaded_launchagents[binding.label] = loaded.stdout
    launchd_environment = runner.run(("launchctl", "getenv", "CAO_A2A_STATE_DIR"))
    if launchd_environment.returncode != 0:
        raise RuntimeError("launchd state directory environment is unknown")
    configured_database = _configured_control_plane_database(
        plan.control_plane,
        bound_plists[plan.control_plane.label],
        loaded_launchagents[plan.control_plane.label],
        launchd_environment.stdout,
    )
    if configured_database != Path(plan.database_path).resolve(strict=False):
        raise RuntimeError("upgrade database does not match the loaded Control Plane config")
    backup = Path(plan.backup_destination)
    if backup.exists() or backup.is_symlink():
        raise FileExistsError("upgrade backup destination already exists")
    observed = inspect_database(Path(plan.database_path))
    if not observed.ok or observed != plan.source_identity:
        raise RuntimeError("source database identity drifted after the upgrade plan")
    if release_identity() != plan.target_identity:
        raise RuntimeError("target release identity drifted after the upgrade plan")
    _checked_codex_socket(Path(plan.codex_app_server_socket))
    verifier = migration_preflight or verify_dashboard_upgrade_migration
    migrated = verifier(plan, backup_database)
    return DashboardUpgradePreflightResult(
        database_identity=observed,
        migration_identity=migrated,
        migration_projection_healthy=True,
        control_plane_plist_sha256=plan.control_plane.plist_sha256,
        edge_plist_sha256=plan.edge.plist_sha256,
    )


def verify_dashboard_upgrade_migration(
    plan: DashboardUpgradePlan,
    backup_database: DashboardUpgradeBackup,
) -> SQLiteDatabaseIdentity:
    """Migrate and project a disposable online snapshot, never the live DB."""

    source_before = inspect_sqlite_database(Path(plan.database_path))
    if source_before != plan.source_identity:
        raise RuntimeError("source database drifted before migration preflight")
    with tempfile.TemporaryDirectory(prefix="cao-upgrade-preflight-") as directory:
        root = Path(directory)
        root.chmod(0o700)
        copy = root / "control-plane.sqlite3"
        snapshot = backup_database(
            Path(plan.database_path),
            copy,
            replace=False,
        )
        if (
            snapshot.path.resolve(strict=False) != copy.resolve(strict=False)
            or snapshot.source_identity != plan.source_identity
            or snapshot.backup_identity != plan.source_identity
        ):
            raise RuntimeError("migration preflight backup evidence is invalid")
        settings = Settings(
            state_dir=root,
            runtime_launch_dir=root / "runtime-launches",
        )
        migrated = Database(settings)
        projection = verify_projection(
            migrated,
            owner_private_state_dir=Path(plan.database_path).resolve(strict=True).parent,
        )
        migrated_identity = inspect_sqlite_database(copy)
        if (
            not projection.healthy
            or not migrated_identity.ok
            or migrated_identity.schema_version != plan.target_identity.schema_version
        ):
            raise RuntimeError("target migration or projection preflight failed")
    source_after = inspect_sqlite_database(Path(plan.database_path))
    if source_after != source_before:
        raise RuntimeError("migration preflight changed the source database schema")
    return migrated_identity


def _content_type(result: HttpResult, expected: str) -> bool:
    return result.headers.get("content-type", "").lower().startswith(expected)


def _edge_html_ready(result: HttpResult) -> bool:
    return (
        result.status_code == 200
        and _content_type(result, "text/html")
        and "attachment" not in result.headers.get("content-disposition", "").lower()
    )


def _release_identity_ready(
    result: HttpResult,
    target: ReleaseIdentity,
    *,
    require_ready: bool,
) -> bool:
    body = result.body
    return bool(
        result.status_code == 200
        and _content_type(result, "application/json")
        and body is not None
        and (not require_ready or body.get("ready") is True)
        and body.get("release_id") == target.release_id
        and body.get("schema_version") == target.schema_version
        and body.get("mcp_catalog_digest") == target.mcp_catalog_digest
    )


class HttpxDashboardUpgradeProbe:
    """Bounded loopback readiness checks for a reviewed upgrade plan."""

    def __init__(
        self,
        probe: HttpProbe,
        *,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self.probe = probe
        self.sleeper = sleeper

    def _get(self, url: str, headers: Mapping[str, str]) -> HttpResult:
        try:
            return self.probe.get(url, headers)
        except (httpx.HTTPError, OSError):
            return HttpResult(0, {})

    def _dashboard_bearer(self, plan: DashboardUpgradePlan) -> str:
        return load_dashboard_credentials(Path(plan.credentials_file)).dashboard_bearer

    def require_preflight_ready(
        self, plan: DashboardUpgradePlan, *, target_projection_verified: bool
    ) -> None:
        bearer = self._dashboard_bearer(plan)
        health = self._get(plan.control_plane_origin + "/health", {})
        ready = self._get(plan.control_plane_origin + "/ready", {})
        snapshot = self._get(
            plan.control_plane_origin + "/api/v1/dashboard/v1/snapshot",
            {"Authorization": f"Bearer {bearer}"},
        )
        edge = self._get(plan.edge_origin + "/dashboard/", {})
        current_ready = bool(
            health.status_code == 200
            and health.body is not None
            and health.body.get("status") == "ok"
            and ready.status_code == 200
            and ready.body is not None
            and ready.body.get("ready") is True
        )
        projection_repair_ready = self._projection_repair_preflight_ready(
            plan,
            health,
            ready,
            target_projection_verified=target_projection_verified,
        )
        if not (
            (current_ready or projection_repair_ready)
            and snapshot.status_code == 200
            and _content_type(snapshot, "application/json")
            and _edge_html_ready(edge)
        ):
            raise RuntimeError(
                "upgrade preflight requires a ready or target-verified "
                "projection-repairable Control Plane and a ready Edge"
            )

    @staticmethod
    def _projection_repair_preflight_ready(
        plan: DashboardUpgradePlan,
        health: HttpResult,
        ready: HttpResult,
        *,
        target_projection_verified: bool,
    ) -> bool:
        """Admit only a target-verified repair of source projection readiness.

        This is not a general degraded-mode bypass.  The source database,
        canonical authority, dispatcher, live release identity, Dashboard, and
        Edge must all remain healthy.  The only failing readiness component is
        the source projection, and the disposable target migration must already
        have rebuilt that exact database copy without violations.
        """

        if (
            not target_projection_verified
            or health.status_code != 200
            or ready.status_code != 503
            or not _content_type(health, "application/json")
            or not _content_type(ready, "application/json")
            or health.body is None
            or ready.body is None
            or health.body.get("status") != "ok"
            or ready.body.get("ready") is not False
            or health.body.get("schema_version") != plan.source_identity.schema_version
            or ready.body.get("schema_version") != plan.source_identity.schema_version
            or health.body.get("release_id") != ready.body.get("release_id")
            or health.body.get("mcp_catalog_digest") != ready.body.get("mcp_catalog_digest")
        ):
            return False
        database = ready.body.get("database")
        authority = ready.body.get("authority")
        projection = ready.body.get("projection")
        dispatcher = ready.body.get("dispatcher")
        return bool(
            isinstance(database, Mapping)
            and database.get("ok") is True
            and database.get("integrity") == ["ok"]
            and database.get("foreign_key_errors") == []
            and isinstance(authority, Mapping)
            and authority.get("mode") == "canonical"
            and isinstance(projection, Mapping)
            and projection.get("healthy") is False
            and isinstance(projection.get("violations"), list)
            and len(projection["violations"]) > 0
            and isinstance(dispatcher, Mapping)
            and dispatcher.get("healthy") is True
            and dispatcher.get("running") is True
            and dispatcher.get("issues") == []
        )

    def _wait(
        self,
        plan: DashboardUpgradePlan,
        check: Callable[[], bool],
        *,
        failure: str,
    ) -> None:
        for attempt in range(plan.readiness_attempts):
            if check():
                return
            if attempt + 1 < plan.readiness_attempts:
                self.sleeper(plan.readiness_interval_seconds)
        raise LifecycleUnknownOutcome(failure)

    def wait_for_stopped(self, plan: DashboardUpgradePlan) -> None:
        def stopped() -> bool:
            control_plane = self._get(plan.control_plane_origin + "/health", {})
            edge = self._get(plan.edge_origin + "/dashboard/", {})
            return control_plane.status_code == 0 and edge.status_code == 0

        self._wait(
            plan,
            stopped,
            failure="Control Plane or Dashboard Edge remained reachable after bootout",
        )

    def wait_for_control_plane(self, plan: DashboardUpgradePlan) -> None:
        def ready() -> bool:
            health = self._get(plan.control_plane_origin + "/health", {})
            readiness = self._get(plan.control_plane_origin + "/ready", {})
            return _release_identity_ready(
                health,
                plan.target_identity,
                require_ready=False,
            ) and _release_identity_ready(
                readiness,
                plan.target_identity,
                require_ready=True,
            )

        self._wait(
            plan,
            ready,
            failure="Control Plane did not reach the plan-bound release identity",
        )

    def wait_for_edge(self, plan: DashboardUpgradePlan) -> None:
        bearer = self._dashboard_bearer(plan)

        def ready() -> bool:
            snapshot = self._get(
                plan.control_plane_origin + "/api/v1/dashboard/v1/snapshot",
                {"Authorization": f"Bearer {bearer}"},
            )
            edge = self._get(plan.edge_origin + "/dashboard/", {})
            return (
                snapshot.status_code == 200
                and _content_type(snapshot, "application/json")
                and _edge_html_ready(edge)
            )

        self._wait(
            plan,
            ready,
            failure="Dashboard Edge did not reach the plan-bound release identity",
        )


async def _read_exact_app_server_result(
    rpc: _JsonRpcDesktopSocket,
    request_id: int,
    *,
    timeout: float,
    require_empty: bool,
) -> Mapping[str, object]:
    """Read an exact JSON-RPC result without normalizing a missing result."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise RuntimeError("Codex app-server response timed out")
        try:
            value = await rpc.read_line(remaining)
        except TimeoutError as error:
            raise RuntimeError("Codex app-server response timed out") from error
        if value.get("id") == request_id:
            result = value.get("result")
            if (
                "error" in value
                or "result" not in value
                or not isinstance(result, Mapping)
                or (require_empty and result != {})
            ):
                raise RuntimeError("Codex app-server returned an invalid response")
            return result
        rpc._observe_notification(value)
        await rpc._handle_server_request(value)


def _require_codex_initialize_result(
    result: Mapping[str, object],
    *,
    allow_legacy_lifecycle_originator: bool = False,
) -> None:
    """Reject a generic JSON-RPC peer before sending the reload effect."""

    user_agent = result.get("userAgent")
    codex_home = result.get("codexHome")
    platform_family = result.get("platformFamily")
    platform_os = result.get("platformOs")
    if (
        not isinstance(user_agent, str)
        or not 1 <= len(user_agent) <= 1024
        or (
            _CODEX_USER_AGENT_PATTERN.search(user_agent) is None
            and not (
                allow_legacy_lifecycle_originator
                and user_agent.startswith(_LEGACY_LIFECYCLE_USER_AGENT_PREFIX)
            )
        )
        or not isinstance(codex_home, str)
        or not 1 <= len(codex_home) <= 4096
        or any(character in codex_home for character in "\0\r\n")
        or not Path(codex_home).is_absolute()
        or platform_family != "unix"
        or platform_os not in {"macos", "darwin"}
    ):
        raise RuntimeError("Codex app-server initialize identity is invalid")


def _run_bounded_codex_command(
    argv: Sequence[str],
    timeout: float,
) -> CommandResult:
    """Run one exact Codex host command without a shell or inherited output."""

    completed = subprocess.run(
        list(argv),
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return CommandResult(
        tuple(argv),
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )


def _file_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _verify_openai_codex_signature(
    executable: Path,
    info: os.stat_result,
    command_runner: BoundedCodexCommandRunner,
) -> None:
    requirement = (
        '=identifier "codex" and anchor apple generic and '
        f'certificate leaf[subject.OU] = "{_OPENAI_TEAM_IDENTIFIER}"'
    )
    try:
        verified = command_runner(
            (
                "/usr/bin/codesign",
                "--verify",
                "--strict",
                "--test-requirement",
                requirement,
                os.fspath(executable),
            ),
            _CODEX_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise RuntimeError("Codex app-server signature verification failed") from error
    if verified.returncode != 0:
        raise RuntimeError("Codex app-server signature verification failed")
    try:
        verified_info = executable.lstat()
    except OSError as error:
        raise RuntimeError("Codex app-server executable identity changed") from error
    if _file_identity(verified_info) != _file_identity(info):
        raise RuntimeError("Codex app-server executable identity changed")


def _checked_codex_app_server_executable(
    command_runner: BoundedCodexCommandRunner,
) -> tuple[Path, os.stat_result]:
    """Require the immutable bundled executable and its OpenAI signature."""

    executable = canonical_codex_app_server_executable()
    if not executable.is_absolute():
        raise RuntimeError("Codex app-server executable identity is invalid")
    try:
        info = executable.lstat()
        resolved = executable.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise RuntimeError("Codex app-server executable is unavailable") from error
    if (
        resolved != executable
        or stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
        or not os.access(executable, os.X_OK)
    ):
        raise RuntimeError("Codex app-server executable identity is invalid")
    _verify_openai_codex_signature(executable, info, command_runner)
    return executable, info


def _bound_codex_desktop_launchagent(
    binding: CodexDesktopLaunchAgentBinding,
) -> Mapping[str, object]:
    """Re-read the exact owner plist without exposing its environment."""

    _require_codex_desktop_launchagent_shape(binding)
    path = Path(binding.plist_path)
    try:
        info = path.lstat()
        payload = path.read_bytes()
        parsed = plistlib.loads(payload)
        after = path.lstat()
    except (OSError, plistlib.InvalidFileException, ValueError) as error:
        raise RuntimeError("Codex Desktop LaunchAgent plist is unavailable") from error
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
        or _file_identity(after) != _file_identity(info)
        or hashlib.sha256(payload).hexdigest() != binding.plist_sha256
        or not isinstance(parsed, Mapping)
    ):
        raise RuntimeError("Codex Desktop LaunchAgent plist identity changed")
    arguments = parsed.get("ProgramArguments")
    try:
        environment_sha256 = _codex_launchagent_environment_digest(
            parsed.get("EnvironmentVariables")
        )
    except ValueError as error:
        raise RuntimeError("Codex Desktop LaunchAgent environment is invalid") from error
    if (
        parsed.get("Label") != binding.label
        or not isinstance(arguments, list)
        or tuple(arguments) != binding.program_arguments
        or not _codex_launchagent_fixed_semantics(parsed)
        or environment_sha256 != binding.environment_sha256
    ):
        raise RuntimeError("Codex Desktop LaunchAgent plist semantics changed")
    return parsed


def _loaded_codex_desktop_launchagent_asid(
    binding: CodexDesktopLaunchAgentBinding,
    peer_pid: int,
    command_runner: BoundedCodexCommandRunner,
) -> int:
    _bound_codex_desktop_launchagent(binding)
    command = (
        "/bin/launchctl",
        "print",
        f"gui/{os.getuid()}/{binding.label}",
    )
    try:
        observed = command_runner(command, _CODEX_COMMAND_TIMEOUT_SECONDS)
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise RuntimeError("Codex Desktop LaunchAgent observation failed") from error
    raw_pid = _launchctl_scalar(observed.stdout, "pid")
    raw_asid = _launchctl_scalar(observed.stdout, "asid")
    if (
        observed.returncode != 0
        or _launchctl_scalar(observed.stdout, "path") != binding.plist_path
        or _launchctl_scalar(observed.stdout, "state") != "running"
        or _launchctl_scalar(observed.stdout, "program") != binding.program_arguments[0]
        or _launchctl_arguments(observed.stdout) != binding.program_arguments
        or raw_pid is None
        or raw_asid is None
        or not raw_pid.isascii()
        or not raw_pid.isdecimal()
        or not raw_asid.isascii()
        or not raw_asid.isdecimal()
        or int(raw_pid) != peer_pid
        or int(raw_asid) <= 0
    ):
        raise RuntimeError("loaded Codex Desktop LaunchAgent identity changed")
    return int(raw_asid)


def _looks_like_cao_mcp_bridge_command(command: str) -> bool:
    return "mcp-stdio" in command and any(
        marker in command for marker in ("cao-a2a", "cao-dashboard", "cao_control_plane.cli")
    )


def _is_cao_mcp_bridge_process(executable: Path, argv: tuple[str, ...]) -> bool:
    if not argv:
        return False
    try:
        entry = Path(argv[0])
        if not entry.is_absolute() or entry.resolve(strict=True) != executable:
            return False
    except (OSError, UnicodeError):
        return False
    arguments = list(argv[1:])
    if executable.name == "uv":
        if len(arguments) >= 2 and arguments[0] == "--directory":
            arguments = arguments[2:]
        elif arguments and arguments[0].startswith("--directory="):
            arguments = arguments[1:]
        return (
            len(arguments) >= 3
            and arguments[0] == "run"
            and arguments[1] in {"cao-a2a", "cao-dashboard"}
            and arguments[2] == "mcp-stdio"
        )
    if len(arguments) >= 3 and arguments[:3] == [
        "-m",
        "cao_control_plane.cli",
        "mcp-stdio",
    ]:
        return True
    if len(arguments) < 2 or arguments[1] != "mcp-stdio":
        return False
    try:
        console_script = Path(arguments[0]).resolve(strict=True)
        interpreter_bin = entry.parent.resolve(strict=True)
    except (OSError, UnicodeError):
        return False
    return (
        console_script.name in {"cao-a2a", "cao-dashboard"}
        and console_script.parent == interpreter_bin
    )


def _lifecycle_process_argv(pid: int) -> tuple[str, ...]:
    """Read argv for lifecycle identity checks without issuer dependencies."""

    try:
        result = subprocess.run(
            ["/bin/ps", "-ww", "-p", str(pid), "-o", "command="],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=1.0,
        )
        if result.returncode != 0 or len(result.stdout) > 1_048_576:
            raise ValueError
        argv = tuple(shlex.split(result.stdout.strip(), posix=True))
        if not argv:
            raise ValueError
        return argv
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise RuntimeError("lifecycle process argv is unavailable") from error


class DarwinCodexAppServerHostInspector:
    """Kernel and process-table checks for one owner-local Desktop host."""

    def __init__(
        self,
        *,
        process_api: _DarwinDashboardProcessAPI | None = None,
    ) -> None:
        if sys.platform != "darwin":
            raise RuntimeError("Codex Desktop host inspection requires macOS")
        self._process_api: _DarwinDashboardProcessAPI = (
            process_api or DarwinDashboardProcessAPI()
        )

    def inspect(self, socket_path: Path) -> CodexAppServerPeerObservation:
        from .attachment_issuer import AttachmentIssuerError, _peer_uid
        from .runtime_enrollment import EnrollmentCapabilityError, _peer_pid, _process_identity

        peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            peer.settimeout(5.0)
            peer.connect(os.fspath(socket_path))
            uid = _peer_uid(peer)
            pid = _peer_pid(peer)
        except (OSError, AttachmentIssuerError, EnrollmentCapabilityError) as error:
            raise RuntimeError("Codex app-server socket peer identity is unavailable") from error
        finally:
            peer.close()
        if uid != os.geteuid() or pid <= 1:
            raise RuntimeError("Codex app-server socket peer is not owner-local")
        try:
            identity = _process_identity(pid)
        except EnrollmentCapabilityError as error:
            raise RuntimeError("Codex app-server process generation is unavailable") from error
        try:
            argv = _lifecycle_process_argv(pid)
        except RuntimeError:
            argv = None
        return CodexAppServerPeerObservation(
            pid=pid,
            uid=uid,
            start_signature=identity.start_signature,
            executable_path=None,
            argv=argv,
        )

    def capture_process(
        self,
        pid: int,
        *,
        expected_asid: int,
    ) -> DarwinAuditProcessIdentity:
        return self._process_api.capture(pid, expected_asid=expected_asid)

    def executable_path(self, process: DarwinAuditProcessIdentity) -> Path:
        return self._process_api.executable_path(process)

    @staticmethod
    def _process_rows() -> tuple[tuple[int, int, str], ...]:
        try:
            observed = subprocess.run(
                ["/bin/ps", "-axo", "pid=,ppid=,command="],
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=2.0,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise RuntimeError("Codex app-server descendant table is unavailable") from error
        if observed.returncode != 0 or len(observed.stdout) > 8_000_000:
            raise RuntimeError("Codex app-server descendant table is unavailable")
        rows: list[tuple[int, int, str]] = []
        for line in observed.stdout.splitlines():
            fields = line.strip().split(None, 2)
            if (
                len(fields) != 3
                or not fields[0].isascii()
                or not fields[0].isdecimal()
                or not fields[1].isascii()
                or not fields[1].isdecimal()
            ):
                continue
            rows.append((int(fields[0]), int(fields[1]), fields[2]))
        if len(rows) > 100_000:
            raise RuntimeError("Codex app-server descendant table is unbounded")
        return tuple(rows)

    @classmethod
    def _descendant_candidates(cls, root_pid: int) -> tuple[int, ...]:
        rows = cls._process_rows()
        children: dict[int, list[tuple[int, str]]] = {}
        for pid, parent_pid, command in rows:
            children.setdefault(parent_pid, []).append((pid, command))
        candidates: set[int] = set()
        frontier = {root_pid}
        visited = {root_pid}
        for _depth in range(8):
            next_frontier: set[int] = set()
            for parent_pid in frontier:
                for pid, command in children.get(parent_pid, []):
                    if pid in visited:
                        continue
                    visited.add(pid)
                    next_frontier.add(pid)
                    if _looks_like_cao_mcp_bridge_command(command):
                        candidates.add(pid)
            if not next_frontier:
                break
            if len(visited) > 4096:
                raise RuntimeError("Codex app-server descendant set is unbounded")
            frontier = next_frontier
        return tuple(sorted(candidates))

    def _capture_cao_bridge_set(
        self,
        root_pid: int,
        *,
        expected_asid: int,
    ) -> tuple[DarwinAuditProcessIdentity, ...]:
        captured: list[DarwinAuditProcessIdentity] = []
        for pid in self._descendant_candidates(root_pid):
            try:
                process = self._process_api.capture(pid, expected_asid=expected_asid)
                executable = self._process_api.executable_path(process)
                argv = _lifecycle_process_argv(pid)
            except RuntimeError as error:
                raise RuntimeError("CAO MCP bridge generation is unavailable") from error
            if not _is_cao_mcp_bridge_process(executable, argv):
                raise RuntimeError("CAO MCP bridge generation is invalid")
            captured.append(process)
        return tuple(sorted(captured, key=lambda item: item.pid))

    def capture_cao_bridges(
        self,
        root_pid: int,
        *,
        expected_asid: int,
    ) -> tuple[DarwinAuditProcessIdentity, ...]:
        first = self._capture_cao_bridge_set(root_pid, expected_asid=expected_asid)
        second = self._capture_cao_bridge_set(root_pid, expected_asid=expected_asid)
        if first != second:
            raise RuntimeError("CAO MCP bridge generations changed during preflight")
        return first

    def is_live(self, process: DarwinAuditProcessIdentity) -> bool:
        return self._process_api.is_live(process)


def _mapped_codex_executable_matches(
    process: DarwinAuditProcessIdentity,
    executable: Path,
    signed_info: os.stat_result,
    command_runner: BoundedCodexCommandRunner,
) -> bool:
    command = (
        "/usr/sbin/lsof",
        "-a",
        "-p",
        str(process.pid),
        "-d",
        "txt",
        "-F",
        "pfDin",
    )
    try:
        observed = command_runner(command, _CODEX_COMMAND_TIMEOUT_SECONDS)
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise RuntimeError("Codex app-server mapped image check failed") from error
    if observed.returncode != 0 or len(observed.stdout) > 8_000_000:
        raise RuntimeError("Codex app-server mapped image check failed")
    process_lines = [line for line in observed.stdout.splitlines() if line.startswith("p")]
    if process_lines != [f"p{process.pid}"]:
        raise RuntimeError("Codex app-server mapped image response is invalid")
    records: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in observed.stdout.splitlines():
        if line.startswith("f"):
            if current is not None:
                records.append(current)
            current = {"f": line[1:]}
        elif current is not None and line[:1] in {"D", "i", "n"}:
            current[line[0]] = line[1:]
    if current is not None:
        records.append(current)
    try:
        current_info = executable.lstat()
    except OSError as error:
        raise RuntimeError("Codex app-server signed executable changed") from error
    if _file_identity(current_info) != _file_identity(signed_info):
        raise RuntimeError("Codex app-server signed executable changed")
    if not records or records[0].get("f") != "txt":
        raise RuntimeError("Codex app-server mapped image response is invalid")
    record = records[0]
    raw_name = record.get("n", "")
    try:
        device = int(record.get("D", ""), 0)
        inode = int(record.get("i", ""), 10)
    except ValueError as error:
        raise RuntimeError("Codex app-server mapped image response is invalid") from error
    if (
        not raw_name
        or not Path(raw_name).is_absolute()
        or any(character in raw_name for character in "\0\r\n")
    ):
        raise RuntimeError("Codex app-server mapped image response is invalid")
    if device != signed_info.st_dev or inode != signed_info.st_ino:
        return False
    try:
        mapped_path = Path(raw_name).resolve(strict=True)
    except OSError as error:
        raise RuntimeError("Codex app-server mapped image response is invalid") from error
    if mapped_path != executable:
        raise RuntimeError("Codex app-server mapped image response is inconsistent")
    return True


async def _reload_codex_mcp_server(
    socket_path: Path,
    release_id: str,
    *,
    expected_socket_identity: tuple[int, int] | None = None,
    allow_legacy_lifecycle_originator: bool = False,
) -> None:
    from .runtime import _JsonRpcDesktopSocket

    socket_before = _checked_codex_socket(socket_path)
    if (
        expected_socket_identity is not None
        and (
            socket_before.st_dev,
            socket_before.st_ino,
        )
        != expected_socket_identity
    ):
        raise RuntimeError("Codex app-server socket changed after daemon preflight")
    rpc: _JsonRpcDesktopSocket | None = None
    reload_sent = False
    try:
        rpc = await _JsonRpcDesktopSocket.connect(socket_path, timeout=5.0)
        socket_after = _checked_codex_socket(socket_path)
        if (socket_before.st_dev, socket_before.st_ino) != (
            socket_after.st_dev,
            socket_after.st_ino,
        ):
            raise RuntimeError("Codex app-server socket changed identity while connecting")
        initialize_request_id = rpc.next_id
        rpc.next_id += 1
        if initialize_request_id != 1:
            raise RuntimeError("Codex app-server request sequence is invalid")
        await rpc.send(
            {
                "id": initialize_request_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        # This is the app-server's documented non-originating
                        # probe identity. A custom client name changes the
                        # process-global originator and is then echoed in the
                        # returned user agent, which makes identity validation
                        # both disruptive and self-referential.
                        "name": _CODEX_APP_SERVER_PROBE_CLIENT_NAME,
                        "version": release_id,
                    },
                    "capabilities": {"experimentalApi": True},
                },
            },
        )
        initialize_result = await _read_exact_app_server_result(
            rpc,
            initialize_request_id,
            timeout=5.0,
            require_empty=False,
        )
        _require_codex_initialize_result(
            initialize_result,
            allow_legacy_lifecycle_originator=(allow_legacy_lifecycle_originator),
        )
        await rpc.send({"method": "initialized", "params": {}})
        request_id = rpc.next_id
        if request_id != 2:
            raise RuntimeError("Codex app-server request sequence is invalid")
        rpc.next_id += 1
        reload_sent = True
        await rpc.send({"id": request_id, "method": "config/mcpServer/reload"})
        await _read_exact_app_server_result(
            rpc,
            request_id,
            timeout=5.0,
            require_empty=True,
        )
    except Exception as error:
        if reload_sent:
            raise LifecycleUnknownOutcome("Codex MCP reload outcome is unknown") from error
        raise RuntimeError("Codex app-server MCP reload preflight failed") from error
    finally:
        if rpc is not None:
            await rpc.close()


def reload_codex_mcp_server(
    socket_path: Path,
    release_id: str,
    *,
    expected_socket_identity: tuple[int, int] | None = None,
) -> None:
    """Queue one bounded refresh through the owner Desktop WebSocket."""

    asyncio.run(
        _reload_codex_mcp_server(
            socket_path,
            release_id,
            expected_socket_identity=expected_socket_identity,
        )
    )


def _reload_codex_mcp_server_after_preflight(
    socket_path: Path,
    release_id: str,
    prepared: CodexAppServerMCPPreflight,
) -> None:
    """Reload one attested host, including a bounded legacy-originator recovery."""

    if (
        prepared.backend not in {"desktop_owner_local", "managed_pid"}
        or prepared.desktop_restart_required
    ):
        raise RuntimeError("Codex app-server reload preflight token is invalid")
    asyncio.run(
        _reload_codex_mcp_server(
            socket_path,
            release_id,
            expected_socket_identity=prepared.socket_identity,
            allow_legacy_lifecycle_originator=True,
        )
    )


def _bounded_codex_version(value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 256
        or any(character in value for character in "\0\r\n")
    ):
        raise RuntimeError("Codex app-server daemon version identity is invalid")
    return value


def _checked_owner_directory(path: Path, *, label: str) -> os.stat_result:
    try:
        info = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise RuntimeError(f"{label} is unavailable") from error
    if (
        resolved != path
        or stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise RuntimeError(f"{label} identity is invalid")
    return info


def _checked_managed_codex_executable(
    path: Path,
    expected_version: str,
    command_runner: BoundedCodexCommandRunner,
) -> None:
    """Accept only the standalone installer's controlled symlink lineage."""

    current = path.parent
    standalone = current.parent
    packages = standalone.parent
    codex_home = packages.parent
    releases = standalone / "releases"
    for directory, label in (
        (codex_home, "Codex home"),
        (packages, "managed Codex packages directory"),
        (standalone, "managed Codex standalone directory"),
        (releases, "managed Codex releases directory"),
    ):
        _checked_owner_directory(directory, label=label)
    try:
        current_info = current.lstat()
        current_target = Path(os.readlink(current))
        release = current.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise RuntimeError("managed Codex current link is unavailable") from error
    if (
        not stat.S_ISLNK(current_info.st_mode)
        or current_info.st_uid not in {0, os.getuid()}
        or not release.is_absolute()
        or release.parent != releases
        or not 1 <= len(release.name) <= 128
        or release.name.startswith(".")
        or release.name != expected_version
    ):
        raise RuntimeError("managed Codex current link identity is invalid")
    target_candidate = (
        current_target if current_target.is_absolute() else current.parent / current_target
    )
    try:
        if target_candidate.resolve(strict=True) != release:
            raise RuntimeError("managed Codex current link identity is invalid")
    except OSError as error:
        raise RuntimeError("managed Codex current link identity is invalid") from error
    _checked_owner_directory(release, label="managed Codex release directory")
    try:
        leaf_info = path.lstat()
        if stat.S_ISLNK(leaf_info.st_mode):
            if leaf_info.st_uid not in {0, os.getuid()} or os.readlink(path) != "bin/codex":
                raise RuntimeError("managed Codex executable link identity is invalid")
            target = release / "bin" / "codex"
            _checked_owner_directory(target.parent, label="managed Codex binary directory")
        elif stat.S_ISREG(leaf_info.st_mode):
            target = release / "codex"
        else:
            raise RuntimeError("managed Codex executable link identity is invalid")
        resolved = path.resolve(strict=True)
        info = resolved.lstat()
    except (FileNotFoundError, OSError) as error:
        raise RuntimeError("managed Codex daemon executable is unavailable") from error
    if (
        resolved != target
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
        or not os.access(resolved, os.X_OK)
    ):
        raise RuntimeError("managed Codex daemon executable identity is invalid")
    _verify_openai_codex_signature(resolved, info, command_runner)
    version_command = (os.fspath(resolved), "--version")
    try:
        observed_version = command_runner(
            version_command,
            _CODEX_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise RuntimeError("managed Codex daemon version check failed") from error
    expected_output = f"codex-cli {expected_version}"
    if observed_version.returncode != 0 or observed_version.stdout not in {
        expected_output,
        expected_output + "\n",
    }:
        raise RuntimeError("managed Codex daemon version identity is invalid")
    try:
        if (
            _file_identity(current.lstat()) != _file_identity(current_info)
            or _file_identity(path.lstat()) != _file_identity(leaf_info)
            or path.resolve(strict=True) != resolved
            or _file_identity(resolved.lstat()) != _file_identity(info)
        ):
            raise RuntimeError("managed Codex executable lineage changed")
    except OSError as error:
        raise RuntimeError("managed Codex executable lineage changed") from error


def preflight_codex_app_server_mcp(
    socket_path: Path,
    *,
    command_runner: BoundedCodexCommandRunner = _run_bounded_codex_command,
    desktop_launchagent: CodexDesktopLaunchAgentBinding | None = None,
    host_inspector: CodexAppServerHostInspector | None = None,
    expected_bundled_executable_identity: tuple[int, int, int, int, int] | None = None,
) -> CodexAppServerMCPPreflight:
    """Classify the canonical running host without mutating its lifecycle."""

    if socket_path != canonical_codex_app_server_socket():
        raise RuntimeError("Codex app-server refresh requires the canonical control socket")
    _require_codex_desktop_launchagent_shape(desktop_launchagent)
    socket_before = _checked_codex_socket(socket_path)
    executable, signed_info = _checked_codex_app_server_executable(command_runner)
    if (
        expected_bundled_executable_identity is not None
        and _file_identity(signed_info) != expected_bundled_executable_identity
    ):
        raise RuntimeError("Codex app-server executable changed after planning")
    version_command = (
        os.fspath(executable),
        "app-server",
        "daemon",
        "version",
    )
    try:
        observed = command_runner(version_command, _CODEX_COMMAND_TIMEOUT_SECONDS)
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise RuntimeError("Codex app-server daemon preflight failed") from error
    if observed.returncode != 0:
        raise RuntimeError("Codex app-server daemon preflight failed")
    try:
        payload = json.loads(observed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("Codex app-server daemon preflight response is invalid") from error
    socket_after = _checked_codex_socket(socket_path)
    if (socket_before.st_dev, socket_before.st_ino) != (
        socket_after.st_dev,
        socket_after.st_ino,
    ):
        raise RuntimeError("Codex app-server socket changed during daemon preflight")
    expected_managed_path = (
        socket_path.parent.parent / "packages" / "standalone" / "current" / "codex"
    )
    if (
        not isinstance(payload, Mapping)
        or payload.get("status") != "running"
        or payload.get("socketPath") != os.fspath(socket_path)
        or payload.get("managedCodexPath") != os.fspath(expected_managed_path)
        or _bounded_codex_version(payload.get("cliVersion")) is None
        or _bounded_codex_version(payload.get("appServerVersion")) is None
    ):
        raise RuntimeError("Codex app-server daemon preflight response is invalid")
    backend = payload.get("backend")
    managed_version = _bounded_codex_version(payload.get("managedCodexVersion"))
    if backend is None:
        if desktop_launchagent is None:
            raise RuntimeError("Desktop app-server requires a plan-bound owner LaunchAgent")
        inspector = host_inspector or DarwinCodexAppServerHostInspector()
        peer = inspector.inspect(socket_path)
        socket_peer_checked = _checked_codex_socket(socket_path)
        if (socket_peer_checked.st_dev, socket_peer_checked.st_ino) != (
            socket_after.st_dev,
            socket_after.st_ino,
        ):
            raise RuntimeError("Codex app-server socket changed during peer attestation")
        asid = _loaded_codex_desktop_launchagent_asid(
            desktop_launchagent,
            peer.pid,
            command_runner,
        )
        process = inspector.capture_process(peer.pid, expected_asid=asid)
        if process.pid != peer.pid or process.uid != os.geteuid():
            raise RuntimeError("Codex Desktop process generation is invalid")
        try:
            process_path = inspector.executable_path(process)
        except RuntimeError:
            process_path = None
        peer = CodexAppServerPeerObservation(
            pid=peer.pid,
            uid=peer.uid,
            start_signature=peer.start_signature,
            executable_path=(os.fspath(process_path) if process_path is not None else None),
            argv=peer.argv,
        )
        mapped_executable_current = _mapped_codex_executable_matches(
            process,
            executable,
            signed_info,
            command_runner,
        )
        if mapped_executable_current:
            if process_path is None:
                raise RuntimeError("Codex app-server mapped executable path is unavailable")
            if process_path != executable:
                raise RuntimeError("Codex app-server mapped executable identity is inconsistent")
            if peer.argv is None:
                raise RuntimeError("Codex app-server argv identity is unavailable")
        attested = (
            process_path == executable
            and peer.argv == desktop_launchagent.program_arguments
            and mapped_executable_current
        )
        bridges = () if attested else inspector.capture_cao_bridges(peer.pid, expected_asid=asid)
        process_rechecked = inspector.capture_process(peer.pid, expected_asid=asid)
        if process_rechecked != process:
            raise RuntimeError("Codex Desktop process generation changed during preflight")
        socket_final = _checked_codex_socket(socket_path)
        if (socket_final.st_dev, socket_final.st_ino) != (
            socket_after.st_dev,
            socket_after.st_ino,
        ):
            raise RuntimeError("Codex app-server socket changed during host attestation")
        return CodexAppServerMCPPreflight(
            backend="desktop_owner_local",
            socket_device=socket_final.st_dev,
            socket_inode=socket_final.st_ino,
            bundled_executable_identity=_file_identity(signed_info),
            desktop_peer=peer,
            desktop_process=process,
            desktop_bridges=bridges,
            desktop_restart_required=not attested,
        )
    if backend != "pid" or managed_version is None:
        raise RuntimeError("Codex app-server managed daemon identity is invalid")
    _checked_managed_codex_executable(
        expected_managed_path,
        managed_version,
        command_runner,
    )
    return CodexAppServerMCPPreflight(
        backend="managed_pid",
        socket_device=socket_after.st_dev,
        socket_inode=socket_after.st_ino,
        bundled_executable_identity=_file_identity(signed_info),
    )


def _kickstart_codex_desktop_app_server(
    socket_path: Path,
    prepared: CodexAppServerMCPPreflight,
    binding: CodexDesktopLaunchAgentBinding,
    *,
    command_runner: BoundedCodexCommandRunner,
    host_inspector: CodexAppServerHostInspector,
    sleeper: Callable[[float], None],
    attempts: int,
    interval_seconds: float,
    expected_bundled_executable_identity: tuple[int, int, int, int, int] | None,
) -> CodexAppServerMCPPreflight:
    """Replace one exact stale Desktop generation through its owner LaunchAgent."""

    old_peer = prepared.desktop_peer
    old_process = prepared.desktop_process
    if (
        not prepared.desktop_restart_required
        or old_peer is None
        or old_process is None
        or attempts < 1
        or interval_seconds < 0
    ):
        raise RuntimeError("Codex Desktop restart preconditions are invalid")

    # Revalidate every plan and kernel fence immediately before the one effect.
    executable, executable_info = _checked_codex_app_server_executable(command_runner)
    if (
        executable != Path(binding.program_arguments[0])
        or _file_identity(executable_info) != prepared.bundled_executable_identity
        or (
            expected_bundled_executable_identity is not None
            and _file_identity(executable_info) != expected_bundled_executable_identity
        )
    ):
        raise RuntimeError("signed Codex Desktop executable changed before restart")
    current_socket = _checked_codex_socket(socket_path)
    if (current_socket.st_dev, current_socket.st_ino) != prepared.socket_identity:
        raise RuntimeError("Codex app-server socket changed before targeted restart")
    current_peer = host_inspector.inspect(socket_path)
    if (
        current_peer.pid != old_peer.pid
        or current_peer.uid != old_peer.uid
        or current_peer.start_signature != old_peer.start_signature
    ):
        raise RuntimeError("Codex Desktop peer changed before targeted restart")
    asid = _loaded_codex_desktop_launchagent_asid(
        binding,
        old_peer.pid,
        command_runner,
    )
    if host_inspector.capture_process(old_peer.pid, expected_asid=asid) != old_process:
        raise RuntimeError("Codex Desktop process changed before targeted restart")
    if (
        host_inspector.capture_cao_bridges(old_peer.pid, expected_asid=asid)
        != prepared.desktop_bridges
    ):
        raise RuntimeError("CAO MCP bridge generations changed before targeted restart")

    command = (
        "/bin/launchctl",
        "kickstart",
        "-kp",
        f"gui/{os.getuid()}/{binding.label}",
    )
    try:
        kicked = command_runner(command, _CODEX_COMMAND_TIMEOUT_SECONDS)
    except (OSError, subprocess.SubprocessError, TimeoutError) as error:
        raise LifecycleUnknownOutcome(
            "targeted Codex Desktop restart outcome is unknown"
        ) from error
    raw_new_pid = kicked.stdout.strip()
    if (
        kicked.returncode != 0
        or not raw_new_pid.isascii()
        or not raw_new_pid.isdecimal()
        or int(raw_new_pid) <= 1
        or int(raw_new_pid) == old_peer.pid
    ):
        raise LifecycleUnknownOutcome("targeted Codex Desktop restart outcome is unknown")
    new_pid = int(raw_new_pid)

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            socket_info = _checked_codex_socket(socket_path)
            if (socket_info.st_dev, socket_info.st_ino) == prepared.socket_identity:
                raise RuntimeError("Codex app-server socket was not replaced")
            candidate_peer = host_inspector.inspect(socket_path)
            if candidate_peer.pid != new_pid:
                raise RuntimeError("Codex app-server socket is not owned by the kicked generation")
            if host_inspector.is_live(old_process):
                raise RuntimeError("old Codex Desktop process generation remains live")
            if any(host_inspector.is_live(bridge) for bridge in prepared.desktop_bridges):
                raise RuntimeError("old CAO MCP bridge generation remains live")
            post_restart = preflight_codex_app_server_mcp(
                socket_path,
                command_runner=command_runner,
                desktop_launchagent=binding,
                host_inspector=host_inspector,
                expected_bundled_executable_identity=(expected_bundled_executable_identity),
            )
            if (
                post_restart.desktop_restart_required
                or post_restart.desktop_peer is None
                or post_restart.desktop_peer.pid != new_pid
                or post_restart.socket_identity == prepared.socket_identity
            ):
                raise RuntimeError("restarted Codex Desktop host is not attested")
            return post_restart
        except (LifecycleUnknownOutcome, RuntimeError, ValueError) as error:
            last_error = error
        if attempt + 1 < attempts:
            sleeper(interval_seconds)
    raise LifecycleUnknownOutcome(
        "targeted Codex Desktop restart postcondition is unknown"
    ) from last_error


def refresh_codex_app_server_mcp(
    socket_path: Path,
    release_id: str,
    *,
    command_runner: BoundedCodexCommandRunner = _run_bounded_codex_command,
    reload_codex_mcp: CodexMCPReloader = reload_codex_mcp_server,
    desktop_launchagent: CodexDesktopLaunchAgentBinding | None = None,
    host_inspector: CodexAppServerHostInspector | None = None,
    sleeper: Callable[[float], None] = sleep,
    restart_attempts: int = _CODEX_HOST_RESTART_ATTEMPTS,
    restart_interval_seconds: float = _CODEX_HOST_RESTART_INTERVAL_SECONDS,
    expected_bundled_executable_identity: tuple[int, int, int, int, int] | None = None,
) -> str:
    """Reload a healthy host, replacing it only for a proven stale image."""

    prepared = preflight_codex_app_server_mcp(
        socket_path,
        command_runner=command_runner,
        desktop_launchagent=desktop_launchagent,
        host_inspector=host_inspector,
        expected_bundled_executable_identity=expected_bundled_executable_identity,
    )
    restart_status = _CODEX_APP_SERVER_RESTART_STATUS
    if prepared.desktop_restart_required:
        if desktop_launchagent is None:
            raise RuntimeError("stale Desktop host has no plan-bound LaunchAgent")
        inspector = host_inspector or DarwinCodexAppServerHostInspector()
        prepared = _kickstart_codex_desktop_app_server(
            socket_path,
            prepared,
            desktop_launchagent,
            command_runner=command_runner,
            host_inspector=inspector,
            sleeper=sleeper,
            attempts=restart_attempts,
            interval_seconds=restart_interval_seconds,
            expected_bundled_executable_identity=(expected_bundled_executable_identity),
        )
        restart_status = _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS
    try:
        if expected_bundled_executable_identity is not None:
            _require_current_codex_executable_plan_identity(
                os.fspath(canonical_codex_app_server_executable()),
                expected_bundled_executable_identity,
            )
        if reload_codex_mcp is reload_codex_mcp_server:
            _reload_codex_mcp_server_after_preflight(
                socket_path,
                release_id,
                prepared,
            )
        else:
            current = _checked_codex_socket(socket_path)
            if (current.st_dev, current.st_ino) != prepared.socket_identity:
                raise RuntimeError("Codex app-server socket changed after daemon preflight")
            reload_codex_mcp(socket_path, release_id)
    except LifecycleUnknownOutcome as error:
        if restart_status == _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS:
            raise CodexMCPRefreshPhaseUnknown(
                "Codex MCP reload outcome is unknown after targeted restart",
                reload_status="unknown",
            ) from error
        raise
    except Exception as error:
        if restart_status == _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS:
            raise CodexMCPRefreshPhaseError(
                "Codex MCP reload was not submitted after targeted restart",
                reload_status="not_submitted",
            ) from error
        raise
    return restart_status


def refresh_codex_mcp(
    plan: CodexMCPRefreshPlan,
    *,
    reload_codex_mcp: CodexMCPReloader = refresh_codex_app_server_mcp,
    release_identity: Callable[[], ReleaseIdentity] = current_release_identity,
    receipt: EffectAuthorization | None = None,
    execute: bool = False,
    owner_confirmed: bool = False,
) -> CodexMCPRefreshResult:
    """Refresh the host without signaling bridge PIDs or using a broad kill."""

    _require_codex_mcp_refresh_plan(plan)
    require_effect_authorization(
        plan,
        "refresh-mcp",
        receipt=receipt,
        execute=execute,
        owner_confirmed=owner_confirmed,
    )
    if release_identity() != plan.target_identity:
        raise RuntimeError("target release identity drifted after the MCP refresh plan")
    socket_path = Path(plan.codex_app_server_socket)
    _checked_codex_socket(socket_path)
    raw_restart_status: str | None
    if reload_codex_mcp is refresh_codex_app_server_mcp:
        _require_current_codex_executable_plan_identity(
            plan.codex_app_server_executable,
            plan.codex_app_server_executable_identity,
        )
        raw_restart_status = refresh_codex_app_server_mcp(
            socket_path,
            plan.target_identity.release_id,
            desktop_launchagent=plan.codex_desktop_launchagent,
            expected_bundled_executable_identity=(plan.codex_app_server_executable_identity),
        )
    else:
        raw_restart_status = reload_codex_mcp(
            socket_path,
            plan.target_identity.release_id,
        )
    restart_status = _normalized_codex_restart_status(raw_restart_status)
    return CodexMCPRefreshResult(
        status=_CODEX_MCP_RELOAD_STATUS,
        plan_digest=plan.plan_digest,
        target_identity=plan.target_identity,
        codex_app_server_restart=restart_status,
        codex_mcp_reload=_CODEX_MCP_RELOAD_STATUS,
        codex_mcp_reload_scope=plan.codex_mcp_reload_scope,
        codex_mcp_reload_application=plan.codex_mcp_reload_application,
        current_conversation_verification=_CODEX_CURRENT_CONVERSATION_VERIFICATION,
    )


def _normalized_codex_restart_status(value: str | None) -> str:
    if value is None or value == _CODEX_APP_SERVER_RESTART_STATUS:
        return _CODEX_APP_SERVER_RESTART_STATUS
    if value == _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS:
        return _CODEX_APP_SERVER_RESTART_PERFORMED_STATUS
    raise RuntimeError("Codex app-server restart result is invalid")


def _launchctl_effect(
    runner: CommandRunner,
    action: str,
    binding: UpgradeLaunchAgentBinding,
) -> None:
    _require_upgrade_plist(binding)
    command = ("launchctl", action, f"gui/{os.getuid()}", binding.plist_path)
    if runner.run(command).returncode != 0:
        raise LifecycleUnknownOutcome(f"launchctl {action} outcome is unknown for {binding.label}")


def _require_launchagent_unloaded(
    runner: CommandRunner,
    binding: UpgradeLaunchAgentBinding,
) -> None:
    command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
    if runner.run(command).returncode == 0:
        raise LifecycleUnknownOutcome(f"LaunchAgent {binding.label} is still loaded")


def upgrade_dashboard_lifecycle(
    plan: DashboardUpgradePlan,
    runner: CommandRunner,
    *,
    probe: DashboardUpgradeReadinessProbe,
    process_controller: DashboardUpgradeProcessController | None = None,
    preflight: DashboardUpgradePreflight = preflight_dashboard_upgrade,
    backup_database: DashboardUpgradeBackup = backup_sqlite_database,
    reload_codex_mcp: CodexMCPReloader = refresh_codex_app_server_mcp,
    codex_host_preflight: Callable[
        [Path], CodexAppServerMCPPreflight
    ] = preflight_codex_app_server_mcp,
    receipt: EffectAuthorization | None = None,
    execute: bool = False,
    owner_confirmed: bool = False,
) -> DashboardUpgradeResult:
    """Back up and restart Edge/Control Plane in one fail-closed order."""

    _require_dashboard_upgrade_plan(plan)
    require_effect_authorization(
        plan,
        "upgrade",
        receipt=receipt,
        execute=execute,
        owner_confirmed=owner_confirmed,
    )
    if (
        reload_codex_mcp is refresh_codex_app_server_mcp
        and Path(plan.codex_app_server_socket) != canonical_codex_app_server_socket()
    ):
        raise ValueError("dashboard upgrade requires the canonical Codex app-server control socket")
    raw_restart_status: str | None
    if reload_codex_mcp is refresh_codex_app_server_mcp:
        _require_current_codex_executable_plan_identity(
            plan.codex_app_server_executable,
            plan.codex_app_server_executable_identity,
        )
        if codex_host_preflight is preflight_codex_app_server_mcp:
            preflight_codex_app_server_mcp(
                Path(plan.codex_app_server_socket),
                desktop_launchagent=plan.codex_desktop_launchagent,
                expected_bundled_executable_identity=(plan.codex_app_server_executable_identity),
            )
        else:
            codex_host_preflight(Path(plan.codex_app_server_socket))
    checked = preflight(plan, runner, backup_database)
    if checked.migration_identity.schema_version != plan.target_identity.schema_version:
        raise RuntimeError("migration preflight did not reach the target schema")
    probe.require_preflight_ready(
        plan,
        target_projection_verified=checked.migration_projection_healthy,
    )

    # Re-read both exact plist identities after the disposable migration test
    # and immediately before stopping either service.
    _require_upgrade_plist(plan.control_plane)
    _require_upgrade_plist(plan.edge)
    controller = process_controller or ExactDashboardUpgradeProcessController()
    edge_process_group = controller.capture(runner, plan.edge)
    control_plane_process_group = controller.capture(runner, plan.control_plane)
    if edge_process_group.pgid == control_plane_process_group.pgid:
        raise RuntimeError("Dashboard services unexpectedly share one process group")
    _launchctl_effect(runner, "bootout", plan.edge)
    _launchctl_effect(runner, "bootout", plan.control_plane)
    _require_launchagent_unloaded(runner, plan.edge)
    _require_launchagent_unloaded(runner, plan.control_plane)
    controller.fence_after_bootout((edge_process_group, control_plane_process_group))
    probe.wait_for_stopped(plan)

    # This is the plan-bound rollback backup.  It is taken only after the old
    # Control Plane has stopped, so no committed write can race the snapshot.
    backup = backup_database(
        Path(plan.database_path),
        Path(plan.backup_destination),
        replace=False,
    )
    if (
        backup.path.resolve(strict=False) != Path(plan.backup_destination).resolve(strict=False)
        or backup.source_identity != checked.database_identity
        or backup.backup_identity != checked.database_identity
        or not _is_sha256(backup.sha256)
    ):
        raise RuntimeError("migration-free backup evidence does not match the upgrade plan")

    _launchctl_effect(runner, "bootstrap", plan.control_plane)
    probe.wait_for_control_plane(plan)
    _launchctl_effect(runner, "bootstrap", plan.edge)
    probe.wait_for_edge(plan)

    if reload_codex_mcp is refresh_codex_app_server_mcp:
        raw_restart_status = refresh_codex_app_server_mcp(
            Path(plan.codex_app_server_socket),
            plan.target_identity.release_id,
            desktop_launchagent=plan.codex_desktop_launchagent,
            expected_bundled_executable_identity=(plan.codex_app_server_executable_identity),
        )
    else:
        raw_restart_status = reload_codex_mcp(
            Path(plan.codex_app_server_socket),
            plan.target_identity.release_id,
        )
    restart_status = _normalized_codex_restart_status(raw_restart_status)
    return DashboardUpgradeResult(
        status="ready",
        plan_digest=plan.plan_digest,
        backup_path=str(backup.path),
        backup_sha256=backup.sha256,
        source_identity=backup.source_identity,
        backup_identity=backup.backup_identity,
        target_identity=plan.target_identity,
        codex_app_server_restart=restart_status,
        codex_mcp_reload=_CODEX_MCP_RELOAD_STATUS,
        codex_mcp_reload_scope=plan.codex_mcp_reload_scope,
        codex_mcp_reload_application=plan.codex_mcp_reload_application,
        current_conversation_verification=_CODEX_CURRENT_CONVERSATION_VERIFICATION,
    )


@dataclass(frozen=True, slots=True)
class EffectAuthorization:
    plan_digest: str
    action: str


def load_effect_authorization(path: Path) -> EffectAuthorization:
    raw = _read_owner_only_file(path)
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("effect authorization receipt must contain JSON") from error
    if not isinstance(value, Mapping) or set(value) != {"format", "action", "plan_digest"}:
        raise ValueError("effect authorization receipt shape is invalid")
    if (
        value.get("format") != _RECEIPT_FORMAT
        or not isinstance(value.get("action"), str)
        or not isinstance(value.get("plan_digest"), str)
    ):
        raise ValueError("effect authorization receipt is invalid")
    return EffectAuthorization(value["plan_digest"], value["action"])


def require_effect_authorization(
    plan: DashboardLifecyclePlan | DashboardUpgradePlan | CodexMCPRefreshPlan,
    action: str,
    *,
    receipt: EffectAuthorization | None = None,
    execute: bool = False,
    owner_confirmed: bool = False,
) -> None:
    if receipt is not None and receipt.action == action and receipt.plan_digest == plan.plan_digest:
        if _recomputed_plan_digest(plan) != plan.plan_digest:
            raise PermissionError(
                "effect receipt does not authorize modified lifecycle plan fields"
            )
        return
    if execute and owner_confirmed:
        return
    raise PermissionError(
        "lifecycle effect requires a matching receipt or --execute with --owner-confirm"
    )


def _ensure_launchagent_directories(plan: LaunchAgentPlan) -> None:
    path = Path(plan.plist_path)
    for directory in (path.parent.parent, path.parent, Path(plan.log_path).parent):
        canonical_launchagents = Path.home() / "Library" / "LaunchAgents"
        expected_mode = 0o755 if directory == canonical_launchagents else 0o700
        directory.mkdir(mode=expected_mode, parents=True, exist_ok=True)
        info = directory.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != expected_mode
        ):
            raise RuntimeError(
                "dashboard lifecycle directories have an unsafe owner or mode"
            )


def _write_all(descriptor: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        count = os.write(descriptor, payload[written:])
        if count <= 0:
            raise OSError("dashboard plist write made no progress")
        written += count


def _atomic_replace_plist(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o644)
    try:
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_canonical_plist(plan: LaunchAgentPlan) -> str:
    path = Path(plan.plist_path)
    state = _plist_state(plan)
    if state == "ready":
        return "ready"
    canonical = canonical_launchagent_plist(plan)
    if state == "content-conflict":
        previous = _previous_canonical_launchagent_plist(plan)
        try:
            actual = path.read_bytes()
        except OSError as error:
            raise RuntimeError(f"refusing to replace {plan.label}: unknown") from error
        if previous is None or actual != previous:
            raise RuntimeError(f"refusing to replace {plan.label}: {state}")
        _ensure_launchagent_directories(plan)
        _atomic_replace_plist(path, canonical)
        return "migrated"
    if state != "missing":
        raise RuntimeError(f"refusing to replace {plan.label}: {state}")
    _ensure_launchagent_directories(plan)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        _write_all(descriptor, canonical)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return "created"


def _checked_cloudflare_token_file(path: Path) -> None:
    token = _read_owner_only_file(path).strip()
    if (
        not 64 <= len(token) <= 16 * 1024
        or not token.isascii()
        or any(character.isspace() for character in token)
    ):
        raise ValueError("Cloudflare tunnel token file is invalid")


def _checked_cloudflared_binary(path: Path) -> None:
    info = path.lstat()
    mode = stat.S_IMODE(info.st_mode)
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid not in {0, os.getuid()}
        or mode & 0o022
        or not mode & stat.S_IXUSR
    ):
        raise ValueError("cloudflared binary has an unsafe identity or mode")


def _apply_launch_agents(
    agents: Sequence[LaunchAgentPlan],
    runner: CommandRunner,
) -> None:
    for agent in agents:
        plist_result = _write_canonical_plist(agent)
        loaded = runner.run(("launchctl", "print", f"gui/{os.getuid()}/{agent.label}"))
        if plist_result == "migrated" and loaded.returncode == 0:
            result = runner.run(
                ("launchctl", "bootout", f"gui/{os.getuid()}", agent.plist_path)
            )
            if result.returncode != 0:
                raise LifecycleUnknownOutcome(
                    f"launchctl bootout outcome is unknown for {agent.label}"
                )
        if loaded.returncode != 0 or plist_result == "migrated":
            result = runner.run(
                ("launchctl", "bootstrap", f"gui/{os.getuid()}", agent.plist_path)
            )
            if result.returncode != 0:
                raise LifecycleUnknownOutcome(
                    f"launchctl bootstrap outcome is unknown for {agent.label}"
                )


def apply_dashboard_lifecycle(
    plan: DashboardLifecyclePlan,
    runner: CommandRunner,
    *,
    receipt: EffectAuthorization | None = None,
    execute: bool = False,
    owner_confirmed: bool = False,
) -> None:
    require_effect_authorization(
        plan, "apply", receipt=receipt, execute=execute, owner_confirmed=owner_confirmed
    )
    if plan.tailscale is not None:
        version = runner.run(plan.tailscale.version_command)
        serve = runner.run(plan.tailscale.serve_status_command)
        serve_state, _ = _serve_state(plan.tailscale, serve)
        if (
            version.returncode != 0
            or not version.stdout.strip()
            or serve_state in {"unknown", "conflict", "conflict-control-plane-exposed"}
        ):
            raise LifecycleUnknownOutcome(
                "Tailnet state is unknown or conflicting; apply is blocked"
            )
        _apply_launch_agents((plan.control_plane, plan.edge), runner)
        if serve_state == "missing":
            result = runner.run(plan.tailscale.apply_command)
            if result.returncode != 0:
                raise LifecycleUnknownOutcome("tailscale serve outcome is unknown")
        return
    cloudflare = plan.cloudflare
    if cloudflare is None:  # pragma: no cover - constructor invariant
        raise ValueError("Dashboard exposure is unavailable")
    _checked_cloudflared_binary(Path(cloudflare.version_command[0]))
    _checked_cloudflare_token_file(Path(cloudflare.token_file))
    version = runner.run(cloudflare.version_command)
    if version.returncode != 0 or not version.stdout.strip():
        raise RuntimeError("cloudflared is unavailable; apply is blocked")
    _apply_launch_agents(
        (plan.control_plane, plan.edge, cloudflare.launch_agent),
        runner,
    )


def remove_dashboard_lifecycle(
    plan: DashboardLifecyclePlan,
    runner: CommandRunner,
    *,
    receipt: EffectAuthorization | None = None,
    execute: bool = False,
    owner_confirmed: bool = False,
) -> None:
    require_effect_authorization(
        plan, "remove", receipt=receipt, execute=execute, owner_confirmed=owner_confirmed
    )
    agents: tuple[LaunchAgentPlan, ...]
    if plan.tailscale is not None:
        serve = runner.run(plan.tailscale.serve_status_command)
        serve_state, _ = _serve_state(plan.tailscale, serve)
        if serve_state in {"unknown", "conflict", "conflict-control-plane-exposed"}:
            raise LifecycleUnknownOutcome(
                "Tailnet state is unknown or conflicting; removal is blocked"
            )
        if serve_state == "ready":
            result = runner.run(plan.tailscale.remove_command)
            if result.returncode != 0:
                raise LifecycleUnknownOutcome("tailscale serve removal outcome is unknown")
        agents = (plan.edge, plan.control_plane)
    else:
        cloudflare = plan.cloudflare
        if cloudflare is None:  # pragma: no cover - constructor invariant
            raise ValueError("Dashboard exposure is unavailable")
        agents = (cloudflare.launch_agent, plan.edge, plan.control_plane)
    for agent in agents:
        state = _plist_state(agent)
        if state not in {"missing", "ready"}:
            raise RuntimeError(f"refusing to remove {agent.label}: {state}")
        if state == "ready":
            result = runner.run(("launchctl", "bootout", f"gui/{os.getuid()}", agent.plist_path))
            if result.returncode != 0:
                raise LifecycleUnknownOutcome(
                    f"launchctl bootout outcome is unknown for {agent.label}"
                )
            Path(agent.plist_path).unlink()
