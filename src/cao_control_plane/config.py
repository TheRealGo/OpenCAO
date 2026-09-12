from __future__ import annotations

import ipaddress
import json
import os
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from .provider_models import model_identifier


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _list(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


ManagedWorkerAdapter = Literal["codex-app-server", "claude"]
ManagedWorkerReasoningEffort = Literal["low", "medium", "high", "xhigh", "max", "ultra"]
_MANAGED_WORKER_ADAPTERS = frozenset({"codex-app-server", "claude"})
_MANAGED_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})
_ADAPTER_REASONING_EFFORTS: dict[str, frozenset[str]] = {
    "codex-app-server": _MANAGED_REASONING_EFFORTS,
    "claude": frozenset({"low", "medium", "high", "xhigh", "max"}),
}
_MANAGED_WORKER_PROFILE_ID = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class ManagedWorkerProfile:
    """Runner policy and an omitted-value default, not a provider model catalog."""

    profile_id: str
    adapter: ManagedWorkerAdapter
    default_model: str
    reasoning_efforts: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _MANAGED_WORKER_PROFILE_ID.fullmatch(self.profile_id):
            raise ValueError("managed_worker_profiles contains an invalid profile id")
        if self.adapter not in _MANAGED_WORKER_ADAPTERS:
            raise ValueError("managed_worker_profiles contains an unsupported adapter")
        if model_identifier(self.default_model) is None:
            raise ValueError("managed_worker_profiles contains an invalid default model")
        _validate_profile_values(
            self.reasoning_efforts,
            field_name="reasoning_efforts",
            allowed=_MANAGED_REASONING_EFFORTS,
            matcher=None,
        )
        if not set(self.reasoning_efforts).issubset(_ADAPTER_REASONING_EFFORTS[self.adapter]):
            raise ValueError(
                "managed_worker_profiles adapter does not support one or more reasoning efforts"
            )


def _validate_profile_values(
    values: tuple[str, ...],
    *,
    field_name: str,
    allowed: frozenset[str] | None,
    matcher: re.Pattern[str] | None,
) -> None:
    if not isinstance(values, tuple):
        raise ValueError(f"managed_worker_profiles {field_name} must be an immutable tuple")
    if not values:
        raise ValueError(f"managed_worker_profiles {field_name} must not be empty")
    for value in values:
        if not isinstance(value, str) or not value:
            raise ValueError(f"managed_worker_profiles {field_name} must contain strings")
        if allowed is not None and value not in allowed:
            raise ValueError("managed_worker_profiles contains an unsupported reasoning effort")
        if matcher is not None and not matcher.fullmatch(value):
            raise ValueError("managed_worker_profiles contains an invalid model identifier")
    if len(set(values)) != len(values):
        raise ValueError(f"managed_worker_profiles {field_name} must not contain duplicates")


def _profile_values(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"managed_worker_profiles {field_name} must be an array")
    if not all(isinstance(item, str) for item in value):
        raise ValueError(f"managed_worker_profiles {field_name} must contain strings")
    return tuple(value)


def _parse_managed_worker_profiles(value: object) -> tuple[ManagedWorkerProfile, ...]:
    """Parse runner defaults, including the first entry of legacy model arrays."""

    if not isinstance(value, Mapping) or not value:
        raise ValueError("managed_worker_profiles must be a non-empty table or object")
    profiles: list[ManagedWorkerProfile] = []
    for profile_id, raw_profile in value.items():
        if not isinstance(profile_id, str) or not isinstance(raw_profile, Mapping):
            raise ValueError("managed_worker_profiles contains an invalid profile")
        common = {"adapter", "reasoning_efforts"}
        if set(raw_profile) not in (common | {"default_model"}, common | {"models"}):
            raise ValueError(
                "managed_worker_profiles require adapter, default_model, and reasoning_efforts"
            )
        adapter = raw_profile["adapter"]
        if not isinstance(adapter, str):
            raise ValueError("managed_worker_profiles contains an unsupported adapter")
        if "models" in raw_profile:
            legacy_models = _profile_values(raw_profile["models"], field_name="models")
            if not legacy_models:
                raise ValueError("managed_worker_profiles models must not be empty")
            default_model = legacy_models[0]
        else:
            default_model = raw_profile["default_model"]
        if not isinstance(default_model, str):
            raise ValueError("managed_worker_profiles default_model must be a string")
        profiles.append(
            ManagedWorkerProfile(
                profile_id=profile_id,
                adapter=adapter,  # type: ignore[arg-type]
                default_model=default_model,
                reasoning_efforts=_profile_values(
                    raw_profile["reasoning_efforts"], field_name="reasoning_efforts"
                ),
            )
        )
    return tuple(profiles)


def _json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("managed_worker_profiles must not contain duplicate keys")
        value[key] = item
    return value


DEFAULT_MANAGED_WORKER_PROFILES: tuple[ManagedWorkerProfile, ...] = (
    ManagedWorkerProfile(
        profile_id="codex",
        adapter="codex-app-server",
        default_model="gpt-5.6-terra",
        reasoning_efforts=("low", "medium", "high", "xhigh", "max", "ultra"),
    ),
    ManagedWorkerProfile(
        profile_id="claude",
        adapter="claude",
        default_model="opus",
        reasoning_efforts=("low", "medium", "high", "xhigh", "max"),
    ),
)


def _is_loopback_bind(host: str) -> bool:
    value = host.strip().lower().strip("[]").rstrip(".")
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _canonical_origin(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"invalid HTTP origin: {value!r}")
    if parsed.username or parsed.password or parsed.path not in {"", "/"}:
        raise ValueError(f"origin must not contain credentials or a path: {value!r}")
    if parsed.params or parsed.query or parsed.fragment:
        raise ValueError(f"origin must not contain params, query, or fragment: {value!r}")
    host = parsed.hostname.lower().rstrip(".")
    default_port = 80 if parsed.scheme == "http" else 443
    port = parsed.port or default_port
    rendered_host = f"[{host}]" if ":" in host else host
    suffix = "" if port == default_port else f":{port}"
    return f"{parsed.scheme}://{rendered_host}{suffix}"


@dataclass(frozen=True, slots=True)
class Settings:
    """Validated runtime configuration.

    The production default is deliberately local-only. Enabling a non-loopback
    bind is an explicit deployment decision and also requires a HTTPS public
    URL unless the operator separately opts into insecure development mode.
    """

    state_dir: Path = Path.home() / ".local" / "state" / "cao-a2a"
    host: str = "127.0.0.1"
    port: int = 8768
    public_base_url: str = "http://127.0.0.1:8768"
    trusted_hosts: tuple[str, ...] = ("127.0.0.1", "localhost", "[::1]")
    allowed_origins: tuple[str, ...] = (
        "http://127.0.0.1:8768",
        "http://localhost:8768",
    )
    allow_remote_bind: bool = False
    allow_insecure_remote_http: bool = False
    allow_remote_callbacks: bool = False
    enable_a2a: bool = True
    enable_a2a_push: bool = True
    # A2A-Version is optional in the HTTP binding; when supplied it is checked
    # strictly.  Requiring the optional header breaks otherwise conforming
    # clients, so the production default is false.
    require_a2a_version: bool = False
    enable_api_docs: bool = False
    enable_dashboard: bool = False
    # Dashboard access is an independent read-only projection.  Enabling its
    # probe never makes browser state part of CAO or Worker readiness.
    enable_dashboard_access_probe: bool = False
    dashboard_credentials_file: Path | None = None
    dashboard_bootstrap_record_dir: Path | None = None
    dashboard_session_record_dir: Path | None = None
    dashboard_edge_base_url: str = "http://127.0.0.1:8769"
    dashboard_access_timeout_seconds: float = 10.0
    max_request_bytes: int = 2 * 1024 * 1024
    max_runtime_output_bytes: int = 256 * 1024
    callback_timeout_seconds: float = 10.0
    # Startup is a short protocol boundary.  Once a managed Worker has
    # authenticated and begun its turn, liveness is governed by ordered MCP
    # heartbeats and durable progress rather than this wall-clock deadline.
    runtime_mcp_startup_timeout_seconds: float = 30.0
    worker_inactivity_timeout_seconds: float = 900.0
    # An operator may choose a bounded emergency ceiling for managed Worker
    # turns.  It is disabled by default and is deliberately server-owned;
    # Worker/task metadata cannot set or extend it.
    managed_worker_hard_timeout_seconds: float | None = None
    # Timeout for non-managed subprocesses and short attached CAO wake turns.
    # It is not a managed Worker work-duration limit.
    runtime_timeout_seconds: float = 120.0
    # Dispatcher wake-ups are commit-driven.  This is only the bounded
    # cross-process/expiry recovery deadline, not an idle polling interval.
    dispatcher_recovery_scan_seconds: float = 30.0
    dispatcher_lease_seconds: float = 30.0
    dispatcher_concurrency: int = 4
    max_dispatch_attempts: int = 5
    provider_rate_limit_cooldown_seconds: float = 300.0
    sse_heartbeat_seconds: float = 15.0
    a2a_blocking_timeout_seconds: float = 300.0
    mcp_private_cache_ttl_ms: int = 1_000
    mcp_discovery_cache_ttl_ms: int = 60_000
    # Runtime launch directories are private per-user control-plane state.
    runtime_launch_dir: Path = Path.home() / ".local" / "state" / "cao-a2a" / "runtime-launches"
    runtime_enrollment_ticket_ttl_seconds: int = 300
    runtime_credential_ttl_seconds: int = 3_600
    cao_conversation_credential_ttl_seconds: int = 86_400
    # A bootstrap capability only reaches the attachment create/renew endpoint.
    # It is deliberately distinct from the CAO principal/admin bearer.
    cao_attachment_bootstrap_credential_ttl_seconds: int = 86_400
    # Owner-private placement data lives in an explicitly supplied local file.
    # The path is optional for deployments without owner-private placement. An
    # enforced deployment sets require_owner_private_policy; a missing file is
    # then a fail-closed launch error rather than a fallback placement.
    owner_private_policy_file: Path | None = None
    require_owner_private_policy: bool = False
    # Historical unbound WorkItems remain durable queued records but never
    # dispatch to an arbitrary CAO session. Canonical deployments keep this
    # true to reject new unbound Work at delegation time.
    require_cao_attachment_for_work: bool = True
    # A TOML or environment override replaces these runner defaults and effort
    # policies. Explicit provider model selectors never require registration.
    managed_worker_profiles: tuple[ManagedWorkerProfile, ...] = DEFAULT_MANAGED_WORKER_PROFILES
    event_retention_days: int = 30
    message_retention_days: int = 90
    server_name: str = "OpenCAO Control Plane"
    server_version: str = "1.0.0"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    @property
    def database_path(self) -> Path:
        return self.state_dir / "control-plane.sqlite3"

    @property
    def token_export_path(self) -> Path:
        return self.state_dir / "bootstrap-tokens.json"

    @property
    def callback_key_path(self) -> Path:
        return self.state_dir / "callback-secrets.key"

    @property
    def owner_private_workspace_registry_path(self) -> Path:
        """Return the owner-only dynamic Directory registry locator."""

        return self.state_dir / "managed-worker-workspaces-v1.json"

    @property
    def owner_private_dynamic_policy_path(self) -> Path:
        """Return the owner-only default policy used by dynamic Directories."""

        return self.state_dir / "managed-worker-dynamic-policy-v1.json"

    @property
    def canonical_public_origin(self) -> str:
        return _canonical_origin(self.public_base_url)

    @property
    def local_only(self) -> bool:
        return _is_loopback_bind(self.host)

    def managed_worker_profile(self, profile_id: str) -> ManagedWorkerProfile | None:
        """Return the exact server-owned profile selected by an MCP request."""

        for profile in self.managed_worker_profiles:
            if profile.profile_id == profile_id:
                return profile
        return None

    def validate(self) -> None:
        if not (1 <= int(self.port) <= 65535):
            raise ValueError("port must be between 1 and 65535")
        if int(self.max_request_bytes) < 1024:
            raise ValueError("max_request_bytes must be at least 1024")
        if int(self.max_runtime_output_bytes) < 1024:
            raise ValueError("max_runtime_output_bytes must be at least 1024")
        if int(self.max_dispatch_attempts) < 1:
            raise ValueError("max_dispatch_attempts must be positive")
        if not 1.0 <= float(self.provider_rate_limit_cooldown_seconds) <= 86_400.0:
            raise ValueError("provider_rate_limit_cooldown_seconds must be between 1 and 86400")
        if int(self.dispatcher_concurrency) < 1:
            raise ValueError("dispatcher_concurrency must be positive")
        if float(self.dispatcher_recovery_scan_seconds) <= 0:
            raise ValueError("dispatcher_recovery_scan_seconds must be positive")
        if float(self.dispatcher_lease_seconds) <= 0:
            raise ValueError("dispatcher_lease_seconds must be positive")
        if float(self.runtime_timeout_seconds) <= 0:
            raise ValueError("runtime_timeout_seconds must be positive")
        if float(self.runtime_mcp_startup_timeout_seconds) <= 0:
            raise ValueError("runtime_mcp_startup_timeout_seconds must be positive")
        if float(self.worker_inactivity_timeout_seconds) <= 0:
            raise ValueError("worker_inactivity_timeout_seconds must be positive")
        if self.managed_worker_hard_timeout_seconds is not None and not (
            1.0 <= float(self.managed_worker_hard_timeout_seconds) <= 604_800.0
        ):
            raise ValueError("managed_worker_hard_timeout_seconds must be between 1 and 604800")
        if float(self.callback_timeout_seconds) <= 0:
            raise ValueError("callback_timeout_seconds must be positive")
        if float(self.dashboard_access_timeout_seconds) <= 0:
            raise ValueError("dashboard_access_timeout_seconds must be positive")
        dashboard_edge = urlparse(self.dashboard_edge_base_url)
        if (
            dashboard_edge.scheme != "http"
            or not dashboard_edge.hostname
            or not _is_loopback_bind(dashboard_edge.hostname)
            or dashboard_edge.username
            or dashboard_edge.password
            or dashboard_edge.path not in {"", "/"}
            or dashboard_edge.params
            or dashboard_edge.query
            or dashboard_edge.fragment
        ):
            raise ValueError("dashboard_edge_base_url must be a loopback HTTP origin")
        dashboard_paths = (
            self.dashboard_credentials_file,
            self.dashboard_bootstrap_record_dir,
            self.dashboard_session_record_dir,
        )
        for dashboard_path in dashboard_paths:
            if dashboard_path is not None and not dashboard_path.expanduser().is_absolute():
                raise ValueError("dashboard owner-private paths must be absolute")
        if self.enable_dashboard_access_probe:
            if not self.enable_dashboard:
                raise ValueError("Dashboard access probing needs the Control Plane Dashboard API")
            if self.dashboard_credentials_file is None:
                raise ValueError("Dashboard access probing needs an owner-private credentials file")
        if float(self.sse_heartbeat_seconds) <= 0:
            raise ValueError("sse_heartbeat_seconds must be positive")
        if float(self.a2a_blocking_timeout_seconds) <= 0:
            raise ValueError("a2a_blocking_timeout_seconds must be positive")
        if int(self.mcp_private_cache_ttl_ms) < 0:
            raise ValueError("mcp_private_cache_ttl_ms must be non-negative")
        if int(self.mcp_discovery_cache_ttl_ms) < 0:
            raise ValueError("mcp_discovery_cache_ttl_ms must be non-negative")
        if not self.runtime_launch_dir.expanduser().is_absolute():
            raise ValueError("runtime_launch_dir must be an absolute path")
        if int(self.runtime_enrollment_ticket_ttl_seconds) <= 0:
            raise ValueError("runtime_enrollment_ticket_ttl_seconds must be positive")
        if int(self.runtime_credential_ttl_seconds) <= 0:
            raise ValueError("runtime_credential_ttl_seconds must be positive")
        if int(self.cao_conversation_credential_ttl_seconds) <= 0:
            raise ValueError("cao_conversation_credential_ttl_seconds must be positive")
        if int(self.cao_attachment_bootstrap_credential_ttl_seconds) <= 0:
            raise ValueError("cao_attachment_bootstrap_credential_ttl_seconds must be positive")
        if self.require_owner_private_policy and self.owner_private_policy_file is None:
            raise ValueError("require_owner_private_policy requires owner_private_policy_file")
        if (
            self.owner_private_policy_file is not None
            and not self.owner_private_policy_file.expanduser().is_absolute()
        ):
            raise ValueError("owner_private_policy_file must be an absolute path")
        if not isinstance(self.managed_worker_profiles, tuple):
            raise ValueError("managed_worker_profiles must be an immutable tuple")
        if not self.managed_worker_profiles:
            raise ValueError("managed_worker_profiles must not be empty")
        if not all(
            isinstance(profile, ManagedWorkerProfile) for profile in self.managed_worker_profiles
        ):
            raise ValueError("managed_worker_profiles must contain typed profiles")
        profile_ids = [profile.profile_id for profile in self.managed_worker_profiles]
        if len(set(profile_ids)) != len(profile_ids):
            raise ValueError("managed_worker_profiles must not contain duplicate profile ids")
        if int(self.event_retention_days) < 1:
            raise ValueError("event_retention_days must be at least 1")
        if int(self.message_retention_days) < 1:
            raise ValueError("message_retention_days must be at least 1")

        parsed = urlparse(self.public_base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("public_base_url must be an absolute http(s) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("public_base_url must not contain credentials, query, or fragment")
        if parsed.path not in {"", "/"}:
            raise ValueError("public_base_url must not contain an application path")

        local_bind = _is_loopback_bind(self.host)
        if not local_bind and not self.allow_remote_bind:
            raise ValueError(
                "non-loopback host requires allow_remote_bind=true; local-only is the safe default"
            )
        if not local_bind and parsed.scheme != "https" and not self.allow_insecure_remote_http:
            raise ValueError(
                "a remote bind requires an https public_base_url unless "
                "allow_insecure_remote_http=true is explicitly set"
            )

        canonical_origins: set[str] = set()
        for origin in self.allowed_origins:
            if origin.strip().lower() == "null":
                raise ValueError("the opaque 'null' Origin is never trusted")
            canonical = _canonical_origin(origin)
            if canonical in canonical_origins:
                raise ValueError(f"duplicate allowed origin: {canonical}")
            canonical_origins.add(canonical)

        normalized_hosts = {value.strip().lower() for value in self.trusted_hosts if value.strip()}
        if not normalized_hosts:
            raise ValueError("trusted_hosts must not be empty")
        if "*" in normalized_hosts and not self.allow_remote_bind:
            raise ValueError("wildcard trusted_hosts requires allow_remote_bind=true")

    def ensure_directories(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.state_dir.chmod(0o700)
        launch_dir = self.runtime_launch_dir.expanduser()
        launch_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        launch_dir.chmod(0o700)
        for directory in (
            self.dashboard_bootstrap_record_dir,
            self.dashboard_session_record_dir,
        ):
            if directory is not None:
                resolved = directory.expanduser()
                resolved.mkdir(parents=True, exist_ok=True, mode=0o700)
                resolved.chmod(0o700)

    @classmethod
    def load(cls, path: Path | None = None) -> Settings:
        config_path = (
            path
            or Path(
                os.environ.get(
                    "CAO_A2A_CONFIG",
                    str(Path.home() / ".config" / "cao-a2a" / "config.toml"),
                )
            ).expanduser()
        )
        values: dict[str, Any] = {}
        if config_path.exists():
            with config_path.open("rb") as handle:
                raw = tomllib.load(handle)
            sections = (
                "server",
                "security",
                "protocols",
                "runtime",
                "dashboard",
                "retention",
            )
            unknown_sections = sorted(set(raw) - {*sections, "metadata"})
            if unknown_sections:
                raise ValueError(
                    f"unsupported configuration sections: {', '.join(unknown_sections)}"
                )
            for section in sections:
                section_value = raw.get(section, {})
                if not isinstance(section_value, dict):
                    raise ValueError(f"configuration section {section} must be a table")
                values.update(section_value)
            metadata = raw.get("metadata", {})
            if not isinstance(metadata, dict):
                raise ValueError("configuration section metadata must be a table")
            if metadata:
                values["metadata"] = metadata
            supported_fields = {item.name for item in fields(cls)}
            unknown_keys = sorted(set(values) - supported_fields)
            if unknown_keys:
                raise ValueError(f"unsupported configuration keys: {', '.join(unknown_keys)}")

        removed_environment_keys = {
            "CAO_A2A_ENABLE_MCP_2026",
            "CAO_A2A_ENABLE_MCP_2025_COMPAT",
            "CAO_A2A_ENABLE_LEGACY_MCP",
            "CAO_A2A_MANAGED_WORKER_CATALOG_FILE",
            "CAO_A2A_REQUIRE_DASHBOARD_VISIBILITY_FOR_SUPERVISION",
            "CAO_A2A_DASHBOARD_PRESENTER",
            "CAO_A2A_DASHBOARD_VISIBILITY_TIMEOUT_SECONDS",
            "CAO_A2A_A2A_BLOCKING_TIMEOUT_SECONDS",
        }
        configured_removed_keys = sorted(removed_environment_keys.intersection(os.environ))
        if configured_removed_keys:
            raise ValueError(
                "removed environment settings are not supported: "
                + ", ".join(configured_removed_keys)
            )

        env_map: dict[str, tuple[str, Any]] = {
            "CAO_A2A_STATE_DIR": ("state_dir", Path),
            "CAO_A2A_HOST": ("host", str),
            "CAO_A2A_PORT": ("port", int),
            "CAO_A2A_PUBLIC_BASE_URL": ("public_base_url", str),
            "CAO_A2A_TRUSTED_HOSTS": ("trusted_hosts", _list),
            "CAO_A2A_ALLOWED_ORIGINS": ("allowed_origins", _list),
            "CAO_A2A_ALLOW_REMOTE_BIND": ("allow_remote_bind", _bool),
            "CAO_A2A_ALLOW_INSECURE_REMOTE_HTTP": ("allow_insecure_remote_http", _bool),
            "CAO_A2A_ALLOW_REMOTE_CALLBACKS": ("allow_remote_callbacks", _bool),
            "CAO_A2A_ENABLE_A2A": ("enable_a2a", _bool),
            "CAO_A2A_ENABLE_A2A_PUSH": ("enable_a2a_push", _bool),
            "CAO_A2A_REQUIRE_A2A_VERSION": ("require_a2a_version", _bool),
            "CAO_A2A_ENABLE_API_DOCS": ("enable_api_docs", _bool),
            "CAO_A2A_ENABLE_DASHBOARD": ("enable_dashboard", _bool),
            "CAO_A2A_ENABLE_DASHBOARD_ACCESS_PROBE": (
                "enable_dashboard_access_probe",
                _bool,
            ),
            "CAO_A2A_DASHBOARD_CREDENTIALS_FILE": (
                "dashboard_credentials_file",
                Path,
            ),
            "CAO_A2A_DASHBOARD_BOOTSTRAP_RECORD_DIR": (
                "dashboard_bootstrap_record_dir",
                Path,
            ),
            "CAO_A2A_DASHBOARD_SESSION_RECORD_DIR": (
                "dashboard_session_record_dir",
                Path,
            ),
            "CAO_A2A_DASHBOARD_EDGE_BASE_URL": ("dashboard_edge_base_url", str),
            "CAO_A2A_DASHBOARD_ACCESS_TIMEOUT_SECONDS": (
                "dashboard_access_timeout_seconds",
                float,
            ),
            "CAO_A2A_MAX_REQUEST_BYTES": ("max_request_bytes", int),
            "CAO_A2A_CALLBACK_TIMEOUT_SECONDS": ("callback_timeout_seconds", float),
            "CAO_A2A_RUNTIME_MCP_STARTUP_TIMEOUT_SECONDS": (
                "runtime_mcp_startup_timeout_seconds",
                float,
            ),
            "CAO_A2A_WORKER_INACTIVITY_TIMEOUT_SECONDS": (
                "worker_inactivity_timeout_seconds",
                float,
            ),
            "CAO_A2A_MANAGED_WORKER_HARD_TIMEOUT_SECONDS": (
                "managed_worker_hard_timeout_seconds",
                float,
            ),
            "CAO_A2A_RUNTIME_TIMEOUT_SECONDS": ("runtime_timeout_seconds", float),
            "CAO_A2A_DISPATCHER_RECOVERY_SCAN_SECONDS": (
                "dispatcher_recovery_scan_seconds",
                float,
            ),
            "CAO_A2A_DISPATCHER_LEASE_SECONDS": ("dispatcher_lease_seconds", float),
            "CAO_A2A_DISPATCHER_CONCURRENCY": ("dispatcher_concurrency", int),
            "CAO_A2A_MAX_DISPATCH_ATTEMPTS": ("max_dispatch_attempts", int),
            "CAO_A2A_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS": (
                "provider_rate_limit_cooldown_seconds",
                float,
            ),
            "CAO_A2A_MAX_RUNTIME_OUTPUT_BYTES": ("max_runtime_output_bytes", int),
            "CAO_A2A_SSE_HEARTBEAT_SECONDS": ("sse_heartbeat_seconds", float),
            "CAO_A2A_BLOCKING_TIMEOUT_SECONDS": (
                "a2a_blocking_timeout_seconds",
                float,
            ),
            "CAO_A2A_MCP_PRIVATE_CACHE_TTL_MS": ("mcp_private_cache_ttl_ms", int),
            "CAO_A2A_MCP_DISCOVERY_CACHE_TTL_MS": ("mcp_discovery_cache_ttl_ms", int),
            "CAO_A2A_RUNTIME_LAUNCH_DIR": ("runtime_launch_dir", Path),
            "CAO_A2A_RUNTIME_ENROLLMENT_TICKET_TTL_SECONDS": (
                "runtime_enrollment_ticket_ttl_seconds",
                int,
            ),
            "CAO_A2A_RUNTIME_CREDENTIAL_TTL_SECONDS": ("runtime_credential_ttl_seconds", int),
            "CAO_A2A_CAO_CONVERSATION_CREDENTIAL_TTL_SECONDS": (
                "cao_conversation_credential_ttl_seconds",
                int,
            ),
            "CAO_A2A_CAO_ATTACHMENT_BOOTSTRAP_CREDENTIAL_TTL_SECONDS": (
                "cao_attachment_bootstrap_credential_ttl_seconds",
                int,
            ),
            "CAO_A2A_OWNER_PRIVATE_POLICY_FILE": ("owner_private_policy_file", Path),
            "CAO_A2A_REQUIRE_OWNER_PRIVATE_POLICY": (
                "require_owner_private_policy",
                _bool,
            ),
            "CAO_A2A_REQUIRE_CAO_ATTACHMENT_FOR_WORK": (
                "require_cao_attachment_for_work",
                _bool,
            ),
            "CAO_A2A_EVENT_RETENTION_DAYS": ("event_retention_days", int),
            "CAO_A2A_MESSAGE_RETENTION_DAYS": ("message_retention_days", int),
        }
        for env_name, (field_name, converter) in env_map.items():
            if env_name in os.environ:
                values[field_name] = converter(os.environ[env_name])

        if "CAO_A2A_MANAGED_WORKER_PROFILES" in os.environ:
            try:
                catalog_value = json.loads(
                    os.environ["CAO_A2A_MANAGED_WORKER_PROFILES"],
                    object_pairs_hook=_json_object_without_duplicates,
                )
            except (json.JSONDecodeError, ValueError) as error:
                raise ValueError("CAO_A2A_MANAGED_WORKER_PROFILES must be valid JSON") from error
            values["managed_worker_profiles"] = _parse_managed_worker_profiles(catalog_value)
        elif "managed_worker_profiles" in values:
            values["managed_worker_profiles"] = _parse_managed_worker_profiles(
                values["managed_worker_profiles"]
            )

        if "state_dir" in values:
            values["state_dir"] = Path(values["state_dir"]).expanduser()
        if "runtime_launch_dir" in values:
            values["runtime_launch_dir"] = Path(values["runtime_launch_dir"]).expanduser()
        if "owner_private_policy_file" in values:
            values["owner_private_policy_file"] = Path(
                values["owner_private_policy_file"]
            ).expanduser()
        for path_name in (
            "dashboard_credentials_file",
            "dashboard_bootstrap_record_dir",
            "dashboard_session_record_dir",
        ):
            if path_name in values:
                values[path_name] = Path(values[path_name]).expanduser()
        for list_name in ("trusted_hosts", "allowed_origins"):
            if list_name in values and isinstance(values[list_name], list):
                values[list_name] = tuple(str(value) for value in values[list_name])

        settings = replace(cls(), **values)
        settings.ensure_directories()
        return settings
