from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import plistlib
import signal
import socket
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from cao_control_plane.canonical import canonical_sha256
from cao_control_plane.config import Settings
from cao_control_plane.dashboard_cli import main
from cao_control_plane.dashboard_lifecycle import (
    CodexAppServerPeerObservation,
    CodexDesktopLaunchAgentBinding,
    CodexMCPRefreshPhaseError,
    CodexMCPRefreshPhaseUnknown,
    CodexMCPRefreshResult,
    CommandResult,
    DarwinAuditProcessIdentity,
    DarwinDashboardProcessAPI,
    DashboardLifecycleSettings,
    DashboardUpgradePreflightResult,
    DashboardUpgradeProcessGroup,
    EffectAuthorization,
    ExactDashboardUpgradeProcessController,
    HttpResult,
    HttpxDashboardUpgradeProbe,
    LifecycleUnknownOutcome,
    UpgradeLaunchAgentBinding,
    _lifecycle_process_argv,
    _previous_canonical_launchagent_plist,
    apply_dashboard_lifecycle,
    build_codex_mcp_refresh_plan,
    build_dashboard_lifecycle_plan,
    build_dashboard_upgrade_plan,
    canonical_codex_app_server_executable,
    canonical_codex_app_server_socket,
    canonical_launchagent_plist,
    dashboard_lifecycle_status,
    discover_codex_desktop_launchagent_binding,
    load_effect_authorization,
    preflight_codex_app_server_mcp,
    preflight_dashboard_upgrade,
    refresh_codex_app_server_mcp,
    refresh_codex_mcp,
    reload_codex_mcp_server,
    remove_dashboard_lifecycle,
    upgrade_dashboard_lifecycle,
    verify_dashboard_upgrade_migration,
)
from cao_control_plane.database import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    Database,
    SQLiteBackupResult,
    SQLiteDatabaseIdentity,
    backup_sqlite_database,
    inspect_sqlite_database,
)
from cao_control_plane.release_identity import ReleaseIdentity


class FakeRunner:
    def __init__(
        self,
        responses: Mapping[tuple[str, ...], CommandResult],
        *,
        trace: list[str] | None = None,
    ) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, ...]] = []
        self.trace = trace

    def run(self, argv: Sequence[str]) -> CommandResult:
        key = tuple(argv)
        self.calls.append(key)
        if self.trace is not None:
            self.trace.append("command:" + " ".join(key[1:2] + key[3:]))
        return self.responses.get(key, CommandResult(key, 1, stderr="unconfigured fake command"))


def test_lifecycle_process_argv_is_owned_by_lifecycle_not_attachment_issuer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: Sequence[str], **kwargs: object) -> CommandResult:
        calls.append(tuple(argv))
        assert kwargs["timeout"] == 1.0
        return CommandResult(
            tuple(argv),
            0,
            '/usr/bin/python3 -m cao_control_plane.cli mcp-stdio "bounded arg"\n',
        )

    monkeypatch.setattr("cao_control_plane.dashboard_lifecycle.subprocess.run", run)

    assert _lifecycle_process_argv(4321) == (
        "/usr/bin/python3",
        "-m",
        "cao_control_plane.cli",
        "mcp-stdio",
        "bounded arg",
    )
    assert calls == [("/bin/ps", "-ww", "-p", "4321", "-o", "command=")]

    import cao_control_plane.attachment_issuer as attachment_issuer

    assert not hasattr(attachment_issuer, "_darwin_process_argv")


class FakeProbe:
    def __init__(self, responses: Mapping[str, HttpResult]) -> None:
        self.responses = dict(responses)
        self.calls: list[str] = []

    def get(self, url: str, headers: Mapping[str, str]) -> HttpResult:
        self.calls.append(url)
        return self.responses[url]


class FakeUpgradeProbe:
    def __init__(self, trace: list[str], *, fail_control_plane: bool = False) -> None:
        self.trace = trace
        self.fail_control_plane = fail_control_plane

    def require_preflight_ready(self, plan: object, *, target_projection_verified: bool) -> None:
        del plan
        assert target_projection_verified is True
        self.trace.append("probe:preflight")

    def wait_for_stopped(self, plan: object) -> None:
        del plan
        self.trace.append("probe:stopped")

    def wait_for_control_plane(self, plan: object) -> None:
        del plan
        self.trace.append("probe:control-plane")
        if self.fail_control_plane:
            raise LifecycleUnknownOutcome("Control Plane did not become ready")

    def wait_for_edge(self, plan: object) -> None:
        del plan
        self.trace.append("probe:edge")


def _audit_process(
    pid: int,
    *,
    pgid: int | None = None,
    asid: int = 10,
) -> DarwinAuditProcessIdentity:
    group = pid if pgid is None else pgid
    return DarwinAuditProcessIdentity(
        pid=pid,
        pgid=group,
        uid=os.geteuid(),
        asid=asid,
        audit_token=(0, os.geteuid(), 0, os.getuid(), 0, pid, asid, pid),
    )


class FakeUpgradeProcessController:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def capture(self, runner, binding) -> DashboardUpgradeProcessGroup:
        del runner
        self.trace.append(f"process:capture:{binding.label}")
        pid = 1000 + len(self.trace)
        return DashboardUpgradeProcessGroup(
            pgid=pid,
            members=(_audit_process(pid),),
        )

    def fence_after_bootout(self, identities) -> None:
        assert len(identities) == 2
        self.trace.append("process:fenced")


def _credentials(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "upstream_base_url": "http://127.0.0.1:8768",
                "dashboard_bearer": "dashboard-bearer-credential-value",
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _settings(tmp_path: Path) -> DashboardLifecycleSettings:
    credentials = _credentials(tmp_path / "credentials.json")
    records = tmp_path / "bootstrap"
    records.mkdir(mode=0o700, exist_ok=True)
    records.chmod(0o700)
    sessions = tmp_path / "sessions"
    sessions.mkdir(mode=0o700, exist_ok=True)
    sessions.chmod(0o700)
    return DashboardLifecycleSettings(
        application_support_dir=tmp_path / "Application Support" / "CAO" / "dashboard",
        working_directory=tmp_path,
        control_plane_command=("cao-a2a", "--config", str(tmp_path / "control-plane.toml")),
        credentials_file=credentials,
        bootstrap_record_dir=records,
        session_record_dir=sessions,
    )


def _cloudflare_settings(tmp_path: Path) -> DashboardLifecycleSettings:
    base = _settings(tmp_path)
    token = tmp_path / "cloudflare-tunnel-token"
    token.write_text("t" * 128, encoding="utf-8")
    token.chmod(0o600)
    binary = tmp_path / "cloudflared"
    binary.write_text("#!/bin/sh\n", encoding="utf-8")
    binary.chmod(0o755)
    return replace(
        base,
        exposure_provider="cloudflare",
        cloudflared_binary=str(binary),
        cloudflare_token_file=token,
        cloudflare_hostname="dashboard.example.test",
        cloudflare_access_team_domain="owner-team.cloudflareaccess.com",
    )


def _database_identity(version: int = 28) -> SQLiteDatabaseIdentity:
    return SQLiteDatabaseIdentity(
        application_id=APPLICATION_ID,
        user_version=version,
        schema_version=version,
        integrity=("ok",),
        foreign_key_error_count=0,
    )


def _release_identity(version: int = SCHEMA_VERSION) -> ReleaseIdentity:
    return ReleaseIdentity(
        release_id="a" * 64,
        schema_version=version,
        mcp_catalog_digest="b" * 64,
    )


def _redigest(plan):
    payload = asdict(plan)
    payload.pop("plan_digest")
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return replace(plan, plan_digest=digest)


def _upgrade_plan(
    tmp_path: Path,
    *,
    codex_socket: Path | None = None,
):
    settings = _settings(tmp_path)
    control_plane_config = tmp_path / "control-plane.toml"
    control_plane_config.write_text(
        f"[server]\nstate_dir = {json.dumps(str(tmp_path))}\n",
        encoding="utf-8",
    )
    control_plane_config.chmod(0o600)
    lifecycle = build_dashboard_lifecycle_plan(settings)
    launchagents = tmp_path / "Library" / "LaunchAgents"
    launchagents.mkdir(parents=True)
    control_plane_path = launchagents / "owner-control-plane.plist"
    edge_path = launchagents / "owner-edge.plist"
    control_plane_payload = canonical_launchagent_plist(lifecycle.control_plane)
    edge_payload = canonical_launchagent_plist(lifecycle.edge)
    control_plane_path.write_bytes(control_plane_payload)
    edge_path.write_bytes(edge_payload)
    control_plane_path.chmod(0o644)
    edge_path.chmod(0o644)
    if codex_socket is None:
        codex_socket = _detached_owner_unix_socket()
    plan = build_dashboard_upgrade_plan(
        lifecycle,
        database_path=tmp_path / "control-plane.sqlite3",
        backup_destination=tmp_path / "backups" / "pre-upgrade.sqlite3",
        credentials_file=settings.credentials_file,
        source_identity=_database_identity(),
        control_plane_plist_path=control_plane_path,
        control_plane_plist_sha256=hashlib.sha256(control_plane_payload).hexdigest(),
        edge_plist_path=edge_path,
        edge_plist_sha256=hashlib.sha256(edge_payload).hexdigest(),
        codex_app_server_socket=codex_socket,
        target_identity=_release_identity(),
        readiness_attempts=2,
        readiness_interval_seconds=0,
    )
    return settings, lifecycle, plan


_SOCKET_DIRECTORY_LEASES: list[tempfile.TemporaryDirectory[str]] = []


def _detached_owner_unix_socket() -> Path:
    """Create a short-lived test socket node that remains valid for lstat checks."""

    temporary_root = Path(tempfile.gettempdir()).resolve()
    lease = tempfile.TemporaryDirectory(prefix="cao-sock-node-", dir=temporary_root)
    _SOCKET_DIRECTORY_LEASES.append(lease)
    socket_dir = Path(lease.name)
    socket_dir.chmod(0o700)
    socket_path = socket_dir / "app.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    socket_path.chmod(0o600)
    server.close()
    return socket_path


@contextmanager
def _owner_unix_socket():
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(
        prefix="cao-sock-",
        dir=temporary_root,
    ) as directory:
        socket_dir = Path(directory)
        socket_dir.chmod(0o700)
        socket_path = socket_dir / "app.sock"
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(socket_path))
        socket_path.chmod(0o600)
        try:
            yield socket_path
        finally:
            server.close()


def _test_codex_app_server_executable(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    executable = root / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    return executable


def _codex_desktop_binding(
    root: Path,
    executable: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    optional_plist_values: Mapping[str, object] | None = None,
) -> CodexDesktopLaunchAgentBinding:
    launchagents = root / "Library" / "LaunchAgents"
    launchagents.mkdir(parents=True)
    launchagents.chmod(0o755)
    label = "dev.example.codex-app-server"
    arguments = (
        os.fspath(executable),
        "-c",
        "features.code_mode_host=true",
        "app-server",
        "--listen",
        "unix://",
        "--analytics-default-enabled",
    )
    log_root = root / "Library" / "Application Support" / "TestCodex"
    log_root.mkdir(parents=True)
    log_root.chmod(0o700)
    for log_name in ("app-server.log", "app-server-error.log"):
        log_path = log_root / log_name
        log_path.write_text("", encoding="utf-8")
        log_path.chmod(0o644)
    path = launchagents / f"{label}.plist"
    payload: dict[str, object] = {
        "Label": label,
        "ProgramArguments": list(arguments),
        "KeepAlive": True,
        "RunAtLoad": True,
        "ProcessType": "Interactive",
        "ThrottleInterval": 10,
        "StandardOutPath": os.fspath(log_root / "app-server.log"),
        "StandardErrorPath": os.fspath(log_root / "app-server-error.log"),
    }
    payload.update(optional_plist_values or {})
    path.write_bytes(
        plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)
    )
    path.chmod(0o644)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle._canonical_owner_launchagent_dir",
        lambda: launchagents,
    )
    binding = discover_codex_desktop_launchagent_binding()
    assert binding is not None
    return binding


def test_codex_desktop_discovery_binds_bounded_environment_and_resource_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _test_codex_app_server_executable(tmp_path, monkeypatch)
    binding = _codex_desktop_binding(
        tmp_path,
        executable,
        monkeypatch,
        optional_plist_values={
            "EnvironmentVariables": {"PATH": "/usr/local/bin:/usr/bin:/bin"},
            "HardResourceLimits": {"NumberOfFiles": 65_536},
            "SoftResourceLimits": {"NumberOfFiles": 65_536},
        },
    )

    assert binding.environment_sha256 != canonical_sha256({})


@pytest.mark.parametrize(
    "unsafe_values",
    [
        {"EnvironmentVariables": {"PATH": "/usr/bin\n/tmp"}},
        {
            "HardResourceLimits": {"NumberOfFiles": 1_024},
            "SoftResourceLimits": {"NumberOfFiles": 2_048},
        },
        {"LimitLoadToSessionType": "Aqua"},
    ],
)
def test_codex_desktop_discovery_rejects_unbounded_launch_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_values: Mapping[str, object],
) -> None:
    executable = _test_codex_app_server_executable(tmp_path, monkeypatch)
    binding = _codex_desktop_binding(tmp_path, executable, monkeypatch)
    path = Path(binding.plist_path)
    payload = plistlib.loads(path.read_bytes())
    payload.update(unsafe_values)
    path.write_bytes(plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True))

    assert discover_codex_desktop_launchagent_binding() is None


def _codex_launchctl_output(
    binding: CodexDesktopLaunchAgentBinding,
    *,
    pid: int,
    asid: int,
) -> str:
    arguments = "\n".join(f"\t\t{argument}" for argument in binding.program_arguments)
    return (
        f"gui/{os.getuid()}/{binding.label} = {{\n"
        f"\tpath = {binding.plist_path}\n"
        "\ttype = LaunchAgent\n"
        "\tstate = running\n"
        f"\tprogram = {binding.program_arguments[0]}\n"
        "\targuments = {\n"
        f"{arguments}\n"
        "\t}\n"
        f"\tasid = {asid}\n"
        f"\tpid = {pid}\n"
        "}\n"
    )


def _codex_lsof_output(pid: int, executable: Path) -> str:
    info = executable.stat()
    return f"p{pid}\nftxt\nD{hex(info.st_dev)}\ni{info.st_ino}\nn{executable}\n"


class _HealthyCodexHostInspector:
    def __init__(
        self,
        binding: CodexDesktopLaunchAgentBinding,
        *,
        pid: int,
        asid: int,
    ) -> None:
        self.peer = CodexAppServerPeerObservation(
            pid=pid,
            uid=os.geteuid(),
            start_signature=f"start-{pid}",
            executable_path=None,
            argv=binding.program_arguments,
        )
        self.process = DarwinAuditProcessIdentity(
            pid=pid,
            pgid=pid,
            uid=os.geteuid(),
            asid=asid,
            audit_token=(0, os.geteuid(), 0, os.getuid(), 0, pid, asid, pid + 10),
        )
        self.executable = Path(binding.program_arguments[0])

    def inspect(self, socket_path: Path) -> CodexAppServerPeerObservation:
        del socket_path
        return self.peer

    def capture_process(
        self,
        pid: int,
        *,
        expected_asid: int,
    ) -> DarwinAuditProcessIdentity:
        assert pid == self.process.pid
        assert expected_asid == self.process.asid
        return self.process

    def executable_path(self, process: DarwinAuditProcessIdentity) -> Path:
        assert process == self.process
        return self.executable

    def capture_cao_bridges(
        self,
        root_pid: int,
        *,
        expected_asid: int,
    ) -> tuple[DarwinAuditProcessIdentity, ...]:
        assert root_pid == self.process.pid
        assert expected_asid == self.process.asid
        return ()

    def is_live(self, process: DarwinAuditProcessIdentity) -> bool:
        return process == self.process


def _serve_payload(origin: str) -> str:
    return json.dumps({"Web": {"phone.tailnet.ts.net:443": {"Handlers": {"/": {"Proxy": origin}}}}})


def _loaded_launchctl_output(binding: UpgradeLaunchAgentBinding) -> str:
    plist = plistlib.loads(Path(binding.plist_path).read_bytes())
    arguments = plist["ProgramArguments"]
    rendered_arguments = "\n".join(f"\t\t{argument}" for argument in arguments)
    return (
        f"gui/{os.getuid()}/{binding.label} = {{\n"
        f"\tpath = {binding.plist_path}\n"
        "\tstate = running\n"
        "\targuments = {\n"
        f"{rendered_arguments}\n"
        "\t}\n"
        f"\tworking directory = {binding.working_directory}\n"
        "\tasid = 100023\n"
        "\tpid = 4321\n"
        "\tresource coalition = {\n"
        "\t\tstate = active\n"
        "\t}\n"
        "}\n"
    )


def _runner_for(plan: object, *, serve: str, loaded: bool = False) -> FakeRunner:
    # Keeping the fake exact makes command syntax and retry behavior executable.
    lifecycle = plan
    assert hasattr(lifecycle, "tailscale") and hasattr(lifecycle, "control_plane")
    tailscale = lifecycle.tailscale
    control_plane = lifecycle.control_plane
    edge = lifecycle.edge
    responses: dict[tuple[str, ...], CommandResult] = {
        tailscale.version_command: CommandResult(tailscale.version_command, 0, "1.80.0\n"),
        tailscale.serve_status_command: CommandResult(tailscale.serve_status_command, 0, serve),
        tailscale.tailnet_status_command: CommandResult(
            tailscale.tailnet_status_command,
            0,
            json.dumps({"Self": {"DNSName": "phone.tailnet.ts.net."}}),
        ),
        tailscale.apply_command: CommandResult(tailscale.apply_command, 0),
        tailscale.remove_command: CommandResult(tailscale.remove_command, 0),
    }
    for item in (control_plane, edge):
        print_command = ("launchctl", "print", f"gui/{os.getuid()}/{item.label}")
        responses[print_command] = CommandResult(print_command, 0 if loaded else 1)
        bootstrap = ("launchctl", "bootstrap", f"gui/{os.getuid()}", item.plist_path)
        responses[bootstrap] = CommandResult(bootstrap, 0)
        bootout = ("launchctl", "bootout", f"gui/{os.getuid()}", item.plist_path)
        responses[bootout] = CommandResult(bootout, 0)
    return FakeRunner(responses)


def _cloudflare_runner(plan: object, *, loaded: bool) -> FakeRunner:
    lifecycle = plan
    assert lifecycle.cloudflare is not None
    cloudflare = lifecycle.cloudflare
    responses: dict[tuple[str, ...], CommandResult] = {
        cloudflare.version_command: CommandResult(
            cloudflare.version_command,
            0,
            "cloudflared version 2026.8.0\n",
        ),
        cloudflare.ready_command: CommandResult(cloudflare.ready_command, 0),
    }
    for item in (lifecycle.control_plane, lifecycle.edge, cloudflare.launch_agent):
        print_command = ("launchctl", "print", f"gui/{os.getuid()}/{item.label}")
        responses[print_command] = CommandResult(print_command, 0 if loaded else 1)
        bootstrap = ("launchctl", "bootstrap", f"gui/{os.getuid()}", item.plist_path)
        responses[bootstrap] = CommandResult(bootstrap, 0)
        bootout = ("launchctl", "bootout", f"gui/{os.getuid()}", item.plist_path)
        responses[bootout] = CommandResult(bootout, 0)
    return FakeRunner(responses)


def test_plan_is_deterministic_loopback_only_and_never_persists_bearer(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    again = build_dashboard_lifecycle_plan(settings)
    assert plan.plan_digest == again.plan_digest
    assert plan.tailscale.apply_command == (
        "tailscale",
        "serve",
        "--https=443",
        "http://127.0.0.1:8769",
    )
    assert plan.tailscale.prohibited_control_plane_origin == "http://127.0.0.1:8768"
    rendered = json.dumps(plan.to_dict())
    assert "dashboard-bearer-credential-value" not in rendered
    assert "--token" not in rendered
    assert "credentials.json" in rendered
    assert "--session-record-dir" in plan.edge.program_arguments
    assert str(settings.session_record_dir) in plan.edge.program_arguments
    assert "127.0.0.1" in rendered
    for agent in (plan.control_plane, plan.edge):
        assert plistlib.loads(canonical_launchagent_plist(agent))["KeepAlive"] is True


def test_apply_is_authorized_idempotent_and_only_serves_the_edge(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    runner = _runner_for(plan, serve="{}")
    apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)
    for item in (plan.control_plane, plan.edge):
        path = Path(item.plist_path)
        assert path.read_bytes() == canonical_launchagent_plist(item)
        assert path.stat().st_mode & 0o777 == 0o644
        assert "dashboard-bearer-credential-value" not in path.read_text(encoding="utf-8")
    assert plan.tailscale.apply_command in runner.calls

    second = _runner_for(
        plan, serve=_serve_payload(plan.tailscale.expected_edge_origin), loaded=True
    )
    apply_dashboard_lifecycle(plan, second, execute=True, owner_confirmed=True)
    assert plan.tailscale.apply_command not in second.calls
    assert not any(command[1:3] == ("bootstrap", f"gui/{os.getuid()}") for command in second.calls)


def test_conflicting_or_unknown_serve_is_never_replaced_or_retried(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    conflict = _runner_for(
        plan, serve=_serve_payload(plan.tailscale.prohibited_control_plane_origin)
    )
    with pytest.raises(LifecycleUnknownOutcome, match="unknown or conflicting"):
        apply_dashboard_lifecycle(plan, conflict, execute=True, owner_confirmed=True)
    assert plan.tailscale.apply_command not in conflict.calls

    unknown = _runner_for(plan, serve="not-json")
    with pytest.raises(LifecycleUnknownOutcome, match="unknown or conflicting"):
        apply_dashboard_lifecycle(plan, unknown, execute=True, owner_confirmed=True)
    assert plan.tailscale.apply_command not in unknown.calls


def test_existing_plist_identity_or_content_conflict_is_not_overwritten(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    path = Path(plan.control_plane.plist_path)
    path.parent.mkdir(parents=True)
    path.write_text("foreign", encoding="utf-8")
    path.chmod(0o644)
    runner = _runner_for(plan, serve="{}")
    with pytest.raises(RuntimeError, match="content-conflict"):
        apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)
    assert path.read_text(encoding="utf-8") == "foreign"


def test_exact_previous_plists_are_atomically_migrated_and_reloaded(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    for agent in (plan.control_plane, plan.edge):
        path = Path(agent.plist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.parent.chmod(0o700)
        path.parent.chmod(0o700)
        previous = _previous_canonical_launchagent_plist(agent)
        assert previous is not None
        path.write_bytes(previous)
        path.chmod(0o644)

    runner = _runner_for(
        plan,
        serve=_serve_payload(plan.tailscale.expected_edge_origin),
        loaded=True,
    )
    apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)

    for agent in (plan.control_plane, plan.edge):
        assert Path(agent.plist_path).read_bytes() == canonical_launchagent_plist(agent)
        bootout = ("launchctl", "bootout", f"gui/{os.getuid()}", agent.plist_path)
        bootstrap = ("launchctl", "bootstrap", f"gui/{os.getuid()}", agent.plist_path)
        assert runner.calls.index(bootout) < runner.calls.index(bootstrap)


def test_one_field_drift_from_previous_plist_is_not_migrated(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    path = Path(plan.control_plane.plist_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    previous = _previous_canonical_launchagent_plist(plan.control_plane)
    assert previous is not None
    drifted = plistlib.loads(previous)
    drifted["ProcessType"] = "Interactive"
    rendered = plistlib.dumps(drifted, fmt=plistlib.FMT_XML, sort_keys=True)
    path.write_bytes(rendered)
    path.chmod(0o644)

    runner = _runner_for(plan, serve="{}", loaded=True)
    with pytest.raises(RuntimeError, match="content-conflict"):
        apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)
    assert path.read_bytes() == rendered
    assert not any(command[1] in {"bootout", "bootstrap"} for command in runner.calls)


def test_status_checks_tailnet_url_health_and_safari_download_regression(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    probe = FakeProbe(
        {
            "http://127.0.0.1:8768/api/v1/dashboard/v1/snapshot": HttpResult(
                200, {"content-type": "application/json"}
            ),
            "http://127.0.0.1:8769/dashboard/": HttpResult(
                200, {"content-type": "text/html; charset=utf-8"}
            ),
        }
    )
    status = dashboard_lifecycle_status(
        plan,
        _runner_for(plan, serve=_serve_payload(plan.tailscale.expected_edge_origin), loaded=True),
        probe=probe,
        credentials_file=settings.credentials_file,
    )
    assert status.tailnet_url == "https://phone.tailnet.ts.net"
    assert status.tailscale_serve == "ready"
    assert status.health == {"control_plane": "ready", "edge": "ready"}

    safari = FakeProbe(
        {
            "http://127.0.0.1:8768/api/v1/dashboard/v1/snapshot": HttpResult(
                200, {"content-type": "application/json"}
            ),
            "http://127.0.0.1:8769/dashboard/": HttpResult(
                200,
                {"content-type": "application/octet-stream", "content-disposition": "attachment"},
            ),
        }
    )
    regression = dashboard_lifecycle_status(
        plan,
        _runner_for(plan, serve=_serve_payload(plan.tailscale.expected_edge_origin), loaded=True),
        probe=safari,
        credentials_file=settings.credentials_file,
    )
    assert regression.health["edge"] == "safari-content-type-regression"


def test_cloudflare_plan_keeps_secrets_out_of_plists_and_manages_named_tunnel(
    tmp_path: Path,
) -> None:
    settings = _cloudflare_settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    assert plan.format == "cao-dashboard-lifecycle/v2"
    assert plan.tailscale is None
    assert plan.cloudflare is not None
    assert plan.cloudflare.public_origin == "https://dashboard.example.test"
    assert plan.cloudflare.expected_edge_origin == "http://127.0.0.1:8769"
    assert plan.cloudflare.prohibited_control_plane_origin == "http://127.0.0.1:8768"
    arguments = plan.cloudflare.launch_agent.program_arguments
    assert arguments[-2:] == ("--token-file", str(settings.cloudflare_token_file))
    assert arguments[3:6] == ("--metrics", "127.0.0.1:8770", "run")
    assert plan.cloudflare.ready_command[-1] == "ready"
    assert plan.cloudflare.metrics_origin == "http://127.0.0.1:8770"
    rendered = json.dumps(plan.to_dict())
    assert "t" * 128 not in rendered
    assert "127.0.0.1:8768" in rendered
    assert "dashboard.example.test" in rendered

    runner = _cloudflare_runner(plan, loaded=False)
    apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)
    for item in (plan.control_plane, plan.edge, plan.cloudflare.launch_agent):
        plist = Path(item.plist_path)
        assert plist.read_bytes() == canonical_launchagent_plist(item)
        assert "t" * 128 not in plist.read_text(encoding="utf-8")
        assert (
            "launchctl",
            "bootstrap",
            f"gui/{os.getuid()}",
            item.plist_path,
        ) in runner.calls
    assert not any("tailscale" in " ".join(command) for command in runner.calls)


def test_apply_reuses_exact_plist_semantics_without_reformatting_or_restart(
    tmp_path: Path,
) -> None:
    settings = _cloudflare_settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    assert plan.cloudflare is not None
    for item in (plan.control_plane, plan.edge):
        path = Path(item.plist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.parent.chmod(0o700)
        path.parent.chmod(0o700)
        rendered = canonical_launchagent_plist(item) + b"\n"
        path.write_bytes(rendered)
        path.chmod(0o644)

    runner = _cloudflare_runner(plan, loaded=True)
    apply_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)

    for item in (plan.control_plane, plan.edge):
        assert Path(item.plist_path).read_bytes() == canonical_launchagent_plist(item) + b"\n"
        assert not any(command[1:2] == ("bootout",) and command[-1] == item.plist_path for command in runner.calls)


def test_cloudflare_status_requires_local_health_and_access_redirect(tmp_path: Path) -> None:
    settings = _cloudflare_settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    assert plan.cloudflare is not None
    for item in (plan.control_plane, plan.edge, plan.cloudflare.launch_agent):
        path = Path(item.plist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.parent.chmod(0o700)
        path.parent.chmod(0o700)
        path.write_bytes(canonical_launchagent_plist(item))
        path.chmod(0o644)
    probe = FakeProbe(
        {
            "http://127.0.0.1:8768/api/v1/dashboard/v1/snapshot": HttpResult(
                200, {"content-type": "application/json"}
            ),
            "http://127.0.0.1:8769/dashboard/": HttpResult(
                200, {"content-type": "text/html; charset=utf-8"}
            ),
            "https://dashboard.example.test/dashboard/": HttpResult(
                302,
                {
                    "location": (
                        "https://owner-team.cloudflareaccess.com/"
                        "cdn-cgi/access/login/dashboard.example.test"
                    )
                },
            ),
        }
    )
    status = dashboard_lifecycle_status(
        plan,
        _cloudflare_runner(plan, loaded=True),
        probe=probe,
        credentials_file=settings.credentials_file,
    )
    assert status.ready is True
    assert status.exposure_provider == "cloudflare"
    assert status.cloudflare_tunnel == "ready"
    assert status.cloudflare_url == "https://dashboard.example.test"
    assert status.health == {
        "control_plane": "ready",
        "edge": "ready",
        "public": "access-protected",
    }


def test_cloudflare_remove_stops_connector_before_private_services(tmp_path: Path) -> None:
    settings = _cloudflare_settings(tmp_path)
    plan = build_dashboard_lifecycle_plan(settings)
    assert plan.cloudflare is not None
    apply_runner = _cloudflare_runner(plan, loaded=False)
    apply_dashboard_lifecycle(plan, apply_runner, execute=True, owner_confirmed=True)
    remove_runner = _cloudflare_runner(plan, loaded=True)
    remove_dashboard_lifecycle(plan, remove_runner, execute=True, owner_confirmed=True)
    bootouts = [command for command in remove_runner.calls if command[1:2] == ("bootout",)]
    assert bootouts[0][-1] == plan.cloudflare.launch_agent.plist_path
    assert not Path(plan.cloudflare.launch_agent.plist_path).exists()
    assert not Path(plan.edge.plist_path).exists()
    assert not Path(plan.control_plane.plist_path).exists()


def test_remove_has_a_rollback_plan_and_requires_an_explicit_effect_boundary(
    tmp_path: Path,
) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    with pytest.raises(PermissionError, match="requires a matching receipt"):
        remove_dashboard_lifecycle(plan, _runner_for(plan, serve="{}"))

    for item in (plan.control_plane, plan.edge):
        path = Path(item.plist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_launchagent_plist(item))
        path.chmod(0o644)
    runner = _runner_for(plan, serve=_serve_payload(plan.tailscale.expected_edge_origin))
    remove_dashboard_lifecycle(plan, runner, execute=True, owner_confirmed=True)
    assert plan.tailscale.remove_command in runner.calls
    assert not Path(plan.control_plane.plist_path).exists()
    assert not Path(plan.edge.plist_path).exists()


def test_matching_owner_only_receipt_authorizes_without_execute_flag(tmp_path: Path) -> None:
    plan = build_dashboard_lifecycle_plan(_settings(tmp_path))
    receipt_path = tmp_path / "apply-receipt.json"
    receipt_path.write_text(
        json.dumps(
            {
                "format": "cao-dashboard-lifecycle-effect-receipt/v1",
                "action": "apply",
                "plan_digest": plan.plan_digest,
            }
        ),
        encoding="utf-8",
    )
    receipt_path.chmod(0o600)
    apply_dashboard_lifecycle(
        plan,
        _runner_for(plan, serve="{}"),
        receipt=load_effect_authorization(receipt_path),
    )


def test_cli_plan_does_not_run_commands_or_require_a_live_tailnet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = _settings(tmp_path)
    result = main(
        [
            "lifecycle",
            "plan",
            "--application-support-dir",
            str(settings.application_support_dir),
            "--working-directory",
            str(settings.working_directory),
            "--control-plane-command-json",
            json.dumps(list(settings.control_plane_command)),
            "--credentials-file",
            str(settings.credentials_file),
            "--bootstrap-record-dir",
            str(settings.bootstrap_record_dir),
            "--session-record-dir",
            str(settings.session_record_dir),
        ]
    )
    assert result == 0
    assert json.loads(capsys.readouterr().out)["format"] == "cao-dashboard-lifecycle/v2"


def test_upgrade_plan_binds_explicit_existing_plists_database_and_target(tmp_path: Path) -> None:
    settings, lifecycle, plan = _upgrade_plan(tmp_path)

    assert plan.format == "cao-dashboard-lifecycle-upgrade/v1"
    assert plan.lifecycle_plan_digest == lifecycle.plan_digest
    assert plan.control_plane.plist_path.endswith("owner-control-plane.plist")
    assert plan.edge.plist_path.endswith("owner-edge.plist")
    assert plan.control_plane.plist_path != lifecycle.control_plane.plist_path
    assert plan.edge.plist_path != lifecycle.edge.plist_path
    assert plan.source_identity == _database_identity()
    assert plan.target_identity == _release_identity()
    assert plan.credentials_file == str(settings.credentials_file)
    assert plan.codex_app_server_executable == str(canonical_codex_app_server_executable())
    assert plan.codex_mcp_reload_scope == "all_loaded_codex_threads"
    assert plan.codex_mcp_reload_application == "next_active_turn"
    assert len(plan.plan_digest) == 64

    default_plan = build_dashboard_upgrade_plan(
        lifecycle,
        database_path=tmp_path / "default-control-plane.sqlite3",
        backup_destination=tmp_path / "backups" / "default.sqlite3",
        credentials_file=settings.credentials_file,
        source_identity=_database_identity(),
        target_identity=_release_identity(),
    )
    assert default_plan.codex_app_server_socket == str(
        Path.home() / ".codex" / "app-server-control" / "app-server-control.sock"
    )
    assert Path(default_plan.codex_app_server_socket) == canonical_codex_app_server_socket()

    socket_deployment = tmp_path / "socket-deployment"
    socket_deployment.mkdir()
    with _owner_unix_socket() as socket_path:
        _socket_settings, _socket_lifecycle, socket_plan = _upgrade_plan(
            socket_deployment,
            codex_socket=socket_path,
        )
    assert socket_plan.codex_app_server_socket == str(socket_path)
    assert socket_plan.codex_mcp_reload_scope == "all_loaded_codex_threads"
    assert socket_plan.codex_mcp_reload_application == "next_active_turn"


@pytest.mark.parametrize("lifecycle_command", ["upgrade", "restart"])
def test_cli_upgrade_and_restart_plan_only_are_read_only_and_display_release_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    lifecycle_command: str,
) -> None:
    settings, lifecycle, upgrade_plan = _upgrade_plan(tmp_path)
    state = tmp_path / "state"
    database = Database(
        Settings(
            state_dir=state,
            runtime_launch_dir=state / "runtime-launches",
        )
    )
    backup = tmp_path / "backups" / "final.sqlite3"

    result = main(
        [
            "lifecycle",
            lifecycle_command,
            "--application-support-dir",
            str(settings.application_support_dir),
            "--working-directory",
            str(settings.working_directory),
            "--control-plane-command-json",
            json.dumps(list(settings.control_plane_command)),
            "--credentials-file",
            str(settings.credentials_file),
            "--bootstrap-record-dir",
            str(settings.bootstrap_record_dir),
            "--session-record-dir",
            str(settings.session_record_dir),
            "--database-path",
            str(database.path),
            "--backup-destination",
            str(backup),
            "--control-plane-plist-path",
            upgrade_plan.control_plane.plist_path,
            "--control-plane-plist-sha256",
            upgrade_plan.control_plane.plist_sha256,
            "--edge-plist-path",
            upgrade_plan.edge.plist_path,
            "--edge-plist-sha256",
            upgrade_plan.edge.plist_sha256,
            "--plan-only",
        ]
    )

    rendered = json.loads(capsys.readouterr().out)
    assert result == 0
    assert rendered["format"] == "cao-dashboard-lifecycle-upgrade/v1"
    assert rendered["target_identity"]["schema_version"] == SCHEMA_VERSION
    assert len(rendered["target_identity"]["release_id"]) == 64
    assert rendered["codex_mcp_reload_scope"] == "all_loaded_codex_threads"
    assert rendered["codex_mcp_reload_application"] == "next_active_turn"
    assert rendered["codex_app_server_executable"] == str(canonical_codex_app_server_executable())
    assert rendered["codex_app_server_socket"] == str(
        Path.home() / ".codex" / "app-server-control" / "app-server-control.sock"
    )
    assert not backup.exists()
    assert lifecycle.tailscale.apply_command[0] == "tailscale"


def test_lifecycle_help_exposes_restart_as_the_same_release_safe_upgrade_alias(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as lifecycle_help:
        main(["lifecycle", "--help"])
    assert lifecycle_help.value.code == 0
    summary = " ".join(capsys.readouterr().out.split())
    assert "restart" in summary
    assert "refresh-mcp" in summary
    assert "full shared-system restart" in summary

    with pytest.raises(SystemExit) as restart_help:
        main(["lifecycle", "restart", "--help"])
    assert restart_help.value.code == 0
    detail = " ".join(capsys.readouterr().out.split())
    assert "explicit alias of upgrade" in detail
    assert "even on the same release" in detail
    assert "--plan-only" in detail
    assert "--execute" in detail
    assert "--owner-confirm" in detail


def test_cli_refresh_mcp_plan_uses_the_canonical_codex_socket_without_touching_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(["lifecycle", "refresh-mcp", "--plan-only"])

    rendered = json.loads(capsys.readouterr().out)
    assert result == 0
    assert rendered["format"] == "cao-dashboard-lifecycle-refresh-mcp/v1"
    assert rendered["codex_app_server_executable"] == str(canonical_codex_app_server_executable())
    assert rendered["codex_app_server_socket"] == str(
        Path.home() / ".codex" / "app-server-control" / "app-server-control.sock"
    )
    assert rendered["codex_mcp_reload_scope"] == "all_loaded_codex_threads"
    assert rendered["codex_mcp_reload_application"] == "next_active_turn"


def test_cli_refresh_mcp_rejects_socket_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as rejected:
        main(
            [
                "lifecycle",
                "refresh-mcp",
                "--codex-app-server-socket",
                "/tmp/unreviewed.sock",
                "--plan-only",
            ]
        )

    assert rejected.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err


def test_refresh_mcp_submits_one_owner_socket_reload_without_service_commands() -> None:
    calls: list[tuple[Path, str]] = []
    with _owner_unix_socket() as socket_path:
        plan = build_codex_mcp_refresh_plan(
            codex_app_server_socket=socket_path,
            target_identity=_release_identity(),
        )
        result = refresh_codex_mcp(
            plan,
            reload_codex_mcp=lambda path, release_id: calls.append((path, release_id)),
            release_identity=_release_identity,
            execute=True,
            owner_confirmed=True,
        )

    assert calls == [(socket_path, plan.target_identity.release_id)]
    assert result.status == "submitted_for_next_active_turn"
    assert result.codex_app_server_restart == "not_performed"
    assert result.codex_mcp_reload == "submitted_for_next_active_turn"
    assert result.codex_mcp_reload_scope == "all_loaded_codex_threads"
    assert result.codex_mcp_reload_application == "next_active_turn"
    assert result.current_conversation_verification == "pending"


def test_cli_refresh_mcp_executes_without_service_or_worker_arguments(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def submit(plan, *, receipt, execute, owner_confirmed):
        captured.update(
            {
                "plan": plan,
                "receipt": receipt,
                "execute": execute,
                "owner_confirmed": owner_confirmed,
            }
        )
        return CodexMCPRefreshResult(
            status="submitted_for_next_active_turn",
            plan_digest=plan.plan_digest,
            target_identity=plan.target_identity,
            codex_app_server_restart="not_performed",
            codex_mcp_reload="submitted_for_next_active_turn",
            codex_mcp_reload_scope=plan.codex_mcp_reload_scope,
            codex_mcp_reload_application=plan.codex_mcp_reload_application,
            current_conversation_verification="pending",
        )

    monkeypatch.setattr("cao_control_plane.dashboard_lifecycle.refresh_codex_mcp", submit)
    result = main(
        [
            "lifecycle",
            "refresh-mcp",
            "--execute",
            "--owner-confirm",
        ]
    )

    rendered = json.loads(capsys.readouterr().out)
    assert result == 0
    assert rendered["status"] == "submitted_for_next_active_turn"
    assert captured["receipt"] is None
    assert captured["execute"] is True
    assert captured["owner_confirmed"] is True
    assert captured["plan"].codex_app_server_socket == str(canonical_codex_app_server_socket())


@pytest.mark.parametrize(
    ("error", "reload_status", "reason_code"),
    (
        (
            CodexMCPRefreshPhaseError(
                "bounded pre-send failure",
                reload_status="not_submitted",
            ),
            "not_submitted",
            "codex_mcp_refresh_incomplete",
        ),
        (
            CodexMCPRefreshPhaseUnknown(
                "bounded post-send failure",
                reload_status="unknown",
            ),
            "unknown",
            "codex_mcp_reload_outcome_unknown",
        ),
    ),
)
def test_cli_refresh_mcp_reports_bounded_restart_and_reload_phase_failure(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    error: CodexMCPRefreshPhaseError,
    reload_status: str,
    reason_code: str,
) -> None:
    def submit(*_args, **_kwargs):
        raise error

    monkeypatch.setattr("cao_control_plane.dashboard_lifecycle.refresh_codex_mcp", submit)

    result = main(
        [
            "lifecycle",
            "refresh-mcp",
            "--execute",
            "--owner-confirm",
        ]
    )

    rendered = json.loads(capsys.readouterr().out)
    assert result == 2
    assert rendered == {
        "status": "stopped",
        "reason_code": reason_code,
        "retryable": False,
        "codex_app_server_restart": "performed",
        "codex_mcp_reload": reload_status,
        "current_conversation_verification": "pending",
    }


@pytest.mark.parametrize("socket_kind", ["missing", "regular-file"])
def test_refresh_mcp_fails_closed_on_missing_or_invalid_codex_socket(
    tmp_path: Path,
    socket_kind: str,
) -> None:
    socket_dir = tmp_path / "codex-control"
    socket_dir.mkdir(mode=0o700)
    socket_dir.chmod(0o700)
    socket_path = socket_dir / "app-server-control.sock"
    if socket_kind == "regular-file":
        socket_path.write_text("not a socket", encoding="utf-8")
        socket_path.chmod(0o600)
    plan = build_codex_mcp_refresh_plan(
        codex_app_server_socket=socket_path,
        target_identity=_release_identity(),
    )
    reload_calls: list[tuple[Path, str]] = []

    with pytest.raises(ValueError, match="socket"):
        refresh_codex_mcp(
            plan,
            reload_codex_mcp=lambda path, release_id: reload_calls.append((path, release_id)),
            release_identity=_release_identity,
            execute=True,
            owner_confirmed=True,
        )

    assert reload_calls == []


def test_refresh_receipt_cannot_authorize_replaced_socket_or_result_scope() -> None:
    reload_calls: list[tuple[Path, str]] = []
    with _owner_unix_socket() as reviewed_socket, _owner_unix_socket() as substituted_socket:
        plan = build_codex_mcp_refresh_plan(
            codex_app_server_socket=reviewed_socket,
            target_identity=_release_identity(),
        )
        receipt = EffectAuthorization(plan.plan_digest, "refresh-mcp")
        modified_plans = (
            replace(plan, codex_app_server_socket=str(substituted_socket)),
            _redigest(replace(plan, codex_app_server_executable="/tmp/unreviewed-codex")),
            _redigest(replace(plan, codex_mcp_reload_scope="one_unreviewed_thread")),
        )
        for modified in modified_plans:
            with pytest.raises(ValueError, match="reviewed digest"):
                refresh_codex_mcp(
                    modified,
                    reload_codex_mcp=lambda path, release_id: reload_calls.append(
                        (path, release_id)
                    ),
                    release_identity=_release_identity,
                    receipt=receipt,
                )

    assert reload_calls == []


def test_upgrade_default_host_refresh_rejects_noncanonical_socket_before_mutation(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    runner = FakeRunner({})

    with pytest.raises(ValueError, match="canonical Codex app-server control socket"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe([]),
            preflight=lambda *_args: pytest.fail("preflight must not run"),
            execute=True,
            owner_confirmed=True,
        )

    assert runner.calls == []


def test_upgrade_broken_managed_host_preflight_fails_before_service_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    socket_path = Path(plan.codex_app_server_socket)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    runner = FakeRunner({})
    observed: list[Path] = []

    def reject_broken_managed_host(path: Path) -> str:
        observed.append(path)
        raise RuntimeError("Codex app-server managed daemon identity is invalid")

    with pytest.raises(RuntimeError, match="managed daemon identity is invalid"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe([]),
            preflight=lambda *_args: pytest.fail("CAO preflight must not run"),
            codex_host_preflight=reject_broken_managed_host,
            execute=True,
            owner_confirmed=True,
        )

    assert observed == [socket_path]
    assert runner.calls == []


def test_upgrade_missing_codex_socket_fails_before_any_service_mutation(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    socket_dir = tmp_path / "missing-codex-control"
    socket_dir.mkdir(mode=0o700)
    socket_dir.chmod(0o700)
    plan = _redigest(replace(plan, codex_app_server_socket=str(socket_dir / "missing.sock")))
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[command] = CommandResult(command, 0, _loaded_launchctl_output(binding))
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")
    runner = FakeRunner(responses)

    def exact_preflight(plan_value, runner_value, backup_database):
        return preflight_dashboard_upgrade(
            plan_value,
            runner_value,
            backup_database,
            inspect_database=lambda _path: plan.source_identity,
            release_identity=lambda: plan.target_identity,
            migration_preflight=lambda *_args: pytest.fail("migration must not run"),
        )

    with pytest.raises(ValueError, match="socket does not exist"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe([]),
            preflight=exact_preflight,
            backup_database=lambda *_args, **_kwargs: pytest.fail("backup must not run"),
            reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
            execute=True,
            owner_confirmed=True,
        )

    assert all(command[1] not in {"bootout", "bootstrap"} for command in runner.calls)


def test_upgrade_receipt_cannot_authorize_replaced_socket_or_reload_scope(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    receipt = EffectAuthorization(plan.plan_digest, "upgrade")
    modified_plans = (
        replace(plan, codex_app_server_socket=str(tmp_path / "substituted.sock")),
        _redigest(replace(plan, codex_app_server_executable="/tmp/unreviewed-codex")),
        _redigest(replace(plan, codex_mcp_reload_scope="one_unreviewed_thread")),
    )

    for modified in modified_plans:
        runner = FakeRunner({})
        with pytest.raises(ValueError, match="reviewed digest"):
            upgrade_dashboard_lifecycle(
                modified,
                runner,
                probe=FakeUpgradeProbe([]),
                preflight=lambda *_args: pytest.fail("preflight must not run"),
                reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
                receipt=receipt,
            )
        assert runner.calls == []


def test_upgrade_preflight_checks_loaded_exact_plists_and_migration_evidence(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[command] = CommandResult(
            command,
            0,
            _loaded_launchctl_output(binding),
        )
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")
    migration_calls: list[str] = []

    def migration_preflight(plan_value, backup_database):
        del plan_value, backup_database
        migration_calls.append("verified")
        return _database_identity(SCHEMA_VERSION)

    result = preflight_dashboard_upgrade(
        plan,
        FakeRunner(responses),
        lambda source, destination, *, replace=True: pytest.fail(
            f"unexpected backup {source} {destination} {replace}"
        ),
        inspect_database=lambda path: _database_identity(),
        release_identity=_release_identity,
        migration_preflight=migration_preflight,
    )

    assert migration_calls == ["verified"]
    assert result.database_identity == _database_identity()
    assert result.migration_identity == _database_identity(SCHEMA_VERSION)

    mismatched_database = replace(
        plan,
        database_path=str(tmp_path / "different-control-plane.sqlite3"),
    )
    with pytest.raises(RuntimeError, match="does not match the loaded Control Plane config"):
        preflight_dashboard_upgrade(
            mismatched_database,
            FakeRunner(responses),
            lambda source, destination, *, replace=True: pytest.fail(
                f"unexpected backup {source} {destination} {replace}"
            ),
            inspect_database=lambda path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=migration_preflight,
        )

    Path(plan.edge.plist_path).write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="content-conflict"):
        preflight_dashboard_upgrade(
            plan,
            FakeRunner(responses),
            lambda source, destination, *, replace=True: pytest.fail(
                f"unexpected backup {source} {destination} {replace}"
            ),
            inspect_database=lambda path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=migration_preflight,
        )


@pytest.mark.parametrize("config_occurrences", [0, 2])
def test_upgrade_preflight_requires_one_explicit_control_plane_config(
    tmp_path: Path,
    config_occurrences: int,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    control_plane_path = Path(plan.control_plane.plist_path)
    plist = plistlib.loads(control_plane_path.read_bytes())
    arguments = list(plist["ProgramArguments"])
    index = arguments.index("--config")
    config_pair = arguments[index : index + 2]
    del arguments[index : index + 2]
    arguments.extend(config_pair * config_occurrences)
    plist["ProgramArguments"] = arguments
    payload = plistlib.dumps(plist, fmt=plistlib.FMT_XML, sort_keys=True)
    control_plane_path.write_bytes(payload)
    binding = replace(
        plan.control_plane,
        plist_sha256=hashlib.sha256(payload).hexdigest(),
    )
    changed = replace(plan, control_plane=binding)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for item in (changed.control_plane, changed.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{item.label}")
        responses[command] = CommandResult(command, 0, _loaded_launchctl_output(item))
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")

    with pytest.raises(RuntimeError, match="exactly one explicit Control Plane --config"):
        preflight_dashboard_upgrade(
            changed,
            FakeRunner(responses),
            lambda *_args, **_kwargs: pytest.fail("backup must not run"),
            inspect_database=lambda _path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=lambda *_args: pytest.fail("migration must not run"),
        )


def test_upgrade_preflight_rejects_a_loaded_state_directory_override(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        output = _loaded_launchctl_output(binding)
        if binding == plan.control_plane:
            output = output.replace(
                "\tworking directory = ",
                "\tinherited environment = {\n"
                f"\t\tCAO_A2A_STATE_DIR => {tmp_path / 'other-state'}\n"
                "\t}\n\tworking directory = ",
            )
        responses[command] = CommandResult(command, 0, output)
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")

    with pytest.raises(RuntimeError, match="state_dir to come from its exact config"):
        preflight_dashboard_upgrade(
            plan,
            FakeRunner(responses),
            lambda *_args, **_kwargs: pytest.fail("backup must not run"),
            inspect_database=lambda _path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=lambda *_args: pytest.fail("migration must not run"),
        )


def test_upgrade_preflight_matches_runtime_semantics_for_a_tilde_config_argument(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    literal_parent = tmp_path / "~"
    literal_parent.mkdir()
    runtime_config = literal_parent / "control-plane.toml"
    runtime_config.write_text(
        f"[server]\nstate_dir = {json.dumps(str(tmp_path))}\n",
        encoding="utf-8",
    )
    runtime_config.chmod(0o600)
    fake_home = tmp_path / "fake-home"
    fake_home.mkdir()
    misleading_config = fake_home / "control-plane.toml"
    misleading_config.write_text(
        f"[server]\nstate_dir = {json.dumps(str(tmp_path / 'wrong-state'))}\n",
        encoding="utf-8",
    )
    misleading_config.chmod(0o600)
    monkeypatch.setenv("HOME", str(fake_home))

    control_plane_path = Path(plan.control_plane.plist_path)
    plist = plistlib.loads(control_plane_path.read_bytes())
    arguments = list(plist["ProgramArguments"])
    arguments[arguments.index("--config") + 1] = "~/control-plane.toml"
    plist["ProgramArguments"] = arguments
    payload = plistlib.dumps(plist, fmt=plistlib.FMT_XML, sort_keys=True)
    control_plane_path.write_bytes(payload)
    binding = replace(
        plan.control_plane,
        plist_sha256=hashlib.sha256(payload).hexdigest(),
    )
    changed = replace(plan, control_plane=binding)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for item in (changed.control_plane, changed.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{item.label}")
        responses[command] = CommandResult(command, 0, _loaded_launchctl_output(item))
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")

    result = preflight_dashboard_upgrade(
        changed,
        FakeRunner(responses),
        lambda *_args, **_kwargs: pytest.fail("backup must not run"),
        inspect_database=lambda _path: _database_identity(),
        release_identity=_release_identity,
        migration_preflight=lambda *_args: _database_identity(SCHEMA_VERSION),
    )

    assert result.database_identity == _database_identity()


def test_upgrade_preflight_rejects_an_oversized_config_before_a_late_override(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    config_path = tmp_path / "control-plane.toml"
    prefix = f"[server]\nstate_dir = {json.dumps(str(tmp_path))}\n#"
    suffix = f"\n[runtime]\nstate_dir = {json.dumps(str(tmp_path / 'unprotected-state'))}\n"
    config_path.write_text(
        prefix + ("x" * (1024 * 1024)) + suffix,
        encoding="utf-8",
    )
    config_path.chmod(0o600)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[command] = CommandResult(command, 0, _loaded_launchctl_output(binding))
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")

    with pytest.raises(RuntimeError, match="Control Plane config identity is invalid"):
        preflight_dashboard_upgrade(
            plan,
            FakeRunner(responses),
            lambda *_args, **_kwargs: pytest.fail("backup must not run"),
            inspect_database=lambda _path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=lambda *_args: pytest.fail("migration must not run"),
        )


@pytest.mark.parametrize("state_dir", [None, "~/cao-state"])
def test_upgrade_preflight_requires_an_explicit_absolute_config_state_directory(
    tmp_path: Path,
    state_dir: str | None,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    config_path = tmp_path / "control-plane.toml"
    config_path.write_text(
        (
            '[server]\nhost = "127.0.0.1"\n'
            if state_dir is None
            else f"[server]\nstate_dir = {json.dumps(state_dir)}\n"
        ),
        encoding="utf-8",
    )
    config_path.chmod(0o600)
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[command] = CommandResult(command, 0, _loaded_launchctl_output(binding))
    environment_command = ("launchctl", "getenv", "CAO_A2A_STATE_DIR")
    responses[environment_command] = CommandResult(environment_command, 0, "")

    with pytest.raises(RuntimeError, match="explicit absolute state_dir"):
        preflight_dashboard_upgrade(
            plan,
            FakeRunner(responses),
            lambda *_args, **_kwargs: pytest.fail("backup must not run"),
            inspect_database=lambda _path: _database_identity(),
            release_identity=_release_identity,
            migration_preflight=lambda *_args: pytest.fail("migration must not run"),
        )


def test_migration_preflight_migrates_only_disposable_copy_and_verifies_projection(
    tmp_path: Path,
) -> None:
    state = tmp_path / "live-state"
    database = Database(
        Settings(
            state_dir=state,
            runtime_launch_dir=state / "runtime-launches",
        )
    )
    with database.transaction() as connection:
        connection.execute("DROP INDEX IF EXISTS runtime_enrollment_tickets_attempt_generation_idx")
        connection.execute("ALTER TABLE runtime_enrollment_tickets DROP COLUMN attempt_id")
        connection.execute("DELETE FROM schema_migrations WHERE version = 29")
        connection.execute("UPDATE metadata SET value = '28' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 28")
    source = inspect_sqlite_database(database.path)
    assert source.schema_version == 28
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    _settings_value, _lifecycle, base_plan = _upgrade_plan(deployment)
    plan = replace(
        base_plan,
        database_path=str(database.path),
        source_identity=source,
    )

    migrated = verify_dashboard_upgrade_migration(plan, backup_sqlite_database)

    assert migrated.ok is True
    assert migrated.schema_version == SCHEMA_VERSION
    assert inspect_sqlite_database(database.path) == source


@pytest.mark.parametrize(
    "source_schema_version",
    [28, SCHEMA_VERSION],
    ids=["schema-upgrade", "same-release-restart"],
)
def test_upgrade_and_same_release_restart_use_the_exact_offline_backup_order(
    tmp_path: Path,
    source_schema_version: int,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    source_identity = _database_identity(source_schema_version)
    plan = _redigest(replace(plan, source_identity=source_identity))
    if source_schema_version == plan.target_identity.schema_version:
        # A matching release/schema is version evidence, not restart evidence.
        # The executor must still cross the stop/fence/bootstrap boundaries below.
        assert source_schema_version == SCHEMA_VERSION
    trace: list[str] = []
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        for action in ("bootout", "bootstrap"):
            command = (
                "launchctl",
                action,
                f"gui/{os.getuid()}",
                binding.plist_path,
            )
            responses[command] = CommandResult(command, 0)
        unloaded = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[unloaded] = CommandResult(unloaded, 1)
    runner = FakeRunner(responses, trace=trace)

    def preflight(plan_value, runner_value, backup_database):
        del plan_value, runner_value, backup_database
        trace.append("preflight:migration-copy")
        return DashboardUpgradePreflightResult(
            database_identity=source_identity,
            migration_identity=_database_identity(SCHEMA_VERSION),
            migration_projection_healthy=True,
            control_plane_plist_sha256=plan.control_plane.plist_sha256,
            edge_plist_sha256=plan.edge.plist_sha256,
        )

    def backup(source: Path, destination: Path, *, replace: bool = True):
        assert source == Path(plan.database_path)
        assert destination == Path(plan.backup_destination)
        assert replace is False
        trace.append("backup:final-offline")
        return SQLiteBackupResult(
            path=destination,
            source_identity=source_identity,
            backup_identity=source_identity,
            sha256="c" * 64,
        )

    def reload_mcp(socket_path: Path, release_id: str) -> None:
        assert socket_path == Path(plan.codex_app_server_socket)
        assert release_id == plan.target_identity.release_id
        trace.append("codex:mcp-reload-ack")

    result = upgrade_dashboard_lifecycle(
        plan,
        runner,
        probe=FakeUpgradeProbe(trace),
        process_controller=FakeUpgradeProcessController(trace),
        preflight=preflight,
        backup_database=backup,
        reload_codex_mcp=reload_mcp,
        execute=True,
        owner_confirmed=True,
    )

    assert result.status == "ready"
    assert result.codex_app_server_restart == "not_performed"
    assert result.codex_mcp_reload == "submitted_for_next_active_turn"
    assert result.codex_mcp_reload_scope == "all_loaded_codex_threads"
    assert result.codex_mcp_reload_application == "next_active_turn"
    assert result.current_conversation_verification == "pending"
    assert trace == [
        "preflight:migration-copy",
        "probe:preflight",
        f"process:capture:{plan.edge.label}",
        f"process:capture:{plan.control_plane.label}",
        f"command:bootout {plan.edge.plist_path}",
        f"command:bootout {plan.control_plane.plist_path}",
        "command:print",
        "command:print",
        "process:fenced",
        "probe:stopped",
        "backup:final-offline",
        f"command:bootstrap {plan.control_plane.plist_path}",
        "probe:control-plane",
        f"command:bootstrap {plan.edge.plist_path}",
        "probe:edge",
        "codex:mcp-reload-ack",
    ]
    assert not any("tailscale" in " ".join(command) for command in runner.calls)


def test_exact_upgrade_process_controller_escalates_only_the_captured_generation() -> None:
    root = _audit_process(4321)
    child = _audit_process(4322, pgid=root.pgid)
    live = {root.pid, child.pid}
    signals: list[tuple[int, int]] = []

    class FakeProcessAPI:
        def is_live(self, identity: DarwinAuditProcessIdentity) -> bool:
            return identity.pid in live

        def list_process_group(self, pgid: int) -> tuple[int, ...]:
            assert pgid == root.pgid
            return tuple(sorted(live))

        def signal(self, identity: DarwinAuditProcessIdentity, value: int) -> bool:
            signals.append((identity.pid, value))
            if value == signal.SIGKILL:
                live.remove(identity.pid)
            return True

    controller = ExactDashboardUpgradeProcessController(
        process_api=FakeProcessAPI(),
        sleeper=lambda _value: None,
        attempts=1,
        interval_seconds=0,
    )
    controller.fence_after_bootout((DashboardUpgradeProcessGroup(root.pgid, (root, child)),))

    assert signals == [
        (root.pid, signal.SIGTERM),
        (child.pid, signal.SIGTERM),
        (root.pid, signal.SIGKILL),
        (child.pid, signal.SIGKILL),
    ]


def test_exact_upgrade_process_controller_captures_the_complete_stable_group(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    binding = plan.control_plane
    root = _audit_process(4321, asid=100023)
    child = _audit_process(4322, pgid=root.pgid, asid=root.asid)
    command = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
    runner = FakeRunner({command: CommandResult(command, 0, _loaded_launchctl_output(binding))})
    captured: list[int] = []

    class CaptureProcessAPI:
        def capture(self, pid: int, *, expected_asid: int) -> DarwinAuditProcessIdentity:
            assert expected_asid == root.asid
            captured.append(pid)
            return {root.pid: root, child.pid: child}[pid]

        def list_process_group(self, pgid: int) -> tuple[int, ...]:
            assert pgid == root.pgid
            return (root.pid, child.pid)

        def is_live(self, _identity: DarwinAuditProcessIdentity) -> bool:
            return True

        def signal(self, _identity: DarwinAuditProcessIdentity, _value: int) -> bool:
            pytest.fail("capture must not signal a process")

    group = ExactDashboardUpgradeProcessController(
        process_api=CaptureProcessAPI(),
        attempts=1,
        interval_seconds=0,
    ).capture(runner, binding)

    assert group == DashboardUpgradeProcessGroup(root.pgid, (root, child))
    assert captured == [root.pid, child.pid]


@pytest.mark.parametrize("captured_still_live", [True, False])
def test_exact_upgrade_process_controller_never_signals_unknown_group_members(
    captured_still_live: bool,
) -> None:
    identity = _audit_process(4321)
    signals: list[int] = []

    class DriftedProcessAPI:
        def is_live(self, _identity: DarwinAuditProcessIdentity) -> bool:
            return captured_still_live

        def list_process_group(self, pgid: int) -> tuple[int, ...]:
            assert pgid == identity.pgid
            return (identity.pid, 9999)

        def signal(self, _identity: DarwinAuditProcessIdentity, value: int) -> bool:
            signals.append(value)
            return True

    controller = ExactDashboardUpgradeProcessController(
        process_api=DriftedProcessAPI(),
        sleeper=lambda _value: None,
        attempts=1,
        interval_seconds=0,
    )

    with pytest.raises(LifecycleUnknownOutcome, match="unknown member"):
        controller.fence_after_bootout((DashboardUpgradeProcessGroup(identity.pgid, (identity,)),))
    assert signals == []


def test_exact_upgrade_process_controller_waits_for_known_dead_zombie_to_leave_group() -> None:
    identity = _audit_process(4321)
    group_members = [(identity.pid,), ()]
    sleeps: list[float] = []
    signals: list[int] = []

    class ZombieProcessAPI:
        def is_live(self, _identity: DarwinAuditProcessIdentity) -> bool:
            return False

        def list_process_group(self, pgid: int) -> tuple[int, ...]:
            assert pgid == identity.pgid
            return group_members.pop(0)

        def signal(self, _identity: DarwinAuditProcessIdentity, value: int) -> bool:
            signals.append(value)
            return True

    ExactDashboardUpgradeProcessController(
        process_api=ZombieProcessAPI(),
        sleeper=sleeps.append,
        attempts=2,
        interval_seconds=0.25,
    ).fence_after_bootout((DashboardUpgradeProcessGroup(identity.pgid, (identity,)),))

    assert sleeps == [0.25]
    assert signals == []


def test_exact_upgrade_process_controller_accepts_exit_between_token_and_group_reads() -> None:
    identity = _audit_process(4321)
    token_states = [True, False, False]
    signals: list[int] = []

    class ExitRaceProcessAPI:
        def is_live(self, _identity: DarwinAuditProcessIdentity) -> bool:
            return token_states.pop(0)

        def list_process_group(self, pgid: int) -> tuple[int, ...]:
            assert pgid == identity.pgid
            return ()

        def signal(self, _identity: DarwinAuditProcessIdentity, value: int) -> bool:
            signals.append(value)
            return True

    ExactDashboardUpgradeProcessController(
        process_api=ExitRaceProcessAPI(),
        sleeper=lambda _value: None,
        attempts=1,
        interval_seconds=0,
    ).fence_after_bootout((DashboardUpgradeProcessGroup(identity.pgid, (identity,)),))

    assert token_states == []
    assert signals == []


def test_darwin_process_group_zero_with_errno_is_never_treated_as_empty() -> None:
    class FailingProc:
        @staticmethod
        def proc_listpgrppids(_pgid, _buffer, _size):
            ctypes.set_errno(errno.EPERM)
            return 0

    process_api = object.__new__(DarwinDashboardProcessAPI)
    process_api._proc = FailingProc()

    with pytest.raises(RuntimeError, match="membership is unavailable"):
        process_api.list_process_group(4321)

    identity = _audit_process(4321)

    class PostBootoutFailure:
        def is_live(self, _identity: DarwinAuditProcessIdentity) -> bool:
            return False

        def list_process_group(self, _pgid: int) -> tuple[int, ...]:
            raise RuntimeError("membership is unavailable")

        def signal(self, _identity: DarwinAuditProcessIdentity, _value: int) -> bool:
            pytest.fail("unknown group state must never be signalled")

    with pytest.raises(LifecycleUnknownOutcome, match="group state is unknown"):
        ExactDashboardUpgradeProcessController(
            process_api=PostBootoutFailure(),
            attempts=1,
            interval_seconds=0,
        ).fence_after_bootout((DashboardUpgradeProcessGroup(identity.pgid, (identity,)),))


def test_upgrade_never_backs_up_or_bootstraps_while_old_process_is_alive(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    trace: list[str] = []
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        bootout = (
            "launchctl",
            "bootout",
            f"gui/{os.getuid()}",
            binding.plist_path,
        )
        responses[bootout] = CommandResult(bootout, 0)
        unloaded = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[unloaded] = CommandResult(unloaded, 1)
    runner = FakeRunner(responses, trace=trace)

    def preflight(*_args):
        return DashboardUpgradePreflightResult(
            _database_identity(),
            _database_identity(SCHEMA_VERSION),
            True,
            plan.control_plane.plist_sha256,
            plan.edge.plist_sha256,
        )

    class StuckProcessController(FakeUpgradeProcessController):
        def fence_after_bootout(self, identities) -> None:
            assert len(identities) == 2
            raise LifecycleUnknownOutcome("exact booted-out process remained alive")

    with pytest.raises(LifecycleUnknownOutcome, match="remained alive"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe(trace),
            process_controller=StuckProcessController(trace),
            preflight=preflight,
            backup_database=lambda *_args, **_kwargs: pytest.fail(
                "backup must not run while an old process remains live"
            ),
            reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
            execute=True,
            owner_confirmed=True,
        )

    assert "probe:stopped" not in trace
    assert not any(command[1] == "bootstrap" for command in runner.calls)


def test_upgrade_requires_authority_before_preflight_backup_or_launchctl(tmp_path: Path) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    trace: list[str] = []
    runner = FakeRunner({}, trace=trace)

    with pytest.raises(PermissionError, match="requires a matching receipt"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe(trace),
            process_controller=FakeUpgradeProcessController(trace),
            preflight=lambda *_args: pytest.fail("preflight must not run"),
            backup_database=lambda *_args, **_kwargs: pytest.fail("backup must not run"),
        )

    assert trace == []
    assert runner.calls == []


def test_control_plane_readiness_failure_never_bootstraps_edge_or_reloads_codex(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    trace: list[str] = []
    responses: dict[tuple[str, ...], CommandResult] = {}
    for binding in (plan.control_plane, plan.edge):
        bootout = (
            "launchctl",
            "bootout",
            f"gui/{os.getuid()}",
            binding.plist_path,
        )
        responses[bootout] = CommandResult(bootout, 0)
        unloaded = ("launchctl", "print", f"gui/{os.getuid()}/{binding.label}")
        responses[unloaded] = CommandResult(unloaded, 1)
    cp_bootstrap = (
        "launchctl",
        "bootstrap",
        f"gui/{os.getuid()}",
        plan.control_plane.plist_path,
    )
    responses[cp_bootstrap] = CommandResult(cp_bootstrap, 0)
    runner = FakeRunner(responses, trace=trace)

    def preflight(plan_value, runner_value, backup_database):
        del plan_value, runner_value, backup_database
        return DashboardUpgradePreflightResult(
            _database_identity(),
            _database_identity(SCHEMA_VERSION),
            True,
            plan.control_plane.plist_sha256,
            plan.edge.plist_sha256,
        )

    def backup(source: Path, destination: Path, *, replace: bool = True):
        del source, replace
        return SQLiteBackupResult(
            destination,
            _database_identity(),
            _database_identity(),
            "c" * 64,
        )

    with pytest.raises(LifecycleUnknownOutcome, match="did not become ready"):
        upgrade_dashboard_lifecycle(
            plan,
            runner,
            probe=FakeUpgradeProbe(trace, fail_control_plane=True),
            process_controller=FakeUpgradeProcessController(trace),
            preflight=preflight,
            backup_database=backup,
            reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
            execute=True,
            owner_confirmed=True,
        )

    edge_bootstrap = (
        "launchctl",
        "bootstrap",
        f"gui/{os.getuid()}",
        plan.edge.plist_path,
    )
    assert edge_bootstrap not in runner.calls


def test_upgrade_probe_requires_exact_release_schema_catalog_and_edge_html(tmp_path: Path) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    identity = plan.target_identity.to_dict()
    responses = {
        plan.control_plane_origin + "/health": HttpResult(
            200,
            {"content-type": "application/json"},
            {"status": "ok", **identity},
        ),
        plan.control_plane_origin + "/ready": HttpResult(
            200,
            {"content-type": "application/json"},
            {"ready": True, **identity},
        ),
        plan.control_plane_origin + "/api/v1/dashboard/v1/snapshot": HttpResult(
            200,
            {"content-type": "application/json"},
            {},
        ),
        plan.edge_origin + "/dashboard/": HttpResult(
            200,
            {"content-type": "text/html; charset=utf-8"},
        ),
    }
    probe = HttpxDashboardUpgradeProbe(FakeProbe(responses), sleeper=lambda _value: None)

    probe.require_preflight_ready(plan, target_projection_verified=True)
    probe.wait_for_control_plane(plan)
    probe.wait_for_edge(plan)

    stopped = HttpxDashboardUpgradeProbe(
        FakeProbe(
            {
                plan.control_plane_origin + "/health": HttpResult(0, {}),
                plan.edge_origin + "/dashboard/": HttpResult(0, {}),
            }
        ),
        sleeper=lambda _value: None,
    )
    stopped.wait_for_stopped(plan)

    responses[plan.control_plane_origin + "/ready"] = HttpResult(
        200,
        {"content-type": "application/json"},
        {"ready": True, **identity, "schema_version": 28},
    )
    with pytest.raises(LifecycleUnknownOutcome, match="Control Plane"):
        HttpxDashboardUpgradeProbe(
            FakeProbe(responses), sleeper=lambda _value: None
        ).wait_for_control_plane(plan)


def test_upgrade_probe_allows_only_a_target_verified_source_projection_repair(
    tmp_path: Path,
) -> None:
    _settings_value, _lifecycle, plan = _upgrade_plan(tmp_path)
    source_identity = {
        "release_id": "source-release",
        "schema_version": plan.source_identity.schema_version,
        "mcp_catalog_digest": "source-catalog",
    }
    ready_body = {
        "ready": False,
        **source_identity,
        "database": {"ok": True, "integrity": ["ok"], "foreign_key_errors": []},
        "authority": {"mode": "canonical"},
        "projection": {
            "healthy": False,
            "violations": [{"code": "fixed.by.target"}],
        },
        "dispatcher": {"healthy": True, "running": True, "issues": []},
    }

    def probe_for(body: Mapping[str, object]) -> HttpxDashboardUpgradeProbe:
        return HttpxDashboardUpgradeProbe(
            FakeProbe(
                {
                    plan.control_plane_origin + "/health": HttpResult(
                        200,
                        {"content-type": "application/json"},
                        {"status": "ok", **source_identity},
                    ),
                    plan.control_plane_origin + "/ready": HttpResult(
                        503,
                        {"content-type": "application/json"},
                        body,
                    ),
                    plan.control_plane_origin + "/api/v1/dashboard/v1/snapshot": HttpResult(
                        200,
                        {"content-type": "application/json"},
                        {},
                    ),
                    plan.edge_origin + "/dashboard/": HttpResult(
                        200,
                        {"content-type": "text/html; charset=utf-8"},
                    ),
                }
            ),
            sleeper=lambda _value: None,
        )

    probe_for(ready_body).require_preflight_ready(plan, target_projection_verified=True)
    with pytest.raises(RuntimeError, match="target-verified"):
        probe_for(ready_body).require_preflight_ready(plan, target_projection_verified=False)

    unsafe_bodies: list[dict[str, object]] = []
    for section, field, value in (
        ("database", "ok", False),
        ("authority", "mode", "legacy"),
        ("dispatcher", "healthy", False),
        ("projection", "violations", []),
    ):
        unsafe = json.loads(json.dumps(ready_body))
        assert isinstance(unsafe[section], dict)
        unsafe[section][field] = value
        unsafe_bodies.append(unsafe)
    for unsafe in unsafe_bodies:
        with pytest.raises(RuntimeError, match="target-verified"):
            probe_for(unsafe).require_preflight_ready(plan, target_projection_verified=True)


def test_codex_reload_uses_existing_bounded_websocket_client_and_exact_envelopes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    del tmp_path
    calls: list[object] = []

    class FakeRPC:
        def __init__(self) -> None:
            self.next_id = 1
            self.responses = [
                {
                    "id": 1,
                    "result": {
                        "userAgent": "codex-app-server-test/1",
                        "codexHome": "/owner/codex",
                        "platformFamily": "unix",
                        "platformOs": "macos",
                    },
                },
                {"id": 2, "result": {}},
            ]

        async def send(self, payload):
            calls.append(("send", payload))

        async def read_line(self, timeout):
            assert 0 < timeout <= 5.0
            calls.append("read")
            return self.responses.pop(0)

        async def close(self):
            calls.append("close")

    fake_rpc = FakeRPC()

    async def connect(path, *, timeout, trusted_mcp_servers=frozenset()):
        calls.append(("connect", path, timeout, trusted_mcp_servers))
        return fake_rpc

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", staticmethod(connect))
    with _owner_unix_socket() as socket_path:
        reload_codex_mcp_server(socket_path, "a" * 64)

    assert calls == [
        ("connect", socket_path, 5.0, frozenset()),
        (
            "send",
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "codex_app_server_daemon",
                        "version": "a" * 64,
                    },
                    "capabilities": {"experimentalApi": True},
                },
            },
        ),
        "read",
        ("send", {"method": "initialized", "params": {}}),
        ("send", {"id": 2, "method": "config/mcpServer/reload"}),
        "read",
        "close",
    ]


def test_codex_host_refresh_accepts_desktop_daemon_without_managed_install_and_reloads_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_root = tempfile.TemporaryDirectory(
        prefix="cao-r-",
        dir=Path(tempfile.gettempdir()).resolve(),
    )
    control = Path(control_root.name)
    control.chmod(0o700)
    socket_path = control / "app-server-control.sock"
    old_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old_listener.bind(os.fspath(socket_path))
    old_listener.listen(1)
    socket_path.chmod(0o600)
    old_identity = socket_path.stat()
    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    binding = _codex_desktop_binding(
        tmp_path / "owner-job",
        executable,
        monkeypatch,
    )
    peer_pid = 1201
    peer_asid = 2201
    host_inspector = _HealthyCodexHostInspector(
        binding,
        pid=peer_pid,
        asid=peer_asid,
    )
    commands: list[tuple[tuple[str, ...], float]] = []

    def run(argv: Sequence[str], timeout: float) -> CommandResult:
        command = tuple(argv)
        commands.append((command, timeout))
        if command[0] == "/usr/bin/codesign":
            return CommandResult(command, 0)
        if command[0] == "/bin/launchctl":
            return CommandResult(
                command,
                0,
                _codex_launchctl_output(binding, pid=peer_pid, asid=peer_asid),
            )
        if command[0] == "/usr/sbin/lsof":
            return CommandResult(
                command,
                0,
                _codex_lsof_output(peer_pid, executable),
            )
        return CommandResult(
            command,
            0,
            json.dumps(
                {
                    "status": "running",
                    "managedCodexPath": os.fspath(
                        socket_path.parent.parent / "packages" / "standalone" / "current" / "codex"
                    ),
                    "managedCodexVersion": None,
                    "socketPath": os.fspath(socket_path),
                    "cliVersion": "0.148.0-alpha.15",
                    "appServerVersion": "0.148.0-alpha.9",
                }
            ),
        )

    reloads: list[tuple[Path, str]] = []

    def reload(path: Path, release_id: str) -> None:
        current = path.stat()
        assert (current.st_dev, current.st_ino) == (
            old_identity.st_dev,
            old_identity.st_ino,
        )
        reloads.append((path, release_id))

    try:
        restart_status = refresh_codex_app_server_mcp(
            socket_path,
            "a" * 64,
            command_runner=run,
            reload_codex_mcp=reload,
            desktop_launchagent=binding,
            host_inspector=host_inspector,
        )
    finally:
        old_listener.close()
        socket_path.unlink(missing_ok=True)
        control_root.cleanup()

    assert commands[0][0][:5] == (
        "/usr/bin/codesign",
        "--verify",
        "--strict",
        "--test-requirement",
        '=identifier "codex" and anchor apple generic and certificate '
        'leaf[subject.OU] = "2DC432GLL2"',
    )
    assert commands[0][0][-1] == os.fspath(executable)
    assert commands[1][0] == (
        os.fspath(executable),
        "app-server",
        "daemon",
        "version",
    )
    assert all(timeout == 20.0 for _command, timeout in commands)
    assert not any(
        operation in command
        for command, _timeout in commands
        for operation in ("pkill", "restart", "bootstrap", "stop")
    )
    assert reloads == [(socket_path, "a" * 64)]
    assert restart_status == "not_performed"


def test_codex_host_refresh_rejects_missing_mapped_image_evidence_before_kickstart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    binding = _codex_desktop_binding(
        tmp_path / "owner-job",
        executable,
        monkeypatch,
    )
    peer_pid = 1251
    peer_asid = 2251
    host_inspector = _HealthyCodexHostInspector(
        binding,
        pid=peer_pid,
        asid=peer_asid,
    )
    commands: list[tuple[str, ...]] = []

    with _owner_unix_socket() as socket_path:
        monkeypatch.setattr(
            "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
            lambda: socket_path,
        )

        def run(argv: Sequence[str], timeout: float) -> CommandResult:
            command = tuple(argv)
            commands.append(command)
            assert timeout == 20.0
            if command[0] == "/usr/bin/codesign":
                return CommandResult(command, 0)
            if command[0] == "/bin/launchctl":
                return CommandResult(
                    command,
                    0,
                    _codex_launchctl_output(
                        binding,
                        pid=peer_pid,
                        asid=peer_asid,
                    ),
                )
            if command[0] == "/usr/sbin/lsof":
                return CommandResult(command, 0, f"p{peer_pid}\n")
            return CommandResult(
                command,
                0,
                json.dumps(
                    {
                        "status": "running",
                        "managedCodexPath": os.fspath(
                            socket_path.parent.parent
                            / "packages"
                            / "standalone"
                            / "current"
                            / "codex"
                        ),
                        "managedCodexVersion": None,
                        "socketPath": os.fspath(socket_path),
                        "cliVersion": "0.148.0-alpha.15",
                        "appServerVersion": "0.148.0-alpha.15",
                    }
                ),
            )

        with pytest.raises(
            RuntimeError,
            match="mapped image response is invalid",
        ):
            refresh_codex_app_server_mcp(
                socket_path,
                "a" * 64,
                command_runner=run,
                reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
                desktop_launchagent=binding,
                host_inspector=host_inspector,
            )

    assert not any(command[:3] == ("/bin/launchctl", "kickstart", "-kp") for command in commands)


def test_codex_host_refresh_replaces_one_exact_stale_desktop_generation_then_reloads_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temporary_root = (
        Path("/private/tmp")
        if Path("/private/tmp").is_dir()
        else Path(tempfile.gettempdir()).resolve()
    )
    control_root = tempfile.TemporaryDirectory(prefix="cao-k-", dir=temporary_root)
    control = Path(control_root.name)
    control.chmod(0o700)
    socket_path = control / "app.sock"
    retired_socket_path = control / "retired-app.sock"
    old_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old_listener.bind(os.fspath(socket_path))
    old_listener.listen(1)
    socket_path.chmod(0o600)
    old_socket_identity = (socket_path.stat().st_dev, socket_path.stat().st_ino)

    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-current-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    binding = _codex_desktop_binding(
        tmp_path / "owner-job",
        executable,
        monkeypatch,
    )
    old_pid = 1401
    new_pid = 1402
    asid = 2401
    old_process = DarwinAuditProcessIdentity(
        pid=old_pid,
        pgid=old_pid,
        uid=os.geteuid(),
        asid=asid,
        audit_token=(0, os.geteuid(), 0, os.getuid(), 0, old_pid, asid, 1),
    )
    new_process = DarwinAuditProcessIdentity(
        pid=new_pid,
        pgid=new_pid,
        uid=os.geteuid(),
        asid=asid,
        audit_token=(0, os.geteuid(), 0, os.getuid(), 0, new_pid, asid, 2),
    )
    old_bridge = DarwinAuditProcessIdentity(
        pid=1411,
        pgid=1411,
        uid=os.geteuid(),
        asid=asid,
        audit_token=(0, os.geteuid(), 0, os.getuid(), 0, 1411, asid, 3),
    )

    class Inspector:
        stage = "old"

        def inspect(self, path: Path) -> CodexAppServerPeerObservation:
            assert path == socket_path
            pid = old_pid if self.stage == "old" else new_pid
            return CodexAppServerPeerObservation(
                pid=pid,
                uid=os.geteuid(),
                start_signature=f"start-{pid}",
                executable_path=None,
                argv=binding.program_arguments,
            )

        def capture_process(
            self,
            pid: int,
            *,
            expected_asid: int,
        ) -> DarwinAuditProcessIdentity:
            assert expected_asid == asid
            if pid == old_pid:
                return old_process
            assert pid == new_pid
            return new_process

        def executable_path(self, process: DarwinAuditProcessIdentity) -> Path:
            if process == old_process:
                raise RuntimeError("old mapped image was deleted")
            assert process in {old_process, new_process}
            return executable

        def capture_cao_bridges(
            self,
            root_pid: int,
            *,
            expected_asid: int,
        ) -> tuple[DarwinAuditProcessIdentity, ...]:
            assert expected_asid == asid
            return (old_bridge,) if root_pid == old_pid else ()

        def is_live(self, process: DarwinAuditProcessIdentity) -> bool:
            if self.stage == "old":
                return process in {old_process, old_bridge}
            return process == new_process

    inspector = Inspector()
    replacement_listener: socket.socket | None = None
    commands: list[tuple[str, ...]] = []

    def old_lsof_output() -> str:
        info = executable.stat()
        return (
            f"p{old_pid}\n"
            "ftxt\n"
            f"D{hex(info.st_dev)}\n"
            f"i{info.st_ino + 1}\n"
            "n/private/tmp/deleted-codex-image\n"
        )

    def run(argv: Sequence[str], timeout: float) -> CommandResult:
        nonlocal replacement_listener
        command = tuple(argv)
        commands.append(command)
        assert timeout == 20.0
        if command[0] == "/usr/bin/codesign":
            return CommandResult(command, 0)
        if command[:3] == ("/bin/launchctl", "kickstart", "-kp"):
            assert inspector.stage == "old"
            old_listener.close()
            socket_path.replace(retired_socket_path)
            replacement_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            replacement_listener.bind(os.fspath(socket_path))
            replacement_listener.listen(1)
            socket_path.chmod(0o600)
            inspector.stage = "new"
            return CommandResult(command, 0, f"{new_pid}\n")
        if command[0] == "/bin/launchctl":
            pid = old_pid if inspector.stage == "old" else new_pid
            return CommandResult(
                command,
                0,
                _codex_launchctl_output(binding, pid=pid, asid=asid),
            )
        if command[0] == "/usr/sbin/lsof":
            return CommandResult(
                command,
                0,
                (
                    old_lsof_output()
                    if inspector.stage == "old"
                    else _codex_lsof_output(new_pid, executable)
                ),
            )
        return CommandResult(
            command,
            0,
            json.dumps(
                {
                    "status": "running",
                    "managedCodexPath": os.fspath(
                        socket_path.parent.parent / "packages" / "standalone" / "current" / "codex"
                    ),
                    "managedCodexVersion": None,
                    "socketPath": os.fspath(socket_path),
                    "cliVersion": "0.148.0-alpha.15",
                    "appServerVersion": "0.148.0-alpha.15",
                }
            ),
        )

    reloads: list[tuple[Path, str]] = []
    try:
        restart_status = refresh_codex_app_server_mcp(
            socket_path,
            "b" * 64,
            command_runner=run,
            reload_codex_mcp=lambda path, release_id: reloads.append((path, release_id)),
            desktop_launchagent=binding,
            host_inspector=inspector,
            sleeper=lambda _value: None,
            restart_attempts=2,
        )
        new_socket_identity = (socket_path.stat().st_dev, socket_path.stat().st_ino)
    finally:
        old_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)
        retired_socket_path.unlink(missing_ok=True)
        control_root.cleanup()

    assert restart_status == "performed"
    assert old_socket_identity != new_socket_identity
    assert reloads == [(socket_path, "b" * 64)]
    kickstarts = [
        command for command in commands if command[:3] == ("/bin/launchctl", "kickstart", "-kp")
    ]
    assert kickstarts == [
        (
            "/bin/launchctl",
            "kickstart",
            "-kp",
            f"gui/{os.getuid()}/{binding.label}",
        )
    ]
    assert not any("pkill" in command for command in commands)


def test_codex_host_preflight_accepts_signed_managed_pid_symlink_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temporary_root = (
        Path("/private/tmp")
        if Path("/private/tmp").is_dir()
        else Path(tempfile.gettempdir()).resolve()
    )
    control_root = tempfile.TemporaryDirectory(prefix="cao-m-", dir=temporary_root)
    root = Path(control_root.name)
    codex_home = root / "h"
    control = codex_home / "app-server-control"
    control.mkdir(parents=True)
    codex_home.chmod(0o700)
    control.chmod(0o700)
    socket_path = control / "app-server-control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.fspath(socket_path))
    listener.listen(1)
    socket_path.chmod(0o600)

    managed_version = "0.148.0-alpha.15"
    standalone = codex_home / "packages" / "standalone"
    release = standalone / "releases" / managed_version
    managed_executable = release / "bin" / "codex"
    managed_executable.parent.mkdir(parents=True)
    managed_executable.write_bytes(b"signed-managed-codex-test-double")
    managed_executable.chmod(0o755)
    (release / "codex").symlink_to("bin/codex")
    (standalone / "current").symlink_to(release)
    managed_path = standalone / "current" / "codex"

    bundled_executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    bundled_executable.parent.mkdir(parents=True)
    bundled_executable.write_bytes(b"signed-bundled-codex-test-double")
    bundled_executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: bundled_executable,
    )
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    commands: list[tuple[tuple[str, ...], float]] = []

    def run(argv: Sequence[str], timeout: float) -> CommandResult:
        command = tuple(argv)
        commands.append((command, timeout))
        if command[0] == "/usr/bin/codesign":
            return CommandResult(command, 0)
        if command == (os.fspath(managed_executable), "--version"):
            return CommandResult(
                command,
                0,
                stdout=f"codex-cli {managed_version}\n",
            )
        return CommandResult(
            command,
            0,
            json.dumps(
                {
                    "status": "running",
                    "backend": "pid",
                    "managedCodexPath": os.fspath(managed_path),
                    "managedCodexVersion": managed_version,
                    "socketPath": os.fspath(socket_path),
                    "cliVersion": managed_version,
                    "appServerVersion": "0.148.0-alpha.9",
                }
            ),
        )

    try:
        prepared = preflight_codex_app_server_mcp(
            socket_path,
            command_runner=run,
        )
        socket_info = socket_path.stat()
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)
        control_root.cleanup()

    assert prepared.backend == "managed_pid"
    assert prepared.socket_identity == (socket_info.st_dev, socket_info.st_ino)
    assert [
        Path(command[-1]) for command, _timeout in commands if command[0] == "/usr/bin/codesign"
    ] == [bundled_executable, managed_executable]
    assert [command for command, _timeout in commands if command[0] != "/usr/bin/codesign"] == [
        (
            os.fspath(bundled_executable),
            "app-server",
            "daemon",
            "version",
        ),
        (os.fspath(managed_executable), "--version"),
    ]
    assert all(timeout == 20.0 for _command, timeout in commands)


def test_codex_reload_rejects_socket_replacement_after_classified_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    temporary_root = Path(tempfile.gettempdir()).resolve()
    control_root = tempfile.TemporaryDirectory(prefix="cao-i-", dir=temporary_root)
    control = Path(control_root.name)
    control.chmod(0o700)
    socket_path = control / "app-server-control.sock"
    retired_socket_path = control / "retired-app-server-control.sock"
    bundled_executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    bundled_executable.parent.mkdir(parents=True)
    bundled_executable.write_bytes(b"signed-bundled-codex-test-double")
    bundled_executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: bundled_executable,
    )
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    binding = _codex_desktop_binding(
        tmp_path / "owner-job",
        bundled_executable,
        monkeypatch,
    )
    peer_pid = 1301
    peer_asid = 2301
    host_inspector = _HealthyCodexHostInspector(
        binding,
        pid=peer_pid,
        asid=peer_asid,
    )

    def run(argv: Sequence[str], timeout: float) -> CommandResult:
        command = tuple(argv)
        assert timeout == 20.0
        if command[0] == "/usr/bin/codesign":
            return CommandResult(command, 0)
        if command[0] == "/bin/launchctl":
            return CommandResult(
                command,
                0,
                _codex_launchctl_output(binding, pid=peer_pid, asid=peer_asid),
            )
        if command[0] == "/usr/sbin/lsof":
            return CommandResult(
                command,
                0,
                _codex_lsof_output(peer_pid, bundled_executable),
            )
        return CommandResult(
            command,
            0,
            json.dumps(
                {
                    "status": "running",
                    "managedCodexPath": os.fspath(
                        socket_path.parent.parent / "packages" / "standalone" / "current" / "codex"
                    ),
                    "managedCodexVersion": None,
                    "socketPath": os.fspath(socket_path),
                    "cliVersion": "0.148.0-alpha.15",
                    "appServerVersion": "0.148.0-alpha.9",
                }
            ),
        )

    old_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old_listener.bind(os.fspath(socket_path))
    old_listener.listen(1)
    socket_path.chmod(0o600)
    replacement_listener: socket.socket | None = None
    connect_calls: list[Path] = []

    async def connect(path, *, timeout, trusted_mcp_servers=frozenset()):
        del timeout, trusted_mcp_servers
        connect_calls.append(path)
        pytest.fail("socket replacement must be rejected before connect")

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", staticmethod(connect))
    try:
        prepared = preflight_codex_app_server_mcp(
            socket_path,
            command_runner=run,
            desktop_launchagent=binding,
            host_inspector=host_inspector,
        )
        old_listener.close()
        socket_path.replace(retired_socket_path)
        replacement_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        replacement_listener.bind(os.fspath(socket_path))
        replacement_listener.listen(1)
        socket_path.chmod(0o600)
        replacement = socket_path.stat()
        assert (replacement.st_dev, replacement.st_ino) != prepared.socket_identity

        with pytest.raises(RuntimeError, match="socket changed after daemon preflight"):
            reload_codex_mcp_server(
                socket_path,
                "a" * 64,
                expected_socket_identity=prepared.socket_identity,
            )
    finally:
        old_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)
        retired_socket_path.unlink(missing_ok=True)
        control_root.cleanup()

    assert connect_calls == []


def test_codex_host_preflight_rejects_broken_managed_daemon_before_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_root = tempfile.TemporaryDirectory(
        prefix="cao-r-",
        dir=Path(tempfile.gettempdir()).resolve(),
    )
    control = Path(control_root.name)
    control.chmod(0o700)
    socket_path = control / "app-server-control.sock"
    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"signed-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
        lambda: socket_path,
    )
    commands: list[tuple[str, ...]] = []

    def run(argv: Sequence[str], timeout: float) -> CommandResult:
        assert timeout == 20.0
        command = tuple(argv)
        commands.append(command)
        if command[0] == "/usr/bin/codesign":
            return CommandResult(command, 0)
        return CommandResult(
            command,
            0,
            json.dumps(
                {
                    "status": "running",
                    "backend": "pid",
                    "managedCodexPath": os.fspath(
                        socket_path.parent.parent / "packages" / "standalone" / "current" / "codex"
                    ),
                    "managedCodexVersion": None,
                    "socketPath": os.fspath(socket_path),
                    "cliVersion": "0.148.0-alpha.15",
                    "appServerVersion": "0.148.0-alpha.9",
                }
            ),
        )

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.fspath(socket_path))
    socket_path.chmod(0o600)
    try:
        with pytest.raises(RuntimeError, match="managed daemon identity is invalid"):
            refresh_codex_app_server_mcp(
                socket_path,
                "a" * 64,
                command_runner=run,
                reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
            )
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)
        control_root.cleanup()

    assert [command[1:] for command in commands[1:]] == [("app-server", "daemon", "version")]


def test_codex_host_refresh_rejects_unsigned_executable_before_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "ChatGPT.app" / "Contents" / "Resources" / "codex"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"unsigned-codex-test-double")
    executable.chmod(0o755)
    monkeypatch.setattr(
        "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_executable",
        lambda: executable,
    )
    commands: list[tuple[str, ...]] = []

    def reject_signature(argv: Sequence[str], timeout: float) -> CommandResult:
        assert timeout == 20.0
        command = tuple(argv)
        commands.append(command)
        return CommandResult(command, 1)

    with _owner_unix_socket() as socket_path:
        monkeypatch.setattr(
            "cao_control_plane.dashboard_lifecycle.canonical_codex_app_server_socket",
            lambda: socket_path,
        )
        with pytest.raises(RuntimeError, match="signature verification failed"):
            refresh_codex_app_server_mcp(
                socket_path,
                "a" * 64,
                command_runner=reject_signature,
                reload_codex_mcp=lambda *_args: pytest.fail("reload must not run"),
            )

    assert len(commands) == 1
    assert commands[0][0] == "/usr/bin/codesign"


@pytest.mark.parametrize(
    "initialize_result",
    [
        {},
        {
            "userAgent": "other-json-rpc-product/1",
            "codexHome": "/owner/other-product",
            "platformFamily": "unix",
            "platformOs": "macos",
        },
        {
            "userAgent": (
                "cao-dashboard-lifecycle/0.148.0-alpha.15 "
                "(Mac OS; arm64) unknown "
                "(cao-dashboard-lifecycle; prior-release)"
            ),
            "codexHome": "/owner/codex",
            "platformFamily": "unix",
            "platformOs": "macos",
        },
    ],
    ids=["empty", "other-product", "legacy-lifecycle-originator"],
)
def test_codex_reload_rejects_non_codex_initialize_before_sending_reload(
    monkeypatch: pytest.MonkeyPatch,
    initialize_result: dict[str, object],
) -> None:
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    sent: list[dict[str, object]] = []

    class FakeRPC:
        next_id = 1

        async def send(self, payload):
            sent.append(payload)

        async def read_line(self, timeout):
            assert 0 < timeout <= 5.0
            return {"id": 1, "result": initialize_result}

        async def close(self):
            return None

    async def connect(path, *, timeout, trusted_mcp_servers=frozenset()):
        del path, timeout, trusted_mcp_servers
        return FakeRPC()

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", staticmethod(connect))
    with (
        _owner_unix_socket() as socket_path,
        pytest.raises(RuntimeError, match="MCP reload preflight failed"),
    ):
        reload_codex_mcp_server(socket_path, "a" * 64)

    assert [message["method"] for message in sent] == ["initialize"]


def test_codex_reload_accepts_legacy_originator_only_after_attested_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cao_control_plane.dashboard_lifecycle import (
        CodexAppServerMCPPreflight,
        _reload_codex_mcp_server_after_preflight,
    )
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    sent: list[dict[str, object]] = []

    class FakeRPC:
        def __init__(self) -> None:
            self.next_id = 1
            self.responses = [
                {
                    "id": 1,
                    "result": {
                        "userAgent": (
                            "cao-dashboard-lifecycle/0.148.0-alpha.15 "
                            "(Mac OS; arm64) unknown "
                            "(cao-dashboard-lifecycle; prior-release)"
                        ),
                        "codexHome": "/owner/codex",
                        "platformFamily": "unix",
                        "platformOs": "macos",
                    },
                },
                {"id": 2, "result": {}},
            ]

        async def send(self, payload):
            sent.append(payload)

        async def read_line(self, timeout):
            assert 0 < timeout <= 5.0
            return self.responses.pop(0)

        async def close(self):
            return None

    async def connect(path, *, timeout, trusted_mcp_servers=frozenset()):
        del path, timeout, trusted_mcp_servers
        return FakeRPC()

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", staticmethod(connect))
    with _owner_unix_socket() as socket_path:
        socket_info = socket_path.stat()
        _reload_codex_mcp_server_after_preflight(
            socket_path,
            "a" * 64,
            CodexAppServerMCPPreflight(
                backend="desktop_owner_local",
                socket_device=socket_info.st_dev,
                socket_inode=socket_info.st_ino,
                bundled_executable_identity=(1, 2, 3, 4, 5),
            ),
        )

    assert [message["method"] for message in sent] == [
        "initialize",
        "initialized",
        "config/mcpServer/reload",
    ]
    assert sent[0]["params"] == {
        "clientInfo": {
            "name": "codex_app_server_daemon",
            "version": "a" * 64,
        },
        "capabilities": {"experimentalApi": True},
    }


def test_codex_reload_eof_after_request_is_unknown_and_never_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    del tmp_path
    reload_calls = 0

    class FakeRPC:
        next_id = 1
        reads = 0

        async def send(self, payload):
            nonlocal reload_calls
            if payload.get("method") == "config/mcpServer/reload":
                reload_calls += 1

        async def read_line(self, timeout):
            assert 0 < timeout <= 5.0
            self.reads += 1
            if self.reads == 1:
                return {
                    "id": 1,
                    "result": {
                        "userAgent": "codex-app-server-test/1",
                        "codexHome": "/owner/codex",
                        "platformFamily": "unix",
                        "platformOs": "macos",
                    },
                }
            raise EOFError

        async def close(self):
            return None

    async def connect(path, *, timeout, trusted_mcp_servers=frozenset()):
        del path, timeout, trusted_mcp_servers
        return FakeRPC()

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", staticmethod(connect))
    with (
        _owner_unix_socket() as socket_path,
        pytest.raises(LifecycleUnknownOutcome, match="outcome is unknown"),
    ):
        reload_codex_mcp_server(socket_path, "a" * 64)

    assert reload_calls == 1
