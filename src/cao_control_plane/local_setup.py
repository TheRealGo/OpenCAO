"""First-run configuration and foreground services for a private local install."""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from .config import Settings
from .dashboard_credentials import load_dashboard_credentials
from .dashboard_edge import issue_dashboard_bootstrap_url
from .database import Database
from .models import PrincipalCreate, PrincipalRole
from .service import ControlPlane


def default_config_path() -> Path:
    return Path(
        os.environ.get("CAO_A2A_CONFIG", str(Path.home() / ".config" / "cao-a2a" / "config.toml"))
    ).expanduser()


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if (
        path.resolve() != path
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("configuration directory must be canonical and owner-only (0700)")


def _write_new_private_file(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def setup_local(
    *, config_path: Path, state_dir: Path, port: int = 8768, dashboard_port: int = 8769
) -> dict[str, Any]:
    """Create a new installation without adopting or overwriting existing state."""

    config = config_path.expanduser().absolute()
    state = state_dir.expanduser().absolute()
    if config.exists() or config.is_symlink():
        raise ValueError("configuration already exists; setup never overwrites an installation")
    if state.exists() or state.is_symlink():
        raise ValueError("state directory already exists; choose a new directory")
    if not 1 <= port <= 65535 or not 1 <= dashboard_port <= 65535 or port == dashboard_port:
        raise ValueError("Control Plane and Dashboard require two distinct valid ports")
    if state in config.parents or config == state:
        raise ValueError("configuration must be outside the state directory")
    _private_directory(config.parent)
    state.parent.mkdir(parents=True, exist_ok=True)
    if state.parent.resolve() != state.parent:
        raise ValueError("state directory parent must be canonical")
    cp_origin = f"http://127.0.0.1:{port}"
    edge_origin = f"http://127.0.0.1:{dashboard_port}"
    staging = Path(tempfile.mkdtemp(prefix=".cao-setup-", dir=state.parent))
    installed = False
    try:
        temporary = Settings(state_dir=staging, runtime_launch_dir=staging / "runtime-launches")
        service = ControlPlane(Database(temporary), temporary)
        bootstrap = service.bootstrap()
        actor = service.authenticate(bootstrap["tokens"]["cao"]["token"])
        dashboard = service.create_principal(
            actor, PrincipalCreate(name="local-dashboard", role=PrincipalRole.DASHBOARD)
        )
        for directory in ("dashboard-bootstrap", "dashboard-sessions"):
            (staging / directory).mkdir(mode=0o700)
        _write_new_private_file(
            staging / "dashboard-credentials.json",
            json.dumps(
                {
                    "upstream_base_url": cp_origin,
                    "dashboard_bearer": dashboard["token"],
                    "public_origin": edge_origin,
                },
                indent=2,
            )
            + "\n",
        )
        python = str(Path(sys.executable).absolute())
        arguments = ["-m", "cao_control_plane.cli", "--config", str(config), "mcp-stdio"]
        _write_new_private_file(
            staging / "codex-mcp.toml",
            "[mcp_servers.cao_control_plane]\n"
            f"command = {json.dumps(python)}\n"
            f"args = {json.dumps(arguments)}\n"
            'env_vars = ["CODEX_THREAD_ID"]\n',
        )
        content = (
            "[server]\n"
            f"state_dir = {json.dumps(str(state))}\n"
            'host = "127.0.0.1"\n'
            f"port = {port}\npublic_base_url = {json.dumps(cp_origin)}\n"
            "enable_dashboard = true\n\n"
            "[runtime]\n"
            f"runtime_launch_dir = {json.dumps(str(state / 'runtime-launches'))}\n"
            "require_cao_attachment_for_work = true\n\n"
            "[dashboard]\n"
            f"dashboard_credentials_file = {json.dumps(str(state / 'dashboard-credentials.json'))}\n"
            f"dashboard_bootstrap_record_dir = {json.dumps(str(state / 'dashboard-bootstrap'))}\n"
            f"dashboard_session_record_dir = {json.dumps(str(state / 'dashboard-sessions'))}\n"
            f"dashboard_edge_base_url = {json.dumps(edge_origin)}\n"
        )
        # mkdir reserves the destination without replacing a concurrent directory.
        state.mkdir(mode=0o700)
        installed = True
        for source in staging.iterdir():
            source.rename(state / source.name)
        _write_new_private_file(config, content)
        return {
            "status": "created",
            "config_file": str(config),
            "state_directory": str(state),
            "codex_mcp_example": str(state / "codex-mcp.toml"),
            "dashboard_origin": edge_origin,
            "next_command": [
                python,
                "-m",
                "cao_control_plane.cli",
                "--config",
                str(config),
                "run-local",
            ],
        }
    except BaseException:
        # Only directories created by this invocation are eligible for rollback.
        if installed and not config.exists():
            shutil.rmtree(state)
        raise
    finally:
        shutil.rmtree(staging)


def local_dashboard_link(settings: Settings) -> str:
    credentials_path = settings.dashboard_credentials_file
    records = settings.dashboard_bootstrap_record_dir
    if credentials_path is None or records is None:
        raise ValueError("run setup first to configure the local Dashboard")
    credentials = load_dashboard_credentials(credentials_path)
    if credentials.public_origin != settings.dashboard_edge_base_url:
        raise ValueError("dashboard-link requires the configured local Dashboard origin")
    return issue_dashboard_bootstrap_url(records, settings.dashboard_edge_base_url)


def local_service_commands(settings: Settings, config_path: Path) -> list[list[str]]:
    if not settings.enable_dashboard or settings.host != "127.0.0.1":
        raise ValueError("run-local requires an enabled Dashboard and loopback Control Plane")
    paths = (
        settings.dashboard_credentials_file,
        settings.dashboard_bootstrap_record_dir,
        settings.dashboard_session_record_dir,
    )
    if any(path is None for path in paths):
        raise ValueError("run setup first to configure the local Dashboard")
    edge = urlparse(settings.dashboard_edge_base_url)
    if edge.scheme != "http" or edge.hostname != "127.0.0.1" or edge.port == settings.port:
        raise ValueError("run-local requires two distinct loopback listeners")
    assert settings.dashboard_credentials_file is not None
    credentials = load_dashboard_credentials(settings.dashboard_credentials_file)
    if (
        credentials.upstream_base_url != settings.public_base_url
        or credentials.public_origin != settings.dashboard_edge_base_url
        or credentials.cloudflare_access is not None
    ):
        raise ValueError("run-local requires matching local Dashboard credentials")
    python = str(Path(sys.executable).absolute())
    return [
        [python, "-m", "cao_control_plane.cli", "--config", str(config_path), "serve"],
        [
            python,
            "-m",
            "cao_control_plane.dashboard_cli",
            "serve",
            "--credentials-file",
            str(paths[0]),
            "--bootstrap-record-dir",
            str(paths[1]),
            "--session-record-dir",
            str(paths[2]),
            "--host",
            "127.0.0.1",
            "--port",
            str(edge.port or 80),
        ],
    ]


def run_local(settings: Settings, config_path: Path) -> int:
    """Own two foreground children; never signal, reuse or restart another service."""

    commands = local_service_commands(settings, config_path)
    edge_port = urlparse(settings.dashboard_edge_base_url).port or 80
    reservations: list[socket.socket] = []
    try:
        for port in (settings.port, edge_port):
            listener = socket.socket()
            reservations.append(listener)
            listener.bind(("127.0.0.1", port))
    finally:
        for listener in reservations:
            listener.close()
    processes: list[subprocess.Popen[bytes]] = []

    def stop(_signal: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, stop)
    try:
        for command in commands:
            processes.append(subprocess.Popen(command))
        pending = {
            f"{settings.public_base_url}/ready",
            f"{settings.dashboard_edge_base_url}/dashboard/",
        }
        deadline = time.monotonic() + 30
        with httpx.Client(timeout=1, trust_env=False) as client:
            while pending:
                if any(process.poll() is not None for process in processes):
                    raise RuntimeError("a local service failed to start")
                if time.monotonic() >= deadline:
                    raise RuntimeError("local service readiness timed out")
                for url in tuple(pending):
                    try:
                        response = client.get(url)
                    except httpx.HTTPError:
                        continue
                    if response.status_code == 200:
                        pending.remove(url)
                if pending:
                    time.sleep(0.1)
        print(
            "CAO and Dashboard are ready. Use dashboard-link to sign in. Ctrl+C stops both.",
            flush=True,
        )
        while all(process.poll() is None for process in processes):
            time.sleep(0.2)
        raise RuntimeError("a local service exited; both services have been stopped")
    except KeyboardInterrupt:
        return 0
    finally:
        signal.signal(signal.SIGTERM, previous)
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
