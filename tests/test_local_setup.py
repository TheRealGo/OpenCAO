from __future__ import annotations

import json
import os
import socket
import stat
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from cao_control_plane.config import Settings
from cao_control_plane.local_setup import (
    local_dashboard_link,
    local_service_commands,
    setup_local,
)


def _ports() -> tuple[int, int]:
    with socket.socket() as first, socket.socket() as second:
        first.bind(("127.0.0.1", 0))
        second.bind(("127.0.0.1", 0))
        return first.getsockname()[1], second.getsockname()[1]


def test_setup_uses_private_files_absolute_mcp_command_and_preserves_existing_state(tmp_path):
    config = tmp_path / "config.toml"
    state = tmp_path / "state"
    setup_local(config_path=config, state_dir=state)
    settings = Settings.load(config)
    snippet = tomllib.loads((state / "codex-mcp.toml").read_text())
    server = snippet["mcp_servers"]["cao_control_plane"]
    assert Path(server["command"]).is_absolute()
    assert "mcp-stdio" in server["args"]
    assert str(config) in server["args"]
    assert "token" not in json.dumps(server).lower()
    assert settings.require_cao_attachment_for_work
    assert settings.enable_dashboard
    assert settings.runtime_launch_dir == state / "runtime-launches"
    private_files = [config, state / "codex-mcp.toml", state / "dashboard-credentials.json"]
    before = {path: path.read_bytes() for path in private_files}
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in private_files)
    with pytest.raises(ValueError, match="already exists"):
        setup_local(config_path=config, state_dir=state)
    assert before == {path: path.read_bytes() for path in private_files}
    commands = local_service_commands(settings, config)
    bearer = json.loads((state / "dashboard-credentials.json").read_text())["dashboard_bearer"]
    assert bearer not in json.dumps(commands)


def test_setup_rejects_unsafe_directory_and_port_conflict_without_creating_state(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="owner-only"):
        setup_local(config_path=parent / "config.toml", state_dir=tmp_path / "state")
    assert not (tmp_path / "state").exists()
    with pytest.raises(ValueError, match="distinct"):
        setup_local(config_path=tmp_path / "config.toml", state_dir=tmp_path / "state", port=8769)
    assert not (tmp_path / "state").exists()


@pytest.mark.parametrize("config_source", ["argument", "environment"])
def test_documented_local_launcher_serves_authenticated_dashboard_and_stops_both(
    tmp_path, config_source
):
    cp_port, edge_port = _ports()
    config = tmp_path / "config.toml"
    setup_local(
        config_path=config, state_dir=tmp_path / "state", port=cp_port, dashboard_port=edge_port
    )
    settings = Settings.load(config)
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CAO_", "CODEX_THREAD"))
    }
    command = [sys.executable, "-m", "cao_control_plane.cli"]
    if config_source == "environment":
        environment["CAO_A2A_CONFIG"] = str(config)
    else:
        command.extend(["--config", str(config)])
    command.append("run-local")
    with (tmp_path / "launcher.log").open("w") as log:
        process = subprocess.Popen(
            command,
            env=environment,
            stdout=log,
            stderr=log,
        )
        try:
            edge = f"http://127.0.0.1:{edge_port}"
            with httpx.Client(timeout=1, trust_env=False) as client:
                deadline = time.monotonic() + 20
                while True:
                    assert process.poll() is None, "local launcher exited"
                    try:
                        ready = client.get(f"http://127.0.0.1:{cp_port}/ready")
                        page = client.get(edge + "/dashboard/")
                        if ready.status_code == page.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    assert time.monotonic() < deadline, "local startup timed out"
                    time.sleep(0.05)
                assert client.get(edge + "/dashboard/api/snapshot").status_code == 401
                link = local_dashboard_link(settings)
                secret = parse_qs(urlparse(link).fragment)["dashboard_bootstrap"][0]
                assert (
                    client.post(edge + "/dashboard/session", json={"secret": secret}).status_code
                    == 204
                )
                assert client.get(edge + "/dashboard/api/snapshot").status_code == 200
                assert client.get(edge + "/dashboard/api/history").status_code == 200
                assert (
                    client.post(edge + "/dashboard/session", json={"secret": secret}).status_code
                    == 401
                )
        finally:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        assert process.returncode == 0
        for port in (cp_port, edge_port):
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", port))
