from __future__ import annotations

import asyncio
import json
import os
import socket
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

from cao_control_plane.runtime import (
    ClaudeAdapter,
    CodexAppServerAdapter,
    RuntimeAdapterError,
    RuntimeDispatchPhaseError,
    _codex_delivery_client_user_message_id,
    _codex_thread_mcp_config,
    _default_claude_command,
    _default_codex_app_server_command,
    _enrollment_mcp_config,
    _JsonRpcProcess,
    _managed_codex_mcp_server_name,
    _managed_codex_notification_matches_turn,
    _runtime_failure_code,
    _safe_env,
    _sanitize_runtime_value,
)
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    EnrollmentCapabilityError,
    receive_enrollment_capability,
)

SENTINEL = "enrollment-secret-must-not-cross-the-launch-boundary"
ENDPOINT = "http://127.0.0.1:8768/mcp"


def test_managed_codex_mcp_name_survives_connection_epoch_replacement(tmp_path: Path) -> None:
    first = _runtime(tmp_path / "first.sock")
    second = _runtime(tmp_path / "second.sock")
    first["id"] = "run-first-epoch"
    second["id"] = "run-second-epoch"
    first["managed_worker_spec"] = {"id": "mws-stable-worker"}
    second["managed_worker_spec"] = {"id": "mws-stable-worker"}

    assert _managed_codex_mcp_server_name(first) == _managed_codex_mcp_server_name(second)
    second["managed_worker_spec"] = {"id": "mws-other-worker"}
    assert _managed_codex_mcp_server_name(first) != _managed_codex_mcp_server_name(second)


def test_managed_codex_activity_accepts_only_the_exact_thread_and_turn() -> None:
    exact = {
        "method": "item/commandExecution/outputDelta",
        "params": {
            "threadId": "native-thread",
            "turnId": "active-turn",
            "delta": "untrusted output is ignored by the liveness binder",
        },
    }
    assert _managed_codex_notification_matches_turn(
        exact,
        native_thread_id="native-thread",
        turn_id="active-turn",
    )
    assert not _managed_codex_notification_matches_turn(
        exact,
        native_thread_id="other-thread",
        turn_id="active-turn",
    )
    assert not _managed_codex_notification_matches_turn(
        exact,
        native_thread_id="native-thread",
        turn_id="other-turn",
    )
    assert not _managed_codex_notification_matches_turn(
        {
            "method": "mcpServer/startupStatus/updated",
            "params": {"threadId": "native-thread", "turnId": "active-turn"},
        },
        native_thread_id="native-thread",
        turn_id="active-turn",
    )


@pytest.mark.parametrize("path_cli", [None, "/older-cli/codex"])
@pytest.mark.parametrize("bundle_name", ["Codex.app", "ChatGPT.app"])
def test_default_codex_command_uses_the_desktop_release(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, settings, path_cli: str | None, bundle_name: str
) -> None:
    bundled = tmp_path / bundle_name / "Contents" / "Resources" / "codex"
    bundled.parent.mkdir(parents=True)
    bundled.write_text("", encoding="utf-8")
    bundled.chmod(0o700)
    monkeypatch.setattr("cao_control_plane.runtime.shutil.which", lambda _name: path_cli)
    monkeypatch.setattr(
        "cao_control_plane.runtime._BUNDLED_CODEX_EXECUTABLES", (bundled,)
    )

    expected = [
        str(bundled),
        "app-server",
        "--stdio",
    ]
    assert _default_codex_app_server_command() == expected
    from cao_control_plane.dashboard_lifecycle import canonical_codex_app_server_executable

    assert canonical_codex_app_server_executable() == bundled
    assert CodexAppServerAdapter(settings).prepare_launch({"metadata": {}})[
        "_resolved_codex_command"
    ] == expected


def test_default_claude_command_uses_native_install_when_daemon_path_is_minimal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    native = tmp_path / ".local" / "bin" / "claude"
    native.parent.mkdir(parents=True)
    native.write_text("", encoding="utf-8")
    native.chmod(0o700)
    monkeypatch.setattr("cao_control_plane.runtime.shutil.which", lambda _name: None)
    monkeypatch.setattr(
        "cao_control_plane.runtime._NATIVE_CLAUDE_EXECUTABLES", (native,)
    )

    assert _default_claude_command() == [str(native)]


def test_default_claude_command_fails_closed_without_executable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "private-owner-path" / "claude"
    monkeypatch.setattr("cao_control_plane.runtime.shutil.which", lambda _name: None)
    monkeypatch.setattr(
        "cao_control_plane.runtime._NATIVE_CLAUDE_EXECUTABLES", (missing,)
    )

    with pytest.raises(RuntimeAdapterError) as error:
        _default_claude_command()

    assert str(error.value) == "Claude runtime executable is unavailable"
    assert str(missing) not in str(error.value)


def test_prepared_default_commands_resolve_exactly_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, settings
) -> None:
    calls = {"claude": 0, "codex": 0}

    def claude_command() -> list[str]:
        calls["claude"] += 1
        if calls["claude"] > 1:
            raise RuntimeAdapterError("Claude resolver must not run twice")
        return [str(tmp_path / "claude")]

    def codex_command() -> list[str]:
        calls["codex"] += 1
        if calls["codex"] > 1:
            raise RuntimeAdapterError("Codex resolver must not run twice")
        return [str(tmp_path / "codex"), "app-server", "--stdio"]

    monkeypatch.setattr("cao_control_plane.runtime._default_claude_command", claude_command)
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_codex_app_server_command", codex_command
    )
    launch: dict[str, Any] = {"metadata": {}}

    claude = ClaudeAdapter(settings)
    prepared_claude = claude.prepare_launch(launch)
    assert claude.prepare_launch(prepared_claude) is prepared_claude
    assert prepared_claude["_resolved_claude_command"] == [str(tmp_path / "claude")]

    codex = CodexAppServerAdapter(settings)
    prepared_codex = codex.prepare_launch(launch)
    assert codex.prepare_launch(prepared_codex) is prepared_codex
    assert prepared_codex["_resolved_codex_command"] == [
        str(tmp_path / "codex"),
        "app-server",
        "--stdio",
    ]
    assert calls == {"claude": 1, "codex": 1}


def test_native_claude_version_smoke_with_launchagent_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the installed native executable without starting a model turn."""

    native = Path.home() / ".local" / "bin" / "claude"
    if not native.is_file() or not os.access(native, os.X_OK):
        pytest.skip("native Claude Code is not installed")
    daemon_path = "/usr/bin:/bin:/usr/sbin:/sbin"
    monkeypatch.setenv("PATH", daemon_path)

    command = _default_claude_command()
    result = subprocess.run(
        [*command, "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env={"HOME": str(Path.home()), "PATH": daemon_path},
    )

    assert Path(command[0]).is_absolute()
    assert result.returncode == 0
    assert "Claude Code" in result.stdout


def test_process_bound_broker_exchanges_once_and_redelivers_within_launch_tree(tmp_path: Path):
    async def exercise() -> None:
        exchanges: list[str] = []
        broker = EnrollmentCapabilityBroker(
            configured_root=tmp_path / "launch",
            ticket_id="ent_authorized",
            raw_ticket=SENTINEL,
            exchange=lambda ticket: exchanges.append(ticket)
            or {
                "token": "runtime-bearer",
                "runtime_id": "run_authorized",
                "generation": 1,
                "heartbeat_lease_seconds": 60,
            },
            delivery_failed=lambda _reason: pytest.fail("credential delivery failed"),
        )
        await broker.start()
        broker.bind_runner_pid(os.getpid())
        assert stat.S_IMODE(broker.path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(broker.path.stat().st_mode) == 0o600
        result = await receive_enrollment_capability(broker.path, timeout_seconds=2)
        assert result["runtime_id"] == "run_authorized"
        assert exchanges == [SENTINEL]
        retry = await receive_enrollment_capability(broker.path, timeout_seconds=2)
        assert retry == result
        assert exchanges == [SENTINEL]
        socket_path = broker.path
        await broker.close()
        assert not socket_path.exists()

    asyncio.run(exercise())


def test_broker_zeroizes_cached_credential_when_delivery_drain_fails(tmp_path: Path):
    class FailingWriter:
        def __init__(self, peer_socket: socket.socket) -> None:
            self.peer_socket = peer_socket
            self.written = b""

        def get_extra_info(self, name: str):
            return self.peer_socket if name == "socket" else None

        def write(self, value: bytes) -> None:
            self.written += value

        async def drain(self) -> None:
            raise OSError("simulated closed bridge")

        def close(self) -> None:
            return None

        async def wait_closed(self) -> None:
            return None

    async def exercise() -> None:
        failures: list[str] = []
        broker = EnrollmentCapabilityBroker(
            configured_root=tmp_path / "launch",
            ticket_id="ent_delivery_failure",
            raw_ticket=SENTINEL,
            exchange=lambda _ticket: {"token": "runtime-bearer"},
            delivery_failed=failures.append,
        )
        left, right = socket.socketpair(socket.AF_UNIX)
        try:
            broker.bind_runner_pid(os.getpid())
            await broker._handle_peer(asyncio.StreamReader(), FailingWriter(left))
            assert not broker._credential_payload
            assert failures == ["managed enrollment credential delivery failed"]
        finally:
            left.close()
            right.close()
            await broker.close()

    asyncio.run(exercise())


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS /tmp alias contract")
def test_macos_tmp_alias_uses_the_canonical_private_tmp_root():
    async def exercise() -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            relative = Path(temporary).relative_to("/private/tmp")
            configured = Path("/tmp") / relative / "launch"
            broker = EnrollmentCapabilityBroker(
                configured_root=configured,
                ticket_id="ent_tmp_alias",
                raw_ticket=SENTINEL,
                exchange=lambda _ticket: {"token": "runtime-bearer"},
                delivery_failed=lambda _reason: pytest.fail(
                    "credential delivery failed"
                ),
            )
            await broker.start()
            try:
                assert str(broker.path).startswith("/private/tmp/")
                assert broker.path.exists()
            finally:
                await broker.close()

    asyncio.run(exercise())


def test_same_uid_sibling_cannot_consume_bound_launch_capability(tmp_path: Path):
    async def exercise() -> None:
        exchanges: list[str] = []
        runner_script = (
            "import socket,sys; "
            "\nfor path in sys.stdin:\n"
            " client=socket.socket(socket.AF_UNIX); client.connect(path.strip()); data=b''\n"
            " while not data.endswith(b'\\n'): data += client.recv(16384)\n"
            " client.close(); sys.stdout.buffer.write(data); sys.stdout.buffer.flush()"
        )
        runner = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            runner_script,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        broker = EnrollmentCapabilityBroker(
            configured_root=tmp_path / "launch",
            ticket_id="ent_isolated",
            raw_ticket=SENTINEL,
            exchange=lambda ticket: exchanges.append(ticket)
            or {
                "token": "runtime-bearer",
                "runtime_id": "run_isolated",
                "generation": 2,
                "heartbeat_lease_seconds": 60,
            },
            delivery_failed=lambda _reason: pytest.fail("credential delivery failed"),
        )
        try:
            await broker.start()
            broker.bind_runner_pid(runner.pid)
            with pytest.raises(EnrollmentCapabilityError):
                await receive_enrollment_capability(broker.path, timeout_seconds=2)
            assert runner.stdin is not None
            assert runner.stdout is not None
            runner.stdin.write((str(broker.path) + "\n").encode())
            await runner.stdin.drain()
            response = json.loads(await asyncio.wait_for(runner.stdout.readline(), timeout=3))
            assert response["runtime_id"] == "run_isolated"
            assert response["token"] == "runtime-bearer"
            assert exchanges == [SENTINEL]
            # The sibling remains unauthorized after the broker has cached the
            # post-exchange credential; it cannot consume or alter that cache.
            with pytest.raises(EnrollmentCapabilityError):
                await receive_enrollment_capability(broker.path, timeout_seconds=2)
            assert exchanges == [SENTINEL]
            runner.stdin.write((str(broker.path) + "\n").encode())
            await runner.stdin.drain()
            retry = json.loads(await asyncio.wait_for(runner.stdout.readline(), timeout=3))
            assert retry == response
            assert exchanges == [SENTINEL]
        finally:
            await broker.close()
            if runner.returncode is None:
                runner.terminate()
            await runner.wait()

    asyncio.run(exercise())


def test_enrollment_mcp_config_contains_socket_but_never_ticket_contents(tmp_path: Path):
    socket_path = tmp_path / "broker.sock"
    config = _enrollment_mcp_config(
        {"endpoint": ENDPOINT, "enrollment_capability_socket": str(socket_path)}
    )

    assert config == {
        "mcpServers": {
            "cao_control_plane": {
                "command": sys.executable,
                "args": [
                    "-m",
                    "cao_control_plane.cli",
                    "mcp-stdio",
                    "--url",
                    ENDPOINT,
                    "--enrollment-broker-socket",
                    str(socket_path),
                ],
            }
        }
    }
    rendered = json.dumps(config)
    assert SENTINEL not in rendered
    assert SENTINEL not in json.dumps(_codex_thread_mcp_config(config))
    with pytest.raises(RuntimeAdapterError):
        _enrollment_mcp_config(
            {"endpoint": ENDPOINT, "enrollment_capability_socket": "bad\x00path"}
        )


def test_codex_thread_mcp_config_allows_only_host_bound_nonsecret_environment() -> None:
    config = {
        "mcpServers": {
            "cao_control_plane": {
                "command": sys.executable,
                "args": ["-m", "cao_control_plane.cli", "mcp-stdio"],
                "env_vars": ["CODEX_THREAD_ID", "CAO_A2A_STATE_DIR"],
            }
        }
    }
    rendered = _codex_thread_mcp_config(config)
    assert set(rendered) == {"mcp_servers.cao_control_plane"}
    assert rendered["mcp_servers.cao_control_plane"]["env_vars"] == [
        "CODEX_THREAD_ID",
        "CAO_A2A_STATE_DIR",
    ]
    assert "native-thread" not in json.dumps(rendered)
    assert "/private/state" not in json.dumps(rendered)
    config["mcpServers"]["cao_control_plane"]["env_vars"] = ["CAO_A2A_TOKEN"]
    with pytest.raises(RuntimeAdapterError):
        _codex_thread_mcp_config(config)


class _Stdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, value: bytes) -> None:
        self.writes.append(value)

    async def drain(self) -> None:
        return None


class _CodexProcess:
    def __init__(self, server_name: str = "cao_control_plane") -> None:
        self.pid = os.getpid()
        self.stdin = _Stdin()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode = 0
        client_id = _codex_delivery_client_user_message_id(
            {"kind": "instruction", "payload": {"message": "go"}}
        )
        for payload in (
            {"id": 1, "result": {}},
            {"id": 2, "result": {"thread": {"id": "thread-1"}}},
            {
                "method": "mcpServer/startupStatus/updated",
                "params": {
                    "threadId": "thread-1",
                    "name": server_name,
                    "status": "ready",
                },
            },
            {
                "id": 3,
                "result": {
                    "queuedSubmission": {
                        "id": "queued-1",
                        "clientUserMessageId": client_id,
                        "input": [],
                    }
                },
            },
            {
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "startedAtMs": 1,
                    "item": {
                        "id": "user-message-1",
                        "type": "userMessage",
                        "clientId": client_id,
                        "content": [],
                    },
                },
            },
            {"method": "turn/completed", "params": {"turn": {"id": "turn-1", "status": "completed"}}},
        ):
            self.stdout.feed_data((json.dumps(payload) + "\n").encode())
        self.stdout.feed_eof()


class _RpcOnlyProcess:
    def __init__(self) -> None:
        self.stdin = _Stdin()


class _CodexNoMcpReadyProcess:
    def __init__(self) -> None:
        self.pid = os.getpid()
        self.stdin = _Stdin()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode = 0
        for payload in (
            {"id": 1, "result": {}},
            {"id": 2, "result": {"thread": {"id": "thread-before-submit"}}},
        ):
            self.stdout.feed_data((json.dumps(payload) + "\n").encode())
        self.stdout.feed_eof()


def test_codex_mcp_startup_failure_is_bounded_as_not_submitted(
    monkeypatch: pytest.MonkeyPatch, settings, tmp_path: Path
) -> None:
    async def spawn(*_argv: str, **_kwargs: Any) -> _CodexNoMcpReadyProcess:
        return _CodexNoMcpReadyProcess()

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_codex_app_server_command",
        lambda: [sys.executable, "-c", ""],
    )
    runtime = _runtime(tmp_path / "not-ready.sock")
    with pytest.raises(RuntimeDispatchPhaseError) as failed:
        asyncio.run(
            CodexAppServerAdapter(settings).dispatch(
                runtime,
                {"kind": "instruction", "payload": {"message": "go"}},
            )
        )

    assert failed.value.metadata == {
        "delivery_acceptance": "not_submitted",
        "dispatch_phase": "mcp_startup",
    }


def test_codex_json_rpc_reader_accepts_one_bounded_message_over_stream_limit() -> None:
    async def exercise() -> dict[str, Any]:
        process = _RpcOnlyProcess()
        process.stdout = asyncio.StreamReader(limit=64)  # type: ignore[attr-defined]
        payload = {"id": 1, "result": {"thread": {"rollout": "x" * 512}}}
        process.stdout.feed_data((json.dumps(payload) + "\n").encode())  # type: ignore[attr-defined]
        rpc = _JsonRpcProcess(process, max_message_bytes=1024)  # type: ignore[arg-type]
        return await rpc.read_until_response(1, timeout=0.5)

    assert asyncio.run(exercise()) == {"thread": {"rollout": "x" * 512}}


@pytest.mark.parametrize(
    ("method", "expected_result"),
    [
        ("item/commandExecution/requestApproval", {"decision": "decline"}),
        ("item/fileChange/requestApproval", {"decision": "decline"}),
        (
            "applyPatchApproval",
            {
                "decision": {
                    "denied": {
                        "rejection": (
                            "The control-plane runtime adapter does not infer effect authority."
                        )
                    }
                }
            },
        ),
        (
            "item/permissions/requestApproval",
            {
                "permissions": {
                    "fileSystem": {"entries": []},
                    "network": {"enabled": False},
                },
                "scope": "turn",
                "strictAutoReview": False,
            },
        ),
        ("item/tool/requestUserInput", {"answers": {}}),
    ],
)
def test_codex_app_server_requests_receive_fail_closed_protocol_responses(
    method: str,
    expected_result: dict[str, Any],
):
    process = _RpcOnlyProcess()
    rpc = _JsonRpcProcess(process)  # type: ignore[arg-type]

    assert asyncio.run(rpc._handle_server_request({"id": 17, "method": method}))

    assert json.loads(process.stdin.writes[-1]) == {"id": 17, "result": expected_result}
    assert rpc.server_request_methods == [method]


def test_unknown_codex_app_server_request_is_rejected_instead_of_hanging():
    process = _RpcOnlyProcess()
    rpc = _JsonRpcProcess(process)  # type: ignore[arg-type]

    assert asyncio.run(rpc._handle_server_request({"id": 19, "method": "future/request"}))

    response = json.loads(process.stdin.writes[-1])
    assert response["id"] == 19
    assert response["error"]["code"] == -32601
    assert rpc.timeout_diagnostic("turn").endswith("server requests: future/request")


def test_codex_mcp_readiness_is_scoped_to_the_exact_managed_thread():
    process = _RpcOnlyProcess()
    rpc = _JsonRpcProcess(process)  # type: ignore[arg-type]
    rpc._observe_notification(
        {
            "method": "mcpServer/startupStatus/updated",
            "params": {
                "threadId": None,
                "name": "cao_control_plane",
                "status": "cancelled",
            },
        }
    )
    rpc._observe_notification(
        {
            "method": "mcpServer/startupStatus/updated",
            "params": {
                "threadId": "managed-thread",
                "name": "cao_control_plane",
                "status": "ready",
            },
        }
    )

    asyncio.run(
        rpc.wait_for_mcp_server_ready(
            "cao_control_plane", 0.1, thread_id="managed-thread"
        )
    )
    assert rpc.mcp_server_states == {
        ("", "cao_control_plane"): "cancelled",
        ("managed-thread", "cao_control_plane"): "ready",
    }


def test_codex_mcp_readiness_survives_transient_thread_cancellation():
    async def exercise() -> _JsonRpcProcess:
        process = _RpcOnlyProcess()
        process.stdout = asyncio.StreamReader()  # type: ignore[attr-defined]
        for status in ("cancelled", "starting", "ready"):
            process.stdout.feed_data(  # type: ignore[attr-defined]
                (
                    json.dumps(
                        {
                            "method": "mcpServer/startupStatus/updated",
                            "params": {
                                "threadId": "managed-thread",
                                "name": "cao_control_plane",
                                "status": status,
                            },
                        }
                    )
                    + "\n"
                ).encode()
            )
        rpc = _JsonRpcProcess(process)  # type: ignore[arg-type]
        await rpc.wait_for_mcp_server_ready(
            "cao_control_plane", 0.1, thread_id="managed-thread"
        )
        return rpc

    rpc = asyncio.run(exercise())

    assert rpc.mcp_server_states[("managed-thread", "cao_control_plane")] == "ready"


def test_codex_accepts_only_the_managed_control_plane_mcp_server():
    process = _RpcOnlyProcess()
    rpc = _JsonRpcProcess(
        process,  # type: ignore[arg-type]
        trusted_mcp_servers=frozenset({"cao_control_plane"}),
    )

    assert asyncio.run(
        rpc._handle_server_request(
            {
                "id": 21,
                "method": "mcpServer/elicitation/request",
                "params": {
                    "serverName": "cao_control_plane",
                    "_meta": {"codex_approval_kind": "mcp_tool_call"},
                },
            }
        )
    )
    assert json.loads(process.stdin.writes[-1]) == {
        "id": 21,
        "result": {"action": "accept", "content": {}, "_meta": None},
    }

    assert asyncio.run(
        rpc._handle_server_request(
            {
                "id": 22,
                "method": "mcpServer/elicitation/request",
                "params": {"serverName": "unrelated_user_server"},
            }
        )
    )
    assert json.loads(process.stdin.writes[-1]) == {
        "id": 22,
        "result": {"action": "decline", "content": None, "_meta": None},
    }

    assert asyncio.run(
        rpc._handle_server_request(
            {
                "id": 23,
                "method": "mcpServer/elicitation/request",
                "params": {
                    "serverName": "cao_control_plane",
                    "_meta": {"codex_approval_kind": "unrelated_form"},
                },
            }
        )
    )
    assert json.loads(process.stdin.writes[-1]) == {
        "id": 23,
        "result": {"action": "decline", "content": None, "_meta": None},
    }


def _runtime(socket_path: Path) -> dict[str, Any]:
    return {
        "id": "run-managed-codex-test",
        "endpoint": ENDPOINT,
        "native_session_id": "",
        "enrollment_capability_socket": str(socket_path),
        "metadata": {"environment": {"SAFE": "present"}},
    }


@pytest.mark.parametrize(
    "provider_failure",
    [
        '{"type":"rate_limit","organization":"vendor-org-sentinel","session_id":"session-sentinel"}',
        '{"error":"rate_limit","locator":"private-owner-locator"}',
        '{"status":429,"message":"vendor-specific quota prose"}',
        "HTTP status 429",
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "result": (
                    'API Error: 429 {"error":{"type":"rate_limit",'
                    '"message":"request throttled"}}'
                ),
            }
        ),
    ],
)
def test_provider_rate_limit_maps_to_one_fixed_non_conversational_code(
    provider_failure: str,
) -> None:
    assert _runtime_failure_code(provider_failure) == "runtime_provider_rate_limited"


def test_provider_rate_limit_classifies_parsed_nested_mapping_without_stderr_429() -> None:
    assert (
        _runtime_failure_code(
            {
                "error": {
                    "type": "rate_limit",
                    "organization": "vendor-org-sentinel",
                    "session_id": "session-sentinel",
                }
            }
        )
        == "runtime_provider_rate_limited"
    )
    assert _runtime_failure_code("provider rate_limit prose") == "runtime_dispatch_failed"
    assert _runtime_failure_code("xhttp429suffix") == "runtime_dispatch_failed"
    assert _runtime_failure_code("note: API Error: 429") == "runtime_dispatch_failed"


def test_claude_rate_limit_result_never_returns_raw_provider_body(
    monkeypatch: pytest.MonkeyPatch, settings, tmp_path: Path
) -> None:
    private_body = {
        "type": "rate_limit",
        "organization": "vendor-org-sentinel",
        "session_id": "session-sentinel",
        "locator": "private-owner-locator",
    }

    async def spawn(*_argv: str, **_kwargs: Any) -> object:
        return type("Process", (), {"pid": os.getpid()})()

    async def communicate(*_args: Any, **_kwargs: Any) -> tuple[int, str, str]:
        return 1, json.dumps({"error": private_body}), ""

    monkeypatch.setattr(
        "cao_control_plane.runtime._default_claude_command",
        lambda: [str(tmp_path / "claude")],
    )
    monkeypatch.setattr(
        "cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn
    )
    monkeypatch.setattr(
        "cao_control_plane.runtime._communicate_limited", communicate
    )

    result = asyncio.run(
        ClaudeAdapter(settings).dispatch(
            {"metadata": {}}, {"kind": "instruction", "payload": {"message": "go"}}
        )
    )

    assert result.success is False
    assert result.error == "runtime_provider_rate_limited"
    assert "vendor-org-sentinel" not in result.error
    assert "session-sentinel" not in result.error
    assert "private-owner-locator" not in result.error


def test_claude_injects_inline_strict_mcp_config_without_secret(monkeypatch, settings, tmp_path: Path):
    principal_secret = "cao.prn_parent.parent-control-plane-secret"
    monkeypatch.setenv("CAO_A2A_TOKEN", principal_secret)
    socket_path = tmp_path / "claude.sock"
    captured: dict[str, Any] = {}

    async def spawn(*argv: str, **kwargs: Any) -> object:
        captured["argv"] = list(argv)
        captured["env"] = kwargs["env"]
        return type("Process", (), {"pid": os.getpid()})()

    async def communicate(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        return 0, json.dumps({"session_id": "claude-session", "result": "ok"}), ""

    resolved_claude = tmp_path / "native" / "claude"
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_claude_command",
        lambda: [str(resolved_claude)],
    )
    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr("cao_control_plane.runtime._communicate_limited", communicate)
    result = asyncio.run(ClaudeAdapter(settings).dispatch(_runtime(socket_path), {"kind": "instruction", "payload": {"message": "go"}}))

    argv = captured["argv"]
    config = json.loads(argv[argv.index("--mcp-config") + 1])
    assert argv[:3] == [str(resolved_claude), "-p", "--output-format"]
    assert argv[argv.index("--output-format") + 1] == "json"
    assert "--strict-mcp-config" not in argv
    assert config["mcpServers"]["cao_control_plane"]["args"][-2:] == [
        "--enrollment-broker-socket",
        str(socket_path),
    ]
    assert SENTINEL not in json.dumps(argv)
    assert SENTINEL not in json.dumps(captured["env"])
    assert "CAO_A2A_TOKEN" not in captured["env"]
    assert principal_secret not in json.dumps(captured["env"])
    assert SENTINEL not in json.dumps(result.metadata)


@pytest.mark.parametrize("native_session_id", ["", "existing-thread"])
def test_codex_uses_thread_scoped_mcp_config_without_secret(
    monkeypatch, settings, tmp_path: Path, native_session_id: str
):
    principal_secret = "cao.prn_parent.parent-control-plane-secret"
    monkeypatch.setenv("CAO_A2A_TOKEN", principal_secret)
    monkeypatch.setenv("CODEX_THREAD_ID", "outer-cao-conversation")
    socket_path = tmp_path / "codex.sock"
    captured: dict[str, Any] = {}

    async def spawn(*argv: str, **kwargs: Any) -> _CodexProcess:
        captured["argv"] = list(argv)
        captured["env"] = kwargs["env"]
        process = _CodexProcess(_managed_codex_mcp_server_name(_runtime(socket_path)))
        captured["process"] = process
        return process

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr(
        "cao_control_plane.runtime._default_codex_app_server_command",
        lambda: ["codex", "app-server", "--stdio"],
    )
    runtime = _runtime(socket_path)
    runtime["native_session_id"] = native_session_id
    result = asyncio.run(
        CodexAppServerAdapter(settings).dispatch(
            runtime, {"kind": "instruction", "payload": {"message": "go"}}
        )
    )

    argv = captured["argv"]
    assert argv[:3] == ["codex", "app-server", "--stdio"]
    assert argv[3:] == []
    writes = [json.loads(value) for value in captured["process"].stdin.writes]
    thread_method = "thread/resume" if native_session_id else "thread/start"
    thread_request = next(value for value in writes if value.get("method") == thread_method)
    if native_session_id:
        assert thread_request["params"]["excludeTurns"] is True
    else:
        assert "excludeTurns" not in thread_request["params"]
    server_name = _managed_codex_mcp_server_name(runtime)
    assert set(thread_request["params"]["config"]) == {
        f"mcp_servers.{server_name}",
    }
    assert thread_request["params"]["config"][f"mcp_servers.{server_name}"][
        "args"
    ][-2] == "--enrollment-broker-socket"
    assert len([value for value in writes if value.get("method") in {"thread/start", "thread/resume"}]) == 1
    assert SENTINEL not in json.dumps(argv)
    assert SENTINEL not in json.dumps(captured["env"])
    assert "CAO_A2A_TOKEN" not in captured["env"]
    assert "CODEX_THREAD_ID" not in captured["env"]
    assert principal_secret not in json.dumps(captured["env"])
    assert SENTINEL not in json.dumps(result.metadata)


def test_codex_managed_launch_preserves_inherited_mcp_servers_for_thread_injection(
    monkeypatch, settings, tmp_path: Path
):
    captured: dict[str, Any] = {}

    async def spawn(*argv: str, **kwargs: Any) -> _CodexProcess:
        captured["argv"] = list(argv)
        return _CodexProcess(_managed_codex_mcp_server_name(runtime))

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    runtime = _runtime(tmp_path / "codex.sock")
    runtime["metadata"]["command"] = [
        "codex",
        "app-server",
        "--stdio",
        "-c",
        'mcp_servers={inherited={command="untrusted",args=[]}}',
    ]

    asyncio.run(
        CodexAppServerAdapter(settings).dispatch(
            runtime, {"kind": "instruction", "payload": {"message": "go"}}
        )
    )

    assert captured["argv"] == runtime["metadata"]["command"]


def test_runtime_without_internal_broker_key_does_not_inject_mcp_config():
    assert _enrollment_mcp_config({"endpoint": ENDPOINT, "metadata": {}}) is None


def test_runtime_environment_strips_parent_authority_and_rejects_reinjection(monkeypatch):
    principal_secret = "cao.prn_parent.parent-control-plane-secret"
    monkeypatch.setenv("CAO_A2A_TOKEN", principal_secret)
    monkeypatch.setenv("CAO_WORK_CONTINUATION", "supervisor-private-state")
    monkeypatch.setenv("UNRELATED_SECRET_ALIAS", principal_secret)

    environment = _safe_env({"environment": {"SAFE": "present"}})

    assert environment["SAFE"] == "present"
    assert "CAO_A2A_TOKEN" not in environment
    assert "CAO_WORK_CONTINUATION" not in environment
    assert "UNRELATED_SECRET_ALIAS" not in environment
    assert "CODEX_THREAD_ID" not in environment
    assert principal_secret not in json.dumps(environment)
    with pytest.raises(RuntimeAdapterError, match="reserved variable"):
        _safe_env({"environment": {"CODEX_THREAD_ID": "forged-worker-thread"}})
    with pytest.raises(RuntimeAdapterError, match="reserved variable"):
        _safe_env({"environment": {"CAO_A2A_TOKEN": principal_secret}})
    with pytest.raises(RuntimeAdapterError, match="credential-like secret"):
        _safe_env({"environment": {"UNRELATED_SECRET_ALIAS": principal_secret}})
    assert principal_secret not in str(_sanitize_runtime_value(principal_secret))
