from __future__ import annotations

import asyncio
import json
import socket
import sys
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

import uvicorn

from cao_control_plane.api import create_app
from cao_control_plane.config import Settings
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)
from cao_control_plane.models import PrincipalCreate, RuntimeRegistration
from cao_control_plane.runtime import ClaudeAdapter, CodexAppServerAdapter
from cao_control_plane.runtime_enrollment import EnrollmentCapabilityBroker

_SENTINEL = "runner-contract-ticket-must-never-appear-in-launch-data"


class _ReadyServer(uvicorn.Server):
    """Expose one startup boundary without polling a server port."""

    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self.ready = threading.Event()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self.ready.set()


def _mcp_tools_list_request(request_id: int = 1) -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/list",
            "params": {
                "_meta": {
                    PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                    CLIENT_CAPABILITIES_META_KEY: {},
                    CLIENT_INFO_META_KEY: {
                        "name": "managed-runner-contract",
                        "version": "1",
                    },
                }
            },
        }
    )


def _runner_program(kind: str) -> str:
    """A local vendor-client double that launches the supplied stdio MCP command.

    It deliberately receives the real command line through create_subprocess_exec.
    Claude receives its injected configuration in CLI arguments, while Codex
    receives it only in the exact ``thread/start`` request.  The only output
    persisted by the test double is that scoped configuration, never the ticket
    contents or exchanged runtime credential.
    """

    return f'''\
import json
import os
import subprocess
import sys
from pathlib import Path

SENTINEL = {json.dumps(_SENTINEL)}
KIND = {json.dumps(kind)}
argv = sys.argv[1:]
assert SENTINEL not in json.dumps(argv)
assert all(SENTINEL not in value for value in os.environ.values())

def verify_bridge(config):
    server = config["mcpServers"]["cao_control_plane"]
    assert server["args"][:3] == ["-m", "cao_control_plane.cli", "mcp-stdio"]
    bridge = subprocess.Popen(
        [server["command"], *server["args"]],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert bridge.stdin is not None and bridge.stdout is not None
    bridge.stdin.write({json.dumps(_mcp_tools_list_request())} + "\\n")
    bridge.stdin.flush()
    bridge_line = bridge.stdout.readline()
    bridge.stdin.close()
    bridge.wait(timeout=10)
    bridge_stderr = bridge.stderr.read() if bridge.stderr else ""
    if bridge.returncode != 0:
        Path(os.environ["CAO_RUNNER_CONTRACT_RECORD"]).write_text(
            json.dumps({{"bridge_returncode": bridge.returncode, "bridge_stderr": bridge_stderr, "bridge_line": bridge_line}}),
            encoding="utf-8",
        )
        raise RuntimeError(bridge_stderr)
    bridge_response = json.loads(bridge_line)
    assert "error" not in bridge_response, bridge_response
    assert any(tool["name"] == "cao_get_inbox" for tool in bridge_response["result"]["tools"])
    return bridge_response

if KIND == "claude":
    assert "--strict-mcp-config" not in argv
    config = json.loads(argv[argv.index("--mcp-config") + 1])
    bridge_response = verify_bridge(config)
    Path(os.environ["CAO_RUNNER_CONTRACT_RECORD"]).write_text(
        json.dumps({{"argv": argv, "config": config, "bridge": bridge_response}}, separators=(",", ":")),
        encoding="utf-8",
    )
    print(json.dumps({{"session_id": "contract-claude", "result": "ok"}}))
else:
    # The app-server keeps inherited MCP servers.  The managed bridge arrives
    # as one additive dotted override on the Worker thread it is about to run.
    assert "--strict-mcp-config" not in argv
    assert argv == []

    def read():
        value = sys.stdin.readline()
        assert value
        return json.loads(value)

    initial = read()
    print(json.dumps({{"id": initial["id"], "result": {{}}}}), flush=True)
    read()  # initialized notification
    thread = read()
    assert thread["method"] == "thread/start"
    thread_config = thread["params"].get("config")
    assert isinstance(thread_config, dict)
    managed_keys = [
        key for key in thread_config if key.startswith("mcp_servers.cao_managed_")
    ]
    assert len(managed_keys) == 1
    assert set(thread_config) == set(managed_keys)
    managed_key = managed_keys[0]
    server_name = managed_key.removeprefix("mcp_servers.")
    scoped_server = thread_config[managed_key]
    config = {{"mcpServers": {{"cao_control_plane": scoped_server}}}}
    bridge_response = verify_bridge(config)
    print(json.dumps({{"id": thread["id"], "result": {{"thread": {{"id": "contract-thread"}}}}}}), flush=True)
    print(json.dumps({{
        "method": "mcpServer/startupStatus/updated",
        "params": {{
            "threadId": "contract-thread",
            "name": server_name,
            "status": "ready",
        }},
    }}), flush=True)
    delivery = read()
    Path(os.environ["CAO_RUNNER_CONTRACT_RECORD"]).write_text(
        json.dumps(
            {{
                "argv": argv,
                "thread": thread,
                "delivery": delivery,
                "config": config,
                "bridge": bridge_response,
            }},
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    if delivery["method"] == "thread/queue/add":
        client_id = delivery["params"]["clientUserMessageId"]
        print(json.dumps({{
            "id": delivery["id"],
            "result": {{
                "queuedSubmission": {{
                    "id": "queued-contract-submission",
                    "clientUserMessageId": client_id,
                    "input": delivery["params"]["input"],
                }}
            }},
        }}), flush=True)
        print(json.dumps({{
            "method": "item/started",
            "params": {{
                "threadId": "contract-thread",
                "turnId": "contract-turn",
                "startedAtMs": 1,
                "item": {{
                    "id": "contract-user-message",
                    "type": "userMessage",
                    "clientId": client_id,
                    "content": delivery["params"]["input"],
                }},
            }},
        }}), flush=True)
    else:
        print(json.dumps({{"id": delivery["id"], "result": {{"turn": {{"id": "contract-turn"}}}}}}), flush=True)
    print(json.dumps({{"method": "turn/completed", "params": {{"turn": {{"id": "contract-turn", "status": "completed"}}}}}}), flush=True)
'''


def _start_server(
    tmp_path: Path,
) -> tuple[_ReadyServer, threading.Thread, socket.socket, Settings, Any, dict[str, Any]]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = int(listener.getsockname()[1])
    base_url = f"http://127.0.0.1:{port}"
    settings = replace(
        Settings(),
        state_dir=tmp_path / "state",
        runtime_launch_dir=tmp_path / "runtime-launches",
        host="127.0.0.1",
        port=port,
        public_base_url=base_url,
        trusted_hosts=("127.0.0.1",),
        allowed_origins=(base_url,),
        runtime_timeout_seconds=10.0,
    )
    settings.ensure_directories()
    app = create_app(settings)
    server = _ReadyServer(uvicorn.Config(app, log_config=None, access_log=False, log_level="warning"))
    errors: list[BaseException] = []

    def run() -> None:
        try:
            server.run(sockets=[listener])
        except BaseException as error:  # pragma: no cover - asserted via readiness
            errors.append(error)

    thread = threading.Thread(target=run, name="managed-runner-contract", daemon=True)
    thread.start()
    assert server.ready.wait(10), errors
    service = app.state.service
    cao = service.authenticate(app.state.bootstrap["tokens"]["cao"]["token"])
    return server, thread, listener, settings, service, cao


async def _runtime_with_broker(
    service: Any,
    cao: dict[str, Any],
    settings: Settings,
    adapter: str,
    command: list[str],
    record: Path,
) -> tuple[dict[str, Any], EnrollmentCapabilityBroker, str]:
    worker = service.create_principal(cao, PrincipalCreate(name=f"{adapter}-contract", role="worker"))
    runtime = service.register_runtime(
        cao,
        worker["principal"]["id"],
        RuntimeRegistration(adapter=adapter, endpoint=f"{settings.public_base_url}/mcp"),
    )
    issued = service.issue_runtime_launch_ticket(runtime["id"])
    broker = EnrollmentCapabilityBroker(
        configured_root=settings.runtime_launch_dir,
        ticket_id=str(issued["ticket_id"]),
        raw_ticket=str(issued["ticket"]),
        exchange=service.exchange_runtime_launch_ticket,
        delivery_failed=lambda reason: service.fail_runtime_enrollment(
            runtime["id"], reason=reason
        ),
    )
    await broker.start()
    return (
        {
            **runtime,
            "enrollment_capability_socket": broker.path,
            "_enrollment_capability_broker": broker,
            "metadata": {
                "command": command,
                "environment": {"CAO_RUNNER_CONTRACT_RECORD": str(record)},
                "timeout_seconds": 10,
            },
        },
        broker,
        str(issued["ticket"]),
    )


def _assert_record(record: Path, socket_path: str, ticket: str) -> dict[str, Any]:
    contents = record.read_text(encoding="utf-8")
    parsed = json.loads(contents)
    assert _SENTINEL not in contents
    assert ticket not in contents
    assert "--strict-mcp-config" not in parsed["argv"]
    server = parsed["config"]["mcpServers"]["cao_control_plane"]
    assert server["command"] == sys.executable
    assert server["args"][-1] == socket_path
    assert not Path(socket_path).exists()
    assert "cao_get_context" in {tool["name"] for tool in parsed["bridge"]["result"]["tools"]}
    return parsed


def test_claude_real_process_launches_injected_stdio_bridge_additively(
    tmp_path: Path,
) -> None:
    server, thread, listener, settings, service, cao = _start_server(tmp_path)
    try:
        script = tmp_path / "fake-claude.py"
        script.write_text(_runner_program("claude"), encoding="utf-8")
        record = tmp_path / "claude-launch.json"
        async def exercise() -> tuple[Any, dict[str, Any], str]:
            runtime, broker, ticket = await _runtime_with_broker(
                service, cao, settings, "claude", [sys.executable, str(script)], record
            )
            try:
                result = await ClaudeAdapter(settings).dispatch(
                    runtime, {"kind": "instruction", "payload": {"message": "go"}}
                )
            finally:
                await broker.close()
            return result, runtime, ticket

        result, runtime, ticket = asyncio.run(exercise())

        assert result.success
        _assert_record(record, str(runtime["enrollment_capability_socket"]), ticket)
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        assert not thread.is_alive()


def test_codex_real_process_binds_stdio_bridge_to_exact_thread(tmp_path: Path) -> None:
    server, thread, listener, settings, service, cao = _start_server(tmp_path)
    try:
        script = tmp_path / "fake-codex-app-server.py"
        script.write_text(_runner_program("codex"), encoding="utf-8")
        record = tmp_path / "codex-launch.json"
        async def exercise() -> tuple[Any, dict[str, Any], str]:
            runtime, broker, ticket = await _runtime_with_broker(
                service,
                cao,
                settings,
                "codex-app-server",
                [sys.executable, str(script)],
                record,
            )
            try:
                result = await CodexAppServerAdapter(settings).dispatch(
                    runtime,
                    {
                        "id": "msg_managed_runner_contract",
                        "kind": "instruction",
                        "payload": {"message": "go"},
                    },
                )
            finally:
                await broker.close()
            return result, runtime, ticket

        result, runtime, ticket = asyncio.run(exercise())

        assert result.success
        assert result.metadata["delivery_method"] == "thread_queue"
        assert result.metadata["delivery_acceptance"] == "completed"
        parsed = _assert_record(record, str(runtime["enrollment_capability_socket"]), ticket)
        assert parsed["argv"] == []
        recorded_thread = parsed["thread"]
        assert recorded_thread["method"] == "thread/start"
        config = recorded_thread["params"]["config"]
        managed_keys = [
            key for key in config if key.startswith("mcp_servers.cao_managed_")
        ]
        assert len(managed_keys) == 1
        assert set(config) == set(managed_keys)
        assert (
            config[managed_keys[0]]
            == parsed["config"]["mcpServers"]["cao_control_plane"]
        )
        delivery = parsed["delivery"]
        assert delivery["method"] == "thread/queue/add"
        assert delivery["params"]["threadId"] == "contract-thread"
        assert delivery["params"]["clientUserMessageId"].startswith(
            "cao-delivery-"
        )
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        assert not thread.is_alive()
