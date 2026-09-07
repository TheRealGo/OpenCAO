"""Real app-server coverage for thread-scoped MCP startup and model binding.

Most cases exercise the installed app-server's JSON-RPC protocol and a local
stdio MCP server without a model turn. The catalog regression additionally
uses a local fake Responses endpoint to inspect the exact tools bound to three
real turns; it invokes no external service or vendor model.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import sys
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from cao_control_plane.api import create_app
from cao_control_plane.config import Settings
from cao_control_plane.models import PrincipalCreate, PrincipalRole, RuntimeRegistration
from cao_control_plane.runtime import (
    _codex_thread_mcp_config,
    _enrollment_mcp_config,
    _JsonRpcProcess,
    _managed_codex_mcp_server_name,
)
from cao_control_plane.runtime_enrollment import EnrollmentCapabilityBroker

_PROBE_SERVER_NAME = "cao_thread_scoped_probe"
_PROBE_ENV = "CAO_THREAD_MCP_PROTOCOL_RECORD"
_DASHBOARD_SERVER_NAME = "cao_dashboard_native_snapshot"
_MACOS_CODEX = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
_OWNER_UV = Path.home() / ".nix-profile/bin/uv"
_AMBIENT_MCP_SERVER_NAMES = ("colab", "memory", "github")


def _sse(*events: dict[str, Any]) -> bytes:
    chunks = [f"event: {event['type']}\ndata: {json.dumps(event)}\n" for event in events]
    return ("\n".join(chunks) + "\n").encode("utf-8")


def _completed_event(response_id: str) -> dict[str, Any]:
    return {
        "type": "response.completed",
        "response": {
            "id": response_id,
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": None,
                "output_tokens": 1,
                "output_tokens_details": None,
                "total_tokens": 2,
            },
        },
    }


def _mcp_child_tools(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    children_by_name: dict[str, dict[str, Any]] = {}
    tools = body.get("tools")
    if not isinstance(tools, list):
        return children_by_name
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = tool.get("name")
        if tool.get("type") == "namespace" and isinstance(name, str):
            if not name.endswith("cao_control_plane"):
                continue
            children = tool.get("tools")
            if isinstance(children, list):
                children_by_name.update(
                    {
                        str(child["name"]): child
                        for child in children
                        if isinstance(child, dict) and isinstance(child.get("name"), str)
                    }
                )
        elif isinstance(name, str) and "cao_control_plane" in name:
            children_by_name[name.rsplit("__", 1)[-1]] = tool
    return children_by_name


def _function_call_output(body: dict[str, Any], call_id: str) -> str:
    inputs = body.get("input")
    if not isinstance(inputs, list):
        return ""
    for item in inputs:
        if (
            isinstance(item, dict)
            and item.get("type") == "function_call_output"
            and item.get("call_id") == call_id
        ):
            output = item.get("output")
            return output if isinstance(output, str) else ""
    return ""


class _CatalogTurnModelServer(ThreadingHTTPServer):
    """Capture real model bindings and drive one MCP call per user turn."""

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _CatalogTurnModelHandler)
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.native_thread_id = ""
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self.server_address
        return f"http://{host}:{port}"

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=2.0)

    def capture_and_respond(self, body: dict[str, Any]) -> bytes:
        with self.lock:
            request_index = len(self.requests)
            self.requests.append(body)
        response_id = f"response-{request_index + 1}"
        created = {"type": "response.created", "response": {"id": response_id}}
        call_steps = {
            0: (
                "call-cao-start",
                "cao_start",
                {"native_thread_id": self.native_thread_id},
            ),
            2: ("call-worker-list", "cao_list_managed_workers", {}),
            4: (
                "call-worker-delete",
                "cao_delete_worker_thread",
                {
                    "worker_thread_id": "mwt_model_catalog_probe",
                    "expected_generation": 1,
                    "idempotency_key": "model-catalog-delete-probe",
                },
            ),
            6: (
                "call-work-read",
                "cao_get_work",
                {"work_item_id": "wrk_model_catalog_probe"},
            ),
        }
        call_step = call_steps.get(request_index)
        if call_step is not None:
            call_id, child_name, arguments = call_step
            function_call: dict[str, Any] = {
                "type": "function_call",
                "call_id": call_id,
                "name": child_name,
                "arguments": json.dumps(arguments, separators=(",", ":")),
            }
            tools = body.get("tools")
            namespace = (
                next(
                    (
                        tool.get("name")
                        for tool in tools
                        if isinstance(tools, list)
                        and isinstance(tool, dict)
                        and tool.get("type") == "namespace"
                        and isinstance(tool.get("name"), str)
                        and str(tool["name"]).endswith("cao_control_plane")
                    ),
                    None,
                )
                if isinstance(tools, list)
                else None
            )
            if isinstance(namespace, str):
                function_call["namespace"] = namespace
            else:
                matching_name = (
                    next(
                        (
                            tool.get("name")
                            for tool in tools
                            if isinstance(tools, list)
                            and isinstance(tool, dict)
                            and isinstance(tool.get("name"), str)
                            and str(tool["name"]).endswith(child_name)
                        ),
                        child_name,
                    )
                    if isinstance(tools, list)
                    else child_name
                )
                function_call["name"] = matching_name
            return _sse(
                created,
                {"type": "response.output_item.done", "item": function_call},
                _completed_event(response_id),
            )
        return _sse(
            created,
            {
                "type": "response.output_item.done",
                "item": {
                    "type": "message",
                    "role": "assistant",
                    "id": f"message-{request_index + 1}",
                    "content": [{"type": "output_text", "text": "turn complete"}],
                },
            },
            _completed_event(response_id),
        )


class _CatalogTurnModelHandler(BaseHTTPRequestHandler):
    server: _CatalogTurnModelServer

    def log_message(self, _format: str, *_args: object) -> None:
        return None

    def do_GET(self) -> None:
        payload = json.dumps(
            {
                "object": "list",
                "data": [
                    {
                        "id": "mock-model",
                        "object": "model",
                        "created": 0,
                        "owned_by": "openai",
                    }
                ],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        payload = self.server.capture_and_respond(body)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


# The program is a deliberately harmless MCP implementation.  It records only
# method names, exposes a single pure tool, and never touches the network.
_PROBE_SERVER_PROGRAM = r"""
import json
import os
import sys
from pathlib import Path

record = Path(os.environ["CAO_THREAD_MCP_PROTOCOL_RECORD"])
methods = []

def write_record():
    record.write_text(json.dumps(methods), encoding="utf-8")

for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    methods.append(method)
    write_record()
    request_id = request.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        result = {
            "protocolVersion": request.get("params", {}).get("protocolVersion", "2025-06-18"),
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "cao-thread-scoped-probe", "version": "1"},
        }
    elif method == "tools/list":
        result = {
            "tools": [{
                "name": "probe",
                "description": "Pure MCP transport probe.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            }]
        }
    elif method == "tools/call":
        result = {
            "content": [{"type": "text", "text": "thread-scoped-mcp-ok"}],
            "structuredContent": {"result": "thread-scoped-mcp-ok"},
        }
    elif method in {"resources/list", "resources/templates/list", "prompts/list"}:
        result = {method.split("/")[0]: []}
    else:
        print(json.dumps({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": method}}), flush=True)
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
"""

_MULTI_SERVER_PROGRAM = r"""
import json
import os
import sys
from pathlib import Path

server_name, marker, record_path = sys.argv[1:]
record = Path(record_path)
methods = []
tool_name = f"probe_{server_name}"

def write_record():
    record.write_text(json.dumps(methods), encoding="utf-8")

for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    methods.append(method)
    write_record()
    request_id = request.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        result = {
            "protocolVersion": request.get("params", {}).get("protocolVersion", "2025-06-18"),
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": server_name, "version": "1"},
        }
    elif method == "tools/list":
        result = {
            "tools": [{
                "name": tool_name,
                "description": "Pure local MCP namespace probe.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            }]
        }
    elif method == "tools/call":
        assert request.get("params", {}).get("name") == tool_name
        result = {
            "content": [{"type": "text", "text": marker}],
            "structuredContent": {
                "server": server_name,
                "marker": marker,
                "ambientLeak": os.environ.get("AMBIENT_CAO_SENTINEL", ""),
            },
        }
    elif method in {"resources/list", "resources/templates/list", "prompts/list"}:
        result = {method.split("/")[0]: []}
    else:
        print(json.dumps({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": method}}), flush=True)
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
"""


def _installed_codex() -> str | None:
    executable = shutil.which("codex")
    if executable:
        return executable
    if _MACOS_CODEX.is_file() and os.access(_MACOS_CODEX, os.X_OK):
        return str(_MACOS_CODEX)
    return None


def _toml_string(value: str) -> str:
    """JSON strings are valid TOML basic strings for these bounded values."""

    return json.dumps(value)


def _write_ambient_mcp_config(
    codex_home: Path,
    server_program: Path,
    records: dict[str, Path],
    *,
    include_ambient_cao: bool,
) -> None:
    codex_home.mkdir(mode=0o700)
    sections: list[str] = []
    names = (
        (*_AMBIENT_MCP_SERVER_NAMES, "cao_control_plane")
        if include_ambient_cao
        else _AMBIENT_MCP_SERVER_NAMES
    )
    for name in names:
        marker = f"ambient-{name}"
        sections.extend(
            [
                f"[mcp_servers.{name}]",
                f"command = {_toml_string(sys.executable)}",
                "args = ["
                + ", ".join(
                    _toml_string(value)
                    for value in (str(server_program), name, marker, str(records[name]))
                )
                + "]",
                "enabled = true",
                "startup_timeout_sec = 10",
                "tool_timeout_sec = 10",
                "",
            ]
        )
        if name == "cao_control_plane":
            sections.extend(
                [
                    "[mcp_servers.cao_control_plane.env]",
                    'AMBIENT_CAO_SENTINEL = "ambient-only-value"',
                    "",
                ]
            )
    (codex_home / "config.toml").write_text("\n".join(sections), encoding="utf-8")


def _thread_cao_override(server_program: Path, record: Path, *, server_name: str) -> dict[str, Any]:
    return {
        f"mcp_servers.{server_name}": {
            "command": sys.executable,
            "args": [
                str(server_program),
                server_name,
                "thread-cao_control_plane",
                str(record),
            ],
            "enabled": True,
            "startup_timeout_sec": 10,
            "tool_timeout_sec": 10,
        }
    }


class _RecordingJsonRpcProcess(_JsonRpcProcess):
    """Retain status notifications while preserving the production parser."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.statuses: list[dict[str, Any]] = []

    def _observe_notification(self, value: Any) -> None:
        if value.get("method") == "mcpServer/startupStatus/updated":
            params = value.get("params")
            if isinstance(params, dict) and params.get("name") in self.trusted_mcp_servers:
                self.statuses.append(dict(params))
        super()._observe_notification(value)


def _thread_scoped_config() -> dict[str, Any]:
    return {
        "mcp_servers": {
            _PROBE_SERVER_NAME: {
                "command": sys.executable,
                "args": ["-c", _PROBE_SERVER_PROGRAM],
                "env_vars": [_PROBE_ENV],
            }
        }
    }


async def _exercise_real_app_server(
    record_path: Path, cwd: Path
) -> tuple[list[dict[str, Any]], str]:
    environment = dict(os.environ)
    environment.pop("OPENAI_API_KEY", None)
    environment.pop("ANTHROPIC_API_KEY", None)
    environment[_PROBE_ENV] = str(record_path)
    process = await asyncio.create_subprocess_exec(
        "codex",
        "app-server",
        "--stdio",
        cwd=str(cwd),
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=1024 * 1024,
    )
    rpc = _RecordingJsonRpcProcess(process, trusted_mcp_servers=frozenset({_PROBE_SERVER_NAME}))
    try:
        await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-thread-mcp-protocol-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        _, started = await rpc.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "config": _thread_scoped_config(),
            },
        )
        thread = started.get("thread", started)
        if not isinstance(thread, dict):
            raise AssertionError(f"app-server returned non-object thread: {thread!r}")
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        if not thread_id:
            raise AssertionError("app-server did not return a thread ID")
        await rpc.wait_for_mcp_server_ready(_PROBE_SERVER_NAME, 20.0, thread_id=thread_id)
        _, invoked = await rpc.request(
            "mcpServer/tool/call",
            {"threadId": thread_id, "server": _PROBE_SERVER_NAME, "tool": "probe", "arguments": {}},
        )
        if invoked.get("structuredContent") != {"result": "thread-scoped-mcp-ok"}:
            raise AssertionError(f"thread MCP probe returned unexpected result: {invoked!r}")
        return rpc.statuses, thread_id
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()


def test_real_app_server_starts_one_thread_scoped_mcp_server_without_model_turn(
    tmp_path: Path,
) -> None:
    """A real app-server reaches discovery and a pure tool only for its thread."""

    if shutil.which("codex") is None:
        pytest.skip("installed Codex app-server is unavailable")
    record_path = tmp_path / "mcp-methods.json"
    try:
        statuses, thread_id = asyncio.run(_exercise_real_app_server(record_path, tmp_path))
    except FileNotFoundError:
        pytest.skip("installed Codex app-server is unavailable")

    assert record_path.exists(), "the local MCP server was never started"
    methods = json.loads(record_path.read_text(encoding="utf-8"))
    assert methods.count("initialize") == 1
    assert methods.count("tools/list") == 1
    assert methods.count("tools/call") == 1
    assert all(status.get("threadId") == thread_id for status in statuses)
    status_values = [status.get("status") for status in statuses]
    assert status_values[0] == "starting"
    assert status_values.count("ready") >= 1
    assert all(status == "ready" for status in status_values[status_values.index("ready") :])


async def _exercise_additive_thread_mcp_catalog(
    *, codex: str, tmp_path: Path, include_ambient_cao: bool
) -> tuple[
    str,
    dict[str, dict[str, Any]],
    dict[str, dict[str, str]],
    dict[str, Path],
]:
    """Exercise the real app-server catalog without starting a model turn."""

    codex_home = tmp_path / "codex-home"
    server_program = tmp_path / "local-mcp-server.py"
    server_program.write_text(_MULTI_SERVER_PROGRAM, encoding="utf-8")
    ambient_records = {
        name: tmp_path / f"ambient-{name}.json"
        for name in (*_AMBIENT_MCP_SERVER_NAMES, "cao_control_plane")
    }
    thread_cao_record = tmp_path / "thread-cao_control_plane.json"
    _write_ambient_mcp_config(
        codex_home,
        server_program,
        ambient_records,
        include_ambient_cao=include_ambient_cao,
    )

    environment = dict(os.environ)
    for key in (
        "ANTHROPIC_API_KEY",
        "CAO_A2A_TOKEN",
        "CAO_SESSION",
        "CODEX_THREAD_ID",
        "OPENAI_API_KEY",
    ):
        environment.pop(key, None)
    environment["CODEX_HOME"] = str(codex_home)
    process = await asyncio.create_subprocess_exec(
        codex,
        "app-server",
        "--stdio",
        cwd=str(tmp_path),
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=1024 * 1024,
    )
    managed_name = _managed_codex_mcp_server_name({"id": "run-additive-e2e"})
    expected_names = {
        *_AMBIENT_MCP_SERVER_NAMES,
        managed_name,
    }
    if include_ambient_cao:
        expected_names.add("cao_control_plane")
    rpc = _RecordingJsonRpcProcess(process, trusted_mcp_servers=frozenset({managed_name}))

    async def inventory(thread_id: str) -> dict[str, dict[str, Any]]:
        _, result = await rpc.request(
            "mcpServerStatus/list",
            {"threadId": thread_id, "detail": "full"},
        )
        values = result.get("data")
        if not isinstance(values, list):
            raise AssertionError(f"app-server returned invalid MCP inventory: {result!r}")
        return {str(value.get("name")): value for value in values if isinstance(value, dict)}

    async def call_all(thread_id: str) -> dict[str, dict[str, str]]:
        called: dict[str, dict[str, str]] = {}
        names = [*_AMBIENT_MCP_SERVER_NAMES]
        if include_ambient_cao:
            names.append("cao_control_plane")
        names.append(managed_name)
        for name in names:
            _, result = await rpc.request(
                "mcpServer/tool/call",
                {
                    "threadId": thread_id,
                    "server": name,
                    "tool": f"probe_{name}",
                    "arguments": {},
                },
            )
            structured = result.get("structuredContent")
            if not isinstance(structured, dict):
                raise AssertionError(f"{name} returned no structured result: {result!r}")
            called[name] = {
                "server": str(structured.get("server", "")),
                "marker": str(structured.get("marker", "")),
                "ambientLeak": str(structured.get("ambientLeak", "")),
            }
        return called

    stderr = ""
    try:
        await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-additive-mcp-protocol-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        thread_override = _thread_cao_override(
            server_program,
            thread_cao_record,
            server_name=managed_name,
        )
        _, started = await rpc.request(
            "thread/start",
            {
                "cwd": str(tmp_path),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "config": thread_override,
            },
        )
        thread = started.get("thread", started)
        if not isinstance(thread, dict):
            raise AssertionError(f"app-server returned non-object thread: {thread!r}")
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        if not thread_id:
            raise AssertionError("app-server did not return a thread ID")
        for name in expected_names:
            await rpc.wait_for_mcp_server_ready(name, 20.0, thread_id=thread_id)
        discovered = await inventory(thread_id)
        called = await call_all(thread_id)
        return (
            thread_id,
            discovered,
            called,
            {
                **ambient_records,
                "thread_cao_control_plane": thread_cao_record,
            },
        )
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()
        if process.stderr is not None:
            stderr = (await process.stderr.read()).decode("utf-8", errors="replace")
        if process.returncode not in {0, -15}:
            raise AssertionError(f"app-server failed: {stderr}")


@pytest.mark.parametrize("include_ambient_cao", [False, True])
def test_real_app_server_preserves_three_ambient_mcps_with_isolated_managed_cao(
    tmp_path: Path, include_ambient_cao: bool
) -> None:
    """One no-model gate covers additive inventory and collision isolation."""

    codex = _installed_codex()
    if codex is None:
        pytest.skip("installed Codex app-server is unavailable")

    thread_id, inventory, called, records = asyncio.run(
        _exercise_additive_thread_mcp_catalog(
            codex=codex,
            tmp_path=tmp_path,
            include_ambient_cao=include_ambient_cao,
        )
    )

    managed_name = _managed_codex_mcp_server_name({"id": "run-additive-e2e"})
    expected_names = {
        *_AMBIENT_MCP_SERVER_NAMES,
        managed_name,
    }
    if include_ambient_cao:
        expected_names.add("cao_control_plane")
    expected_markers = {name: f"ambient-{name}" for name in _AMBIENT_MCP_SERVER_NAMES}
    if include_ambient_cao:
        expected_markers["cao_control_plane"] = "ambient-cao_control_plane"
    expected_markers[managed_name] = "thread-cao_control_plane"
    assert thread_id
    assert set(inventory) == expected_names
    assert set(called) == expected_names
    for name in expected_names:
        assert f"probe_{name}" in inventory[name]["tools"]
        assert called[name]["server"] == name
        assert called[name]["marker"] == expected_markers[name]
    if include_ambient_cao:
        assert called["cao_control_plane"]["ambientLeak"] == "ambient-only-value"
    assert called[managed_name]["ambientLeak"] == ""

    for name in _AMBIENT_MCP_SERVER_NAMES:
        methods = json.loads(records[name].read_text(encoding="utf-8"))
        assert "initialize" in methods
        assert "tools/list" in methods
        assert methods.count("tools/call") == 1
    thread_cao_methods = json.loads(records["thread_cao_control_plane"].read_text(encoding="utf-8"))
    assert "initialize" in thread_cao_methods
    assert "tools/list" in thread_cao_methods
    assert thread_cao_methods.count("tools/call") == 1
    if include_ambient_cao:
        ambient_cao_methods = json.loads(records["cao_control_plane"].read_text(encoding="utf-8"))
        assert ambient_cao_methods.count("tools/call") == 1
    else:
        assert not records["cao_control_plane"].exists()


class _ReadyServer(uvicorn.Server):
    """Expose the server's real startup boundary without an interval poll."""

    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self.ready = threading.Event()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self.ready.set()


async def _exercise_fresh_cao_attachment(
    *, settings: Settings, cwd: Path
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[str],
    list[str],
    list[dict[str, Any]],
    str,
]:
    """Run real model turns through start, scoped read, and fenced Delete."""

    uv = shutil.which("uv") or (str(_OWNER_UV) if _OWNER_UV.is_file() else None)
    codex = shutil.which("codex") or (str(_MACOS_CODEX) if _MACOS_CODEX.is_file() else None)
    if uv is None or codex is None:
        raise FileNotFoundError("uv is unavailable")
    environment = dict(os.environ)
    environment.pop("OPENAI_API_KEY", None)
    environment.pop("ANTHROPIC_API_KEY", None)
    environment.pop("CODEX_THREAD_ID", None)
    environment.pop("CAO_A2A_CONFIG", None)
    environment["CAO_A2A_STATE_DIR"] = str(settings.state_dir)
    environment["CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG"] = "1"
    codex_home = settings.state_dir / "codex-fresh-attachment"
    codex_home.mkdir(mode=0o700)
    environment["CODEX_HOME"] = str(codex_home)
    model_server = _CatalogTurnModelServer()
    model_server.start()
    (codex_home / "config.toml").write_text(
        (
            'model = "mock-model"\n'
            'model_provider = "mock_provider"\n'
            'approval_policy = "on-request"\n'
            'sandbox_mode = "read-only"\n\n'
            "[model_providers.mock_provider]\n"
            'name = "CAO catalog model-turn probe"\n'
            f'base_url = "{model_server.url}/v1"\n'
            'wire_api = "responses"\n'
            "request_max_retries = 0\n"
            "stream_max_retries = 0\n"
            "supports_websockets = false\n"
        ),
        encoding="utf-8",
    )
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = (
        source_root
        if not environment.get("PYTHONPATH")
        else f"{source_root}{os.pathsep}{environment['PYTHONPATH']}"
    )
    process = await asyncio.create_subprocess_exec(
        codex,
        "app-server",
        "--stdio",
        cwd=str(cwd),
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=1024 * 1024,
    )
    rpc = _RecordingJsonRpcProcess(process, trusted_mcp_servers=frozenset({"cao_control_plane"}))
    result: (
        tuple[
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            list[str],
            list[str],
            list[dict[str, Any]],
        ]
        | None
    ) = None
    stderr = ""
    try:
        await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-fresh-attachment-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        _, started = await rpc.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "approvalPolicy": "on-request",
                "sandbox": "read-only",
                "config": {
                    "mcp_servers": {
                        "cao_control_plane": {
                            "command": uv,
                            "args": [
                                "--directory",
                                str(cwd),
                                "run",
                                "cao-a2a",
                                "mcp-stdio",
                                "--url",
                                f"{settings.public_base_url}/mcp",
                            ],
                            "env_vars": ["CAO_A2A_STATE_DIR"],
                        }
                    }
                },
            },
        )
        thread = started.get("thread", started)
        if not isinstance(thread, dict):
            raise AssertionError(f"app-server returned non-object thread: {thread!r}")
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        if not thread_id:
            raise AssertionError("app-server did not return a thread ID")
        await rpc.wait_for_mcp_server_ready("cao_control_plane", 30.0, thread_id=thread_id)

        async def loaded_catalog_tools() -> list[str]:
            _, inventory = await rpc.request(
                "mcpServerStatus/list",
                {"threadId": thread_id, "detail": "full"},
            )
            entries = inventory.get("data")
            if not isinstance(entries, list):
                return []
            entry = next(
                (
                    value
                    for value in entries
                    if isinstance(value, dict) and value.get("name") == "cao_control_plane"
                ),
                None,
            )
            tools = entry.get("tools") if isinstance(entry, dict) else None
            if isinstance(tools, dict):
                return [str(tool) for tool in tools]
            if isinstance(tools, list):
                return [str(tool) for tool in tools]
            return []

        initial_catalog_tools = await loaded_catalog_tools()

        async def run_model_turn(prompt: str) -> None:
            request_id = rpc.next_id
            rpc.next_id += 1
            await rpc.send(
                {
                    "id": request_id,
                    "method": "turn/start",
                    "params": {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": prompt}],
                    },
                }
            )
            deadline = asyncio.get_running_loop().time() + 30.0
            while True:
                value = await rpc.read_line(deadline - asyncio.get_running_loop().time())
                if await rpc._handle_server_request(value):
                    continue
                rpc._observe_notification(value)
                if value.get("method") == "turn/completed":
                    turn = value.get("params", {}).get("turn", {})
                    if turn.get("status") not in {
                        "completed",
                        "succeeded",
                        "success",
                    }:
                        raise AssertionError(f"model turn did not complete: {turn!r}")
                    return

        model_server.native_thread_id = thread_id
        await run_model_turn("Attach this CAO conversation.")
        await run_model_turn("List the managed Workers.")
        await run_model_turn("Exercise the visible Delete operation.")
        await run_model_turn("Read one Work through the local supervision hub.")
        _, attached = await rpc.request(
            "mcpServer/tool/call",
            {
                "threadId": thread_id,
                "server": "cao_control_plane",
                "tool": "cao_start",
                "arguments": {"native_thread_id": thread_id},
            },
        )
        catalog_tools: list[str] = []
        deadline = asyncio.get_running_loop().time() + 2.0
        while asyncio.get_running_loop().time() < deadline:
            catalog_tools = await loaded_catalog_tools()
            if "cao_list_managed_workers" in catalog_tools:
                break
            await asyncio.sleep(0.05)
        _, repeated = await rpc.request(
            "mcpServer/tool/call",
            {
                "threadId": thread_id,
                "server": "cao_control_plane",
                "tool": "cao_start",
                "arguments": {"native_thread_id": thread_id},
            },
        )
        attached_content = attached.get("structuredContent")
        repeated_content = repeated.get("structuredContent")
        if (
            not isinstance(attached_content, dict)
            or attached_content.get("status") != "ready"
            or not isinstance(repeated_content, dict)
            or repeated_content.get("status") != "ready"
        ):
            raise AssertionError(
                f"direct attachment did not remain ready: {attached!r}; {repeated!r}"
            )
        _, listed = await rpc.request(
            "mcpServer/tool/call",
            {
                "threadId": thread_id,
                "server": "cao_control_plane",
                "tool": "cao_list_managed_workers",
                "arguments": {},
            },
        )
        result = (
            attached,
            repeated,
            listed,
            initial_catalog_tools,
            catalog_tools,
            list(model_server.requests),
        )
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()
        if process.stderr is not None:
            stderr = (await process.stderr.read()).decode("utf-8", errors="replace")
        model_server.close()
    if result is None:
        raise AssertionError("fresh CAO attachment did not return a protocol result")
    return (*result, stderr)


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS real app-server only")
def test_fresh_real_app_server_model_turns_keep_full_cao_catalog_after_start(
    tmp_path: Path,
) -> None:
    """The real model binding keeps local reads and lifecycle calls dispatchable."""

    if (shutil.which("codex") is None and not _MACOS_CODEX.is_file()) or (
        shutil.which("uv") is None and not _OWNER_UV.is_file()
    ):
        pytest.skip("installed Codex app-server and uv are required")
    port = _available_loopback_port()
    settings = replace(
        Settings(),
        state_dir=tmp_path / "state",
        runtime_launch_dir=tmp_path / "runtime-launches",
        host="127.0.0.1",
        port=port,
        public_base_url=f"http://127.0.0.1:{port}",
        trusted_hosts=("127.0.0.1",),
        allowed_origins=(f"http://127.0.0.1:{port}",),
    )
    settings.ensure_directories()
    app = create_app(settings)
    attachment_posts = 0

    @app.middleware("http")
    async def count_attachment_posts(request, call_next):
        nonlocal attachment_posts
        if request.method == "POST" and request.url.path == "/api/v1/cao-session-attachments":
            attachment_posts += 1
        return await call_next(request)

    server = _ReadyServer(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        if not server.ready.wait(10):
            raise AssertionError("fresh CAO control plane did not start")
        (
            attached,
            repeated,
            listed,
            initial_catalog_tools,
            catalog_tools,
            model_requests,
            stderr,
        ) = asyncio.run(
            _exercise_fresh_cao_attachment(
                settings=settings,
                cwd=Path(__file__).resolve().parents[1],
            )
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)

    attached_content = attached.get("structuredContent")
    repeated_content = repeated.get("structuredContent")
    assert isinstance(attached_content, dict)
    assert isinstance(repeated_content, dict)
    for start_content in (attached_content, repeated_content):
        assert start_content["status"] == "ready"
        assert start_content["attachment_verification"] == "verified"
        assert start_content["catalog_verification"] == {
            "status": "verified",
            "mcp_catalog_digest": app.state.release_identity.mcp_catalog_digest,
        }
        assert len(start_content["attachment"]["peer_binding_digest"]) == 64
    assert repeated_content["attachment"] == attached_content["attachment"]
    listed_content = listed.get("structuredContent")
    assert isinstance(listed_content, dict)
    assert set(listed_content) == {"workers"}
    assert listed_content.get("workers") == []
    assert "cao_list_managed_workers" in initial_catalog_tools
    assert "cao_delete_worker_thread" in initial_catalog_tools
    assert set(catalog_tools) == set(initial_catalog_tools)
    assert len(model_requests) == 8
    expected_model_tools = {
        "cao_start",
        "cao_get_work",
        "cao_list_managed_workers",
        "cao_delete_worker_thread",
    }
    for request_index in (0, 2, 4, 6):
        assert expected_model_tools <= set(_mcp_child_tools(model_requests[request_index]))
    delete_tool = _mcp_child_tools(model_requests[0])["cao_delete_worker_thread"]
    delete_schema = delete_tool.get("parameters")
    assert isinstance(delete_schema, dict)
    delete_properties = delete_schema.get("properties")
    assert isinstance(delete_properties, dict)
    assert {
        "worker_thread_id",
        "expected_generation",
        "idempotency_key",
    } <= set(delete_properties)
    assert "acknowledge_delete" not in delete_properties
    assert "conversation_evidence_id" not in delete_properties
    observed_outputs = {
        str(item.get("call_id"))
        for request in model_requests
        for item in request.get("input", [])
        if isinstance(item, dict) and item.get("type") == "function_call_output"
    }
    assert {
        "call-cao-start",
        "call-work-read",
        "call-worker-list",
        "call-worker-delete",
    } <= observed_outputs
    assert "Worker thread is not available in this CAO conversation" in (
        _function_call_output(model_requests[5], "call-worker-delete")
    )
    work_read_output = _function_call_output(model_requests[7], "call-work-read")
    assert "not found" in work_read_output.lower() or "not available" in work_read_output.lower()
    assert attachment_posts == 1
    bootstrap_events = app.state.service.db.fetchall(
        "SELECT id FROM events WHERE event_type = 'cao.attachment_bootstrap_issued'"
    )
    assert len(bootstrap_events) == 1
    credential_events = app.state.service.db.fetchall(
        "SELECT id FROM events WHERE event_type = 'cao.conversation_credential_issued'"
    )
    assert len(credential_events) == 1
    assert "CAO conversation attachment failed" not in stderr
    assert "cao.cab_" not in stderr
    assert "cao.csc_" not in stderr


async def _exercise_enrolled_worker_mcp(
    *, app: Any, settings: Settings, cwd: Path
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], int, str]:
    """Run the production broker → stdio bridge → real app-server path.

    No turn is started, so this validates only the deterministic protocol and
    process boundary.  The pure heartbeat call proves the real app-server kept
    the enrolled tool surface after its own initialize/tools discovery.
    """

    service = app.state.service
    cao = service.authenticate(app.state.bootstrap["tokens"]["cao"]["token"])
    worker = service.create_principal(
        cao, PrincipalCreate(name="native-enrolled-worker", role=PrincipalRole.WORKER)
    )
    runtime = service.register_runtime(
        cao,
        worker["principal"]["id"],
        RuntimeRegistration(adapter="codex-app-server", endpoint=f"{settings.public_base_url}/mcp"),
    )
    issued = service.issue_runtime_launch_ticket(runtime["id"])
    exchanges: list[str] = []
    broker = EnrollmentCapabilityBroker(
        configured_root=settings.runtime_launch_dir,
        ticket_id=str(issued["ticket_id"]),
        raw_ticket=str(issued["ticket"]),
        exchange=lambda ticket: (
            exchanges.append(ticket) or service.exchange_runtime_launch_ticket(ticket)
        ),
        delivery_failed=lambda reason: service.fail_runtime_enrollment(
            runtime["id"], reason=reason
        ),
    )
    await broker.start()
    environment = dict(os.environ)
    environment.pop("OPENAI_API_KEY", None)
    environment.pop("ANTHROPIC_API_KEY", None)
    codex_home = settings.state_dir / "codex-worker-enrollment"
    codex_home.mkdir(mode=0o700)
    environment["CODEX_HOME"] = str(codex_home)
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = (
        source_root
        if not environment.get("PYTHONPATH")
        else f"{source_root}{os.pathsep}{environment['PYTHONPATH']}"
    )
    mcp_config = _codex_thread_mcp_config(
        _enrollment_mcp_config(
            {
                "endpoint": runtime["endpoint"],
                "enrollment_capability_socket": str(broker.path),
            }
        )
    )
    process = await asyncio.create_subprocess_exec(
        "codex",
        "app-server",
        "--stdio",
        cwd=str(cwd),
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=1024 * 1024,
    )
    broker.bind_runner_pid(process.pid)
    rpc = _RecordingJsonRpcProcess(process, trusted_mcp_servers=frozenset({"cao_control_plane"}))
    result: tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]] | None = None
    stderr = ""
    try:
        await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-enrolled-worker-protocol-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        _, started = await rpc.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "config": mcp_config,
            },
        )
        thread = started.get("thread", started)
        if not isinstance(thread, dict):
            raise AssertionError(f"app-server returned non-object thread: {thread!r}")
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        if not thread_id:
            raise AssertionError("app-server did not return a thread ID")
        await rpc.wait_for_mcp_server_ready("cao_control_plane", 20.0, thread_id=thread_id)
        _, heartbeat = await rpc.request(
            "mcpServer/tool/call",
            {
                "threadId": thread_id,
                "server": "cao_control_plane",
                "tool": "cao_runtime_heartbeat",
                "arguments": {
                    "runtime_id": runtime["id"],
                    "state": "ready",
                    "lease_seconds": 60,
                    "expected_enrollment_generation": 1,
                    "sequence": 2,
                },
            },
        )
        result = (rpc.statuses, heartbeat, service.get_runtime(runtime["id"]))
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()
        if process.stderr is not None:
            stderr = (await process.stderr.read()).decode("utf-8", errors="replace")
        await broker.close()
    if result is None:
        raise AssertionError("enrolled app-server did not return a protocol result")
    return (*result, len(exchanges), stderr)


def test_real_app_server_keeps_enrolled_mcp_tools_after_stdio_initialize_without_model(
    tmp_path: Path,
) -> None:
    """The managed Worker bridge reaches ready and keeps a real MCP tool surface."""

    if shutil.which("codex") is None:
        pytest.skip("installed Codex app-server is unavailable")
    port = _available_loopback_port()
    settings = replace(
        Settings(),
        state_dir=tmp_path / "state",
        runtime_launch_dir=tmp_path / "runtime-launches",
        host="127.0.0.1",
        port=port,
        public_base_url=f"http://127.0.0.1:{port}",
        trusted_hosts=("127.0.0.1",),
        allowed_origins=(f"http://127.0.0.1:{port}",),
        runtime_timeout_seconds=20.0,
    )
    settings.ensure_directories()
    app = create_app(settings)
    server = _ReadyServer(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        if not server.ready.wait(10):
            raise AssertionError("enrolled Worker control plane did not start")
        statuses, heartbeat, enrolled, exchange_count, stderr = asyncio.run(
            _exercise_enrolled_worker_mcp(app=app, settings=settings, cwd=tmp_path)
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)

    # The installed app-server may replace its short-lived stdio child. The
    # broker must serve the replacement while the ticket exchange stays one-shot.
    assert any(status.get("status") == "ready" for status in statuses)
    assert all(status.get("status") != "failed" for status in statuses)
    assert exchange_count == 1
    assert enrolled["state"] == "busy"
    assert enrolled["enrollment"]["state"] == "ready"
    assert enrolled["enrollment"]["heartbeat_sequence"] == 2
    assert enrolled["state"] not in {"cancelled", "failed"}
    assert "MCP startup failed" not in stderr
    assert "cao.ent_" not in stderr
    # The runtime is intentionally still busy while the app-server process is
    # serving this protocol test.  A successful structured result proves the
    # real enrolled tool remained available after initialization.
    assert heartbeat["structuredContent"]["state"] in {"ready", "busy"}


def _available_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def _exercise_native_dashboard_resource(
    credentials_file: Path, cwd: Path
) -> tuple[str, dict[str, Any]]:
    environment = dict(os.environ)
    environment.pop("OPENAI_API_KEY", None)
    environment.pop("ANTHROPIC_API_KEY", None)
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = (
        source_root
        if not environment.get("PYTHONPATH")
        else f"{source_root}{os.pathsep}{environment['PYTHONPATH']}"
    )
    process = await asyncio.create_subprocess_exec(
        "codex",
        "app-server",
        "--stdio",
        cwd=str(cwd),
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=1024 * 1024,
    )
    rpc = _RecordingJsonRpcProcess(process, trusted_mcp_servers=frozenset({_DASHBOARD_SERVER_NAME}))
    try:
        await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-native-dashboard-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        _, started = await rpc.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "config": {
                    "mcp_servers": {
                        _DASHBOARD_SERVER_NAME: {
                            "command": sys.executable,
                            "args": [
                                "-m",
                                "cao_control_plane.dashboard_cli",
                                "mcp-stdio",
                                "--credentials-file",
                                str(credentials_file),
                            ],
                        }
                    }
                },
            },
        )
        thread = started.get("thread", started)
        if not isinstance(thread, dict):
            raise AssertionError(f"app-server returned non-object thread: {thread!r}")
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        if not thread_id:
            raise AssertionError("app-server did not return a thread ID")
        await rpc.wait_for_mcp_server_ready(_DASHBOARD_SERVER_NAME, 20.0, thread_id=thread_id)
        _, read = await rpc.request(
            "mcpServer/resource/read",
            {
                "threadId": thread_id,
                "server": _DASHBOARD_SERVER_NAME,
                "uri": "cao://dashboard/v1/snapshot",
            },
        )
        if not isinstance(read, dict):
            raise AssertionError(f"native dashboard resource returned non-object result: {read!r}")
        return thread_id, read
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()


def test_real_app_server_reads_one_thread_scoped_dashboard_resource_without_model_turn(
    tmp_path: Path,
) -> None:
    """Native Codex receives only the sanitized snapshot from its MCP thread."""

    if shutil.which("codex") is None:
        pytest.skip("installed Codex app-server is unavailable")
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    port = _available_loopback_port()
    settings = replace(
        Settings(),
        state_dir=state_dir,
        runtime_launch_dir=state_dir / "runtime-launches",
        public_base_url=f"http://127.0.0.1:{port}",
        enable_dashboard=True,
        require_cao_attachment_for_work=False,
    )
    settings.ensure_directories()
    app = create_app(settings)
    cao = app.state.service.authenticate(app.state.bootstrap["tokens"]["cao"]["token"])
    dashboard = app.state.service.create_principal(
        cao,
        PrincipalCreate(name="native-dashboard", role=PrincipalRole.DASHBOARD, metadata={}),
    )
    credentials_file = tmp_path / "dashboard-credentials.json"
    credentials_file.write_text(
        json.dumps(
            {
                "upstream_base_url": settings.public_base_url,
                "dashboard_bearer": dashboard["token"],
                "public_origin": None,
            }
        ),
        encoding="utf-8",
    )
    credentials_file.chmod(0o600)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                response = httpx.get(
                    f"{settings.public_base_url}/api/v1/dashboard/v1/snapshot",
                    headers={"Authorization": f"Bearer {dashboard['token']}"},
                    timeout=0.2,
                )
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        else:
            raise AssertionError("native dashboard control plane did not become ready")

        try:
            thread_id, read = asyncio.run(
                _exercise_native_dashboard_resource(credentials_file, tmp_path)
            )
        except FileNotFoundError:
            pytest.skip("installed Codex app-server is unavailable")
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)

    rendered = json.dumps(read, ensure_ascii=False)
    assert "cao-dashboard-read-model/v1" in rendered
    assert "cao://dashboard/v1/snapshot" in rendered
    assert "cao_assign" not in rendered
    assert thread_id
