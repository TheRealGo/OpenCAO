"""Retained native connection proof with a deliberately missed ready transition."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
from test_codex_thread_mcp_protocol import (
    _MULTI_SERVER_PROGRAM,
    _CatalogTurnModelServer,
    _completed_event,
    _installed_codex,
    _sse,
)

from cao_control_plane.runtime import (
    _BUNDLED_CODEX_EXECUTABLES,
    _codex_thread_mcp_config,
    _JsonRpcProcess,
    _terminate_runtime_process,
)


class _LocalCompletionModel(_CatalogTurnModelServer):
    def capture_and_respond(self, body: dict[str, Any]) -> bytes:
        with self.lock:
            self.requests.append(body)
            index = len(self.requests)
        identity = f"retained-response-{index}"
        return _sse(
            {"type": "response.created", "response": {"id": identity}},
            {"type": "response.output_item.done", "item": {
                "type": "message", "role": "assistant", "id": f"retained-message-{index}",
                "content": [{"type": "output_text", "text": "local checkpoint complete"}],
            }},
            _completed_event(identity),
        )


@pytest.mark.parametrize("provider", ["path", "bundled"])
def test_real_retained_thread_uses_its_current_connection_after_a_missed_transition(tmp_path: Path, provider: str) -> None:
    executable = (_installed_codex() if provider == "path" else next(
        (str(path) for path in _BUNDLED_CODEX_EXECUTABLES if path.is_file() and os.access(path, os.X_OK)), None,
    ))
    if executable is None:
        pytest.skip("installed provider is unavailable")
    name = "cao_managed_retained_probe"
    server = tmp_path / "pure-mcp.py"
    server.write_text(_MULTI_SERVER_PROGRAM, encoding="utf-8")
    provider_home = tmp_path / "provider-home"
    provider_home.mkdir(mode=0o700)
    model = _LocalCompletionModel()
    model.start()
    (provider_home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="local_mock"\napproval_policy="never"\nsandbox_mode="read-only"\n'
        '[model_providers.local_mock]\nname="Local retained protocol probe"\n'
        f'base_url="{model.url}/v1"\nwire_api="responses"\nrequest_max_retries=0\nstream_max_retries=0\nsupports_websockets=false\n',
        encoding="utf-8",
    )
    environment = {key: value for key, value in os.environ.items()
        if key not in {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "CODEX_THREAD_ID"}}
    environment.update(CODEX_HOME=str(provider_home), CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG="1")

    class Rpc(_JsonRpcProcess):
        drop_ready = False
        dropped = 0
        status_reads = 0

        def _observe_notification(self, value: Any) -> None:
            params = value.get("params", {})
            if (self.drop_ready and value.get("method") == "mcpServer/startupStatus/updated"
                    and params.get("name") == name and params.get("status") == "ready"):
                self.dropped += 1
                return
            super()._observe_notification(value)

        async def request(self, method: str, params: Any, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            if method == "mcpServerStatus/list":
                assert params["serverName"] == name and params["threadId"] == native
                self.status_reads += 1
            return await super().request(method, params, **kwargs)

    def config(epoch: str) -> dict[str, Any]:
        return _codex_thread_mcp_config({"mcpServers": {"cao_control_plane": {
            "command": sys.executable,
            "args": [str(server), name, epoch, str(tmp_path / f"{epoch}.json")],
        }}}, server_name=name, startup_timeout_seconds=30)

    async def connect() -> Rpc:
        process = await asyncio.create_subprocess_exec(executable, "app-server", "--stdio",
            cwd=tmp_path, env=environment, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
        rpc = Rpc(process, trusted_mcp_servers=frozenset({name}))
        await rpc.request("initialize", {"clientInfo": {"name": "retained-mcp-probe", "version": "1"},
            "capabilities": {"experimentalApi": True}})
        await rpc.send({"method": "initialized", "params": {}})
        return rpc

    async def complete_turn(rpc: Rpc) -> None:
        _, result = await rpc.request("turn/start", {"threadId": native,
            "input": [{"type": "text", "text": "Return the local checkpoint."}]})
        expected = result["turn"]["id"]
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            value = await rpc.read_line(deadline - asyncio.get_running_loop().time())
            rpc._observe_notification(value)
            if await rpc._handle_server_request(value):
                continue
            turn = value.get("params", {}).get("turn", {})
            if value.get("method") == "turn/completed" and turn.get("id") == expected:
                assert turn["status"] == "completed"
                return

    native = ""

    async def exercise() -> None:
        nonlocal native
        first = await connect()
        try:
            _, result = await first.request("thread/start", {"cwd": str(tmp_path),
                "approvalPolicy": "never", "sandbox": "read-only", "config": config("initial")})
            native = result["thread"]["id"]
            await first.wait_for_mcp_server_ready(name, 30, thread_id=native)
            await complete_turn(first)
        finally:
            await _terminate_runtime_process(first.process)
        second = await connect()
        second.drop_ready = True
        try:
            _, result = await second.request("thread/resume", {"threadId": native, "excludeTurns": True,
                "cwd": str(tmp_path), "config": config("replacement")})
            assert result["thread"]["id"] == native
            deadline = asyncio.get_running_loop().time() + 30
            while second.dropped == 0:
                value = await second.read_line(deadline - asyncio.get_running_loop().time())
                second._observe_notification(value)
                await second._handle_server_request(value)
            source = await second.wait_for_mcp_server_ready(name, 30, thread_id=native, query_current_status=True)
            assert source == "thread_status" and second.status_reads == 1 and second.dropped > 0
            _, invoked = await second.request("mcpServer/tool/call", {"threadId": native,
                "server": name, "tool": f"probe_{name}", "arguments": {}})
            assert invoked["structuredContent"]["marker"] == "replacement"
            await complete_turn(second)
        finally:
            await _terminate_runtime_process(second.process)

    try:
        asyncio.run(exercise())
    finally:
        model.close()
    assert len(model.requests) == 2
    assert json.loads((tmp_path / "replacement.json").read_text()).count("tools/call") == 1
