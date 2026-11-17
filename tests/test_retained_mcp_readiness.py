from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from test_runtime_enrollment import _runtime

from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    RuntimeDispatchPhaseError,
    _durable_dispatch_summary,
    _managed_codex_mcp_server_name,
)


def _retained_dispatch(settings, tmp_path, monkeypatch, snapshot, *, notification=""):
    runtime = _runtime(tmp_path / "broker.sock")
    runtime["native_session_id"] = "retained-thread"
    runtime["metadata"]["command"] = ["fixture-codex", "app-server", "--stdio"]
    name = _managed_codex_mcp_server_name(runtime)
    calls = []

    class Process:
        pid = 1
        returncode = 0
        stderr = None

        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.stdin = self

        def write(self, value):
            request = json.loads(value)
            if "id" not in request:
                return
            method, params = request["method"], request.get("params", {})
            calls.append((method, params))
            if method == "thread/resume":
                self.feed({"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": "retained-thread", "name": name, "status": "starting",
                }})
                result = {"thread": {"id": "retained-thread"}}
            elif method == "mcpServerStatus/list":
                if snapshot == "unsupported":
                    self.feed({"id": request["id"], "error": {"code": -32601, "message": "unsupported"}})
                    self.feed({"method": "mcpServer/startupStatus/updated", "params": {
                        "threadId": "retained-thread", "name": name, "status": "ready",
                    }})
                    return
                entry = {"name": name, "runtimeStatus": "connected", "authStatus": "unsupported",
                    "toolsError": None, "tools": {"probe": {"name": "probe", "inputSchema": {"type": "object"}}}}
                entry.update(snapshot)
                result = {"data": [entry], "nextCursor": None}
            elif method == "turn/start":
                result = {"turn": {"id": "turn-fixture", "status": "inProgress"}}
                self.feed({"id": request["id"], "result": result})
                self.feed({"method": "turn/completed", "params": {
                    "threadId": "retained-thread", "turn": {"id": "turn-fixture", "status": "completed"},
                }})
                return
            else:
                result = {}
            self.feed({"id": request["id"], "result": result})
            if notification:
                self.feed({"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": "another-thread", "name": name, "status": notification,
                }})

        def feed(self, value):
            self.stdout.feed_data((json.dumps(value) + "\n").encode())

        async def drain(self):
            pass

    async def spawn(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    return (lambda: asyncio.run(CodexAppServerAdapter(replace(settings,
        runtime_mcp_startup_timeout_seconds=.05)).dispatch(runtime, {
            "id": "msg-retained", "kind": "instruction", "payload": {"message": "A later checkpoint."},
        }))), calls, name


def test_retained_current_connection_is_ready_without_a_replayed_transition(settings, tmp_path, monkeypatch):
    run, calls, name = _retained_dispatch(settings, tmp_path, monkeypatch, {})
    result = run()
    assert result.success
    assert _durable_dispatch_summary(result)["diagnostics"]["mcp_startup_readiness_source"] == "thread_status"
    assert _durable_dispatch_summary(result)["diagnostics"]["mcp_startup_ready_notification_observed"] is False
    assert [method for method, _ in calls] == ["initialize", "thread/resume", "mcpServerStatus/list", "turn/start"]
    assert calls[2][1] == {"threadId": "retained-thread", "serverName": name,
        "detail": "toolsAndAuthOnly", "limit": 1}


@pytest.mark.parametrize("snapshot,expected", [
    ({"runtimeStatus": None}, "unavailable"),
    ({"runtimeStatus": "starting"}, "starting"),
    ({"runtimeStatus": "failed"}, "failed"),
    ({"runtimeStatus": {"private": "value"}}, "unavailable"),
    ({"name": "another-server"}, "invalid"),
    ({"tools": {}}, "connected"),
    ({"tools": {"probe": {"name": "probe", "inputSchema": None}}}, "connected"),
    ({"toolsError": "private-provider-body /private/source"}, "connected"),
])
def test_retained_snapshot_never_accepts_cached_or_unusable_catalogs(settings, tmp_path, monkeypatch, snapshot, expected):
    run, calls, _ = _retained_dispatch(settings, tmp_path, monkeypatch, snapshot, notification="ready")
    with pytest.raises(RuntimeDispatchPhaseError) as failure:
        run()
    assert failure.value.metadata["failure_code"] == "mcp_startup_timeout"
    assert failure.value.metadata["mcp_startup_probe_status"] == expected
    assert failure.value.metadata["delivery_acceptance"] == "not_submitted"
    assert "turn/start" not in [method for method, _ in calls]
    assert "private-provider-body" not in repr(failure.value)


def test_retained_status_read_cannot_veto_an_exact_ready_notification(settings, tmp_path, monkeypatch):
    run, calls, _ = _retained_dispatch(settings, tmp_path, monkeypatch, "unsupported")
    result = run()
    assert result.success and result.metadata["mcp_startup_readiness_source"] == "notification"
    assert sum(method == "mcpServerStatus/list" for method, _ in calls) == 1
