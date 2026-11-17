from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from test_runtime_enrollment import _runtime, _Stdin

from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    RuntimeDispatchPhaseError,
    _codex_delivery_client_user_message_id,
    _managed_codex_mcp_server_name,
)


@pytest.mark.parametrize("mode,code", [
    ("delayed", None), ("lost_event", None), ("event_first", None),
    ("empty_reply", "runtime_turn_activation_invalid"),
    ("unrelated_reply", "runtime_assignment_start_timeout"),
    ("rejected", "runtime_rpc_rejected"),
    ("conflicting_turn", "runtime_turn_activation_invalid"),
])
def test_exact_turn_activation_reply_survives_delayed_input_event(settings, tmp_path, monkeypatch, mode, code):
    """Input preparation may outlast startup after the exact Turn is already created."""
    monkeypatch.setattr("cao_control_plane.runtime.shutil.which", lambda _name: None)
    monkeypatch.setattr("cao_control_plane.runtime._BUNDLED_CODEX_EXECUTABLES", ())
    runtime = _runtime(tmp_path / "fixture.sock")
    runtime["metadata"]["command"] = ["fixture-codex", "app-server", "--stdio"]
    message = {"id": "msg-activation-receipt", "kind": "instruction", "payload": {"message": "Finite local checkpoint."}}
    server_name = _managed_codex_mcp_server_name(runtime)
    observations = []
    writes = []
    runtime["_worker_dispatch_observer"] = observations.append

    class Process:
        def __init__(self):
            self.pid, self.returncode = 1, 0
            self.stdin = _Stdin()
            writes.append(self.stdin.writes)
            self.stdout = asyncio.StreamReader()
            self.stderr = None
            for value in (
                {"id": 1, "result": {}},
                {"id": 2, "result": {"thread": {"id": "native-activation"}}},
                {"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": "native-activation", "name": server_name, "status": "ready"}},
            ):
                self.feed(value)
            user = {"method": "item/started", "params": {"threadId": "native-activation", "turnId": "exact-turn",
                "item": {"type": "userMessage", "clientId": _codex_delivery_client_user_message_id(message)}}}
            if mode == "event_first":
                self.feed(user)
            reply = {"id": 3, "result": {"turn": {"id": "exact-turn", "status": "inProgress", "items": []}}}
            if mode == "empty_reply":
                reply["result"] = {}
            elif mode == "unrelated_reply":
                reply["id"] = 99
            elif mode == "rejected":
                reply = {"id": 3, "error": {"code": -32600, "message": "private-provider-sentinel"}}
            self.feed(reply)
            if mode == "delayed":
                asyncio.get_running_loop().call_later(.1, self.feed, user)
            elif mode == "conflicting_turn":
                user["params"]["turnId"] = "different-turn"
                self.feed(user)

        def feed(self, value):
            self.stdout.feed_data((json.dumps(value) + "\n").encode())

    async def spawn(*_args, **_kwargs):
        assert _args[:3] == ("fixture-codex", "app-server", "--stdio")
        process = Process()
        asyncio.get_running_loop().call_later(.15, process.feed, {
            "method": "turn/completed", "params": {"threadId": "native-activation",
                "turn": {"id": "exact-turn", "status": "completed"}}})
        return process

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    operation = CodexAppServerAdapter(replace(settings,
        runtime_mcp_startup_timeout_seconds=.05, runtime_timeout_seconds=2)).dispatch(runtime, message)
    if code:
        with pytest.raises(RuntimeDispatchPhaseError) as failure:
            asyncio.run(operation)
        assert failure.value.metadata["failure_code"] == code
        assert "private-provider-sentinel" not in str(failure.value)
    else:
        result = asyncio.run(operation)
        assert result.success
        assert [event.kind for event in observations] == ["turn_started"]
        assert observations[0].activation_source == ("user_message_event" if mode == "event_first" else "turn_start_reply")
    requests = [json.loads(value) for value in writes[0]]
    assert not any(request.get("method", "").startswith("thread/queue/") for request in requests)
    starts = [request for request in requests if request.get("method") == "turn/start"]
    assert len(starts) == 1 and starts[0]["params"]["clientUserMessageId"] == _codex_delivery_client_user_message_id(message)
