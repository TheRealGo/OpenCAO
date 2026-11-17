from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from test_runtime_enrollment import _runtime, _Stdin

from cao_control_plane.mcp import _conversation_work_projection
from cao_control_plane.models import RuntimeDispatchResult, WorkAssignment
from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    CodexRpcError,
    Dispatcher,
    RuntimeAdapterError,
    RuntimeDispatchPhaseError,
    _durable_dispatch_summary,
    _JsonRpcProcess,
    _managed_codex_mcp_server_name,
)


def test_failed_exact_mcp_startup_is_reported_before_waiting_for_timeout(settings, tmp_path, monkeypatch):
    runtime = _runtime(tmp_path / "broker.sock")
    runtime["metadata"]["command"] = ["fixture-codex", "app-server", "--stdio"]
    name = _managed_codex_mcp_server_name(runtime)
    writes = []

    class Process:
        pid = 1
        returncode = 0
        stderr = None

        def __init__(self):
            self.stdin = _Stdin()
            writes.append(self.stdin.writes)
            self.stdout = asyncio.StreamReader()
            for value in (
                {"id": 1, "result": {}},
                {"id": 2, "result": {"thread": {"id": "managed-thread"}}},
                {"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": "managed-thread", "name": name, "status": "failed",
                    "failureReason": "reauthenticationRequired", "error": "private-provider-body /private/source",
                }},
            ):
                self.stdout.feed_data((json.dumps(value) + "\n").encode())

    async def spawn(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    with pytest.raises(RuntimeDispatchPhaseError) as failed:
        asyncio.run(CodexAppServerAdapter(replace(settings, runtime_mcp_startup_timeout_seconds=.05)).dispatch(
            runtime, {"id": "msg-fixture", "kind": "instruction", "payload": {"message": "Finite checkpoint."}}))
    assert failed.value.metadata == {
        "delivery_acceptance": "not_submitted", "dispatch_phase": "mcp_startup",
        "failure_code": "mcp_startup_failed", "mcp_startup_status": "failed",
        "mcp_startup_failure_reason": "reauthentication_required",
    }
    assert "private-provider-body" not in repr(failed.value)
    assert not any(json.loads(value).get("method") == "turn/start" for value in writes[0])
    started = next(json.loads(value) for value in writes[0] if json.loads(value).get("method") == "thread/start")
    assert started["params"]["config"][f"mcp_servers.{name}"]["startup_timeout_sec"] == .05


def test_startup_timeout_retains_only_the_exact_thread_state():
    class Process:
        def __init__(self):
            self.stdin = _Stdin()
            self.stdout = asyncio.StreamReader()

    async def exercise():
        rpc = _JsonRpcProcess(Process())
        for thread, state in ((None, "ready"), ("other-thread", "ready"), ("managed-thread", "starting")):
            rpc._observe_notification({"method": "mcpServer/startupStatus/updated", "params": {
                "threadId": thread, "name": "managed", "status": state, "error": "private-provider-body",
            }})
        with pytest.raises(RuntimeAdapterError) as failed:
            await rpc.wait_for_mcp_server_ready("managed", .01, thread_id="managed-thread")
        assert str(failed.value) == "mcp_startup_timeout"
        assert failed.value.diagnostics == {"mcp_startup_status": "starting"}

    asyncio.run(exercise())


def test_startup_diagnostics_never_persist_arbitrary_provider_fields():
    result = RuntimeDispatchResult(success=False, state="failed", error="mcp_startup_failed", metadata={
        "mcp_startup_status": "failed", "mcp_startup_failure_reason": "private-provider-body",
        "mcp_startup_probe_status": {"private": "provider-body"}, "mcp_startup_probe_catalog": "private-provider-body",
        "mcp_startup_readiness_source": {"private": "provider-body"},
        "raw_error": "private-provider-body", "native_thread_id": "private-native-thread",
    })
    summary = _durable_dispatch_summary(result)
    assert summary["diagnostics"]["mcp_startup_status"] == "failed"
    assert "mcp_startup_failure_reason" not in summary["diagnostics"]
    assert "mcp_startup_probe_status" not in summary["diagnostics"]
    assert "mcp_startup_readiness_source" not in summary["diagnostics"]
    assert "private-" not in json.dumps(summary)


def test_cao_reads_only_the_exact_assignment_startup_diagnosis(system):
    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(system["cao"], WorkAssignment(
        worker_id=system["worker"]["id"], runtime_session_id=runtime["id"],
        title="Startup diagnosis", objective="Observe bounded failure evidence.", acceptance=["No input is submitted."]))

    class FailedAdapter:
        async def dispatch(self, _runtime, _message):
            return RuntimeDispatchResult(success=False, state="failed", error="mcp_startup_failed", metadata={
                "delivery_acceptance": "not_submitted", "dispatch_phase": "mcp_startup",
                "mcp_startup_status": "failed", "mcp_startup_failure_reason": "reauthentication_required",
                "mcp_startup_probe_status": "authenticationRequired", "mcp_startup_probe_catalog": "unavailable",
                "raw_error": "private-provider-body",
            })

    class Registry:
        def get(self, _name):
            return FailedAdapter()

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 1
    current = _conversation_work_projection(service.get_work(work["id"]))
    diagnosis = current["current_attempt"]["assignment_delivery"]
    assert diagnosis["mcp_startup_status"] == "failed"
    assert diagnosis["mcp_startup_failure_reason"] == "reauthentication_required"
    assert diagnosis["mcp_startup_probe_status"] == "authenticationRequired"
    assert diagnosis["mcp_startup_probe_catalog"] == "unavailable"
    assert "private-provider-body" not in json.dumps(current)
    metadata = service.get_runtime(runtime["id"])["metadata"]
    metadata["last_dispatch_message_id"] = "msg-other-assignment"
    service.db.execute("UPDATE runtime_sessions SET metadata_json=? WHERE id=?", (json.dumps(metadata), runtime["id"]))
    stale = service.get_work(work["id"])["current_attempt"]["assignment_delivery"]
    assert "mcp_startup_status" not in stale and "mcp_startup_failure_reason" not in stale


@pytest.mark.parametrize(("failure", "stage", "state", "code"), [
    (FileNotFoundError("private-provider-body"), "spawn", "unavailable", "runtime_recovery_launch_unavailable"),
    (TimeoutError("private-provider-body"), "initialize", "unavailable", "runtime_recovery_timeout"),
    (CodexRpcError({"code": -32600, "message": "thread not loaded: 00000000-0000-0000-0000-000000000001"}),
     "thread_read", "blocked", "runtime_thread_not_loaded"),
    (CodexRpcError({"code": -32600, "message": "private-provider-body"}),
     "thread_read", "unavailable", "runtime_recovery_rpc_rejected"),
])
def test_recovery_probe_retains_fixed_failure_and_stage(settings, tmp_path, monkeypatch, failure, stage, state, code):
    calls = []

    async def spawn(*_args, **_kwargs):
        if stage == "spawn":
            raise failure
        return object()

    async def terminate(_process):
        pass

    class Rpc:
        def __init__(self, _process):
            pass

        async def send(self, _message):
            pass

        async def request(self, method, params, **_kwargs):
            calls.append(method)
            if method == ("initialize" if stage == "initialize" else "thread/read"):
                raise failure
            return 1, {}

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr("cao_control_plane.runtime._default_codex_app_server_command", lambda: ["fixture-codex"])
    monkeypatch.setattr("cao_control_plane.runtime._terminate_runtime_process", terminate)
    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcProcess", Rpc)
    observation = asyncio.run(CodexAppServerAdapter(settings).probe_worker_recovery(
        {"native_session_id": "00000000-0000-0000-0000-000000000001", "metadata": {}}, release_id="fixture-release"))
    assert (observation.state, observation.code, observation.probe_stage) == (state, code, stage)
    assert "private-" not in repr(observation)
    assert set(calls) <= {"initialize", "thread/read"}
