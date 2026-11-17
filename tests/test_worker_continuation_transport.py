"""Real runner process boundaries and local fake Responses; no vendor model."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import sys
import threading
from dataclasses import replace
from pathlib import Path

import pytest
import uvicorn
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_codex_thread_mcp_protocol import (
    _PROBE_ENV,
    _PROBE_SERVER_PROGRAM,
    _CatalogTurnModelServer,
    _completed_event,
    _sse,
)
from test_managed_worker_command_admission import _new_thread, _thread_route
from test_worker_mcp_live_e2e import _ReadyServer

from cao_control_plane.api import create_app
from cao_control_plane.config import Settings
from cao_control_plane.models import InstructWorkerThreadInput, WorkerThreadLifecycleInput
from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    Dispatcher,
    RuntimeDispatchPhaseError,
    _codex_delivery_client_user_message_id,
    _JsonRpcProcess,
    _terminate_runtime_process,
)


class _LocalModel(_CatalogTurnModelServer):
    def capture_and_respond(self, body):
        self.requests.append(body)
        response_id = f"response-{len(self.requests)}"
        return _sse(
            {"type": "response.created", "response": {"id": response_id}},
            {"type": "response.output_item.done", "item": {
                "type": "message", "role": "assistant", "id": f"output-{len(self.requests)}",
                "content": [{"type": "output_text", "text": "isolated checkpoint verified"}],
            }},
            _completed_event(response_id),
        )


def _isolated_environment(root: Path, model: _LocalModel) -> dict[str, str]:
    codex_home = root / "provider-home"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="mock_provider"\n'
        'approval_policy="never"\nsandbox_mode="read-only"\n'
        '[model_providers.mock_provider]\nname="local fixture"\n'
        f'base_url="{model.url}/v1"\nwire_api="responses"\n'
        'request_max_retries=0\nstream_max_retries=0\nsupports_websockets=false\n',
        encoding="utf-8",
    )
    return {"CODEX_HOME": str(codex_home), "CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG": "1",
            _PROBE_ENV: str(root / "mcp-methods.json")}


async def _host(codex: str, root: Path, environment: dict[str, str]):
    env = {k: v for k, v in os.environ.items()
           if k not in {"CODEX_THREAD_ID", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"}}
    env.update(environment)
    process = await asyncio.create_subprocess_exec(
        codex, "app-server", "--stdio", cwd=root, env=env,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, start_new_session=True,
    )
    rpc = _JsonRpcProcess(process)
    await rpc.request("initialize", {
        "clientInfo": {"name": "isolated-continuation-host", "version": "1"},
        "capabilities": {"experimentalApi": True},
    })
    await rpc.send({"method": "initialized", "params": {}})
    return process, rpc


@pytest.mark.parametrize("condition", ["clean_reconnect", "idle_writer", "pending_input"])
def test_real_worker_second_assignment_across_processes(tmp_path, monkeypatch, condition):
    codex = shutil.which("codex")
    if codex is None:
        pytest.skip("installed Codex App Server is unavailable")
    model = _LocalModel()
    model.start()
    environment = _isolated_environment(tmp_path, model)
    # This fixture isolates provider transport only. The real enrollment broker
    # and its dispatch CAS are covered separately; no production CAO is attached.
    monkeypatch.setattr("cao_control_plane.runtime._enrollment_mcp_config", lambda _: {
        "mcpServers": {"cao_control_plane": {
            "command": sys.executable, "args": ["-c",
                f"import os; os.environ[{_PROBE_ENV!r}]={environment[_PROBE_ENV]!r}\n" + _PROBE_SERVER_PROGRAM],
        }},
    })
    settings = replace(Settings(), state_dir=tmp_path / "state",
        runtime_launch_dir=tmp_path / "launches", runtime_mcp_startup_timeout_seconds=15)
    settings.ensure_directories()
    observations, outputs = [], []
    runtime = {"id": "run-transport-fixture", "native_session_id": "",
        "managed_worker_spec": {"id": "mws-transport-fixture", "effective_model": "mock-model",
                                "effective_reasoning_effort": "low"},
        "metadata": {"command": [codex, "app-server", "--stdio"], "cwd": str(tmp_path),
                     "environment": environment},
        "_worker_dispatch_observer": observations.append, "_worker_output_observer": outputs.append}

    async def exercise():
        adapter = CodexAppServerAdapter(settings)
        first = await adapter.dispatch(runtime, {"id": "msg-first", "kind": "instruction",
                                               "payload": {"message": "Complete isolated checkpoint one."}})
        assert first.success
        original = first.native_session_id
        assert original
        runtime["native_session_id"] = original
        terminal = await adapter.probe_worker_recovery(
            {**runtime, "diagnostic_message_id": "msg-first"}, release_id="fixture-release")
        assert terminal.input_state == "terminal" and terminal.history_complete
        assert terminal.code == "runtime_assignment_terminal" and len(model.requests) == 1
        process = None
        rpc = None
        try:
            if condition != "clean_reconnect":
                process, rpc = await _host(codex, tmp_path, environment)
                if condition == "idle_writer":
                    await rpc.request("thread/resume", {"threadId": original, "excludeTurns": True,
                                                       "config": {"mcp_servers": {}}})
                else:
                    await rpc.request("thread/queue/add", {"threadId": original,
                        "input": [{"type": "text", "text": "Historical input must remain unexecuted."}],
                        "clientUserMessageId": "historical-input"})
                    diagnosis = await adapter.probe_worker_recovery(runtime, release_id="fixture-release")
                    assert diagnosis.state == "blocked" and diagnosis.code == "runtime_thread_queue_blocked"
                    assert len(model.requests) == 1
                    await rpc.request("thread/queue/add", {"threadId": original,
                        "input": [{"type": "text", "text": "Exact retained Assignment must remain unexecuted."}],
                        "clientUserMessageId": _codex_delivery_client_user_message_id({"id": "msg-retained"})})
                    diagnosis = await adapter.probe_worker_recovery(
                        {**runtime, "diagnostic_message_id": "msg-retained"}, release_id="fixture-release")
                    assert diagnosis.code == "runtime_assignment_pending" and diagnosis.input_state == "pending"
                    assert diagnosis.other_pending_input and diagnosis.history_complete
                    assert len(model.requests) == 1
            second_message = {"id": "msg-second", "kind": "instruction",
                              "payload": {"message": "Complete isolated checkpoint two."}}
            if condition == "pending_input":
                with pytest.raises(RuntimeDispatchPhaseError) as failure:
                    await adapter.dispatch(runtime, second_message)
                assert failure.value.metadata["delivery_acceptance"] == "not_submitted"
                assert failure.value.metadata["failure_code"] == "runtime_thread_queue_blocked"
                _, queue = await rpc.request("thread/queue/list", {"threadId": original})
                assert [i["clientUserMessageId"] for i in queue["data"]] == [
                    "historical-input", _codex_delivery_client_user_message_id({"id": "msg-retained"})]
                assert len(model.requests) == 1
                return
            second = await adapter.dispatch(runtime, second_message)
            assert second.success
            assert second.metadata["delivery_acceptance"] == "completed"
            assert len(model.requests) == 2
            assert "isolated checkpoint verified" in json.dumps(model.requests[1]["input"])
            assert sum(event.kind == "turn_started" for event in observations) == 2
            assert len([event for event in outputs if event.kind == "turn_end"]) == 2
            if condition == "idle_writer":
                assert second.native_session_id != original
                rebound = [event for event in observations if event.kind == "thread_rebound"]
                assert len(rebound) == 1
                assert rebound[0].previous_native_thread_id == original
                # Reading the original stays possible; it never receives input two.
                _, source = await rpc.request("thread/turns/list", {"threadId": original, "limit": 10})
                assert len(source["data"]) == 1
            else:
                assert second.native_session_id == original
        finally:
            if process is not None:
                await _terminate_runtime_process(process)
    try:
        asyncio.run(exercise())
    finally:
        model.close()


@pytest.mark.parametrize("idle_writer,drop_input_events", [(False, False), (True, False), (False, True)])
def test_real_scoped_mcp_and_provider_start_across_two_worker_launches(tmp_path, monkeypatch, idle_writer, drop_input_events):
    """SQLite -> Dispatcher -> real broker/MCP/runner -> persisted start/output."""
    codex = shutil.which("codex")
    if codex is None:
        pytest.skip("installed Codex App Server is unavailable")
    if drop_input_events:
        original_read = _JsonRpcProcess.read_line

        async def without_input_event(rpc, timeout):
            while True:
                value = await original_read(rpc, timeout)
                if value.get("method") == "item/started" and value.get("params", {}).get("item", {}).get("type") == "userMessage":
                    continue
                return value

        monkeypatch.setattr(_JsonRpcProcess, "read_line", without_input_event)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"
    settings = replace(Settings(), state_dir=tmp_path / "state", runtime_launch_dir=tmp_path / "launches",
        host="127.0.0.1", port=port, public_base_url=base_url, trusted_hosts=("127.0.0.1",),
        allowed_origins=(base_url,), runtime_mcp_startup_timeout_seconds=15)
    settings.ensure_directories()
    app = create_app(settings)
    # Only the actual Worker delivery is driven. This isolated fixture never
    # launches a CAO model or touches an owner's production attachment.
    server = _ReadyServer(uvicorn.Config(app, lifespan="off", log_level="error", access_log=False))
    server_thread = threading.Thread(target=lambda: server.run(sockets=[listener]), daemon=True)
    model = _LocalModel()
    model.start()
    environment = _isolated_environment(tmp_path, model)
    server_thread.start()
    try:
        assert server.ready.wait(10)
        service = app.state.service
        attached = attach_cao_session_with_peer(service, current_cao_session_attachment(
            native_thread_id="isolated-continuation-supervisor", project_digest="c" * 64,
        ))
        cao = service.authenticate(attached["context_token"])
        worker_thread_id = _new_thread(service, cao, tmp_path, suffix="transport")
        route = _thread_route(service, worker_thread_id)
        principal_id = route["principal_id"]
        runtime_id = route["runtime_session_id"]

        class IsolatedAdapter(CodexAppServerAdapter):
            def prepare_launch(self, runtime):
                prepared = dict(super().prepare_launch(runtime))
                prepared["metadata"] = {**prepared.get("metadata", {}), "environment": environment}
                return prepared

        class Registry:
            def get(self, _name):
                return IsolatedAdapter(settings)

        dispatcher = Dispatcher(service, settings, registry=Registry())
        native_ids = []

        async def launch_checkpoint(index):
            instructed = service.instruct_worker_thread(cao, InstructWorkerThreadInput(
                worker_thread_id=worker_thread_id, expected_purpose_version=1,
                instruction=f"Verify finite local checkpoint {index}; preserve evidence for review.",
                idempotency_key=f"checkpoint-{index}"))
            work = service.get_work(instructed["task"]["work_item_id"])
            service.reconcile_completed_worker_turns()
            service.reconcile_terminal_headless_delivery_lanes()
            # A supervisor notice from the prior checkpoint remains separate.
            # Claim the exact Worker lane through the real canonical selector.
            delivery = dispatcher._claim_delivery()
            while delivery is not None and delivery["recipient_id"] != principal_id:
                # Do not dispatch CAO models in this fixture. Definitively
                # unsubmitted notices stay queued for their real owner.
                service.db.execute("UPDATE message_deliveries SET state='queued', owner_token='', "
                    "lease_until=NULL, next_attempt_at='2999-01-01T00:00:00Z' WHERE message_id=? AND recipient_id=?",
                    (delivery["message_id"], delivery["recipient_id"]))
                delivery = dispatcher._claim_delivery()
            assert delivery is not None
            assert service.db.fetchone("SELECT work_item_id FROM messages WHERE id=?", (delivery["message_id"],))["work_item_id"] == work["id"]
            await dispatcher._process_delivery(delivery)
            current = service.get_work(work["id"])
            activity = current["current_attempt"]["activity"]
            assert activity["provider_queue_acceptance"] is None
            assert activity["provider_turn_start"]["source"] == "provider_transport"
            assert activity["execution_start"] is None  # no fabricated Worker business-action report
            assert service.db.fetchone("SELECT COUNT(*) n FROM worker_output_receipts WHERE attempt_id=? AND event_kind='turn_end'",
                (current["current_attempt"]["id"],))["n"] == 1
            native_ids.append(service.get_runtime(runtime_id)["native_session_id"])

        async def exercise():
            await launch_checkpoint(1)
            retained = None
            try:
                if idle_writer:
                    retained, rpc = await _host(codex, tmp_path, environment)
                    await rpc.request("thread/resume", {"threadId": native_ids[0], "excludeTurns": True,
                                                       "config": {"mcp_servers": {}}})
                await launch_checkpoint(2)
            finally:
                if retained is not None:
                    await _terminate_runtime_process(retained)

        asyncio.run(exercise())
        assert bool(native_ids[0])
        assert (native_ids[0] != native_ids[1]) is idle_writer
        assert len(model.requests) == 2
        assert service.db.fetchone("SELECT COUNT(*) n FROM runtime_enrollment_tickets WHERE state='consumed'")["n"] == 2
        assert service.db.fetchone("SELECT COUNT(*) n FROM runtime_credentials WHERE state='active'")["n"] == 0
        assert not list(settings.runtime_launch_dir.rglob("*.sock"))
        service.finish_worker_thread(cao, WorkerThreadLifecycleInput(
            worker_thread_id=worker_thread_id, idempotency_key="cleanup-isolated-worker"))
        assert service.db.fetchone("SELECT state FROM managed_worker_threads WHERE id=?", (worker_thread_id,))["state"] == "archived"
    finally:
        server.should_exit = True
        server_thread.join(timeout=10)
        listener.close()
        model.close()
        assert not server_thread.is_alive()
