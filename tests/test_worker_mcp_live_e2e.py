from __future__ import annotations

import asyncio
import json
import os
import select
import socket
import subprocess
import sys
import threading
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.api import create_app
from cao_control_plane.config import Settings
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    MessageKind,
    PrincipalCreate,
    RuntimeDispatchResult,
    RuntimeRegistration,
    RuntimeState,
    WorkAssignment,
)
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    receive_enrollment_capability,
)


class _ReadyServer(uvicorn.Server):
    """Expose uvicorn's one real startup boundary without readiness polling."""

    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self.ready = threading.Event()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self.ready.set()


class _BrokerLoop:
    """Run the real asynchronous one-shot broker beside this synchronous E2E."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.started = False
        self.errors: list[BaseException] = []
        self.thread = threading.Thread(
            target=self._run,
            name="live-mcp-enrollment-broker",
            daemon=True,
        )

    def _run(self) -> None:
        try:
            asyncio.set_event_loop(self.loop)
            self.ready.set()
            self.loop.run_forever()
        except BaseException as error:  # pragma: no cover - asserted by callers
            self.errors.append(error)
        finally:
            self.loop.close()

    def start(self) -> None:
        self.thread.start()
        self.started = True
        assert self.ready.wait(10), self.errors

    def run(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=10)

    def bind(self, broker: EnrollmentCapabilityBroker, pid: int) -> None:
        async def bind_in_loop() -> None:
            broker.bind_runner_pid(pid)

        self.run(bind_in_loop())

    def stop(self) -> None:
        if not self.started:
            self.loop.close()
            return
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)
        assert not self.thread.is_alive()
        assert not self.errors


def _request(request_id: int, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    values = dict(params or {})
    values["_meta"] = {
        PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
        CLIENT_CAPABILITIES_META_KEY: {},
        CLIENT_INFO_META_KEY: {"name": "live-worker-e2e", "version": "1"},
    }
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": values}


def _tool_call(request_id: int, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return _request(request_id, "tools/call", {"name": name, "arguments": arguments})


def _read_response(process: subprocess.Popen[str], captured: list[str]) -> dict[str, Any]:
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], 10)
    assert readable, f"MCP stdio bridge did not respond; returncode={process.poll()}"
    line = process.stdout.readline()
    captured.append(line)
    assert line, f"MCP stdio bridge closed stdout; returncode={process.poll()}"
    response = json.loads(line)
    assert "error" not in response, response
    return response


def _send(
    process: subprocess.Popen[str], request: dict[str, Any], captured: list[str]
) -> dict[str, Any]:
    assert process.stdin is not None
    process.stdin.write(json.dumps(request) + "\n")
    process.stdin.flush()
    return _read_response(process, captured)


def _structured(response: dict[str, Any]) -> dict[str, Any]:
    return response["result"]["structuredContent"]


def _sqlite_bytes(database_path: Path) -> bytes:
    return b"".join(
        candidate.read_bytes()
        for candidate in (
            database_path,
            Path(f"{database_path}-wal"),
            Path(f"{database_path}-shm"),
        )
        if candidate.exists()
    )


def _close_process(process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None and not stream.closed:
            stream.close()


class _CAOBrokerAdapter:
    """Consume the real one-shot CAO capability without starting a model."""

    name = "codex-app-server"

    def __init__(self, service: Any) -> None:
        self.service = service
        self.delivered = threading.Event()
        self.calls: list[dict[str, Any]] = []

    async def dispatch(
        self, runtime: Mapping[str, Any], message: Mapping[str, Any]
    ) -> RuntimeDispatchResult:
        broker = runtime.get("_enrollment_capability_broker")
        assert isinstance(broker, EnrollmentCapabilityBroker)
        broker.bind_runner_pid(os.getpid())
        capability_path = runtime.get("cao_runtime_capability_socket")
        assert capability_path == broker.path
        exchange = await receive_enrollment_capability(broker.path, timeout_seconds=2)
        actor = self.service.authenticate(str(exchange["token"]))
        assert actor["_runtime_session_id"] == runtime["id"]
        assert actor["_native_thread_id"] == runtime["native_session_id"]
        self.service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
        boundary_id = str(message["payload"].get("boundary_id") or "")
        assert boundary_id
        work = self.service.get_work(str(message["work_item_id"]))
        turn = self.service.acquire_reasoner_turn(
            actor,
            str(message["work_item_id"]),
            boundary_id=boundary_id,
            expected_generation=int(work["generation"]),
            idempotency_key=f"live-e2e:{message['id']}",
        )
        self.service.dispose_boundary(
            actor,
            boundary_id,
            BoundaryDispositionInput(
                turn_id=turn["id"],
                lease_token=turn["lease_token"],
                expected_generation=int(work["generation"]),
                kind=BoundaryDispositionKind.ACCEPT,
                reason="the deterministic CAO adapter verified the completion boundary",
            ),
        )
        self.service.mark_message_handled(
            actor,
            str(message["id"]),
            evidence="deterministic CAO broker adapter consumed the boundary",
        )
        self.calls.append({"runtime": dict(runtime), "message": dict(message), "actor": actor})
        self.delivered.set()
        return RuntimeDispatchResult(
            success=True,
            native_session_id=str(runtime["native_session_id"]),
            state=RuntimeState.READY,
            output="deterministic CAO broker adapter handled the boundary",
        )


class _CAOBrokerRegistry:
    def __init__(self, adapter: _CAOBrokerAdapter) -> None:
        self.adapter = adapter

    def get(self, name: str) -> _CAOBrokerAdapter:
        assert name == "codex-app-server"
        return self.adapter


def test_managed_worker_mcp_stdio_live_enrollment_and_completion_delivery(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
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
        dispatcher_recovery_scan_seconds=60.0,
        runtime_timeout_seconds=10.0,
    )
    settings.ensure_directories()
    app = create_app(settings)
    bearer_values: list[str] = []

    @app.middleware("http")
    async def capture_bridge_bearer(request, call_next):
        authorization = request.headers.get("authorization", "")
        if authorization.startswith("Bearer cao.rtc_"):
            bearer_values.append(authorization.removeprefix("Bearer "))
        return await call_next(request)

    server = _ReadyServer(
        uvicorn.Config(app, log_config=None, access_log=False, log_level="warning")
    )
    server_errors: list[BaseException] = []

    def run_server() -> None:
        try:
            server.run(sockets=[listener])
        except BaseException as error:  # pragma: no cover - asserted through startup boundary
            server_errors.append(error)

    thread = threading.Thread(target=run_server, name="live-mcp-e2e-uvicorn", daemon=True)
    broker_loop = _BrokerLoop()
    broker: EnrollmentCapabilityBroker | None = None
    broker_two: EnrollmentCapabilityBroker | None = None
    process: subprocess.Popen[str] | None = None
    process_two: subprocess.Popen[str] | None = None
    captured_output: list[str] = []
    try:
        thread.start()
        assert server.ready.wait(10), server_errors
        broker_loop.start()

        service = app.state.service
        cao = service.authenticate(app.state.bootstrap["tokens"]["cao"]["token"])
        attachment = attach_cao_session_with_peer(
            service,
            current_cao_session_attachment(
                native_thread_id="live-e2e-cao-thread",
                project_digest="a" * 64,
            ),
        )
        conversation_cao = service.authenticate(attachment["context_token"])
        worker = service.create_principal(
            cao, PrincipalCreate(name="live-stdio-worker", role="worker")
        )
        runtime = service.register_runtime(
            cao,
            worker["principal"]["id"],
            RuntimeRegistration(adapter="claude", endpoint=f"{base_url}/mcp"),
        )
        launch = service.issue_runtime_launch_ticket(runtime["id"])
        ticket = str(launch["ticket"])
        broker = EnrollmentCapabilityBroker(
            configured_root=settings.runtime_launch_dir / runtime["id"],
            ticket_id=str(launch["ticket_id"]),
            raw_ticket=ticket,
            exchange=service.exchange_runtime_launch_ticket,
            delivery_failed=lambda reason: service.fail_runtime_enrollment(
                runtime["id"], reason=reason
            ),
        )
        broker_loop.run(broker.start())

        env = os.environ | {
            "PYTHONPATH": os.pathsep.join(
                value
                for value in (str(Path.cwd() / "src"), os.environ.get("PYTHONPATH", ""))
                if value
            )
        }
        process = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-m",
                "cao_control_plane.cli",
                "mcp-stdio",
                "--url",
                f"{base_url}/mcp",
                "--enrollment-broker-socket",
                str(broker.path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        broker_loop.bind(broker, process.pid)

        tools = _send(process, _request(1, "tools/list"), captured_output)
        assert any(tool["name"] == "cao_report" for tool in tools["result"]["tools"])
        assert broker.path.exists()
        ticket_row = service.db.fetchone(
            "SELECT state FROM runtime_enrollment_tickets WHERE id = ?", (launch["ticket_id"],)
        )
        assert ticket_row is not None
        assert ticket_row["state"] == "consumed"
        enrolled = service.get_runtime(runtime["id"])["enrollment"]
        assert enrolled["state"] == "ready"
        assert enrolled["protocol_version"] == MCP_LATEST_VERSION
        assert enrolled["heartbeat_sequence"] == 1
        assert enrolled["discovered_tools_digest"]
        assert len(set(bearer_values)) == 1
        bearer = bearer_values[0]

        # Start a second, independent real Worker bridge while the first is
        # still connected.  Each has a different principal, runtime, one-shot
        # broker, ticket, and resulting runtime credential.
        worker_two = service.create_principal(
            cao, PrincipalCreate(name="live-stdio-worker-two", role="worker")
        )
        runtime_two = service.register_runtime(
            cao,
            worker_two["principal"]["id"],
            RuntimeRegistration(adapter="claude", endpoint=f"{base_url}/mcp"),
        )
        launch_two = service.issue_runtime_launch_ticket(runtime_two["id"])
        ticket_two = str(launch_two["ticket"])
        broker_two = EnrollmentCapabilityBroker(
            configured_root=settings.runtime_launch_dir / runtime_two["id"],
            ticket_id=str(launch_two["ticket_id"]),
            raw_ticket=ticket_two,
            exchange=service.exchange_runtime_launch_ticket,
            delivery_failed=lambda reason: service.fail_runtime_enrollment(
                runtime_two["id"], reason=reason
            ),
        )
        broker_loop.run(broker_two.start())
        process_two = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-m",
                "cao_control_plane.cli",
                "mcp-stdio",
                "--url",
                f"{base_url}/mcp",
                "--enrollment-broker-socket",
                str(broker_two.path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        broker_loop.bind(broker_two, process_two.pid)
        tools_two = _send(process_two, _request(101, "tools/list"), captured_output)
        assert any(tool["name"] == "cao_report" for tool in tools_two["result"]["tools"])
        assert broker_two.path.exists()
        ticket_row_two = service.db.fetchone(
            "SELECT state FROM runtime_enrollment_tickets WHERE id = ?", (launch_two["ticket_id"],)
        )
        assert ticket_row_two is not None
        assert ticket_row_two["state"] == "consumed"
        enrolled_two = service.get_runtime(runtime_two["id"])["enrollment"]
        assert enrolled_two["state"] == "ready"
        assert enrolled_two["protocol_version"] == MCP_LATEST_VERSION
        assert enrolled_two["heartbeat_sequence"] == 1
        assert enrolled_two["discovered_tools_digest"]
        all_bearers = set(bearer_values)
        assert len(all_bearers) == 2
        bearer_two = next(value for value in all_bearers if value != bearer)

        # Assign only after each real bridge's tools/list discovery and
        # automatic heartbeat made its own runtime ready.
        work = service.assign_work(
            conversation_cao,
            WorkAssignment(
                worker_id=worker["principal"]["id"],
                title="Live MCP Worker completion",
                objective="Complete the real stdio Worker control-plane path",
                acceptance=["Completion claim reaches a durable CAO boundary"],
                non_goals=["Do not use a mocked HTTP client"],
                runtime_session_id=runtime["id"],
                idempotency_key="live-worker-mcp-assignment",
            ),
        )
        attempt = work["current_attempt"]
        work_two = service.assign_work(
            conversation_cao,
            WorkAssignment(
                worker_id=worker_two["principal"]["id"],
                title="Second live MCP Worker assignment",
                objective="Prove concurrent Worker isolation over the real bridge",
                acceptance=["Only this Worker can read and acknowledge its assignment"],
                non_goals=["Do not share the first Worker's inbox"],
                runtime_session_id=runtime_two["id"],
                idempotency_key="live-worker-mcp-assignment-two",
            ),
        )
        attempt_two = work_two["current_attempt"]

        context = _structured(_send(process, _tool_call(2, "cao_get_context", {}), captured_output))
        assert context["work"]["id"] == work["id"]
        assert context["attempt"]["id"] == attempt["id"]
        context_two = _structured(
            _send(process_two, _tool_call(102, "cao_get_context", {}), captured_output)
        )
        assert context_two["work"]["id"] == work_two["id"]
        assert context_two["attempt"]["id"] == attempt_two["id"]

        inbox = _structured(
            _send(
                process,
                _tool_call(3, "cao_get_inbox", {"attempt_id": attempt["id"]}),
                captured_output,
            )
        )
        assert {item["work_item_id"] for item in inbox["items"]} == {work["id"]}
        assignment = next(item for item in inbox["items"] if item["kind"] == "assignment")
        acknowledged = _structured(
            _send(
                process,
                _tool_call(4, "cao_ack", {"message_ids": [assignment["id"]]}),
                captured_output,
            )
        )
        assert assignment["id"] in acknowledged["acknowledged"]

        inbox_two = _structured(
            _send(
                process_two,
                _tool_call(103, "cao_get_inbox", {"attempt_id": attempt_two["id"]}),
                captured_output,
            )
        )
        assert {item["work_item_id"] for item in inbox_two["items"]} == {work_two["id"]}
        assignment_two = next(item for item in inbox_two["items"] if item["kind"] == "assignment")
        acknowledged_two = _structured(
            _send(
                process_two,
                _tool_call(104, "cao_ack", {"message_ids": [assignment_two["id"]]}),
                captured_output,
            )
        )
        assert assignment_two["id"] in acknowledged_two["acknowledged"]

        cao_adapter = _CAOBrokerAdapter(service)
        app.state.dispatcher.registry = _CAOBrokerRegistry(cao_adapter)
        reported = _structured(
            _send(
                process,
                _tool_call(
                    5,
                    "cao_report",
                    {
                        "attempt_id": attempt["id"],
                        "kind": "completion_claim",
                        "expected_goal_version": work["goal_version"],
                        "expected_goal_packet_digest": attempt["goal_packet_digest"],
                        "expected_task_packet_digest": attempt["task_packet_digest"],
                        "expected_generation": work["generation"],
                        "summary": "Live MCP stdio worker completed the assigned path",
                        "trajectory": "complete",
                        "evidence": [{"check": "live-stdio", "result": "pass"}],
                        "idempotency_key": "live-worker-mcp-completion",
                    },
                ),
                captured_output,
            )
        )
        assert reported["state"] == "waiting_supervisor"
        boundary = reported["open_boundaries"][0]
        # The completion commit must wake the in-process Dispatcher without
        # waiting for an interval scan or starting a real Codex app-server.
        assert cao_adapter.delivered.wait(10)
        assert len(cao_adapter.calls) == 1
        delivery = service.db.fetchone(
            """
            SELECT d.*
            FROM message_deliveries AS d
            JOIN messages AS m ON m.id = d.message_id
            WHERE d.recipient_id = ? AND m.work_item_id = ?
              AND json_extract(m.payload_json, '$.boundary_id') = ?
            """,
            (cao["id"], work["id"], boundary["id"]),
        )
        assert delivery is not None
        assert delivery["state"] == "handled"
        assignment_delivery = service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (assignment["id"], worker["principal"]["id"]),
        )
        assert assignment_delivery is not None
        assert assignment_delivery["state"] == "handled"
        assignment_delivery_two = service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (assignment_two["id"], worker_two["principal"]["id"]),
        )
        assert assignment_delivery_two is not None
        assert assignment_delivery_two["state"] == "acknowledged"

        successor = service.send_message(
            cao,
            [worker["principal"]["id"]],
            kind=MessageKind.INSTRUCTION,
            payload={"message": "Apply the review correction."},
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
        )
        successor_inbox = _structured(
            _send(process, _tool_call(6, "cao_get_inbox", {}), captured_output)
        )
        assert [item["id"] for item in successor_inbox["items"]] == [successor["id"]]

        replay = httpx.post(
            f"{base_url}/api/v1/runtime-enrollment:exchange", json={"ticket": ticket}, timeout=10
        )
        assert replay.status_code == 404
        assert ticket not in replay.text
        assert bearer not in replay.text
        with pytest.raises(AuthenticationError):
            service.exchange_runtime_launch_ticket(ticket)
        with pytest.raises(AuthenticationError):
            service.exchange_runtime_launch_ticket(ticket_two)

        assert process.stdin is not None
        process.stdin.close()
        assert process.wait(timeout=10) == 0
        assert process.stderr is not None
        captured_output.append(process.stdout.read() if process.stdout is not None else "")
        captured_output.append(process.stderr.read())
        assert process_two.stdin is not None
        process_two.stdin.close()
        assert process_two.wait(timeout=10) == 0
        assert process_two.stderr is not None
        captured_output.append(process_two.stdout.read() if process_two.stdout is not None else "")
        captured_output.append(process_two.stderr.read())
        log_output = "\n".join(record.getMessage() for record in caplog.records)
        for protected_value in (
            ticket,
            ticket_two,
            bearer,
            bearer_two,
            str(broker.path),
            str(broker_two.path),
        ):
            assert protected_value.encode() not in _sqlite_bytes(settings.database_path)
            assert protected_value not in "".join(captured_output)
            assert protected_value not in log_output
    finally:
        _close_process(process_two)
        _close_process(process)
        if broker_two is not None:
            broker_loop.run(broker_two.close())
            assert not broker_two.path.exists()
        if broker is not None:
            broker_loop.run(broker.close())
            assert not broker.path.exists()
        broker_loop.stop()
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        assert not thread.is_alive()
        assert not server_errors
