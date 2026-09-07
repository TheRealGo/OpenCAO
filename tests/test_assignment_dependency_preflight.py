from __future__ import annotations

import asyncio
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import EnrollmentHandshakeAdapter, EnrollmentHandshakeRegistry

from cao_control_plane.models import (
    AckInput,
    AssignmentDependency,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    ReportInput,
    ReportKind,
    RuntimeDispatchResult,
    RuntimeState,
    WorkAssignment,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import Dispatcher, check_docker_api_ping
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    receive_enrollment_capability,
)


def _assignment(system, *, key: str = "docker-preflight"):
    actor = system.get("attached_cao") or system["cao"]
    return system["service"].assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Docker API prerequisite",
            objective="Run dynamic validation only after the Docker daemon is ready.",
            acceptance=["Dynamic validation completed"],
            runtime_session_id=system["runtime"]["id"],
            dependencies=[AssignmentDependency.DOCKER_API_PING],
            idempotency_key=key,
        ),
    )


def _assignment_delivery(service, attempt_id: str) -> dict[str, object]:
    row = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.kind = 'assignment' AND m.attempt_id = ?
        """,
        (attempt_id,),
    )
    assert row is not None
    return dict(row)


def _sink_contains(service, sentinel: str) -> bool:
    for table, column in (
        ("message_deliveries", "last_error"),
        ("events", "data_json"),
        ("boundaries", "metadata_json"),
        ("messages", "payload_json"),
        ("runtime_sessions", "metadata_json"),
        ("goal_revisions", "packet_json"),
        ("source_receipts", "payload_json"),
        ("idempotency_results", "result_json"),
    ):
        row = service.db.fetchone(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
            (f"%{sentinel}%",),
        )
        assert row is not None
        if int(row["count"]):
            return True
    return False


class _CAOWakeAdapter:
    name = "codex-app-server"

    def __init__(self, service: Any) -> None:
        self.service = service
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
        self.calls.append({"runtime": dict(runtime), "message": dict(message), "actor": actor})

        # Complete a real CAO-owned disposition so Dispatcher can prove the
        # wake reached the exact attached conversation without manufacturing
        # a semantic-recovery wake for an intentionally incomplete fake turn.
        self.service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
        work = self.service.get_work(str(message["work_item_id"]))
        boundary_id = str(message["payload"]["boundary_id"])
        turn = self.service.acquire_reasoner_turn(
            actor,
            str(message["work_item_id"]),
            boundary_id=boundary_id,
            expected_generation=int(work["generation"]),
            idempotency_key=f"dependency-wake:{message['id']}",
        )
        self.service.dispose_boundary(
            actor,
            boundary_id,
            BoundaryDispositionInput(
                turn_id=turn["id"],
                lease_token=turn["lease_token"],
                expected_generation=int(work["generation"]),
                kind=BoundaryDispositionKind.FAIL,
                reason="The fake CAO turn recorded the unavailable prerequisite.",
            ),
        )
        self.service.mark_message_handled(
            actor,
            str(message["id"]),
            evidence="the exact attached CAO turn disposed the dependency boundary",
        )
        return RuntimeDispatchResult(
            success=True,
            native_session_id=str(runtime["native_session_id"]),
            state=RuntimeState.READY,
            output="fake attached CAO handled dependency boundary",
        )


class _DependencyRegistry:
    def __init__(
        self,
        worker_adapter: EnrollmentHandshakeAdapter,
        cao_adapter: _CAOWakeAdapter,
    ) -> None:
        self.worker_adapter = worker_adapter
        self.cao_adapter = cao_adapter

    def get(self, name: str) -> EnrollmentHandshakeAdapter | _CAOWakeAdapter:
        if name == "claude":
            return self.worker_adapter
        assert name == "codex-app-server"
        return self.cao_adapter


@pytest.mark.parametrize(
    "outcome",
    [
        False,
        PermissionError("owner-private-locator-sentinel"),
        FileNotFoundError("owner-private-locator-sentinel"),
        ValueError("HTTP 000 invalid-response owner-private-locator-sentinel"),
        TimeoutError("timeout owner-private-locator-sentinel"),
    ],
)
def test_failed_dependency_is_unstarted_one_shot_and_wakes_exact_cao(system, outcome):
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="docker-preflight-cao-thread",
            project_digest="d" * 64,
        ),
    )
    system["attached_cao"] = service.authenticate(attachment["context_token"])
    work = _assignment(system, key=f"failure-{type(outcome).__name__}")
    adapter = EnrollmentHandshakeAdapter(service)
    cao_adapter = _CAOWakeAdapter(service)
    calls: list[str] = []

    async def check(dependency: str) -> bool:
        calls.append(dependency)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=_DependencyRegistry(adapter, cao_adapter),
        assignment_dependency_checker=check,
    )
    assert asyncio.run(dispatcher.run_once()) == 1
    assert calls == ["docker_api_ping"]
    assert adapter.deliveries == []

    current = service.get_work(work["id"])
    attempt = current["current_attempt"]
    delivery = _assignment_delivery(service, attempt["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["attention_owner"] == "cao"
    assert attempt["state"] == "waiting_supervisor"
    assert delivery["state"] == "dead"
    assert delivery["attempts"] == 1
    assert delivery["last_error"] == "assignment_dependency_unavailable"
    assert service.get_runtime(system["runtime"]["id"])["state"] == "ready"
    enrollment = service.db.fetchone(
        "SELECT state, generation FROM worker_enrollments WHERE runtime_session_id = ?",
        (system["runtime"]["id"],),
    )
    assert enrollment is not None
    assert dict(enrollment) == {"state": "ready", "generation": 1}
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM effect_operations")["count"] == 0

    assert len(current["open_boundaries"]) == 1
    boundary = current["open_boundaries"][0]
    assert boundary["kind"] == "blocker"
    assert boundary["runtime_state"] == "ready"
    assert boundary["metadata"] == {
        "dependency": "docker_api_ping",
        "pre_dispatch": True,
        "reason": "assignment_dependency_unavailable",
    }
    wake = service.db.fetchone(
        """
        SELECT m.payload_json, d.recipient_id, d.runtime_session_id, d.state
        FROM messages AS m JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE json_extract(m.payload_json, '$.action') =
              'review_assignment_dependency_blocker'
        """
    )
    assert wake is not None
    assert wake["recipient_id"] == system["cao"]["id"]
    assert wake["state"] == "queued"
    assert wake["runtime_session_id"] == attachment["runtime_session_id"]
    assert json.loads(wake["payload_json"])["boundary_id"] == boundary["id"]

    # The second cycle performs the exact attached-conversation CAO wake.  It
    # must not repeat the Worker dependency check or reach the Worker adapter.
    assert asyncio.run(dispatcher.run_once()) == 1
    assert len(cao_adapter.calls) == 1
    assert cao_adapter.calls[0]["runtime"]["id"] == attachment["runtime_session_id"]
    assert cao_adapter.calls[0]["runtime"]["native_session_id"] == ("docker-preflight-cao-thread")
    assert cao_adapter.calls[0]["message"]["payload"]["boundary_id"] == boundary["id"]
    assert calls == ["docker_api_ping"]
    assert adapter.deliveries == []
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM boundaries WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM messages "
            "WHERE work_item_id = ? AND json_extract(payload_json, '$.action') = "
            "'review_assignment_dependency_blocker'",
            (work["id"],),
        )["count"]
        == 1
    )
    assert build_projection(service.db).healthy is True
    assert not _sink_contains(service, "owner-private-locator-sentinel")
    assert not _sink_contains(service, "HTTP 000")


def test_ready_receipt_is_exact_and_new_attempt_rechecks(system):
    service = system["service"]
    work = _assignment(system, key="ready-then-recovery")
    calls: list[str] = []
    reported: list[dict[str, Any]] = []

    async def ready_then_fail(dependency: str) -> bool:
        calls.append(dependency)
        return len(calls) == 1

    adapter: EnrollmentHandshakeAdapter

    def report_recovery_boundary(_runtime: Mapping[str, Any], message: Mapping[str, Any]) -> None:
        reported.append(
            service.report(
                adapter.actors[-1],
                str(message["attempt_id"]),
                ReportInput(
                    kind=ReportKind.BLOCKER,
                    expected_goal_version=int(message["goal_version"]),
                    expected_goal_packet_digest=str(message["goal_packet_digest"]),
                    expected_task_packet_digest=str(message["task_packet_digest"]),
                    expected_generation=int(message["payload"]["generation"]),
                    summary="The explicit dependency recovery step is now required.",
                    idempotency_key="explicit-dependency-recovery",
                ),
            )
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=report_recovery_boundary)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
        assignment_dependency_checker=ready_then_fail,
    )
    assert asyncio.run(dispatcher.run_once()) == 1
    first = service.get_work(work["id"])["current_attempt"]
    first_delivery = _assignment_delivery(service, first["id"])
    assert first_delivery["state"] == "handled"
    assert len(adapter.deliveries) == 1
    receipt = service.db.fetchone(
        """
        SELECT data_json FROM events
        WHERE event_type = 'runtime.assignment_dependency_ready'
        """
    )
    assert receipt is not None
    receipt_data = json.loads(receipt["data_json"])
    assert receipt_data["work_item_id"] == work["id"]
    assert receipt_data["attempt_id"] == first["id"]
    assert receipt_data["message_id"] == first_delivery["message_id"]
    assert receipt_data["delivery_generation"] == first_delivery["generation"]
    assert receipt_data["work_generation"] == work["generation"]
    assert receipt_data["task_packet_digest"] == first["task_packet_digest"]

    # A real Worker report creates the canonical recovery boundary.  Explicit
    # CAO RETRY creates a new Attempt, which must perform a fresh preflight;
    # the first Attempt's readiness receipt is evidence, never authority.
    assert len(reported) == 1
    waiting = service.get_work(work["id"])
    boundary = waiting["open_boundaries"][0]
    notification = next(
        item
        for item in service.get_inbox(system["cao"], attempt_id=first["id"])["items"]
        if item["kind"] == ReportKind.BLOCKER.value
        and item["payload"].get("boundary_id") == boundary["id"]
    )
    service.acknowledge(system["cao"], AckInput(message_ids=[notification["id"]]))
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="dependency-recovery-turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind=BoundaryDispositionKind.RETRY,
            reason="Dependency recovery was explicitly requested.",
        ),
    )
    # Public disposition settles the explicitly received CAO delivery, leaving
    # only the new Worker Assignment dispatchable.
    assert service.get_inbox(system["cao"])["items"] == []
    second = service.get_work(work["id"])["current_attempt"]
    assert second["id"] != first["id"]
    assert second["task_packet_digest"] != first["task_packet_digest"]
    assert asyncio.run(dispatcher.run_once()) == 1
    assert calls == ["docker_api_ping", "docker_api_ping"]
    assert len(adapter.deliveries) == 1
    second_delivery = _assignment_delivery(service, second["id"])
    assert second_delivery["state"] == "dead"
    assert service.get_work(work["id"])["state"] == "waiting_supervisor"
    assert (
        len(
            service.db.fetchall(
                "SELECT sequence FROM events WHERE event_type = 'runtime.assignment_dependency_ready'"
            )
        )
        == 1
    )


def test_absent_dependency_preserves_legacy_request_and_packet_digests(system):
    service = system["service"]
    plain = WorkAssignment(
        worker_id=system["worker"]["id"],
        title="Legacy digest",
        objective="Keep the dependency-free representation unchanged.",
        acceptance=["No new dependency key"],
        runtime_session_id=system["runtime"]["id"],
    )
    assert "dependencies" not in plain.model_dump(mode="json")
    work = service.assign_work(system["cao"], plain)
    goal_packet = work["goal_history"][0]["packet"]
    message = service.db.fetchone(
        "SELECT payload_json FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (work["current_attempt"]["id"],),
    )
    assert "dependencies" not in goal_packet
    assert message is not None
    assert "dependencies" not in json.loads(message["payload_json"])


def test_default_checker_accepts_only_exact_http_200_ok(monkeypatch):
    counter = 0

    async def serve(
        response: bytes, *, split: int = 0, keep_open: bool = False
    ) -> tuple[bool, bytes]:
        nonlocal counter
        counter += 1
        # macOS limits AF_UNIX locator length to roughly 100 bytes; pytest's
        # nested temporary path can exceed that before the test starts.
        socket_path = Path(tempfile.gettempdir()) / (f"cao-docker-{os.getpid()}-{counter}.sock")
        request = bytearray()
        release = asyncio.Event()

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            request.extend(await reader.readuntil(b"\r\n\r\n"))
            chunks = (response[:split], response[split:]) if split else (response,)
            for chunk in chunks:
                writer.write(chunk)
                await writer.drain()
                await asyncio.sleep(0)
            if keep_open:
                await release.wait()
            writer.close()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        monkeypatch.setattr("cao_control_plane.runtime._DOCKER_API_SOCKET", socket_path)
        try:
            result = await asyncio.wait_for(check_docker_api_ping(), timeout=1)
        finally:
            release.set()
            server.close()
            await server.wait_closed()
            socket_path.unlink(missing_ok=True)
        return result, bytes(request)

    ok, request = asyncio.run(
        serve(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK",
            split=17,
            keep_open=True,
        )
    )
    assert ok is True
    assert request == (b"GET /_ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    for response in (
        b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nOK\n",
        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 2\r\n\r\nOK",
        b"not-http\r\n\r\nOK",
    ):
        invalid, _ = asyncio.run(serve(response))
        assert invalid is False

    monkeypatch.setattr("cao_control_plane.runtime._DOCKER_API_PING_TIMEOUT_SECONDS", 0.01)
    timed_out, _ = asyncio.run(serve(b"", keep_open=True))
    assert timed_out is False
