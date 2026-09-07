"""Test-only helpers for the managed Worker MCP enrollment contract.

These helpers intentionally use the public service lifecycle.  They do not
seed enrollment rows or mint credentials directly, so regressions exercise the
same ticket and handshake boundaries that a launched Worker uses.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from cao_control_plane.models import (
    RuntimeDispatchResult,
    RuntimeHeartbeat,
    RuntimeRegistration,
)
from cao_control_plane.runtime_enrollment import (
    EnrollmentCapabilityBroker,
    receive_enrollment_capability,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane


def enroll_ready_worker_runtime(
    service: ControlPlane,
    cao: dict[str, Any],
    worker_id: str,
    *,
    adapter: str = "claude",
    metadata: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Register and complete one managed Worker runtime handshake."""

    runtime = service.register_runtime(
        cao,
        worker_id,
        RuntimeRegistration(adapter=adapter, metadata=metadata or {"command": ["/bin/cat"]}),
    )
    ticket = service.issue_runtime_launch_ticket(runtime["id"])["ticket"]
    exchange = service.exchange_runtime_launch_ticket(ticket)
    credential = str(exchange["token"])
    actor = service.authenticate(credential)
    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=actor["_enrollment_generation"],
            sequence=1,
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'ready' WHERE id = ?", (runtime["id"],)
    )
    return service.get_runtime(runtime["id"]), actor, credential


class EnrollmentHandshakeAdapter:
    """A launched Worker double that consumes the per-delivery ticket safely."""

    def __init__(
        self,
        service: ControlPlane,
        *,
        after_handshake: Callable[[Mapping[str, Any], Mapping[str, Any]], None] | None = None,
        native_session_id: str = "",
    ) -> None:
        self.service = service
        self.after_handshake = after_handshake
        self.native_session_id = native_session_id
        self.deliveries: list[dict[str, str]] = []
        self.actors: list[dict[str, Any]] = []
        self.credentials: list[str] = []

    async def dispatch(
        self, runtime: Mapping[str, Any], message: Mapping[str, Any]
    ) -> RuntimeDispatchResult:
        broker = runtime.get("_enrollment_capability_broker")
        assert isinstance(broker, EnrollmentCapabilityBroker)
        broker.bind_runner_pid(os.getpid())
        socket_path = runtime.get("enrollment_capability_socket")
        assert socket_path == broker.path
        exchange = await receive_enrollment_capability(
            broker.path,
            timeout_seconds=5,
        )
        credential = str(exchange["token"])
        actor = self.service.authenticate(credential)
        assert str(exchange["runtime_id"]) == str(runtime["id"])
        assert actor["id"] == runtime["principal_id"]
        self.service.record_mcp_tool_discovery(
            actor,
            protocol_version="2025-06-18",
            tool_names=WORKER_MCP_REQUIRED_TOOLS,
        )
        self.service.heartbeat_runtime(
            actor,
            str(runtime["id"]),
            RuntimeHeartbeat(
                expected_enrollment_generation=actor["_enrollment_generation"],
                sequence=1,
            ),
        )
        self.actors.append(actor)
        self.credentials.append(credential)
        if self.after_handshake is not None:
            self.after_handshake(runtime, message)
        self.deliveries.append(
            {
                "runtime_id": str(runtime["id"]),
                "message_id": str(message["id"]),
                "capability_path": str(socket_path),
            }
        )
        return RuntimeDispatchResult(
            success=True,
            state="ready",
            output="managed Worker MCP handshake completed",
            native_session_id=self.native_session_id,
            metadata={"managed_mcp": "handshake-complete"},
        )


class EnrollmentHandshakeRegistry:
    def __init__(self, adapter: EnrollmentHandshakeAdapter) -> None:
        self.adapter = adapter

    def get(self, name: str) -> EnrollmentHandshakeAdapter:
        assert name in {"claude", "codex-app-server"}
        return self.adapter
