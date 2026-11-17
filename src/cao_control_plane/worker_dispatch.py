"""Exact provider dispatch observations; transport repair grants no task authority."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .database import utc_now
from .errors import ConflictError
from .security import canonical_json

if TYPE_CHECKING:
    from .service import ControlPlane


@dataclass(frozen=True, slots=True)
class WorkerDispatchObservation:
    kind: Literal["thread_rebound", "queue_accepted", "turn_started"]
    native_thread_id: str
    previous_native_thread_id: str = ""
    turn_id: str = ""
    activation_source: Literal["", "turn_start_reply", "user_message_event"] = ""


RUNTIME_REPAIR_FAILURE_CODES = frozenset({
    "runtime_rpc_rejected", "runtime_thread_writer_conflict", "runtime_thread_not_found",
    "runtime_thread_queue_blocked", "runtime_thread_source_unsettled",
    "runtime_assignment_start_timeout", "mcp_startup_timeout", "mcp_startup_failed",
    "runtime_turn_activation_invalid",
    "runtime_process_exited", "runtime_dispatch_failed",
})


def _digest(value: str) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def observe_dispatch(
    service: ControlPlane, *, runtime_id: str, attempt_id: str,
    delivery_message_id: str, delivery_generation: int, enrollment_generation: int,
    owner_token: str, event: WorkerDispatchObservation,
) -> None:
    """Keep queue admission, provider start and Worker execution reports distinct.

    Only the owning adapter may replace a native handle, before submitting
    input, after proving an idle source with no pending native input. The
    logical Worker, Work, packets, purpose and effect fences stay unchanged.
    Native handles remain private; events contain only opaque provenance.
    """
    if (event.kind not in {"thread_rebound", "queue_accepted", "turn_started"}
            or not event.native_thread_id or len(event.native_thread_id) > 512
            or len(event.previous_native_thread_id) > 512 or len(event.turn_id) > 512
            or (event.kind == "turn_started" and not event.turn_id)):
        raise ConflictError("invalid Worker dispatch observation")
    with service.db.transaction() as connection:
        row = connection.execute(
            """SELECT s.*, r.native_session_id, r.state AS runtime_state,
                      w.goal_version, w.generation AS current_generation, w.state AS work_state,
                      w.managed_worker_thread_id, w.managed_worker_thread_generation,
                      a.worker_id, a.goal_version AS attempt_goal_version,
                      e.generation AS current_enrollment_generation, e.state AS enrollment_state,
                      d.state AS delivery_state, d.owner_token
               FROM worker_output_streams s
               JOIN runtime_sessions r ON r.id = s.runtime_session_id
               JOIN attempts a ON a.id = s.attempt_id
               JOIN work_items w ON w.id = s.work_item_id
               JOIN worker_enrollments e ON e.runtime_session_id = r.id
               JOIN message_deliveries d ON d.message_id = s.source_message_id
                 AND d.runtime_session_id = r.id AND d.recipient_id = a.worker_id
               WHERE s.runtime_session_id = ? AND s.attempt_id = ?
                 AND s.source_message_id = ? AND s.delivery_generation = ?
                 AND s.enrollment_generation = ? AND d.generation = s.delivery_generation""",
            (runtime_id, attempt_id, delivery_message_id, delivery_generation, enrollment_generation),
        ).fetchone()
        latest = service._latest_attempt_tx(connection, str(row["work_item_id"])) if row else None
        if (row is None or not owner_token
                or not hmac.compare_digest(str(row["owner_token_digest"]), _digest(owner_token))
                or row["delivery_state"] != "dispatched" or row["owner_token"] != owner_token
                or row["runtime_state"] != "busy" or row["settled_at"]
                or row["work_state"] in {"completed", "canceled", "failed"}
                or row["current_generation"] != row["work_generation"]
                or row["goal_version"] != row["attempt_goal_version"]
                or row["enrollment_state"] in {"revoked", "failed", "stale"}
                or row["current_enrollment_generation"] not in {
                    enrollment_generation - 1, enrollment_generation,
                }
                or latest is None or latest["id"] != attempt_id):
            raise ConflictError("Worker dispatch observation lost its exact authority")
        thread = connection.execute(
            "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
            (row["managed_worker_thread_id"],),
        ).fetchone()
        if row["managed_worker_thread_id"] and (
                thread is None or thread["state"] != "active"
                or thread["generation"] != row["managed_worker_thread_generation"]):
            raise ConflictError("Worker dispatch targets a retired logical thread")
        if event.kind == "thread_rebound":
            unsafe = connection.execute(
                """SELECT 1 WHERE
                    EXISTS (SELECT 1 FROM effect_operations WHERE principal_id = ?
                            AND status IN ('started', 'unknown'))
                    OR EXISTS (SELECT 1 FROM message_deliveries d JOIN messages m ON m.id = d.message_id
                       JOIN attempts other ON other.id = m.attempt_id
                       WHERE d.recipient_id = ? AND m.id <> ?
                         AND m.kind IN ('assignment', 'instruction', 'status_request')
                         AND d.state IN ('dispatched', 'delivered', 'acknowledged')
                         AND (d.runtime_session_id = ? OR other.state IN ('assigned', 'accepted', 'working')
                              OR (m.work_item_id = ? AND d.state = 'dispatched')))
                    OR EXISTS (SELECT 1 FROM events WHERE aggregate_id = ?
                       AND event_type IN ('attempt.provider_queue_accepted', 'attempt.provider_turn_started')
                       AND json_extract(data_json, '$.message_id') = ?)""",
                (row["worker_id"], row["worker_id"], delivery_message_id, runtime_id,
                 row["work_item_id"], attempt_id, delivery_message_id),
            ).fetchone()
            if (not row["managed_worker_thread_id"] or unsafe is not None
                    or row["source_thread_digest"] or row["source_turn_digest"]
                    or not event.previous_native_thread_id
                    or event.previous_native_thread_id == event.native_thread_id
                    or row["native_session_id"] != event.previous_native_thread_id):
                raise ConflictError("Native Worker rebinding has an unsettled execution fence")
            connection.execute(
                "UPDATE runtime_sessions SET native_session_id = ?, updated_at = ? WHERE id = ?",
                (event.native_thread_id, utc_now(), runtime_id),
            )
        elif row["native_session_id"] and row["native_session_id"] != event.native_thread_id:
            raise ConflictError("Worker dispatch belongs to another native thread")
        else:
            connection.execute(
                "UPDATE runtime_sessions SET native_session_id = ? WHERE id = ? AND native_session_id = ''",
                (event.native_thread_id, runtime_id),
            )
        event_type = {
            "thread_rebound": "worker.runtime_thread_rebound",
            "queue_accepted": "attempt.provider_queue_accepted",
            "turn_started": "attempt.provider_turn_started",
        }[event.kind]
        data: dict[str, Any] = {
            "message_id": delivery_message_id, "runtime_session_id": runtime_id,
            "delivery_generation": delivery_generation, "enrollment_generation": enrollment_generation,
            "worker_thread_id": row["managed_worker_thread_id"],
            "thread_digest": _digest(event.native_thread_id),
        }
        if event.previous_native_thread_id:
            data["previous_thread_digest"] = _digest(event.previous_native_thread_id)
        if event.turn_id:
            data["turn_digest"] = _digest(event.turn_id)
        if event.activation_source:
            if event.kind != "turn_started" or event.activation_source not in {"turn_start_reply", "user_message_event"}:
                raise ConflictError("invalid Worker activation evidence")
            data["activation_source"] = event.activation_source
        prior = connection.execute(
            "SELECT data_json FROM events WHERE event_type = ? AND aggregate_id = ? "
            "AND json_extract(data_json, '$.message_id') = ? ORDER BY sequence DESC LIMIT 1",
            (event_type, attempt_id, delivery_message_id),
        ).fetchone()
        if prior is None:
            service._event(connection, event_type, "attempt", attempt_id, "", data,
                           causation_id=delivery_message_id)
