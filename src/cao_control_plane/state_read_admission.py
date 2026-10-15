from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .errors import AuthorizationError
from .service import ControlPlane

STATE_READ_TOOLS = frozenset({
    "cao_get_work", "cao_get_inbox", "cao_query", "cao_list_managed_workers",
    "cao_read_work_history",
})
READINESS_PROOF_META_KEY = "cao/attachment-readiness-proof"

# Only committed supervisor actions renew a read. Transport renewal, runtime
# heartbeats, output capture and the read itself never do.
_COMMAND_EVENTS = (
    "work.assigned", "managed_worker_thread.instructed", "work.goal_replaced",
    "directive.created", "work.status_requested", "work.user_needed_resumed",
    "work.resumed", "work.paused", "work.pause_resumed", "work.reviewed",
    "work.requester_decision_recorded", "work.canceled", "boundary.disposed",
    "reasoner.turn_acquired", "message.acknowledged", "message.handled",
    "worker.purpose_defined", "managed_worker_thread.finished",
    "managed_worker_thread.resumed", "managed_worker_thread.deleted",
    "managed_worker.created",
)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def readiness_proof(key: str, request_id: Any) -> str:
    """Authenticate a proxy-owned readiness probe without exposing its bearer."""
    payload = json.dumps(["attachment-readiness", request_id], separators=(",", ":")).encode()
    return hmac.new(key.encode(), payload, hashlib.sha256).hexdigest()


def authenticate_readiness_probe(
    actor: Mapping[str, Any], params: Mapping[str, Any], request_id: Any,
) -> bool:
    if params.get("name") != "cao_list_managed_workers":
        return False
    meta = params.get("_meta")
    proof = meta.get(READINESS_PROOF_META_KEY) if isinstance(meta, Mapping) else None
    key = actor.get("_cao_readiness_proof_key")
    if not actor.get("_cao_conversation_credential_id") or not isinstance(key, str) or not isinstance(proof, str) or len(proof) != 64:
        return False
    return hmac.compare_digest(proof, readiness_proof(key, request_id))


@dataclass(frozen=True)
class StateReadReservation:
    attachment_id: str
    resource: str
    reservation_id: str
    previous: dict[str, Any] | None


class StateReadAdmission:
    def __init__(self, service: ControlPlane, input_reader: Callable[[str], str | None]) -> None:
        self.service = service
        self.input_reader = input_reader

    def reserve(
        self, actor: dict[str, Any], name: str, values: Mapping[str, Any],
    ) -> StateReadReservation | None:
        if name not in STATE_READ_TOOLS or not (
            actor.get("_cao_conversation_credential_id") or actor.get("_cao_runtime_credential_id")
        ):
            return None
        if name == "cao_list_managed_workers" and actor.get("_cao_authenticated_readiness_probe"):
            return None
        attachment_id = str(actor.get("_cao_attachment_id") or "")
        work_id = str(values.get("work_item_id") or "") if name in {"cao_get_work", "cao_read_work_history"} else ""
        resource = f"{name}:{work_id}" if work_id else name
        attachment = self.service.db.fetchone("SELECT native_thread_id FROM cao_session_attachments WHERE id = ?", (attachment_id,))
        native_digest = None
        if attachment:
            # This callback is owner-side, never a tool argument or _meta field.
            native_digest = self.input_reader(str(attachment["native_thread_id"]))
        if name == "cao_get_inbox":
            supplied_cursor = values.get("after", 0)
        elif name == "cao_read_work_history":
            supplied_cursor = values.get("before_sequence", 0)
        else:
            supplied_cursor = values.get("cursor", "")
        with self.service.db.transaction() as connection:
            self.service._require_current_cao_attachment_actor_tx(connection, actor)
            if native_digest:
                connection.execute(
                    "INSERT OR IGNORE INTO cao_state_read_inputs(attachment_id, input_digest) VALUES(?, ?)",
                    (attachment_id, native_digest),
                )
            input_sequence = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM cao_state_read_inputs WHERE attachment_id = ?",
                (attachment_id,),
            ).fetchone()[0]
            notification_sequence = connection.execute(
                "SELECT COALESCE(MAX(m.sequence), 0) FROM messages m JOIN message_deliveries d ON d.message_id = m.id "
                "WHERE d.recipient_attachment_id = ? AND d.recipient_id = ? AND m.sender_id != ? "
                "AND (? = '' OR m.work_item_id = ?)",
                (attachment_id, actor["id"], actor["id"], work_id, work_id),
            ).fetchone()[0]
            inventory_scope = (
                " OR EXISTS (SELECT 1 FROM managed_worker_specs s JOIN managed_worker_threads t ON t.managed_spec_id=s.id "
                "JOIN cao_session_attachments source ON source.id=s.attachment_id "
                "WHERE source.project_scope_digest=(SELECT project_scope_digest FROM cao_session_attachments WHERE id=?) "
                "AND (e.aggregate_id=t.id OR e.aggregate_id=s.id))"
                if name == "cao_list_managed_workers" else ""
            )
            event_sequence = connection.execute(
                "SELECT COALESCE(MAX(e.sequence), 0) FROM events e WHERE e.actor_id = ? "
                f"AND e.event_type IN ({','.join('?' for _ in _COMMAND_EVENTS)}) "
                "AND (EXISTS (SELECT 1 FROM work_items w WHERE w.supervisor_attachment_id = ? "
                "AND (? = '' OR w.id = ?) AND (e.aggregate_id = w.id OR e.correlation_id = w.id "
                "OR (e.aggregate_type = 'managed_worker_thread' AND e.aggregate_id = w.managed_worker_thread_id) "
                "OR (e.aggregate_type = 'message' AND EXISTS (SELECT 1 FROM messages cm WHERE cm.id = e.aggregate_id AND cm.work_item_id = w.id))))"
                + inventory_scope + ")",
                (actor["id"], *_COMMAND_EVENTS, attachment_id, work_id, work_id, *((attachment_id,) if inventory_scope else ())),
            ).fetchone()[0]
            cause = _digest([input_sequence, notification_sequence, event_sequence])
            row = connection.execute(
                "SELECT * FROM cao_state_read_cursors WHERE attachment_id = ? AND resource = ?",
                (attachment_id, resource),
            ).fetchone()
            previous = dict(row) if row else None
            forward_page = bool(
                row and supplied_cursor and row["next_cursor_digest"]
                and _digest(supplied_cursor) == row["next_cursor_digest"] and not row["pending"]
            )
            if row and row["cause_digest"] == cause and not forward_page:
                raise AuthorizationError(
                    "State polling is prohibited. End this turn and resume from a new notification or requester input.",
                    reason_code="cao_state_polling_prohibited", retryable=False,
                    next_action="end_turn_await_notification",
                )
            reservation_id = secrets.token_hex(16)
            connection.execute(
                "INSERT INTO cao_state_read_cursors(attachment_id, resource, cause_digest, reservation_id, pending, next_cursor_digest) "
                "VALUES(?, ?, ?, ?, 1, '') ON CONFLICT(attachment_id, resource) DO UPDATE SET "
                "cause_digest=excluded.cause_digest, reservation_id=excluded.reservation_id, pending=1, next_cursor_digest=''",
                (attachment_id, resource, cause, reservation_id),
            )
        return StateReadReservation(attachment_id, resource, reservation_id, previous)

    def finish(self, reservation: StateReadReservation | None, result: Any = None, *, failed: bool = False) -> None:
        if reservation is None:
            return
        with self.service.db.transaction() as connection:
            keys = (reservation.attachment_id, reservation.resource, reservation.reservation_id)
            if failed:
                prior = reservation.previous
                if prior:
                    connection.execute(
                        "UPDATE cao_state_read_cursors SET cause_digest=?, reservation_id=?, pending=?, next_cursor_digest=? "
                        "WHERE attachment_id=? AND resource=? AND reservation_id=?",
                        (prior["cause_digest"], prior["reservation_id"], prior["pending"], prior["next_cursor_digest"], *keys),
                    )
                else:
                    connection.execute("DELETE FROM cao_state_read_cursors WHERE attachment_id=? AND resource=? AND reservation_id=?", keys)
                return
            cursor = result.get("next_cursor", result.get("next_before_sequence")) if isinstance(result, Mapping) else None
            connection.execute(
                "UPDATE cao_state_read_cursors SET pending=0, next_cursor_digest=? WHERE attachment_id=? AND resource=? AND reservation_id=?",
                (_digest(cursor) if cursor else "", *keys),
            )
