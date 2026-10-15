"""Reconcile replaceable native wake execution with durable supervision state.

The provider queue is the source of pending submission identities. Existing
Delivery and queue-admission records are the cancellation authority, including
after restart; no second queue or synthetic supervision receipt is necessary.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from .database import utc_now
from .delivery_lane import (
    cao_informational_wake_sql,
    cao_superseding_boundary_sql,
    codex_delivery_client_id,
)

QUEUE_FAILURE_CODES = frozenset({
    "provider_timeout", "provider_authorization_failed", "queue_runtime_unavailable",
    "queue_adapter_unavailable", "provider_queue_operation_failed", "queue_reconciliation_failed",
})

if TYPE_CHECKING:
    from .service import ControlPlane


def obsolete_queue_notifications(service: ControlPlane) -> dict[str, list[dict[str, Any]]]:
    """Select only exact, confirmed native queue admissions safe to retract.

    A delivered informational wake remains untouched here. Only a successful
    provider deletion may supersede it; absence is never consumption evidence.
    Unknown dispatches, unclaimed inputs and decision notifications remain fenced.
    """
    if not service.db.is_canonical_authority():
        return {}
    with service.db.connection_scope() as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT m.*, d.recipient_id, d.generation AS delivery_generation,
                   d.state AS delivery_state, a.id AS attachment_id,
                   a.generation AS attachment_generation, a.native_thread_id,
                   a.runtime_session_id AS attachment_runtime_id,
                   CASE WHEN {cao_informational_wake_sql("m")}
                        THEN ({cao_superseding_boundary_sql("m", "d")}) END AS superseding_id
            FROM events AS admission
            JOIN messages AS m ON m.id = admission.causation_id
            JOIN message_deliveries AS d ON d.message_id = m.id
            JOIN cao_session_attachments AS a ON a.id = d.recipient_attachment_id
              AND a.principal_id = d.recipient_id
            JOIN principals AS recipient ON recipient.id = a.principal_id AND recipient.role = 'cao'
            JOIN runtime_sessions AS runtime ON runtime.id = a.runtime_session_id
              AND runtime.adapter = 'codex-app-server'
              AND runtime.native_session_id = a.native_thread_id
              AND runtime.id = d.runtime_session_id
            WHERE admission.event_type = 'runtime.message_delivered'
              AND admission.aggregate_type = 'runtime'
              AND admission.aggregate_id = runtime.id
              AND json_extract(admission.data_json, '$.message_id') = m.id
              AND json_extract(admission.data_json, '$.adapter') = 'codex-app-server'
              AND json_extract(admission.data_json, '$.result.success') = 1
              AND json_extract(admission.data_json, '$.result.diagnostics.delivery_method') = 'thread_queue'
              AND json_extract(admission.data_json, '$.result.diagnostics.delivery_acceptance') = 'queued'
              AND d.delivered_at IS NOT NULL
              AND (
                d.state = 'handled'
                OR (d.state = 'dead' AND d.reactivation_policy = 'terminal')
                OR (d.state IN ('delivered', 'acknowledged')
                    AND {cao_informational_wake_sql("m")}
                    AND ({cao_superseding_boundary_sql("m", "d")}) IS NOT NULL)
              )
            ORDER BY m.sequence
            """
        ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            value = dict(row)
            value["message"] = service._message_view(row, connection=connection)
            value["client_user_message_id"] = codex_delivery_client_id(str(row["id"]))
            grouped.setdefault(str(row["attachment_id"]), []).append(value)
        return grouped


def current_queue_removal(
    service: ControlPlane, candidate: dict[str, Any]
) -> dict[str, Any] | None:
    """Recheck exact durable identity immediately before a provider effect."""
    for current in obsolete_queue_notifications(service).get(str(candidate["attachment_id"]), []):
        if all(
            current[key] == candidate[key]
            for key in (
                "id",
                "recipient_id",
                "delivery_generation",
                "attachment_generation",
                "native_thread_id",
                "attachment_runtime_id",
            )
        ):
            return current
    return None


def record_queue_removal_request(
    service: ControlPlane,
    candidate: dict[str, Any],
    queue_id: str,
) -> int:
    """Persist the exact cancellation intent before crossing the provider boundary."""
    with service.db.transaction() as connection:
        return service._event(
            connection,
            "cao.notification_queue_removal_requested",
            "message",
            str(candidate["id"]),
            "",
            {
                "attachment_id": candidate["attachment_id"],
                "delivery_generation": candidate["delivery_generation"],
                "queue_item_digest": hashlib.sha256(queue_id.encode()).hexdigest(),
            },
            causation_id=str(candidate["id"]),
        )


def unsettled_queue_removals(service: ControlPlane) -> int:
    """Keep a lost removal response visible across process restarts."""
    row = service.db.fetchone(
        """SELECT COUNT(*) FROM events AS request
           WHERE request.event_type = 'cao.notification_queue_removal_requested'
             AND NOT EXISTS (
               SELECT 1 FROM events AS result
               WHERE result.aggregate_type = request.aggregate_type
                 AND result.aggregate_id = request.aggregate_id
                 AND result.event_type IN ('cao.notification_queue_removed', 'cao.notification_queue_retained')
                 AND json_extract(result.data_json, '$.request_sequence') = request.sequence
             )"""
    )
    return int(row[0]) if row is not None else 0


def record_queue_removal(
    service: ControlPlane,
    candidate: dict[str, Any],
    queue_id: str,
    request_sequence: int,
    deleted: bool,
) -> None:
    """Record an exact successful deletion without impersonating CAO judgment."""
    now = utc_now()
    with service.db.transaction() as connection:
        if not deleted:
            service._event(
                connection,
                "cao.notification_queue_retained",
                "message",
                str(candidate["id"]),
                "",
                {
                    "request_sequence": request_sequence,
                    "queue_item_digest": hashlib.sha256(queue_id.encode()).hexdigest(),
                    "reason_code": "queue_item_not_pending",
                },
                causation_id=str(candidate["id"]),
            )
            return
        # Handled/dead state is already terminal. Preserve every timestamp,
        # generation, error, ACK, Review and disposition in those records.
        changed = connection.execute(
            f"""UPDATE message_deliveries AS d
                SET state = 'dead', reactivation_policy = 'terminal', owner_token = '',
                    lease_until = NULL, updated_at = ?,
                    last_error = 'cao_progress_wake_superseded_by_boundary'
                WHERE d.message_id = ? AND d.recipient_id = ? AND d.generation = ?
                  AND d.recipient_attachment_id = ?
                  AND EXISTS (SELECT 1 FROM cao_session_attachments AS attachment
                    WHERE attachment.id = d.recipient_attachment_id
                      AND attachment.principal_id = d.recipient_id
                      AND attachment.generation = ?)
                  AND d.state IN ('delivered', 'acknowledged')
                  AND EXISTS (SELECT 1 FROM messages AS m WHERE m.id = d.message_id
                    AND {cao_informational_wake_sql("m")}
                    AND ({cao_superseding_boundary_sql("m", "d")}) IS NOT NULL)
            """,
            (
                now,
                candidate["id"],
                candidate["recipient_id"],
                candidate["delivery_generation"],
                candidate["attachment_id"],
                candidate["attachment_generation"],
            ),
        ).rowcount
        service._event(
            connection,
            "cao.notification_queue_removed",
            "message",
            str(candidate["id"]),
            "",
            {
                "attachment_id": candidate["attachment_id"],
                "request_sequence": request_sequence,
                "delivery_generation": candidate["delivery_generation"],
                "queue_item_digest": hashlib.sha256(queue_id.encode()).hexdigest(),
                "reason_code": (
                    "terminal_delivery"
                    if candidate["delivery_state"] in {"handled", "dead"}
                    else "superseded_information"
                ),
                "delivery_superseded": bool(changed),
                "superseding_message_id": candidate.get("superseding_id"),
            },
            causation_id=str(candidate["id"]),
        )
