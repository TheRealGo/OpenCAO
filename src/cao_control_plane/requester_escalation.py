"""Current requester escalation, derived from existing decision and Delivery authority."""

from __future__ import annotations

import sqlite3
from typing import Any


def requester_escalation_tx(
    connection: sqlite3.Connection, work: sqlite3.Row
) -> dict[str, Any] | None:
    if work["state"] == "user_needed":
        pointer, kind = work["user_needed_boundary_id"], "wait_user"
    elif work["state"] == "suspended":
        pointer, kind = work["paused_boundary_id"], "pause"
    else:
        return None
    if not pointer:
        return None
    row = connection.execute(
        """
        SELECT d.boundary_id, d.reason, d.instruction, d.resume_condition,
               d.created_at, m.id AS message_id, delivery.state AS notification_state
        FROM boundary_dispositions d
        JOIN boundaries b ON b.id = d.boundary_id
        JOIN work_items w ON w.id = b.work_item_id
        LEFT JOIN messages m
          ON m.sender_id = d.decided_by AND m.work_item_id = w.id
         AND m.attempt_id = b.attempt_id AND m.kind = 'system'
         AND json_valid(m.payload_json)
         AND json_extract(m.payload_json, '$.action') = 'requester_escalation'
         AND json_extract(m.payload_json, '$.disposition_id') = d.id
        LEFT JOIN message_deliveries delivery
          ON delivery.message_id = m.id AND delivery.recipient_id = w.supervisor_id
         AND delivery.recipient_attachment_id IS w.supervisor_attachment_id
        WHERE b.id = ? AND b.work_item_id = ? AND d.kind = ?
        ORDER BY m.sequence DESC LIMIT 1
        """,
        (pointer, work["id"], kind),
    ).fetchone()
    if row is None:
        return None
    return {
        "boundary_id": str(row["boundary_id"]),
        "message_id": str(row["message_id"] or ""),
        "reason": str(row["reason"]),
        "decision": str(row["instruction"]),
        "resume_condition": str(row["resume_condition"]),
        "requested_at": str(row["created_at"]),
        "notification_state": str(row["notification_state"] or "unavailable"),
    }


def requester_escalation_delivery_tx(
    connection: sqlite3.Connection, message_id: str
) -> sqlite3.Row | None:
    """Return only the latest notice for a still-current escalation and live owner."""
    row = connection.execute(
        """
        SELECT message.*, delivery.recipient_id, delivery.generation,
               delivery.state AS delivery_state, delivery.recipient_attachment_id,
               attachment.native_thread_id, attachment.generation AS attachment_generation,
               attachment.runtime_session_id, attachment.lease_expires_at,
               json_extract(message.payload_json, '$.source_boundary_id') AS boundary_id
        FROM messages message
        JOIN message_deliveries delivery ON delivery.message_id = message.id
        JOIN work_items work ON work.id = message.work_item_id
        JOIN cao_session_attachments attachment
          ON attachment.id = delivery.recipient_attachment_id
         AND attachment.id = work.supervisor_attachment_id
         AND attachment.principal_id = delivery.recipient_id
        JOIN runtime_sessions runtime ON runtime.id = attachment.runtime_session_id
         AND runtime.principal_id = attachment.principal_id
         AND runtime.native_session_id = attachment.native_thread_id
        WHERE message.id = ? AND message.kind = 'system'
          AND json_valid(message.payload_json)
          AND json_extract(message.payload_json, '$.action') = 'requester_escalation'
          AND work.supervisor_id = delivery.recipient_id
          AND attachment.state = 'active' AND runtime.adapter = 'codex-app-server'
          AND runtime.state NOT IN ('stopped', 'failed', 'missing')
        """,
        (message_id,),
    ).fetchone()
    if row is None:
        return None
    work = connection.execute(
        "SELECT * FROM work_items WHERE id = ?", (row["work_item_id"],)
    ).fetchone()
    assert work is not None
    current = requester_escalation_tx(connection, work)
    return row if current and current["message_id"] == message_id else None
