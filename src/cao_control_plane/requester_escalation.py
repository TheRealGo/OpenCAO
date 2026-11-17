"""Current requester escalation, derived from existing decision and Delivery authority."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def system_recovery_boundary_tx(
    connection: sqlite3.Connection, work: sqlite3.Row
) -> sqlite3.Row | None:
    """Resolve a single exact current CAO-owned recovery, never a history guess."""
    if work["state"] != "waiting_supervisor" or work["attention_owner"] != "cao":
        return None
    rows = connection.execute(
        """
        SELECT b.*, d.reason AS recovery_reason, d.instruction AS recovery_plan,
               d.resume_condition, d.recovery_blocker, d.recovery_probe_digest, d.created_at AS waiting_since,
               d.id AS disposition_id
        FROM boundaries b
        JOIN boundary_dispositions d ON d.boundary_id = b.id AND d.kind = 'wait_system'
        JOIN attempts a ON a.id = b.attempt_id AND a.work_item_id = b.work_item_id
        JOIN reasoner_turns t ON t.id = d.reasoner_turn_id AND t.boundary_id = b.id
        LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
        WHERE b.work_item_id = ? AND b.generation = ? AND d.generation = b.generation
          AND b.goal_version = ? AND a.state = 'waiting_supervisor'
          AND a.attempt_number = (SELECT MAX(attempt_number) FROM attempts WHERE work_item_id = ?)
          AND b.goal_packet_digest = a.goal_packet_digest
          AND b.task_packet_digest = a.task_packet_digest
          AND t.state = 'completed' AND t.work_item_id = b.work_item_id
          AND t.generation = b.generation AND t.supervisor_id = ? AND d.decided_by = ?
          AND t.goal_packet_digest = b.goal_packet_digest
          AND t.task_packet_digest = b.task_packet_digest
          AND s.boundary_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM boundaries open
            LEFT JOIN boundary_dispositions decision ON decision.boundary_id = open.id
            LEFT JOIN boundary_supersessions replaced ON replaced.boundary_id = open.id
            WHERE open.work_item_id = b.work_item_id AND open.generation = b.generation
              AND decision.id IS NULL AND replaced.boundary_id IS NULL
          )
        LIMIT 2
        """,
        (work["id"], work["generation"], work["goal_version"], work["id"],
         work["supervisor_id"], work["supervisor_id"]),
    ).fetchall()
    return rows[0] if len(rows) == 1 else None


def system_recovery_tx(
    connection: sqlite3.Connection, work: sqlite3.Row
) -> dict[str, Any] | None:
    boundary = system_recovery_boundary_tx(connection, work)
    if boundary is None:
        return None
    notice = connection.execute(
        """
        SELECT m.id, delivery.state FROM messages m
        JOIN message_deliveries delivery ON delivery.message_id = m.id
        WHERE m.work_item_id = ? AND m.attempt_id = ? AND m.sender_id = ?
          AND m.kind = 'system' AND json_valid(m.payload_json)
          AND json_extract(m.payload_json, '$.action') = 'requester_escalation'
          AND json_extract(m.payload_json, '$.disposition_id') = ?
          AND json_extract(m.payload_json, '$.disposition') = 'wait_system'
          AND delivery.recipient_id = ?
          AND delivery.recipient_attachment_id IS ?
        ORDER BY m.sequence DESC LIMIT 1
        """,
        (work["id"], boundary["attempt_id"], work["supervisor_id"],
         boundary["disposition_id"], work["supervisor_id"], work["supervisor_attachment_id"]),
    ).fetchone()
    observed = connection.execute(
        "SELECT e.data_json FROM events e JOIN boundaries b ON b.id=json_extract(e.data_json,'$.boundary_id') "
        "WHERE e.event_type = 'system_recovery.observed' AND b.work_item_id = ? "
        "AND b.attempt_id = ? ORDER BY e.sequence DESC LIMIT 1", (work["id"], boundary["attempt_id"]),
    ).fetchone()
    diagnostic = json.loads(observed["data_json"]) if observed else {}
    return {
        "boundary_id": str(boundary["id"]),
        "owner": "cao", "requester_action_required": False,
        "reason": str(boundary["recovery_reason"]),
        "recovery_plan": str(boundary["recovery_plan"]),
        "resume_condition": str(boundary["resume_condition"]),
        "waiting_since": str(boundary["waiting_since"]),
        "message_id": str(notice["id"]) if notice else "",
        "notification_state": str(notice["state"]) if notice else "unavailable",
        "watch_kind": "runtime_admission_and_diagnostics" if boundary["recovery_blocker"] else "runtime_diagnostics",
        "admission_blocker": str(boundary["recovery_blocker"]),
        "diagnostic_state": diagnostic.get("state", "pending"),
        "diagnostic_code": diagnostic.get("reason_code", ""),
    }


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
        SELECT d.boundary_id, d.reason, d.instruction, d.resume_condition, d.requester_input_kind,
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
        **({"requester_input_kind": str(row["requester_input_kind"])}
           if row["requester_input_kind"] else {}),
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
    current = (
        system_recovery_tx(connection, work)
        if json.loads(str(row["payload_json"])).get("disposition") == "wait_system"
        else requester_escalation_tx(connection, work)
    )
    return row if current and current["message_id"] == message_id else None
