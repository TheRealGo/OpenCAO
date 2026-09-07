"""Read-only, allowlisted exchanges for the Dashboard's work reader.

This is a presentation projection of durable Goals, messages and decisions.
It never reads provider transcripts, artifact bodies or generic event payloads.
References are display handles, not authority or database identifiers.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from typing import Any

from .canonical import canonical_sha256
from .projection import sanitize_operator_text

HISTORY_REFERENCE = re.compile(r"^[a-f0-9]{64}$")
HISTORY_KINDS = frozenset(
    {
        "goal",
        "instruction",
        "progress",
        "question",
        "blocker",
        "artifact",
        "completion_claim",
        "worker_output",
        "review",
        "requester_decision",
        "decision",
    }
)


def history_reference(kind: str, identity: str) -> str:
    return canonical_sha256({"format": "cao-dashboard-history/v1", "kind": kind, "key": identity})


def full_operator_text(value: object) -> str | None:
    return sanitize_operator_text(value, limit=None, collapse_whitespace=False)


def _object(value: object) -> Mapping[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        result = json.loads(value)
    except (ValueError, TypeError):
        return {}
    return result if isinstance(result, Mapping) else {}


def work_exchanges(connection: sqlite3.Connection, work_id: str) -> list[dict[str, Any]]:
    """Read typed exchanges in the same snapshot as the selected Work."""

    entries: list[tuple[str, int, int, dict[str, Any]]] = []
    event_order: dict[tuple[str, str], int] = {}

    def add(
        kind: str,
        identity: str,
        at: str,
        body: object,
        rank: int,
        order: int,
        *,
        outcome: str | None = None,
    ) -> None:
        content = full_operator_text(body)
        if content is None:
            return
        source = kind if kind in {"goal", "review", "requester_decision", "decision"} else "message"
        entries.append(
            (
                at,
                event_order.get((source, identity), 0),
                rank * 1_000_000 + order,
                {
                    "reference": history_reference(kind, identity),
                    "kind": kind,
                    "occurred_at": at,
                    "text": content,
                    "outcome": outcome,
                },
            )
        )

    work = connection.execute(
        "SELECT assigned_worker_id, supervisor_id FROM work_items "
        "WHERE id = ? AND operator_scope = 'production'",
        (work_id,),
    ).fetchone()
    if work is None:
        return []
    # Read event identity/order only. Payload fields are never display text.
    for event in connection.execute(
        """SELECT sequence, event_type, aggregate_id, data_json FROM events
           WHERE (aggregate_type = 'work_item' AND aggregate_id = ?)
              OR (aggregate_type = 'message' AND aggregate_id IN
                  (SELECT id FROM messages WHERE work_item_id = ?))
           ORDER BY sequence""",
        (work_id, work_id),
    ):
        data = _object(event["data_json"])
        event_type = event["event_type"]
        source_key: tuple[str, str] | None = None
        if event_type == "message.created":
            source_key = ("message", event["aggregate_id"])
        elif event_type == "work.assigned":
            source_key = ("goal", f"{work_id}:1")
        elif event_type == "work.goal_replaced" and isinstance(data.get("version"), int):
            source_key = ("goal", f"{work_id}:{data['version']}")
        else:
            field = {
                "work.reviewed": ("review", "review_id"),
                "work.requester_decision_recorded": (
                    "requester_decision",
                    "requester_decision_id",
                ),
                "boundary.disposed": ("decision", "disposition_id"),
            }.get(event_type)
            if field and isinstance(data.get(field[1]), str):
                source_key = (field[0], data[field[1]])
        if source_key:
            event_order.setdefault(source_key, event["sequence"])
    for row in connection.execute(
        "SELECT * FROM goal_revisions WHERE work_item_id = ? ORDER BY version",
        (work_id,),
    ):
        add(
            "goal",
            f"{work_id}:{row['version']}",
            row["created_at"],
            row["objective"],
            0,
            row["version"],
        )
    for row in connection.execute(
        """SELECT m.*, a.worker_id AS attempt_worker,
                  EXISTS(SELECT 1 FROM worker_output_receipts output
                         WHERE output.notification_message_id = m.id) AS captured
           FROM messages m JOIN attempts a ON a.id = m.attempt_id
           WHERE m.work_item_id = ? AND a.work_item_id = m.work_item_id
           ORDER BY m.sequence""",
        (work_id,),
    ):
        payload = _object(row["payload_json"])
        kind = row["kind"]
        if row["sender_id"] == row["attempt_worker"]:
            if (
                kind == "system"
                and payload.get("action") == "worker_output"
                and row["captured"]
            ):
                kind = "worker_output"
            if kind in {
                "progress",
                "question",
                "blocker",
                "artifact",
                "completion_claim",
                "worker_output",
            }:
                add(
                    kind,
                    row["id"],
                    row["created_at"],
                    payload.get("summary"),
                    1,
                    row["sequence"],
                )
        elif row["sender_id"] == work["supervisor_id"] and kind == "instruction":
            add(
                "instruction",
                row["id"],
                row["created_at"],
                payload.get("instruction"),
                1,
                row["sequence"],
            )
    for row in connection.execute(
        "SELECT * FROM reviews WHERE work_item_id = ? AND reviewer_role = 'cao' ORDER BY rowid",
        (work_id,),
    ):
        add(
            "review",
            row["id"],
            row["created_at"],
            row["summary"],
            2,
            len(entries),
            outcome=row["verdict"],
        )
    for row in connection.execute(
        "SELECT * FROM requester_decisions WHERE work_item_id = ? ORDER BY rowid",
        (work_id,),
    ):
        add(
            "requester_decision",
            row["id"],
            row["created_at"],
            row["summary"],
            3,
            len(entries),
            outcome=row["verdict"],
        )
    for row in connection.execute(
        """SELECT d.* FROM boundary_dispositions d JOIN boundaries b ON b.id = d.boundary_id
           WHERE b.work_item_id = ? ORDER BY d.rowid""",
        (work_id,),
    ):
        body = "\n\n".join(
            str(row[key]) for key in ("reason", "instruction", "resume_condition") if row[key]
        )
        add(
            "decision", row["id"], row["created_at"], body, 4, len(entries), outcome=row["kind"]
        )
    entries.sort(key=lambda value: value[:3])
    return [entry for _at, _rank, _order, entry in entries]
