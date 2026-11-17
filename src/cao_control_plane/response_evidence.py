"""Input-scoped response facts; never an automatic business-progress judgment."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from typing import Any

REPORT_KINDS = ("progress", "question", "blocker", "completion_claim", "artifact")
_STATES = {"available", "empty", "absent", "partial", "withheld", "unavailable"}


def _load(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return fallback


def response_snapshot_tx(
    connection: sqlite3.Connection, stream: sqlite3.Row, terminal: sqlite3.Row,
    *, legacy: bool = False,
) -> dict[str, Any]:
    """Freeze facts while the owning settlement transaction holds the writer lock."""
    source = connection.execute(
        "SELECT m.sequence,m.kind,d.state,d.acknowledged_at FROM messages m "
        "JOIN message_deliveries d ON d.message_id=m.id WHERE m.id=? "
        "AND d.runtime_session_id=?", (stream["source_message_id"], stream["runtime_session_id"]),
    ).fetchone()
    outputs = connection.execute(
        "SELECT phase,capture_state,content_digest,byte_count FROM worker_output_receipts "
        "WHERE stream_id=? AND event_kind='message' ORDER BY sequence", (stream["id"],),
    ).fetchall()
    finals = [row for row in outputs if row["phase"] == "final"]
    final_state = next((state for state in (
        "available", "partial", "withheld", "unavailable", "empty"
    ) if any(row["capture_state"] == state for row in finals)), "absent")
    result: dict[str, Any] = {
        "record_availability": "legacy_unavailable" if legacy else "recorded",
        "source_message_id": str(stream["source_message_id"]),
        "output_id": str(terminal["id"]),
        "input_kind": str(source["kind"]) if source is not None else "unknown",
        "turn_status": str(terminal["turn_status"]),
        "final_output_state": final_state,
        "readable_final_items": sum(bool(row["content_digest"]) for row in finals),
        "readable_commentary_items": sum(
            row["phase"] != "final" and bool(row["content_digest"]) for row in outputs
        ),
        "worker_reports": None,
        "new_artifact_records_since_previous_response": None,
        "acknowledgment_observed": None,
        "execution_start_report_observed": None,
        "business_progress": "not_judged",
    }
    if legacy or source is None:
        return result
    counts = connection.execute(
        "SELECT m.kind,COUNT(*) n FROM messages m JOIN attempts a ON a.id=m.attempt_id "
        "WHERE m.attempt_id=? AND m.sender_id=a.worker_id AND m.sequence>? "
        "AND m.goal_packet_digest=a.goal_packet_digest AND m.task_packet_digest=a.task_packet_digest "
        "GROUP BY m.kind", (stream["attempt_id"], source["sequence"]),
    ).fetchall()
    reports = {str(row["kind"]): int(row["n"]) for row in counts}
    result["worker_reports"] = {kind: reports.get(kind, 0) for kind in REPORT_KINDS}
    result["acknowledgment_observed"] = source["acknowledged_at"] is not None
    result["execution_start_report_observed"] = connection.execute(
        "SELECT 1 FROM events e JOIN messages m ON m.id=e.causation_id "
        "WHERE e.event_type='attempt.execution_started' AND e.aggregate_id=? AND m.sequence>? LIMIT 1",
        (stream["attempt_id"], source["sequence"]),
    ).fetchone() is not None
    previous = connection.execute(
        "SELECT artifact_manifest_json FROM worker_output_receipts WHERE attempt_id=? "
        "AND event_kind='turn_end' AND sequence<? AND goal_packet_digest=? AND task_packet_digest=? "
        "ORDER BY sequence DESC LIMIT 1", (stream["attempt_id"], terminal["sequence"],
            terminal["goal_packet_digest"], terminal["task_packet_digest"]),
    ).fetchone()

    def identities(encoded: str) -> set[str]:
        items = _load(encoded, [])
        return {item["id"] for item in (items if isinstance(items, list) else []) if isinstance(item, dict)
                and re.fullmatch(r"art_[a-f0-9]{32}", str(item.get("id", "")))}

    current = identities(terminal["artifact_manifest_json"])
    prior = identities(previous["artifact_manifest_json"]) if previous else set()
    result["new_artifact_records_since_previous_response"] = len(current - prior)
    return result


def response_evidence_tx(
    connection: sqlite3.Connection, work_id: str, attempt_id: str,
) -> dict[str, Any] | None:
    streams = connection.execute(
        "SELECT * FROM worker_output_streams WHERE work_item_id=? AND attempt_id=? "
        "ORDER BY rowid DESC LIMIT 2", (work_id, attempt_id),
    ).fetchall()
    snapshots: list[dict[str, Any]] = []
    for stream in streams:
        if not stream["terminal_output_id"] or not stream["settled_at"]:
            snapshots.append({"record_availability": "pending",
                "source_message_id": str(stream["source_message_id"]), "turn_status": "running",
                "business_progress": "not_judged"})
            continue
        terminal = connection.execute("SELECT * FROM worker_output_receipts WHERE id=?",
                                      (stream["terminal_output_id"],)).fetchone()
        if terminal is None:
            continue
        event = connection.execute(
            "SELECT data_json FROM events WHERE event_type='worker_output.settled' AND aggregate_id=? "
            "AND json_extract(data_json,'$.output_id')=? ORDER BY sequence DESC LIMIT 1",
            (attempt_id, terminal["id"]),
        ).fetchone()
        snapshot = _load(event["data_json"], {}).get("response_evidence") if event else None
        exact = (isinstance(snapshot, dict) and snapshot.get("source_message_id") == stream["source_message_id"]
                 and snapshot.get("output_id") == terminal["id"])
        snapshots.append(snapshot if isinstance(snapshot, dict) and exact else response_snapshot_tx(
            connection, stream, terminal, legacy=True
        ))
    return {"current": snapshots[0], "previous": snapshots[1] if len(snapshots) > 1 else None} if snapshots else None


def public_response_evidence(value: Any) -> dict[str, Any] | None:
    """Never forward arbitrary stored mappings, text, locators or credentials."""
    if not isinstance(value, Mapping):
        return None

    def count(item: Any) -> int | None:
        return min(item, 1_000_000) if type(item) is int and item >= 0 else None

    def snapshot(raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, Mapping):
            return None
        result: dict[str, Any] = {"business_progress": "not_judged"}
        enums = {
            "record_availability": {"recorded", "legacy_unavailable", "pending"},
            "input_kind": {"assignment", "instruction", "status_request", "unknown"},
            "turn_status": {"completed", "failed", "interrupted", "running"},
            "final_output_state": _STATES,
        }
        for key, allowed in enums.items():
            if isinstance(raw.get(key), str) and raw[key] in allowed:
                result[key] = raw[key]
        for key, pattern in (("source_message_id", r"msg_[a-f0-9]{32}"),
                             ("output_id", r"wout_[a-f0-9]{48}")):
            if isinstance(raw.get(key), str) and re.fullmatch(pattern, raw[key]):
                result[key] = raw[key]
        for key in ("readable_final_items", "readable_commentary_items",
                    "new_artifact_records_since_previous_response"):
            item = raw.get(key)
            result[key] = count(item)
        for key in ("acknowledgment_observed", "execution_start_report_observed"):
            result[key] = raw.get(key) if type(raw.get(key)) is bool else None
        counts = raw.get("worker_reports")
        result["worker_reports"] = ({key: count(counts.get(key)) for key in REPORT_KINDS}
            if isinstance(counts, Mapping) else None)
        return result

    return {"current": snapshot(value.get("current")), "previous": snapshot(value.get("previous"))}
