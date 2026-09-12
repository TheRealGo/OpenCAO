"""Versioned, read-only operator dashboard contract.

This module is the only Dashboard read edge.  It deliberately composes the
Control Plane's sanitized projection and durable event log; it never exposes a
database file, an internal event payload, or a CAO credential to a client.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import sqlite3
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

from .canonical import canonical_json as _canonical_json
from .canonical import canonical_sha256 as _digest
from .dashboard_history import full_operator_text, history_reference, work_exchanges
from .projection import (
    build_projection,
    project_work_items_from_connection,
    sanitize_operator_text,
)
from .provider_models import model_identifier
from .service import ControlPlane

DASHBOARD_FORMAT = "cao-dashboard-read-model/v1"
OPERATOR_FORMAT = "cao-dashboard-operator/v1"
_CURSOR_VERSION = 1
_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}\.[a-z][a-z0-9_]{0,63}$")
_AGGREGATE_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_WORK_STATES = frozenset(
    {
        "open",
        "active",
        "suspended",
        "waiting_supervisor",
        "waiting_review",
        "waiting_user",
        "user_needed",
        "completed",
        "canceled",
        "failed",
    }
)
_ATTEMPT_STATES = frozenset(
    {
        "assigned",
        "accepted",
        "working",
        "suspended",
        "waiting_supervisor",
        "input_required",
        "blocked",
        "submitted",
        "completed",
        "failed",
        "canceled",
    }
)
_TRAJECTORIES = frozenset({"untracked", "advancing", "at_risk", "stalled", "drifting", "complete"})
_ATTENTION_OWNERS = frozenset({"none", "worker", "cao", "user", "external"})
_CLOSURE_STATES = frozenset({"open", "awaiting-explicit-close", "closed"})
_REQUESTER_DECISIONS = frozenset({"accepted", "rejected", "pending"})
_CAO_REVIEW_DECISIONS = frozenset({"ok", "needs-work", "pending"})
_ARTIFACT_PRESERVATION_STATES = frozenset({"preserved", "pending"})
_CLEANUP_STATES = frozenset({"verified", "pending", "unknown"})
_RUNNER_ADAPTERS = frozenset({"codex-app-server", "claude", "subprocess", "webhook"})
_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})
_RUNNER_AVAILABILITY = frozenset({"available", "unavailable"})
_RUNNER_SPEC_STATES = frozenset(
    {"enabled", "stopped", "revoked", "unsupported", "mismatched", "invalid", "unavailable"}
)
_RUNNER_CONNECTION_STATES = frozenset(
    {
        "enrolling",
        "connected-idle",
        "connected-busy",
        "enrolled-reopenable",
        "stopped",
        "failed",
        "missing",
        "unavailable",
    }
)
_OPERATOR_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_REPORT_KINDS = frozenset({"progress", "question", "blocker", "artifact", "completion_claim", "worker_output"})
_STATUS_REQUEST_STATES = frozenset({"pending", "overdue", "responded"})
_RECOVERY_ACTIONS = frozenset(
    {
        "dispose_continue_or_correct",
        "reconcile_continue_same_thread",
        "system_reconciliation",
    }
)
_RECOVERY_NOTIFICATION_STATES = frozenset(
    {"queued", "leased", "dispatched", "delivered", "acknowledged", "handled", "dead"}
)
_CAO_SUPERVISION_STATES = frozenset({"scheduled", "active", "unscheduled"})
_COMPLETION_CONTRACTS = frozenset(
    {"completion_required", "no_artifact_expected", "legacy_unclassified"}
)
_DELIVERY_STATES = frozenset(
    {"pending", "not_required", "legacy_unclassified", "ready", "delivery_missing"}
)
_ATTENTION_REASONS = frozenset(
    {
        "user-action-required",
        "cao-action-required",
        "cao-processing",
        "external-action-required",
        "progress-at-risk",
        "awaiting-explicit-close",
        "system-reconciliation",
        "runner-failed",
        "runner-missing",
        "runner-stopped",
    }
)


def _encode(value: Mapping[str, Any]) -> str:
    raw = _canonical_json(value).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode(value: str) -> dict[str, Any]:
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("invalid dashboard cursor") from error
    if not isinstance(payload, dict):
        raise ValueError("invalid dashboard cursor")
    return payload


@dataclass(frozen=True, slots=True)
class DashboardCursor:
    """A versioned opaque cursor scoped to one authority generation.

    The integrity digest detects accidental alteration without storing a new
    secret.  Authentication is still required: a cursor is not a bearer token.
    """

    authority_generation: int
    sequence: int

    def encode(self) -> str:
        stable = {
            "format": DASHBOARD_FORMAT,
            "version": _CURSOR_VERSION,
            "authority_generation": self.authority_generation,
            "sequence": self.sequence,
        }
        return _encode({**stable, "integrity": _digest(stable)})

    @classmethod
    def decode(cls, value: str) -> DashboardCursor:
        payload = _decode(value)
        stable = {
            "format": payload.get("format"),
            "version": payload.get("version"),
            "authority_generation": payload.get("authority_generation"),
            "sequence": payload.get("sequence"),
        }
        generation = stable["authority_generation"]
        sequence = stable["sequence"]
        if (
            stable["format"] != DASHBOARD_FORMAT
            or stable["version"] != _CURSOR_VERSION
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
            or not isinstance(payload.get("integrity"), str)
            or payload["integrity"] != _digest(stable)
        ):
            raise ValueError("invalid dashboard cursor")
        return cls(authority_generation=generation, sequence=sequence)


@dataclass(frozen=True, slots=True)
class DashboardResyncRequired:
    reason: str
    authority_generation: int
    cursor: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": DASHBOARD_FORMAT,
            "status": "resync-required",
            "reason": self.reason,
            "authority_generation": self.authority_generation,
            "cursor": self.cursor,
        }


class DashboardReadModel:
    """Build and stream the operator-only dashboard representation."""

    def __init__(self, service: ControlPlane) -> None:
        self.service = service

    def snapshot(self) -> dict[str, Any]:
        projection = build_projection(self.service.db)
        authority = projection.snapshot["authority"]
        operator = build_operator_view(projection.snapshot)
        cursor = DashboardCursor(
            authority_generation=int(authority["generation"]),
            sequence=int(projection.watermark["event_sequence"]),
        ).encode()
        payload = {
            "format": DASHBOARD_FORMAT,
            "authority": {
                "mode": str(authority["mode"]),
                "generation": int(authority["generation"]),
            },
            "cursor": cursor,
            # ``operator`` is the only human-actionable representation.  It
            # deliberately starts from the canonical projection rather than
            # database tables, event payloads, or service query methods.
            "operator": operator,
            "projection": _dashboard_projection(projection.as_dict(), operator=operator),
        }
        return {**payload, "snapshot_digest": _digest(payload)}

    def history(
        self, *, after: str = "", limit: int = 100
    ) -> dict[str, Any] | DashboardResyncRequired:
        projection = build_projection(self.service.db)
        authority = projection.snapshot["authority"]
        generation = int(authority["generation"])
        requested = DashboardCursor(generation, 0) if not after else DashboardCursor.decode(after)
        current_cursor = DashboardCursor(
            generation,
            int(projection.watermark["event_sequence"]),
        ).encode()
        if requested.authority_generation != generation:
            return DashboardResyncRequired(
                "authority-generation-changed", generation, current_cursor
            )

        bounds = self.service.db.fetchone(
            "SELECT MIN(sequence) AS first_sequence, MAX(sequence) AS last_sequence FROM events"
        )
        first = int(bounds["first_sequence"] or 0) if bounds else 0
        last = int(bounds["last_sequence"] or 0) if bounds else 0
        if requested.sequence > last or (
            requested.sequence > 0 and (first == 0 or first > requested.sequence + 1)
        ):
            return DashboardResyncRequired("cursor-pruned-or-invalid", generation, current_cursor)

        effective_limit = min(max(limit, 1), 1000)
        next_sequence = requested.sequence
        items: list[dict[str, Any]] = []
        while len(items) < effective_limit:
            page = self.service.list_events(after=next_sequence, limit=1000)
            raw_items = page["items"]
            if not raw_items:
                break
            reached_limit = False
            for raw in raw_items:
                next_sequence = int(raw["sequence"])
                if self._event_is_operator_visible(raw):
                    items.append(self._event(raw, generation))
                    if len(items) >= effective_limit:
                        reached_limit = True
                        break
            if reached_limit or not page["next_cursor"]:
                break
        later = self.service.db.fetchone(
            "SELECT 1 FROM events WHERE sequence > ? LIMIT 1", (next_sequence,)
        )
        return {
            "format": DASHBOARD_FORMAT,
            "authority": {"mode": str(authority["mode"]), "generation": generation},
            "after": after or None,
            "items": items,
            "next_cursor": DashboardCursor(generation, next_sequence).encode(),
            "has_more": later is not None,
        }

    def work_history(self, *, work: str = "", before: str = "", limit: int = 20) -> dict[str, Any]:
        """Page retained production Work separately from active Worker membership."""
        with self.service.db.connection_scope() as connection:
            connection.execute("BEGIN")
            try:
                return self._work_history_from_connection(
                    connection, work=work, before=before, limit=limit
                )
            finally:
                connection.rollback()

    @staticmethod
    def _work_history_from_connection(
        connection: sqlite3.Connection, *, work: str, before: str, limit: int
    ) -> dict[str, Any]:
        # Resolve opaque references from the small retained index. Canonical
        # Work details are reconstructed only for the selected page or Work.
        records = connection.execute(
            """SELECT w.id, worker.operator_label
               FROM work_items w
               JOIN principals worker ON worker.id = w.assigned_worker_id
                                      AND worker.role = 'worker'
               JOIN goal_revisions goal ON goal.work_item_id = w.id
                                       AND goal.version = w.goal_version
               WHERE w.operator_scope = 'production'
               ORDER BY w.created_at DESC, w.id DESC"""
        ).fetchall()
        response: dict[str, Any] = {
            "format": DASHBOARD_FORMAT,
            "work": None,
            "items": [],
            "entries": [],
            "next_before": None,
            "has_more": False,
        }
        if work:
            record = next(
                (raw for raw in records if history_reference("work", str(raw["id"])) == work), None
            )
            if record is None:
                raise ValueError("dashboard work is unavailable")
            raw = project_work_items_from_connection(connection, work_ids=[record["id"]])[0]
            response["work"] = _work_item_view(
                raw,
                display_label="Work item 1",
                worker_label=sanitize_operator_text(record["operator_label"]) or "Worker",
            )
            entries = work_exchanges(connection, str(raw["id"]))
            end = len(entries)
            if before:
                end = next(
                    (i for i, entry in enumerate(entries) if entry["reference"] == before), -1
                )
                if end < 0:
                    raise ValueError("dashboard history position is unavailable")
            start = max(0, end - limit)
            response.update(
                entries=entries[start:end],
                has_more=start > 0,
                next_before=entries[start]["reference"] if start > 0 else None,
            )
            return response
        start = 0
        if before:
            position = next(
                (
                    i
                    for i, raw in enumerate(records)
                    if history_reference("work", str(raw["id"])) == before
                ),
                -1,
            )
            if position < 0:
                raise ValueError("dashboard history position is unavailable")
            start = position + 1
        page = records[start : start + limit]
        projected = {
            raw["id"]: raw
            for raw in project_work_items_from_connection(
                connection, work_ids=[record["id"] for record in page]
            )
        }
        for index, record in enumerate(page, start + 1):
            item = _work_item_view(
                projected[record["id"]],
                display_label=f"Work item {index}",
                worker_label=sanitize_operator_text(record["operator_label"]) or "Worker",
            )
            response["items"].append(
                {
                    key: item[key]
                    for key in (
                        "history_reference",
                        "work_title",
                        "worker_label",
                        "completed_at",
                        "state",
                    )
                }
            )
        response["has_more"] = start + limit < len(records)
        if response["has_more"] and page:
            response["next_before"] = history_reference("work", str(page[-1]["id"]))
        return response

    async def stream(
        self,
        *,
        after: str = "",
        max_events: int | None = None,
        heartbeat_seconds: float,
    ) -> AsyncIterator[dict[str, Any]]:
        """Replay durable changes then wait for later changes."""

        cursor = after
        synchronized_cursor: str | None = None
        emitted = 0
        commit_generation = self.service.db.commit_generation()
        timed_out = False
        while True:
            page = await asyncio.to_thread(self.history, after=cursor, limit=100)
            if isinstance(page, DashboardResyncRequired):
                yield {"resync-required": page.as_dict()}
                return
            for item in page["items"]:
                cursor = str(item["cursor"])
                emitted += 1
                yield {"event": item, "cursor": cursor}
                if max_events is not None and emitted >= max_events:
                    return
            # Hidden acceptance/system events still advance the internal
            # durable cursor.  They produce neither an event nor an operator
            # count, but they must not be scanned forever while this stream is
            # alive.
            cursor = str(page["next_cursor"] or cursor)
            if page["has_more"]:
                continue
            # A cursor advances for every durable event, while operator
            # history deliberately exposes production events only.  Publish
            # an explicit replay barrier even when the intervening events
            # were hidden.  This gives EventSource a durable Last-Event-ID
            # and lets the browser reconcile exactly once after reconnect
            # without polling or treating transport-open as convergence.
            if cursor != synchronized_cursor:
                synchronized_cursor = cursor
                yield {
                    "synced": {
                        "format": DASHBOARD_FORMAT,
                        "status": "synced",
                        "cursor": cursor,
                    }
                }
            if timed_out:
                timed_out = False
                yield {"heartbeat": True}
                continue
            observed = await asyncio.to_thread(
                self.service.db.wait_for_commit,
                commit_generation,
                heartbeat_seconds,
            )
            if observed != commit_generation:
                commit_generation = observed
                continue
            timed_out = True

    @staticmethod
    def _event(raw: Mapping[str, Any], generation: int) -> dict[str, Any]:
        event_type = str(raw.get("event_type", ""))
        aggregate_type = str(raw.get("aggregate_type", ""))
        if not _EVENT_TYPE.fullmatch(event_type) or not _AGGREGATE_TYPE.fullmatch(aggregate_type):
            event_type = "control_plane.changed"
            aggregate_type = "control_plane"
        sequence = int(raw["sequence"])
        return {
            "cursor": DashboardCursor(generation, sequence).encode(),
            "event": {
                "type": event_type,
                "aggregate_type": aggregate_type,
                "occurred_at": str(raw["created_at"]),
            },
        }

    def _event_is_operator_visible(self, raw: Mapping[str, Any]) -> bool:
        """Use the immutable event-time scope; never reclassify history later."""

        return raw.get("operator_scope") == "production"


def build_operator_view(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Return a strict, human-actionable DTO from canonical projection data.

    Its operator content is already a bounded, secret-redacted canonical
    projection field. This read edge never reconstructs it from ids,
    metadata, generic messages, raw prompts, event payloads, or runtime state.
    Closure evidence is limited to a canonical, status-only
    ``closure_summary``; paths, payloads, evidence references, and internal
    identifiers are never copied into the DTO. Stable ordinal labels preserve
    operator orientation without exposing opaque database locators.
    """

    raw_items = snapshot.get("work_items", [])
    items = (
        [item for item in raw_items if isinstance(item, Mapping)]
        if isinstance(raw_items, list)
        else []
    )
    raw_workers = snapshot.get("operator_workers", [])
    workers: list[Mapping[str, Any]] = (
        [item for item in raw_workers if isinstance(item, Mapping)]
        if isinstance(raw_workers, list)
        else []
    )
    scoped_contract = bool(workers) or any("operator_scope" in item for item in items)
    if not workers and not scoped_contract:
        workers = _legacy_workers(items)

    visible_workers = [item for item in workers if item.get("operator_scope") == "production"]
    visible_workers.sort(key=lambda item: str(item.get("principal_id", "")))
    labels = _worker_labels(visible_workers)
    current_by_worker: dict[str, list[tuple[Mapping[str, Any], dict[str, Any]]]] = {}
    recently_completed: list[tuple[Mapping[str, Any], dict[str, Any]]] = []
    legacy_all_work: list[dict[str, Any]] = []
    work_index = 0
    for raw in items:
        if scoped_contract and raw.get("operator_scope") != "production":
            continue
        worker_id = raw.get("assigned_worker_id")
        if not isinstance(worker_id, str) or worker_id not in labels:
            continue
        work_index += 1
        item = _work_item_view(
            raw,
            display_label=f"Work item {work_index}",
            worker_label=labels[worker_id],
        )
        if not scoped_contract:
            legacy_all_work.append(item)
        if _is_current_work(item):
            current_by_worker.setdefault(worker_id, []).append((raw, item))
        elif _is_supervisor_settled_completion(item):
            recently_completed.append((raw, item))

    for values in current_by_worker.values():
        values.sort(key=_work_sort_key)

    grouped: dict[str, list[tuple[Mapping[str, Any], dict[str, Any]]]] = {
        "cao_processing": [],
        "user_confirmation": [],
        "stopped_or_failed": [],
        "working": [],
        "ready": [],
        "inactive_workers": [],
    }
    for raw in visible_workers:
        worker_id = str(raw.get("principal_id", ""))
        current = current_by_worker.get(worker_id, [])
        category = _worker_category(raw, current)
        public = _worker_view(
            raw,
            worker_label=labels[worker_id],
            current_work_items=[item for _source, item in current],
            category=category,
        )
        grouped[category].append((raw, public))

    for category in ("user_confirmation", "stopped_or_failed", "cao_processing"):
        grouped[category].sort(key=_attention_worker_sort_key)
    for category in ("working", "ready", "inactive_workers"):
        grouped[category].sort(key=lambda value: value[1]["worker_label"].casefold())
    canonical_categories = (
        "cao_processing",
        "user_confirmation",
        "stopped_or_failed",
        "working",
        "ready",
        "inactive_workers",
    )
    public_groups = {
        key: [public for _raw, public in grouped[key]]
        for key in canonical_categories
    }
    legacy_needs_attention = [
        *public_groups["user_confirmation"],
        *public_groups["stopped_or_failed"],
        *public_groups["cao_processing"],
    ]
    current_work = [
        item
        for category in canonical_categories
        for worker in public_groups[category]
        for item in worker["current_work_items"]
    ]
    recently_completed.sort(
        key=lambda value: (
            str(value[0].get("updated_at", "")),
            str(value[1].get("display_label", "")),
        ),
        reverse=True,
    )
    public_recently_completed = [item for _raw, item in recently_completed[:20]]
    return {
        "format": OPERATOR_FORMAT,
        "counts": {
            "needs_attention": len(legacy_needs_attention),
            "working": len(public_groups["working"]),
            "ready": len(public_groups["ready"]),
            "inactive_workers": len(public_groups["inactive_workers"]),
            "current_work_items": len(current_work),
        },
        # Deprecated v1 aggregate retained for non-visual API consumers. The
        # Dashboard renders the explicit ownership categories below.
        "needs_attention": legacy_needs_attention,
        **public_groups,
        "recently_completed": public_recently_completed,
        # Compatibility for existing consumers.  A canonical scoped snapshot
        # contains only current production Work here; old direct unit fixtures
        # retain their previous all-Work behavior.
        "work_items": current_work if scoped_contract else legacy_all_work,
        "runtime_delivery": _runtime_delivery(snapshot),
    }


def _legacy_workers(items: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [
        {
            "principal_id": worker_id,
            "operator_scope": "production",
            "operator_label": None,
            "principal_enabled": True,
            "worker_state": "enabled",
            "runner_availability": "unavailable",
            "runner_connection_state": "unavailable",
        }
        for worker_id in sorted(
            {
                str(item["assigned_worker_id"])
                for item in items
                if isinstance(item.get("assigned_worker_id"), str)
            }
        )
    ]


def _worker_labels(workers: list[Mapping[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for production_index, worker in enumerate(workers, start=1):
        worker_id = str(worker.get("principal_id", ""))
        label = sanitize_operator_text(worker.get("operator_label"))
        labels[worker_id] = label or f"Worker {production_index}"
    return labels


def _work_item_view(
    raw: Mapping[str, Any],
    *,
    display_label: str,
    worker_label: str,
) -> dict[str, Any]:
    state = _enum(raw.get("state"), _WORK_STATES)
    attempt_state = _enum(raw.get("current_attempt_state"), _ATTEMPT_STATES)
    trajectory = _enum(raw.get("current_attempt_trajectory"), _TRAJECTORIES)
    attention_owner = _enum(raw.get("attention_owner"), _ATTENTION_OWNERS)
    boundaries = _nonnegative_int(raw.get("open_boundary_count"))
    closure = _closure_summary(raw.get("closure_summary"))
    content = raw.get("operator_content")
    operator_content = content if isinstance(content, Mapping) else {}
    title = sanitize_operator_text(operator_content.get("work_title"))
    objective = sanitize_operator_text(operator_content.get("objective_summary"))
    progress_stage = sanitize_operator_text(operator_content.get("progress_stage"))
    next_boundary = sanitize_operator_text(operator_content.get("next_boundary_summary"))
    recovery_action = _enum(operator_content.get("recovery_action"), _RECOVERY_ACTIONS)
    recovery_waiting_since = _timestamp(operator_content.get("recovery_waiting_since"))
    recovery_notification_state = _enum(
        operator_content.get("recovery_notification_state"),
        _RECOVERY_NOTIFICATION_STATES,
    )
    cao_supervision_state = _enum(
        operator_content.get("cao_supervision_state"),
        _CAO_SUPERVISION_STATES,
    )
    cao_supervision_updated_at = _timestamp(
        operator_content.get("cao_supervision_updated_at")
    )
    report_kind = _enum(operator_content.get("latest_report_kind"), _REPORT_KINDS)
    report = sanitize_operator_text(operator_content.get("latest_worker_report_summary"))
    reported_at = _timestamp(operator_content.get("latest_reported_at"))
    runtime_heartbeat_at = _timestamp(operator_content.get("runtime_heartbeat_at"))
    last_worker_activity_at = _timestamp(operator_content.get("last_worker_activity_at"))
    last_artifact_at = _timestamp(operator_content.get("last_artifact_at"))
    status_request_state = _enum(
        operator_content.get("status_request_state"), _STATUS_REQUEST_STATES
    )
    status_requested_at = _timestamp(operator_content.get("status_requested_at"))
    status_response_due_at = _timestamp(operator_content.get("status_response_due_at"))
    status_responded_at = _timestamp(operator_content.get("status_responded_at"))
    completion_contract = _enum(operator_content.get("completion_contract"), _COMPLETION_CONTRACTS)
    delivery_state = _enum(operator_content.get("delivery_state"), _DELIVERY_STATES)
    runner_adapter = _enum(operator_content.get("runner_adapter"), _RUNNER_ADAPTERS)
    runner_model = _model_label(operator_content.get("runner_model"))
    runner_reasoning_effort = _enum(
        operator_content.get("runner_reasoning_effort"), _REASONING_EFFORTS
    )
    runner_requested_model = _model_label(operator_content.get("runner_requested_model"))
    runner_effective_model = _model_label(operator_content.get("runner_effective_model"))
    runner_requested_reasoning_effort = _enum(
        operator_content.get("runner_requested_reasoning_effort"), _REASONING_EFFORTS
    )
    runner_effective_reasoning_effort = _enum(
        operator_content.get("runner_effective_reasoning_effort"), _REASONING_EFFORTS
    )
    runner_availability = _enum(operator_content.get("runner_availability"), _RUNNER_AVAILABILITY)
    runner_state = _enum(operator_content.get("runner_state"), _RUNNER_SPEC_STATES)
    runner_connection_state = _enum(
        operator_content.get("runner_connection_state"), _RUNNER_CONNECTION_STATES
    )
    return {
        "display_label": display_label,
        "history_reference": history_reference("work", str(raw["id"])) if raw.get("id") else None,
        "worker_label": worker_label,
        "work_title": title,
        "objective_summary": objective,
        "objective_text": full_operator_text(operator_content.get("objective_text")),
        "state": state,
        "attempt_state": attempt_state,
        "progress_stage": progress_stage,
        "trajectory": trajectory,
        "attention_owner": attention_owner,
        "supervision_pause": _supervision_pause_view(raw.get("supervision_pause")),
        "next_boundary_summary": next_boundary,
        "pending_supervisor_boundary": boundaries > 0,
        "recovery_action": recovery_action,
        "recovery_waiting_since": recovery_waiting_since,
        "recovery_notification_state": recovery_notification_state,
        "cao_supervision_state": cao_supervision_state,
        "cao_supervision_updated_at": cao_supervision_updated_at,
        "latest_report_kind": report_kind,
        "latest_report_summary": report,
        "latest_report_text": full_operator_text(operator_content.get("latest_report_text")),
        "latest_reported_at": reported_at,
        "runtime_heartbeat_at": runtime_heartbeat_at,
        "last_worker_activity_at": last_worker_activity_at,
        "last_artifact_at": last_artifact_at,
        "status_request_state": status_request_state,
        "status_requested_at": status_requested_at,
        "status_response_due_at": status_response_due_at,
        "status_responded_at": status_responded_at,
        "completion_contract": completion_contract,
        "delivery_state": delivery_state,
        # Compatibility aliases retained for existing v1 consumers.  The new
        # fields above keep lifecycle state, Worker-declared progress, and a
        # pending CAO boundary as three distinct concepts.
        "stage": attempt_state,
        "next_observable_boundary": "pending" if boundaries else None,
        "latest_worker_report_summary": report,
        "runner_adapter": runner_adapter,
        "runner_model": runner_model,
        "runner_reasoning_effort": runner_reasoning_effort,
        "runner_requested_model": runner_requested_model,
        "runner_effective_model": runner_effective_model,
        "runner_requested_reasoning_effort": runner_requested_reasoning_effort,
        "runner_effective_reasoning_effort": runner_effective_reasoning_effort,
        "runner_availability": runner_availability,
        "runner_state": runner_state,
        "runner_connection_state": runner_connection_state,
        "latest_cao_review_decision": closure["cao_review"],
        "requester_decision": closure["requester_decision"],
        "closure_state": _closure_state(raw.get("closure_state"), state),
        "closure_summary": closure,
        "completed_at": (
            _timestamp(raw.get("updated_at")) if _is_successful_completion_fields(
                state=state,
                attempt_state=attempt_state,
                latest_cao_review=closure["cao_review"],
                pending_supervisor_boundary=boundaries > 0,
                recovery_action=recovery_action,
            ) else None
        ),
        "availability": {
            "work_title": _availability(title),
            "objective_summary": _availability(objective),
            "attempt_state": _availability(attempt_state),
            "progress_stage": _availability(progress_stage),
            "next_boundary_summary": _availability(next_boundary),
            "pending_supervisor_boundary": "available",
            "recovery_action": _availability(recovery_action),
            "recovery_waiting_since": _availability(recovery_waiting_since),
            "recovery_notification_state": _availability(recovery_notification_state),
            "latest_report_kind": _availability(report_kind),
            "latest_report_summary": _availability(report),
            "latest_reported_at": _availability(reported_at),
            "runtime_heartbeat_at": _availability(runtime_heartbeat_at),
            "last_worker_activity_at": _availability(last_worker_activity_at),
            "last_artifact_at": _availability(last_artifact_at),
            "status_request_state": _availability(status_request_state),
            "status_requested_at": _availability(status_requested_at),
            "status_response_due_at": _availability(status_response_due_at),
            "status_responded_at": _availability(status_responded_at),
            "completion_contract": _availability(completion_contract),
            "delivery_state": _availability(delivery_state),
            "next_observable_boundary": "available" if boundaries else "unavailable",
            "latest_worker_report_summary": _availability(report),
            "runner_adapter": _availability(runner_adapter),
            "runner_model": _availability(runner_model),
            "runner_reasoning_effort": _availability(runner_reasoning_effort),
            "runner_requested_model": _availability(runner_requested_model),
            "runner_effective_model": _availability(runner_effective_model),
            "runner_requested_reasoning_effort": _availability(runner_requested_reasoning_effort),
            "runner_effective_reasoning_effort": _availability(runner_effective_reasoning_effort),
            "runner_availability": _availability(runner_availability),
            "runner_state": _availability(runner_state),
            "runner_connection_state": _availability(runner_connection_state),
            "latest_cao_review_decision": _availability(closure["cao_review"]),
            "requester_decision": _availability(closure["requester_decision"]),
            "closure_summary": _closure_summary_availability(closure),
        },
    }


def _supervision_pause_view(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    source_generation = _nonnegative_int(value.get("source_generation"))
    pause_generation = _nonnegative_int(value.get("pause_generation"))
    if source_generation < 1 or pause_generation != source_generation + 1:
        return None
    # The operator edge deliberately omits the internal Boundary identifier
    # and never exposes resumption instructions or evidence.
    return {
        "source_generation": source_generation,
        "pause_generation": pause_generation,
        "reason": sanitize_operator_text(value.get("reason")),
        "resume_condition": sanitize_operator_text(value.get("resume_condition")),
        "paused_at": _timestamp(value.get("paused_at")),
    }


def _worker_view(
    raw: Mapping[str, Any],
    *,
    worker_label: str,
    current_work_items: list[dict[str, Any]],
    category: str,
) -> dict[str, Any]:
    return {
        "worker_label": worker_label,
        "attention_reason": (
            _worker_attention_reason(raw, current_work_items)
            if category in {"cao_processing", "user_confirmation", "stopped_or_failed"}
            else None
        ),
        "worker_state": _enum(raw.get("worker_state"), _RUNNER_SPEC_STATES),
        "runner_adapter": _enum(raw.get("runner_adapter"), _RUNNER_ADAPTERS),
        "runner_model": _model_label(raw.get("runner_model")),
        "runner_reasoning_effort": _enum(raw.get("runner_reasoning_effort"), _REASONING_EFFORTS),
        "runner_requested_model": _model_label(raw.get("runner_requested_model")),
        "runner_effective_model": _model_label(raw.get("runner_effective_model")),
        "runner_requested_reasoning_effort": _enum(
            raw.get("runner_requested_reasoning_effort"), _REASONING_EFFORTS
        ),
        "runner_effective_reasoning_effort": _enum(
            raw.get("runner_effective_reasoning_effort"), _REASONING_EFFORTS
        ),
        "runner_availability": _enum(raw.get("runner_availability"), _RUNNER_AVAILABILITY),
        "runner_connection_state": _enum(
            raw.get("runner_connection_state"), _RUNNER_CONNECTION_STATES
        ),
        "current_work_items": current_work_items,
    }


def _worker_attention_reason(
    raw: Mapping[str, Any], current_work_items: list[dict[str, Any]]
) -> str | None:
    """Return one server-owned, bounded reason for an actionable Worker card."""

    if any(
        item.get("state") in {"waiting_user", "user_needed"}
        or item.get("attempt_state") == "input_required"
        or item.get("attention_owner") == "user"
        for item in current_work_items
    ):
        return "user-action-required"
    if any(
        item.get("cao_supervision_state") in {"scheduled", "active"}
        for item in current_work_items
    ):
        return "cao-processing"
    if any(
        item.get("recovery_action") == "system_reconciliation"
        for item in current_work_items
    ):
        return "system-reconciliation"
    for owner, reason in (
        ("user", "user-action-required"),
        ("cao", "cao-action-required"),
        ("external", "external-action-required"),
    ):
        if any(item.get("attention_owner") == owner for item in current_work_items):
            return reason
    if any(
        item.get("state") in {"waiting_supervisor", "waiting_review"}
        or item.get("attempt_state") in {"waiting_supervisor", "submitted"}
        for item in current_work_items
    ):
        return "cao-action-required"
    if any(
        item.get("state") in {"waiting_user", "user_needed"}
        or item.get("attempt_state") == "input_required"
        for item in current_work_items
    ):
        return "user-action-required"
    if any(
        item.get("trajectory") in {"at_risk", "stalled", "drifting"} for item in current_work_items
    ):
        return "progress-at-risk"
    if any(item.get("closure_state") == "awaiting-explicit-close" for item in current_work_items):
        return "awaiting-explicit-close"
    connection = raw.get("runner_connection_state")
    if connection in {"failed", "missing", "stopped"}:
        reason = f"runner-{connection}"
        return reason if reason in _ATTENTION_REASONS else None
    if raw.get("principal_enabled") is not True or raw.get("worker_state") in {
        "stopped",
        "revoked",
    }:
        return "runner-stopped"
    if connection == "unavailable" or raw.get("enrollment_state") in {
        "failed",
        "revoked",
        "stale",
    }:
        return "cao-action-required"
    return None


def _is_current_work(item: Mapping[str, Any]) -> bool:
    return (
        not _is_supervisor_settled_completion(item)
        and item.get("closure_state") != "closed"
        and item.get("state")
        in {
            "open",
            "active",
            "suspended",
            "waiting_supervisor",
            "waiting_review",
            "waiting_user",
            "user_needed",
            "completed",
        }
    )


def _is_supervisor_settled_completion(item: Mapping[str, Any]) -> bool:
    """Separate a verified result from a genuine requester-input boundary.

    Completion disposition historically moves Work to ``waiting_user`` so a
    later requester decision and explicit close can remain durable. That
    bookkeeping state must not make an already verified, boundary-free result
    look like an active request for user input on the Dashboard. Real
    requester questions use ``input_required`` and/or retain an open Boundary,
    so they remain current and visible.
    """

    return _is_successful_completion_fields(
        state=item.get("state"),
        attempt_state=item.get("attempt_state"),
        latest_cao_review=item.get("latest_cao_review_decision"),
        pending_supervisor_boundary=item.get("pending_supervisor_boundary") is True,
        recovery_action=item.get("recovery_action"),
    )


def _is_successful_completion_fields(
    *,
    state: object,
    attempt_state: object,
    latest_cao_review: object,
    pending_supervisor_boundary: bool,
    recovery_action: object,
) -> bool:
    return bool(
        state in {"waiting_user", "completed"}
        and attempt_state == "completed"
        and latest_cao_review == "ok"
        and not pending_supervisor_boundary
        and recovery_action is None
    )


def _worker_category(
    raw: Mapping[str, Any],
    current: list[tuple[Mapping[str, Any], dict[str, Any]]],
) -> str:
    public_work = [item for _source, item in current]
    connection = raw.get("runner_connection_state")
    enrollment = raw.get("enrollment_state")
    if any(
        item.get("attention_owner") == "user"
        or item.get("state") in {"waiting_user", "user_needed"}
        or item.get("attempt_state") == "input_required"
        for item in public_work
    ):
        return "user_confirmation"
    cao_work = [
        item
        for item in public_work
        if item.get("attention_owner") == "cao"
        or item.get("state") in {"waiting_supervisor", "waiting_review"}
        or item.get("attempt_state") in {"waiting_supervisor", "submitted"}
    ]
    if cao_work:
        if any(item.get("cao_supervision_state") == "unscheduled" for item in cao_work):
            return "stopped_or_failed"
        return "cao_processing"
    if any(
        item.get("attention_owner") == "external"
        or item.get("attempt_state") == "blocked"
        or item.get("trajectory") in {"at_risk", "stalled", "drifting"}
        or item.get("closure_state") == "awaiting-explicit-close"
        for item in public_work
    ):
        return "stopped_or_failed"
    if public_work:
        if (
            raw.get("principal_enabled") is not True
            or raw.get("worker_state") in {"stopped", "revoked"}
            or connection in {"failed", "missing", "stopped", "unavailable"}
            or enrollment in {"failed", "revoked", "stale"}
        ):
            return "stopped_or_failed"
        return "working"
    if raw.get("principal_enabled") is not True or raw.get("worker_state") in {
        "stopped",
        "revoked",
    }:
        return "inactive_workers"
    # An idle active Worker is a durable handle, not a promise that its
    # replaceable transport is currently connected. Instruct can reconnect or
    # queue it, so transport loss without current Work is not operator work.
    return "ready"


def _work_sort_key(
    value: tuple[Mapping[str, Any], Mapping[str, Any]],
) -> tuple[int, str, str]:
    raw, public = value
    priority = raw.get("priority")
    numeric_priority = (
        priority if isinstance(priority, int) and not isinstance(priority, bool) else 0
    )
    return (-numeric_priority, str(raw.get("updated_at", "")), str(public.get("display_label", "")))


def _attention_worker_sort_key(
    value: tuple[Mapping[str, Any], Mapping[str, Any]],
) -> tuple[int, int, str]:
    _raw, public = value
    work = public.get("current_work_items", [])
    items = work if isinstance(work, list) else []
    owner_rank = {"user": 0, "cao": 1, "external": 2, "worker": 3, "none": 4}
    rank = min((owner_rank.get(str(item.get("attention_owner")), 5) for item in items), default=5)
    return (rank, -len(items), str(public.get("worker_label", "")).casefold())


def _dashboard_projection(
    projection: Mapping[str, Any],
    *,
    operator: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep production-scoped integrity evidence without internal locators."""

    snapshot = projection.get("snapshot", {})
    values = snapshot if isinstance(snapshot, Mapping) else {}
    violations = projection.get("violations", [])
    safe_violations = [
        item
        for item in violations
        if isinstance(item, Mapping)
        and item.get("subject_type") in {"database", "control_authority"}
    ]
    counts = operator.get("counts", {})
    operator_counts = counts if isinstance(counts, Mapping) else {}
    return {
        "format": "cao-control-plane-dashboard-projection/v1",
        "healthy": not safe_violations,
        "snapshot": {
            # Submitted intents do not yet carry an operator scope.  Failing
            # closed avoids folding acceptance/system traffic into a human
            # count until an attributable Work exists.
            "pending_intents": 0,
            "work_item_count": _nonnegative_int(operator_counts.get("current_work_items")),
            "runtime_delivery": _runtime_delivery(values),
        },
        "violations": [
            {
                "code": _safe_label(item.get("code")),
                "subject_type": _safe_label(item.get("subject_type")),
                "count": _nonnegative_int(item.get("count")),
            }
            for item in safe_violations
        ],
    }


def _runtime_delivery(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    scoped = snapshot.get("operator_runtime_delivery")
    if isinstance(scoped, Mapping):
        effects = scoped.get("effects_by_state", {})
        return {
            "runtime_count": _nonnegative_int(scoped.get("runtime_count")),
            "queued_deliveries": _nonnegative_int(scoped.get("queued_deliveries")),
            "unknown_delivery_outcomes": _nonnegative_int(scoped.get("unknown_delivery_outcomes")),
            "dead_deliveries": _nonnegative_int(scoped.get("dead_deliveries")),
            "effects_by_state": _count_map(effects),
        }
    runtimes = snapshot.get("runtimes", [])
    scheduler = snapshot.get("scheduler", {})
    values = scheduler if isinstance(scheduler, Mapping) else {}
    effects = snapshot.get("effects_by_state", snapshot.get("effects", {}))
    return {
        "runtime_count": len(runtimes) if isinstance(runtimes, list) else 0,
        "queued_deliveries": _nonnegative_int(values.get("queued_due")),
        "unknown_delivery_outcomes": _nonnegative_int(values.get("outcome_unknown")),
        "dead_deliveries": _nonnegative_int(values.get("dead")),
        "effects_by_state": _count_map(effects),
    }


def _enum(value: object, allowed: frozenset[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _model_label(value: object) -> str | None:
    """Accept only the bounded canonical model label, never arbitrary text."""

    return model_identifier(value)


def _timestamp(value: object) -> str | None:
    """Accept only the canonical bounded timestamp used by the event store."""

    return value if isinstance(value, str) and _OPERATOR_TIMESTAMP.fullmatch(value) else None


def _closure_state(value: object, work_state: str | None) -> str:
    """Normalize an explicit projection field without inventing a close."""

    explicit = _enum(value, _CLOSURE_STATES)
    if explicit is not None:
        return explicit
    return "awaiting-explicit-close" if work_state == "completed" else "open"


def _closure_summary(value: object) -> dict[str, str | int | None]:
    """Allowlist status-only close evidence supplied by the projection.

    Missing projection support remains visibly unavailable rather than being
    inferred from WorkItem state or aggregate runtime counters.
    """

    raw = value if isinstance(value, Mapping) else {}
    return {
        "requester_decision": _enum(raw.get("requester_decision"), _REQUESTER_DECISIONS),
        "cao_review": _enum(raw.get("cao_review"), _CAO_REVIEW_DECISIONS),
        "artifact_preservation": _enum(
            raw.get("artifact_preservation"), _ARTIFACT_PRESERVATION_STATES
        ),
        "cleanup": _enum(raw.get("cleanup"), _CLEANUP_STATES),
        "unresolved_deliveries": _optional_nonnegative_int(raw.get("unresolved_deliveries")),
        "unresolved_effects": _optional_nonnegative_int(raw.get("unresolved_effects")),
        "active_runtimes": _optional_nonnegative_int(raw.get("active_runtimes")),
    }


def _availability(value: object) -> str:
    return "available" if value is not None else "unavailable"


def _closure_summary_availability(summary: Mapping[str, object]) -> str:
    return "available" if any(value is not None for value in summary.values()) else "unavailable"


def _nonnegative_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _optional_nonnegative_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _safe_label(value: object) -> str:
    return value if isinstance(value, str) and _AGGREGATE_TYPE.fullmatch(value) else "unknown"


def _count_map(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: _nonnegative_int(item)
        for key, item in value.items()
        if isinstance(key, str) and _AGGREGATE_TYPE.fullmatch(key)
    }
