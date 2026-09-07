"""Canonical immutable goal and attempt packet identities.

The digest functions in this module are deliberately transport- and runtime-
independent.  A Worker prompt, MCP request, terminal screen, or projection may
carry these digests, but none of those surfaces defines them.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .canonical import canonical_json as _canonical_json
from .canonical import canonical_sha256

GOAL_PACKET_FORMAT = "cao-goal-packet/v1"
TASK_PACKET_FORMAT = "cao-task-packet/v1"


def canonical_json(value: object) -> str:
    """Compatibility entry point for the persisted packet encoding."""

    return _canonical_json(value)


def canonical_digest(value: object) -> str:
    return canonical_sha256(value)


def build_goal_packet(
    *,
    work_item_id: str,
    version: int,
    title: str,
    objective: str,
    maturity: str,
    acceptance: Sequence[str],
    non_goals: Sequence[str],
    priority: int,
    requester_id: str | None,
    supervisor_id: str | None,
    metadata: Mapping[str, Any],
    reason: str,
    created_by: str,
    source_intent_id: str | None,
    source_directive_id: str | None,
    correlation_id: str,
    prior_version: int | None,
    completion_contract: str = "legacy_unclassified",
    supervisor_attachment: Mapping[str, Any] | None = None,
    dependencies: Sequence[str] = (),
    managed_task_policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the complete immutable goal-revision packet."""

    packet: dict[str, Any] = {
        "format": GOAL_PACKET_FORMAT,
        "work_item_id": work_item_id,
        "version": version,
        "title": title,
        "objective": objective,
        "maturity": maturity,
        "acceptance": list(acceptance),
        "non_goals": list(non_goals),
        "priority": priority,
        "requester_id": requester_id,
        "supervisor_id": supervisor_id,
        "metadata": dict(metadata),
        "reason": reason,
        "created_by": created_by,
        "source_intent_id": source_intent_id,
        "source_directive_id": source_directive_id,
        "correlation_id": correlation_id,
        "prior_version": prior_version,
    }
    # Attachment binding was added after the original v1 packet format had
    # already been persisted.  An unbound legacy packet omitted this key; an
    # explicit null would silently change its immutable digest during schema
    # migration.  Keep the v1 unbound representation byte-for-byte stable and
    # add the field only for a genuinely attached conversation.
    if supervisor_attachment is not None:
        packet["supervisor_attachment"] = dict(supervisor_attachment)
    # Completion contracts postdate the v1 packet format.  Omission remains
    # the canonical representation for historical packets so their immutable
    # digests survive upgrade and replay.
    if completion_contract != "legacy_unclassified":
        packet["completion_contract"] = completion_contract
    # Assignment dependencies were added after v1 packets were already
    # persisted.  Omission is the canonical empty representation so existing
    # packet digests remain byte-for-byte stable.
    if dependencies:
        packet["dependencies"] = list(dependencies)
    if managed_task_policy is not None:
        packet["managed_task_policy"] = dict(managed_task_policy)
    return packet


def goal_packet_digest(packet: Mapping[str, Any]) -> str:
    if packet.get("format") != GOAL_PACKET_FORMAT:
        raise ValueError("unsupported goal packet format")
    return canonical_digest(dict(packet))


def build_task_packet(
    *,
    goal_packet_digest_value: str,
    work_item_id: str,
    goal_version: int,
    attempt_id: str,
    attempt_number: int,
    worker_id: str,
    runtime_session_id: str | None,
    supervisor_attachment: Mapping[str, Any] | None = None,
    dependencies: Sequence[str] = (),
) -> dict[str, Any]:
    """Build one immutable execution packet for a concrete Attempt."""

    packet: dict[str, Any] = {
        "format": TASK_PACKET_FORMAT,
        "goal_packet_digest": goal_packet_digest_value,
        "work_item_id": work_item_id,
        "goal_version": goal_version,
        "attempt_id": attempt_id,
        "attempt_number": attempt_number,
        "worker_id": worker_id,
        "runtime_session_id": runtime_session_id,
    }
    if supervisor_attachment is not None:
        packet["supervisor_attachment"] = dict(supervisor_attachment)
    if dependencies:
        packet["dependencies"] = list(dependencies)
    return packet


def task_packet_digest(packet: Mapping[str, Any]) -> str:
    if packet.get("format") != TASK_PACKET_FORMAT:
        raise ValueError("unsupported task packet format")
    return canonical_digest(dict(packet))
