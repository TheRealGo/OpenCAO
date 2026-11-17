"""Durable Worker business purpose, independent of individual instructions."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from .canonical import canonical_sha256
from .errors import ConflictError, ValidationError
from .goal_packets import goal_packet_digest
from .models import WorkerPurposeContract

PURPOSE_FORMAT = "cao-worker-purpose/v1"
STRUCTURED_PURPOSE_FORMAT = "cao-worker-purpose/v2"
ALLOCATION_GUIDANCE = (
    "CAO chooses Worker allocation and serial or parallel execution before New or instruction "
    "from outcome dependencies, purpose, authority, cost and shared-resource conflicts. "
    "Independent work normally uses separate Workers; dependent checkpoints may retain the same Worker. "
    "When shared files or authenticated sessions conflict, choose either the same Worker or separate Workers "
    "with ordering or isolation case by case. A shared Directory, broad topic, existing Worker or "
    "historical task count alone cannot decide allocation. Queue admission is a delivery capability, "
    "not a routing decision. Keep a concise allocation reason in CAO supervision context. "
    "Do not return routine allocation choices to the requester. Confirm distinct Worker execution "
    "before reporting parallel progress. Preserve exact purpose, recovery and effect authority."
)
PURPOSE_AUTHORING_GUIDANCE = (
    "Write business purpose as a stable desired state within the entrusted responsibility, "
    "with the background and need that make that state valuable. Put this explanation in "
    "the purpose text; use the contract for observable outcome, boundaries and evidence. "
    "Do not merely copy an issue title, its WHAT/WHY, a deliverable or the latest instruction. "
    "Choose enough context to explain why the task serves the state without broadening the "
    "requester's scope or inventing a larger mission. Keep methods, current checkpoints, "
    "approved payloads and unrelated operating rules in their task or authority context. "
    "A next task, method change or approval satisfying an existing condition does not change "
    "the purpose. If a sealed scope or authority correction requires a new version, preserve "
    "the unchanged background and desired state and identify the actual contract correction. "
    "When an unexpected obstacle blocks a task, reason from the same desired state: identify "
    "the unmet condition, the evidence and uncertainty, what must be checked or satisfied, "
    "and why the next authorized action helps. A failed method is not a new purpose. "
    "Revise business direction only when requester direction changes the entrusted need or "
    "desired state; state that change explicitly. Examples and planning vocabularies clarify "
    "intent, not a mandatory hierarchy or granularity. "
    "Write Worker-facing purpose, titles, instructions, acceptance, non-goals and forwarded "
    "context as the requester's direct business request. Do not introduce an internal "
    "supervisor as a separate requester, approver or recipient, spell out its role, or send "
    "a warning about omitting its name. Refer to existing task records when they already "
    "identify the material to change. Preserve actual authority; requester voice does not "
    "invent personal approval, completed actions or permission for external effects. "
    "Keep technical API identifiers and accurate internal audit records unchanged."
)
PURPOSE_GUIDANCE = PURPOSE_AUTHORING_GUIDANCE + " " + (
    "CAO is the Chief Agent Officer supervising its Workers. Before every instruction "
    "or judgment of a proposal, progress, or result, read this Worker's current business "
    "purpose and judge the next action against it. Confirm its version in the command. "
    "The Worker owns methods, implementation, experiments, diagnosis and ordinary choices "
    "within the sealed task. CAO evaluates submitted results against task background, purpose "
    "and acceptance, returning missing evidence or unmet outcomes without prescribing the "
    "implementation or doing the Worker's work. Do not add approval gates for ordinary "
    "choices. Intervene for an actual scope, authority, resource or unresolved blocker boundary. "
    "Normally judge the report; audit original records for concrete needs and record the actual basis. "
    "Distinguish reported findings, mechanically checked bindings and independent audits in "
    "judgments and requester-facing conclusions. Routine progress needs "
    "no approval. Accept a satisfied task without moving its acceptance conditions merely "
    "because the broader purpose remains ongoing; delegate the next outcome when needed. "
    "A task is a means toward that purpose; task completion is not purpose achievement. "
    "A local instruction or constraint never replaces the business purpose. Define or "
    "revise purpose only from the requester's business direction, not the latest task."
    " Continue while the assigned purpose is unmet. If the Worker cannot proceed, it "
    "must escalate to CAO. CAO owns routine system diagnosis and recovery without "
    "requester approval, preserving unknown outcomes. A technical blocker remains "
    "wait_system under CAO ownership. Report unresolved system impact. "
    "A Worker-reported capability blocker can enter wait_system with a concrete independently "
    "reviewed recovery instruction and resume condition; it need not be a runtime crash. "
    "Execute that remedy through an admitted correction or an appropriate authorized Worker. "
    "Check actual tool/provider/account availability, reuse an already functioning authorized "
    "route when appropriate, and preserve scope and unknown outcomes. Tool absence, provider "
    "disconnection and a missing new grant are different facts. Neither a passive hold nor "
    "an unchanged status report repairs them. Recovery consultation "
    "requires concrete new authority, budget, business direction or private information. "
    "For an unresolved business blocker, ask for the concrete advice or decision needed, "
    "including missing knowledge or an unknown solution. "
    "Never silently stop after a failed attempt, "
    "pause or completed checkpoint. A handled notification is not a resolved purpose. "
    "Accepting a managed checkpoint retains a durable purpose_continuation. Commit the "
    "next sealed Work with that identity and generation, or record its evidence-backed "
    "achievement, scheduled observation or exact missing requester input. A task's "
    "requester-acceptance wait does not suspend the entrusted purpose."
    " Worker Finish/Delete does not achieve or pause its business purpose; preserve "
    "the next decision for an appropriate handoff without automatically reviving a closed Worker. "
    "CAO owns purpose achievement and explicit requester-directed purpose pause/resumption; "
    "ordinary continuation needs no fresh requester grant."
    " Purpose setting is CAO's primary judgment: define outcome, scope, fixed criteria with "
    "evidence, completion, authority and unmet_action before delegation. Changes require "
    "requester direction and an explicit new purpose version. Review the frozen task's "
    "review_criteria; map each finding to its criterion and evidence. Do not reject a "
    "satisfied checkpoint because other business-purpose outcomes remain unmet. Read "
    "purpose_alignment after a purpose revision; a saved purpose is not a revised task. "
    "If changed, explicitly confirm the assignment purpose version for the frozen review "
    "or revise the task at an admitted safe boundary."
    " Only an actual Worker instruction receives independent Codex alignment review before "
    "commit. The checker compares the registered purpose and contract with the effective "
    "instruction; it has no tools and does not read requester conversations, approval "
    "records or prior results. CAO owns purpose quality, requester intent, approved content "
    "and evidence judgment. Purpose registration/revision and CAO-only next decisions do "
    "not invoke another AI. Correct a rejected instruction's actual mismatch; do not rewrite "
    "purpose or fixed criteria merely to pass the check, or retry the same rejected proposal. "
    "Explicit Finish/Delete and unrelated lifecycle remain available."
) + " " + ALLOCATION_GUIDANCE


def purpose_packet(
    *,
    worker_thread_id: str,
    version: int,
    purpose: str,
    actor_id: str,
    attachment_id: str,
    project_digest: str,
    reason: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "format": STRUCTURED_PURPOSE_FORMAT if contract is not None else PURPOSE_FORMAT,
        "worker_thread_id": worker_thread_id,
        "version": version,
        "purpose": purpose,
        "created_by": actor_id,
        "supervisor_attachment_id": attachment_id,
        "project_digest": project_digest,
        "reason": reason,
        **({"contract": dict(contract)} if contract is not None else {}),
    }


def purpose_row_view(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    try:
        stored = json.loads(str(row["packet_json"]))
        contract = (
            WorkerPurposeContract.model_validate(stored["contract"]).model_dump(mode="json")
            if stored.get("format") == STRUCTURED_PURPOSE_FORMAT else None
        )
    except (KeyError, AttributeError, TypeError, ValueError):
        raise ConflictError("Worker business purpose integrity is invalid",
                            reason_code="worker_purpose_integrity_invalid") from None
    packet = purpose_packet(
        worker_thread_id=str(row["worker_thread_id"]),
        version=int(row["version"]),
        purpose=str(row["purpose"]),
        actor_id=str(row["created_by"]),
        attachment_id=str(row["supervisor_attachment_id"]),
        project_digest=str(row["project_digest"]),
        reason=str(row["reason"]),
        contract=contract,
    )
    try:
        valid = stored == packet and canonical_sha256(packet) == str(
            row["packet_digest"]
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ConflictError(
            "Worker business purpose integrity is invalid",
            reason_code="worker_purpose_integrity_invalid",
        )
    return {
        "worker_thread_id": str(row["worker_thread_id"]),
        "version": int(row["version"]),
        "text": str(row["purpose"]),
        "digest": str(row["packet_digest"]),
        **({"contract": contract} if contract is not None else {}),
    }


def purpose_display_text(purpose: Mapping[str, Any] | None) -> str | None:
    if purpose is None:
        return None
    text = str(purpose["text"])
    contract = purpose.get("contract")
    if not isinstance(contract, Mapping):
        return text
    lines = [text, "", f"到達状態: {contract['outcome']}", f"対象範囲: {contract['scope']}", "合格条件と証拠:"]
    for criterion in contract["criteria"]:
        lines.extend([f"[{criterion['id']}] {criterion['condition']}", f"証拠: {criterion['evidence']}"])
    lines.extend([f"終了条件: {contract['completion']}", f"権限: {contract['authority']}",
                  f"未達時の対応: {contract['unmet_action']}"])
    return "\n".join(lines)


def task_review_criteria(goal: Mapping[str, Any] | sqlite3.Row) -> list[dict[str, str]]:
    """Use only the immutable task conditions, never a later purpose revision."""
    goal = dict(goal)
    packet = goal.get("packet")
    if packet is None and "packet_json" in goal:
        packet = json.loads(str(goal["packet_json"]))
    if packet is not None:
        if not isinstance(packet, Mapping) or goal_packet_digest(packet) != goal["packet_digest"]:
            raise ConflictError("Frozen task criteria integrity is invalid", reason_code="review_task_packet_invalid")
        goal = dict(packet)
    acceptance = goal.get("acceptance", [])
    if acceptance:
        return [{"id": f"task_{index}", "condition": str(condition)}
                for index, condition in enumerate(acceptance, 1)]
    return [{"id": "task_instruction", "condition": str(goal["objective"])}]


def purpose_alignment(current: Mapping[str, Any] | None,
                      assigned: Mapping[str, Any] | None) -> dict[str, Any]:
    changed = current != assigned
    return {
        "status": "changed" if changed else "current" if current else "unset",
        "current_version": current["version"] if current else None,
        "assignment_version": assigned["version"] if assigned else None,
        "contract_defined": bool(current and current.get("contract")),
        "next_action": ("retain_frozen_task_or_revise_at_safe_boundary" if changed else
                        "judge_frozen_task"),
    }


def worker_purpose_tx(
    connection: sqlite3.Connection, worker_thread_id: str
) -> dict[str, Any] | None:
    if not worker_thread_id:
        return None
    row = connection.execute(
        "SELECT * FROM worker_purpose_revisions WHERE worker_thread_id = ? "
        "ORDER BY version DESC LIMIT 1",
        (worker_thread_id,),
    ).fetchone()
    return purpose_row_view(row) if row is not None else None


def readable_worker_purpose_tx(
    connection: sqlite3.Connection, worker_thread_id: str
) -> tuple[dict[str, Any] | None, str]:
    """Keep unrelated listing/lifecycle available when purpose evidence is corrupt."""
    try:
        purpose = worker_purpose_tx(connection, worker_thread_id)
        return purpose, "defined" if purpose is not None else "unset"
    except (ConflictError, TypeError, ValueError):
        return None, "invalid"


def work_purpose_tx(
    connection: sqlite3.Connection, work_item_id: str, goal_version: int
) -> dict[str, Any] | None:
    row = connection.execute(
        """SELECT purpose.* FROM work_purpose_bindings AS binding
           JOIN worker_purpose_revisions AS purpose
             ON purpose.worker_thread_id = binding.worker_thread_id
            AND purpose.version = binding.purpose_version
           WHERE binding.work_item_id = ? AND binding.goal_version = ?""",
        (work_item_id, goal_version),
    ).fetchone()
    return purpose_row_view(row) if row is not None else None


def bind_work_purpose_tx(
    connection: sqlite3.Connection,
    work_item_id: str,
    goal_version: int,
    purpose: Mapping[str, Any] | None,
) -> None:
    if purpose is not None:
        connection.execute(
            "INSERT INTO work_purpose_bindings "
            "(work_item_id, goal_version, worker_thread_id, purpose_version) VALUES (?, ?, ?, ?)",
            (work_item_id, goal_version, purpose["worker_thread_id"], purpose["version"]),
        )


def confirm_worker_purpose_tx(
    connection: sqlite3.Connection,
    worker_thread_id: str,
    expected_version: int | None,
    *,
    required: bool = False,
) -> dict[str, Any] | None:
    if expected_version is not None and (
        isinstance(expected_version, bool)
        or not isinstance(expected_version, int)
        or expected_version < 0
    ):
        raise ValidationError("purpose confirmation must be a nonnegative integer")
    purpose = worker_purpose_tx(connection, worker_thread_id)
    if purpose is None:
        if required:
            raise ConflictError(
                "Define this Worker's business purpose from the requester direction before "
                "assigning a task; do not use the task or its constraints as purpose",
                reason_code="worker_purpose_unset",
            )
        if expected_version not in (None, 0):
            raise ConflictError(
                "Worker business purpose confirmation is stale",
                reason_code="worker_purpose_version_conflict",
            )
        # Historical sealed work has no retroactively invented purpose.
        return None
    if required and "contract" not in purpose:
        raise ConflictError(
            "Define the fixed purpose contract before assigning new managed work; preserve historical tasks",
            reason_code="worker_purpose_contract_required",
        )
    if expected_version != purpose["version"]:
        raise ConflictError(
            "Read the Worker's current business purpose before deciding and confirm its version",
            reason_code=(
                "worker_purpose_confirmation_required"
                if expected_version is None
                else "worker_purpose_version_conflict"
            ),
            current_purpose_version=purpose["version"],
        )
    return purpose


def confirm_work_purpose_tx(
    connection: sqlite3.Connection,
    work: sqlite3.Row | Mapping[str, Any],
    expected_version: int | None,
) -> dict[str, Any] | None:
    return confirm_worker_purpose_tx(
        connection, str(work["managed_worker_thread_id"] or ""), expected_version
    )
