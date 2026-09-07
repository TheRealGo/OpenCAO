"""Pure, fail-closed contract for explicit WorkItem closure.

Completion, CAO review, requester acceptance, and resource cleanup are separate
boundaries.  This module defines the evidence that must exist before a service
may record ``closed``.  It performs no filesystem, process, git, or external
operation; those effects remain owned by the effect ledger.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from enum import StrEnum

from .canonical import canonical_sha256


class CloseContractError(ValueError):
    """Raised when a close request is incomplete or stale."""


class CleanupTargetKind(StrEnum):
    RUNTIME = "runtime"
    SUPERVISION_REGISTRATION = "supervision-registration"
    WORKSPACE = "workspace"
    TEMPORARY = "temporary"
    LOG = "log"
    BRANCH = "branch"


class CleanupAction(StrEnum):
    STOP = "stop"
    DETACH = "detach"
    ARCHIVE = "archive"
    TRASH = "trash"
    DELETE = "delete"


class CleanupOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    NOT_APPLIED = "not-applied"
    UNKNOWN = "unknown"


_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DESTRUCTIVE_ACTIONS = {CleanupAction.TRASH, CleanupAction.DELETE}


@dataclass(frozen=True, slots=True)
class ArtifactPreservation:
    artifact_id: str
    digest: str
    evidence_id: str


@dataclass(frozen=True, slots=True)
class CleanupRecord:
    target_kind: CleanupTargetKind
    target_fingerprint: str
    action: CleanupAction
    outcome: CleanupOutcome
    evidence_id: str
    effect_operation_id: str = ""
    destructive_authority_evidence_id: str = ""


@dataclass(frozen=True, slots=True)
class ClosePlan:
    work_item_id: str
    attempt_id: str
    review_id: str
    requester_decision_id: str
    expected_goal_version: int
    expected_goal_packet_digest: str
    expected_task_packet_digest: str
    expected_generation: int
    retention_policy_evidence_id: str
    artifact_manifest_evidence_id: str
    cleanup_inventory_evidence_id: str
    artifacts: tuple[ArtifactPreservation, ...]
    cleanup: tuple[CleanupRecord, ...]
    close_preparation_id: str = ""

    def digest(self) -> str:
        return canonical_sha256(asdict(self))


@dataclass(frozen=True, slots=True)
class CloseReadiness:
    """Canonical state observed in the same transaction as close."""

    work_state: str
    attempt_state: str
    review_verdict: str
    requester_decision_verdict: str
    goal_version: int
    goal_packet_digest: str
    task_packet_digest: str
    generation: int
    artifact_ids: tuple[str, ...]
    open_delivery_count: int
    active_runtime_count: int
    unresolved_effect_count: int


@dataclass(frozen=True, slots=True)
class CloseGate:
    ready: bool
    plan_digest: str
    errors: tuple[str, ...]

    def require_ready(self) -> None:
        if not self.ready:
            raise CloseContractError("; ".join(self.errors))


def _valid_identifier(value: object) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def evaluate_close(plan: ClosePlan, readiness: CloseReadiness) -> CloseGate:
    """Evaluate closure without performing cleanup or changing durable state."""

    errors: list[str] = []
    if not isinstance(plan, ClosePlan) or not isinstance(readiness, CloseReadiness):
        return CloseGate(False, "", ("close plan and readiness must use canonical types",))

    reference_fields = {
        "work item": plan.work_item_id,
        "attempt": plan.attempt_id,
        "review": plan.review_id,
        "requester decision": plan.requester_decision_id,
        "retention policy evidence": plan.retention_policy_evidence_id,
        "artifact manifest evidence": plan.artifact_manifest_evidence_id,
        "cleanup inventory evidence": plan.cleanup_inventory_evidence_id,
    }
    for label, value in reference_fields.items():
        if not _valid_identifier(value):
            errors.append(f"{label} must be a non-sensitive reference identifier")
    if plan.expected_goal_version < 1 or plan.expected_generation < 1:
        errors.append("goal version and generation must be positive")
    for label, value in (
        ("goal packet", plan.expected_goal_packet_digest),
        ("task packet", plan.expected_task_packet_digest),
    ):
        if not _valid_digest(value):
            errors.append(f"{label} digest must be canonical sha256")

    artifact_ids: set[str] = set()
    for artifact in plan.artifacts:
        if not isinstance(artifact, ArtifactPreservation):
            errors.append("artifact preservation record is invalid")
            continue
        if not _valid_identifier(artifact.artifact_id):
            errors.append("artifact id must be a non-sensitive reference identifier")
        elif artifact.artifact_id in artifact_ids:
            errors.append("artifact preservation records must be unique")
        artifact_ids.add(artifact.artifact_id)
        if not _valid_digest(artifact.digest):
            errors.append("artifact digest must be canonical sha256")
        if not _valid_identifier(artifact.evidence_id):
            errors.append("artifact evidence must be a non-sensitive reference identifier")

    cleanup_targets: set[tuple[CleanupTargetKind, str]] = set()
    for record in plan.cleanup:
        if not isinstance(record, CleanupRecord):
            errors.append("cleanup record is invalid")
            continue
        if not isinstance(record.target_kind, CleanupTargetKind):
            errors.append("cleanup target kind is invalid")
        if not _valid_digest(record.target_fingerprint):
            errors.append("cleanup target must be an opaque canonical fingerprint")
        target = (record.target_kind, record.target_fingerprint)
        if target in cleanup_targets:
            errors.append("cleanup targets must be unique")
        cleanup_targets.add(target)
        if not isinstance(record.action, CleanupAction):
            errors.append("cleanup action is invalid")
        if not isinstance(record.outcome, CleanupOutcome):
            errors.append("cleanup outcome is invalid")
        elif record.outcome is CleanupOutcome.UNKNOWN:
            errors.append("cleanup with an unknown outcome blocks close")
        if not _valid_identifier(record.evidence_id):
            errors.append("cleanup evidence must be a non-sensitive reference identifier")
        if record.action in _DESTRUCTIVE_ACTIONS:
            if not _valid_identifier(record.effect_operation_id):
                errors.append("destructive cleanup requires a resolved effect operation")
            if not _valid_identifier(record.destructive_authority_evidence_id):
                errors.append("destructive cleanup requires authority evidence")
        elif record.destructive_authority_evidence_id:
            errors.append("non-destructive cleanup cannot carry destructive authority evidence")

    if readiness.work_state != "completed":
        errors.append("work must be completed before close")
    if readiness.attempt_state != "completed":
        errors.append("attempt must be completed before close")
    if readiness.review_verdict != "ok":
        errors.append("CAO review must be ok before close")
    if readiness.requester_decision_verdict != "accepted":
        errors.append("requester decision must be accepted before close")
    if readiness.goal_version != plan.expected_goal_version:
        errors.append("close plan targets a stale goal version")
    if readiness.goal_packet_digest != plan.expected_goal_packet_digest:
        errors.append("close plan targets a stale goal packet")
    if readiness.task_packet_digest != plan.expected_task_packet_digest:
        errors.append("close plan targets a stale task packet")
    if readiness.generation != plan.expected_generation:
        errors.append("close plan targets a stale work generation")
    if set(readiness.artifact_ids) != artifact_ids:
        errors.append(
            "artifact manifest does not cover the scope-selected canonical artifact set"
        )
    if readiness.open_delivery_count:
        errors.append("open deliveries block close")
    if readiness.active_runtime_count:
        errors.append("active runtimes block close")
    if readiness.unresolved_effect_count:
        errors.append("unresolved effects block close")

    unique_errors = tuple(dict.fromkeys(errors))
    return CloseGate(not unique_errors, plan.digest(), unique_errors)


def require_close_ready(plan: ClosePlan, readiness: CloseReadiness) -> str:
    """Return the sealed plan digest, or raise before recording ``closed``."""

    gate = evaluate_close(plan, readiness)
    gate.require_ready()
    return gate.plan_digest
