"""Report-first supervision using existing claims, manifests and read receipts."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from .artifact_preservation_edge import ArtifactPreservationProviderError
from .errors import ConflictError
from .models import ReportKind, ReviewInput, WorkerDecisionReport

if TYPE_CHECKING:
    from .service import ControlPlane

REPORT_GUIDANCE = (
    "Report the outcome, each frozen acceptance criterion and its evidence, verification "
    "performed and its results, limitations or unfinished work, your recommendation and "
    "the decision needed from the requester. Retain detailed execution records as registered artifacts "
    "for questions or targeted audit; do not copy raw logs into the report. Use decision_report "
    "on cao_report for structured reporting when useful; a normal final answer can carry the "
    "same information without a second reporting call. Respect exact-output and no-artifact "
    "tasks. Keep working on ordinary methods and recoverable task-local failures within the "
    "sealed authority; an intermediate finding or progress report does not require permission "
    "to continue. Escalate at completion, a genuine blocker, a material direction decision, "
    "or a need for additional authority, budget or private input. "
    "When frozen criteria require execution records, arrange capture before the run: "
    "preserve stdout, stderr (including an empty stream), exit status and required checks "
    "where the actual program executes. cao-capture is an optional task-owned local "
    "command for this purpose. A remote client's return code or CompletedProcess display "
    "is not the remote program's output. Register the original file bytes and their "
    "digests; a reformatted JSON receipt or stripped stdout is a different artifact. "
    "Preserve an irrecoverable old gap honestly; "
    "do not repeat a completed or unknown operation merely to recreate its evidence."
)
REVIEW_GUIDANCE = (
    "Use the current Worker report and frozen criteria as the normal decision input. "
    "Check the reported result, verification, limitations and recommendation; ask a focused "
    "question when decision-critical information is missing. Retained raw logs and data "
    "need not be reread or independently recalculated for every review. Audit only to resolve "
    "a concrete contradiction, evidence gap or material impact, or an explicit task requirement; "
    "record review_basis=artifact_audit, the reason and exact byte scopes actually read. "
    "There is no periodic or fixed-rate sampling audit. Default review_basis=report records "
    "report-based judgment, not independent verification of all underlying facts. Mechanical "
    "packet, manifest and read checks do not prove business correctness. Preserve explicit "
    "acceptance requirements, requester decisions, authority and unknown-effect fences. "
    "Use provider_response for facts about the exact settled input, separate from old "
    "Attempt-wide progress. ACK means receipt; a promise describes intent after the model "
    "turn has ended. Do not infer ongoing execution from that promise; separately evidenced "
    "running jobs remain real work and must be checked before acting. These facts do "
    "not themselves judge business progress or require artifacts on no-artifact tasks. "
    "If an answer contains only intent or unchanged missing evidence, choose a concrete "
    "diagnosis and an evidence-based change of approach instead of repeating the same "
    "corrective method. Identify the failed operation, capability or information gap; "
    "preserve legitimate incremental results. Irrecoverable evidence cannot be repaired "
    "by another request for that same lost record. Keep the old result and limitation "
    "distinct from any newly authorized work; methods and allocation remain CAO judgment."
)


def validate_report_tx(
    connection: sqlite3.Connection,
    *,
    report: WorkerDecisionReport,
    kind: ReportKind,
    goal: Mapping[str, Any] | sqlite3.Row,
    attempt_id: str,
) -> dict[str, Any]:
    """Validate bindings, never manufacture an independent business assessment."""
    from .worker_purpose import task_review_criteria

    expected = {item["id"] for item in task_review_criteria(goal)}
    actual = [item.criterion_id for item in report.criterion_assessments]
    if set(actual) != expected or len(actual) != len(set(actual)):
        raise ConflictError(
            "Report each frozen task criterion exactly once",
            reason_code="decision_report_criteria_mismatch",
            expected_criterion_ids=sorted(expected),
        )
    if kind == ReportKind.COMPLETION_CLAIM and any(
        item.verdict != "met" for item in report.criterion_assessments
    ):
        raise ConflictError(
            "A completion report cannot leave a task criterion unmet or unverified",
            reason_code="decision_report_incomplete",
        )
    for artifact_id in report.record_artifact_ids:
        row = connection.execute(
            "SELECT digest, uri FROM artifacts WHERE id = ? AND attempt_id = ?",
            (artifact_id, attempt_id),
        ).fetchone()
        if (row is None or re.fullmatch(r"[0-9a-f]{64}", str(row["digest"])) is None
                or str(row["uri"]) != f"owner-private-artifact:{row['digest']}"):
            raise ConflictError(
                "Report records must be verified artifacts of this exact Attempt",
                reason_code="decision_report_record_binding_mismatch",
            )
    return {
        "criteria_coverage": "matched",
        "record_bindings": "matched",
        "record_count": len(report.record_artifact_ids),
        "verification_provenance": "worker_reported",
        "business_correctness": "not_independently_verified",
    }


def review_basis_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    *,
    actor: Mapping[str, Any],
    work: Mapping[str, Any] | sqlite3.Row,
    attempt: Mapping[str, Any] | sqlite3.Row,
    request: ReviewInput,
) -> dict[str, Any]:
    """Record the real review basis; scope audits to authenticated read coverage."""
    audited: list[dict[str, Any]] = []
    for scope in request.audit_scopes:
        if not actor.get("_cao_attachment_id"):
            raise ConflictError("An artifact audit requires the current CAO attachment")
        artifact = connection.execute(
            "SELECT * FROM artifacts WHERE id = ? AND attempt_id = ? AND work_item_id = ?",
            (scope.artifact_id, attempt["id"], work["id"]),
        ).fetchone()
        if artifact is None:
            raise ConflictError("Audit scope is not an artifact of this exact Attempt")
        rows = connection.execute(
            """SELECT sequence, data_json FROM events
               WHERE event_type = 'artifact.content_read' AND aggregate_id = ? AND actor_id = ?
                 AND json_extract(data_json, '$.work_item_id') = ?
                 AND json_extract(data_json, '$.attempt_id') = ?
                 AND json_extract(data_json, '$.digest') = ?
                 AND json_extract(data_json, '$.supervisor_attachment_id') = ?
                 AND json_extract(data_json, '$.supervisor_attachment_generation') = ?
               ORDER BY sequence""",
            (scope.artifact_id, actor["id"], work["id"], attempt["id"], artifact["digest"],
             actor.get("_cao_attachment_id"), actor.get("_cao_attachment_generation")),
        ).fetchall()
        reads = [(int(row["sequence"]), json.loads(row["data_json"])) for row in rows]
        if not _covers(reads, scope.byte_offset, scope.byte_offset + scope.byte_count):
            raise ConflictError(
                "Audit scope exceeds the exact content read on this attachment generation",
                reason_code="review_audit_scope_unread",
            )
        # Existing reader verifies whole-file integrity without returning its text to CAO.
        try:
            service.owner_private_artifact_preservation.read_verified_text_chunk(
                digest=str(artifact["digest"]), byte_offset=scope.byte_offset, max_bytes=4
            )
        except ArtifactPreservationProviderError as error:
            raise ConflictError(
                "Audited artifact is no longer verifiable", reason_code=error.code
            ) from None
        audited.append({
            **scope.model_dump(mode="json"),
            "digest": str(artifact["digest"]),
            "read_event_sequences": [sequence for sequence, data in reads
                                     if int(data["byte_offset"]) < scope.byte_offset + scope.byte_count
                                     and int(data["byte_offset"]) + int(data["byte_count"]) > scope.byte_offset],
        })
    try:
        claim = json.loads(str(attempt["completion_claim_json"]))
    except (TypeError, ValueError):
        raise ConflictError("Completion report metadata is invalid", reason_code="review_report_invalid") from None
    if not isinstance(claim, Mapping):
        raise ConflictError("Completion report metadata is invalid", reason_code="review_report_invalid")
    return {
        "kind": "review_basis",
        "basis": request.review_basis,
        "report_format": "structured" if claim.get("decision_report") else "free_text",
        "report_checks": claim.get("report_checks", {}),
        "mechanical_checks": {
            "task_binding": "matched",
            "criteria_consistency": "matched" if request.criterion_assessments else "not_supplied",
        },
        "business_correctness": "cao_judgment",
        "audit_reason": request.audit_reason,
        "audit_scopes": audited,
    }


def _covers(reads: Sequence[tuple[int, dict[str, Any]]], start: int, end: int) -> bool:
    covered = start
    for left, right in sorted(
        (int(data["byte_offset"]), int(data["byte_offset"]) + int(data["byte_count"]))
        for _, data in reads
    ):
        if right <= covered:
            continue
        if left > covered:
            break
        covered = max(covered, right)
    return covered >= end
