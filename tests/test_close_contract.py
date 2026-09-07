from __future__ import annotations

from dataclasses import replace

import pytest

from cao_control_plane.close_contract import (
    ArtifactPreservation,
    CleanupAction,
    CleanupOutcome,
    CleanupRecord,
    CleanupTargetKind,
    CloseContractError,
    ClosePlan,
    CloseReadiness,
    evaluate_close,
    require_close_ready,
)

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_DIGEST_C = "c" * 64


def _plan() -> ClosePlan:
    return ClosePlan(
        work_item_id="wrk-example",
        attempt_id="att-example",
        review_id="rev-example",
        requester_decision_id="rdec-example",
        expected_goal_version=2,
        expected_goal_packet_digest=_DIGEST_A,
        expected_task_packet_digest=_DIGEST_B,
        expected_generation=4,
        retention_policy_evidence_id="retention-policy-v1",
        artifact_manifest_evidence_id="artifact-manifest-example",
        cleanup_inventory_evidence_id="cleanup-inventory-example",
        artifacts=(ArtifactPreservation("art-example", _DIGEST_C, "artifact-copy-verified"),),
        cleanup=(
            CleanupRecord(
                CleanupTargetKind.RUNTIME,
                _DIGEST_A,
                CleanupAction.STOP,
                CleanupOutcome.SUCCEEDED,
                "runtime-stop-verified",
            ),
            CleanupRecord(
                CleanupTargetKind.WORKSPACE,
                _DIGEST_B,
                CleanupAction.TRASH,
                CleanupOutcome.SUCCEEDED,
                "workspace-trash-verified",
                effect_operation_id="eff-cleanup-example",
                destructive_authority_evidence_id="authority-cleanup-example",
            ),
        ),
    )


def _readiness() -> CloseReadiness:
    return CloseReadiness(
        work_state="completed",
        attempt_state="completed",
        review_verdict="ok",
        requester_decision_verdict="accepted",
        goal_version=2,
        goal_packet_digest=_DIGEST_A,
        task_packet_digest=_DIGEST_B,
        generation=4,
        artifact_ids=("art-example",),
        open_delivery_count=0,
        active_runtime_count=0,
        unresolved_effect_count=0,
    )


def test_exact_accepted_cleanup_plan_is_ready_and_digest_stable() -> None:
    plan = _plan()
    gate = evaluate_close(plan, _readiness())

    assert gate.ready is True
    assert gate.errors == ()
    assert gate.plan_digest == plan.digest()
    assert require_close_ready(plan, _readiness()) == plan.digest()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("work_state", "waiting_user", "work must be completed"),
        ("attempt_state", "submitted", "attempt must be completed"),
        ("review_verdict", "pending", "CAO review must be ok"),
        ("requester_decision_verdict", "", "requester decision must be accepted"),
    ],
)
def test_completion_review_and_acceptance_are_separate_gates(
    field: str, value: str, message: str
) -> None:
    readiness = replace(_readiness(), **{field: value})

    gate = evaluate_close(_plan(), readiness)

    assert gate.ready is False
    assert any(message in error for error in gate.errors)


def test_stale_packet_generation_and_inexact_artifact_manifest_fail_closed() -> None:
    readiness = replace(
        _readiness(),
        goal_packet_digest=_DIGEST_C,
        generation=5,
        artifact_ids=("art-example", "art-unplanned"),
    )

    gate = evaluate_close(_plan(), readiness)

    assert gate.ready is False
    assert "close plan targets a stale goal packet" in gate.errors
    assert "close plan targets a stale work generation" in gate.errors
    assert (
        "artifact manifest does not cover the scope-selected canonical artifact set"
        in gate.errors
    )


def test_unknown_cleanup_or_unresolved_runtime_delivery_effect_blocks_close() -> None:
    plan = replace(
        _plan(),
        cleanup=(
            replace(_plan().cleanup[0], outcome=CleanupOutcome.UNKNOWN),
            _plan().cleanup[1],
        ),
    )
    readiness = replace(
        _readiness(), open_delivery_count=1, active_runtime_count=1, unresolved_effect_count=1
    )

    gate = evaluate_close(plan, readiness)

    assert gate.ready is False
    assert "cleanup with an unknown outcome blocks close" in gate.errors
    assert "open deliveries block close" in gate.errors
    assert "active runtimes block close" in gate.errors
    assert "unresolved effects block close" in gate.errors


def test_destructive_cleanup_requires_effect_and_authority_evidence() -> None:
    destructive = replace(
        _plan().cleanup[1],
        effect_operation_id="",
        destructive_authority_evidence_id="",
    )
    plan = replace(_plan(), cleanup=(_plan().cleanup[0], destructive))

    gate = evaluate_close(plan, _readiness())

    assert gate.ready is False
    assert "destructive cleanup requires a resolved effect operation" in gate.errors
    assert "destructive cleanup requires authority evidence" in gate.errors


def test_raw_locator_cannot_be_used_as_cleanup_identity() -> None:
    unsafe = replace(_plan().cleanup[0], target_fingerprint="/private/worktree")
    plan = replace(_plan(), cleanup=(unsafe, _plan().cleanup[1]))

    gate = evaluate_close(plan, _readiness())

    assert gate.ready is False
    assert "cleanup target must be an opaque canonical fingerprint" in gate.errors


def test_duplicate_artifacts_and_targets_fail_closed() -> None:
    artifact = _plan().artifacts[0]
    target = _plan().cleanup[0]
    plan = replace(
        _plan(), artifacts=(artifact, artifact), cleanup=(target, target, _plan().cleanup[1])
    )

    gate = evaluate_close(plan, _readiness())

    assert gate.ready is False
    assert "artifact preservation records must be unique" in gate.errors
    assert "cleanup targets must be unique" in gate.errors


def test_require_close_ready_raises_with_sanitized_reason() -> None:
    with pytest.raises(CloseContractError, match="open deliveries block close"):
        require_close_ready(_plan(), replace(_readiness(), open_delivery_count=1))
