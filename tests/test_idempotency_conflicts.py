from __future__ import annotations

import hashlib
import json

import pytest

from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    ArtifactInput,
    BoundaryDispositionInput,
    ReportInput,
    ReviewInput,
    WorkAssignment,
)


def _assignment(system, *, idempotency_key: str = ""):
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Idempotency contract",
            objective="Keep request identity exact",
            acceptance=["Requests are replay-safe"],
            idempotency_key=idempotency_key,
        ),
    )


def _completion_ready_for_review(system):
    service = system["service"]
    work = _assignment(system)
    attempt = work["current_attempt"]
    submitted = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The requested implementation and checks are complete",
            evidence=[{"check": "pytest", "result": "pass"}],
            idempotency_key="completion-claim",
        ),
    )
    boundary = submitted["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=submitted["generation"],
        idempotency_key="completion-disposition",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=submitted["generation"],
            kind="accept",
            reason="The completion claim is ready for independent review",
        ),
    )
    return work


def _cao_reviewed_work(system):
    service = system["service"]
    work = _completion_ready_for_review(system)
    request = ReviewInput(
        attempt_id=work["current_attempt"]["id"],
        verdict="ok",
        summary="Evidence independently verified",
        idempotency_key="cao-review",
    )
    reviewed = service.review(system["cao"], request)
    assert service.review(system["cao"], request) == reviewed
    return reviewed


def test_assignment_key_replay_requires_identical_canonical_payload(system):
    service = system["service"]
    first = _assignment(system, idempotency_key="assignment-command")
    assert _assignment(system, idempotency_key="assignment-command") == first

    changed = WorkAssignment(
        worker_id=system["worker"]["id"],
        title="Different assignment",
        objective="Keep request identity exact",
        acceptance=["Requests are replay-safe"],
        idempotency_key="assignment-command",
    )
    with pytest.raises(ConflictError, match=r"idempotency key.*different"):
        service.assign_work(system["cao"], changed)


def test_report_key_replay_rejects_changed_payload_without_reapplying(system):
    service = system["service"]
    work = _assignment(system)
    attempt = work["current_attempt"]
    request = ReportInput(
        kind="progress",
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        summary="The first implementation step is complete",
        idempotency_key="worker-progress",
    )
    first = service.report(system["worker"], attempt["id"], request)
    assert service.report(system["worker"], attempt["id"], request) == first

    with pytest.raises(ConflictError, match=r"idempotency key.*different"):
        service.report(
            system["worker"],
            attempt["id"],
            request.model_copy(update={"summary": "A different progress report"}),
        )

    messages = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM messages WHERE kind = 'progress'"
    )
    assert messages["count"] == 1


def test_report_replays_pre_instruction_evidence_request_digest(system):
    service = system["service"]
    work = _assignment(system)
    attempt = work["current_attempt"]
    request = ReportInput(
        kind="progress",
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        summary="Replay the report created before explicit instruction evidence.",
        incorporated_message_ids=[],
        idempotency_key="legacy-worker-progress",
    )
    first = service.report(system["worker"], attempt["id"], request)
    legacy_request = request.model_dump(mode="json")
    legacy_request.pop("incorporated_message_ids")
    encoded = json.dumps(
        {"attempt_id": attempt["id"], "request": legacy_request},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert service.db.execute(
        "UPDATE idempotency_results SET request_digest = ? "
        "WHERE actor_id = ? AND operation = 'report' AND idempotency_key = ?",
        (
            hashlib.sha256(encoded).hexdigest(),
            system["worker"]["id"],
            request.idempotency_key,
        ),
    ) == 1

    assert service.report(system["worker"], attempt["id"], request) == first


def test_artifact_report_replays_a_pre_opaque_locator_request_digest(system):
    service = system["service"]
    work = _assignment(system)
    attempt = work["current_attempt"]
    raw_uri = "data:text/plain,legacy-idempotent-artifact"
    request = ReportInput(
        kind="artifact",
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        summary="Replay an artifact report sealed before opaque request bindings.",
        artifacts=[
            ArtifactInput(
                name="legacy.txt",
                uri=raw_uri,
                media_type="text/plain",
                digest=hashlib.sha256(b"legacy-idempotent-artifact").hexdigest(),
            )
        ],
        idempotency_key="legacy-artifact-report",
    )
    first = service.report(system["worker"], attempt["id"], request)
    legacy_request = request.model_dump(mode="json")
    legacy_request.pop("incorporated_message_ids")
    encoded = json.dumps(
        {"attempt_id": attempt["id"], "request": legacy_request},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert service.db.execute(
        "UPDATE idempotency_results SET request_digest = ? "
        "WHERE actor_id = ? AND operation = 'report' AND idempotency_key = ?",
        (
            hashlib.sha256(encoded).hexdigest(),
            system["worker"]["id"],
            request.idempotency_key,
        ),
    ) == 1

    assert service.report(system["worker"], attempt["id"], request) == first
    assert service.db.fetchone(
        "SELECT COUNT(*) AS count FROM artifacts WHERE attempt_id = ?",
        (attempt["id"],),
    )["count"] == 1


def test_cao_review_key_replay_rejects_changed_payload_without_second_review(system):
    service = system["service"]
    reviewed = _cao_reviewed_work(system)
    attempt_id = reviewed["current_attempt"]["id"]

    with pytest.raises(ConflictError, match=r"idempotency key.*different"):
        service.review(
            system["cao"],
            ReviewInput(
                attempt_id=attempt_id,
                verdict="ok",
                summary="Different review conclusion text",
                idempotency_key="cao-review",
            ),
        )

    reviews = service.db.fetchone("SELECT COUNT(*) AS count FROM reviews")
    assert reviews["count"] == 1
