from __future__ import annotations

import hashlib

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.errors import ConflictError, StaleGoalError
from cao_control_plane.goal_packets import (
    build_goal_packet,
    build_task_packet,
    canonical_json,
    goal_packet_digest,
    task_packet_digest,
)
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryInput,
    GoalRevision,
    ReportInput,
    RequesterDecisionInput,
    ReviewInput,
    WorkAssignment,
)


def test_unbound_v1_packets_preserve_the_pre_attachment_identity() -> None:
    """Adding conversation binding must not rewrite an old unbound digest."""

    goal = build_goal_packet(
        work_item_id="work-legacy",
        version=1,
        title="Legacy goal",
        objective="Keep the immutable pre-attachment packet",
        maturity="defined",
        acceptance=("Digest is unchanged",),
        non_goals=(),
        priority=50,
        requester_id=None,
        supervisor_id="cao-legacy",
        metadata={},
        reason="",
        created_by="cao-legacy",
        source_intent_id=None,
        source_directive_id=None,
        correlation_id="",
        prior_version=None,
        supervisor_attachment=None,
    )
    assert "supervisor_attachment" not in goal
    assert goal_packet_digest(goal) == hashlib.sha256(
        canonical_json(goal).encode("utf-8")
    ).hexdigest()

    task = build_task_packet(
        goal_packet_digest_value=goal_packet_digest(goal),
        work_item_id="work-legacy",
        goal_version=1,
        attempt_id="attempt-legacy",
        attempt_number=1,
        worker_id="worker-legacy",
        runtime_session_id=None,
        supervisor_attachment=None,
    )
    assert "supervisor_attachment" not in task
    assert len(task_packet_digest(task)) == 64


def _assignment(system, *, key: str) -> WorkAssignment:
    return WorkAssignment(
        worker_id=system["worker"]["id"],
        title="Packet integrity",
        objective="Carry one immutable task packet through independent acceptance",
        acceptance=["Every recorded handoff has the same packet identities"],
        non_goals=["Do not publish externally"],
        idempotency_key=key,
    )


def _packet_fields(row: dict) -> tuple[int, str, str]:
    return (
        int(row["goal_version"]),
        str(row["goal_packet_digest"]),
        str(row["task_packet_digest"]),
    )


def test_requester_decision_keeps_packet_chain_immutable(system):
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="packet-chain",
            project_digest="d" * 64,
        ),
    )
    cao = service.authenticate(attachment["context_token"])
    request = _assignment(system, key="packet-chain").model_copy(
        update={
            "requester_id": system["user"]["id"],
        }
    )
    work = service.assign_work(cao, request)
    attempt = work["current_attempt"]
    expected = _packet_fields(attempt)

    assignment = service.get_inbox(system["worker"])["items"][0]
    assert assignment["kind"] == "assignment"
    assert _packet_fields(assignment) == expected
    assert _packet_fields(assignment["payload"]) == expected
    service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    service.mark_message_handled(
        system["worker"], assignment["id"], evidence="Loaded the sealed assignment packet"
    )

    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="packet-chain:runtime-ready",
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            expected_goal_version=expected[0],
            expected_goal_packet_digest=expected[1],
            expected_task_packet_digest=expected[2],
            expected_generation=work["generation"],
            kind="idle",
            summary="Ready for the next instruction",
            runtime_state="ready",
        ),
    )
    assert _packet_fields(boundary) == expected

    turn = service.acquire_reasoner_turn(
        cao,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="packet-chain:ready-turn",
    )
    assert _packet_fields(turn) == expected
    service.dispose_boundary(
        cao,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=work["generation"],
            kind="continue",
            reason="Proceed with the sealed task packet",
        ),
    )
    reasoner = service.db.fetchone("SELECT * FROM reasoner_turns WHERE id = ?", (turn["id"],))
    assert reasoner is not None
    assert _packet_fields(dict(reasoner)) == expected
    assert reasoner["input_digest"]
    assert reasoner["result_digest"]

    continuation = service.get_inbox(system["worker"])["items"][0]
    assert _packet_fields(continuation) == expected
    service.acknowledge(system["worker"], AckInput(message_ids=[continuation["id"]]))
    service.mark_message_handled(
        system["worker"], continuation["id"], evidence="Completed the requested continuation"
    )
    claimed = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=expected[0],
            expected_goal_packet_digest=expected[1],
            expected_task_packet_digest=expected[2],
            expected_generation=work["generation"],
            summary="Completed with local verification",
            evidence=[{"check": "pytest", "result": "pass"}],
            idempotency_key="packet-chain:completion",
        ),
    )
    claim = claimed["current_attempt"]["completion_claim"]
    assert (claim["goal_packet_digest"], claim["task_packet_digest"]) == expected[1:]
    completion_boundary = claimed["open_boundaries"][0]
    assert _packet_fields(completion_boundary) == expected

    completion_turn = service.acquire_reasoner_turn(
        cao,
        work["id"],
        boundary_id=completion_boundary["id"],
        expected_generation=claimed["generation"],
        idempotency_key="packet-chain:completion-turn",
    )
    service.dispose_boundary(
        cao,
        completion_boundary["id"],
        BoundaryDispositionInput(
            turn_id=completion_turn["id"],
            lease_token=completion_turn["lease_token"],
            expected_generation=claimed["generation"],
            kind="accept",
            reason="Completion is ready for the independent review gate",
        ),
    )
    reviewed = service.review(
        cao,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict="ok",
            summary="Evidence independently verified",
            idempotency_key="packet-chain:review",
        ),
    )
    review = reviewed["reviews"][0]
    assert _packet_fields(review) == expected

    accepted = service.record_requester_decision(
        cao,
        RequesterDecisionInput(
            review_id=review["id"],
            verdict="accepted",
            summary="Accepted by requester",
            conversation_evidence_id="packet-chain:acceptance",
            idempotency_key="packet-chain:acceptance",
        ),
    )
    decision = accepted["requester_decisions"][0]
    assert _packet_fields(decision) == expected


def test_wrong_or_stale_packet_identity_fails_closed_and_revision_preserves_history(system):
    service = system["service"]
    work = service.assign_work(system["cao"], _assignment(system, key="packet-revision"))
    first_attempt = work["current_attempt"]
    first_goal = dict(work["current_goal_revision"])
    first_expected = _packet_fields(first_attempt)

    with pytest.raises(ConflictError, match="task packet"):
        service.record_boundary(
            system["worker"],
            BoundaryInput(
                source_event_id="packet-revision:wrong-digest",
                work_item_id=work["id"],
                attempt_id=first_attempt["id"],
                expected_goal_version=first_expected[0],
                expected_goal_packet_digest="0" * 64,
                expected_task_packet_digest=first_expected[2],
                expected_generation=work["generation"],
                kind="idle",
                summary="This must not become a durable boundary",
                runtime_state="ready",
            ),
        )

    revised = service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=first_expected[0],
            objective="Use the revised immutable packet",
            maturity="defined",
            acceptance=["The replacement packet is independently verifiable"],
            non_goals=["Do not publish externally"],
            reason="The requested outcome changed",
            idempotency_key="packet-revision:goal",
        ),
    )
    assert len(revised["goal_history"]) == 2
    preserved, current = revised["goal_history"]
    assert preserved["packet"] == first_goal["packet"]
    assert preserved["packet_digest"] == first_goal["packet_digest"]
    assert current["packet_digest"] != first_goal["packet_digest"]
    assert revised["current_attempt"]["goal_packet_digest"] == current["packet_digest"]
    assert revised["current_attempt"]["task_packet_digest"] != first_expected[2]

    with pytest.raises(StaleGoalError):
        service.report(
            system["worker"],
            revised["current_attempt"]["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=first_expected[0],
                expected_goal_packet_digest=first_expected[1],
                expected_task_packet_digest=first_expected[2],
                expected_generation=revised["generation"],
                summary="A stale packet must not report against the revision",
                idempotency_key="packet-revision:stale-report",
            ),
        )
