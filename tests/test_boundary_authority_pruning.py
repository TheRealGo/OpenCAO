from __future__ import annotations

from typing import Any

import pytest

from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    CompletionContract,
    ReportInput,
    ReviewInput,
    ReviewVerdict,
    WorkAssignment,
)
from cao_control_plane.projection import verify_projection

_OLD_TIMESTAMP = "2000-01-01T00:00:00Z"


def _completion_boundary(
    system: dict[str, Any], *, key: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Prunable review authority {key}",
            objective="Keep the canonical review decision after audit-event retention.",
            acceptance=["Boundary disposition still follows the durable review."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key=f"assign:prunable-review:{key}",
        ),
    )
    attempt = work["current_attempt"]
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The result is ready for independent review.",
            idempotency_key=f"report:prunable-review:{key}",
        ),
    )
    return reported, reported["open_boundaries"][0]


def test_pruned_review_event_cannot_erase_the_completion_boundary_decision(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    reported, boundary = _completion_boundary(system, key="needs-work")
    reviewed = service.review(
        system["cao"],
        ReviewInput(
            attempt_id=reported["current_attempt"]["id"],
            verdict=ReviewVerdict.NEEDS_WORK,
            summary="The completion claim needs one bounded correction.",
            idempotency_key="review:prunable-review:needs-work",
        ),
    )
    review = reviewed["reviews"][-1]
    assert review["boundary_id"] == boundary["id"]
    service.db.execute(
        "UPDATE events SET created_at = ? WHERE event_type = 'work.reviewed' "
        "AND aggregate_id = ? AND json_extract(data_json, '$.review_id') = ?",
        (_OLD_TIMESTAMP, reported["id"], review["id"]),
    )

    pruned = service.db.prune(event_days=1, message_days=36500)

    assert pruned["events"] >= 1
    assert (
        service.db.fetchone(
            "SELECT sequence FROM events WHERE event_type = 'work.reviewed' "
            "AND aggregate_id = ? AND json_extract(data_json, '$.review_id') = ?",
            (reported["id"], review["id"]),
        )
        is None
    )
    with pytest.raises(ConflictError, match="already has a CAO review"):
        service.review(
            system["cao"],
            ReviewInput(
                attempt_id=reported["current_attempt"]["id"],
                verdict=ReviewVerdict.OK,
                summary="A second review must not replace the canonical decision.",
                idempotency_key="review:prunable-review:duplicate",
            ),
        )

    turn = service.acquire_reasoner_turn(
        system["cao"],
        reported["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key="turn:prunable-review:needs-work",
    )
    with pytest.raises(ConflictError, match="needs-work completion review"):
        service.dispose_boundary(
            system["cao"],
            boundary["id"],
            BoundaryDispositionInput(
                turn_id=turn["id"],
                lease_token=turn["lease_token"],
                expected_generation=reported["generation"],
                kind=BoundaryDispositionKind.ACCEPT,
                reason="An accept cannot contradict the durable needs-work review.",
            ),
        )


def test_prune_retains_both_events_that_authorize_boundary_supersession(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Prunable supersession authority",
            objective="Retain the exact opening and cancellation events.",
            acceptance=["Historical cancellation remains projection-safe after pruning."],
            idempotency_key="assign:prunable-supersession",
        ),
    )
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="This boundary will be superseded by explicit cancellation.",
            idempotency_key="report:prunable-supersession",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    service.cancel_work(
        system["cao"],
        work["id"],
        "The requester canceled this exact historical task.",
        idempotency_key="cancel:prunable-supersession",
    )
    supersession = service.db.fetchone(
        "SELECT boundary_event_sequence, superseding_event_sequence "
        "FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert supersession is not None
    authority_sequences = (
        int(supersession["boundary_event_sequence"]),
        int(supersession["superseding_event_sequence"]),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE events SET created_at = ? WHERE sequence IN (?, ?)",
            (_OLD_TIMESTAMP, *authority_sequences),
        )
        unrelated_sequence = connection.execute(
            "INSERT INTO events(id, event_type, aggregate_type, aggregate_id, "
            "actor_id, data_json, correlation_id, causation_id, created_at) "
            "VALUES(?, 'test.unrelated', 'test', ?, '', '{}', '', '', ?)",
            (
                "evt_unrelated_prunable_supersession",
                work["id"],
                _OLD_TIMESTAMP,
            ),
        ).lastrowid

    pruned = service.db.prune(event_days=1, message_days=36500)

    assert pruned["events"] >= 1
    assert (
        service.db.fetchone("SELECT sequence FROM events WHERE sequence = ?", (unrelated_sequence,))
        is None
    )
    retained = service.db.fetchall(
        "SELECT sequence FROM events WHERE sequence IN (?, ?) ORDER BY sequence",
        authority_sequences,
    )
    assert [int(row["sequence"]) for row in retained] == sorted(authority_sequences)
    assert verify_projection(service.db).healthy is True


def test_v24_upgrade_reseals_a_pruned_open_boundary_before_later_cancel(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Legacy pruned opening event",
            objective="Keep an actionable Boundary safe across a v24 upgrade.",
            acceptance=["A later cancellation binds one reconstructed opening event."],
            idempotency_key="assign:v24-pruned-opening",
        ),
    )
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The legacy opening event will be pruned before upgrade.",
            idempotency_key="report:v24-pruned-opening",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    service.db.execute(
        "DELETE FROM events WHERE event_type = 'boundary.recorded' "
        "AND aggregate_id = ? AND json_extract(data_json, '$.boundary_id') = ?",
        (work["id"], boundary["id"]),
    )
    service.db.execute("UPDATE metadata SET value = '24' WHERE key = 'schema_version'")
    service.db.execute("PRAGMA user_version = 24")

    service.db.initialize()

    reconstructed = service.db.fetchall(
        "SELECT sequence, data_json FROM events "
        "WHERE event_type = 'boundary.recorded' AND aggregate_id = ? "
        "AND json_extract(data_json, '$.boundary_id') = ?",
        (work["id"], boundary["id"]),
    )
    assert len(reconstructed) == 1
    assert '"migration_reconstructed":true' in reconstructed[0]["data_json"]
    service.cancel_work(
        system["cao"],
        work["id"],
        "Cancel after the legacy Boundary authority is re-sealed.",
        idempotency_key="cancel:v24-pruned-opening",
    )
    supersession = service.db.fetchone(
        "SELECT boundary_event_sequence FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert supersession is not None
    assert int(supersession["boundary_event_sequence"]) == int(reconstructed[0]["sequence"])
    assert verify_projection(service.db).healthy is True


def test_v24_upgrade_binds_a_review_after_its_audit_event_was_pruned(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    reported, boundary = _completion_boundary(system, key="v24-review-backfill")
    reviewed = service.review(
        system["cao"],
        ReviewInput(
            attempt_id=reported["current_attempt"]["id"],
            verdict=ReviewVerdict.NEEDS_WORK,
            summary="The exact completion Boundary needs one correction.",
            idempotency_key="review:v24-review-backfill",
        ),
    )
    review = reviewed["reviews"][-1]
    service.db.execute("UPDATE reviews SET boundary_id = NULL WHERE id = ?", (review["id"],))
    service.db.execute(
        "DELETE FROM events WHERE event_type = 'work.reviewed' "
        "AND aggregate_id = ? AND json_extract(data_json, '$.review_id') = ?",
        (reported["id"], review["id"]),
    )
    service.db.execute("UPDATE metadata SET value = '24' WHERE key = 'schema_version'")
    service.db.execute("PRAGMA user_version = 24")

    service.db.initialize()

    rebound = service.db.fetchone("SELECT boundary_id FROM reviews WHERE id = ?", (review["id"],))
    assert rebound is not None and rebound["boundary_id"] == boundary["id"]
    with pytest.raises(ConflictError, match="already has a CAO review"):
        service.review(
            system["cao"],
            ReviewInput(
                attempt_id=reported["current_attempt"]["id"],
                verdict=ReviewVerdict.OK,
                summary="A duplicate review must remain rejected after upgrade.",
                idempotency_key="review:v24-review-backfill:duplicate",
            ),
        )
