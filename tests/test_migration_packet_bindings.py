from __future__ import annotations

from typing import Any

import pytest

from cao_control_plane.database import Database
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    BoundaryInput,
    BoundaryKind,
    ReportInput,
    ReportKind,
    ReviewInput,
    ReviewVerdict,
    RuntimeState,
    WorkAssignment,
)


def _packet_fields(row: dict[str, Any]) -> tuple[int, str, str]:
    return (
        int(row["goal_version"]),
        str(row["goal_packet_digest"]),
        str(row["task_packet_digest"]),
    )


def _complete_packet_chain(system: dict[str, Any]) -> dict[str, str]:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Migration binding verification",
            objective="Reject any packet identity that cannot be reconstructed",
            acceptance=["Every dependent binding is verified before startup"],
            idempotency_key="migration-packet-bindings",
        ),
    )
    attempt = work["current_attempt"]
    expected = _packet_fields(attempt)
    assignment = service.get_inbox(system["worker"])["items"][0]
    service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    service.mark_message_handled(
        system["worker"], assignment["id"], evidence="Loaded the sealed assignment packet"
    )
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="migration-packet-bindings:ready",
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            expected_goal_version=expected[0],
            expected_goal_packet_digest=expected[1],
            expected_task_packet_digest=expected[2],
            expected_generation=work["generation"],
            kind=BoundaryKind.IDLE,
            summary="Ready for migration verification",
            runtime_state=RuntimeState.READY,
        ),
    )
    ready_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="migration-packet-bindings:ready-turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=ready_turn["id"],
            lease_token=ready_turn["lease_token"],
            expected_generation=work["generation"],
            kind=BoundaryDispositionKind.CONTINUE,
            reason="Continue to the completion gate",
        ),
    )
    continuation = service.get_inbox(system["worker"])["items"][0]
    service.acknowledge(system["worker"], AckInput(message_ids=[continuation["id"]]))
    service.mark_message_handled(
        system["worker"], continuation["id"], evidence="Completed the requested continuation"
    )
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=expected[0],
            expected_goal_packet_digest=expected[1],
            expected_task_packet_digest=expected[2],
            expected_generation=work["generation"],
            summary="Completed with migration evidence",
            evidence=[{"check": "packet-chain", "result": "pass"}],
            idempotency_key="migration-packet-bindings:completion",
        ),
    )
    completion_boundary = reported["open_boundaries"][0]
    completion_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=completion_boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key="migration-packet-bindings:completion-turn",
    )
    service.dispose_boundary(
        system["cao"],
        completion_boundary["id"],
        BoundaryDispositionInput(
            turn_id=completion_turn["id"],
            lease_token=completion_turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.ACCEPT,
            reason="Ready for independent review",
        ),
    )
    review = service.review(
        system["cao"],
        ReviewInput(
            attempt_id=attempt["id"],
            verdict=ReviewVerdict.OK,
            summary="Verified",
            idempotency_key="migration-packet-bindings:review",
        ),
    )["reviews"][0]
    directive = service.db.fetchone(
        "SELECT id FROM directives WHERE created_work_item_id = ?", (work["id"],)
    )
    message = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? ORDER BY sequence LIMIT 1",
        (attempt["id"],),
    )
    assert directive is not None and message is not None
    return {
        "goal": work["id"],
        "attempt": attempt["id"],
        "directive": str(directive["id"]),
        "message": str(message["id"]),
        "boundary": boundary["id"],
        "reasoner": ready_turn["id"],
        "review": review["id"],
    }


@pytest.mark.parametrize(
    ("table", "column", "subject"),
    [
        ("attempts", "task_packet_digest", "attempt"),
        ("directives", "expected_goal_packet_digest", "directive"),
        ("messages", "goal_packet_digest", "message"),
        ("boundaries", "task_packet_digest", "boundary"),
        ("reasoner_turns", "goal_packet_digest", "reasoner"),
        ("reviews", "task_packet_digest", "review"),
    ],
)
def test_migration_rejects_each_prepopulated_packet_binding_that_disagrees(
    system: dict[str, Any], table: str, column: str, subject: str
) -> None:
    identifiers = _complete_packet_chain(system)
    system["service"].db.execute(
        f"UPDATE {table} SET {column} = ? WHERE id = ?",
        ("0" * 64, identifiers[subject]),
    )

    with pytest.raises(RuntimeError, match=r"packet|input digest"):
        Database(system["settings"])


@pytest.mark.parametrize(
    ("table", "column", "subject"),
    [
        ("goal_revisions", "packet_digest", "goal"),
        ("attempts", "task_packet_digest", "attempt"),
        ("messages", "task_packet_digest", "message"),
        ("boundaries", "task_packet_digest", "boundary"),
        ("reasoner_turns", "task_packet_digest", "reasoner"),
        ("reviews", "task_packet_digest", "review"),
    ],
)
def test_migration_rejects_partial_prepopulated_packet_bindings(
    system: dict[str, Any], table: str, column: str, subject: str
) -> None:
    identifiers = _complete_packet_chain(system)
    where = "work_item_id = ?" if table == "goal_revisions" else "id = ?"
    system["service"].db.execute(
        f"UPDATE {table} SET {column} = '' WHERE {where}",
        (identifiers[subject],),
    )

    with pytest.raises(RuntimeError, match=r"packet|input digest"):
        Database(system["settings"])


def test_migration_backfills_only_an_entirely_empty_attempt_binding(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Empty attempt binding",
            objective="Backfill only the default packet fields",
            acceptance=["The canonical values are restored"],
            idempotency_key="migration-empty-attempt-binding",
        ),
    )
    attempt = work["current_attempt"]
    expected = _packet_fields(attempt)
    service.db.execute(
        "UPDATE attempts SET goal_packet_digest = '', task_packet_digest = '' WHERE id = ?",
        (attempt["id"],),
    )

    migrated = Database(system["settings"])
    restored = migrated.fetchone("SELECT * FROM attempts WHERE id = ?", (attempt["id"],))
    assert restored is not None
    assert _packet_fields(dict(restored)) == expected
