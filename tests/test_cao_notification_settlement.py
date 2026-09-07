from __future__ import annotations

import json
from typing import Any

import pytest
from test_cao_notification_delivery_lane import _attached, _report
from test_worker_output_delivery import _capture_fixture, _event, _read_request, _terminal

from cao_control_plane.errors import AuthenticationError, AuthorizationError, ConflictError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    GoalRevision,
    MessageKind,
    ReviewInput,
)

_PAST = "2000-01-01T00:00:00Z"


def _delivery(service: Any, message_id: str) -> dict[str, Any]:
    row = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?", (message_id,)
    )
    assert row is not None
    return dict(row)


def _count(service: Any, message_id: str, event_type: str) -> int:
    return int(
        service.db.fetchone(
            "SELECT COUNT(*) FROM events WHERE aggregate_id = ? AND event_type = ?",
            (message_id, event_type),
        )[0]
    )


def _boundary_fixture(system: dict[str, Any], *, structured: bool = False):
    service = system["service"]
    if structured:
        actor = _attached(service, "settlement")
        work = _report(system, actor, "settlement")
    else:
        actor, work, binding = _capture_fixture(system)
        service.observe_worker_output(**binding, event=_event())
        _terminal(service, binding)
    work = service.get_work(work["id"])
    boundary = work["open_boundaries"][0]
    source = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND "
        "json_extract(payload_json, '$.boundary_id') = ? ORDER BY sequence LIMIT 1",
        (work["id"], boundary["id"]),
    )["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered', attempts = 3, "
        "delivered_at = ? WHERE message_id = ?",
        (_PAST, source),
    )
    # Model the exact persisted notification-recovery boundary, independently
    # of provider terminal-proof parsing or any live native conversation.
    with service.db.transaction() as connection:
        service._supersede_boundary_deliveries_tx(
            connection,
            boundary_id=boundary["id"],
            recipient_id=actor["id"],
            reason="incomplete-reasoner-turn",
        )
        successor = service._message(
            connection,
            sender_id=system["worker"]["id"],
            recipient_id=actor["id"],
            kind=MessageKind.SYSTEM,
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            goal_version=work["goal_version"],
            payload={
                "action": "recover_incomplete_reasoner_turn",
                "boundary_id": boundary["id"],
                "generation": work["generation"],
                "summary": "The same unresolved boundary remains available for review.",
            },
            idempotency_key="settlement:successor",
        )
    return actor, work, boundary, source, successor["id"]


def _review_and_accept(service: Any, actor: dict[str, Any], work: dict[str, Any]) -> None:
    current = service.get_work(work["id"])
    for output in current["worker_outputs"]:
        if output["capture_state"] == "available" and output["byte_count"]:
            service.read_worker_output(actor, _read_request(current, output))
    service.review(
        actor,
        ReviewInput(
            attempt_id=current["current_attempt"]["id"],
            verdict="ok",
            summary="The exact retained result satisfies the bounded acceptance.",
            evidence=[{"check": "retained-result", "result": "verified"}],
            idempotency_key=f"settlement:review:{current['id']}",
        ),
    )
    boundary = current["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        actor,
        current["id"],
        boundary_id=boundary["id"],
        expected_generation=current["generation"],
        idempotency_key=f"settlement:accept-turn:{current['id']}",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=current["generation"],
            kind="accept",
            reason="The independent review accepts the exact result.",
        ),
    )


@pytest.mark.parametrize("structured", [False, True])
def test_resolved_dead_notification_can_be_explicitly_received_before_requester_acceptance(
    system, structured: bool
) -> None:
    service = system["service"]
    actor, work, boundary, source, successor = _boundary_fixture(system, structured=structured)
    source_before = _delivery(service, source)
    successor_before = _delivery(service, successor)

    _review_and_accept(service, actor, work)

    current = service.get_work(work["id"])
    assert current["state"] == "waiting_user"
    assert current["requester_decisions"] == []
    assert _delivery(service, source) == source_before
    assert _delivery(service, successor) == successor_before
    assert _count(service, successor, "message.handled") == 0
    assert service.acknowledge(actor, AckInput(message_ids=[source]))["acknowledged"] == [source]
    received = _delivery(service, source)
    assert received["state"] == "handled"
    assert received["acknowledged_at"] and received["handled_at"]
    for field in ("generation", "attempts", "delivered_at", "last_error", "runtime_session_id"):
        assert received[field] == source_before[field]
    audit = service.db.fetchone(
        "SELECT data_json FROM events WHERE aggregate_id = ? "
        "AND event_type = 'message.dead_acknowledged'",
        (source,),
    )
    assert json.loads(audit["data_json"])["boundary_id"] == boundary["id"]
    assert _count(service, source, "message.dead_acknowledged") == 1
    assert service.acknowledge(actor, AckInput(message_ids=[source]))["acknowledged"] == [source]
    assert service.mark_message_handled(actor, source, evidence="Exact receipt replay")[
        "state"
    ] == ("handled")
    assert _count(service, source, "message.dead_acknowledged") == 1
    assert _count(service, source, "message.handled") == 0
    assert _delivery(service, source) == received
    inbox = {item["id"] for item in service.get_inbox(actor)["items"]}
    assert source not in inbox and successor in inbox
    assert service.get_work(work["id"])["requester_decisions"] == []


@pytest.mark.parametrize("state", ["queued", "leased", "delivered", "dispatched"])
def test_disposition_never_fabricates_receipt_for_an_unread_successor(system, state: str) -> None:
    service = system["service"]
    actor, work, _, _, successor = _boundary_fixture(system)
    service.db.execute(
        "UPDATE message_deliveries SET state = ?, owner_token = '', lease_until = NULL "
        "WHERE message_id = ?",
        (state, successor),
    )
    before = _delivery(service, successor)

    _review_and_accept(service, actor, work)

    assert _delivery(service, successor) == before
    assert _count(service, successor, "message.acknowledged") == 0
    assert _count(service, successor, "message.handled") == 0
    assert successor in {item["id"] for item in service.get_inbox(actor)["items"]}
    if state == "dispatched":
        with pytest.raises(ConflictError, match="unknown CAO handoff"):
            service.acknowledge(actor, AckInput(message_ids=[successor]))
        assert _delivery(service, successor) == before
    else:
        service.acknowledge(actor, AckInput(message_ids=[successor]))
        service.mark_message_handled(actor, successor, evidence="Explicit exact successor receipt")
        service.mark_message_handled(actor, successor, evidence="Explicit exact successor receipt")
        assert _count(service, successor, "message.acknowledged") == 1
        assert _count(service, successor, "message.handled") == 1


def test_disposition_can_settle_a_previously_explicitly_acknowledged_notification(system) -> None:
    service = system["service"]
    actor, work, _, _, successor = _boundary_fixture(system)
    service.acknowledge(actor, AckInput(message_ids=[successor]))
    before = _delivery(service, successor)

    _review_and_accept(service, actor, work)

    received = _delivery(service, successor)
    assert received["state"] == "handled"
    assert received["acknowledged_at"] == before["acknowledged_at"]
    assert received["delivered_at"] == before["delivered_at"]
    assert _count(service, successor, "message.acknowledged") == 1
    assert _count(service, successor, "message.handled") == 1
    service.acknowledge(actor, AckInput(message_ids=[successor]))
    service.mark_message_handled(actor, successor, evidence="Repeat exact receipt")
    assert _count(service, successor, "message.handled") == 1


def test_dead_notification_for_an_unresolved_boundary_remains_unacknowledgeable(system) -> None:
    service = system["service"]
    actor, _, _, source, _ = _boundary_fixture(system)
    before = _delivery(service, source)
    with pytest.raises(ConflictError, match="not available for acknowledgement"):
        service.acknowledge(actor, AckInput(message_ids=[source]))
    assert _delivery(service, source) == before
    assert _count(service, source, "message.dead_acknowledged") == 0


@pytest.mark.parametrize(
    "fence",
    [
        "foreign_attachment",
        "stale_attachment",
        "goal_version",
        "goal_digest",
        "task_digest",
        "boundary",
        "generation",
        "expected_generation",
        "generation_bool",
        "expected_generation_null",
    ],
)
def test_late_dead_ack_requires_the_exact_owning_boundary_packet(system, fence: str) -> None:
    service = system["service"]
    actor, work, _, source, _ = _boundary_fixture(system)
    _review_and_accept(service, actor, work)
    if fence == "foreign_attachment":
        actor = _attached(service, "foreign-settlement")
    elif fence == "stale_attachment":
        service.db.execute(
            "UPDATE cao_session_attachments SET generation = generation + 1 WHERE id = ?",
            (actor["_cao_attachment_id"],),
        )
    elif fence == "goal_version":
        service.db.execute(
            "UPDATE messages SET goal_version = goal_version + 1 WHERE id = ?", (source,)
        )
    elif fence in {"goal_digest", "task_digest"}:
        column = "goal_packet_digest" if fence == "goal_digest" else "task_packet_digest"
        service.db.execute(f"UPDATE messages SET {column} = ? WHERE id = ?", ("f" * 64, source))
    else:
        row = service.db.fetchone("SELECT payload_json FROM messages WHERE id = ?", (source,))
        payload = json.loads(row["payload_json"])
        if fence == "boundary":
            payload["boundary_id"] = "another-boundary"
        elif fence == "generation_bool":
            payload["generation"] = True
        elif fence == "expected_generation_null":
            payload["expected_generation"] = None
        else:
            payload[fence] = work["generation"] + 1
        service.db.execute(
            "UPDATE messages SET payload_json = ? WHERE id = ?", (json.dumps(payload), source)
        )
    before = _delivery(service, source)
    with pytest.raises((AuthenticationError, AuthorizationError, ConflictError)):
        service.acknowledge(actor, AckInput(message_ids=[source]))
    assert _delivery(service, source) == before
    assert _count(service, source, "message.dead_acknowledged") == 0


@pytest.mark.parametrize(
    "field", ["goal_packet_digest", "task_packet_digest", "boundary_id", "generation"]
)
def test_explicit_handling_rechecks_the_exact_resolved_boundary_envelope(
    system, field: str
) -> None:
    service = system["service"]
    actor, work, _, _, successor = _boundary_fixture(system, structured=True)
    _review_and_accept(service, actor, work)
    service.acknowledge(actor, AckInput(message_ids=[successor]))
    message = service.db.fetchone("SELECT payload_json FROM messages WHERE id = ?", (successor,))
    payload = json.loads(message["payload_json"])
    if field == "boundary_id":
        other = _report(system, actor, "another-resolved-boundary")
        payload[field] = other["open_boundaries"][0]["id"]
        _review_and_accept(service, actor, other)
    elif field == "generation":
        payload[field] = work["generation"] + 1
    else:
        payload[field] = "f" * 64
    service.db.execute(
        "UPDATE messages SET payload_json = ? WHERE id = ?", (json.dumps(payload), successor)
    )
    before = _delivery(service, successor)
    with pytest.raises(ConflictError, match="supervisor boundary must be disposed"):
        service.mark_message_handled(actor, successor, evidence="Receipt cannot change its packet")
    assert _delivery(service, successor) == before
    assert _count(service, successor, "message.handled") == 0


def test_exact_superseded_boundary_can_be_received_without_closing_its_work(system) -> None:
    service = system["service"]
    actor, work, boundary, source, _ = _boundary_fixture(system, structured=True)
    service.revise_goal(
        actor,
        work["id"],
        GoalRevision(
            expected_version=work["goal_version"],
            objective="Return the explicitly revised bounded answer.",
            maturity="defined",
            acceptance=["The revised result can be independently reviewed."],
            reason="The explicit objective has changed.",
            idempotency_key="settlement:revise-goal",
        ),
    )
    assert service.get_work(work["id"])["state"] == "active"
    assert service.db.fetchone(
        "SELECT 1 FROM boundary_supersessions WHERE boundary_id = ?", (boundary["id"],)
    )
    before = _delivery(service, source)
    service.acknowledge(actor, AckInput(message_ids=[source]))
    received = _delivery(service, source)
    assert received["state"] == "handled"
    for field in ("generation", "attempts", "delivered_at", "last_error"):
        assert received[field] == before[field]
    assert service.get_work(work["id"])["state"] == "active"
